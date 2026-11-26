using System.Diagnostics.CodeAnalysis;
using System.Globalization;
using System.Text;
using System.Text.Json;
using KopiQueue.Drivers;
using KopiQueue.Migrations;
using Npgsql;
using NpgsqlTypes;

namespace KopiQueue.Npgsql;

/// <summary>The driver's SQL, loaded from embedded resources and rendered once for a schema and prefix.</summary>
internal sealed class NpgsqlStatements
{
    private const string ResourceRoot = "kopi-sql/";

    public NpgsqlStatements(string? schema, string schemaQualifier, string tablePrefix)
    {
        Schema = schema;

        string Load(string name)
        {
            using var stream = typeof(NpgsqlStatements).Assembly.GetManifestResourceStream(ResourceRoot + name)
                ?? throw new InvalidOperationException($"Embedded SQL '{name}' is missing.");
            using var reader = new StreamReader(stream, Encoding.UTF8);
            return MigrationTemplate.Render(reader.ReadToEnd(), schemaQualifier, tablePrefix);
        }

        JobInsertMany = Load("job_insert_many.sql");
        JobGetById = Load("job_get_by_id.sql");
        JobInsertNotify = Load("job_insert_notify.sql");
    }

    /// <summary>The configured schema, unquoted, or <c>null</c> for the connection's current schema.</summary>
    public string? Schema { get; }

    public string JobInsertMany { get; }

    public string JobGetById { get; }

    public string JobInsertNotify { get; }
}

/// <summary>
/// Runs the driver's statements either on a pooled connection per operation, or on the connection of a caller's transaction.
/// </summary>
[SuppressMessage("Security", "CA2100:Review SQL queries for security vulnerabilities",
    Justification = "Statements are the driver's own embedded SQL, rendered with validated identifiers; values are parameters.")]
internal sealed class NpgsqlExecutor(NpgsqlStatements statements, NpgsqlDataSource dataSource, NpgsqlTransaction? transaction) : IKopiExecutor
{
    public async Task<IReadOnlyList<JobInsertResult>> JobInsertManyAsync(IReadOnlyList<JobInsertParams> jobs, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(jobs);
        if (jobs.Count == 0)
        {
            return [];
        }

        var count = jobs.Count;
        var args = new string[count];
        var kinds = new string[count];
        var maxAttempts = new short[count];
        var metadata = new string[count];
        var priorities = new short[count];
        var queues = new string[count];
        var scheduledAt = new DateTime?[count];
        var states = new string[count];
        var tags = new string[count];
        var uniqueKeys = new byte[]?[count];
        var uniqueStates = new int?[count];

        for (var i = 0; i < count; i++)
        {
            var job = jobs[i] ?? throw new ArgumentException($"Job {i} is null.", nameof(jobs));
            if (job.UniqueKey is not null)
            {
                throw new NotSupportedException("Unique jobs are not supported yet.");
            }

            args[i] = job.ArgsJson;
            kinds[i] = job.Kind;
            maxAttempts[i] = checked((short)job.MaxAttempts);
            metadata[i] = job.MetadataJson;
            priorities[i] = checked((short)job.Priority);
            queues[i] = job.Queue;
            scheduledAt[i] = job.ScheduledAt?.UtcDateTime;
            states[i] = ToDatabase(job.State);
            tags[i] = JoinTags(job.Tags, i);
            uniqueKeys[i] = job.UniqueKey;
            uniqueStates[i] = job.UniqueStates;
        }

        // Notify each queue that received available jobs, once (design §4.3). Scheduled jobs are announced by the scheduler.
        var notifyQueues = jobs.Where(j => j.State == JobState.Available).Select(j => j.Queue).Distinct(StringComparer.Ordinal).ToArray();
        var notify = notifyQueues.Length == 0
            ? null
            : new NpgsqlBatchCommand(statements.JobInsertNotify)
            {
                Parameters = { new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Text, Value = (object?)statements.Schema ?? DBNull.Value }, Array(NpgsqlDbType.Text, notifyQueues) },
            };

        var rows = await QueryAsync(
            statements.JobInsertMany,
            [
                Array(NpgsqlDbType.Jsonb, args), Array(NpgsqlDbType.Text, kinds), Array(NpgsqlDbType.Smallint, maxAttempts),
                Array(NpgsqlDbType.Jsonb, metadata), Array(NpgsqlDbType.Smallint, priorities), Array(NpgsqlDbType.Text, queues),
                Array(NpgsqlDbType.TimestampTz, scheduledAt), Array(NpgsqlDbType.Text, states), Array(NpgsqlDbType.Text, tags),
                Array(NpgsqlDbType.Bytea, uniqueKeys), Array(NpgsqlDbType.Integer, uniqueStates),
            ],
            notify,
            cancellationToken).ConfigureAwait(false);

        // Ids ascend in input order (the statement inserts ordered by ordinal); RETURNING order is not guaranteed.
        return [.. rows.OrderBy(r => r.Id).Select(r => new JobInsertResult(r, UniqueSkippedAsDuplicate: false))];
    }

    public async Task<JobRow?> JobGetByIdAsync(long id, CancellationToken cancellationToken)
    {
        var rows = await QueryAsync(statements.JobGetById, [new NpgsqlParameter { Value = id }], then: null, cancellationToken).ConfigureAwait(false);
        return rows.Count == 0 ? null : rows[0];
    }

    private static NpgsqlParameter Array<T>(NpgsqlDbType elementType, T[] values) =>
        new() { NpgsqlDbType = NpgsqlDbType.Array | elementType, Value = values };

    private static string JoinTags(IReadOnlyList<string> tags, int index)
    {
        foreach (var tag in tags)
        {
            if (tag is null || tag.Contains(',', StringComparison.Ordinal))
            {
                throw new ArgumentException($"Job {index} has a tag that is null or contains a comma.", nameof(tags));
            }
        }

        return string.Join(',', tags);
    }

    /// <summary>
    /// Runs <paramref name="sql"/>, which returns job rows, and then <paramref name="then"/> if given, as one batch. Without a
    /// caller transaction PostgreSQL runs a batch as one implicit transaction, so the two commit or fail together.
    /// </summary>
    private async Task<List<JobRow>> QueryAsync(string sql, NpgsqlParameter[] parameters, NpgsqlBatchCommand? then, CancellationToken cancellationToken)
    {
        var connection = transaction?.Connection;
        var ownConnection = connection is null;
        if (ownConnection)
        {
            connection = await dataSource.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        }

        try
        {
            var batch = new NpgsqlBatch(connection, transaction);
            await using (batch.ConfigureAwait(false))
            {
                var query = new NpgsqlBatchCommand(sql);
                query.Parameters.AddRange(parameters);
                batch.BatchCommands.Add(query);
                if (then is not null)
                {
                    batch.BatchCommands.Add(then);
                }

                var reader = await batch.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
                await using (reader.ConfigureAwait(false))
                {
                    var rows = new List<JobRow>();
                    while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
                    {
                        rows.Add(ReadJob(reader));
                    }

                    // Drain the follow-up command so that its errors surface here.
                    while (await reader.NextResultAsync(cancellationToken).ConfigureAwait(false))
                    {
                    }

                    return rows;
                }
            }
        }
        finally
        {
            if (ownConnection)
            {
                await connection!.DisposeAsync().ConfigureAwait(false);
            }
        }
    }

    /// <summary>Maps the column list shared by the driver's job statements.</summary>
    private static JobRow ReadJob(NpgsqlDataReader reader) => new()
    {
        Id = reader.GetInt64(0),
        Kind = reader.GetString(1),
        Queue = reader.GetString(2),
        State = FromDatabase(reader.GetString(3)),
        Priority = reader.GetInt16(4),
        ArgsJson = reader.GetString(5),
        MetadataJson = reader.GetString(6),
        Tags = reader.GetFieldValue<string[]>(7),
        Attempt = reader.GetInt16(8),
        MaxAttempts = reader.GetInt16(9),
        AttemptedAt = reader.IsDBNull(10) ? null : reader.GetFieldValue<DateTimeOffset>(10),
        AttemptedBy = reader.IsDBNull(11) ? [] : reader.GetFieldValue<string[]>(11),
        Errors = reader.IsDBNull(12) ? [] : [.. reader.GetFieldValue<string[]>(12).Select(ParseError)],
        CreatedAt = reader.GetFieldValue<DateTimeOffset>(13),
        ScheduledAt = reader.GetFieldValue<DateTimeOffset>(14),
        FinalizedAt = reader.IsDBNull(15) ? null : reader.GetFieldValue<DateTimeOffset>(15),
    };

    /// <summary>Parses one element of <c>errors</c>: <c>{"at": ..., "attempt": n, "error": "...", "trace": "..."}</c>.</summary>
    internal static AttemptError ParseError(string json)
    {
        using var document = JsonDocument.Parse(json);
        var root = document.RootElement;
        var at = root.TryGetProperty("at", out var a) && a.ValueKind == JsonValueKind.String
            ? DateTimeOffset.Parse(a.GetString()!, CultureInfo.InvariantCulture, DateTimeStyles.AssumeUniversal)
            : default;
        var attempt = root.TryGetProperty("attempt", out var n) && n.TryGetInt32(out var value) ? value : 0;
        var error = root.TryGetProperty("error", out var e) && e.ValueKind == JsonValueKind.String ? e.GetString()! : string.Empty;
        var trace = root.TryGetProperty("trace", out var t) && t.ValueKind == JsonValueKind.String ? t.GetString() : null;
        return new AttemptError(at, attempt, error, trace);
    }

    internal static string ToDatabase(JobState state) => state switch
    {
        JobState.Available => "available",
        JobState.Cancelled => "cancelled",
        JobState.Completed => "completed",
        JobState.Discarded => "discarded",
        JobState.Pending => "pending",
        JobState.Retryable => "retryable",
        JobState.Running => "running",
        JobState.Scheduled => "scheduled",
        _ => throw new ArgumentOutOfRangeException(nameof(state), state, "Unknown job state."),
    };

    internal static JobState FromDatabase(string state) => state switch
    {
        "available" => JobState.Available,
        "cancelled" => JobState.Cancelled,
        "completed" => JobState.Completed,
        "discarded" => JobState.Discarded,
        "pending" => JobState.Pending,
        "retryable" => JobState.Retryable,
        "running" => JobState.Running,
        "scheduled" => JobState.Scheduled,
        _ => throw new InvalidOperationException($"Unknown job state '{state}' in the database."),
    };
}
