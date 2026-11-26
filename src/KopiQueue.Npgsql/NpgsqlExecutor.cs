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
        JobInsertFast = Load("job_insert_fast.sql").Trim();
        JobGetAvailable = Load("job_get_available.sql");
    }

    /// <summary>The configured schema, unquoted, or <c>null</c> for the connection's current schema.</summary>
    public string? Schema { get; }

    public string JobInsertMany { get; }

    public string JobGetById { get; }

    public string JobInsertNotify { get; }

    public string JobGetAvailable { get; }

    /// <summary>The binary <c>COPY</c> command of the fast insert path. Its columns match <see cref="NpgsqlExecutor.JobInsertFastAsync"/>.</summary>
    public string JobInsertFast { get; }
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
            var job = CheckSupported(jobs, i);
            args[i] = job.ArgsJson;
            kinds[i] = job.Kind;
            maxAttempts[i] = checked((short)job.MaxAttempts);
            metadata[i] = job.MetadataJson;
            priorities[i] = checked((short)job.Priority);
            queues[i] = job.Queue;
            scheduledAt[i] = job.ScheduledAt?.UtcDateTime;
            states[i] = ToDatabase(job.State);
            tags[i] = string.Join(',', job.Tags);
            uniqueKeys[i] = job.UniqueKey;
            uniqueStates[i] = job.UniqueStates;
        }

        var notifyQueues = NotifyQueues(jobs);
        NpgsqlBatchCommand? notify = null;
        if (notifyQueues.Length > 0)
        {
            notify = new NpgsqlBatchCommand(statements.JobInsertNotify);
            notify.Parameters.AddRange(NotifyParameters(notifyQueues));
        }

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

    /// <summary>
    /// Inserts with binary <c>COPY</c> and notifies, in the caller's transaction or in one of its own (FR-INS-04).
    /// Jobs without <see cref="JobInsertParams.ScheduledAt"/> get the transaction's <c>now()</c>, as the batch insert does.
    /// </summary>
    public async Task<long> JobInsertFastAsync(IReadOnlyList<JobInsertParams> jobs, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(jobs);
        for (var i = 0; i < jobs.Count; i++)
        {
            CheckSupported(jobs, i);
        }

        if (jobs.Count == 0)
        {
            return 0;
        }

        var connection = transaction?.Connection;
        var ownConnection = connection is null;
        if (ownConnection)
        {
            connection = await dataSource.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        }

        NpgsqlTransaction? ownTransaction = null;
        try
        {
            ownTransaction = ownConnection ? await connection!.BeginTransactionAsync(cancellationToken).ConfigureAwait(false) : null;
            var tx = transaction ?? ownTransaction;

            DateTime now = default;
            if (jobs.Any(j => j.ScheduledAt is null))
            {
                var nowCommand = new NpgsqlCommand("SELECT now()", connection, tx);
                await using (nowCommand.ConfigureAwait(false))
                {
                    now = (DateTime)(await nowCommand.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false))!;
                }
            }

            ulong copied;
            var importer = await connection!.BeginBinaryImportAsync(statements.JobInsertFast, cancellationToken).ConfigureAwait(false);
            await using (importer.ConfigureAwait(false))
            {
                foreach (var job in jobs)
                {
                    await importer.StartRowAsync(cancellationToken).ConfigureAwait(false);
                    await importer.WriteAsync(job.ArgsJson, NpgsqlDbType.Jsonb, cancellationToken).ConfigureAwait(false);
                    await importer.WriteAsync(job.Kind, NpgsqlDbType.Text, cancellationToken).ConfigureAwait(false);
                    await importer.WriteAsync(checked((short)job.MaxAttempts), NpgsqlDbType.Smallint, cancellationToken).ConfigureAwait(false);
                    await importer.WriteAsync(job.MetadataJson, NpgsqlDbType.Jsonb, cancellationToken).ConfigureAwait(false);
                    await importer.WriteAsync(checked((short)job.Priority), NpgsqlDbType.Smallint, cancellationToken).ConfigureAwait(false);
                    await importer.WriteAsync(job.Queue, NpgsqlDbType.Text, cancellationToken).ConfigureAwait(false);
                    await importer.WriteAsync(job.ScheduledAt?.UtcDateTime ?? now, NpgsqlDbType.TimestampTz, cancellationToken).ConfigureAwait(false);

                    // An enum's binary form is its label's text, so the state is written as text.
                    await importer.WriteAsync(ToDatabase(job.State), NpgsqlDbType.Text, cancellationToken).ConfigureAwait(false);

                    // tags is varchar(255)[], and binary COPY checks the array's element type.
                    await importer.WriteAsync(job.Tags.ToArray(), NpgsqlDbType.Array | NpgsqlDbType.Varchar, cancellationToken).ConfigureAwait(false);
                }

                copied = await importer.CompleteAsync(cancellationToken).ConfigureAwait(false);
            }

            var notifyQueues = NotifyQueues(jobs);
            if (notifyQueues.Length > 0)
            {
                var notify = new NpgsqlCommand(statements.JobInsertNotify, connection, tx);
                await using (notify.ConfigureAwait(false))
                {
                    notify.Parameters.AddRange(NotifyParameters(notifyQueues));
                    await notify.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
                }
            }

            if (ownTransaction is not null)
            {
                await ownTransaction.CommitAsync(cancellationToken).ConfigureAwait(false);
            }

            return checked((long)copied);
        }
        finally
        {
            if (ownTransaction is not null)
            {
                await ownTransaction.DisposeAsync().ConfigureAwait(false);
            }

            if (ownConnection)
            {
                await connection!.DisposeAsync().ConfigureAwait(false);
            }
        }
    }

    public async Task<JobRow?> JobGetByIdAsync(long id, CancellationToken cancellationToken)
    {
        var rows = await QueryAsync(statements.JobGetById, [new NpgsqlParameter { Value = id }], then: null, cancellationToken).ConfigureAwait(false);
        return rows.Count == 0 ? null : rows[0];
    }

    public async Task<IReadOnlyList<JobRow>> JobGetAvailableAsync(JobGetAvailableParams parameters, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(parameters);
        ArgumentException.ThrowIfNullOrEmpty(parameters.Queue, nameof(parameters));
        ArgumentException.ThrowIfNullOrEmpty(parameters.ClientId, nameof(parameters));
        ArgumentOutOfRangeException.ThrowIfLessThan(parameters.Max, 1, nameof(parameters));
        ArgumentOutOfRangeException.ThrowIfLessThan(parameters.MaxAttemptedBy, 1, nameof(parameters));

        var rows = await QueryAsync(
            statements.JobGetAvailable,
            [
                new() { NpgsqlDbType = NpgsqlDbType.Text, Value = parameters.Queue },
                new() { NpgsqlDbType = NpgsqlDbType.Text, Value = parameters.ClientId },
                new() { NpgsqlDbType = NpgsqlDbType.Integer, Value = parameters.Max },
                new() { NpgsqlDbType = NpgsqlDbType.Integer, Value = parameters.MaxAttemptedBy },
            ],
            then: null,
            cancellationToken).ConfigureAwait(false);
        rows.Sort(FetchOrder);
        return rows;
    }

    /// <summary>The fetch order (FR-EXE-02): <c>priority, scheduled_at, id</c>.</summary>
    private static int FetchOrder(JobRow a, JobRow b)
    {
        var byPriority = a.Priority.CompareTo(b.Priority);
        if (byPriority != 0)
        {
            return byPriority;
        }

        var byScheduled = a.ScheduledAt.CompareTo(b.ScheduledAt);
        return byScheduled != 0 ? byScheduled : a.Id.CompareTo(b.Id);
    }

    private static NpgsqlParameter Array<T>(NpgsqlDbType elementType, T[] values) =>
        new() { NpgsqlDbType = NpgsqlDbType.Array | elementType, Value = values };

    /// <summary>Rejects what the driver cannot insert yet, before any SQL runs.</summary>
    private static JobInsertParams CheckSupported(IReadOnlyList<JobInsertParams> jobs, int index)
    {
        var job = jobs[index] ?? throw new ArgumentException($"Job {index} is null.", nameof(jobs));
        if (job.UniqueKey is not null)
        {
            throw new NotSupportedException("Unique jobs are not supported yet.");
        }

        foreach (var tag in job.Tags)
        {
            // The batch insert passes tags comma-joined; the core's tag rule never allows a comma.
            if (tag is null || tag.Contains(',', StringComparison.Ordinal))
            {
                throw new ArgumentException($"Job {index} has a tag that is null or contains a comma.", nameof(jobs));
            }
        }

        return job;
    }

    /// <summary>Each queue that receives available jobs, once (design §4.3). Scheduled jobs are announced by the scheduler.</summary>
    private static string[] NotifyQueues(IReadOnlyList<JobInsertParams> jobs) =>
        [.. jobs.Where(j => j.State == JobState.Available).Select(j => j.Queue).Distinct(StringComparer.Ordinal)];

    private NpgsqlParameter[] NotifyParameters(string[] queues) =>
    [
        new() { NpgsqlDbType = NpgsqlDbType.Text, Value = (object?)statements.Schema ?? DBNull.Value },
        Array(NpgsqlDbType.Text, queues),
    ];

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
