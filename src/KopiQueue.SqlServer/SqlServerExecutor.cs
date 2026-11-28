using System.Data;
using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;
using System.Text;
using KopiQueue.Drivers;
using KopiQueue.Migrations;
using Microsoft.Data.SqlClient;
using static KopiQueue.Drivers.StoredJob;

namespace KopiQueue.SqlServer;

/// <summary>The driver's SQL, loaded from embedded resources and rendered once for a schema and prefix.</summary>
internal sealed class SqlServerStatements
{
    private const string ResourceRoot = "kopi-sql/";

    /// <param name="schema">The schema, validated and unquoted.</param>
    /// <param name="tablePrefix">The table prefix, validated.</param>
    public SqlServerStatements(string schema, string tablePrefix)
    {
        var schemaQualifier = SqlServerIdentifiers.Quote(schema) + ".";
        UniqueLockScope = schema + "." + tablePrefix;

        string Load(string name)
        {
            using var stream = typeof(SqlServerStatements).Assembly.GetManifestResourceStream(ResourceRoot + name)
                ?? throw new InvalidOperationException($"Embedded SQL '{name}' is missing.");
            using var reader = new StreamReader(stream, Encoding.UTF8);
            return MigrationTemplate.Render(reader.ReadToEnd(), schemaQualifier, tablePrefix);
        }

        JobInsertMany = Load("job_insert_many.sql");
        JobUniqueLock = Load("job_unique_lock.sql");
        JobInsertUnique = JobUniqueLock + "\n" + Load("job_insert_unique.sql");
        JobGetById = Load("job_get_by_id.sql");
        JobGetAvailable = Load("job_get_available.sql");
        JobSetStateIfRunningMany = Load("job_set_state_if_running_many.sql");
        QueueGet = Load("queue_get.sql");
    }

    /// <summary>
    /// What the unique-key locks are named after: <c>&lt;schema&gt;.&lt;prefix&gt;</c>, so separate schemas and prefixes
    /// never share a lock. At most 96 characters, so a lock name stays within <c>sp_getapplock</c>'s 255.
    /// </summary>
    public string UniqueLockScope { get; }

    public string JobInsertMany { get; }

    /// <summary>Takes the unique-key locks of a batch, in key order (<c>job_unique_lock.sql</c>).</summary>
    public string JobUniqueLock { get; }

    /// <summary>The unique-key locks, then the unique-aware insert (<c>job_insert_unique.sql</c>), as one batch.</summary>
    public string JobInsertUnique { get; }

    public string JobGetById { get; }

    public string JobGetAvailable { get; }

    public string JobSetStateIfRunningMany { get; }

    public string QueueGet { get; }
}

/// <summary>
/// Runs the driver's statements either on a pooled connection per operation, or on the connection of a caller's
/// transaction (FR-INS-02). Times are UTC both ways, and the JSON-array columns are read as lists (design §4.4).
/// </summary>
/// <remarks>
/// <para>
/// Batches travel as one JSON parameter that the statement reads with <c>OPENJSON</c> (design §8.1, DQ-6). A batch
/// larger than <see cref="MaxRowsPerStatement"/> runs as several statements in one transaction, the caller's or the
/// executor's own, so it stays all-or-nothing (spike 0.9: no statement touches more than 2,000 rows).
/// </para>
/// <para>
/// Every session is checked before its first statement (<see cref="SqlServerSessionCheck"/>): pooled sessions once per
/// driver (<see cref="SqlServerSessionGate"/>), a caller's session once per executor, since the caller's code may have
/// changed its settings. The operations that later M5 tasks add throw <see cref="NotSupportedException"/> until then.
/// </para>
/// </remarks>
[SuppressMessage("Security", "CA2100:Review SQL queries for security vulnerabilities",
    Justification = "Statements are the driver's own embedded SQL, rendered with validated identifiers; values are parameters.")]
internal sealed class SqlServerExecutor(
    SqlServerStatements statements,
    SqlServerConnectionFactory connections,
    SqlServerSessionGate pooledSessions,
    SqlTransaction? transaction) : IKopiExecutor
{
    /// <summary>The most rows one statement touches: the driver's <see cref="DriverCapabilities.MaxRowsPerStatement"/>.</summary>
    public const int MaxRowsPerStatement = 1000;

    private bool _callerSessionChecked;

    public async Task<IReadOnlyList<JobInsertResult>> JobInsertManyAsync(IReadOnlyList<JobInsertParams> jobs, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(jobs);
        if (jobs.Count == 0)
        {
            return [];
        }

        // A unique key the batch repeats is inserted once: later jobs with it are duplicates of the first (FR-UNQ-05).
        // Insert states (available, scheduled) are in every valid unique-states set, so the first always holds the slot.
        var firstWithKey = new Dictionary<byte[], int>(UniqueKeyComparer.Instance);
        var sent = new List<JobInsertParams>(jobs.Count);
        var duplicateOf = new int[jobs.Count];
        for (var i = 0; i < jobs.Count; i++)
        {
            var job = CheckSupported(jobs, i, allowUnique: true);
            duplicateOf[i] = -1;
            if (job.UniqueKey is { } key)
            {
                if (firstWithKey.TryGetValue(key, out var first))
                {
                    duplicateOf[i] = first;
                    continue;
                }

                firstWithKey.Add(key, i);
            }

            sent.Add(job);
        }

        List<(JobRow Job, bool Skipped, byte[]? Key)> rows;
        if (firstWithKey.Count == 0)
        {
            rows = await ChunkedAsync(
                sent.Count,
                (start, count) => [Jobs(InsertBatch(sent, start, count))],
                statements.JobInsertMany,
                reader => (ReadJob(reader), false, (byte[]?)null),
                cancellationToken).ConfigureAwait(false);
        }
        else
        {
            // The locks belong to a transaction: the caller's, or one of the executor's own. A batch of several statements
            // first takes every key's lock in one sorted pass, so its chunks never take keys out of order.
            rows = await ChunkedAsync(
                sent.Count,
                (start, count) => [Jobs(InsertBatch(sent, start, count)), Scope()],
                statements.JobInsertUnique,
                reader => (ReadJob(reader), reader.GetBoolean(16), reader.IsDBNull(17) ? null : reader.GetSqlBinary(17).Value),
                cancellationToken,
                needsTransaction: true,
                prelude: sent.Count > MaxRowsPerStatement
                    ? (statements.JobUniqueLock, [Jobs(InsertBatch(sent, 0, sent.Count)), Scope()])
                    : null).ConfigureAwait(false);
        }

        if (rows.Count != sent.Count)
        {
            throw new InvalidOperationException($"The insert returned {rows.Count} rows for {sent.Count} jobs.");
        }

        // OUTPUT has no order. A unique job's row is found by its key, which the batch sends once. The other jobs were
        // all inserted, and their ids follow the input (the statements' ORDER BY), chunk after chunk.
        var byKey = new Dictionary<byte[], JobInsertResult>(UniqueKeyComparer.Instance);
        foreach (var row in rows.Where(r => r.Key is not null && firstWithKey.ContainsKey(r.Key)))
        {
            byKey.Add(row.Key!, new JobInsertResult(row.Job, row.Skipped));
        }

        using var plain = rows.Where(r => r.Key is null).Select(r => r.Job).OrderBy(j => j.Id).GetEnumerator();
        var results = new JobInsertResult[jobs.Count];
        for (var i = 0; i < jobs.Count; i++)
        {
            if (duplicateOf[i] >= 0)
            {
                results[i] = new JobInsertResult(results[duplicateOf[i]].Job, UniqueSkippedAsDuplicate: true);
            }
            else if (jobs[i].UniqueKey is { } key)
            {
                results[i] = byKey[key];
            }
            else
            {
                plain.MoveNext();
                results[i] = new JobInsertResult(plain.Current, UniqueSkippedAsDuplicate: false);
            }
        }

        return results;
    }

    /// <summary>
    /// SQL Server's fast path is the batched insert itself, without returning rows to the caller: <c>SqlBulkCopy</c> is
    /// not used, because its page locks stall every producer until the caller commits (ADR-014).
    /// </summary>
    public async Task<long> JobInsertFastAsync(IReadOnlyList<JobInsertParams> jobs, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(jobs);
        for (var i = 0; i < jobs.Count; i++)
        {
            CheckSupported(jobs, i, allowUnique: false);
        }

        if (jobs.Count == 0)
        {
            return 0;
        }

        var rows = await ChunkedAsync(
            jobs.Count,
            (start, count) => [Jobs(InsertBatch(jobs, start, count))],
            statements.JobInsertMany,
            static _ => true,
            cancellationToken).ConfigureAwait(false);
        return rows.Count;
    }

    public async Task<JobRow?> JobGetByIdAsync(long id, CancellationToken cancellationToken)
    {
        var rows = await RowsAsync(statements.JobGetById, [new SqlParameter("@id", SqlDbType.BigInt) { Value = id }], ReadJob, cancellationToken)
            .ConfigureAwait(false);
        return rows.Count == 0 ? null : rows[0];
    }

    public async Task<IReadOnlyList<JobRow>> JobGetAvailableAsync(JobGetAvailableParams parameters, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(parameters);
        ArgumentException.ThrowIfNullOrEmpty(parameters.Queue, nameof(parameters));
        ArgumentException.ThrowIfNullOrEmpty(parameters.ClientId, nameof(parameters));
        ArgumentOutOfRangeException.ThrowIfLessThan(parameters.Max, 1, nameof(parameters));
        ArgumentOutOfRangeException.ThrowIfGreaterThan(parameters.Max, MaxRowsPerStatement, nameof(parameters));
        ArgumentOutOfRangeException.ThrowIfLessThan(parameters.MaxAttemptedBy, 1, nameof(parameters));

        var rows = await RowsAsync(
            statements.JobGetAvailable,
            [
                new SqlParameter("@queue", SqlDbType.NVarChar, 128) { Value = parameters.Queue },
                new SqlParameter("@client_id", SqlDbType.NVarChar, -1) { Value = parameters.ClientId },
                new SqlParameter("@max", SqlDbType.Int) { Value = parameters.Max },
                new SqlParameter("@max_attempted_by", SqlDbType.Int) { Value = parameters.MaxAttemptedBy },
            ],
            ReadJob,
            cancellationToken).ConfigureAwait(false);
        rows.Sort(FetchOrder);
        return rows;
    }

    public async Task<IReadOnlyList<JobRow>> JobSetStateIfRunningManyAsync(IReadOnlyList<JobSetStateParams> jobs, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(jobs);
        for (var i = 0; i < jobs.Count; i++)
        {
            var job = jobs[i] ?? throw new ArgumentException($"Outcome {i} is null.", nameof(jobs));
            if (job.State is JobState.Running or JobState.Pending)
            {
                throw new ArgumentException($"Outcome {i} sets job {job.Id} to {job.State}, which an outcome cannot.", nameof(jobs));
            }
        }

        if (jobs.Count == 0)
        {
            return [];
        }

        var rows = await ChunkedAsync(
            jobs.Count,
            (start, count) => [new SqlParameter("@batch", SqlDbType.NVarChar, -1) { Value = OutcomeBatch(jobs, start, count) }],
            statements.JobSetStateIfRunningMany,
            ReadJob,
            cancellationToken).ConfigureAwait(false);

        // A job given twice comes back once, as from the PostgreSQL driver's join.
        rows.Sort((a, b) => a.Id.CompareTo(b.Id));
        return [.. rows.DistinctBy(row => row.Id)];
    }

    public Task<IReadOnlyList<JobScheduleResult>> JobScheduleAsync(JobScheduleParams parameters, CancellationToken cancellationToken) =>
        NotYet<IReadOnlyList<JobScheduleResult>>("5.6");

    public Task<JobListStuckResult> JobListStuckAsync(JobListStuckParams parameters, CancellationToken cancellationToken) =>
        NotYet<JobListStuckResult>("5.6");

    public Task<IReadOnlyList<JobRow>> JobRescueManyAsync(IReadOnlyList<JobRescueParams> jobs, CancellationToken cancellationToken) =>
        NotYet<IReadOnlyList<JobRow>>("5.6");

    public Task<IReadOnlyList<long>> JobDeleteFinalizedAsync(JobDeleteFinalizedParams parameters, CancellationToken cancellationToken) =>
        NotYet<IReadOnlyList<long>>("5.6");

    public Task<JobRow?> JobCancelAsync(long id, CancellationToken cancellationToken) =>
        NotYet<JobRow?>("5.6");

    public Task<IReadOnlyList<long>> JobListCancelAttemptedAsync(IReadOnlyList<long> ids, CancellationToken cancellationToken) =>
        NotYet<IReadOnlyList<long>>("5.7");

    public Task<IReadOnlyList<JobRow>> JobListAsync(JobListQuery query, CancellationToken cancellationToken) =>
        NotYet<IReadOnlyList<JobRow>>("5.6");

    public Task<JobDeleteResult?> JobDeleteAsync(long id, CancellationToken cancellationToken) =>
        NotYet<JobDeleteResult?>("5.6");

    public Task<IReadOnlyList<JobRow>> JobDeleteManyAsync(JobDeleteManyQuery query, CancellationToken cancellationToken) =>
        NotYet<IReadOnlyList<JobRow>>("5.6");

    public Task<JobRow?> JobRetryAsync(long id, CancellationToken cancellationToken) =>
        NotYet<JobRow?>("5.6");

    public Task<bool> LeaderAttemptElectAsync(LeaderElectParams parameters, CancellationToken cancellationToken) =>
        NotYet<bool>("5.6");

    public Task<bool> LeaderAttemptReelectAsync(LeaderElectParams parameters, CancellationToken cancellationToken) =>
        NotYet<bool>("5.6");

    public Task<bool> LeaderResignAsync(string leaderId, CancellationToken cancellationToken) =>
        NotYet<bool>("5.6");

    public Task<IReadOnlyList<string>> QueueDeleteExpiredAsync(QueueDeleteExpiredParams parameters, CancellationToken cancellationToken) =>
        NotYet<IReadOnlyList<string>>("5.6");

    public Task<IReadOnlyList<QueueRow>> QueueCreateOrSetUpdatedAtManyAsync(IReadOnlyList<string> names, CancellationToken cancellationToken) =>
        NotYet<IReadOnlyList<QueueRow>>("5.6");

    public async Task<QueueRow?> QueueGetAsync(string name, CancellationToken cancellationToken)
    {
        ArgumentException.ThrowIfNullOrEmpty(name);
        var rows = await RowsAsync(statements.QueueGet, [new SqlParameter("@name", SqlDbType.NVarChar, 128) { Value = name }], ReadQueue, cancellationToken)
            .ConfigureAwait(false);
        return rows.Count == 0 ? null : rows[0];
    }

    public Task<IReadOnlyList<QueueRow>> QueueListAsync(QueueListQuery query, CancellationToken cancellationToken) =>
        NotYet<IReadOnlyList<QueueRow>>("5.6");

    public Task<bool> QueuePauseAsync(string name, CancellationToken cancellationToken) =>
        NotYet<bool>("5.6");

    public Task<bool> QueueResumeAsync(string name, CancellationToken cancellationToken) =>
        NotYet<bool>("5.6");

    public Task<QueueRow?> QueueUpdateAsync(QueueUpdateParams parameters, CancellationToken cancellationToken) =>
        NotYet<QueueRow?>("5.6");

    /// <summary>Maps the column list shared by the driver's job statements (<c>job_get_by_id.sql</c>).</summary>
    internal static JobRow ReadJob(SqlDataReader reader) => new()
    {
        Id = reader.GetInt64(0),
        Kind = reader.GetString(1),
        Queue = reader.GetString(2),
        State = FromDatabase(reader.GetString(3)),
        Priority = reader.GetInt16(4),
        ArgsJson = reader.GetString(5),
        MetadataJson = reader.GetString(6),
        Tags = SqlServerValues.ReadStrings(reader, 7),
        Attempt = reader.GetInt16(8),
        MaxAttempts = reader.GetInt16(9),
        AttemptedAt = reader.IsDBNull(10) ? null : SqlServerValues.FromDatabase(reader.GetDateTime(10)),
        AttemptedBy = SqlServerValues.ReadStrings(reader, 11),
        Errors = SqlServerValues.ReadErrors(reader, 12),
        CreatedAt = SqlServerValues.FromDatabase(reader.GetDateTime(13)),
        ScheduledAt = SqlServerValues.FromDatabase(reader.GetDateTime(14)),
        FinalizedAt = reader.IsDBNull(15) ? null : SqlServerValues.FromDatabase(reader.GetDateTime(15)),
    };

    /// <summary>Maps the queue column list (<c>queue_get.sql</c>).</summary>
    internal static QueueRow ReadQueue(SqlDataReader reader) => new()
    {
        Name = reader.GetString(0),
        CreatedAt = SqlServerValues.FromDatabase(reader.GetDateTime(1)),
        UpdatedAt = SqlServerValues.FromDatabase(reader.GetDateTime(2)),
        PausedAt = reader.IsDBNull(3) ? null : SqlServerValues.FromDatabase(reader.GetDateTime(3)),
        MetadataJson = reader.GetString(4),
    };

    /// <summary>Runs <paramref name="sql"/> and reads every row of every result set it returns with <paramref name="read"/>.</summary>
    private Task<List<T>> RowsAsync<T>(string sql, SqlParameter[] parameters, Func<SqlDataReader, T> read, CancellationToken cancellationToken) =>
        ChunkedAsync(1, (_, _) => parameters, sql, read, cancellationToken);

    /// <summary>
    /// Runs <paramref name="sql"/> once per chunk of at most <see cref="MaxRowsPerStatement"/> of <paramref name="count"/>
    /// items, with the parameters <paramref name="parameters"/> builds for each chunk's start and length, and reads every
    /// row of every result set. Several chunks on a pooled connection run in a transaction of the executor's own, so they
    /// commit or roll back together; in a caller's transaction they simply run in it.
    /// </summary>
    /// <param name="count">How many items the statements cover.</param>
    /// <param name="parameters">Builds the parameters of the chunk that starts at an item and has a length.</param>
    /// <param name="sql">The statement.</param>
    /// <param name="read">Reads one row.</param>
    /// <param name="cancellationToken">Cancels the statements.</param>
    /// <param name="needsTransaction">Whether even one chunk must run in a transaction, for example to own its locks.</param>
    /// <param name="prelude">A statement to run first, in the same transaction, whose results are not read.</param>
    private async Task<List<T>> ChunkedAsync<T>(
        int count,
        Func<int, int, SqlParameter[]> parameters,
        string sql,
        Func<SqlDataReader, T> read,
        CancellationToken cancellationToken,
        bool needsTransaction = false,
        (string Sql, SqlParameter[] Parameters)? prelude = null)
    {
        var (connection, owned) = await OpenAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            var ownTransaction = owned && (needsTransaction || prelude is not null || count > MaxRowsPerStatement)
                ? (SqlTransaction)await connection.BeginTransactionAsync(IsolationLevel.ReadCommitted, cancellationToken).ConfigureAwait(false)
                : null;
            try
            {
                if (prelude is { } first)
                {
                    var command = new SqlCommand(first.Sql, connection, ownTransaction ?? transaction);
                    await using (command.ConfigureAwait(false))
                    {
                        command.Parameters.AddRange(first.Parameters);
                        await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
                    }
                }

                var rows = new List<T>();
                for (var start = 0; start < count; start += MaxRowsPerStatement)
                {
                    var command = new SqlCommand(sql, connection, ownTransaction ?? transaction);
                    await using (command.ConfigureAwait(false))
                    {
                        command.Parameters.AddRange(parameters(start, Math.Min(MaxRowsPerStatement, count - start)));
                        var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
                        await using (reader.ConfigureAwait(false))
                        {
                            do
                            {
                                while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
                                {
                                    rows.Add(read(reader));
                                }
                            }
                            while (await reader.NextResultAsync(cancellationToken).ConfigureAwait(false));
                        }
                    }
                }

                if (ownTransaction is not null)
                {
                    await ownTransaction.CommitAsync(cancellationToken).ConfigureAwait(false);
                }

                return rows;
            }
            finally
            {
                // Disposing a transaction that did not commit rolls it back.
                if (ownTransaction is not null)
                {
                    await ownTransaction.DisposeAsync().ConfigureAwait(false);
                }
            }
        }
        finally
        {
            if (owned)
            {
                await connection.DisposeAsync().ConfigureAwait(false);
            }
        }
    }

    /// <summary>Rejects what the insert cannot store, before any SQL runs.</summary>
    /// <param name="jobs">The batch.</param>
    /// <param name="index">The job to check.</param>
    /// <param name="allowUnique">Whether the insert handles unique jobs; the fast path does not, as on PostgreSQL.</param>
    private static JobInsertParams CheckSupported(IReadOnlyList<JobInsertParams> jobs, int index, bool allowUnique)
    {
        var job = jobs[index] ?? throw new ArgumentException($"Job {index} is null.", nameof(jobs));
        if (job.UniqueKey is not null && !allowUnique)
        {
            throw new NotSupportedException($"Job {index} is unique, which the fast insert does not support: it cannot skip duplicates.");
        }

        if (job.UniqueKey is { Length: not 32 } || (job.UniqueKey is null) != (job.UniqueStates is null))
        {
            throw new ArgumentException($"Job {index} needs both a 32-byte unique key and unique states, or neither.", nameof(jobs));
        }

        if (job.Tags is null || job.Tags.Any(tag => tag is null))
        {
            throw new ArgumentException($"Job {index} has null tags.", nameof(jobs));
        }

        return job;
    }

    /// <summary>The <c>@jobs</c> parameter of the insert statements.</summary>
    private static SqlParameter Jobs(string json) => new("@jobs", SqlDbType.NVarChar, -1) { Value = json };

    /// <summary>The <c>@scope</c> parameter of the unique-key locks.</summary>
    private SqlParameter Scope() => new("@scope", SqlDbType.NVarChar, 128) { Value = statements.UniqueLockScope };

    /// <summary>
    /// The <c>@jobs</c> parameter of the insert statements for one chunk. A unique job adds its key as 64 hex digits
    /// (<c>uk</c>) and its unique states (<c>us</c>).
    /// </summary>
    internal static string InsertBatch(IReadOnlyList<JobInsertParams> jobs, int start, int count) => SqlServerValues.JsonArray(writer =>
    {
        for (var i = start; i < start + count; i++)
        {
            var job = jobs[i];
            writer.WriteStartObject();
            writer.WriteNumber("o", i);
            writer.WriteString("a", job.ArgsJson);
            writer.WriteString("k", job.Kind);
            writer.WriteNumber("m", checked((short)job.MaxAttempts));
            writer.WriteString("d", job.MetadataJson);
            writer.WriteNumber("p", checked((short)job.Priority));
            writer.WriteString("q", job.Queue);
            if (job.ScheduledAt is { } scheduledAt)
            {
                writer.WriteString("s", SqlServerValues.ToJson(scheduledAt));
            }

            writer.WriteString("st", ToDatabase(job.State));
            writer.WriteString("t", SqlServerValues.WriteStrings(job.Tags));
            if (job.UniqueKey is { } uniqueKey)
            {
                writer.WriteString("uk", Convert.ToHexString(uniqueKey));
                writer.WriteNumber("us", job.UniqueStates!.Value);
            }

            writer.WriteEndObject();
        }
    });

    /// <summary>The <c>@batch</c> parameter of <c>job_set_state_if_running_many.sql</c> for one chunk.</summary>
    internal static string OutcomeBatch(IReadOnlyList<JobSetStateParams> jobs, int start, int count) => SqlServerValues.JsonArray(writer =>
    {
        for (var i = start; i < start + count; i++)
        {
            var job = jobs[i];
            writer.WriteStartObject();
            writer.WriteNumber("i", job.Id);
            if (job.Attempt is { } attempt)
            {
                writer.WriteNumber("at", checked((short)attempt));
            }

            if (job.Error is { } error)
            {
                writer.WriteString("e", FormatError(error));
            }

            if (job.FinalizedAt is { } finalizedAt)
            {
                writer.WriteString("f", SqlServerValues.ToJson(finalizedAt));
            }

            if (job.MetadataUpdates is { } metadata)
            {
                writer.WriteString("d", metadata);
            }

            if (job.ScheduledAt is { } scheduledAt)
            {
                writer.WriteString("s", SqlServerValues.ToJson(scheduledAt));
            }

            writer.WriteString("st", ToDatabase(job.State));
            writer.WriteEndObject();
        }
    });

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

    /// <summary>
    /// The connection to run on: the caller's, or a newly opened pooled one that the caller of this method disposes.
    /// Either way its session has passed the session check.
    /// </summary>
    /// <exception cref="InvalidOperationException">The caller's transaction has completed.</exception>
    /// <exception cref="KopiQueueConfigurationException">The session's settings would break writes.</exception>
    private async Task<(SqlConnection Connection, bool Owned)> OpenAsync(CancellationToken cancellationToken)
    {
        if (transaction is null)
        {
            var pooled = await connections.OpenAsync(cancellationToken).ConfigureAwait(false);
            try
            {
                await pooledSessions.EnsureAsync(pooled, cancellationToken).ConfigureAwait(false);
                return (pooled, true);
            }
            catch
            {
                await pooled.DisposeAsync().ConfigureAwait(false);
                throw;
            }
        }

        // SqlClient clears the connection of a transaction that committed or rolled back, including one SQL Server rolled
        // back itself, for example after an error with XACT_ABORT on.
        var connection = transaction.Connection ?? throw new InvalidOperationException(
            "The caller's transaction has completed (committed or rolled back), so KopiQueue cannot run in it.");
        if (!_callerSessionChecked)
        {
            await SqlServerSessionCheck.EnsureAsync(connection, transaction, cancellationToken).ConfigureAwait(false);
            _callerSessionChecked = true;
        }

        return (connection, false);
    }

    private static Task<T> NotYet<T>(string task, [CallerMemberName] string operation = "") =>
        Task.FromException<T>(new NotSupportedException(
            $"The SQL Server driver does not support {operation} yet: it arrives with plan task {task} (milestone M5)."));
}
