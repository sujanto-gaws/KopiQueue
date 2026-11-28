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
        JobInsertFast = Load("job_insert_fast.sql");
        JobUniqueLock = Load("job_unique_lock.sql");
        JobInsertUnique = JobUniqueLock + "\n" + Load("job_insert_unique.sql");
        JobGetById = Load("job_get_by_id.sql");
        JobGetAvailable = Load("job_get_available.sql");
        JobSetStateIfRunningMany = Load("job_set_state_if_running_many.sql");
        JobSchedule = Load("job_schedule.sql");
        JobListStuck = Load("job_list_stuck.sql");
        JobRescueMany = Load("job_rescue_many.sql");
        JobDeleteFinalized = Load("job_delete_finalized.sql");
        JobCancel = Load("job_cancel.sql");
        JobDelete = Load("job_delete.sql");
        JobDeleteMany = Load("job_delete_many.sql");
        JobRetry = Load("job_retry.sql");
        _jobList = JobListVariants(Load("job_list.sql"));
        LeaderAttemptElect = Load("leader_attempt_elect.sql");
        LeaderAttemptReelect = Load("leader_attempt_reelect.sql");
        LeaderResign = Load("leader_resign.sql");
        QueueCreateOrSetUpdatedAtMany = Load("queue_create_or_set_updated_at_many.sql");
        QueueDeleteExpired = Load("queue_delete_expired.sql");
        QueueGet = Load("queue_get.sql");
        QueueList = Load("queue_list.sql");
        QueuePause = Load("queue_pause.sql");
        QueueResume = Load("queue_resume.sql");
        QueueUpdate = Load("queue_update.sql");
    }

    /// <summary>The placeholder in <see cref="JobList"/> and <see cref="JobDeleteMany"/> for the compiled metadata filter.</summary>
    public const string MetadataPlaceholder = "@@metadata@@";

    /// <summary>The latest <c>datetime2(6)</c>: a <c>null</c> time sorts as this in job lists.</summary>
    private const string LatestTime = "CONVERT(datetime2(6), '9999-12-31T23:59:59.999999', 126)";

    private readonly string[] _jobList;

    /// <summary>
    /// What the unique-key locks are named after: <c>&lt;schema&gt;.&lt;prefix&gt;</c>, so separate schemas and prefixes
    /// never share a lock. At most 96 characters, so a lock name stays within <c>sp_getapplock</c>'s 255.
    /// </summary>
    public string UniqueLockScope { get; }

    public string JobInsertMany { get; }

    /// <summary>The batched insert without <c>OUTPUT</c>, returning only its row count (<c>job_insert_fast.sql</c>).</summary>
    public string JobInsertFast { get; }

    /// <summary>Takes the unique-key locks of a batch, in key order (<c>job_unique_lock.sql</c>).</summary>
    public string JobUniqueLock { get; }

    /// <summary>The unique-key locks, then the unique-aware insert (<c>job_insert_unique.sql</c>), as one batch.</summary>
    public string JobInsertUnique { get; }

    public string JobGetById { get; }

    public string JobGetAvailable { get; }

    public string JobSetStateIfRunningMany { get; }

    public string JobSchedule { get; }

    public string JobListStuck { get; }

    public string JobRescueMany { get; }

    public string JobDeleteFinalized { get; }

    public string JobCancel { get; }

    public string JobDelete { get; }

    /// <summary><c>job_delete_many.sql</c>, with <see cref="MetadataPlaceholder"/> still to fill in.</summary>
    public string JobDeleteMany { get; }

    public string JobRetry { get; }

    public string LeaderAttemptElect { get; }

    public string LeaderAttemptReelect { get; }

    public string LeaderResign { get; }

    public string QueueCreateOrSetUpdatedAtMany { get; }

    public string QueueDeleteExpired { get; }

    public string QueueGet { get; }

    public string QueueList { get; }

    public string QueuePause { get; }

    public string QueueResume { get; }

    public string QueueUpdate { get; }

    /// <summary><c>job_list.sql</c> for a sort and direction, with <see cref="MetadataPlaceholder"/> still to fill in.</summary>
    public string JobList(JobListOrderBy orderBy, bool descending) => _jobList[((int)orderBy * 2) + (descending ? 1 : 0)];

    /// <summary>
    /// Renders <c>job_list.sql</c> for every sort and direction: by ID alone, or by a time and then ID, with a <c>null</c>
    /// time as the latest time, so that it sorts last ascending and first descending, as on PostgreSQL.
    /// </summary>
    private static string[] JobListVariants(string sql)
    {
        var orders = Enum.GetValues<JobListOrderBy>();
        var variants = new string[orders.Length * 2];
        foreach (var orderBy in orders)
        {
            var column = orderBy switch
            {
                JobListOrderBy.Id => null,
                JobListOrderBy.CreatedAt => "created_at",
                JobListOrderBy.ScheduledAt => "scheduled_at",
                JobListOrderBy.AttemptedAt => "attempted_at",
                JobListOrderBy.FinalizedAt => "finalized_at",
                _ => throw new InvalidOperationException($"No SQL for job list order {orderBy}."),
            };
            foreach (var descending in new[] { false, true })
            {
                var (op, direction) = descending ? ("<", "DESC") : (">", "ASC");
                string after, order;
                if (column is null)
                {
                    (after, order) = ($"j.id {op} @after_id", $"j.id {direction}");
                }
                else
                {
                    var value = $"ISNULL(j.{column}, {LatestTime})";
                    var position = $"ISNULL(@after_value, {LatestTime})";
                    after = $"({value} {op} {position} OR ({value} = {position} AND j.id {op} @after_id))";
                    order = $"{value} {direction}, j.id {direction}";
                }

                variants[((int)orderBy * 2) + (descending ? 1 : 0)] = sql.Replace("@@after@@", after, StringComparison.Ordinal)
                    .Replace("@@order@@", order, StringComparison.Ordinal);
            }
        }

        return variants;
    }
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
    /// SQL Server's fast path is the batched insert itself, without returning rows (<c>job_insert_fast.sql</c>):
    /// <c>SqlBulkCopy</c> is not used, because its page locks stall every producer until the caller commits (ADR-014).
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

        var counts = await ChunkedAsync(
            jobs.Count,
            (start, count) => [Jobs(InsertBatch(jobs, start, count))],
            statements.JobInsertFast,
            static reader => reader.GetInt64(0),
            cancellationToken).ConfigureAwait(false);
        return counts.Sum();
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

    public async Task<IReadOnlyList<JobScheduleResult>> JobScheduleAsync(JobScheduleParams parameters, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(parameters);
        ArgumentOutOfRangeException.ThrowIfLessThan(parameters.Max, 1, nameof(parameters));
        ArgumentException.ThrowIfNullOrEmpty(parameters.UniqueConflictError, nameof(parameters));

        // The due rows must stay locked from the read to the update, so even one statement runs in a transaction. Each
        // further statement continues after the last due position, so the jobs a statement left alone are not read again.
        var changed = await TransactedAsync(ownTransaction: true, async (connection, tx) =>
        {
            var results = new List<JobScheduleResult>();
            (DateTime At, long Id)? after = null;
            for (var remaining = parameters.Max; remaining > 0;)
            {
                var limit = Math.Min(MaxRowsPerStatement, remaining);
                var due = new List<(long Id, string Queue, string State, DateTime ScheduledAt, bool Changed)>();
                await ReadAsync(
                    connection,
                    tx,
                    statements.JobSchedule,
                    [
                        new SqlParameter("@max", SqlDbType.Int) { Value = limit },
                        new SqlParameter("@error", SqlDbType.NVarChar, -1) { Value = parameters.UniqueConflictError },
                        Scope(),
                        new SqlParameter("@after_at", SqlDbType.DateTime2) { Value = (object?)after?.At ?? DBNull.Value },
                        new SqlParameter("@after_id", SqlDbType.BigInt) { Value = (object?)after?.Id ?? DBNull.Value },
                    ],
                    reader => (reader.GetInt64(0), reader.GetString(1), reader.GetString(2), reader.GetDateTime(3), reader.GetBoolean(4)),
                    due,
                    cancellationToken).ConfigureAwait(false);
                results.AddRange(due.Where(d => d.Changed).Select(d => new JobScheduleResult(d.Id, d.Queue, FromDatabase(d.State))));
                if (due.Count < limit)
                {
                    break;
                }

                var last = due.MaxBy(d => (d.ScheduledAt, d.Id));
                after = (last.ScheduledAt, last.Id);
                remaining -= limit;
            }

            return results;
        }, cancellationToken).ConfigureAwait(false);
        changed.Sort((a, b) => a.Id.CompareTo(b.Id));
        return changed;
    }

    public async Task<JobListStuckResult> JobListStuckAsync(JobListStuckParams parameters, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(parameters);
        ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(parameters.StuckAfter, TimeSpan.Zero, nameof(parameters));
        ArgumentOutOfRangeException.ThrowIfLessThan(parameters.Max, 1, nameof(parameters));

        var rows = await RowsAsync(
            statements.JobListStuck,
            [.. Duration("stuck", parameters.StuckAfter, nameof(parameters)), new SqlParameter("@max", SqlDbType.Int) { Value = parameters.Max }],
            reader => (Job: reader.IsDBNull(0) ? null : ReadJob(reader), Now: SqlServerValues.FromDatabase(reader.GetDateTime(16))),
            cancellationToken).ConfigureAwait(false);
        return new JobListStuckResult(rows[0].Now, [.. rows.Where(r => r.Job is not null).Select(r => r.Job!)]);
    }

    public async Task<IReadOnlyList<JobRow>> JobRescueManyAsync(IReadOnlyList<JobRescueParams> jobs, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(jobs);
        for (var i = 0; i < jobs.Count; i++)
        {
            var job = jobs[i] ?? throw new ArgumentException($"Rescue {i} is null.", nameof(jobs));
            if (job.State is not (JobState.Available or JobState.Retryable or JobState.Discarded))
            {
                throw new ArgumentException($"Rescue {i} sets job {job.Id} to {job.State}; a rescue makes a job available, retryable or discarded.", nameof(jobs));
            }

            ArgumentNullException.ThrowIfNull(job.Error, nameof(jobs));
        }

        if (jobs.Count == 0)
        {
            return [];
        }

        var rows = await ChunkedAsync(
            jobs.Count,
            (start, count) => [new SqlParameter("@batch", SqlDbType.NVarChar, -1) { Value = RescueBatch(jobs, start, count) }],
            statements.JobRescueMany,
            ReadJob,
            cancellationToken).ConfigureAwait(false);
        rows.Sort((a, b) => a.Id.CompareTo(b.Id));
        return rows;
    }

    public async Task<IReadOnlyList<long>> JobDeleteFinalizedAsync(JobDeleteFinalizedParams parameters, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(parameters);
        ArgumentOutOfRangeException.ThrowIfLessThan(parameters.Max, 1, nameof(parameters));
        foreach (var retention in new[] { parameters.CompletedRetention, parameters.CancelledRetention, parameters.DiscardedRetention })
        {
            if (retention is { } r)
            {
                ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(r, TimeSpan.Zero, nameof(parameters));
            }
        }

        var ids = await RepeatAsync(
            parameters.Max,
            limit =>
            [
                .. Duration("completed", parameters.CompletedRetention, nameof(parameters)),
                .. Duration("cancelled", parameters.CancelledRetention, nameof(parameters)),
                .. Duration("discarded", parameters.DiscardedRetention, nameof(parameters)),
                new SqlParameter("@max", SqlDbType.Int) { Value = limit },
            ],
            statements.JobDeleteFinalized,
            reader => reader.GetInt64(0),
            cancellationToken).ConfigureAwait(false);
        ids.Sort();
        return ids;
    }

    public async Task<JobRow?> JobCancelAsync(long id, CancellationToken cancellationToken)
    {
        var rows = await RowsAsync(statements.JobCancel, [Id(id)], ReadJob, cancellationToken).ConfigureAwait(false);
        return rows.Count == 0 ? null : rows[0];
    }

    public Task<IReadOnlyList<long>> JobListCancelAttemptedAsync(IReadOnlyList<long> ids, CancellationToken cancellationToken) =>
        NotYet<IReadOnlyList<long>>("5.7");

    public async Task<IReadOnlyList<JobRow>> JobListAsync(JobListQuery query, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(query);
        ArgumentNullException.ThrowIfNull(query.Filter, nameof(query));
        ArgumentOutOfRangeException.ThrowIfLessThan(query.Max, 1, nameof(query));
        if (!Enum.IsDefined(query.OrderBy))
        {
            throw new ArgumentOutOfRangeException(nameof(query), query.OrderBy, "Unknown job list order.");
        }

        var (metadataSql, metadataParameters) = Metadata(query.Filter);
        return await RowsAsync(
            statements.JobList(query.OrderBy, query.Descending).Replace(SqlServerStatements.MetadataPlaceholder, metadataSql, StringComparison.Ordinal),
            [
                .. FilterParameters(query.Filter, metadataParameters),
                new SqlParameter("@after_id", SqlDbType.BigInt) { Value = (object?)query.After?.Id ?? DBNull.Value },
                new SqlParameter("@after_value", SqlDbType.DateTime2)
                {
                    Value = query.After?.Value is { } value ? SqlServerValues.ToDatabase(value) : DBNull.Value,
                },
                new SqlParameter("@max", SqlDbType.Int) { Value = query.Max },
            ],
            ReadJob,
            cancellationToken).ConfigureAwait(false);
    }

    public async Task<JobDeleteResult?> JobDeleteAsync(long id, CancellationToken cancellationToken)
    {
        var rows = await RowsAsync(
            statements.JobDelete,
            [Id(id)],
            reader => new JobDeleteResult(ReadJob(reader), Deleted: reader.GetBoolean(16)),
            cancellationToken).ConfigureAwait(false);
        return rows.Count == 0 ? null : rows[0];
    }

    public async Task<IReadOnlyList<JobRow>> JobDeleteManyAsync(JobDeleteManyQuery query, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(query);
        ArgumentNullException.ThrowIfNull(query.Filter, nameof(query));
        ArgumentOutOfRangeException.ThrowIfLessThan(query.Max, 1, nameof(query));

        var (metadataSql, metadataParameters) = Metadata(query.Filter);
        var rows = await RepeatAsync(
            query.Max,
            limit => [.. FilterParameters(query.Filter, metadataParameters), new SqlParameter("@max", SqlDbType.Int) { Value = limit }],
            statements.JobDeleteMany.Replace(SqlServerStatements.MetadataPlaceholder, metadataSql, StringComparison.Ordinal),
            ReadJob,
            cancellationToken).ConfigureAwait(false);
        rows.Sort((a, b) => a.Id.CompareTo(b.Id));
        return rows;
    }

    public async Task<JobRow?> JobRetryAsync(long id, CancellationToken cancellationToken)
    {
        try
        {
            var rows = await RowsAsync(statements.JobRetry, [Id(id)], ReadJob, cancellationToken).ConfigureAwait(false);
            return rows.Count == 0 ? null : rows[0];
        }
        catch (SqlException e) when (e.Number is 2601 or 2627)
        {
            throw new InvalidOperationException(
                $"Job {id} cannot be made available: it is unique, and another job holds its unique key in a state its uniqueness covers.", e);
        }
    }

    public async Task<bool> LeaderAttemptElectAsync(LeaderElectParams parameters, CancellationToken cancellationToken) =>
        (await RowsAsync(statements.LeaderAttemptElect, LeaderParameters(parameters), static _ => true, cancellationToken).ConfigureAwait(false)).Count > 0;

    public async Task<bool> LeaderAttemptReelectAsync(LeaderElectParams parameters, CancellationToken cancellationToken) =>
        (await RowsAsync(statements.LeaderAttemptReelect, LeaderParameters(parameters), static _ => true, cancellationToken).ConfigureAwait(false)).Count > 0;

    public async Task<bool> LeaderResignAsync(string leaderId, CancellationToken cancellationToken)
    {
        ArgumentException.ThrowIfNullOrEmpty(leaderId);
        var rows = await RowsAsync(
            statements.LeaderResign,
            [new SqlParameter("@leader_id", SqlDbType.NVarChar, 128) { Value = leaderId }],
            static _ => true,
            cancellationToken).ConfigureAwait(false);
        return rows.Count > 0;
    }

    public async Task<IReadOnlyList<string>> QueueDeleteExpiredAsync(QueueDeleteExpiredParams parameters, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(parameters);
        ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(parameters.Retention, TimeSpan.Zero, nameof(parameters));
        ArgumentOutOfRangeException.ThrowIfLessThan(parameters.Max, 1, nameof(parameters));

        var names = await RepeatAsync(
            parameters.Max,
            limit => [.. Duration("retention", parameters.Retention, nameof(parameters)), new SqlParameter("@max", SqlDbType.Int) { Value = limit }],
            statements.QueueDeleteExpired,
            reader => reader.GetString(0),
            cancellationToken).ConfigureAwait(false);
        names.Sort(StringComparer.Ordinal);
        return names;
    }

    public async Task<IReadOnlyList<QueueRow>> QueueCreateOrSetUpdatedAtManyAsync(IReadOnlyList<string> names, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(names);
        ArgumentOutOfRangeException.ThrowIfZero(names.Count, nameof(names));
        foreach (var name in names)
        {
            ArgumentException.ThrowIfNullOrEmpty(name, nameof(names));
        }

        if (names.Distinct(StringComparer.Ordinal).Count() != names.Count)
        {
            throw new ArgumentException("The queue names must be distinct.", nameof(names));
        }

        var rows = await ChunkedAsync(
            names.Count,
            (start, count) => [new SqlParameter("@names", SqlDbType.NVarChar, -1) { Value = SqlServerValues.WriteStrings(names.Skip(start).Take(count)) }],
            statements.QueueCreateOrSetUpdatedAtMany,
            ReadQueue,
            cancellationToken).ConfigureAwait(false);
        rows.Sort((a, b) => string.CompareOrdinal(a.Name, b.Name));
        return rows;
    }

    public async Task<QueueRow?> QueueGetAsync(string name, CancellationToken cancellationToken)
    {
        ArgumentException.ThrowIfNullOrEmpty(name);
        var rows = await RowsAsync(statements.QueueGet, [QueueName(name)], ReadQueue, cancellationToken).ConfigureAwait(false);
        return rows.Count == 0 ? null : rows[0];
    }

    public async Task<IReadOnlyList<QueueRow>> QueueListAsync(QueueListQuery query, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(query);
        ArgumentOutOfRangeException.ThrowIfLessThan(query.Max, 1, nameof(query));
        return await RowsAsync(
            statements.QueueList,
            [
                new SqlParameter("@after", SqlDbType.NVarChar, 128) { Value = (object?)query.After ?? DBNull.Value },
                new SqlParameter("@names", SqlDbType.NVarChar, -1) { Value = query.Names is { } names ? SqlServerValues.WriteStrings(names) : DBNull.Value },
                new SqlParameter("@max", SqlDbType.Int) { Value = query.Max },
            ],
            ReadQueue,
            cancellationToken).ConfigureAwait(false);
    }

    public async Task<bool> QueuePauseAsync(string name, CancellationToken cancellationToken)
    {
        ArgumentException.ThrowIfNullOrEmpty(name);
        return (await RowsAsync(statements.QueuePause, [QueueName(name)], static _ => true, cancellationToken).ConfigureAwait(false)).Count > 0;
    }

    public async Task<bool> QueueResumeAsync(string name, CancellationToken cancellationToken)
    {
        ArgumentException.ThrowIfNullOrEmpty(name);
        return (await RowsAsync(statements.QueueResume, [QueueName(name)], static _ => true, cancellationToken).ConfigureAwait(false)).Count > 0;
    }

    public async Task<QueueRow?> QueueUpdateAsync(QueueUpdateParams parameters, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(parameters);
        ArgumentException.ThrowIfNullOrEmpty(parameters.Name, nameof(parameters));
        ArgumentException.ThrowIfNullOrEmpty(parameters.MetadataJson, nameof(parameters));
        var rows = await RowsAsync(
            statements.QueueUpdate,
            [QueueName(parameters.Name), new SqlParameter("@metadata", SqlDbType.NVarChar, -1) { Value = parameters.MetadataJson }],
            ReadQueue,
            cancellationToken).ConfigureAwait(false);
        return rows.Count == 0 ? null : rows[0];
    }

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
    private Task<List<T>> ChunkedAsync<T>(
        int count,
        Func<int, int, SqlParameter[]> parameters,
        string sql,
        Func<SqlDataReader, T> read,
        CancellationToken cancellationToken,
        bool needsTransaction = false,
        (string Sql, SqlParameter[] Parameters)? prelude = null) =>
        TransactedAsync(needsTransaction || prelude is not null || count > MaxRowsPerStatement, async (connection, tx) =>
        {
            if (prelude is { } first)
            {
                await ReadAsync<bool>(connection, tx, first.Sql, first.Parameters, read: null, rows: null, cancellationToken).ConfigureAwait(false);
            }

            var rows = new List<T>();
            for (var start = 0; start < count; start += MaxRowsPerStatement)
            {
                await ReadAsync(connection, tx, sql, parameters(start, Math.Min(MaxRowsPerStatement, count - start)), read, rows, cancellationToken)
                    .ConfigureAwait(false);
            }

            return rows;
        }, cancellationToken);

    /// <summary>
    /// Runs <paramref name="sql"/>, a statement that handles at most its <c>@max</c> rows, until it has handled
    /// <paramref name="max"/> or handles fewer than it was asked to, with at most <see cref="MaxRowsPerStatement"/> per
    /// statement. Several statements on a pooled connection run in a transaction of the executor's own.
    /// </summary>
    /// <param name="max">The most rows in all.</param>
    /// <param name="parameters">Builds a statement's parameters for its row limit; called once per statement.</param>
    /// <param name="sql">The statement.</param>
    /// <param name="read">Reads one row.</param>
    /// <param name="cancellationToken">Cancels the statements.</param>
    private Task<List<T>> RepeatAsync<T>(int max, Func<int, SqlParameter[]> parameters, string sql, Func<SqlDataReader, T> read, CancellationToken cancellationToken) =>
        TransactedAsync(max > MaxRowsPerStatement, async (connection, tx) =>
        {
            var rows = new List<T>();
            for (var remaining = max; remaining > 0;)
            {
                var limit = Math.Min(MaxRowsPerStatement, remaining);
                var before = rows.Count;
                await ReadAsync(connection, tx, sql, parameters(limit), read, rows, cancellationToken).ConfigureAwait(false);
                if (rows.Count - before < limit)
                {
                    break;
                }

                remaining -= limit;
            }

            return rows;
        }, cancellationToken);

    /// <summary>
    /// Runs <paramref name="work"/> on the connection to use (<see cref="OpenAsync"/>), in the caller's transaction or,
    /// when <paramref name="ownTransaction"/> is set on a pooled connection, in a READ COMMITTED transaction of the
    /// executor's own that commits when the work succeeds.
    /// </summary>
    private async Task<T> TransactedAsync<T>(
        bool ownTransaction,
        Func<SqlConnection, SqlTransaction?, Task<T>> work,
        CancellationToken cancellationToken)
    {
        var (connection, owned) = await OpenAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            var own = owned && ownTransaction
                ? (SqlTransaction)await connection.BeginTransactionAsync(IsolationLevel.ReadCommitted, cancellationToken).ConfigureAwait(false)
                : null;
            try
            {
                var result = await work(connection, own ?? transaction).ConfigureAwait(false);
                if (own is not null)
                {
                    await own.CommitAsync(cancellationToken).ConfigureAwait(false);
                }

                return result;
            }
            finally
            {
                // Disposing a transaction that did not commit rolls it back.
                if (own is not null)
                {
                    await own.DisposeAsync().ConfigureAwait(false);
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

    /// <summary>Runs one statement and adds every row of every result set to <paramref name="rows"/>, if given.</summary>
    private static async Task ReadAsync<T>(
        SqlConnection connection,
        SqlTransaction? tx,
        string sql,
        SqlParameter[] parameters,
        Func<SqlDataReader, T>? read,
        List<T>? rows,
        CancellationToken cancellationToken)
    {
        var command = new SqlCommand(sql, connection, tx);
        await using (command.ConfigureAwait(false))
        {
            command.Parameters.AddRange(parameters);
            var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
            await using (reader.ConfigureAwait(false))
            {
                do
                {
                    while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
                    {
                        if (read is not null)
                        {
                            rows?.Add(read(reader));
                        }
                    }
                }
                while (await reader.NextResultAsync(cancellationToken).ConfigureAwait(false));
            }
        }
    }

    /// <summary>
    /// A duration as two parameters, <c>@&lt;name&gt;_s</c> whole seconds and <c>@&lt;name&gt;_us</c> microseconds, since
    /// <c>DATEADD</c> takes an <c>int</c>; both <c>null</c> for <c>null</c>.
    /// </summary>
    /// <exception cref="ArgumentOutOfRangeException">The duration is longer than about 68 years.</exception>
    internal static SqlParameter[] Duration(string name, TimeSpan? value, string paramName)
    {
        object seconds = DBNull.Value, microseconds = DBNull.Value;
        if (value is { } duration)
        {
            var whole = duration.Ticks / TimeSpan.TicksPerSecond;
            if (whole > int.MaxValue)
            {
                throw new ArgumentOutOfRangeException(paramName, duration, "SQL Server takes durations of up to about 68 years.");
            }

            seconds = (int)whole;
            microseconds = (int)(duration.Ticks % TimeSpan.TicksPerSecond / 10);
        }

        return
        [
            new SqlParameter($"@{name}_s", SqlDbType.Int) { Value = seconds },
            new SqlParameter($"@{name}_us", SqlDbType.Int) { Value = microseconds },
        ];
    }

    private static SqlParameter Id(long id) => new("@id", SqlDbType.BigInt) { Value = id };

    private static SqlParameter QueueName(string name) => new("@name", SqlDbType.NVarChar, 128) { Value = name };

    private static SqlParameter[] LeaderParameters(LeaderElectParams parameters)
    {
        ArgumentNullException.ThrowIfNull(parameters);
        ArgumentException.ThrowIfNullOrEmpty(parameters.LeaderId, nameof(parameters));
        ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(parameters.Ttl, TimeSpan.Zero, nameof(parameters));
        return
        [
            new SqlParameter("@leader_id", SqlDbType.NVarChar, 128) { Value = parameters.LeaderId },
            .. Duration("ttl", parameters.Ttl, nameof(parameters)),
        ];
    }

    /// <summary>The compiled metadata filter of <paramref name="filter"/>, or a predicate that is always true.</summary>
    private static (string Sql, IReadOnlyList<SqlParameter> Parameters) Metadata(JobFilter filter) =>
        filter.MetadataContains is { } contains ? SqlServerJsonContainment.Compile(contains, "j.metadata") : ("1 = 1", []);

    /// <summary>
    /// The filter parameters of <c>job_list.sql</c> and <c>job_delete_many.sql</c>: each list as a JSON array, or <c>null</c>
    /// for any, then copies of the metadata filter's parameters, since a parameter belongs to one command.
    /// </summary>
    internal static SqlParameter[] FilterParameters(JobFilter filter, IReadOnlyList<SqlParameter> metadata) =>
    [
        new SqlParameter("@ids", SqlDbType.NVarChar, -1)
        {
            Value = filter.Ids is { } ids ? SqlServerValues.JsonArray(w => ids.ToList().ForEach(w.WriteNumberValue)) : DBNull.Value,
        },
        new SqlParameter("@kinds", SqlDbType.NVarChar, -1) { Value = filter.Kinds is { } kinds ? SqlServerValues.WriteStrings(kinds) : DBNull.Value },
        new SqlParameter("@queues", SqlDbType.NVarChar, -1) { Value = filter.Queues is { } queues ? SqlServerValues.WriteStrings(queues) : DBNull.Value },
        new SqlParameter("@states", SqlDbType.NVarChar, -1)
        {
            Value = filter.States is { } states ? SqlServerValues.WriteStrings(states.Select(ToDatabase)) : DBNull.Value,
        },
        new SqlParameter("@priorities", SqlDbType.NVarChar, -1)
        {
            Value = filter.Priorities is { } priorities ? SqlServerValues.JsonArray(w => priorities.ToList().ForEach(w.WriteNumberValue)) : DBNull.Value,
        },
        .. metadata.Select(p => (SqlParameter)((ICloneable)p).Clone()),
    ];

    /// <summary>The <c>@batch</c> parameter of <c>job_rescue_many.sql</c> for one chunk.</summary>
    internal static string RescueBatch(IReadOnlyList<JobRescueParams> jobs, int start, int count) => SqlServerValues.JsonArray(writer =>
    {
        for (var i = start; i < start + count; i++)
        {
            var job = jobs[i];
            writer.WriteStartObject();
            writer.WriteNumber("i", job.Id);
            writer.WriteString("at", SqlServerValues.ToJson(job.AttemptedAt));
            writer.WriteString("st", ToDatabase(job.State));
            if (job.ScheduledAt is { } scheduledAt)
            {
                writer.WriteString("s", SqlServerValues.ToJson(scheduledAt));
            }

            writer.WriteString("e", FormatError(job.Error));
            writer.WriteEndObject();
        }
    });

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
