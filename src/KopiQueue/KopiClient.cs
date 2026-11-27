using System.Collections.Concurrent;
using System.Data.Common;
using System.Diagnostics.CodeAnalysis;
using System.Text.Json.Nodes;
using KopiQueue.Drivers;
using KopiQueue.Execution;
using KopiQueue.Serialization;
using Microsoft.Extensions.Logging;

namespace KopiQueue;

/// <summary>Inserts jobs (design §6.3). Further operations (bulk insert, job and queue admin) arrive in later tasks.</summary>
/// <remarks>
/// The two <c>InsertAsync</c> overloads differ by a required <see cref="DbTransaction"/>, so their optional parameters
/// never make a call ambiguous (RS0026 is suppressed for them).
/// </remarks>
#pragma warning disable RS0026 // Overloads with optional parameters: they differ by a required DbTransaction (design §6.3).
public interface IKopiClient
{
    /// <summary>
    /// Inserts a job on a connection the client takes from its pool, and commits it (FR-INS-01).
    /// Listening producers of the job's queue are notified when it commits (FR-INS-07).
    /// </summary>
    /// <typeparam name="TArgs">The job's args type, with a <see cref="JobKindAttribute"/>.</typeparam>
    /// <param name="args">The job's arguments.</param>
    /// <param name="options">Insert options. <c>null</c> uses the defaults (<see cref="InsertDefaults"/>).</param>
    /// <param name="cancellationToken">Cancels the insert.</param>
    /// <returns>The inserted job.</returns>
    /// <exception cref="ArgumentException">An insert option is not valid.</exception>
    /// <exception cref="KopiQueueConfigurationException"><typeparamref name="TArgs"/> has no valid <see cref="JobKindAttribute"/>.</exception>
    /// <exception cref="NotSupportedException">The args serialiser has no JSON metadata for <typeparamref name="TArgs"/>.</exception>
    Task<InsertResult<TArgs>> InsertAsync<TArgs>(TArgs args, InsertOptions? options = null, CancellationToken cancellationToken = default)
        where TArgs : IJobArgs;

    /// <summary>
    /// Inserts a job inside the caller's transaction (FR-INS-02): the job commits or rolls back with it, and listening
    /// producers are notified only if it commits (FR-INS-07). The caller keeps ownership of the transaction.
    /// </summary>
    /// <typeparam name="TArgs">The job's args type, with a <see cref="JobKindAttribute"/>.</typeparam>
    /// <param name="args">The job's arguments.</param>
    /// <param name="transaction">
    /// The caller's transaction, of the configured driver's database client: an <c>NpgsqlTransaction</c> for PostgreSQL.
    /// </param>
    /// <param name="options">Insert options. <c>null</c> uses the defaults (<see cref="InsertDefaults"/>).</param>
    /// <param name="cancellationToken">Cancels the insert.</param>
    /// <returns>The inserted job, visible to others once the transaction commits.</returns>
    /// <exception cref="ArgumentException">
    /// An insert option is not valid, or <paramref name="transaction"/> belongs to another database client.
    /// </exception>
    /// <exception cref="InvalidOperationException"><paramref name="transaction"/> has already completed.</exception>
    /// <exception cref="KopiQueueConfigurationException"><typeparamref name="TArgs"/> has no valid <see cref="JobKindAttribute"/>.</exception>
    Task<InsertResult<TArgs>> InsertAsync<TArgs>(TArgs args, DbTransaction transaction, InsertOptions? options = null, CancellationToken cancellationToken = default)
        where TArgs : IJobArgs;

    /// <summary>
    /// Inserts several jobs and returns them as stored, in the order given (FR-INS-04). All are inserted or none: in
    /// <paramref name="transaction"/> when given, otherwise in a transaction of the client's own. Listening producers
    /// are notified once per queue when it commits.
    /// </summary>
    /// <param name="jobs">The jobs. Each one's kind is read from its args' runtime type.</param>
    /// <param name="transaction">The caller's transaction, or <c>null</c> to use a pooled connection.</param>
    /// <param name="cancellationToken">Cancels the insert.</param>
    /// <returns>One result per job, in input order. An empty input returns an empty list without touching the database.</returns>
    /// <exception cref="ArgumentException">A job or its options are not valid; the message names the job's index. Nothing is inserted.</exception>
    /// <exception cref="KopiQueueConfigurationException">An args type has no valid <see cref="JobKindAttribute"/>.</exception>
    Task<IReadOnlyList<InsertResult>> InsertManyAsync(IEnumerable<InsertRequest> jobs, DbTransaction? transaction = null, CancellationToken cancellationToken = default);

    /// <summary>
    /// Inserts several jobs on the database's fastest path and returns how many were inserted, without reading them back
    /// (FR-INS-04): binary <c>COPY</c> on PostgreSQL. Atomicity, validation and notifications are as for
    /// <see cref="InsertManyAsync"/>.
    /// </summary>
    /// <param name="jobs">The jobs. Each one's kind is read from its args' runtime type.</param>
    /// <param name="transaction">The caller's transaction, or <c>null</c> to use a pooled connection.</param>
    /// <param name="cancellationToken">Cancels the insert.</param>
    /// <returns>The number of jobs inserted.</returns>
    /// <exception cref="ArgumentException">A job or its options are not valid; the message names the job's index. Nothing is inserted.</exception>
    /// <exception cref="KopiQueueConfigurationException">An args type has no valid <see cref="JobKindAttribute"/>.</exception>
    Task<long> InsertManyFastAsync(IEnumerable<InsertRequest> jobs, DbTransaction? transaction = null, CancellationToken cancellationToken = default);

    /// <summary>
    /// Cancels a job (FR-ERR-06, FR-ADM-05, design §5.8), on a connection the client takes from its pool.
    /// <list type="bullet">
    /// <item>A job that has not started (available, scheduled, retryable, pending) is cancelled at once and never runs.</item>
    /// <item>
    /// A running job is asked to stop: the client running it cancels the worker's <see cref="CancellationToken"/>, at once
    /// through a notification or within its <see cref="KopiClientOptions.FetchPollInterval"/> without notifications
    /// (FR-DB-05). If the worker then throws, the cancelled token's <see cref="OperationCanceledException"/> included, the
    /// job is cancelled for good, whatever attempts are left, with that exception as its error; if it returns, the job is
    /// completed. An attempt that ended before the request arrived keeps its outcome, except that a retry or a snooze
    /// becomes a cancel. A worker that ignores its token runs on until it finishes or is rescued.
    /// </item>
    /// <item>A finalised job (completed, cancelled, discarded) is left as it is, without error.</item>
    /// </list>
    /// </summary>
    /// <param name="id">The job's ID.</param>
    /// <param name="cancellationToken">Cancels the call.</param>
    /// <returns>The job as it is after the call: running is still <see cref="JobState.Running"/>.</returns>
    /// <exception cref="KopiQueueNotFoundException">No job has that ID.</exception>
    Task<JobRow> JobCancelAsync(long id, CancellationToken cancellationToken = default);

    /// <summary>
    /// Cancels a job inside the caller's transaction (FR-ADM-05), as <see cref="JobCancelAsync(long, CancellationToken)"/>
    /// does: the change commits or rolls back with the transaction, and the client running the job learns of it only if
    /// it commits. The caller keeps ownership of the transaction.
    /// </summary>
    /// <param name="id">The job's ID.</param>
    /// <param name="transaction">The caller's transaction, of the configured driver's database client.</param>
    /// <param name="cancellationToken">Cancels the call.</param>
    /// <returns>The job as it is after the call, visible to others once the transaction commits.</returns>
    /// <exception cref="KopiQueueNotFoundException">No job has that ID.</exception>
    /// <exception cref="ArgumentException"><paramref name="transaction"/> belongs to another database client.</exception>
    Task<JobRow> JobCancelAsync(long id, DbTransaction transaction, CancellationToken cancellationToken = default);

    /// <summary>Reads a job (FR-ADM-01).</summary>
    /// <param name="id">The job's ID.</param>
    /// <param name="cancellationToken">Cancels the read.</param>
    /// <returns>The job, or <c>null</c> if no job has that ID.</returns>
    Task<JobRow?> JobGetAsync(long id, CancellationToken cancellationToken = default);

    /// <summary>
    /// Lists jobs, a page at a time (FR-ADM-02): those that pass every filter of <paramref name="parameters"/>, sorted by
    /// <see cref="JobListParams.OrderBy"/> and then by ID. Pages are keyset pages: pass
    /// <see cref="JobListResult.NextCursor"/> as <see cref="JobListParams.After"/> to get the next one. Takes no locks.
    /// </summary>
    /// <param name="parameters">The filters, the sort, the page size and where to start. <c>null</c> returns the first 100 jobs by ID.</param>
    /// <param name="cancellationToken">Cancels the read.</param>
    /// <returns>The page, and the cursor of the next one if there is more.</returns>
    /// <exception cref="ArgumentOutOfRangeException"><see cref="JobListParams.Limit"/> is not from 1 to 10,000, or <see cref="JobListParams.OrderBy"/> is unknown.</exception>
    /// <exception cref="ArgumentException">
    /// A filter holds a value no job can have, or <see cref="JobListParams.After"/> is not a cursor from this method for
    /// the same sort and direction.
    /// </exception>
    Task<JobListResult> JobListAsync(JobListParams? parameters = null, CancellationToken cancellationToken = default);

    /// <summary>
    /// Deletes a job, in any state but running (FR-ADM-03), on a connection the client takes from its pool. Cancel a
    /// running job first (<see cref="JobCancelAsync(long, CancellationToken)"/>), and delete it once it has stopped.
    /// </summary>
    /// <param name="id">The job's ID.</param>
    /// <param name="cancellationToken">Cancels the call.</param>
    /// <returns>The deleted job as it was, or <c>null</c> if no job has that ID.</returns>
    /// <exception cref="KopiQueueJobRunningException">The job is running; it is not deleted.</exception>
    Task<JobRow?> JobDeleteAsync(long id, CancellationToken cancellationToken = default);

    /// <summary>
    /// Deletes a job inside the caller's transaction (FR-ADM-05), as <see cref="JobDeleteAsync(long, CancellationToken)"/>
    /// does: the delete commits or rolls back with the transaction. The caller keeps ownership of the transaction.
    /// </summary>
    /// <param name="id">The job's ID.</param>
    /// <param name="transaction">The caller's transaction, of the configured driver's database client.</param>
    /// <param name="cancellationToken">Cancels the call.</param>
    /// <returns>The deleted job as it was, or <c>null</c> if no job has that ID.</returns>
    /// <exception cref="KopiQueueJobRunningException">The job is running; it is not deleted.</exception>
    /// <exception cref="ArgumentException"><paramref name="transaction"/> belongs to another database client.</exception>
    Task<JobRow?> JobDeleteAsync(long id, DbTransaction transaction, CancellationToken cancellationToken = default);

    /// <summary>
    /// Deletes up to <see cref="JobDeleteManyParams.Limit"/> jobs that pass every filter of
    /// <paramref name="parameters"/>, in ID order, in one statement (FR-ADM-03). Running jobs are never deleted, and jobs
    /// being fetched or changed at that moment are skipped rather than waited for. Call it again until it returns
    /// nothing to delete every match.
    /// </summary>
    /// <param name="parameters">Which jobs, and the most to delete.</param>
    /// <param name="cancellationToken">Cancels the call.</param>
    /// <returns>The deleted jobs as they were, in ascending ID order.</returns>
    /// <exception cref="ArgumentOutOfRangeException"><see cref="JobDeleteManyParams.Limit"/> is not from 1 to 10,000.</exception>
    /// <exception cref="ArgumentException">
    /// A filter holds a value no job can have, or <see cref="JobFilter.States"/> includes <see cref="JobState.Running"/>.
    /// </exception>
    Task<IReadOnlyList<JobRow>> JobDeleteManyAsync(JobDeleteManyParams parameters, CancellationToken cancellationToken = default);

    /// <summary>
    /// Deletes jobs inside the caller's transaction (FR-ADM-05), as
    /// <see cref="JobDeleteManyAsync(JobDeleteManyParams, CancellationToken)"/> does. The caller keeps ownership of the
    /// transaction.
    /// </summary>
    /// <param name="parameters">Which jobs, and the most to delete.</param>
    /// <param name="transaction">The caller's transaction, of the configured driver's database client.</param>
    /// <param name="cancellationToken">Cancels the call.</param>
    /// <returns>The deleted jobs as they were, in ascending ID order.</returns>
    /// <exception cref="ArgumentOutOfRangeException"><see cref="JobDeleteManyParams.Limit"/> is not from 1 to 10,000.</exception>
    /// <exception cref="ArgumentException">
    /// A filter holds a value no job can have, <see cref="JobFilter.States"/> includes <see cref="JobState.Running"/>, or
    /// <paramref name="transaction"/> belongs to another database client.
    /// </exception>
    Task<IReadOnlyList<JobRow>> JobDeleteManyAsync(JobDeleteManyParams parameters, DbTransaction transaction, CancellationToken cancellationToken = default);

    /// <summary>
    /// Makes a job available now (FR-ERR-07), on a connection the client takes from its pool, and wakes the producers of
    /// its queue.
    /// <list type="bullet">
    /// <item>
    /// A scheduled or retryable job, or a finalised one (completed, cancelled, discarded), becomes available at once. It
    /// keeps its attempt count and errors, so a job whose attempts were used up gets one more; a cancel asked of an
    /// earlier attempt no longer applies.
    /// </item>
    /// <item>An available, pending or running job is left as it is, without error.</item>
    /// </list>
    /// </summary>
    /// <param name="id">The job's ID.</param>
    /// <param name="cancellationToken">Cancels the call.</param>
    /// <returns>The job as it is after the call.</returns>
    /// <exception cref="KopiQueueNotFoundException">No job has that ID.</exception>
    /// <exception cref="InvalidOperationException">
    /// The job is unique, and another job holds its unique key in a state its uniqueness covers. Nothing changes.
    /// </exception>
    Task<JobRow> JobRetryAsync(long id, CancellationToken cancellationToken = default);

    /// <summary>
    /// Makes a job available inside the caller's transaction (FR-ADM-05), as
    /// <see cref="JobRetryAsync(long, CancellationToken)"/> does: the change commits or rolls back with the transaction,
    /// and producers learn of it only if it commits. The caller keeps ownership of the transaction.
    /// </summary>
    /// <param name="id">The job's ID.</param>
    /// <param name="transaction">The caller's transaction, of the configured driver's database client.</param>
    /// <param name="cancellationToken">Cancels the call.</param>
    /// <returns>The job as it is after the call, visible to others once the transaction commits.</returns>
    /// <exception cref="KopiQueueNotFoundException">No job has that ID.</exception>
    /// <exception cref="InvalidOperationException">
    /// The job is unique, and another job holds its unique key in a state its uniqueness covers. On PostgreSQL the
    /// transaction is then aborted, as after any failed statement.
    /// </exception>
    /// <exception cref="ArgumentException"><paramref name="transaction"/> belongs to another database client.</exception>
    Task<JobRow> JobRetryAsync(long id, DbTransaction transaction, CancellationToken cancellationToken = default);

    /// <summary>Reads a queue (FR-ADM-04).</summary>
    /// <param name="name">The queue.</param>
    /// <param name="cancellationToken">Cancels the read.</param>
    /// <returns>The queue, or <c>null</c> if it does not exist.</returns>
    Task<QueueRow?> QueueGetAsync(string name, CancellationToken cancellationToken = default);

    /// <summary>Lists queues in name order, a page at a time (FR-ADM-04).</summary>
    /// <param name="parameters">The page size and where to start. <c>null</c> returns the first 100.</param>
    /// <param name="cancellationToken">Cancels the read.</param>
    /// <exception cref="ArgumentOutOfRangeException"><see cref="QueueListParams.Limit"/> is not from 1 to 10,000.</exception>
    Task<IReadOnlyList<QueueRow>> QueueListAsync(QueueListParams? parameters = null, CancellationToken cancellationToken = default);

    /// <summary>
    /// Pauses a queue, or every queue for <c>"*"</c> (FR-QUE-03): clients fetch none of its jobs until it is resumed, and
    /// jobs already running finish. Everywhere by default: the pause is stored, every client working the queue learns of
    /// it at once through a notification, or within its poll interval without notifications (FR-QUE-04), and River
    /// clients sharing the schema obey it too. With <see cref="QueuePauseOptions.LocalOnly"/>, only this client stops.
    /// Pausing a paused queue changes nothing.
    /// </summary>
    /// <param name="name">The queue, or <c>"*"</c> for all.</param>
    /// <param name="options">Where the pause applies. <c>null</c>: everywhere.</param>
    /// <param name="cancellationToken">Cancels the call.</param>
    /// <exception cref="KopiQueueNotFoundException">
    /// The queue does not exist (no client has worked it yet), or, locally, this client does not work it.
    /// </exception>
    Task QueuePauseAsync(string name, QueuePauseOptions? options = null, CancellationToken cancellationToken = default);

    /// <summary>
    /// Pauses a queue everywhere, or every queue for <c>"*"</c>, inside the caller's transaction (FR-ADM-05), as
    /// <see cref="QueuePauseAsync(string, QueuePauseOptions?, CancellationToken)"/> does: clients learn of the pause only
    /// if the transaction commits. The caller keeps ownership of the transaction.
    /// </summary>
    /// <param name="name">The queue, or <c>"*"</c> for all.</param>
    /// <param name="transaction">The caller's transaction, of the configured driver's database client.</param>
    /// <param name="cancellationToken">Cancels the call.</param>
    /// <exception cref="KopiQueueNotFoundException">The queue does not exist.</exception>
    /// <exception cref="ArgumentException"><paramref name="transaction"/> belongs to another database client.</exception>
    Task QueuePauseAsync(string name, DbTransaction transaction, CancellationToken cancellationToken = default);

    /// <summary>
    /// Resumes a queue, or every queue for <c>"*"</c>: the reverse of <see cref="QueuePauseAsync(string, QueuePauseOptions?, CancellationToken)"/>, which applies the
    /// same way. A local resume does not undo a pause everywhere, nor the other way round.
    /// </summary>
    /// <param name="name">The queue, or <c>"*"</c> for all.</param>
    /// <param name="options">Where the resume applies. <c>null</c>: everywhere.</param>
    /// <param name="cancellationToken">Cancels the call.</param>
    /// <exception cref="KopiQueueNotFoundException">
    /// The queue does not exist, or, locally, this client does not work it.
    /// </exception>
    Task QueueResumeAsync(string name, QueuePauseOptions? options = null, CancellationToken cancellationToken = default);

    /// <summary>
    /// Resumes a queue everywhere, or every queue for <c>"*"</c>, inside the caller's transaction (FR-ADM-05): the reverse
    /// of <see cref="QueuePauseAsync(string, DbTransaction, CancellationToken)"/>.
    /// </summary>
    /// <param name="name">The queue, or <c>"*"</c> for all.</param>
    /// <param name="transaction">The caller's transaction, of the configured driver's database client.</param>
    /// <param name="cancellationToken">Cancels the call.</param>
    /// <exception cref="KopiQueueNotFoundException">The queue does not exist.</exception>
    /// <exception cref="ArgumentException"><paramref name="transaction"/> belongs to another database client.</exception>
    Task QueueResumeAsync(string name, DbTransaction transaction, CancellationToken cancellationToken = default);

    /// <summary>Replaces a queue's metadata (FR-ADM-04).</summary>
    /// <param name="name">The queue.</param>
    /// <param name="metadata">The new metadata, which replaces the old.</param>
    /// <param name="cancellationToken">Cancels the update.</param>
    /// <returns>The updated queue.</returns>
    /// <exception cref="KopiQueueNotFoundException">The queue does not exist.</exception>
    Task<QueueRow> QueueUpdateAsync(string name, JsonObject metadata, CancellationToken cancellationToken = default);

    /// <summary>Replaces a queue's metadata inside the caller's transaction (FR-ADM-05). The caller keeps ownership of the transaction.</summary>
    /// <param name="name">The queue.</param>
    /// <param name="metadata">The new metadata, which replaces the old.</param>
    /// <param name="transaction">The caller's transaction, of the configured driver's database client.</param>
    /// <param name="cancellationToken">Cancels the update.</param>
    /// <returns>The updated queue, visible to others once the transaction commits.</returns>
    /// <exception cref="KopiQueueNotFoundException">The queue does not exist.</exception>
    /// <exception cref="ArgumentException"><paramref name="transaction"/> belongs to another database client.</exception>
    Task<QueueRow> QueueUpdateAsync(string name, JsonObject metadata, DbTransaction transaction, CancellationToken cancellationToken = default);
}
#pragma warning restore RS0026

/// <summary>One job of a bulk insert (<see cref="IKopiClient.InsertManyAsync"/>).</summary>
/// <example>
/// <code>
/// await client.InsertManyAsync([new(new SendEmailArgs("a@example.com", "welcome")), new(new ResizeImageArgs(42), new() { Queue = "images" })]);
/// </code>
/// </example>
public sealed class InsertRequest
{
    /// <summary>Creates a request.</summary>
    /// <param name="args">The job's arguments. The kind comes from its runtime type's <see cref="JobKindAttribute"/>.</param>
    /// <param name="options">Insert options. <c>null</c> uses the defaults (<see cref="InsertDefaults"/>).</param>
    public InsertRequest(IJobArgs args, InsertOptions? options = null)
    {
        ArgumentNullException.ThrowIfNull(args);
        Args = args;
        Options = options;
    }

    /// <summary>The job's arguments.</summary>
    public IJobArgs Args { get; }

    /// <summary>The job's insert options, or <c>null</c> for the defaults.</summary>
    public InsertOptions? Options { get; }
}

/// <summary>The outcome of inserting one job (FR-INS-06).</summary>
public class InsertResult
{
    internal InsertResult(JobRow job, bool uniqueSkippedAsDuplicate)
    {
        Job = job;
        UniqueSkippedAsDuplicate = uniqueSkippedAsDuplicate;
    }

    /// <summary>The job as stored. When <see cref="UniqueSkippedAsDuplicate"/> is <c>true</c>, the existing job.</summary>
    public JobRow Job { get; }

    /// <summary>Whether no job was inserted because a unique job with the same key exists (FR-UNQ-03).</summary>
    public bool UniqueSkippedAsDuplicate { get; }
}

/// <summary>The outcome of inserting one job, with its typed arguments.</summary>
/// <typeparam name="TArgs">The job's args type.</typeparam>
public sealed class InsertResult<TArgs> : InsertResult
    where TArgs : IJobArgs
{
    internal InsertResult(JobRow job, bool uniqueSkippedAsDuplicate, TArgs args)
        : base(job, uniqueSkippedAsDuplicate)
    {
        Args = args;
    }

    /// <summary>The arguments that were inserted.</summary>
    public TArgs Args { get; }
}

/// <summary>Options for <see cref="KopiClient"/>. Every option has a default.</summary>
public sealed class KopiClientOptions
{
    /// <summary>
    /// Serialises job args (design §9). Default: <see cref="SystemTextJsonArgsSerializer.Default"/>, which uses reflection.
    /// Trimmed and Native AOT apps set a <see cref="SystemTextJsonArgsSerializer"/> created from their
    /// <c>JsonSerializerContext</c> (NFR-COMP-06).
    /// </summary>
    public IJobArgsSerializer? ArgsSerializer { get; init; }

    /// <summary>
    /// The clock: whether <see cref="InsertOptions.ScheduledAt"/> is in the future, timeouts, retry times and polling.
    /// Default: the system clock.
    /// </summary>
    public TimeProvider? TimeProvider { get; init; }

    /// <summary>
    /// The queues this client works, by name, each with its concurrency (FR-QUE-01). Empty (the default) makes an
    /// insert-only client, which <see cref="KopiClient.StartAsync"/> leaves idle.
    /// </summary>
    public IReadOnlyDictionary<string, QueueOptions>? Queues { get; init; }

    /// <summary>The workers, one per job kind (FR-DEF-02). Required when <see cref="Queues"/> is set.</summary>
    public WorkerRegistry? Workers { get; init; }

    /// <summary>
    /// Resolves workers and their dependencies, in a new scope per job (FR-DEF-03). Default: none; workers are then
    /// created with their public constructor, or registered as instances.
    /// </summary>
    public IServiceProvider? Services { get; init; }

    /// <summary>Creates the client's loggers. Default: none.</summary>
    public ILoggerFactory? LoggerFactory { get; init; }

    /// <summary>
    /// This client's name, recorded in each job it fetches (<c>attempted_by</c>) and in the leader lease: at most 127
    /// characters. Default: the machine name, the process ID and a random suffix.
    /// </summary>
    public string? ClientId { get; init; }

    /// <summary>How long a job may run unless its worker says (FR-EXE-05). Default: 1 minute; <see cref="Timeout.InfiniteTimeSpan"/> for no limit.</summary>
    public TimeSpan JobTimeout { get; init; } = TimeSpan.FromMinutes(1);

    /// <summary>When a failed job runs again, unless its worker says (FR-ERR-02). Default: <see cref="DefaultRetryPolicy"/>.</summary>
    public IRetryPolicy? RetryPolicy { get; init; }

    /// <summary>
    /// How often each queue looks for jobs when nothing wakes it (FR-EXE-03). Default: 1 s. In poll-only mode this bounds
    /// how long a job inserted by another process waits.
    /// </summary>
    public TimeSpan FetchPollInterval { get; init; } = TimeSpan.FromSeconds(1);

    /// <summary>The least time between two fetches of a queue, which coalesces bursts of inserts. Default: 100 ms.</summary>
    public TimeSpan FetchCooldown { get; init; } = TimeSpan.FromMilliseconds(100);

    /// <summary>The most jobs one fetch takes. Default: 1,000.</summary>
    public int FetchBatchMax { get; init; } = 1_000;

    /// <summary>Whether fetching listens for insert notifications or only polls (FR-EXE-03, FR-DB-04). Default: <see cref="KopiQueue.NotificationMode.Auto"/>.</summary>
    public NotificationMode NotificationMode { get; init; } = NotificationMode.Auto;

    /// <summary>How finished jobs are written (design §5.3). Default: <see cref="KopiQueue.CompleterMode.Batch"/>.</summary>
    public CompleterMode CompleterMode { get; init; } = CompleterMode.Batch;

    /// <summary>
    /// How long <see cref="KopiClient.StopAsync"/> waits for running jobs before it cancels them (FR-HST-03). Default:
    /// <c>null</c>, which waits until the stop's token fires.
    /// </summary>
    public TimeSpan? SoftStopTimeout { get; init; }

    /// <summary>
    /// After a hard stop cancels the running jobs, how long to wait for them to finish, and then for their outcomes to be
    /// written. Default: 5 s.
    /// </summary>
    public TimeSpan HardStopGrace { get; init; } = TimeSpan.FromSeconds(5);

    /// <summary>
    /// How often a client that works jobs tries to become leader, and how often the leader renews its lease (FR-MNT-01,
    /// design §5.5). Default: 5 s.
    /// </summary>
    public TimeSpan ElectInterval { get; init; } = TimeSpan.FromSeconds(5);

    /// <summary>
    /// How long a leader's lease lasts after each renewal: at least twice <see cref="ElectInterval"/>. A leader that cannot
    /// renew steps down when a third of it is left; a crashed leader is replaced once it expires. Default: 15 s.
    /// </summary>
    public TimeSpan LeaderTtl { get; init; } = TimeSpan.FromSeconds(15);

    /// <summary>
    /// How long the leader keeps a queue's row after no client has worked the queue (FR-MNT-05). The row holds the
    /// queue's pause state and metadata; its jobs are never deleted with it. Clients mark the queues they work every
    /// minute, so it must be at least 5 minutes. Default: 24 hours; <c>null</c> keeps queue rows forever.
    /// </summary>
    public TimeSpan? QueueRetention { get; init; } = TimeSpan.FromHours(24);

    /// <summary>
    /// How often the leader's scheduler makes due scheduled and retryable jobs available (FR-SCH-01, FR-SCH-02, design
    /// §5.7), with ±10% jitter. A job is therefore eligible within about this long after its due time. A retry or snooze
    /// due within one interval is made available at once instead. Default: 5 s.
    /// </summary>
    public TimeSpan SchedulerInterval { get; init; } = TimeSpan.FromSeconds(5);

    /// <summary>
    /// How long a job may be <c>running</c> before the leader's rescuer takes it for stuck, for example because the client
    /// running it crashed (FR-MNT-03, NFR-REL-02, design §5.7), and retries or discards it. A job whose worker's timeout
    /// is longer gets at least that long; a job with no timeout (<see cref="Timeout.InfiniteTimeSpan"/>, from its worker or
    /// from <see cref="JobTimeout"/>) is never rescued by age. Delivery is at least once: a rescued attempt may still be
    /// running somewhere, so this is also an upper bound on how long a job may run. Must be greater than
    /// <see cref="JobTimeout"/>. Default: <c>null</c>, which is <see cref="JobTimeout"/> plus 1 hour (1 hour when
    /// <see cref="JobTimeout"/> is infinite).
    /// </summary>
    public TimeSpan? RescueStuckJobsAfter { get; init; }

    /// <summary>
    /// How long the leader keeps a completed job after it finished, before the job cleaner deletes it (FR-MNT-04, design
    /// §5.7). A completed unique job holds its unique slot until then (design §5.6). Default: 24 hours; <c>null</c> keeps
    /// completed jobs forever.
    /// </summary>
    public TimeSpan? CompletedJobRetention { get; init; } = TimeSpan.FromHours(24);

    /// <summary>
    /// How long the leader keeps a cancelled job after it was cancelled, before the job cleaner deletes it (FR-MNT-04).
    /// Default: 24 hours; <c>null</c> keeps cancelled jobs forever.
    /// </summary>
    public TimeSpan? CancelledJobRetention { get; init; } = TimeSpan.FromHours(24);

    /// <summary>
    /// How long the leader keeps a discarded job (one that ran out of attempts) after it was discarded, before the job
    /// cleaner deletes it (FR-MNT-04). Default: 7 days; <c>null</c> keeps discarded jobs forever.
    /// </summary>
    public TimeSpan? DiscardedJobRetention { get; init; } = TimeSpan.FromDays(7);

    /// <summary>
    /// How the args of unique jobs are written into their unique keys (DQ-8). Default:
    /// <see cref="UniqueArgsEncoding.GoCompatible"/>, which gives River's key for the same values however the args were
    /// serialised.
    /// </summary>
    public UniqueArgsEncoding UniqueArgsEncoding { get; init; } = UniqueArgsEncoding.GoCompatible;

    /// <summary>
    /// Jobs the leader inserts on a schedule (FR-PER-01). Give every client that can lead the same list. They can change
    /// at runtime through <see cref="KopiClient.PeriodicJobs"/>. An insert-only client never inserts them. Default: none.
    /// </summary>
    public IReadOnlyList<PeriodicJob>? PeriodicJobs { get; init; }
}

/// <summary>Settings of one queue (design §7).</summary>
public sealed class QueueOptions
{
    /// <summary>The most jobs of the queue this client runs at once (FR-EXE-04): 1 to 10,000.</summary>
    public required int MaxWorkers { get; init; }
}

/// <summary>How a client learns about new jobs (FR-EXE-03, FR-DB-04; design §3.3).</summary>
public enum NotificationMode
{
    /// <summary>Listen when the driver can (PostgreSQL), otherwise poll.</summary>
    Auto,

    /// <summary>Listen; starting fails if the driver cannot.</summary>
    ListenNotify,

    /// <summary>Only poll, for example behind PgBouncer in transaction mode.</summary>
    PollOnly,
}

/// <summary>How a client writes finished jobs (design §5.3).</summary>
public enum CompleterMode
{
    /// <summary>In batches of up to 2,000 every 50 ms: the fewest writes.</summary>
    Batch,

    /// <summary>Each on its own, up to 100 at once.</summary>
    Async,

    /// <summary>Each before the job's slot is freed; mainly for tests.</summary>
    Inline,
}

/// <summary>The KopiQueue client (design §6.3).</summary>
/// <example>
/// <code>
/// var client = new KopiClient(new NpgsqlDriver(dataSource));
/// await client.InsertAsync(new SendEmailArgs("a@example.com", "welcome"), new InsertOptions { Queue = "email" });
/// </code>
/// </example>
/// <remarks>
/// <para>
/// Thread-safe. Args are serialised by <see cref="KopiClientOptions.ArgsSerializer"/>: <c>System.Text.Json</c> with its web
/// defaults (camel-case names) unless configured, for example with a <c>JsonSerializerContext</c> for Native AOT.
/// </para>
/// <para>
/// Inserting needs no queues, workers or start step (FR-INS-08), so the client suits insert-only processes such as web
/// front-ends. It holds no connection of its own and runs no background work: each insert borrows a pooled connection
/// from the driver and returns it. One client can be shared by any number of concurrent callers, and creating one per
/// request is cheap; there is nothing to dispose.
/// </para>
/// <para>
/// To work jobs, configure <see cref="KopiClientOptions.Queues"/> and <see cref="KopiClientOptions.Workers"/>, call
/// <see cref="StartAsync"/>, and call <see cref="StopAsync"/> before the process exits (FR-HST-03). Hosting does both
/// with the application.
/// </para>
/// </remarks>
public sealed partial class KopiClient : IKopiClient
{
    private readonly IKopiDriver _driver;
    private readonly IJobArgsSerializer _serializer;
    private readonly TimeProvider _timeProvider;
    private readonly KopiClientOptions _options;
    private readonly ConcurrentDictionary<Type, IReadOnlySet<string>> _uniqueMembers = new();

    /// <summary>Creates a client on <paramref name="driver"/>.</summary>
    /// <param name="driver">The database driver, for example <c>new NpgsqlDriver(dataSource)</c>.</param>
    /// <param name="options">Serialiser, clock, and the queues and workers to work. <c>null</c> uses the defaults.</param>
    public KopiClient(IKopiDriver driver, KopiClientOptions? options = null)
    {
        ArgumentNullException.ThrowIfNull(driver);
        _driver = driver;
        _options = options ?? new KopiClientOptions();
        _serializer = _options.ArgsSerializer ?? SystemTextJsonArgsSerializer.Default;
        _timeProvider = _options.TimeProvider ?? TimeProvider.System;
        if (!Enum.IsDefined(_options.UniqueArgsEncoding))
        {
            throw new ArgumentOutOfRangeException(
                nameof(options), _options.UniqueArgsEncoding, $"{nameof(KopiClientOptions.UniqueArgsEncoding)} is not a defined value.");
        }

        ClientId = _options.ClientId ?? DefaultClientId();
        PeriodicJobs = new PeriodicJobBundle();
        if (_options.PeriodicJobs is { Count: > 0 } periodic)
        {
            PeriodicJobs.AddMany(periodic);
        }
    }

    /// <summary>
    /// This client's periodic jobs (FR-PER-03): those in <see cref="KopiClientOptions.PeriodicJobs"/>, and any added since.
    /// While this client leads, changes take effect at once; apply them on every client that can lead.
    /// </summary>
    public PeriodicJobBundle PeriodicJobs { get; }

    /// <summary>This client's name, recorded in the jobs it fetches.</summary>
    public string ClientId { get; }

    /// <summary>
    /// Wakes this client's producers after its own pooled inserts, whose commit it knows about (design §3, FR-DB-04).
    /// Inserts in a caller's transaction are not announced here: the client cannot see their commit.
    /// </summary>
    internal LocalWaker LocalWaker { get; } = new();

    /// <summary>The client's options, as given (for tests of how they are mapped).</summary>
    internal KopiClientOptions Options => _options;

#pragma warning disable RS0026 // See IKopiClient.
    /// <inheritdoc />
    public Task<InsertResult<TArgs>> InsertAsync<TArgs>(TArgs args, InsertOptions? options = null, CancellationToken cancellationToken = default)
        where TArgs : IJobArgs =>
        InsertAsync(_driver.GetExecutor(), pooled: true, args, options, cancellationToken);

    /// <inheritdoc />
    public Task<InsertResult<TArgs>> InsertAsync<TArgs>(TArgs args, DbTransaction transaction, InsertOptions? options = null, CancellationToken cancellationToken = default)
        where TArgs : IJobArgs
    {
        ArgumentNullException.ThrowIfNull(transaction);
        return InsertAsync(_driver.GetExecutor(transaction), pooled: false, args, options, cancellationToken);
    }
#pragma warning restore RS0026

    /// <inheritdoc />
    public async Task<IReadOnlyList<InsertResult>> InsertManyAsync(IEnumerable<InsertRequest> jobs, DbTransaction? transaction = null, CancellationToken cancellationToken = default)
    {
        var inserts = Prepare(jobs);
        if (inserts.Length == 0)
        {
            return [];
        }

        var results = await Executor(transaction).JobInsertManyAsync(inserts, cancellationToken).ConfigureAwait(false);
        if (results.Count != inserts.Length)
        {
            throw new InvalidOperationException($"The driver returned {results.Count} rows for {inserts.Length} inserts.");
        }

        WakeIfPooled(transaction, inserts);
        return [.. results.Select(r => new InsertResult(r.Job, r.UniqueSkippedAsDuplicate))];
    }

    /// <inheritdoc />
    public async Task<long> InsertManyFastAsync(IEnumerable<InsertRequest> jobs, DbTransaction? transaction = null, CancellationToken cancellationToken = default)
    {
        var inserts = Prepare(jobs);
        if (inserts.Length == 0)
        {
            return 0;
        }

        var unique = Array.FindIndex(inserts, i => i.UniqueKey is not null);
        if (unique >= 0)
        {
            throw new NotSupportedException(
                $"Job {unique} is unique, which the fast insert does not support: it cannot return or skip duplicates. Use InsertManyAsync.");
        }

        var count = await Executor(transaction).JobInsertFastAsync(inserts, cancellationToken).ConfigureAwait(false);
        WakeIfPooled(transaction, inserts);
        return count;
    }

#pragma warning disable RS0026 // See IKopiClient.
    /// <inheritdoc/>
    public async Task<JobRow> JobCancelAsync(long id, CancellationToken cancellationToken = default)
    {
        var job = await CancelAsync(_driver.GetExecutor(), id, cancellationToken).ConfigureAwait(false);
        QueueMaintainer?.CancelRunning(job); // here at once, without waiting for the notification or the poll
        return job;
    }

    /// <inheritdoc/>
    public Task<JobRow> JobCancelAsync(long id, DbTransaction transaction, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(transaction);
        return CancelAsync(_driver.GetExecutor(transaction), id, cancellationToken);
    }

    /// <inheritdoc/>
    public Task<JobRow?> JobGetAsync(long id, CancellationToken cancellationToken = default) =>
        _driver.GetExecutor().JobGetByIdAsync(id, cancellationToken);

    /// <inheritdoc/>
    public async Task<JobListResult> JobListAsync(JobListParams? parameters = null, CancellationToken cancellationToken = default)
    {
        parameters ??= new JobListParams();
        ArgumentOutOfRangeException.ThrowIfLessThan(parameters.Limit, 1, nameof(parameters));
        ArgumentOutOfRangeException.ThrowIfGreaterThan(parameters.Limit, 10_000, nameof(parameters));
        if (!Enum.IsDefined(parameters.OrderBy))
        {
            throw new ArgumentOutOfRangeException(nameof(parameters), parameters.OrderBy, "OrderBy is not a known order.");
        }

        parameters.Validate(nameof(parameters));
        var after = parameters.After is null
            ? null
            : JobListCursor.Decode(parameters.After, parameters.OrderBy, parameters.Descending, nameof(parameters));
        if (parameters.MatchesNothing)
        {
            return new JobListResult { Jobs = [] };
        }

        // One more than the page, to know whether a next page exists without a second query.
        var rows = await _driver.GetExecutor().JobListAsync(
            new JobListQuery { Filter = parameters, OrderBy = parameters.OrderBy, Descending = parameters.Descending, After = after, Max = parameters.Limit + 1 },
            cancellationToken).ConfigureAwait(false);
        if (rows.Count <= parameters.Limit)
        {
            return new JobListResult { Jobs = rows };
        }

        JobRow[] page = [.. rows.Take(parameters.Limit)];
        return new JobListResult { Jobs = page, NextCursor = JobListCursor.Encode(parameters.OrderBy, parameters.Descending, page[^1]) };
    }

    /// <inheritdoc/>
    public Task<JobRow?> JobDeleteAsync(long id, CancellationToken cancellationToken = default) =>
        DeleteAsync(_driver.GetExecutor(), id, cancellationToken);

    /// <inheritdoc/>
    public Task<JobRow?> JobDeleteAsync(long id, DbTransaction transaction, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(transaction);
        return DeleteAsync(_driver.GetExecutor(transaction), id, cancellationToken);
    }

    /// <inheritdoc/>
    public Task<IReadOnlyList<JobRow>> JobDeleteManyAsync(JobDeleteManyParams parameters, CancellationToken cancellationToken = default)
    {
        CheckDeleteMany(parameters);
        return DeleteManyAsync(_driver.GetExecutor(), parameters, cancellationToken);
    }

    /// <inheritdoc/>
    public Task<IReadOnlyList<JobRow>> JobDeleteManyAsync(JobDeleteManyParams parameters, DbTransaction transaction, CancellationToken cancellationToken = default)
    {
        CheckDeleteMany(parameters);
        ArgumentNullException.ThrowIfNull(transaction);
        return DeleteManyAsync(_driver.GetExecutor(transaction), parameters, cancellationToken);
    }

    /// <inheritdoc/>
    public async Task<JobRow> JobRetryAsync(long id, CancellationToken cancellationToken = default)
    {
        var job = await RetryAsync(_driver.GetExecutor(), id, cancellationToken).ConfigureAwait(false);
        if (job.State == JobState.Available)
        {
            LocalWaker.Wake([job.Queue]); // here at once, without waiting for the notification or the poll
        }

        return job;
    }

    /// <inheritdoc/>
    public Task<JobRow> JobRetryAsync(long id, DbTransaction transaction, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(transaction);
        return RetryAsync(_driver.GetExecutor(transaction), id, cancellationToken);
    }

    /// <inheritdoc/>
    public Task<QueueRow?> QueueGetAsync(string name, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(name);
        return _driver.GetExecutor().QueueGetAsync(name, cancellationToken);
    }

    /// <inheritdoc/>
    public Task<IReadOnlyList<QueueRow>> QueueListAsync(QueueListParams? parameters = null, CancellationToken cancellationToken = default)
    {
        parameters ??= new QueueListParams();
        ArgumentOutOfRangeException.ThrowIfLessThan(parameters.Limit, 1, nameof(parameters));
        ArgumentOutOfRangeException.ThrowIfGreaterThan(parameters.Limit, 10_000, nameof(parameters));
        return _driver.GetExecutor().QueueListAsync(new QueueListQuery { Max = parameters.Limit, After = parameters.After }, cancellationToken);
    }

    /// <inheritdoc/>
    public Task QueuePauseAsync(string name, QueuePauseOptions? options = null, CancellationToken cancellationToken = default) =>
        SetPausedAsync(name, paused: true, options, cancellationToken);

    /// <inheritdoc/>
    public Task QueueResumeAsync(string name, QueuePauseOptions? options = null, CancellationToken cancellationToken = default) =>
        SetPausedAsync(name, paused: false, options, cancellationToken);

    /// <inheritdoc/>
    public Task QueuePauseAsync(string name, DbTransaction transaction, CancellationToken cancellationToken = default) =>
        SetPausedInAsync(name, paused: true, transaction, cancellationToken);

    /// <inheritdoc/>
    public Task QueueResumeAsync(string name, DbTransaction transaction, CancellationToken cancellationToken = default) =>
        SetPausedInAsync(name, paused: false, transaction, cancellationToken);

    /// <inheritdoc/>
    public Task<QueueRow> QueueUpdateAsync(string name, JsonObject metadata, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(name);
        ArgumentNullException.ThrowIfNull(metadata);
        return UpdateQueueAsync(_driver.GetExecutor(), name, metadata, cancellationToken);
    }

    /// <inheritdoc/>
    public Task<QueueRow> QueueUpdateAsync(string name, JsonObject metadata, DbTransaction transaction, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(name);
        ArgumentNullException.ThrowIfNull(metadata);
        ArgumentNullException.ThrowIfNull(transaction);
        return UpdateQueueAsync(_driver.GetExecutor(transaction), name, metadata, cancellationToken);
    }

#pragma warning restore RS0026

    private static async Task<QueueRow> UpdateQueueAsync(IKopiExecutor executor, string name, JsonObject metadata, CancellationToken cancellationToken)
    {
        var row = await executor
            .QueueUpdateAsync(new QueueUpdateParams { Name = name, MetadataJson = metadata.ToJsonString() }, cancellationToken)
            .ConfigureAwait(false);
        return row ?? throw new KopiQueueNotFoundException($"Queue '{name}' does not exist.");
    }

    /// <summary>Pauses or resumes everywhere, in the caller's transaction: clients apply it through the notification or the poll, once it commits.</summary>
    private async Task SetPausedInAsync(string name, bool paused, DbTransaction transaction, CancellationToken cancellationToken)
    {
        ArgumentException.ThrowIfNullOrEmpty(name);
        ArgumentNullException.ThrowIfNull(transaction);
        var executor = _driver.GetExecutor(transaction);
        var found = paused
            ? await executor.QueuePauseAsync(name, cancellationToken).ConfigureAwait(false)
            : await executor.QueueResumeAsync(name, cancellationToken).ConfigureAwait(false);
        if (!found)
        {
            throw new KopiQueueNotFoundException($"Queue '{name}' does not exist.");
        }
    }

    private async Task SetPausedAsync(string name, bool paused, QueuePauseOptions? options, CancellationToken cancellationToken)
    {
        ArgumentException.ThrowIfNullOrEmpty(name);
        var queues = QueueMaintainer;
        if (options?.LocalOnly == true)
        {
            if (queues is null || !queues.Apply(name, paused, locally: true))
            {
                throw new KopiQueueNotFoundException($"This client does not work queue '{name}'.");
            }

            return;
        }

        var executor = _driver.GetExecutor();
        var found = paused
            ? await executor.QueuePauseAsync(name, cancellationToken).ConfigureAwait(false)
            : await executor.QueueResumeAsync(name, cancellationToken).ConfigureAwait(false);
        if (!found)
        {
            throw new KopiQueueNotFoundException($"Queue '{name}' does not exist.");
        }

        queues?.Apply(name, paused, locally: false); // here at once, without waiting for the notification or the poll
    }

    private static async Task<JobRow> CancelAsync(IKopiExecutor executor, long id, CancellationToken cancellationToken) =>
        await executor.JobCancelAsync(id, cancellationToken).ConfigureAwait(false)
        ?? throw new KopiQueueNotFoundException($"Job {id} does not exist.");

    private static async Task<JobRow> RetryAsync(IKopiExecutor executor, long id, CancellationToken cancellationToken) =>
        await executor.JobRetryAsync(id, cancellationToken).ConfigureAwait(false)
        ?? throw new KopiQueueNotFoundException($"Job {id} does not exist.");

    private static async Task<JobRow?> DeleteAsync(IKopiExecutor executor, long id, CancellationToken cancellationToken)
    {
        var result = await executor.JobDeleteAsync(id, cancellationToken).ConfigureAwait(false);
        return result switch
        {
            null => null,
            { Deleted: true } => result.Job,
            _ => throw new KopiQueueJobRunningException($"Job {id} is running, so it cannot be deleted. Cancel it, and delete it once it has stopped."),
        };
    }

    /// <summary>Checks a delete-many before any SQL runs.</summary>
    private static void CheckDeleteMany(JobDeleteManyParams parameters)
    {
        ArgumentNullException.ThrowIfNull(parameters);
        ArgumentOutOfRangeException.ThrowIfLessThan(parameters.Limit, 1, nameof(parameters));
        ArgumentOutOfRangeException.ThrowIfGreaterThan(parameters.Limit, 10_000, nameof(parameters));
        parameters.Validate(nameof(parameters));
        if (parameters.States?.Contains(JobState.Running) == true)
        {
            throw new ArgumentException("States includes Running, but running jobs cannot be deleted: cancel them, and delete them once they have stopped.", nameof(parameters));
        }
    }

    private static async Task<IReadOnlyList<JobRow>> DeleteManyAsync(IKopiExecutor executor, JobDeleteManyParams parameters, CancellationToken cancellationToken) =>
        parameters.MatchesNothing
            ? []
            : await executor.JobDeleteManyAsync(new JobDeleteManyQuery { Filter = parameters, Max = parameters.Limit }, cancellationToken).ConfigureAwait(false);

    /// <summary>How unique keys are built for args of <paramref name="argsType"/>; their marked members are looked up once.</summary>
    private JobValidation.UniqueContext Unique(Type argsType) =>
        new(() => _uniqueMembers.GetOrAdd(argsType, _serializer.GetUniqueMemberNames), _options.UniqueArgsEncoding);

    private void WakeIfPooled(DbTransaction? transaction, IEnumerable<JobInsertParams> inserts)
    {
        if (transaction is null)
        {
            LocalWaker.Wake(inserts.Where(i => i.State == JobState.Available).Select(i => i.Queue));
        }
    }

    private IKopiExecutor Executor(DbTransaction? transaction) =>
        transaction is null ? _driver.GetExecutor() : _driver.GetExecutor(transaction);

    /// <summary>Validates and resolves one request, as an insert does: for the periodic enqueuer.</summary>
    internal JobInsertParams Prepare(InsertRequest request, DateTimeOffset now)
    {
        ArgumentNullException.ThrowIfNull(request);
        var type = request.Args.GetType();
        return JobValidation.ToInsertParams(
            JobKindRegistry.GetKind(type), _serializer.Serialize(request.Args, type), InsertOptions.Resolve(request.Options, request.Args), now,
            unique: Unique(type));
    }

    /// <summary>Validates and resolves every request before any SQL runs, so an invalid job inserts nothing.</summary>
    private JobInsertParams[] Prepare(IEnumerable<InsertRequest> jobs)
    {
        ArgumentNullException.ThrowIfNull(jobs);
        var requests = jobs as IReadOnlyList<InsertRequest> ?? [.. jobs];
        var now = _timeProvider.GetUtcNow();
        var inserts = new JobInsertParams[requests.Count];
        for (var i = 0; i < inserts.Length; i++)
        {
            var request = requests[i] ?? throw new ArgumentException($"Job {i} is null.", nameof(jobs));
            var type = request.Args.GetType();
            inserts[i] = JobValidation.ToInsertParams(
                JobKindRegistry.GetKind(type), _serializer.Serialize(request.Args, type), InsertOptions.Resolve(request.Options, request.Args), now,
                $"Job {i}: ", nameof(jobs), Unique(type));
        }

        return inserts;
    }

    private async Task<InsertResult<TArgs>> InsertAsync<TArgs>(IKopiExecutor executor, bool pooled, TArgs args, InsertOptions? options, CancellationToken cancellationToken)
        where TArgs : IJobArgs
    {
        ArgumentNullException.ThrowIfNull(args);
        var kind = JobKindRegistry.GetKind<TArgs>();
        var insert = JobValidation.ToInsertParams(
            kind, _serializer.Serialize(args, typeof(TArgs)), InsertOptions.Resolve(options, args), _timeProvider.GetUtcNow(), unique: Unique(typeof(TArgs)));

        var results = await executor.JobInsertManyAsync([insert], cancellationToken).ConfigureAwait(false);
        var result = results.Count == 1
            ? results[0]
            : throw new InvalidOperationException($"The driver returned {results.Count} rows for one insert.");
        if (pooled && result.Job.State == JobState.Available && !result.UniqueSkippedAsDuplicate)
        {
            LocalWaker.Wake([result.Job.Queue]);
        }

        return new InsertResult<TArgs>(result.Job, result.UniqueSkippedAsDuplicate, args);
    }
}
