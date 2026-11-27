using System.Data.Common;
using System.Diagnostics.CodeAnalysis;
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
        ClientId = _options.ClientId ?? DefaultClientId();
    }

    /// <summary>This client's name, recorded in the jobs it fetches.</summary>
    public string ClientId { get; }

    /// <summary>
    /// Wakes this client's producers after its own pooled inserts, whose commit it knows about (design §3, FR-DB-04).
    /// Inserts in a caller's transaction are not announced here: the client cannot see their commit.
    /// </summary>
    internal LocalWaker LocalWaker { get; } = new();

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

        var count = await Executor(transaction).JobInsertFastAsync(inserts, cancellationToken).ConfigureAwait(false);
        WakeIfPooled(transaction, inserts);
        return count;
    }

    private void WakeIfPooled(DbTransaction? transaction, IEnumerable<JobInsertParams> inserts)
    {
        if (transaction is null)
        {
            LocalWaker.Wake(inserts.Where(i => i.State == JobState.Available).Select(i => i.Queue));
        }
    }

    private IKopiExecutor Executor(DbTransaction? transaction) =>
        transaction is null ? _driver.GetExecutor() : _driver.GetExecutor(transaction);

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
                JobKindRegistry.GetKind(type), _serializer.Serialize(request.Args, type), request.Options, now, $"Job {i}: ", nameof(jobs));
        }

        return inserts;
    }

    private async Task<InsertResult<TArgs>> InsertAsync<TArgs>(IKopiExecutor executor, bool pooled, TArgs args, InsertOptions? options, CancellationToken cancellationToken)
        where TArgs : IJobArgs
    {
        ArgumentNullException.ThrowIfNull(args);
        var kind = JobKindRegistry.GetKind<TArgs>();
        var insert = JobValidation.ToInsertParams(kind, _serializer.Serialize(args, typeof(TArgs)), options, _timeProvider.GetUtcNow());

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
