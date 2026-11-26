using System.Data.Common;
using System.Diagnostics.CodeAnalysis;
using System.Text.Json;
using System.Text.Json.Serialization.Metadata;
using KopiQueue.Drivers;

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

/// <summary>The KopiQueue client (design §6.3).</summary>
/// <example>
/// <code>
/// var client = new KopiClient(new NpgsqlDriver(dataSource));
/// await client.InsertAsync(new SendEmailArgs("a@example.com", "welcome"), new InsertOptions { Queue = "email" });
/// </code>
/// </example>
/// <remarks>
/// <para>
/// Thread-safe. Args are serialised with <c>System.Text.Json</c> and its web defaults (camel-case names); configurable
/// serialisation, including <c>JsonSerializerContext</c> for AOT, arrives with plan task 1.10.
/// </para>
/// <para>
/// Inserting needs no queues, workers or start step (FR-INS-08), so the client suits insert-only processes such as web
/// front-ends. It holds no connection of its own and runs no background work: each insert borrows a pooled connection
/// from the driver and returns it. One client can be shared by any number of concurrent callers, and creating one per
/// request is cheap; there is nothing to dispose.
/// </para>
/// </remarks>
public sealed class KopiClient : IKopiClient
{
    private readonly IKopiDriver _driver;
    private readonly TimeProvider _timeProvider;

    /// <summary>Creates a client on <paramref name="driver"/>.</summary>
    /// <param name="driver">The database driver, for example <c>new NpgsqlDriver(dataSource)</c>.</param>
    /// <param name="timeProvider">The clock used to tell whether <see cref="InsertOptions.ScheduledAt"/> is in the future. Default: the system clock.</param>
    public KopiClient(IKopiDriver driver, TimeProvider? timeProvider = null)
    {
        ArgumentNullException.ThrowIfNull(driver);
        _driver = driver;
        _timeProvider = timeProvider ?? TimeProvider.System;
    }

#pragma warning disable RS0026 // See IKopiClient.
    /// <inheritdoc />
    public Task<InsertResult<TArgs>> InsertAsync<TArgs>(TArgs args, InsertOptions? options = null, CancellationToken cancellationToken = default)
        where TArgs : IJobArgs =>
        InsertAsync(_driver.GetExecutor(), args, options, cancellationToken);

    /// <inheritdoc />
    public Task<InsertResult<TArgs>> InsertAsync<TArgs>(TArgs args, DbTransaction transaction, InsertOptions? options = null, CancellationToken cancellationToken = default)
        where TArgs : IJobArgs
    {
        ArgumentNullException.ThrowIfNull(transaction);
        return InsertAsync(_driver.GetExecutor(transaction), args, options, cancellationToken);
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

        return [.. results.Select(r => new InsertResult(r.Job, r.UniqueSkippedAsDuplicate))];
    }

    /// <inheritdoc />
    public async Task<long> InsertManyFastAsync(IEnumerable<InsertRequest> jobs, DbTransaction? transaction = null, CancellationToken cancellationToken = default)
    {
        var inserts = Prepare(jobs);
        return inserts.Length == 0
            ? 0
            : await Executor(transaction).JobInsertFastAsync(inserts, cancellationToken).ConfigureAwait(false);
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
                JobKindRegistry.GetKind(type), JobArgsSerializer.Serialize(request.Args, type), request.Options, now, $"Job {i}: ", nameof(jobs));
        }

        return inserts;
    }

    private async Task<InsertResult<TArgs>> InsertAsync<TArgs>(IKopiExecutor executor, TArgs args, InsertOptions? options, CancellationToken cancellationToken)
        where TArgs : IJobArgs
    {
        ArgumentNullException.ThrowIfNull(args);
        var kind = JobKindRegistry.GetKind<TArgs>();
        var insert = JobValidation.ToInsertParams(kind, JobArgsSerializer.Serialize(args), options, _timeProvider.GetUtcNow());

        var results = await executor.JobInsertManyAsync([insert], cancellationToken).ConfigureAwait(false);
        var result = results.Count == 1
            ? results[0]
            : throw new InvalidOperationException($"The driver returned {results.Count} rows for one insert.");
        return new InsertResult<TArgs>(result.Job, result.UniqueSkippedAsDuplicate, args);
    }
}

/// <summary>Serialises job args (design §9). Plan task 1.10 replaces this with configurable serialisation.</summary>
internal static class JobArgsSerializer
{
    /// <summary>Web defaults with reflection, or <c>null</c> where the app disabled reflection-based JSON (trimmed and AOT apps).</summary>
    private static readonly JsonSerializerOptions? ReflectionOptions = CreateReflectionOptions();

    /// <exception cref="NotSupportedException">Reflection-based JSON is disabled, as in an AOT app, so <typeparamref name="TArgs"/> cannot be serialised yet.</exception>
    public static string Serialize<TArgs>(TArgs args)
    {
        var options = ReflectionOptions ?? throw new NotSupportedException(
            $"Cannot serialise {typeof(TArgs).FullName}: reflection-based JSON is disabled (for example in a trimmed or AOT app). JsonSerializerContext support arrives with plan task 1.10.");
#pragma warning disable CA2263 // GetTypeInfo<T>() exists only from .NET 11; this also builds for .NET 10.
        return JsonSerializer.Serialize(args, (JsonTypeInfo<TArgs>)options.GetTypeInfo(typeof(TArgs)));
#pragma warning restore CA2263
    }

    /// <summary>Serialises <paramref name="args"/> as its runtime type <paramref name="type"/>, for bulk inserts.</summary>
    /// <exception cref="NotSupportedException">Reflection-based JSON is disabled, as in an AOT app.</exception>
    public static string Serialize(object args, Type type)
    {
        var options = ReflectionOptions ?? throw new NotSupportedException(
            $"Cannot serialise {type.FullName}: reflection-based JSON is disabled (for example in a trimmed or AOT app). JsonSerializerContext support arrives with plan task 1.10.");
        return JsonSerializer.Serialize(args, options.GetTypeInfo(type));
    }

    [UnconditionalSuppressMessage("Trimming", "IL2026:RequiresUnreferencedCode",
        Justification = "Used only when JsonSerializer.IsReflectionEnabledByDefault is true, which trimmed and AOT apps turn off.")]
    [UnconditionalSuppressMessage("AOT", "IL3050:RequiresDynamicCode",
        Justification = "Used only when JsonSerializer.IsReflectionEnabledByDefault is true, which AOT apps turn off.")]
    private static JsonSerializerOptions? CreateReflectionOptions() =>
        JsonSerializer.IsReflectionEnabledByDefault ? JsonSerializerOptions.Web : null;
}
