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
}
#pragma warning restore RS0026

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
/// Thread-safe. Args are serialised with <c>System.Text.Json</c> and its web defaults (camel-case names); configurable
/// serialisation, including <c>JsonSerializerContext</c> for AOT, arrives with plan task 1.10.
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

    [UnconditionalSuppressMessage("Trimming", "IL2026:RequiresUnreferencedCode",
        Justification = "Used only when JsonSerializer.IsReflectionEnabledByDefault is true, which trimmed and AOT apps turn off.")]
    [UnconditionalSuppressMessage("AOT", "IL3050:RequiresDynamicCode",
        Justification = "Used only when JsonSerializer.IsReflectionEnabledByDefault is true, which AOT apps turn off.")]
    private static JsonSerializerOptions? CreateReflectionOptions() =>
        JsonSerializer.IsReflectionEnabledByDefault ? JsonSerializerOptions.Web : null;
}
