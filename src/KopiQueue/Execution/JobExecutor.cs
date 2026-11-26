using KopiQueue.Drivers;
using KopiQueue.Serialization;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace KopiQueue.Execution;

/// <summary>Settings of the <see cref="JobExecutor"/> (design §7).</summary>
internal sealed class JobExecutorOptions
{
    /// <summary>
    /// How long a job may run when its worker does not say (FR-EXE-05). Default: 1 minute.
    /// <see cref="Timeout.InfiniteTimeSpan"/> means no limit.
    /// </summary>
    public TimeSpan JobTimeout { get; init; } = TimeSpan.FromMinutes(1);

    /// <summary>Picks retry jitter: returns a number in [0, 1). Default: <see cref="Random.Shared"/>.</summary>
    public Func<double>? Random { get; init; }
}

/// <summary>
/// Runs one fetched job and hands its outcome to the completer (design §5.2; FR-DEF-03, FR-EXE-05, FR-EXE-07,
/// FR-EXE-08). <see cref="ExecuteAsync"/> is the producer's run function.
/// </summary>
/// <remarks>
/// <para>
/// Each job gets a new DI scope, in which its worker is resolved (or created) after its args are deserialised. The
/// worker runs with a token that its timeout and the client's hard stop cancel. Returning means the job completed;
/// any exception, including a cancellation, is an error: recorded in the job's <c>errors</c> with its message and
/// stack trace, and retried after the worker's <c>NextRetry</c> or the default backoff until the job's attempts run
/// out, when it is discarded. Nothing a worker does escapes to the host.
/// </para>
/// <para>
/// A job whose kind has no worker on this client is an error like any other (DQ-3, as River): it is retried with
/// backoff, so during a rolling deploy a client that has the worker picks it up, and a kind no client knows is
/// discarded once its attempts run out.
/// </para>
/// </remarks>
internal sealed partial class JobExecutor
{
    private readonly WorkerRegistry _workers;
    private readonly IJobArgsSerializer _serializer;
    private readonly IJobCompleter _completer;
    private readonly IServiceProvider _services;
    private readonly IServiceScopeFactory? _scopes;
    private readonly JobExecutorOptions _options;
    private readonly TimeProvider _time;
    private readonly Func<double> _random;
    private readonly ILogger _logger;

    public JobExecutor(
        WorkerRegistry workers,
        IJobArgsSerializer serializer,
        IJobCompleter completer,
        IServiceProvider? services = null,
        JobExecutorOptions? options = null,
        TimeProvider? timeProvider = null,
        ILogger<JobExecutor>? logger = null)
    {
        ArgumentNullException.ThrowIfNull(workers);
        ArgumentNullException.ThrowIfNull(serializer);
        ArgumentNullException.ThrowIfNull(completer);
        _options = options ?? new JobExecutorOptions();
        if (_options.JobTimeout <= TimeSpan.Zero && _options.JobTimeout != Timeout.InfiniteTimeSpan)
        {
            throw new ArgumentOutOfRangeException(nameof(options), _options.JobTimeout, "The job timeout must be positive, or infinite.");
        }

        _workers = workers;
        _serializer = serializer;
        _completer = completer;
        _services = services ?? EmptyServiceProvider.Instance;
        _scopes = _services.GetService<IServiceScopeFactory>();
        _time = timeProvider ?? TimeProvider.System;
        _random = _options.Random ?? System.Random.Shared.NextDouble;
        _logger = (ILogger?)logger ?? NullLogger.Instance;
    }

    /// <summary>
    /// Runs <paramref name="job"/> and returns once the completer has accepted its outcome. Never throws for anything
    /// the job does; only the completer's own failures escape.
    /// </summary>
    /// <param name="job">A job the producer fetched: running, with this attempt's number.</param>
    /// <param name="hardStop">Cancelled when the client stops hard; the worker's token is cancelled with it.</param>
    public async Task ExecuteAsync(JobRow job, CancellationToken hardStop)
    {
        ArgumentNullException.ThrowIfNull(job);
        var started = _time.GetTimestamp();
        var (error, retryDelay) = await RunAsync(job, hardStop).ConfigureAwait(false);
        var completion = Classify(job, error, retryDelay, _time.GetElapsedTime(started));
        await _completer.CompleteAsync(completion, CancellationToken.None).ConfigureAwait(false);
    }

    /// <summary>Runs the worker; returns the attempt's error (<c>null</c> if it completed) and the worker's retry delay.</summary>
    private async Task<(AttemptError? Error, TimeSpan? RetryDelay)> RunAsync(JobRow job, CancellationToken hardStop)
    {
        if (!_workers.TryGet(job.Kind, out var registration))
        {
            LogUnknownKind(job.Id, job.Kind);
            return (Error(job, $"No worker is registered for kind '{job.Kind}' on this client.", trace: null), null);
        }

        var scope = _scopes?.CreateAsyncScope();
        try
        {
            IWorkUnit unit;
            try
            {
                unit = registration.CreateWorkUnit(job, _serializer, scope?.ServiceProvider ?? _services);
            }
#pragma warning disable CA1031 // Bad args or a worker that cannot be built fail this attempt, not the host (FR-EXE-08).
            catch (Exception e)
#pragma warning restore CA1031
            {
                return (Error(job, e), null);
            }

            await using (unit.ConfigureAwait(false))
            {
                return (await WorkAsync(job, unit, hardStop).ConfigureAwait(false), RetryDelay(job, unit));
            }
        }
        finally
        {
            if (scope is { } s)
            {
                await s.DisposeAsync().ConfigureAwait(false);
            }
        }
    }

    private async Task<AttemptError?> WorkAsync(JobRow job, IWorkUnit unit, CancellationToken hardStop)
    {
        TimeSpan timeout;
        try
        {
            timeout = unit.Timeout() ?? _options.JobTimeout;
        }
#pragma warning disable CA1031 // A worker's Timeout override that throws fails the attempt (FR-EXE-08).
        catch (Exception e)
#pragma warning restore CA1031
        {
            return Error(job, e);
        }

        // A zero timeout means the client's; any negative one means no limit.
        if (timeout == TimeSpan.Zero)
        {
            timeout = _options.JobTimeout;
        }

        using var timer = timeout < TimeSpan.Zero ? new CancellationTokenSource() : new CancellationTokenSource(timeout, _time);
        using var linked = CancellationTokenSource.CreateLinkedTokenSource(timer.Token, hardStop);
        try
        {
            await unit.WorkAsync(linked.Token).ConfigureAwait(false);
            return null;
        }
        catch (OperationCanceledException e) when (timer.IsCancellationRequested && !hardStop.IsCancellationRequested)
        {
            LogTimedOut(job.Id, job.Kind, timeout);
            return Error(job, $"The job did not finish within its timeout of {timeout}.", e.ToString());
        }
        catch (OperationCanceledException e) when (hardStop.IsCancellationRequested)
        {
            return Error(job, "The client stopped before the job finished.", e.ToString());
        }
#pragma warning disable CA1031 // Whatever the worker throws fails the attempt, never the host (FR-EXE-08).
        catch (Exception e)
#pragma warning restore CA1031
        {
            return Error(job, e);
        }
    }

    /// <summary>The worker's retry delay; a <c>NextRetry</c> override that throws or returns a negative delay is ignored.</summary>
    private TimeSpan? RetryDelay(JobRow job, IWorkUnit unit)
    {
        try
        {
            var delay = unit.NextRetry();
            return delay is { } d && d < TimeSpan.Zero ? null : delay;
        }
#pragma warning disable CA1031 // Fall back to the default backoff rather than lose the outcome.
        catch (Exception e)
#pragma warning restore CA1031
        {
            LogNextRetryFailed(e, job.Id, job.Kind);
            return null;
        }
    }

    /// <summary>The outcome table of design §5.2 for completed and failed attempts.</summary>
    private JobCompletion Classify(JobRow job, AttemptError? error, TimeSpan? retryDelay, TimeSpan duration)
    {
        var now = _time.GetUtcNow();
        if (error is null)
        {
            return new JobCompletion(job, JobState.Completed, now, null, null, duration);
        }

        if (job.Attempt >= job.MaxAttempts)
        {
            LogDiscarded(job.Id, job.Kind, job.Attempt, error.Error);
            return new JobCompletion(job, JobState.Discarded, now, null, error, duration);
        }

        var delay = retryDelay ?? RetryBackoff.Delay(job.Attempt, _random());
        LogRetrying(job.Id, job.Kind, job.Attempt, delay, error.Error);
        return new JobCompletion(job, JobState.Retryable, null, now + delay, error, duration);
    }

    private AttemptError Error(JobRow job, Exception exception) => Error(job, exception.Message, exception.ToString());

    private AttemptError Error(JobRow job, string message, string? trace) => new(_time.GetUtcNow(), job.Attempt, message, trace);

    [LoggerMessage(EventId = 2100, Level = LogLevel.Warning, Message = "Job {JobId} has kind {Kind}, which has no worker on this client; it will be retried")]
    private partial void LogUnknownKind(long jobId, string kind);

    [LoggerMessage(EventId = 2101, Level = LogLevel.Warning, Message = "Job {JobId} ({Kind}) timed out after {Timeout}")]
    private partial void LogTimedOut(long jobId, string kind, TimeSpan timeout);

    [LoggerMessage(EventId = 2102, Level = LogLevel.Warning, Message = "Job {JobId} ({Kind}) attempt {Attempt} failed; retrying in {Delay}: {Error}")]
    private partial void LogRetrying(long jobId, string kind, int attempt, TimeSpan delay, string error);

    [LoggerMessage(EventId = 2103, Level = LogLevel.Error, Message = "Job {JobId} ({Kind}) attempt {Attempt} failed and no attempts are left; discarded: {Error}")]
    private partial void LogDiscarded(long jobId, string kind, int attempt, string error);

    [LoggerMessage(EventId = 2104, Level = LogLevel.Warning, Message = "The NextRetry override of job {JobId} ({Kind}) threw; using the default backoff")]
    private partial void LogNextRetryFailed(Exception exception, long jobId, string kind);

    /// <summary>The services of a client created without DI: workers with public constructors still work.</summary>
    private sealed class EmptyServiceProvider : IServiceProvider
    {
        public static readonly EmptyServiceProvider Instance = new();

        public object? GetService(Type serviceType) => null;
    }
}
