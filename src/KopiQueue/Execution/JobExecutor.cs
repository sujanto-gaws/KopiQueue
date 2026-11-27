using System.Globalization;
using System.Text.Json;
using System.Text.Json.Nodes;
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

    /// <summary>
    /// How often the leader's scheduler makes due jobs available (design §5.7). A job due again within this interval is
    /// made <see cref="JobState.Available"/> at once instead of waiting for the scheduler, as River does. Default: 5 s.
    /// </summary>
    public TimeSpan SchedulerInterval { get; init; } = TimeSpan.FromSeconds(5);

    /// <summary>When a failed job runs again, unless its worker says (FR-ERR-02). Default: <see cref="DefaultRetryPolicy"/>.</summary>
    public IRetryPolicy? RetryPolicy { get; init; }
}

/// <summary>
/// Runs one fetched job and hands its outcome to the completer (design §5.2; FR-DEF-03, FR-EXE-05, FR-EXE-07,
/// FR-EXE-08). <see cref="ExecuteAsync"/> is the producer's run function.
/// </summary>
/// <remarks>
/// <para>
/// Each job gets a new DI scope, in which its worker is resolved (or created) after its args are deserialised. The
/// worker runs with a token that its timeout and the client's hard stop cancel. Returning means the job completed.
/// <see cref="JobControl.Cancel"/> cancels it for good and <see cref="JobControl.Snooze"/> reschedules it without using
/// an attempt. A cancellation caused by the client stopping interrupts the attempt: the job is made available again and
/// the attempt does not count. Any other exception, a timeout included, is an error: recorded in the job's
/// <c>errors</c> with its message and stack trace, and retried after the worker's <c>NextRetry</c> or the retry policy
/// until the job's attempts run out, when it is discarded. Nothing a worker does escapes to the host. These rules are River's
/// (v0.51.2 <c>jobexecutor.reportError</c>), so jobs move the same way whichever library works them.
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
    private readonly IRetryPolicy _retryPolicy;
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
        _retryPolicy = _options.RetryPolicy ?? DefaultRetryPolicy.Instance;
        _logger = (ILogger?)logger ?? NullLogger.Instance;
    }

    private enum OutcomeKind
    {
        Completed,
        Interrupted,
        Failed,
        Cancelled,
        Snoozed,
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
        var startedAt = _time.GetUtcNow();
        var started = _time.GetTimestamp();
        var outcome = await RunAsync(job, startedAt, hardStop).ConfigureAwait(false);
        var completion = Classify(job, outcome, _time.GetElapsedTime(started));
        await _completer.CompleteAsync(completion, CancellationToken.None).ConfigureAwait(false);
    }

    private static AttemptError Error(JobRow job, DateTimeOffset startedAt, Exception exception) =>
        new(startedAt, job.Attempt, exception.Message, exception.ToString());

    /// <summary>Runs the worker and says how the attempt ended.</summary>
    private async Task<Outcome> RunAsync(JobRow job, DateTimeOffset startedAt, CancellationToken hardStop)
    {
        if (!_workers.TryGet(job.Kind, out var registration))
        {
            LogUnknownKind(job.Id, job.Kind);
            return Outcome.Failed(new AttemptError(startedAt, job.Attempt, $"No worker is registered for kind '{job.Kind}' on this client.", null));
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
                return Outcome.Failed(Error(job, startedAt, e));
            }

            await using (unit.ConfigureAwait(false))
            {
                var outcome = await WorkAsync(job, unit, startedAt, hardStop).ConfigureAwait(false);
                return outcome.Kind == OutcomeKind.Failed ? outcome with { RetryDelay = RetryDelay(job, unit) } : outcome;
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

    private async Task<Outcome> WorkAsync(JobRow job, IWorkUnit unit, DateTimeOffset startedAt, CancellationToken hardStop)
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
            return Outcome.Failed(Error(job, startedAt, e));
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
            return Outcome.Completed;
        }
        catch (Exception e) when (Find<JobCancelException>(e) is { } cancel)
        {
            LogCancelled(job.Id, job.Kind, cancel.Reason);
            return new Outcome(OutcomeKind.Cancelled, new AttemptError(startedAt, job.Attempt, cancel.Reason, null));
        }
        catch (Exception e) when (Find<JobSnoozeException>(e) is { } snooze)
        {
            return new Outcome(OutcomeKind.Snoozed, RetryDelay: snooze.Delay);
        }
        catch (OperationCanceledException) when (hardStop.IsCancellationRequested)
        {
            LogInterrupted(job.Id, job.Kind);
            return Outcome.Interrupted;
        }
        catch (OperationCanceledException e) when (timer.IsCancellationRequested)
        {
            LogTimedOut(job.Id, job.Kind, timeout);
            return Outcome.Failed(new AttemptError(startedAt, job.Attempt, $"The job did not finish within its timeout of {timeout}.", e.ToString()));
        }
#pragma warning disable CA1031 // Whatever the worker throws fails the attempt, never the host (FR-EXE-08).
        catch (Exception e)
#pragma warning restore CA1031
        {
            return Outcome.Failed(Error(job, startedAt, e));
        }
    }

    /// <summary>The worker's retry delay, or <c>null</c> for the default; a <c>NextRetry</c> override that throws is ignored.</summary>
    private TimeSpan? RetryDelay(JobRow job, IWorkUnit unit)
    {
        try
        {
            return unit.NextRetry();
        }
#pragma warning disable CA1031 // Fall back to the default backoff rather than lose the outcome.
        catch (Exception e)
#pragma warning restore CA1031
        {
            LogNextRetryFailed(e, job.Id, job.Kind);
            return null;
        }
    }

    /// <summary>The outcome table of design §5.2.</summary>
    private JobCompletion Classify(JobRow job, Outcome outcome, TimeSpan duration)
    {
        var now = _time.GetUtcNow();
        if (outcome.Kind == OutcomeKind.Completed)
        {
            return new JobCompletion(job, JobState.Completed, now, null, null, duration);
        }

        if (outcome.Kind == OutcomeKind.Interrupted)
        {
            // The client stopped: run it again soon, as if this attempt had not happened (River: JobSetStateInterrupted).
            return new JobCompletion(job, JobState.Available, null, now, null, duration, Attempt: Math.Max(job.Attempt - 1, 0));
        }

        if (outcome.Kind == OutcomeKind.Snoozed)
        {
            // River: JobSetStateSnoozed(Available): the attempt does not count, and metadata.snoozes counts the snoozes.
            var delay = outcome.RetryDelay!.Value;
            var snoozes = NextSnoozeCount(job.MetadataJson);
            LogSnoozed(job.Id, job.Kind, delay, snoozes);
            return new JobCompletion(
                job,
                delay <= _options.SchedulerInterval ? JobState.Available : JobState.Scheduled,
                null,
                now + delay,
                null,
                duration,
                Attempt: Math.Max(job.Attempt - 1, 0),
                MetadataUpdates: new JsonObject { ["snoozes"] = snoozes }.ToJsonString());
        }

        var error = outcome.Error!;
        if (outcome.Kind == OutcomeKind.Cancelled)
        {
            return new JobCompletion(job, JobState.Cancelled, now, null, error, duration);
        }

        if (job.Attempt >= job.MaxAttempts)
        {
            LogDiscarded(job.Id, job.Kind, job.Attempt, error.Error);
            return new JobCompletion(job, JobState.Discarded, now, null, error, duration);
        }

        var retryAt = NextRetry(job, outcome.RetryDelay, now);
        LogRetrying(job.Id, job.Kind, job.Attempt, retryAt - now, error.Error);
        var state = retryAt - now <= _options.SchedulerInterval ? JobState.Available : JobState.Retryable;
        return new JobCompletion(job, state, null, retryAt, error, duration);
    }

    /// <summary>
    /// The worker's delay, else the retry policy's time; a time in the past (a negative delay, or a policy that got it
    /// wrong) falls back to <see cref="DefaultRetryPolicy"/>, as in River.
    /// </summary>
    private DateTimeOffset NextRetry(JobRow job, TimeSpan? workerDelay, DateTimeOffset now) =>
        RetrySchedule.Next(
            job,
            workerDelay,
            _retryPolicy,
            now,
            e => LogRetryPolicyFailed(e, job.Id, job.Kind),
            () => LogRetryInThePast(job.Id, job.Kind));

    /// <summary>The job's <c>snoozes</c> metadata plus one, read leniently as River does; 1 when absent.</summary>
    internal static long NextSnoozeCount(string metadataJson)
    {
        try
        {
            using var document = JsonDocument.Parse(metadataJson);
            if (document.RootElement.ValueKind != JsonValueKind.Object
                || !document.RootElement.TryGetProperty("snoozes", out var snoozes))
            {
                return 1;
            }

            return snoozes.ValueKind switch
            {
                JsonValueKind.Number when snoozes.TryGetInt64(out var n) => n + 1,
                JsonValueKind.Number => (long)snoozes.GetDouble() + 1,
                JsonValueKind.String when long.TryParse(snoozes.GetString(), NumberStyles.Integer, CultureInfo.InvariantCulture, out var n) => n + 1,
                _ => 1,
            };
        }
        catch (JsonException)
        {
            return 1;
        }
    }

    /// <summary>Finds a <typeparamref name="T"/> in <paramref name="exception"/> or the exceptions it wraps.</summary>
    private static T? Find<T>(Exception exception)
        where T : Exception
    {
        for (var e = exception; e is not null; e = e.InnerException)
        {
            if (e is T found)
            {
                return found;
            }

            if (e is AggregateException { InnerExceptions.Count: > 1 } aggregate)
            {
                return aggregate.InnerExceptions.Select(Find<T>).FirstOrDefault(f => f is not null);
            }
        }

        return null;
    }

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

    [LoggerMessage(EventId = 2105, Level = LogLevel.Information, Message = "Job {JobId} ({Kind}) was interrupted by the client stopping; it will run again")]
    private partial void LogInterrupted(long jobId, string kind);

    [LoggerMessage(EventId = 2106, Level = LogLevel.Information, Message = "Job {JobId} ({Kind}) was cancelled by its worker: {Reason}")]
    private partial void LogCancelled(long jobId, string kind, string reason);

    [LoggerMessage(EventId = 2107, Level = LogLevel.Debug, Message = "Job {JobId} ({Kind}) was snoozed for {Delay} (snooze {Snoozes})")]
    private partial void LogSnoozed(long jobId, string kind, TimeSpan delay, long snoozes);

    [LoggerMessage(EventId = 2108, Level = LogLevel.Warning, Message = "The retry policy failed for job {JobId} ({Kind}); using the default policy")]
    private partial void LogRetryPolicyFailed(Exception exception, long jobId, string kind);

    [LoggerMessage(EventId = 2109, Level = LogLevel.Warning, Message = "The next retry of job {JobId} ({Kind}) was in the past; using the default policy")]
    private partial void LogRetryInThePast(long jobId, string kind);

    /// <summary>How an attempt ended, before classification.</summary>
    private sealed record Outcome(OutcomeKind Kind, AttemptError? Error = null, TimeSpan? RetryDelay = null)
    {
        public static readonly Outcome Completed = new(OutcomeKind.Completed);

        public static readonly Outcome Interrupted = new(OutcomeKind.Interrupted);

        public static Outcome Failed(AttemptError error) => new(OutcomeKind.Failed, error);
    }

    /// <summary>The services of a client created without DI: workers with public constructors still work.</summary>
    internal sealed class EmptyServiceProvider : IServiceProvider
    {
        public static readonly EmptyServiceProvider Instance = new();

        public object? GetService(Type serviceType) => null;
    }
}
