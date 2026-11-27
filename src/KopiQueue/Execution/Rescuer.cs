using System.Globalization;
using KopiQueue.Drivers;
using KopiQueue.Serialization;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace KopiQueue.Execution;

/// <summary>Settings of the <see cref="Rescuer"/> (design §5.7, §7).</summary>
internal sealed class RescuerOptions
{
    /// <summary>How long a job may run before it is taken for stuck, unless its worker's timeout is longer. Positive.</summary>
    public required TimeSpan RescueStuckJobsAfter { get; init; }

    /// <summary>The client's job timeout, for jobs whose worker does not say. <see cref="Timeout.InfiniteTimeSpan"/> for none.</summary>
    public TimeSpan JobTimeout { get; init; } = TimeSpan.FromMinutes(1);

    /// <summary>A retry due within this interval makes the job available at once, as the executor does (§5.2).</summary>
    public TimeSpan SchedulerInterval { get; init; } = TimeSpan.FromSeconds(5);

    /// <summary>The client's retry policy. Default: <see cref="DefaultRetryPolicy"/>.</summary>
    public IRetryPolicy? RetryPolicy { get; init; }

    /// <summary>The client's workers, or <c>null</c> for none: every job then gets the client's defaults.</summary>
    public WorkerRegistry? Workers { get; init; }

    /// <summary>Deserialises args to resolve a job's worker.</summary>
    public IJobArgsSerializer Serializer { get; init; } = SystemTextJsonArgsSerializer.Default;

    /// <summary>The DI root workers are resolved from, in a scope per job, or <c>null</c> for none.</summary>
    public IServiceProvider? Services { get; init; }

    /// <summary>How long to wait after a batch that was not full. Default: 1 minute.</summary>
    public TimeSpan Interval { get; init; } = TimeSpan.FromMinutes(1);

    /// <summary>The most jobs one batch looks at. Default: 1,000.</summary>
    public int BatchSize { get; init; } = 1_000;
}

/// <summary>
/// Rescues stuck jobs (plan task 3.4, FR-MNT-03, NFR-REL-02, UC-9, design §5.7): jobs left <c>running</c> because the
/// client running them crashed or lost its connection. Runs on the leader every minute, in batches of up to 1,000.
/// </summary>
/// <remarks>
/// <para>
/// A job is stuck when its attempt started, by the database's clock, more than the longer of
/// <see cref="RescuerOptions.RescueStuckJobsAfter"/> and its effective timeout ago. The effective timeout is resolved as
/// the executor does: the worker's <c>Timeout(job)</c>, else the client's job timeout; a job whose kind has no worker
/// here, or whose args or worker cannot be built, gets the client's. A job with no timeout is never rescued by age.
/// </para>
/// <para>
/// A stuck job with attempts left is retried by the executor's rules (the worker's <c>NextRetry</c>, else the retry
/// policy) and becomes <see cref="JobState.Retryable"/>, or <see cref="JobState.Available"/> if due within the scheduler
/// interval; one without is discarded. Either way the stuck attempt counts, and one error saying it was rescued is
/// appended. A job whose kind is unknown here is rescued, not discarded, so a client that has the worker can take it
/// (rolling deploys, task 2.9). The rescue applies only to a job still running the attempt the rescuer read.
/// </para>
/// </remarks>
internal sealed partial class Rescuer : IMaintenanceService
{
    private readonly IKopiDriver _driver;
    private readonly RescuerOptions _options;
    private readonly IRetryPolicy _retryPolicy;
    private readonly IServiceProvider _services;
    private readonly IServiceScopeFactory? _scopes;
    private readonly ILogger _logger;

    public Rescuer(IKopiDriver driver, RescuerOptions options, ILogger<Rescuer>? logger = null)
    {
        ArgumentNullException.ThrowIfNull(driver);
        ArgumentNullException.ThrowIfNull(options);
        ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(options.RescueStuckJobsAfter, TimeSpan.Zero, nameof(options));
        _driver = driver;
        _options = options;
        _retryPolicy = options.RetryPolicy ?? DefaultRetryPolicy.Instance;
        _services = options.Services ?? JobExecutor.EmptyServiceProvider.Instance;
        _scopes = _services.GetService<IServiceScopeFactory>();
        _logger = (ILogger?)logger ?? NullLogger.Instance;
    }

    public string Name => "rescuer";

    public TimeSpan Interval => _options.Interval;

    public int BatchSize => _options.BatchSize;

    public async Task<int> RunBatchAsync(CancellationToken cancellationToken)
    {
        var executor = _driver.GetExecutor();
        var stuck = await executor
            .JobListStuckAsync(new JobListStuckParams { StuckAfter = _options.RescueStuckJobsAfter, Max = BatchSize }, cancellationToken)
            .ConfigureAwait(false);
        var now = stuck.Now;
        var rescues = new List<JobRescueParams>();
        foreach (var job in stuck.Jobs)
        {
            if (job.AttemptedAt is not { } attemptedAt)
            {
                continue;
            }

            if (await RescueOfAsync(job, attemptedAt, now).ConfigureAwait(false) is { } rescue)
            {
                rescues.Add(rescue);
            }
        }

        if (rescues.Count == 0)
        {
            return 0;
        }

        var rescued = await executor.JobRescueManyAsync(rescues, cancellationToken).ConfigureAwait(false);
        foreach (var job in rescued)
        {
            if (job.State == JobState.Discarded)
            {
                LogDiscarded(job.Id, job.Kind, job.Attempt);
            }
            else
            {
                LogRescued(job.Id, job.Kind, job.Attempt, job.State, job.ScheduledAt);
            }
        }

        return rescued.Count;
    }

    /// <summary>What to do with a candidate, or <c>null</c> if it is not stuck yet (or never will be by age).</summary>
    private async Task<JobRescueParams?> RescueOfAsync(JobRow job, DateTimeOffset attemptedAt, DateTimeOffset now)
    {
        var scope = _scopes?.CreateAsyncScope();
        try
        {
            var unit = CreateUnit(job, scope);
            try
            {
                return Rescue(job, attemptedAt, now, unit);
            }
            finally
            {
                if (unit is not null)
                {
                    await unit.DisposeAsync().ConfigureAwait(false);
                }
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

    private JobRescueParams? Rescue(JobRow job, DateTimeOffset attemptedAt, DateTimeOffset now, IWorkUnit? unit)
    {
        var timeout = EffectiveTimeout(job, unit);
        if (timeout < TimeSpan.Zero)
        {
            return null; // no timeout: never rescued by age
        }

        var threshold = timeout > _options.RescueStuckJobsAfter ? timeout : _options.RescueStuckJobsAfter;
        if (now - attemptedAt <= threshold)
        {
            return null;
        }

        var error = new AttemptError(now, job.Attempt, StuckMessage(threshold), null);
        if (job.Attempt >= job.MaxAttempts)
        {
            return new JobRescueParams { Id = job.Id, AttemptedAt = attemptedAt, State = JobState.Discarded, Error = error };
        }

        var retryAt = RetrySchedule.Next(
            job,
            WorkerRetryDelay(job, unit),
            _retryPolicy,
            now,
            e => LogRetryPolicyFailed(e, job.Id, job.Kind),
            () => LogRetryInThePast(job.Id, job.Kind));
        return new JobRescueParams
        {
            Id = job.Id,
            AttemptedAt = attemptedAt,
            State = retryAt - now <= _options.SchedulerInterval ? JobState.Available : JobState.Retryable,
            ScheduledAt = retryAt,
            Error = error,
        };
    }

    /// <summary>The message of the error appended to a rescued job: KopiQueue's own wording.</summary>
    internal static string StuckMessage(TimeSpan threshold) => string.Create(
        CultureInfo.InvariantCulture,
        $"The job was still running {threshold:c} after this attempt started, so the rescuer took it for stuck (its client may have stopped or crashed) and rescued it.");

    /// <summary>The job's worker for this attempt, or <c>null</c> when there is none here or it cannot be built.</summary>
    private IWorkUnit? CreateUnit(JobRow job, AsyncServiceScope? scope)
    {
        if (_options.Workers is null || !_options.Workers.TryGet(job.Kind, out var registration))
        {
            return null;
        }

        try
        {
            return registration.CreateWorkUnit(job, _options.Serializer, scope?.ServiceProvider ?? _services);
        }
#pragma warning disable CA1031 // Bad args or a worker that cannot be built leave the client's defaults, as for an unknown kind.
        catch (Exception e)
#pragma warning restore CA1031
        {
            LogWorkerUnavailable(e, job.Id, job.Kind);
            return null;
        }
    }

    /// <summary>The executor's rule (§5.2): the worker's timeout, else the client's; zero is the client's, negative is none.</summary>
    private TimeSpan EffectiveTimeout(JobRow job, IWorkUnit? unit)
    {
        TimeSpan? timeout = null;
        try
        {
            timeout = unit?.Timeout();
        }
#pragma warning disable CA1031 // A Timeout override that throws leaves the client's timeout.
        catch (Exception e)
#pragma warning restore CA1031
        {
            LogWorkerUnavailable(e, job.Id, job.Kind);
        }

        return timeout is { } t && t != TimeSpan.Zero ? t : _options.JobTimeout;
    }

    private TimeSpan? WorkerRetryDelay(JobRow job, IWorkUnit? unit)
    {
        try
        {
            return unit?.NextRetry();
        }
#pragma warning disable CA1031 // A NextRetry override that throws leaves the retry policy, as in the executor.
        catch (Exception e)
#pragma warning restore CA1031
        {
            LogWorkerUnavailable(e, job.Id, job.Kind);
            return null;
        }
    }

    [LoggerMessage(EventId = 2600, Level = LogLevel.Warning, Message = "Rescued stuck job {JobId} ({Kind}) after attempt {Attempt}: {State}, due {ScheduledAt}")]
    private partial void LogRescued(long jobId, string kind, int attempt, JobState state, DateTimeOffset scheduledAt);

    [LoggerMessage(EventId = 2601, Level = LogLevel.Warning, Message = "Discarded stuck job {JobId} ({Kind}): attempt {Attempt} was its last")]
    private partial void LogDiscarded(long jobId, string kind, int attempt);

    [LoggerMessage(EventId = 2602, Level = LogLevel.Information, Message = "The worker of stuck job {JobId} ({Kind}) could not be resolved or failed; using the client's defaults")]
    private partial void LogWorkerUnavailable(Exception exception, long jobId, string kind);

    [LoggerMessage(EventId = 2603, Level = LogLevel.Warning, Message = "The retry policy failed for stuck job {JobId} ({Kind}); using the default policy")]
    private partial void LogRetryPolicyFailed(Exception exception, long jobId, string kind);

    [LoggerMessage(EventId = 2604, Level = LogLevel.Warning, Message = "The next retry of stuck job {JobId} ({Kind}) was in the past; using the default policy")]
    private partial void LogRetryInThePast(long jobId, string kind);
}
