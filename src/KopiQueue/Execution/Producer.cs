using System.Collections.Concurrent;
using System.Text.Json;
using KopiQueue.Drivers;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace KopiQueue.Execution;

/// <summary>Settings of one queue's <see cref="Producer"/> (design §5.1, §7).</summary>
internal sealed class ProducerOptions
{
    /// <summary>The queue.</summary>
    public required string Queue { get; init; }

    /// <summary>The most jobs of this queue that run at once on this client (FR-EXE-04).</summary>
    public required int MaxWorkers { get; init; }

    /// <summary>The client's ID, recorded in each fetched job's <c>attempted_by</c>.</summary>
    public required string ClientId { get; init; }

    /// <summary>The most jobs one fetch asks for. Default: 1,000.</summary>
    public int FetchBatchMax { get; init; } = 1_000;

    /// <summary>The least time between two fetches, which coalesces bursts of wake-ups. Default: 100 ms.</summary>
    public TimeSpan FetchCooldown { get; init; } = TimeSpan.FromMilliseconds(100);

    /// <summary>How long the producer waits for a wake-up before it fetches anyway (FR-EXE-03). Default: 1 s.</summary>
    public TimeSpan FetchPollInterval { get; init; } = TimeSpan.FromSeconds(1);

    /// <summary>After a hard stop cancels the running jobs, how long to wait for them to unwind. Default: 5 s.</summary>
    public TimeSpan HardStopGrace { get; init; } = TimeSpan.FromSeconds(5);
}

/// <summary>
/// Fetches one queue's jobs and runs them, never more than <see cref="ProducerOptions.MaxWorkers"/> at once (design
/// §3, §5.1; FR-EXE-01..04).
/// </summary>
/// <remarks>
/// <para>
/// The producer fetches when woken (an insert notification for its queue, a resync after the notifier reconnects, an
/// insert by its own client, a slot freed while work was waiting) or when the poll interval passes without a wake-up,
/// which is all it has in poll-only mode. Fetches are at least the cooldown apart. A fetch asks for the free slots,
/// capped at the batch size; a full batch means more work is likely, so the producer fetches again at once.
/// </para>
/// <para>
/// Each job runs through the run function on its own task, with a token that a hard stop cancels. The slot stays
/// taken until the function returns, which for the executor is when the completer has accepted the outcome. The run
/// function must not throw; if it does, the error is logged and the slot freed.
/// </para>
/// <para>
/// A paused queue fetches nothing; running jobs carry on (FR-QUE-03). It is paused while either its pause everywhere (the
/// database's <c>paused_at</c>, applied by the <see cref="QueueMaintainer"/>) or its local pause is set; clearing the
/// last one fetches at once.
/// </para>
/// </remarks>
internal sealed partial class Producer : IStartStopService, IAsyncDisposable
{
    private readonly IKopiDriver _driver;
    private readonly ProducerOptions _options;
    private readonly Func<JobRow, CancellationToken, Task> _runJob;
    private readonly Notifier? _notifier;
    private readonly LocalWaker? _localWaker;
    private readonly TimeProvider _time;
    private readonly ILogger _logger;
    private readonly WakeSignal _wake = new();
    private readonly ConcurrentDictionary<long, RunningJob> _running = new();
    private readonly CancellationTokenSource _hardStop = new();
    private readonly Lock _lock = new();
    private readonly int _fetchMax;
    private CancellationTokenSource? _stopFetching;
    private Task? _loop;
    private IDisposable[] _subscriptions = [];
    private int _waitingForSlot;
    private int _pausedEverywhere;
    private int _pausedLocally;

    public Producer(
        IKopiDriver driver,
        ProducerOptions options,
        Func<JobRow, CancellationToken, Task> runJob,
        Notifier? notifier = null,
        LocalWaker? localWaker = null,
        TimeProvider? timeProvider = null,
        ILogger<Producer>? logger = null)
    {
        ArgumentNullException.ThrowIfNull(driver);
        ArgumentNullException.ThrowIfNull(options);
        ArgumentNullException.ThrowIfNull(runJob);
        ArgumentOutOfRangeException.ThrowIfLessThan(options.MaxWorkers, 1, nameof(options));
        ArgumentOutOfRangeException.ThrowIfLessThan(options.FetchBatchMax, 1, nameof(options));
        _driver = driver;
        _options = options;
        _runJob = runJob;
        _notifier = notifier;
        _localWaker = localWaker;
        _time = timeProvider ?? TimeProvider.System;
        _logger = (ILogger?)logger ?? NullLogger.Instance;
        _fetchMax = Math.Min(options.FetchBatchMax, driver.Capabilities.MaxRowsPerStatement ?? int.MaxValue);
    }

    /// <summary>The queue.</summary>
    public string Queue => _options.Queue;

    /// <summary>How many jobs are running now.</summary>
    public int RunningCount => _running.Count;

    /// <summary>The IDs of the jobs running now.</summary>
    public IReadOnlyCollection<long> RunningJobIds => [.. _running.Keys];

    /// <summary>Asks for a fetch as soon as the cooldown allows.</summary>
    public void Wake() => _wake.Set();

    /// <summary>Whether the queue is paused here, everywhere or locally, so nothing is fetched.</summary>
    public bool IsPaused => Volatile.Read(ref _pausedEverywhere) == 1 || Volatile.Read(ref _pausedLocally) == 1;

    /// <summary>
    /// Sets the queue's pause everywhere (<paramref name="locally"/> <c>false</c>) or its local pause. Fetching stops
    /// while either is set; when neither is any more, the producer fetches at once.
    /// </summary>
    public void SetPaused(bool paused, bool locally)
    {
        lock (_lock)
        {
            var was = IsPaused;
            if (locally)
            {
                Volatile.Write(ref _pausedLocally, paused ? 1 : 0);
            }
            else
            {
                Volatile.Write(ref _pausedEverywhere, paused ? 1 : 0);
            }

            if (was == IsPaused)
            {
                return;
            }

            if (was)
            {
                LogResumed(Queue);
                Wake();
            }
            else
            {
                LogPaused(Queue);
            }
        }
    }

    public Task StartAsync(CancellationToken cancellationToken)
    {
        lock (_lock)
        {
            if (_loop is not null)
            {
                throw new InvalidOperationException($"The producer for queue '{Queue}' was already started.");
            }

            var subscriptions = new List<IDisposable>();
            if (_notifier is not null)
            {
                subscriptions.Add(_notifier.Subscribe(NotificationTopic.Insert, OnInsertNotification, Wake));
            }

            if (_localWaker is not null)
            {
                subscriptions.Add(_localWaker.Subscribe(Queue, Wake));
            }

            _subscriptions = [.. subscriptions];
            _stopFetching = new CancellationTokenSource();
            var stop = _stopFetching.Token;
            _wake.Set(); // Fetch at once: jobs may have been waiting while the client was down.
            _loop = Task.Run(() => RunAsync(stop), CancellationToken.None);
        }

        return Task.CompletedTask;
    }

    /// <summary>
    /// Stops fetching, then waits for the running jobs (a soft stop). If <paramref name="cancellationToken"/> fires first,
    /// cancels every running job's token (a hard stop) and waits at most <see cref="ProducerOptions.HardStopGrace"/> more.
    /// Jobs still running after that stay <c>running</c> in the database and are rescued later (design §5.9).
    /// </summary>
    public async Task StopAsync(CancellationToken cancellationToken)
    {
        try
        {
            await StopFetchingAsync().WaitAsync(cancellationToken).ConfigureAwait(false);
            await WaitForRunningAsync(cancellationToken).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            await HardStopAsync().ConfigureAwait(false);
        }
    }

    /// <summary>
    /// Stops fetching and unsubscribes from wake-ups. A fetch in progress completes and its jobs start, since the
    /// database already marked them running; only <see cref="HardStopAsync"/> aborts it. Running jobs carry on.
    /// </summary>
    public async Task StopFetchingAsync()
    {
        Task? loop;
        IDisposable[] subscriptions;
        lock (_lock)
        {
            (loop, subscriptions) = (_loop, _subscriptions);
            _subscriptions = [];
            _stopFetching?.Cancel();
        }

        foreach (var subscription in subscriptions)
        {
            subscription.Dispose();
        }

        if (loop is not null)
        {
            await loop.ConfigureAwait(false);
        }
    }

    /// <summary>Waits until no job is running.</summary>
    public Task WaitForRunningAsync(CancellationToken cancellationToken) =>
        Task.WhenAll(_running.Values.Select(r => r.Task)).WaitAsync(cancellationToken);

    /// <summary>Cancels every running job's token, then waits at most the grace period for them to finish.</summary>
    public async Task HardStopAsync()
    {
        var count = _running.Count;
        if (count > 0)
        {
            LogHardStop(count, Queue);
        }

        lock (_lock)
        {
            _stopFetching?.Cancel();
        }

        await _hardStop.CancelAsync().ConfigureAwait(false);
        try
        {
            // The loop first: a fetch the hard stop aborted may still have started jobs.
            var loop = _loop ?? Task.CompletedTask;
            var deadline = _time.GetUtcNow() + _options.HardStopGrace;
            await loop.WaitAsync(_options.HardStopGrace, _time).ConfigureAwait(false);
            var left = deadline - _time.GetUtcNow();
            await Task.WhenAll(_running.Values.Select(r => r.Task)).WaitAsync(left > TimeSpan.Zero ? left : TimeSpan.Zero, _time).ConfigureAwait(false);
        }
        catch (TimeoutException)
        {
            LogAbandoned(_running.Count, Queue);
        }
    }

    public async ValueTask DisposeAsync()
    {
        await StopAsync(new CancellationToken(canceled: true)).ConfigureAwait(false);
        _stopFetching?.Dispose();
        _hardStop.Dispose();
    }

    /// <summary>Handles an insert notification: wakes the producer if it names this queue.</summary>
    internal void OnInsertNotification(string payload)
    {
        try
        {
            using var json = JsonDocument.Parse(payload);
            if (json.RootElement.ValueKind == JsonValueKind.Object
                && json.RootElement.TryGetProperty("queue", out var queue)
                && queue.ValueKind == JsonValueKind.String
                && queue.ValueEquals(Queue))
            {
                Wake();
            }
        }
        catch (JsonException)
        {
            LogBadNotification(Queue);
        }
    }

    private async Task RunAsync(CancellationToken stop)
    {
        var lastFetch = DateTimeOffset.MinValue;
        while (!stop.IsCancellationRequested)
        {
            try
            {
                await _wake.WaitAsync(_options.FetchPollInterval, _time, stop).ConfigureAwait(false);
                if (IsPaused)
                {
                    // Look again after clearing, so a resume in between is not missed.
                    _wake.Clear();
                    if (IsPaused)
                    {
                        continue;
                    }
                }

                var sinceLast = _time.GetUtcNow() - lastFetch;
                if (sinceLast < _options.FetchCooldown)
                {
                    await Task.Delay(_options.FetchCooldown - sinceLast, _time, stop).ConfigureAwait(false);
                    if (IsPaused)
                    {
                        continue; // paused during the cooldown: only a fetch already sent completes
                    }
                }

                var free = _options.MaxWorkers - _running.Count;
                if (free <= 0)
                {
                    // Ask to be woken when a slot frees, then look again: a slot freed in between is not missed.
                    Volatile.Write(ref _waitingForSlot, 1);
                    free = _options.MaxWorkers - _running.Count;
                    if (free <= 0)
                    {
                        continue;
                    }

                    Volatile.Write(ref _waitingForSlot, 0);
                }

                var limit = Math.Min(free, _fetchMax);
                lastFetch = _time.GetUtcNow();
                _wake.Clear(); // this fetch covers every wake-up so far

                // Not the stop token: cancelling a fetch mid-statement could leave jobs marked running that never start.
                var jobs = await _driver.GetExecutor()
                    .JobGetAvailableAsync(new JobGetAvailableParams { Queue = Queue, Max = limit, ClientId = _options.ClientId }, _hardStop.Token)
                    .ConfigureAwait(false);
                foreach (var job in jobs)
                {
                    Start(job);
                }

                if (jobs.Count >= limit)
                {
                    // A full batch: more is likely waiting. Fetch again after the cooldown, or when a slot frees up.
                    Wake();
                }
            }
            catch (OperationCanceledException) when (stop.IsCancellationRequested || _hardStop.IsCancellationRequested)
            {
                break;
            }
#pragma warning disable CA1031 // A failed fetch (for example the database is down) must not end the loop: poll again.
            catch (Exception e)
#pragma warning restore CA1031
            {
                LogFetchFailed(e, Queue);
            }
        }
    }

    private void Start(JobRow job)
    {
        var running = new RunningJob(CancellationTokenSource.CreateLinkedTokenSource(_hardStop.Token));

        // Created unstarted and registered first, so a stop that looks at the running set always sees the real task.
        var run = new Task<Task>(() => RunJobAsync(job, running));
        running.Task = run.Unwrap();
        if (!_running.TryAdd(job.Id, running))
        {
            // The database hands a job to one fetch at a time; seeing it twice means it was rescued while still running here.
            LogDuplicate(job.Id, Queue);
            running.Cancellation.Dispose();
            return;
        }

        run.Start(TaskScheduler.Default);
    }

    private async Task RunJobAsync(JobRow job, RunningJob running)
    {
        try
        {
            await _runJob(job, running.Cancellation.Token).ConfigureAwait(false);
        }
#pragma warning disable CA1031 // The executor handles every outcome; anything that escapes it is logged so the slot is not lost.
        catch (Exception e)
#pragma warning restore CA1031
        {
            LogRunFailed(e, job.Id, Queue);
        }
        finally
        {
            _running.TryRemove(job.Id, out _);
            running.Cancellation.Dispose();
            if (Interlocked.Exchange(ref _waitingForSlot, 0) == 1)
            {
                Wake();
            }
        }
    }

    [LoggerMessage(EventId = 2000, Level = LogLevel.Warning, Message = "Fetching jobs of queue {Queue} failed; trying again at the next poll")]
    private partial void LogFetchFailed(Exception exception, string queue);

    [LoggerMessage(EventId = 2001, Level = LogLevel.Error, Message = "Running job {JobId} of queue {Queue} failed outside the executor")]
    private partial void LogRunFailed(Exception exception, long jobId, string queue);

    [LoggerMessage(EventId = 2002, Level = LogLevel.Warning, Message = "Job {JobId} of queue {Queue} was fetched while it was still running here; skipped")]
    private partial void LogDuplicate(long jobId, string queue);

    [LoggerMessage(EventId = 2003, Level = LogLevel.Warning, Message = "Hard stop: cancelling {Count} running jobs of queue {Queue}")]
    private partial void LogHardStop(int count, string queue);

    [LoggerMessage(EventId = 2004, Level = LogLevel.Warning, Message = "{Count} jobs of queue {Queue} did not stop within the grace period; they stay running and will be rescued")]
    private partial void LogAbandoned(int count, string queue);

    [LoggerMessage(EventId = 2005, Level = LogLevel.Debug, Message = "Ignored an insert notification for queue {Queue} that is not valid JSON")]
    private partial void LogBadNotification(string queue);

    [LoggerMessage(EventId = 2006, Level = LogLevel.Information, Message = "Queue {Queue} is paused here; running jobs carry on")]
    private partial void LogPaused(string queue);

    [LoggerMessage(EventId = 2007, Level = LogLevel.Information, Message = "Queue {Queue} is resumed here")]
    private partial void LogResumed(string queue);

    private sealed class RunningJob(CancellationTokenSource cancellation)
    {
        public CancellationTokenSource Cancellation { get; } = cancellation;

        public Task Task { get; set; } = Task.CompletedTask;
    }
}
