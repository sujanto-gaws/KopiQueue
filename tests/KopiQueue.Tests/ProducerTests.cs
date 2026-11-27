using System.Collections.Concurrent;
using System.Data.Common;
using KopiQueue.Drivers;
using KopiQueue.Execution;
using Microsoft.Extensions.Time.Testing;

namespace KopiQueue.Tests;

/// <summary>
/// The producer's fetch loop against an in-memory queue (plan task 2.3: design §5.1, FR-EXE-03, FR-EXE-04).
/// </summary>
/// <remarks>
/// Time is a <see cref="FakeTimeProvider"/>. The loop runs on its own thread, so tests advance the clock in small steps
/// until the expected state appears, with a real-time limit.
/// </remarks>
public sealed class ProducerTests
{
    private static readonly TimeSpan Step = TimeSpan.FromMilliseconds(10);

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Fact]
    public async Task Fetches_at_once_on_start_then_polls_at_the_poll_interval()
    {
        var (driver, time) = (new FakeDriver(), new FakeTimeProvider());
        await using var producer = Create(driver, time, options => options);

        await producer.StartAsync(Ct);
        await AdvanceUntil(time, () => driver.Fetches.Count == 1);
        var firstPoll = await AdvanceUntil(time, () => driver.Fetches.Count == 2);
        var secondPoll = await AdvanceUntil(time, () => driver.Fetches.Count == 3);

        Assert.InRange(firstPoll, TimeSpan.FromSeconds(1) - Step, TimeSpan.FromSeconds(1) + (3 * Step));
        Assert.InRange(secondPoll, TimeSpan.FromSeconds(1) - Step, TimeSpan.FromSeconds(1) + (3 * Step));
        Assert.All(driver.Fetches, f => Assert.Equal(("q", "client-1", 10), (f.Queue, f.ClientId, f.Max)));
    }

    [Fact]
    public async Task A_wake_fetches_after_the_cooldown_and_a_burst_of_wakes_fetches_once()
    {
        var (driver, time) = (new FakeDriver(), new FakeTimeProvider());
        await using var producer = Create(driver, time, options => options);
        await producer.StartAsync(Ct);
        await AdvanceUntil(time, () => driver.Fetches.Count == 1);

        for (var i = 0; i < 20; i++)
        {
            producer.Wake();
        }

        var woken = await AdvanceUntil(time, () => driver.Fetches.Count == 2);
        await Settle(time, TimeSpan.FromMilliseconds(500));

        Assert.InRange(woken, TimeSpan.FromMilliseconds(100) - Step, TimeSpan.FromMilliseconds(100) + (3 * Step));
        Assert.Equal(2, driver.Fetches.Count);
    }

    [Fact]
    public async Task Never_runs_more_than_max_workers_and_fetches_again_when_a_slot_frees()
    {
        var (driver, time) = (new FakeDriver(), new FakeTimeProvider());
        driver.Add("q", 10);
        var gates = new ConcurrentDictionary<long, TaskCompletionSource>();
        var peak = 0;
        var running = 0;
        await using var producer = Create(driver, time, options => options with { MaxWorkers = 3 }, async (job, _) =>
        {
            var now = Interlocked.Increment(ref running);
            InterlockedMax(ref peak, now);
            await gates.GetOrAdd(job.Id, _ => new(TaskCreationOptions.RunContinuationsAsynchronously)).Task;
            Interlocked.Decrement(ref running);
        });

        await producer.StartAsync(Ct);
        await AdvanceUntil(time, () => producer.RunningCount == 3);
        await Settle(time, TimeSpan.FromSeconds(3));
        var fetchesWhileFull = driver.Fetches.Count;
        gates.GetOrAdd(1, _ => new(TaskCreationOptions.RunContinuationsAsynchronously)).SetResult();
        var refill = await AdvanceUntil(time, () => producer.RunningJobIds.Contains(4));

        Assert.Equal([3], driver.Fetches.Take(1).Select(f => f.Max));
        Assert.Equal(3, producer.RunningCount);
        Assert.Equal(3, peak);
        Assert.Equal(1, fetchesWhileFull); // a full batch with no free slot waits for a slot instead of polling
        Assert.Equal(1, driver.Fetches[^1].Max);
        Assert.True(refill < TimeSpan.FromMilliseconds(200), $"Refilled after {refill}, not at once.");

        for (long id = 1; id <= 10; id++)
        {
            gates.GetOrAdd(id, _ => new(TaskCreationOptions.RunContinuationsAsynchronously)).TrySetResult();
        }
    }

    [Fact]
    public async Task A_full_batch_fetches_again_after_the_cooldown_instead_of_waiting_to_poll()
    {
        var (driver, time) = (new FakeDriver(), new FakeTimeProvider());
        driver.Add("q", 5);
        await using var producer = Create(driver, time, options => options with { MaxWorkers = 100, FetchBatchMax = 2 });

        await producer.StartAsync(Ct);
        var elapsed = await AdvanceUntil(time, () => driver.Fetches.Count == 3);
        await Settle(time, TimeSpan.FromMilliseconds(500));

        Assert.Equal([2, 2, 2], driver.Fetches.Select(f => f.Max));
        Assert.Equal([2, 2, 1], driver.Fetches.Select(f => f.Returned));
        Assert.InRange(elapsed, TimeSpan.FromMilliseconds(200) - Step, TimeSpan.FromMilliseconds(200) + (5 * Step));
        Assert.Equal(3, driver.Fetches.Count); // the last batch was short, so no immediate refetch
        Assert.Equal(5, driver.Started.Count);
    }

    [Fact]
    public async Task The_drivers_row_limit_caps_the_batch()
    {
        var (driver, time) = (new FakeDriver { MaxRowsPerStatement = 4 }, new FakeTimeProvider());
        await using var producer = Create(driver, time, options => options with { MaxWorkers = 50 });

        await producer.StartAsync(Ct);
        await AdvanceUntil(time, () => driver.Fetches.Count == 1);

        Assert.Equal(4, driver.Fetches[0].Max);
    }

    [Fact]
    public async Task Insert_notifications_for_its_queue_wake_it_and_others_are_ignored()
    {
        var (driver, time) = (new FakeDriver(), new FakeTimeProvider());
        await using var producer = Create(driver, time, options => options);
        await producer.StartAsync(Ct);
        await AdvanceUntil(time, () => driver.Fetches.Count == 1);

        producer.OnInsertNotification("""{"queue": "other"}""");
        producer.OnInsertNotification("not json");
        producer.OnInsertNotification("""["q"]""");
        producer.OnInsertNotification("""{"queue": 5}""");
        await Settle(time, TimeSpan.FromMilliseconds(500));
        var ignored = driver.Fetches.Count;
        producer.OnInsertNotification("""{"queue": "q"}""");
        await AdvanceUntil(time, () => driver.Fetches.Count == 2);

        Assert.Equal(1, ignored);
    }

    [Fact]
    public async Task The_local_waker_wakes_it_for_its_queue_until_it_stops()
    {
        var (driver, time) = (new FakeDriver(), new FakeTimeProvider());
        var waker = new LocalWaker();
        await using var producer = Create(driver, time, options => options, localWaker: waker);
        await producer.StartAsync(Ct);
        await AdvanceUntil(time, () => driver.Fetches.Count == 1);

        waker.Wake(["other"]);
        await Settle(time, TimeSpan.FromMilliseconds(500));
        var ignored = driver.Fetches.Count;
        waker.Wake(["q"]);
        await AdvanceUntil(time, () => driver.Fetches.Count == 2);
        await producer.StopAsync(Ct);
        waker.Wake(["q"]);
        await Settle(time, TimeSpan.FromSeconds(3));

        Assert.Equal(1, ignored);
        Assert.Equal(2, driver.Fetches.Count);
    }

    [Fact]
    public async Task A_failed_fetch_is_retried_at_the_next_poll()
    {
        var (driver, time) = (new FakeDriver { FailNext = 2 }, new FakeTimeProvider());
        driver.Add("q", 1);
        await using var producer = Create(driver, time, options => options);

        await producer.StartAsync(Ct);
        var elapsed = await AdvanceUntil(time, () => driver.Started.Count == 1);

        Assert.Equal(3, driver.Fetches.Count);
        Assert.InRange(elapsed, TimeSpan.FromSeconds(2) - Step, TimeSpan.FromSeconds(2) + (5 * Step));
    }

    [Fact]
    public async Task A_run_function_that_throws_frees_its_slot()
    {
        var (driver, time) = (new FakeDriver(), new FakeTimeProvider());
        driver.Add("q", 3);
        var runs = 0;
        await using var producer = Create(driver, time, options => options with { MaxWorkers = 1 }, (_, _) =>
        {
            Interlocked.Increment(ref runs);
            throw new InvalidOperationException("escaped the executor");
        });

        await producer.StartAsync(Ct);
        await AdvanceUntil(time, () => Volatile.Read(ref runs) == 3 && producer.RunningCount == 0);
    }

    [Fact]
    public async Task Stop_stops_fetching_and_waits_for_running_jobs()
    {
        var (driver, time) = (new FakeDriver(), new FakeTimeProvider());
        driver.Add("q", 1);
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var cancelled = false;
        await using var producer = Create(driver, time, options => options, async (_, ct) =>
        {
            await release.Task;
            cancelled = ct.IsCancellationRequested;
        });
        await producer.StartAsync(Ct);
        await AdvanceUntil(time, () => producer.RunningCount == 1);

        var stop = producer.StopAsync(Ct);
        await Settle(time, TimeSpan.FromSeconds(3));
        var stoppedEarly = stop.IsCompleted;
        var fetches = driver.Fetches.Count;
        release.SetResult();
        await stop.WaitAsync(TimeSpan.FromSeconds(10), Ct);

        Assert.False(stoppedEarly);
        Assert.False(cancelled);
        Assert.Equal(1, fetches);
        Assert.Equal(0, producer.RunningCount);
    }

    [Fact]
    public async Task A_cancelled_stop_cancels_running_jobs_and_abandons_those_that_ignore_it_after_the_grace_period()
    {
        var (driver, time) = (new FakeDriver(), new FakeTimeProvider());
        driver.Add("q", 2);
        var stubborn = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var cooperativeCancelled = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var producer = Create(driver, time, options => options with { HardStopGrace = TimeSpan.FromSeconds(5) }, async (job, ct) =>
        {
            if (job.Id == 1)
            {
                await Task.Delay(Timeout.Infinite, ct).ContinueWith(_ => cooperativeCancelled.SetResult(), TaskScheduler.Default);
            }
            else
            {
                await stubborn.Task; // ignores its token
            }
        });
        await producer.StartAsync(Ct);
        await AdvanceUntil(time, () => producer.RunningCount == 2);
        using var shutdown = new CancellationTokenSource();

        var stop = producer.StopAsync(shutdown.Token);
        await shutdown.CancelAsync();
        await cooperativeCancelled.Task.WaitAsync(TimeSpan.FromSeconds(10), Ct);
        await Settle(time, TimeSpan.FromSeconds(4));
        var beforeGrace = stop.IsCompleted;
        await AdvanceUntil(time, () => stop.IsCompleted);
        await stop;

        Assert.False(beforeGrace);
        Assert.Equal([2], producer.RunningJobIds);
        stubborn.SetResult();
    }

    [Fact]
    public async Task CancelJob_cancels_only_that_jobs_own_token_and_not_the_hard_stop()
    {
        var (driver, time) = (new FakeDriver(), new FakeTimeProvider());
        driver.Add("q", 2);
        var tokens = new ConcurrentDictionary<long, (CancellationToken HardStop, CancellationToken Cancel)>();
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var producer = new Producer(
            driver,
            new ProducerOptions { Queue = "q", ClientId = "client-1", MaxWorkers = 10 },
            async (job, hardStop, cancel) =>
            {
                tokens[job.Id] = (hardStop, cancel);
                await release.Task;
            },
            timeProvider: time);
        await producer.StartAsync(Ct);
        await AdvanceUntil(time, () => tokens.Count == 2);

        Assert.True(producer.CancelJob(1));
        Assert.True(producer.CancelJob(1)); // again: nothing more happens
        Assert.False(producer.CancelJob(3)); // not running here

        Assert.True(tokens[1].Cancel.IsCancellationRequested);
        Assert.False(tokens[2].Cancel.IsCancellationRequested);
        Assert.False(tokens[1].HardStop.IsCancellationRequested);
        release.SetResult();
        await AdvanceUntil(time, () => producer.RunningCount == 0);
        Assert.False(producer.CancelJob(2)); // finished
    }

    [Fact]
    public async Task Starting_twice_is_rejected()
    {
        await using var producer = Create(new FakeDriver(), new FakeTimeProvider(), options => options);
        await producer.StartAsync(Ct);

        await Assert.ThrowsAsync<InvalidOperationException>(() => producer.StartAsync(Ct));
    }

    [Theory]
    [InlineData(0, 10)]
    [InlineData(1, 0)]
    public void Invalid_limits_are_rejected(int maxWorkers, int fetchBatchMax) =>
        Assert.Throws<ArgumentOutOfRangeException>(() => Create(
            new FakeDriver(), new FakeTimeProvider(), options => options with { MaxWorkers = maxWorkers, FetchBatchMax = fetchBatchMax }));

    private static Producer Create(
        FakeDriver driver,
        FakeTimeProvider time,
        Func<Settings, Settings> configure,
        Func<JobRow, CancellationToken, Task>? run = null,
        LocalWaker? localWaker = null)
    {
        var settings = configure(new Settings());
        var options = new ProducerOptions
        {
            Queue = "q",
            ClientId = "client-1",
            MaxWorkers = settings.MaxWorkers,
            FetchBatchMax = settings.FetchBatchMax,
            HardStopGrace = settings.HardStopGrace,
        };
        run ??= (job, _) =>
        {
            driver.Started.Add(job.Id);
            return Task.CompletedTask;
        };
        return new Producer(driver, options, run, localWaker: localWaker, timeProvider: time);
    }

    private static void InterlockedMax(ref int target, int value)
    {
        int current;
        while ((current = Volatile.Read(ref target)) < value && Interlocked.CompareExchange(ref target, value, current) != current)
        {
        }
    }

    /// <summary>Advances fake time in steps until <paramref name="condition"/> holds; returns the fake time it took.</summary>
    private static async Task<TimeSpan> AdvanceUntil(FakeTimeProvider time, Func<bool> condition)
    {
        var start = time.GetUtcNow();
        var limit = DateTime.UtcNow.AddSeconds(10);
        while (!condition())
        {
            Assert.True(DateTime.UtcNow < limit, $"The condition was not met within 10 s; fake time advanced {time.GetUtcNow() - start}.");

            // Let the loop reach its next timer before moving the clock, so a step is never skipped over.
            await Task.Delay(5, Ct);
            if (!condition())
            {
                time.Advance(Step);
            }
        }

        return time.GetUtcNow() - start;
    }

    /// <summary>Advances fake time by <paramref name="duration"/> in steps, letting the loop run in between.</summary>
    private static async Task Settle(FakeTimeProvider time, TimeSpan duration)
    {
        for (var passed = TimeSpan.Zero; passed < duration; passed += Step)
        {
            await Task.Delay(5, Ct);
            time.Advance(Step);
        }

        await Task.Delay(20, Ct);
    }

    private sealed record Settings
    {
        public int MaxWorkers { get; init; } = 10;

        public int FetchBatchMax { get; init; } = 1_000;

        public TimeSpan HardStopGrace { get; init; } = TimeSpan.FromSeconds(5);
    }

    private sealed record Fetch(string Queue, string ClientId, int Max, int Returned);

    /// <summary>An in-memory queue: fetch hands out jobs in ID order, each once.</summary>
    private sealed class FakeDriver : IKopiDriver, IKopiExecutor
    {
        private readonly Lock _lock = new();
        private readonly List<JobRow> _available = [];
        private readonly List<Fetch> _fetches = [];
        private long _nextId;

        public int? MaxRowsPerStatement { get; init; }

        /// <summary>How many of the next fetches throw.</summary>
        public int FailNext { get; set; }

        public ConcurrentBag<long> Started { get; } = [];

        public IReadOnlyList<Fetch> Fetches
        {
            get
            {
                lock (_lock)
                {
                    return [.. _fetches];
                }
            }
        }

        public DriverCapabilities Capabilities => new() { ListenNotify = false, BulkCopy = false, MaxRowsPerStatement = MaxRowsPerStatement };

        public void Add(string queue, int count)
        {
            lock (_lock)
            {
                for (var i = 0; i < count; i++)
                {
                    var now = DateTimeOffset.UnixEpoch;
                    _available.Add(new JobRow
                    {
                        Id = ++_nextId, Kind = "k", Queue = queue, State = JobState.Available, Priority = 1, ArgsJson = "{}",
                        MetadataJson = "{}", Tags = [], Attempt = 0, MaxAttempts = 25, CreatedAt = now, ScheduledAt = now,
                    });
                }
            }
        }

        public IKopiExecutor GetExecutor() => this;

        public IKopiExecutor GetExecutor(DbTransaction transaction) => throw new NotSupportedException();

        public KopiQueue.Migrations.KopiMigrator CreateMigrator(Microsoft.Extensions.Logging.ILoggerFactory? loggerFactory = null) =>
            throw new NotSupportedException();

        public IKopiListener? CreateListener() => null;

        public Task<IReadOnlyList<JobRow>> JobGetAvailableAsync(JobGetAvailableParams parameters, CancellationToken cancellationToken)
        {
            lock (_lock)
            {
                if (FailNext > 0)
                {
                    FailNext--;
                    _fetches.Add(new Fetch(parameters.Queue, parameters.ClientId, parameters.Max, -1));
                    throw new InvalidOperationException("database down");
                }

                var jobs = _available.Where(j => j.Queue == parameters.Queue).Take(parameters.Max).ToList();
                _available.RemoveAll(jobs.Contains);
                _fetches.Add(new Fetch(parameters.Queue, parameters.ClientId, parameters.Max, jobs.Count));
                return Task.FromResult<IReadOnlyList<JobRow>>(
                    [.. jobs.Select(j => new JobRow
                    {
                        Id = j.Id, Kind = j.Kind, Queue = j.Queue, State = JobState.Running, Priority = j.Priority, ArgsJson = j.ArgsJson,
                        MetadataJson = j.MetadataJson, Tags = j.Tags, Attempt = j.Attempt + 1, MaxAttempts = j.MaxAttempts,
                        CreatedAt = j.CreatedAt, ScheduledAt = j.ScheduledAt, AttemptedBy = [parameters.ClientId],
                    })]);
            }
        }

        public Task<IReadOnlyList<JobInsertResult>> JobInsertManyAsync(IReadOnlyList<JobInsertParams> jobs, CancellationToken cancellationToken) =>
            throw new NotSupportedException();

        public Task<long> JobInsertFastAsync(IReadOnlyList<JobInsertParams> jobs, CancellationToken cancellationToken) =>
            throw new NotSupportedException();

        public Task<JobRow?> JobGetByIdAsync(long id, CancellationToken cancellationToken) => throw new NotSupportedException();

        public Task<bool> LeaderAttemptElectAsync(LeaderElectParams parameters, CancellationToken cancellationToken) => Task.FromResult(false);

        public Task<bool> LeaderAttemptReelectAsync(LeaderElectParams parameters, CancellationToken cancellationToken) => Task.FromResult(false);

        public Task<bool> LeaderResignAsync(string leaderId, CancellationToken cancellationToken) => Task.FromResult(false);

        public Task<IReadOnlyList<QueueRow>> QueueCreateOrSetUpdatedAtManyAsync(IReadOnlyList<string> names, CancellationToken cancellationToken) =>
            Task.FromResult<IReadOnlyList<QueueRow>>([]);

        public Task<QueueRow?> QueueGetAsync(string name, CancellationToken cancellationToken) => Task.FromResult<QueueRow?>(null);

        public Task<IReadOnlyList<QueueRow>> QueueListAsync(QueueListQuery query, CancellationToken cancellationToken) =>
            Task.FromResult<IReadOnlyList<QueueRow>>([]);

        public Task<bool> QueuePauseAsync(string name, CancellationToken cancellationToken) => Task.FromResult(false);

        public Task<bool> QueueResumeAsync(string name, CancellationToken cancellationToken) => Task.FromResult(false);

        public Task<QueueRow?> QueueUpdateAsync(QueueUpdateParams parameters, CancellationToken cancellationToken) => Task.FromResult<QueueRow?>(null);

        public Task<IReadOnlyList<string>> QueueDeleteExpiredAsync(QueueDeleteExpiredParams parameters, CancellationToken cancellationToken) =>
            Task.FromResult<IReadOnlyList<string>>([]);

        public Task<IReadOnlyList<JobScheduleResult>> JobScheduleAsync(JobScheduleParams parameters, CancellationToken cancellationToken) =>
            Task.FromResult<IReadOnlyList<JobScheduleResult>>([]);

        public Task<JobListStuckResult> JobListStuckAsync(JobListStuckParams parameters, CancellationToken cancellationToken) =>
            Task.FromResult(new JobListStuckResult(DateTimeOffset.UtcNow, []));

        public Task<IReadOnlyList<JobRow>> JobRescueManyAsync(IReadOnlyList<JobRescueParams> jobs, CancellationToken cancellationToken) =>
            Task.FromResult<IReadOnlyList<JobRow>>([]);

        public Task<IReadOnlyList<long>> JobDeleteFinalizedAsync(JobDeleteFinalizedParams parameters, CancellationToken cancellationToken) =>
            Task.FromResult<IReadOnlyList<long>>([]);

        public Task<JobRow?> JobCancelAsync(long id, CancellationToken cancellationToken) =>
            Task.FromResult<JobRow?>(null);

        public Task<IReadOnlyList<long>> JobListCancelAttemptedAsync(IReadOnlyList<long> ids, CancellationToken cancellationToken) =>
            Task.FromResult<IReadOnlyList<long>>([]);

        public Task<IReadOnlyList<JobRow>> JobSetStateIfRunningManyAsync(IReadOnlyList<JobSetStateParams> jobs, CancellationToken cancellationToken) =>
            throw new NotSupportedException();
    }
}
