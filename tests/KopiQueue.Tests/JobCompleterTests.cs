using System.Collections.Concurrent;
using System.Data.Common;
using KopiQueue.Drivers;
using KopiQueue.Execution;
using Microsoft.Extensions.Time.Testing;

namespace KopiQueue.Tests;

/// <summary>The completers: batching, retries, back-pressure and stop (plan task 2.6: FR-EXE-06, NFR-REL-03, NFR-REL-04).</summary>
public sealed class JobCompleterTests
{
    private static readonly TimeSpan Step = TimeSpan.FromMilliseconds(5);

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Fact]
    public async Task A_batch_is_written_once_the_flush_interval_passes_after_its_first_completion()
    {
        var (driver, time) = (new FakeDriver(), new FakeTimeProvider());
        await using var completer = Batch(driver, time);
        await completer.StartAsync(Ct);

        await completer.CompleteAsync(Completion(1), Ct);
        time.Advance(TimeSpan.FromMilliseconds(30));
        await completer.CompleteAsync(Completion(2), Ct);
        await Settle(time, TimeSpan.FromMilliseconds(15));
        var early = driver.Batches.Count;
        await AdvanceUntil(time, () => driver.Batches.Count == 1);

        Assert.Equal(0, early);
        Assert.Equal([1L, 2L], driver.Batches[0].Select(p => p.Id));
    }

    [Fact]
    public async Task A_full_batch_is_written_at_once_and_the_rest_waits_for_the_next()
    {
        var (driver, time) = (new FakeDriver(), new FakeTimeProvider());
        await using var completer = Batch(driver, time, new JobCompleterOptions { MaxBatchSize = 3 });
        await completer.StartAsync(Ct);

        for (long id = 1; id <= 7; id++)
        {
            await completer.CompleteAsync(Completion(id), Ct);
        }

        await Until(() => driver.Batches.Count == 2);
        await AdvanceUntil(time, () => driver.Batches.Count == 3);

        Assert.Equal([3, 3, 1], driver.Batches.Select(b => b.Count));
        Assert.Equal(Enumerable.Range(1, 7).Select(i => (long)i), driver.Batches.SelectMany(b => b).Select(p => p.Id));
    }

    [Fact]
    public async Task The_drivers_row_limit_caps_the_batch()
    {
        var (driver, time) = (new FakeDriver { MaxRowsPerStatement = 2 }, new FakeTimeProvider());
        await using var completer = Batch(driver, time);
        await completer.StartAsync(Ct);

        for (long id = 1; id <= 5; id++)
        {
            await completer.CompleteAsync(Completion(id), Ct);
        }

        await AdvanceUntil(time, () => driver.Batches.Sum(b => b.Count) == 5);

        Assert.All(driver.Batches, b => Assert.InRange(b.Count, 1, 2));
    }

    [Fact]
    public async Task Completions_are_reported_only_after_their_write_succeeded_and_with_the_stored_row()
    {
        var (driver, time) = (new FakeDriver { FailNext = 2 }, new FakeTimeProvider());
        driver.StoredState[2] = JobState.Cancelled; // cancelled while it ran
        var reported = new ConcurrentQueue<CompletedJob>();
        var callsWhenReported = new ConcurrentQueue<int>();
        await using var completer = Batch(driver, time, onCompleted: batch =>
        {
            callsWhenReported.Enqueue(driver.Calls);
            foreach (var c in batch)
            {
                reported.Enqueue(c);
            }
        });
        await completer.StartAsync(Ct);

        await completer.CompleteAsync(Completion(1), Ct);
        await completer.CompleteAsync(Completion(2), Ct);
        await completer.CompleteAsync(Completion(5000), Ct);
        await AdvanceUntil(time, () => reported.Count == 3);

        var byId = reported.ToDictionary(c => c.Completion.Job.Id);
        Assert.Equal(JobState.Completed, byId[1].Row!.State);
        Assert.Equal(JobState.Cancelled, byId[2].Row!.State);
        Assert.Null(byId[5000].Row);
        Assert.Equal([3], callsWhenReported); // after the successful write, not after the failed ones
    }

    [Fact]
    public async Task Failed_writes_are_retried_with_a_doubling_delay_up_to_the_maximum()
    {
        var time = new FakeTimeProvider();
        var driver = new FakeDriver { FailNext = 5, Clock = time };
        await using var completer = Batch(driver, time, new JobCompleterOptions
        {
            RetryMinDelay = TimeSpan.FromMilliseconds(100), RetryMaxDelay = TimeSpan.FromMilliseconds(500),
        });
        await completer.StartAsync(Ct);

        await completer.CompleteAsync(Completion(1), Ct);
        await AdvanceUntil(time, () => driver.Calls == 6);

        var gaps = driver.CallTimes.Zip(driver.CallTimes.Skip(1), (a, b) => b - a).ToArray();
        Assert.Equal([100, 200, 400, 500, 500], gaps.Select(g => Math.Round(g.TotalMilliseconds / 10) * 10));
    }

    [Fact]
    public async Task A_full_queue_makes_callers_wait()
    {
        var (driver, time) = (new FakeDriver(), new FakeTimeProvider());
        var gate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        driver.Gate = gate.Task;
        await using var completer = Batch(driver, time, new JobCompleterOptions { MaxBatchSize = 2, Capacity = 2 });
        await completer.StartAsync(Ct);

        await completer.CompleteAsync(Completion(1), Ct);
        await completer.CompleteAsync(Completion(2), Ct);
        await Until(() => driver.Calls == 1); // the first batch is being written, and holds the write
        await completer.CompleteAsync(Completion(3), Ct);
        await completer.CompleteAsync(Completion(4), Ct);
        var blocked = completer.CompleteAsync(Completion(5), Ct).AsTask();
        await Task.Delay(50, Ct);
        var waited = !blocked.IsCompleted;
        gate.SetResult();
        await blocked.WaitAsync(TimeSpan.FromSeconds(10), Ct);

        Assert.True(waited);
    }

    [Fact]
    public async Task Stop_writes_what_is_pending_and_later_completions_are_refused()
    {
        var (driver, time) = (new FakeDriver(), new FakeTimeProvider());
        var completer = Batch(driver, time);
        await completer.StartAsync(Ct);
        await completer.CompleteAsync(Completion(1), Ct);

        await completer.StopAsync(Ct);

        Assert.Equal([1L], driver.Batches.SelectMany(b => b).Select(p => p.Id));
        await Assert.ThrowsAsync<InvalidOperationException>(() => completer.CompleteAsync(Completion(2), Ct).AsTask());
        await completer.DisposeAsync();
    }

    [Fact]
    public async Task A_stop_whose_token_fires_abandons_writes_that_keep_failing()
    {
        var (driver, time) = (new FakeDriver { FailNext = int.MaxValue }, new FakeTimeProvider());
        var reported = 0;
        await using var completer = Batch(driver, time, onCompleted: _ => reported++);
        await completer.StartAsync(Ct);
        await completer.CompleteAsync(Completion(1), Ct);
        await AdvanceUntil(time, () => driver.Calls >= 2);
        using var shutdown = new CancellationTokenSource();

        var stop = completer.StopAsync(shutdown.Token);
        await Task.Delay(50, Ct);
        var stillTrying = !stop.IsCompleted;
        await shutdown.CancelAsync();
        await stop.WaitAsync(TimeSpan.FromSeconds(10), Ct);

        Assert.True(stillTrying);
        Assert.Equal(0, reported);
    }

    [Fact]
    public async Task The_inline_completer_writes_before_returning()
    {
        var (driver, time) = (new FakeDriver(), new FakeTimeProvider());
        await using var completer = JobCompleter.Create(CompleterKind.Inline, driver, timeProvider: time);

        await completer.CompleteAsync(Completion(1), Ct);

        Assert.Equal([1L], Assert.Single(driver.Batches).Select(p => p.Id));
    }

    [Fact]
    public async Task The_async_completer_writes_each_completion_on_its_own_and_stop_waits_for_them()
    {
        var (driver, time) = (new FakeDriver(), new FakeTimeProvider());
        var completer = JobCompleter.Create(CompleterKind.Async, driver, new JobCompleterOptions { MaxConcurrentWrites = 2 }, timeProvider: time);

        for (long id = 1; id <= 5; id++)
        {
            await completer.CompleteAsync(Completion(id), Ct);
        }

        await completer.StopAsync(Ct);

        Assert.Equal(5, driver.Batches.Count);
        Assert.All(driver.Batches, b => Assert.Single(b));
        Assert.Equal(Enumerable.Range(1, 5).Select(i => (long)i), driver.Batches.Select(b => b[0].Id).Order());
        await completer.DisposeAsync();
    }

    [Fact]
    public async Task Completions_map_onto_the_drivers_parameters()
    {
        var error = new AttemptError(DateTimeOffset.UnixEpoch, 2, "x", null);
        var completion = new JobCompletion(
            Row(9), JobState.Scheduled, null, DateTimeOffset.UnixEpoch.AddHours(1), error, TimeSpan.Zero, Attempt: 1, MetadataUpdates: """{"snoozes":1}""");
        var driver = new FakeDriver();
        await using var completer = JobCompleter.Create(CompleterKind.Inline, driver);

        await completer.CompleteAsync(completion, Ct);

        var p = Assert.Single(Assert.Single(driver.Batches));
        Assert.Equal(
            (9L, JobState.Scheduled, (int?)1, error, (DateTimeOffset?)null, """{"snoozes":1}""", (DateTimeOffset?)DateTimeOffset.UnixEpoch.AddHours(1)),
            (p.Id, p.State, p.Attempt, p.Error, p.FinalizedAt, p.MetadataUpdates, p.ScheduledAt));
    }

    private static JobCompleter Batch(
        FakeDriver driver, FakeTimeProvider time, JobCompleterOptions? options = null, Action<IReadOnlyList<CompletedJob>>? onCompleted = null) =>
        JobCompleter.Create(CompleterKind.Batch, driver, options, onCompleted, time);

    private static JobCompletion Completion(long id) =>
        new(Row(id), JobState.Completed, DateTimeOffset.UnixEpoch, null, null, TimeSpan.Zero);

    private static JobRow Row(long id) => new()
    {
        Id = id, Kind = "k", Queue = "q", State = JobState.Running, Priority = 1, ArgsJson = "{}", MetadataJson = "{}", Tags = [],
        Attempt = 1, MaxAttempts = 25, CreatedAt = DateTimeOffset.UnixEpoch, ScheduledAt = DateTimeOffset.UnixEpoch,
    };

    private static async Task Until(Func<bool> condition)
    {
        var limit = DateTime.UtcNow.AddSeconds(10);
        while (!condition())
        {
            Assert.True(DateTime.UtcNow < limit, "The condition was not met within 10 s.");
            await Task.Delay(5, Ct);
        }
    }

    private static async Task AdvanceUntil(FakeTimeProvider time, Func<bool> condition)
    {
        var limit = DateTime.UtcNow.AddSeconds(10);
        while (!condition())
        {
            Assert.True(DateTime.UtcNow < limit, "The condition was not met within 10 s.");
            await Task.Delay(5, Ct);
            if (!condition())
            {
                time.Advance(Step);
            }
        }
    }

    private static async Task Settle(FakeTimeProvider time, TimeSpan duration)
    {
        for (var passed = TimeSpan.Zero; passed < duration; passed += Step)
        {
            await Task.Delay(5, Ct);
            time.Advance(Step);
        }

        await Task.Delay(20, Ct);
    }

    /// <summary>Records each write; jobs 1..999 exist and are running unless <see cref="StoredState"/> says otherwise.</summary>
    private sealed class FakeDriver : IKopiDriver, IKopiExecutor
    {
        private readonly Lock _lock = new();
        private readonly List<IReadOnlyList<JobSetStateParams>> _batches = [];
        private readonly List<DateTimeOffset> _callTimes = [];
        private int _calls;

        public int? MaxRowsPerStatement { get; init; }

        public int FailNext { get; set; }

        public Task? Gate { get; set; }

        public TimeProvider Clock { get; set; } = TimeProvider.System;

        public ConcurrentDictionary<long, JobState> StoredState { get; } = new();

        public int Calls => Volatile.Read(ref _calls);

        public IReadOnlyList<IReadOnlyList<JobSetStateParams>> Batches
        {
            get
            {
                lock (_lock)
                {
                    return [.. _batches];
                }
            }
        }

        public IReadOnlyList<DateTimeOffset> CallTimes
        {
            get
            {
                lock (_lock)
                {
                    return [.. _callTimes];
                }
            }
        }

        public DriverCapabilities Capabilities => new() { ListenNotify = false, BulkCopy = false, MaxRowsPerStatement = MaxRowsPerStatement };

        public IKopiExecutor GetExecutor() => this;

        public IKopiExecutor GetExecutor(DbTransaction transaction) => throw new NotSupportedException();

        public KopiQueue.Migrations.KopiMigrator CreateMigrator(Microsoft.Extensions.Logging.ILoggerFactory? loggerFactory = null) =>
            throw new NotSupportedException();

        public IKopiListener? CreateListener() => null;

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

        public async Task<IReadOnlyList<JobRow>> JobSetStateIfRunningManyAsync(IReadOnlyList<JobSetStateParams> jobs, CancellationToken cancellationToken)
        {
            Interlocked.Increment(ref _calls);
            lock (_lock)
            {
                _callTimes.Add(Clock.GetUtcNow());
            }

            if (Gate is { } gate)
            {
                await gate.WaitAsync(cancellationToken);
            }

            lock (_lock)
            {
                if (FailNext > 0)
                {
                    FailNext--;
                    throw new InvalidOperationException("database down");
                }

                _batches.Add([.. jobs]);
            }

            return [.. jobs.Where(j => j.Id < 1000).OrderBy(j => j.Id).Select(j =>
            {
                var row = Row(j.Id);
                return new JobRow
                {
                    Id = row.Id, Kind = row.Kind, Queue = row.Queue, State = StoredState.GetValueOrDefault(j.Id, j.State), Priority = row.Priority,
                    ArgsJson = row.ArgsJson, MetadataJson = row.MetadataJson, Tags = row.Tags, Attempt = row.Attempt, MaxAttempts = row.MaxAttempts,
                    CreatedAt = row.CreatedAt, ScheduledAt = row.ScheduledAt,
                };
            })];
        }

        public Task<IReadOnlyList<JobRow>> JobGetAvailableAsync(JobGetAvailableParams parameters, CancellationToken cancellationToken) =>
            throw new NotSupportedException();

        public Task<IReadOnlyList<JobInsertResult>> JobInsertManyAsync(IReadOnlyList<JobInsertParams> jobs, CancellationToken cancellationToken) =>
            throw new NotSupportedException();

        public Task<long> JobInsertFastAsync(IReadOnlyList<JobInsertParams> jobs, CancellationToken cancellationToken) =>
            throw new NotSupportedException();

        public Task<JobRow?> JobGetByIdAsync(long id, CancellationToken cancellationToken) => throw new NotSupportedException();
    }
}
