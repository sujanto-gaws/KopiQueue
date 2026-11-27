using System.Collections.Concurrent;
using System.Data.Common;
using System.Threading.Channels;
using KopiQueue.Drivers;
using KopiQueue.Execution;
using Microsoft.Extensions.Time.Testing;

namespace KopiQueue.Tests;

/// <summary>
/// The queue maintainer and the producers' pause state (plan task 3.6: FR-QUE-02..04, FR-DB-05; design §3): queue rows
/// are created and kept fresh, a pause stops fetching and a resume fetches at once, by notification or by polling.
/// Time is a <see cref="FakeTimeProvider"/>: poll interval 1 s, refresh every minute.
/// </summary>
public sealed class QueueMaintainerTests
{
    private static readonly TimeSpan Step = TimeSpan.FromMilliseconds(100);

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Fact]
    public async Task Starting_marks_the_queues_worked_and_pauses_paused_ones_before_their_first_fetch()
    {
        var time = new FakeTimeProvider();
        var db = new FakeQueues(time);
        db.Paused["a"] = time.GetUtcNow();
        await using var client = await StartedAsync(db, time, ["a", "b"]);

        await Settle(time, TimeSpan.FromSeconds(3));

        Assert.Equal(["a", "b"], db.Refreshed.Single());
        Assert.True(client.Producer("a").IsPaused);
        Assert.Equal(0, db.Fetches("a"));
        Assert.True(db.Fetches("b") >= 3);
    }

    [Fact]
    public async Task A_pause_notification_stops_fetching_and_a_resume_fetches_at_once()
    {
        var time = new FakeTimeProvider();
        var db = new FakeQueues(time);
        await using var client = await StartedAsync(db, time, ["a", "b"]);
        await AdvanceUntil(time, () => db.Fetches("a") > 0);

        db.Paused["a"] = time.GetUtcNow(); // as a pause stores it, then notifies
        client.Queues.OnControlNotification("""{"action": "pause", "queue": "a"}""");
        Assert.True(client.Producer("a").IsPaused);
        var paused = db.Fetches("a");
        await Settle(time, TimeSpan.FromSeconds(3));
        Assert.Equal(paused, db.Fetches("a"));
        Assert.True(db.Fetches("b") > paused);

        db.Paused["a"] = null;
        client.Queues.OnControlNotification("""{"queue":"a","action":"resume"}""");
        await WaitFor(() => db.Fetches("a") > paused); // the clock stands still: the resume itself woke the producer
    }

    [Fact]
    public async Task All_queues_pause_for_a_star_and_other_messages_are_ignored()
    {
        var time = new FakeTimeProvider();
        var db = new FakeQueues(time);
        await using var client = await StartedAsync(db, time, ["a", "b"]);

        client.Queues.OnControlNotification("""{"action": "pause", "queue": "other"}""");
        client.Queues.OnControlNotification("""{"action": "cancel", "queue": "a", "job_id": 1}""");
        client.Queues.OnControlNotification("""{"action": "metadata_changed", "queue": "a", "metadata": {}}""");
        client.Queues.OnControlNotification("""{"action": "pause"}""");
        client.Queues.OnControlNotification("not json");
        client.Queues.OnControlNotification("[1]");
        Assert.False(client.Producer("a").IsPaused || client.Producer("b").IsPaused);

        client.Queues.OnControlNotification("""{"action": "pause", "queue": "*"}""");
        Assert.True(client.Producer("a").IsPaused && client.Producer("b").IsPaused);
        client.Queues.OnControlNotification("""{"action": "resume", "queue": "*"}""");
        Assert.False(client.Producer("a").IsPaused || client.Producer("b").IsPaused);
    }

    [Fact]
    public async Task A_cancel_notification_cancels_only_that_job_on_the_producer_that_runs_it()
    {
        var time = new FakeTimeProvider();
        var db = new FakeQueues(time);
        db.Give("a", 1, 2);
        db.Give("b", 3);
        var running = new Running();
        await using var client = await StartedAsync(db, time, ["a", "b"], run: running.RunAsync);
        await AdvanceUntil(time, () => running.Started.Count == 3);

        client.Queues.OnControlNotification("""{"action": "cancel", "job_id": 1, "queue": "a"}""");
        client.Queues.OnControlNotification("""{"action": "cancel", "job_id": 3, "queue": "a"}"""); // not that queue's job
        client.Queues.OnControlNotification("""{"action": "cancel", "job_id": 99, "queue": "b"}"""); // another client's job
        client.Queues.OnControlNotification("""{"action": "cancel", "job_id": "2", "queue": "a"}""");
        client.Queues.OnControlNotification("""{"action": "cancel", "queue": "a"}""");
        await WaitFor(() => running.Cancelled.Contains(1));
        await Task.Delay(50, Ct);
        Assert.Equal([1L], running.Cancelled.Order());

        client.Queues.OnControlNotification("""{"queue": "b", "job_id": 3, "action": "cancel"}""");
        await WaitFor(() => running.Cancelled.Contains(3));
        Assert.False(client.Producer("a").IsPaused || client.Producer("b").IsPaused); // a cancel is not a pause
    }

    [Fact]
    public async Task Without_notifications_a_cancel_is_read_within_the_poll_interval_for_this_clients_running_jobs()
    {
        var time = new FakeTimeProvider();
        var db = new FakeQueues(time);
        db.Give("a", 1, 2);
        var running = new Running();
        await using var client = await StartedAsync(db, time, ["a"], run: running.RunAsync);
        await AdvanceUntil(time, () => running.Started.Count == 2);
        await Settle(time, TimeSpan.FromSeconds(2));
        Assert.Empty(running.Cancelled);

        db.CancelAttempted[2] = 0; // another client cancelled it; nobody notifies
        db.CancelAttempted[7] = 0; // a job that runs elsewhere
        var waited = await AdvanceUntil(time, () => running.Cancelled.Contains(2));

        Assert.True(waited <= TimeSpan.FromSeconds(1) + Step, $"took {waited}");
        Assert.Equal([1L, 2L], db.LastCancelRead!.Order()); // only its own running jobs are asked about
        Assert.Equal([2L], running.Cancelled);
    }

    [Fact]
    public async Task With_notifications_connected_running_jobs_are_not_polled_for_cancels()
    {
        var time = new FakeTimeProvider();
        var db = new FakeQueues(time);
        db.Give("a", 1);
        var connection = new FakeConnection();
        await using var notifier = new Notifier(new FakeListener(connection), timeProvider: time);
        await notifier.StartAsync(Ct);
        await WaitFor(() => notifier.IsConnected);
        var running = new Running();
        await using var client = await StartedAsync(db, time, ["a"], notifier, running.RunAsync);
        await AdvanceUntil(time, () => running.Started.Count == 1);
        var readsAtStart = db.CancelReads; // at most the catch-up read after connecting
        db.CancelAttempted[1] = 0;

        await Settle(time, TimeSpan.FromSeconds(3));

        Assert.Empty(running.Cancelled);
        Assert.Equal(readsAtStart, db.CancelReads);
    }

    [Fact]
    public async Task Without_notifications_a_pause_is_read_within_the_poll_interval()
    {
        var time = new FakeTimeProvider();
        var db = new FakeQueues(time);
        await using var client = await StartedAsync(db, time, ["a"]);
        await Settle(time, TimeSpan.FromSeconds(2));

        db.Paused["a"] = time.GetUtcNow(); // another client paused it; nobody notifies
        var waited = await AdvanceUntil(time, () => client.Producer("a").IsPaused);
        Assert.True(waited <= TimeSpan.FromSeconds(1) + Step, $"took {waited}");

        db.Paused["a"] = null;
        waited = await AdvanceUntil(time, () => !client.Producer("a").IsPaused);
        Assert.True(waited <= TimeSpan.FromSeconds(1) + Step, $"took {waited}");
    }

    [Fact]
    public async Task The_queues_are_marked_worked_every_minute()
    {
        var time = new FakeTimeProvider();
        var db = new FakeQueues(time);
        await using var client = await StartedAsync(db, time, ["a"]);

        await Settle(time, TimeSpan.FromSeconds(59));
        Assert.Single(db.Refreshed);
        await Settle(time, TimeSpan.FromSeconds(2));
        Assert.Equal(2, db.Refreshed.Count);
    }

    [Fact]
    public async Task With_notifications_connected_it_does_not_poll()
    {
        var time = new FakeTimeProvider();
        var db = new FakeQueues(time);
        var connection = new FakeConnection();
        await using var notifier = new Notifier(new FakeListener(connection), timeProvider: time);
        await notifier.StartAndResyncAsync(Ct);
        await using var client = await StartedAsync(db, time, ["a"], notifier);

        await Settle(time, TimeSpan.FromSeconds(5));
        Assert.Equal(0, db.Polls);

        connection.Send(NotificationTopic.Control, """{"action": "pause", "queue": "a"}""");
        await WaitFor(() => client.Producer("a").IsPaused);
    }

    [Fact]
    public async Task A_read_that_a_notification_overtook_is_read_again()
    {
        var time = new FakeTimeProvider();
        var db = new FakeQueues(time);
        db.Paused["a"] = time.GetUtcNow();
        await using var client = await StartedAsync(db, time, ["a"]);
        Assert.True(client.Producer("a").IsPaused);

        var gate = new TaskCompletionSource();
        db.PollGate = gate.Task;
        var polls = db.Polls;
        await AdvanceUntil(time, () => db.Polls > polls); // a poll has read "paused" and waits

        db.Paused["a"] = null;
        db.PollGate = null;
        client.Queues.OnControlNotification("""{"action": "resume", "queue": "a"}""");
        Assert.False(client.Producer("a").IsPaused);
        gate.SetResult(); // the stale poll returns "paused"

        await WaitFor(() => db.Polls > polls + 1); // and was read again at once, without the clock moving
        await Task.Delay(50, Ct);
        Assert.False(client.Producer("a").IsPaused);
    }

    [Fact]
    public async Task A_local_pause_and_a_pause_everywhere_are_independent()
    {
        var time = new FakeTimeProvider();
        var db = new FakeQueues(time);
        await using var client = await StartedAsync(db, time, ["a"]);
        var producer = client.Producer("a");

        Assert.True(client.Queues.Apply("a", paused: true, locally: true));
        Assert.True(producer.IsPaused);
        Assert.True(client.Queues.Apply("a", paused: false, locally: false));
        Assert.True(producer.IsPaused); // still paused here

        Assert.True(client.Queues.Apply("a", paused: true, locally: false));
        Assert.True(client.Queues.Apply("a", paused: false, locally: true));
        Assert.True(producer.IsPaused); // still paused everywhere
        Assert.True(client.Queues.Apply("*", paused: false, locally: false));
        Assert.False(producer.IsPaused);

        Assert.False(client.Queues.Apply("elsewhere", paused: true, locally: true));
    }

    [Fact]
    public async Task A_failed_first_refresh_is_retried_at_the_next_poll()
    {
        var time = new FakeTimeProvider();
        var db = new FakeQueues(time) { Down = true };
        db.Paused["a"] = time.GetUtcNow();
        await using var client = await StartedAsync(db, time, ["a"]);
        Assert.Empty(db.Refreshed);
        Assert.False(client.Producer("a").IsPaused);

        db.Down = false;
        await AdvanceUntil(time, () => client.Producer("a").IsPaused);
        Assert.Single(db.Refreshed);
    }

    private static async Task<Working> StartedAsync(
        FakeQueues db,
        FakeTimeProvider time,
        string[] queues,
        Notifier? notifier = null,
        Func<JobRow, CancellationToken, CancellationToken, Task>? run = null)
    {
        var producers = queues
            .Select(q => new Producer(
                db,
                new ProducerOptions { Queue = q, MaxWorkers = 5, ClientId = "client" },
                run ?? ((_, _, _) => Task.CompletedTask),
                timeProvider: time))
            .ToArray();
        var maintainer = new QueueMaintainer(db, producers, new QueueMaintainerOptions(), notifier, time);
        await maintainer.StartAsync(Ct);
        foreach (var producer in producers)
        {
            await producer.StartAsync(Ct);
        }

        return new Working(maintainer, producers);
    }

    /// <summary>Waits, on the real clock, for something that needs no fake time to pass.</summary>
    private static async Task WaitFor(Func<bool> condition)
    {
        var limit = DateTime.UtcNow.AddSeconds(10);
        while (!condition())
        {
            Assert.True(DateTime.UtcNow < limit, "The condition was not met within 10 s.");
            await Task.Delay(5, Ct);
        }
    }

    /// <summary>Advances fake time in steps until <paramref name="condition"/> holds, and returns how much passed.</summary>
    private static async Task<TimeSpan> AdvanceUntil(FakeTimeProvider time, Func<bool> condition)
    {
        var start = time.GetUtcNow();
        var limit = DateTime.UtcNow.AddSeconds(20);
        while (!condition())
        {
            Assert.True(DateTime.UtcNow < limit, $"The condition was not met within 20 s; fake time advanced {time.GetUtcNow() - start}.");
            await Task.Delay(5, Ct);
            if (!condition())
            {
                time.Advance(Step);
            }
        }

        return time.GetUtcNow() - start;
    }

    /// <summary>Advances fake time by <paramref name="duration"/> in steps, letting the loops run in between.</summary>
    private static async Task Settle(FakeTimeProvider time, TimeSpan duration)
    {
        for (var passed = TimeSpan.Zero; passed < duration; passed += Step)
        {
            await Task.Delay(5, Ct);
            time.Advance(Step);
        }

        await Task.Delay(20, Ct);
    }

    /// <summary>A run function whose jobs wait for their cancel token (or the hard stop) and record which were cancelled.</summary>
    private sealed class Running
    {
        public ConcurrentBag<long> Started { get; } = [];

        public ConcurrentBag<long> Cancelled { get; } = [];

        public async Task RunAsync(JobRow job, CancellationToken hardStop, CancellationToken cancel)
        {
            Started.Add(job.Id);
            using var either = CancellationTokenSource.CreateLinkedTokenSource(hardStop, cancel);
            await Task.Delay(Timeout.Infinite, either.Token).ContinueWith(_ => { }, TaskScheduler.Default);
            if (cancel.IsCancellationRequested)
            {
                Cancelled.Add(job.Id);
            }
        }
    }

    private sealed class Working(QueueMaintainer queues, Producer[] producers) : IAsyncDisposable
    {
        public QueueMaintainer Queues { get; } = queues;

        public Producer Producer(string queue) => producers.Single(p => p.Queue == queue);

        public async ValueTask DisposeAsync()
        {
            foreach (var producer in producers)
            {
                await producer.DisposeAsync();
            }

            await Queues.DisposeAsync();
        }
    }

    /// <summary>Queue rows (only their pause state matters here) and how often each operation ran.</summary>
    private sealed class FakeQueues(TimeProvider time) : IKopiDriver, IKopiExecutor
    {
        private readonly ConcurrentDictionary<string, int> _fetches = new();
        private readonly ConcurrentDictionary<string, ConcurrentQueue<long>> _jobs = new();
        private int _polls;
        private int _cancelReads;

        /// <summary>Running jobs whose <c>cancel_attempted_at</c> is set.</summary>
        public ConcurrentDictionary<long, byte> CancelAttempted { get; } = new();

        /// <summary>How many times running jobs were asked about.</summary>
        public int CancelReads => Volatile.Read(ref _cancelReads);

        /// <summary>The IDs of the last ask.</summary>
        public IReadOnlyList<long>? LastCancelRead { get; private set; }

        /// <summary>Jobs that the next fetches of <paramref name="queue"/> hand out, once each.</summary>
        public void Give(string queue, params long[] ids)
        {
            var pending = _jobs.GetOrAdd(queue, _ => new ConcurrentQueue<long>());
            foreach (var id in ids)
            {
                pending.Enqueue(id);
            }
        }

        public ConcurrentDictionary<string, DateTimeOffset?> Paused { get; } = new();

        public ConcurrentQueue<string[]> Refreshed { get; } = new();

        public int Polls => Volatile.Read(ref _polls);

        public volatile bool Down;

        /// <summary>When set, a poll reads the rows, then waits for it before returning them.</summary>
        public Task? PollGate { get; set; }

        public DriverCapabilities Capabilities { get; } = new() { ListenNotify = true, BulkCopy = false };

        public int Fetches(string queue) => _fetches.GetValueOrDefault(queue);

        public Task<IReadOnlyList<QueueRow>> QueueCreateOrSetUpdatedAtManyAsync(IReadOnlyList<string> names, CancellationToken cancellationToken)
        {
            ThrowIfDown();
            Refreshed.Enqueue([.. names]);
            return Task.FromResult<IReadOnlyList<QueueRow>>([.. names.Select(Row)]);
        }

        public async Task<IReadOnlyList<QueueRow>> QueueListAsync(QueueListQuery query, CancellationToken cancellationToken)
        {
            ThrowIfDown();
            IReadOnlyList<QueueRow> rows = [.. query.Names!.Select(Row)];
            var gate = PollGate;
            Interlocked.Increment(ref _polls);
            if (gate is not null)
            {
                await gate.WaitAsync(cancellationToken);
            }

            return rows;
        }

        public Task<IReadOnlyList<JobRow>> JobGetAvailableAsync(JobGetAvailableParams parameters, CancellationToken cancellationToken)
        {
            _fetches.AddOrUpdate(parameters.Queue, 1, (_, n) => n + 1);
            var jobs = new List<JobRow>();
            if (_jobs.TryGetValue(parameters.Queue, out var pending))
            {
                while (jobs.Count < parameters.Max && pending.TryDequeue(out var id))
                {
                    jobs.Add(new JobRow
                    {
                        Id = id, Kind = "k", Queue = parameters.Queue, State = JobState.Running, Priority = 1, ArgsJson = "{}", MetadataJson = "{}",
                        Tags = [], Attempt = 1, MaxAttempts = 25, CreatedAt = time.GetUtcNow(), ScheduledAt = time.GetUtcNow(), AttemptedAt = time.GetUtcNow(),
                    });
                }
            }

            return Task.FromResult<IReadOnlyList<JobRow>>(jobs);
        }

        public IKopiExecutor GetExecutor() => this;

        public IKopiExecutor GetExecutor(DbTransaction transaction) => throw new NotSupportedException();

        public KopiQueue.Migrations.KopiMigrator CreateMigrator(Microsoft.Extensions.Logging.ILoggerFactory? loggerFactory = null) =>
            throw new NotSupportedException();

        public IKopiListener? CreateListener() => null;

        public Task<IReadOnlyList<JobInsertResult>> JobInsertManyAsync(IReadOnlyList<JobInsertParams> jobs, CancellationToken cancellationToken) =>
            throw new NotSupportedException();

        public Task<long> JobInsertFastAsync(IReadOnlyList<JobInsertParams> jobs, CancellationToken cancellationToken) =>
            throw new NotSupportedException();

        public Task<JobRow?> JobGetByIdAsync(long id, CancellationToken cancellationToken) => throw new NotSupportedException();

        public Task<IReadOnlyList<JobRow>> JobSetStateIfRunningManyAsync(IReadOnlyList<JobSetStateParams> jobs, CancellationToken cancellationToken) =>
            throw new NotSupportedException();

        public Task<bool> LeaderAttemptElectAsync(LeaderElectParams parameters, CancellationToken cancellationToken) => Task.FromResult(false);

        public Task<bool> LeaderAttemptReelectAsync(LeaderElectParams parameters, CancellationToken cancellationToken) => Task.FromResult(false);

        public Task<bool> LeaderResignAsync(string leaderId, CancellationToken cancellationToken) => Task.FromResult(false);

        public Task<QueueRow?> QueueGetAsync(string name, CancellationToken cancellationToken) => throw new NotSupportedException();

        public Task<bool> QueuePauseAsync(string name, CancellationToken cancellationToken) => throw new NotSupportedException();

        public Task<bool> QueueResumeAsync(string name, CancellationToken cancellationToken) => throw new NotSupportedException();

        public Task<QueueRow?> QueueUpdateAsync(QueueUpdateParams parameters, CancellationToken cancellationToken) => throw new NotSupportedException();

        public Task<IReadOnlyList<string>> QueueDeleteExpiredAsync(QueueDeleteExpiredParams parameters, CancellationToken cancellationToken) =>
            throw new NotSupportedException();

        public Task<IReadOnlyList<JobScheduleResult>> JobScheduleAsync(JobScheduleParams parameters, CancellationToken cancellationToken) =>
            throw new NotSupportedException();

        public Task<JobListStuckResult> JobListStuckAsync(JobListStuckParams parameters, CancellationToken cancellationToken) =>
            Task.FromResult(new JobListStuckResult(DateTimeOffset.UtcNow, []));

        public Task<IReadOnlyList<JobRow>> JobRescueManyAsync(IReadOnlyList<JobRescueParams> jobs, CancellationToken cancellationToken) =>
            Task.FromResult<IReadOnlyList<JobRow>>([]);

        public Task<IReadOnlyList<long>> JobDeleteFinalizedAsync(JobDeleteFinalizedParams parameters, CancellationToken cancellationToken) =>
            Task.FromResult<IReadOnlyList<long>>([]);

        public Task<JobRow?> JobCancelAsync(long id, CancellationToken cancellationToken) =>
            Task.FromResult<JobRow?>(null);

        public Task<IReadOnlyList<long>> JobListCancelAttemptedAsync(IReadOnlyList<long> ids, CancellationToken cancellationToken)
        {
            ThrowIfDown();
            Interlocked.Increment(ref _cancelReads);
            LastCancelRead = [.. ids];
            return Task.FromResult<IReadOnlyList<long>>([.. ids.Where(CancelAttempted.ContainsKey).Order()]);
        }

        private QueueRow Row(string name) => new()
        {
            Name = name,
            CreatedAt = time.GetUtcNow(),
            UpdatedAt = time.GetUtcNow(),
            PausedAt = Paused.GetValueOrDefault(name),
            MetadataJson = "{}",
        };

        private void ThrowIfDown()
        {
            if (Down)
            {
                throw new DbExceptionStub("the database is down");
            }
        }
    }

    private sealed class DbExceptionStub(string message) : DbException(message);

    private sealed class FakeListener(FakeConnection connection) : IKopiListener
    {
        private int _connects;

        public async Task<IKopiListenerConnection> ConnectAsync(CancellationToken cancellationToken)
        {
            if (Interlocked.Increment(ref _connects) > 1)
            {
                await Task.Delay(Timeout.Infinite, cancellationToken);
            }

            return connection;
        }
    }

    private sealed class FakeConnection : IKopiListenerConnection
    {
        private readonly Channel<KopiNotification> _items = Channel.CreateUnbounded<KopiNotification>();

        public void Send(NotificationTopic topic, string payload) => _items.Writer.TryWrite(new KopiNotification(topic, payload));

        public async Task<KopiNotification> WaitForNotificationAsync(CancellationToken cancellationToken) =>
            await _items.Reader.ReadAsync(cancellationToken);

        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }
}
