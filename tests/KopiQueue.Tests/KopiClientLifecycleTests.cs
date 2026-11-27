using System.Data.Common;
using KopiQueue.Drivers;

namespace KopiQueue.Tests;

/// <summary>Starting and stopping a client, and checking its configuration (plan task 2.7: FR-HST-03, FR-DEF-06).</summary>
public sealed class KopiClientLifecycleTests
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Fact]
    public async Task An_insert_only_client_starts_nothing_and_never_touches_the_database()
    {
        var driver = new IdleDriver();
        var client = new KopiClient(driver);

        await client.StartAsync(Ct);
        await Task.Delay(100, Ct);
        await client.StopAsync(Ct);

        Assert.False(client.IsWorking);
        Assert.Equal(0, driver.Fetches);
        Assert.Equal(0, driver.ListenerRequests);
    }

    [Fact]
    public async Task A_working_client_fetches_until_stopped_and_stopping_twice_is_harmless()
    {
        var driver = new IdleDriver();
        var client = new KopiClient(driver, Working());

        await client.StartAsync(Ct);
        await Until(() => driver.Fetches > 0);
        var working = client.IsWorking;
        await client.StopAsync(Ct);
        await client.StopAsync(Ct);
        var fetches = driver.Fetches;
        await Task.Delay(200, Ct);

        Assert.True(working);
        Assert.False(client.IsWorking);
        Assert.Equal(fetches, driver.Fetches);
        Assert.Equal(["a", "b"], driver.Queues.Order());
    }

    [Fact]
    public async Task A_client_starts_once()
    {
        var client = new KopiClient(new IdleDriver(), Working());
        await client.StartAsync(Ct);
        await client.StopAsync(Ct);

        await Assert.ThrowsAsync<InvalidOperationException>(() => client.StartAsync(Ct));
    }

    [Fact]
    public async Task Stopping_a_client_that_never_started_is_harmless() =>
        await new KopiClient(new IdleDriver(), Working()).StopAsync(Ct);

    [Fact]
    public void The_client_id_defaults_to_machine_process_and_a_random_suffix()
    {
        var a = new KopiClient(new IdleDriver()).ClientId;
        var b = new KopiClient(new IdleDriver()).ClientId;

        Assert.StartsWith($"{Environment.MachineName}-{Environment.ProcessId}-", a, StringComparison.Ordinal);
        Assert.NotEqual(a, b);
        Assert.Equal("worker-7", new KopiClient(new IdleDriver(), new KopiClientOptions { ClientId = "worker-7" }).ClientId);
    }

    public static TheoryData<string, KopiClientOptions> InvalidConfigurations() => new()
    {
        { "no queues", new KopiClientOptions { Workers = Workers() } },
        { "no workers", new KopiClientOptions { Queues = Queue("a", 1) } },
        { "an empty registry", new KopiClientOptions { Queues = Queue("a", 1), Workers = new WorkerRegistry() } },
        { "MaxWorkers 0", new KopiClientOptions { Queues = Queue("a", 0), Workers = Workers() } },
        { "MaxWorkers 10,001", new KopiClientOptions { Queues = Queue("a", 10_001), Workers = Workers() } },
        { "a bad queue name", new KopiClientOptions { Queues = Queue("Not A Queue!", 1), Workers = Workers() } },
        { "a zero job timeout", new KopiClientOptions { Queues = TwoQueues(), Workers = Workers(), JobTimeout = TimeSpan.Zero } },
        { "a zero poll interval", new KopiClientOptions { Queues = TwoQueues(), Workers = Workers(), FetchPollInterval = TimeSpan.Zero } },
        { "a negative cooldown", new KopiClientOptions { Queues = TwoQueues(), Workers = Workers(), FetchCooldown = TimeSpan.FromSeconds(-1) } },
        { "a zero batch", new KopiClientOptions { Queues = TwoQueues(), Workers = Workers(), FetchBatchMax = 0 } },
        { "a negative soft stop", new KopiClientOptions { Queues = TwoQueues(), Workers = Workers(), SoftStopTimeout = TimeSpan.FromSeconds(-1) } },
        { "a blank client id", new KopiClientOptions { Queues = TwoQueues(), Workers = Workers(), ClientId = " " } },
        { "a client id the leader lease cannot store", new KopiClientOptions { Queues = TwoQueues(), Workers = Workers(), ClientId = new string('c', 128) } },
        { "a zero elect interval", new KopiClientOptions { Queues = TwoQueues(), Workers = Workers(), ElectInterval = TimeSpan.Zero } },
        { "a lease shorter than two intervals", new KopiClientOptions { Queues = TwoQueues(), Workers = Workers(), LeaderTtl = TimeSpan.FromSeconds(9) } },
        { "a zero queue retention", new KopiClientOptions { Queues = TwoQueues(), Workers = Workers(), QueueRetention = TimeSpan.Zero } },
        { "a queue retention under 5 minutes", new KopiClientOptions { Queues = TwoQueues(), Workers = Workers(), QueueRetention = TimeSpan.FromMinutes(4) } },
        { "a zero scheduler interval", new KopiClientOptions { Queues = TwoQueues(), Workers = Workers(), SchedulerInterval = TimeSpan.Zero } },
        { "a negative scheduler interval", new KopiClientOptions { Queues = TwoQueues(), Workers = Workers(), SchedulerInterval = TimeSpan.FromSeconds(-1) } },
        { "ListenNotify on a driver that cannot listen", new KopiClientOptions { Queues = TwoQueues(), Workers = Workers(), NotificationMode = NotificationMode.ListenNotify } },
    };

    [Theory]
    [MemberData(nameof(InvalidConfigurations))]
    public async Task An_invalid_configuration_fails_start_and_starts_nothing(string what, KopiClientOptions options)
    {
        var driver = new IdleDriver();
        var client = new KopiClient(driver, options);

        var error = await Assert.ThrowsAsync<KopiQueueConfigurationException>(() => client.StartAsync(Ct));

        Assert.False(string.IsNullOrWhiteSpace(error.Message), what);
        await Task.Delay(50, Ct);
        Assert.Equal(0, driver.Fetches);
        Assert.False(client.IsWorking);
    }

    [Fact]
    public async Task Poll_only_never_asks_the_driver_for_a_listener()
    {
        var driver = new IdleDriver();
        var client = new KopiClient(driver, new KopiClientOptions { Queues = TwoQueues(), Workers = Workers(), NotificationMode = NotificationMode.PollOnly });

        await client.StartAsync(Ct);
        await client.StopAsync(Ct);

        Assert.Equal(0, driver.ListenerRequests);
    }

    private static KopiClientOptions Working() => new()
    {
        Queues = TwoQueues(),
        Workers = Workers(),
        FetchPollInterval = TimeSpan.FromMilliseconds(20),
    };

    private static Dictionary<string, QueueOptions> TwoQueues() =>
        new() { ["a"] = new() { MaxWorkers = 2 }, ["b"] = new() { MaxWorkers = 1 } };

    private static Dictionary<string, QueueOptions> Queue(string name, int maxWorkers) => new() { [name] = new() { MaxWorkers = maxWorkers } };

    private static WorkerRegistry Workers() => new WorkerRegistry().Add(new NoopWorker());

    private static async Task Until(Func<bool> condition)
    {
        var limit = DateTime.UtcNow.AddSeconds(10);
        while (!condition())
        {
            Assert.True(DateTime.UtcNow < limit, "The condition was not met within 10 s.");
            await Task.Delay(10, Ct);
        }
    }

    [JobKind("lifecycle_noop")]
    internal sealed record NoopArgs : IJobArgs;

    internal sealed class NoopWorker : Worker<NoopArgs>
    {
        public override Task WorkAsync(Job<NoopArgs> job, CancellationToken cancellationToken) => Task.CompletedTask;
    }

    /// <summary>A database with no jobs and no listener.</summary>
    internal sealed class IdleDriver : IKopiDriver, IKopiExecutor
    {
        private int _fetches;
        private int _listenerRequests;

        public int Fetches => Volatile.Read(ref _fetches);

        public int ListenerRequests => Volatile.Read(ref _listenerRequests);

        public System.Collections.Concurrent.ConcurrentDictionary<string, byte> QueueSet { get; } = new();

        public IEnumerable<string> Queues => QueueSet.Keys;

        public DriverCapabilities Capabilities { get; } = new() { ListenNotify = false, BulkCopy = false };

        public IKopiExecutor GetExecutor() => this;

        public IKopiExecutor GetExecutor(DbTransaction transaction) => throw new NotSupportedException();

        public KopiQueue.Migrations.KopiMigrator CreateMigrator(Microsoft.Extensions.Logging.ILoggerFactory? loggerFactory = null) =>
            throw new NotSupportedException();

        public IKopiListener? CreateListener()
        {
            Interlocked.Increment(ref _listenerRequests);
            return null;
        }

        public Task<IReadOnlyList<JobRow>> JobGetAvailableAsync(JobGetAvailableParams parameters, CancellationToken cancellationToken)
        {
            Interlocked.Increment(ref _fetches);
            QueueSet.TryAdd(parameters.Queue, 0);
            return Task.FromResult<IReadOnlyList<JobRow>>([]);
        }

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

        /// <summary>The parameters of the last scheduler batch.</summary>
        public JobScheduleParams? LastSchedule { get; private set; }

        public Task<IReadOnlyList<JobScheduleResult>> JobScheduleAsync(JobScheduleParams parameters, CancellationToken cancellationToken)
        {
            LastSchedule = parameters;
            return Task.FromResult<IReadOnlyList<JobScheduleResult>>([new JobScheduleResult(1, "default", JobState.Available)]);
        }

        public Task<JobListStuckResult> JobListStuckAsync(JobListStuckParams parameters, CancellationToken cancellationToken) =>
            Task.FromResult(new JobListStuckResult(DateTimeOffset.UtcNow, []));

        public Task<IReadOnlyList<JobRow>> JobRescueManyAsync(IReadOnlyList<JobRescueParams> jobs, CancellationToken cancellationToken) =>
            Task.FromResult<IReadOnlyList<JobRow>>([]);

        /// <summary>The job cleaner's last call, for <c>JobCleanerTests</c>.</summary>
        public JobDeleteFinalizedParams? LastDeleteFinalized { get; private set; }

        /// <summary>The IDs the job cleaner's call "deletes". Default: none.</summary>
        public IReadOnlyList<long> DeletedFinalized { get; init; } = [];

        public Task<IReadOnlyList<long>> JobDeleteFinalizedAsync(JobDeleteFinalizedParams parameters, CancellationToken cancellationToken)
        {
            LastDeleteFinalized = parameters;
            return Task.FromResult(DeletedFinalized);
        }

        public Task<JobRow?> JobCancelAsync(long id, CancellationToken cancellationToken) =>
            Task.FromResult<JobRow?>(null);

        public Task<IReadOnlyList<long>> JobListCancelAttemptedAsync(IReadOnlyList<long> ids, CancellationToken cancellationToken) =>
            Task.FromResult<IReadOnlyList<long>>([]);

        public Task<IReadOnlyList<JobRow>> JobSetStateIfRunningManyAsync(IReadOnlyList<JobSetStateParams> jobs, CancellationToken cancellationToken) =>
            Task.FromResult<IReadOnlyList<JobRow>>([]);

        public Task<IReadOnlyList<JobInsertResult>> JobInsertManyAsync(IReadOnlyList<JobInsertParams> jobs, CancellationToken cancellationToken) =>
            throw new NotSupportedException();

        public Task<long> JobInsertFastAsync(IReadOnlyList<JobInsertParams> jobs, CancellationToken cancellationToken) =>
            throw new NotSupportedException();

        public Task<JobRow?> JobGetByIdAsync(long id, CancellationToken cancellationToken) => throw new NotSupportedException();
    }
}
