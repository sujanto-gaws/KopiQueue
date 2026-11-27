using System.Data.Common;
using System.Text.Json.Serialization;
using KopiQueue.Drivers;
using KopiQueue.Execution;
using KopiQueue.Serialization;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

namespace KopiQueue.Tests;

/// <summary>
/// The rescuer's decisions (plan task 3.4: FR-MNT-03, NFR-REL-02; design §5.7), with a fake driver whose clock is the
/// "database's": which candidates are stuck, how they are retried or discarded, and the option's validation and mapping.
/// </summary>
public sealed class RescuerTests
{
    private static readonly DateTimeOffset Now = new(2030, 1, 1, 12, 0, 0, TimeSpan.Zero);
    private static readonly TimeSpan Hour = TimeSpan.FromHours(1);
    private static readonly IJobArgsSerializer Serializer = new SystemTextJsonArgsSerializer(RescuerJsonContext.Default);

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Fact]
    public void The_rescuer_runs_every_minute_in_batches_of_1_000()
    {
        var rescuer = new Rescuer(new FakeDriver(), new RescuerOptions { RescueStuckJobsAfter = Hour });

        Assert.Equal(("rescuer", TimeSpan.FromMinutes(1), 1_000), (rescuer.Name, rescuer.Interval, rescuer.BatchSize));
    }

    [Fact]
    public async Task Candidates_are_listed_by_the_threshold_and_the_batch_size()
    {
        var driver = new FakeDriver();

        Assert.Equal(0, await Rescuer(driver).RunBatchAsync(Ct));

        Assert.Equal((Hour, 1_000), (driver.LastList!.StuckAfter, driver.LastList.Max));
        Assert.Null(driver.LastRescue); // nothing stuck: no rescue statement
    }

    [Fact]
    public async Task The_threshold_is_the_longer_of_RescueStuckJobsAfter_and_the_workers_timeout()
    {
        var driver = new FakeDriver
        {
            Candidates =
            [
                Running(1, "long_timeout", age: TimeSpan.FromHours(1.5)),  // its worker's 2 h timeout is not over yet
                Running(2, "long_timeout", age: TimeSpan.FromHours(2.5)),
                Running(3, "short_timeout", age: TimeSpan.FromHours(1.5)), // a 1 min timeout: RescueStuckJobsAfter applies
                Running(4, "short_timeout", age: TimeSpan.FromMinutes(59)), // listed by a driver with another clock: not stuck
            ],
        };

        var handled = await Rescuer(driver).RunBatchAsync(Ct);

        Assert.Equal(2, handled);
        Assert.Equal([2L, 3L], driver.LastRescue!.Select(r => r.Id));
        Assert.Contains("02:00:00", driver.LastRescue![0].Error.Error, StringComparison.Ordinal);
        Assert.Contains("01:00:00", driver.LastRescue[1].Error.Error, StringComparison.Ordinal);
    }

    [Fact]
    public async Task A_job_with_no_timeout_is_never_rescued_by_age()
    {
        var driver = new FakeDriver { Candidates = [Running(1, "no_timeout", age: TimeSpan.FromDays(30))] };
        var noClientTimeout = new FakeDriver { Candidates = [Running(2, "unknown_kind", age: TimeSpan.FromDays(30))] };

        Assert.Equal(0, await Rescuer(driver).RunBatchAsync(Ct));
        Assert.Equal(0, await Rescuer(noClientTimeout, jobTimeout: Timeout.InfiniteTimeSpan).RunBatchAsync(Ct));

        Assert.Null(driver.LastRescue);
        Assert.Null(noClientTimeout.LastRescue);
    }

    [Fact]
    public async Task A_job_with_attempts_left_is_retried_and_one_without_is_discarded_and_either_keeps_its_attempt()
    {
        var driver = new FakeDriver
        {
            Candidates = [Running(1, "short_timeout", age: 2 * Hour, attempt: 3, maxAttempts: 5), Running(2, "short_timeout", age: 2 * Hour, attempt: 5, maxAttempts: 5)],
        };
        var policy = new FixedPolicy(TimeSpan.FromMinutes(10));

        Assert.Equal(2, await Rescuer(driver, policy: policy).RunBatchAsync(Ct));

        var (retry, discard) = (driver.LastRescue![0], driver.LastRescue[1]);
        Assert.Equal((JobState.Retryable, Now + TimeSpan.FromMinutes(10), Now - (2 * Hour)), (retry.State, retry.ScheduledAt, retry.AttemptedAt));
        Assert.Equal((JobState.Discarded, (DateTimeOffset?)null), (discard.State, discard.ScheduledAt));
        Assert.Equal((Now, 3, (string?)null), (retry.Error.At, retry.Error.Attempt, retry.Error.Trace));
        Assert.Equal(5, discard.Error.Attempt);
        Assert.StartsWith("The job was still running", retry.Error.Error, StringComparison.Ordinal);
    }

    [Fact]
    public async Task The_workers_NextRetry_wins_over_the_retry_policy()
    {
        var driver = new FakeDriver { Candidates = [Running(1, "own_retry", age: 2 * Hour)] };

        await Rescuer(driver, policy: new FixedPolicy(TimeSpan.FromHours(5))).RunBatchAsync(Ct);

        Assert.Equal(Now + TimeSpan.FromMinutes(42), Assert.Single(driver.LastRescue!).ScheduledAt);
    }

    [Fact]
    public async Task A_retry_due_within_the_scheduler_interval_makes_the_job_available_at_once()
    {
        var driver = new FakeDriver { Candidates = [Running(1, "short_timeout", age: 2 * Hour), Running(2, "short_timeout", age: 2 * Hour)] };
        var policy = new ScriptedPolicy(job => job.Id == 1 ? TimeSpan.FromSeconds(5) : TimeSpan.FromSeconds(6));

        await Rescuer(driver, policy: policy).RunBatchAsync(Ct);

        Assert.Equal([JobState.Available, JobState.Retryable], driver.LastRescue!.Select(r => r.State));
        Assert.Equal(Now + TimeSpan.FromSeconds(5), driver.LastRescue![0].ScheduledAt);
    }

    [Fact]
    public async Task An_unknown_kind_or_unreadable_args_get_the_clients_timeout_and_policy_and_are_rescued_not_discarded()
    {
        var driver = new FakeDriver
        {
            Candidates =
            [
                Running(1, "unknown_kind", age: TimeSpan.FromMinutes(30)),     // past a 1 min job timeout, but not RescueStuckJobsAfter
                Running(2, "unknown_kind", age: TimeSpan.FromMinutes(90)),
                Running(3, "long_timeout", age: TimeSpan.FromMinutes(90), args: "not json"), // its 2 h timeout cannot be read
            ],
        };

        await Rescuer(driver, policy: new FixedPolicy(TimeSpan.FromMinutes(10))).RunBatchAsync(Ct);

        Assert.Equal([2L, 3L], driver.LastRescue!.Select(r => r.Id));
        Assert.All(driver.LastRescue!, r => Assert.Equal((JobState.Retryable, Now + TimeSpan.FromMinutes(10)), (r.State, r.ScheduledAt)));
    }

    [Fact]
    public async Task The_result_counts_the_jobs_the_guarded_rescue_changed()
    {
        var driver = new FakeDriver
        {
            Candidates = [Running(1, "short_timeout", age: 2 * Hour), Running(2, "short_timeout", age: 2 * Hour)],
            Changed = ids => [ids[1]], // job 1 finished meanwhile
        };

        Assert.Equal(1, await Rescuer(driver).RunBatchAsync(Ct));
    }

    [Fact]
    public async Task A_worker_resolved_from_DI_gets_a_scope_that_is_disposed()
    {
        var services = new ServiceCollection().AddScoped<ScopedDependency>().AddScoped<ScopedTimeoutWorker>().BuildServiceProvider();
        var driver = new FakeDriver { Candidates = [Running(1, "scoped_timeout", age: 2 * Hour)] };
        var rescuer = new Rescuer(driver, new RescuerOptions
        {
            RescueStuckJobsAfter = Hour,
            Workers = new WorkerRegistry().Add<ScopedArgs, ScopedTimeoutWorker>(),
            Serializer = Serializer,
            Services = services,
        });

        Assert.Equal(0, await rescuer.RunBatchAsync(Ct)); // the scoped worker's 3 h timeout applies

        Assert.Equal(1, ScopedDependency.Disposed);
    }

    [Fact]
    public void RescueStuckJobsAfter_defaults_to_JobTimeout_plus_an_hour()
    {
        Assert.Null(new KopiClientOptions().RescueStuckJobsAfter);
        Assert.Null(new KopiQueueOptions().RescueStuckJobsAfter);
        Assert.Equal(TimeSpan.FromMinutes(61), KopiClient.RescueStuckJobsAfter(new KopiClientOptions()));
        Assert.Equal(TimeSpan.FromMinutes(70), KopiClient.RescueStuckJobsAfter(new KopiClientOptions { JobTimeout = TimeSpan.FromMinutes(10) }));
        Assert.Equal(Hour, KopiClient.RescueStuckJobsAfter(new KopiClientOptions { JobTimeout = Timeout.InfiniteTimeSpan }));
        Assert.Equal(TimeSpan.FromMinutes(5), KopiClient.RescueStuckJobsAfter(new KopiClientOptions { RescueStuckJobsAfter = TimeSpan.FromMinutes(5) }));
    }

    [Theory]
    [InlineData(60, null, true)]       // default JobTimeout of 1 min
    [InlineData(61, null, false)]
    [InlineData(0, null, true)]
    [InlineData(-1, null, true)]
    [InlineData(1, -1, false)]         // an infinite JobTimeout: any positive value
    [InlineData(0, -1, true)]
    public void RescueStuckJobsAfter_must_be_greater_than_JobTimeout(int seconds, int? jobTimeoutMs, bool invalid)
    {
        var options = new KopiClientOptions
        {
            RescueStuckJobsAfter = TimeSpan.FromSeconds(seconds),
            JobTimeout = jobTimeoutMs is { } ms ? TimeSpan.FromMilliseconds(ms) : TimeSpan.FromMinutes(1),
        };

        var errors = KopiClient.ConfigurationErrors(options);

        Assert.Equal(invalid, errors.Any(e => e.Contains("RescueStuckJobsAfter", StringComparison.Ordinal)));
    }

    [Fact]
    public void The_leader_runs_the_rescuer_with_the_clients_threshold()
    {
        var services = KopiClient.MaintenanceServices(new FakeDriver(), new KopiClientOptions { JobTimeout = TimeSpan.FromMinutes(2) });

        var rescuer = Assert.IsType<Rescuer>(Assert.Single(services, s => s.Name == "rescuer"));

        Assert.Equal(TimeSpan.FromMinutes(1), rescuer.Interval);
    }

    [Fact]
    public void The_hosted_client_takes_RescueStuckJobsAfter_from_configuration()
    {
        var builder = Host.CreateEmptyApplicationBuilder(new HostApplicationBuilderSettings());
        builder.Configuration.AddInMemoryCollection(new Dictionary<string, string?> { ["KopiQueue:RescueStuckJobsAfter"] = "00:30:00" });
        builder.Services.AddKopiQueue(kq => kq.UseDriver(_ => new KopiClientLifecycleTests.IdleDriver())).BindConfiguration("KopiQueue");
        using var host = builder.Build();

        var client = host.Services.GetRequiredService<KopiClient>();

        Assert.Equal(TimeSpan.FromMinutes(30), client.Options.RescueStuckJobsAfter);
    }

    private static Rescuer Rescuer(FakeDriver driver, IRetryPolicy? policy = null, TimeSpan? jobTimeout = null) => new(driver, new RescuerOptions
    {
        RescueStuckJobsAfter = Hour,
        JobTimeout = jobTimeout ?? TimeSpan.FromMinutes(1),
        SchedulerInterval = TimeSpan.FromSeconds(5),
        RetryPolicy = policy ?? new FixedPolicy(TimeSpan.FromMinutes(1)),
        Workers = new WorkerRegistry()
            .Add(new LongTimeoutWorker())
            .Add(new ShortTimeoutWorker())
            .Add(new NoTimeoutWorker())
            .Add(new OwnRetryWorker()),
        Serializer = Serializer,
    });

    private static JobRow Running(long id, string kind, TimeSpan age, int attempt = 1, int maxAttempts = 25, string args = "{}") => new()
    {
        Id = id, Kind = kind, Queue = "default", State = JobState.Running, Priority = 1, ArgsJson = args, MetadataJson = "{}", Tags = [],
        Attempt = attempt, MaxAttempts = maxAttempts, CreatedAt = Now - age, ScheduledAt = Now - age, AttemptedAt = Now - age, AttemptedBy = ["dead-client"],
    };

    [JobKind("long_timeout")]
    public sealed record LongArgs : IJobArgs;

    [JobKind("short_timeout")]
    public sealed record ShortArgs : IJobArgs;

    [JobKind("no_timeout")]
    public sealed record NoTimeoutArgs : IJobArgs;

    [JobKind("own_retry")]
    public sealed record OwnRetryArgs : IJobArgs;

    [JobKind("scoped_timeout")]
    public sealed record ScopedArgs : IJobArgs;

    private sealed class LongTimeoutWorker : Worker<LongArgs>
    {
        public override TimeSpan? Timeout(Job<LongArgs> job) => TimeSpan.FromHours(2);

        public override Task WorkAsync(Job<LongArgs> job, CancellationToken cancellationToken) => Task.CompletedTask;
    }

    private sealed class ShortTimeoutWorker : Worker<ShortArgs>
    {
        public override Task WorkAsync(Job<ShortArgs> job, CancellationToken cancellationToken) => Task.CompletedTask;
    }

    private sealed class NoTimeoutWorker : Worker<NoTimeoutArgs>
    {
        public override TimeSpan? Timeout(Job<NoTimeoutArgs> job) => System.Threading.Timeout.InfiniteTimeSpan;

        public override Task WorkAsync(Job<NoTimeoutArgs> job, CancellationToken cancellationToken) => Task.CompletedTask;
    }

    private sealed class OwnRetryWorker : Worker<OwnRetryArgs>
    {
        public override TimeSpan? NextRetry(Job<OwnRetryArgs> job) => TimeSpan.FromMinutes(42);

        public override Task WorkAsync(Job<OwnRetryArgs> job, CancellationToken cancellationToken) => Task.CompletedTask;
    }

    private sealed class ScopedDependency : IDisposable
    {
        private static int _disposed;

        public static int Disposed => Volatile.Read(ref _disposed);

        public void Dispose() => Interlocked.Increment(ref _disposed);
    }

    private sealed class ScopedTimeoutWorker(ScopedDependency dependency) : Worker<ScopedArgs>
    {
        public override TimeSpan? Timeout(Job<ScopedArgs> job) => dependency is null ? null : TimeSpan.FromHours(3);

        public override Task WorkAsync(Job<ScopedArgs> job, CancellationToken cancellationToken) => Task.CompletedTask;
    }

    private sealed class FixedPolicy(TimeSpan delay) : IRetryPolicy
    {
        public DateTimeOffset NextRetry(JobRow job, DateTimeOffset now) => now + delay;
    }

    private sealed class ScriptedPolicy(Func<JobRow, TimeSpan> delay) : IRetryPolicy
    {
        public DateTimeOffset NextRetry(JobRow job, DateTimeOffset now) => now + delay(job);
    }

    /// <summary>A driver with only the rescuer's operations, judged by <see cref="Now"/>.</summary>
    private sealed class FakeDriver : IKopiDriver, IKopiExecutor
    {
        public IReadOnlyList<JobRow> Candidates { get; init; } = [];

        /// <summary>Which of the rescued ids the guarded update changes. Default: all.</summary>
        public Func<long[], long[]> Changed { get; init; } = ids => ids;

        public JobListStuckParams? LastList { get; private set; }

        public IReadOnlyList<JobRescueParams>? LastRescue { get; private set; }

        public DriverCapabilities Capabilities { get; } = new() { ListenNotify = false, BulkCopy = false };

        public IKopiExecutor GetExecutor() => this;

        public IKopiExecutor GetExecutor(DbTransaction transaction) => throw new NotSupportedException();

        public KopiQueue.Migrations.KopiMigrator CreateMigrator(Microsoft.Extensions.Logging.ILoggerFactory? loggerFactory = null) => throw new NotSupportedException();

        public IKopiListener? CreateListener() => null;

        public Task<JobListStuckResult> JobListStuckAsync(JobListStuckParams parameters, CancellationToken cancellationToken)
        {
            LastList = parameters;
            return Task.FromResult(new JobListStuckResult(Now, Candidates));
        }

        public Task<IReadOnlyList<JobRow>> JobRescueManyAsync(IReadOnlyList<JobRescueParams> jobs, CancellationToken cancellationToken)
        {
            LastRescue = jobs;
            var changed = Changed([.. jobs.Select(j => j.Id)]).ToHashSet();
            IReadOnlyList<JobRow> rows =
            [
                .. jobs.Where(j => changed.Contains(j.Id)).Select(j =>
                {
                    var job = Candidates.Single(c => c.Id == j.Id);
                    return new JobRow
                    {
                        Id = job.Id, Kind = job.Kind, Queue = job.Queue, State = j.State, Priority = job.Priority, ArgsJson = job.ArgsJson,
                        MetadataJson = job.MetadataJson, Tags = job.Tags, Attempt = job.Attempt, MaxAttempts = job.MaxAttempts,
                        CreatedAt = job.CreatedAt, ScheduledAt = j.ScheduledAt ?? job.ScheduledAt, AttemptedAt = job.AttemptedAt,
                        FinalizedAt = j.State == JobState.Discarded ? Now : null, Errors = [j.Error],
                    };
                }),
            ];
            return Task.FromResult(rows);
        }

        public Task<IReadOnlyList<long>> JobDeleteFinalizedAsync(JobDeleteFinalizedParams parameters, CancellationToken cancellationToken) =>
            throw new NotSupportedException();

        public Task<IReadOnlyList<JobRow>> JobListAsync(JobListQuery query, CancellationToken cancellationToken) =>
            throw new NotSupportedException();

        public Task<JobDeleteResult?> JobDeleteAsync(long id, CancellationToken cancellationToken) =>
            throw new NotSupportedException();

        public Task<IReadOnlyList<JobRow>> JobDeleteManyAsync(JobDeleteManyQuery query, CancellationToken cancellationToken) =>
            throw new NotSupportedException();

        public Task<JobRow?> JobRetryAsync(long id, CancellationToken cancellationToken) =>
            throw new NotSupportedException();

        public Task<JobRow?> JobCancelAsync(long id, CancellationToken cancellationToken) =>
            throw new NotSupportedException();

        public Task<IReadOnlyList<long>> JobListCancelAttemptedAsync(IReadOnlyList<long> ids, CancellationToken cancellationToken) =>
            throw new NotSupportedException();

        public Task<IReadOnlyList<JobInsertResult>> JobInsertManyAsync(IReadOnlyList<JobInsertParams> jobs, CancellationToken cancellationToken) => throw new NotSupportedException();

        public Task<long> JobInsertFastAsync(IReadOnlyList<JobInsertParams> jobs, CancellationToken cancellationToken) => throw new NotSupportedException();

        public Task<JobRow?> JobGetByIdAsync(long id, CancellationToken cancellationToken) => throw new NotSupportedException();

        public Task<IReadOnlyList<JobRow>> JobGetAvailableAsync(JobGetAvailableParams parameters, CancellationToken cancellationToken) => throw new NotSupportedException();

        public Task<IReadOnlyList<JobRow>> JobSetStateIfRunningManyAsync(IReadOnlyList<JobSetStateParams> jobs, CancellationToken cancellationToken) => throw new NotSupportedException();

        public Task<IReadOnlyList<JobScheduleResult>> JobScheduleAsync(JobScheduleParams parameters, CancellationToken cancellationToken) => throw new NotSupportedException();

        public Task<bool> LeaderAttemptElectAsync(LeaderElectParams parameters, CancellationToken cancellationToken) => throw new NotSupportedException();

        public Task<bool> LeaderAttemptReelectAsync(LeaderElectParams parameters, CancellationToken cancellationToken) => throw new NotSupportedException();

        public Task<bool> LeaderResignAsync(string leaderId, CancellationToken cancellationToken) => throw new NotSupportedException();

        public Task<IReadOnlyList<string>> QueueDeleteExpiredAsync(QueueDeleteExpiredParams parameters, CancellationToken cancellationToken) => throw new NotSupportedException();

        public Task<IReadOnlyList<QueueRow>> QueueCreateOrSetUpdatedAtManyAsync(IReadOnlyList<string> names, CancellationToken cancellationToken) => throw new NotSupportedException();

        public Task<QueueRow?> QueueGetAsync(string name, CancellationToken cancellationToken) => throw new NotSupportedException();

        public Task<IReadOnlyList<QueueRow>> QueueListAsync(QueueListQuery query, CancellationToken cancellationToken) => throw new NotSupportedException();

        public Task<bool> QueuePauseAsync(string name, CancellationToken cancellationToken) => throw new NotSupportedException();

        public Task<bool> QueueResumeAsync(string name, CancellationToken cancellationToken) => throw new NotSupportedException();

        public Task<QueueRow?> QueueUpdateAsync(QueueUpdateParams parameters, CancellationToken cancellationToken) => throw new NotSupportedException();
    }
}

[JsonSerializable(typeof(RescuerTests.LongArgs))]
[JsonSerializable(typeof(RescuerTests.ShortArgs))]
[JsonSerializable(typeof(RescuerTests.NoTimeoutArgs))]
[JsonSerializable(typeof(RescuerTests.OwnRetryArgs))]
[JsonSerializable(typeof(RescuerTests.ScopedArgs))]
internal sealed partial class RescuerJsonContext : JsonSerializerContext;
