using KopiQueue.Drivers;
using Xunit;

namespace KopiQueue.TestInfrastructure.Integration;

/// <summary>
/// The scheduler on a real database (plan task 3.3: FR-SCH-01, FR-SCH-02, FR-MNT-02; design §5.7): only the leader
/// schedules, and a scheduled job runs soon after it is due.
/// </summary>
public abstract class SchedulerTests<TFixture>(TFixture db)
    where TFixture : IntegrationFixture
{
    /// <summary>How long a test waits for something to happen.</summary>
    protected static readonly TimeSpan Timeout = TimeSpan.FromSeconds(10);

    /// <summary>The driver's fixture.</summary>
    protected TFixture Fixture => db;

    /// <summary>The current test's cancellation token.</summary>
    protected static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Fact]
    public async Task A_follower_does_not_schedule_and_the_leader_does_as_soon_as_it_wins()
    {
        await using var schema = await db.CreateSchemaAsync(Ct);
        var driver = schema.Driver;
        await schema.SetLeaderAsync("someone-else", TimeSpan.FromHours(1), Ct);

        var job = (await driver.GetExecutor().JobInsertManyAsync([Due("sched_work", JobState.Scheduled, DateTimeOffset.UtcNow.AddMinutes(-1))], Ct)).Single().Job;
        var worker = new RecordingWorker();
        var client = await StartedAsync(driver, worker, "follower", TimeSpan.FromMilliseconds(200));
        try
        {
            // Many scheduler intervals and election attempts: the follower never promotes the job.
            await Task.Delay(TimeSpan.FromSeconds(1.5), Ct);
            Assert.Equal(JobState.Scheduled, (await driver.GetExecutor().JobGetByIdAsync(job.Id, Ct))!.State);
            Assert.Equal("someone-else", await schema.LeaderAsync(Ct));

            await schema.DeleteLeaderAsync(Ct);

            await worker.Ran.Task.WaitAsync(Timeout, Ct);
            Assert.Equal("follower", await schema.LeaderAsync(Ct));
        }
        finally
        {
            await client.StopAsync(Ct);
        }
    }

    [Fact]
    public async Task An_insert_only_client_does_not_schedule()
    {
        await using var schema = await db.CreateSchemaAsync(Ct);
        var driver = schema.Driver;
        var client = new KopiClient(driver, new KopiClientOptions { SchedulerInterval = TimeSpan.FromMilliseconds(100) });
        await client.StartAsync(Ct);
        try
        {
            var job = await client.InsertAsync(new ScheduledArgs(), new InsertOptions { Queue = "sched_work", ScheduledAt = DateTimeOffset.UtcNow.AddMilliseconds(200) }, Ct);
            Assert.Equal(JobState.Scheduled, job.Job.State);

            await Task.Delay(TimeSpan.FromSeconds(1), Ct);

            Assert.Equal(JobState.Scheduled, (await driver.GetExecutor().JobGetByIdAsync(job.Job.Id, Ct))!.State);
            Assert.Null(await schema.LeaderAsync(Ct));
        }
        finally
        {
            await client.StopAsync(Ct);
        }
    }

    [Fact]
    public async Task A_job_scheduled_a_few_seconds_ahead_runs_within_the_scheduler_interval_of_its_due_time()
    {
        await using var schema = await db.CreateSchemaAsync(Ct);
        var driver = schema.Driver;
        var interval = TimeSpan.FromSeconds(1);
        var worker = new RecordingWorker();
        var client = await StartedAsync(driver, worker, "on-time", interval);
        try
        {
            await schema.WaitForLeaderAsync(l => l is not null, Timeout, Ct);

            var dueAt = DateTimeOffset.UtcNow.AddSeconds(3);
            var job = await client.InsertAsync(new ScheduledArgs(), new InsertOptions { Queue = "sched_work", ScheduledAt = dueAt }, Ct);
            Assert.Equal(JobState.Scheduled, job.Job.State);

            var ranAt = await worker.Ran.Task.WaitAsync(Timeout, Ct);

            // The scheduler runs every interval ±10%; the notification then wakes the producer. Allow a little for the
            // fetch and for a loaded machine, but far less than the 1 s poll interval would add on its own twice over.
            var late = ranAt - dueAt;
            Assert.True(late >= TimeSpan.FromMilliseconds(-100), $"Ran {-late} before it was due.");
            Assert.True(late <= (interval * 1.1) + TimeSpan.FromMilliseconds(750), $"Ran {late} after it was due.");
        }
        finally
        {
            await client.StopAsync(Ct);
        }
    }

    /// <summary>A job of kind <c>scheduled_job</c> in <paramref name="queue"/>.</summary>
    protected static JobInsertParams Due(string queue, JobState state, DateTimeOffset scheduledAt) => new()
    {
        Kind = "scheduled_job",
        ArgsJson = "{}",
        Queue = queue,
        State = state,
        Priority = 1,
        MaxAttempts = 25,
        ScheduledAt = scheduledAt,
    };

    private static async Task<KopiClient> StartedAsync(IKopiDriver driver, RecordingWorker worker, string clientId, TimeSpan schedulerInterval)
    {
        var client = new KopiClient(driver, new KopiClientOptions
        {
            Queues = new Dictionary<string, QueueOptions> { ["sched_work"] = new() { MaxWorkers = 1 } },
            Workers = new WorkerRegistry().Add(worker),
            ClientId = clientId,
            ElectInterval = TimeSpan.FromMilliseconds(200),
            LeaderTtl = TimeSpan.FromSeconds(2),
            SchedulerInterval = schedulerInterval,
        });
        await client.StartAsync(Ct);
        return client;
    }

    [JobKind("scheduled_job")]
    public sealed record ScheduledArgs : IJobArgs;

    private sealed class RecordingWorker : Worker<ScheduledArgs>
    {
        public TaskCompletionSource<DateTimeOffset> Ran { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public override Task WorkAsync(Job<ScheduledArgs> job, CancellationToken cancellationToken)
        {
            Ran.TrySetResult(DateTimeOffset.UtcNow);
            return Task.CompletedTask;
        }
    }
}
