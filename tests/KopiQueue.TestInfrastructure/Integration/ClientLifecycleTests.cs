using KopiQueue.Drivers;
using Xunit;

namespace KopiQueue.TestInfrastructure.Integration;

/// <summary>A working <see cref="KopiClient"/> on a real database: start, pick-up, soft and hard stop (plan task 2.7: FR-HST-03, FR-EXE-03).</summary>
public abstract class ClientLifecycleTests<TFixture>(TFixture db)
    where TFixture : IntegrationFixture
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Fact]
    public async Task A_job_inserted_by_another_client_is_picked_up_through_listen_long_before_the_next_poll()
    {
        var schema = await db.CreateSchemaAsync(Ct);
        Assert.SkipUnless(db.HasNotifications, "The driver has no notifications.");
        var driver = schema.Driver;
        var client = Client(driver, new Work(), o => o with { FetchPollInterval = TimeSpan.FromMinutes(5) });
        await client.StartAsync(Ct);
        try
        {
            await Task.Delay(300, Ct); // the initial fetch has run and the listener is up
            var id = (await new KopiClient(driver).InsertAsync(new LifecycleArgs("complete"), new InsertOptions { Queue = "work" }, Ct)).Job.Id;

            var job = await WaitForStateAsync(driver, id, JobState.Completed, TimeSpan.FromSeconds(10));

            Assert.Equal([client.ClientId], job.AttemptedBy);
        }
        finally
        {
            await client.StopAsync(Ct);
        }
    }

    [Fact]
    public async Task Poll_only_picks_up_jobs_on_the_next_poll()
    {
        var schema = await db.CreateSchemaAsync(Ct);
        var driver = schema.Driver;
        var client = Client(driver, new Work(), o => o with { NotificationMode = NotificationMode.PollOnly, FetchPollInterval = TimeSpan.FromMilliseconds(200) });
        await client.StartAsync(Ct);
        try
        {
            var id = (await new KopiClient(driver).InsertAsync(new LifecycleArgs("complete"), new InsertOptions { Queue = "work" }, Ct)).Job.Id;

            await WaitForStateAsync(driver, id, JobState.Completed, TimeSpan.FromSeconds(10));
        }
        finally
        {
            await client.StopAsync(Ct);
        }
    }

    [Fact]
    public async Task A_soft_stop_waits_for_running_jobs_and_stores_their_outcome()
    {
        var schema = await db.CreateSchemaAsync(Ct);
        var driver = schema.Driver;
        var work = new Work();
        var client = Client(driver, work);
        var id = (await client.InsertAsync(new LifecycleArgs("gated"), new InsertOptions { Queue = "work" }, Ct)).Job.Id;
        await client.StartAsync(Ct);
        await work.Started.Task.WaitAsync(TimeSpan.FromSeconds(10), Ct);

        var stop = client.StopAsync(Ct);
        await Task.Delay(300, Ct);
        var waited = !stop.IsCompleted;
        work.Release.SetResult();
        await stop.WaitAsync(TimeSpan.FromSeconds(10), Ct);

        Assert.True(waited);
        Assert.False(work.SawCancellation);
        Assert.Equal(JobState.Completed, (await GetAsync(driver, id)).State);
    }

    [Fact]
    public async Task A_hard_stop_cancels_running_jobs_which_are_stored_available_to_run_again()
    {
        var schema = await db.CreateSchemaAsync(Ct);
        var driver = schema.Driver;
        var work = new Work();
        var client = Client(driver, work);
        var id = (await client.InsertAsync(new LifecycleArgs("gated"), new InsertOptions { Queue = "work" }, Ct)).Job.Id;
        await client.StartAsync(Ct);
        await work.Started.Task.WaitAsync(TimeSpan.FromSeconds(10), Ct);
        using var shutdown = new CancellationTokenSource(TimeSpan.FromMilliseconds(300));

        await client.StopAsync(shutdown.Token).WaitAsync(TimeSpan.FromSeconds(10), Ct);

        AssertInterrupted(work, await GetAsync(driver, id));
    }

    [Fact]
    public async Task The_soft_stop_timeout_turns_a_stop_into_a_hard_stop()
    {
        var schema = await db.CreateSchemaAsync(Ct);
        var driver = schema.Driver;
        var work = new Work();
        var client = Client(driver, work, o => o with { SoftStopTimeout = TimeSpan.FromMilliseconds(300) });
        var id = (await client.InsertAsync(new LifecycleArgs("gated"), new InsertOptions { Queue = "work" }, Ct)).Job.Id;
        await client.StartAsync(Ct);
        await work.Started.Task.WaitAsync(TimeSpan.FromSeconds(10), Ct);

        await client.StopAsync(Ct).WaitAsync(TimeSpan.FromSeconds(10), Ct);

        AssertInterrupted(work, await GetAsync(driver, id));
    }

    private static void AssertInterrupted(Work work, JobRow job)
    {
        Assert.True(work.SawCancellation);
        Assert.Equal((JobState.Available, 0), (job.State, job.Attempt)); // River: interrupted, not an error
        Assert.Empty(job.Errors);
        Assert.Null(job.FinalizedAt);
    }

    private static KopiClient Client(IKopiDriver driver, Work work, Func<Settings, Settings>? configure = null)
    {
        var s = (configure ?? (x => x))(new Settings());
        return new KopiClient(driver, new KopiClientOptions
        {
            Queues = new Dictionary<string, QueueOptions> { ["work"] = new() { MaxWorkers = 5 } },
            Workers = new WorkerRegistry().Add(work),
            FetchPollInterval = s.FetchPollInterval,
            FetchCooldown = TimeSpan.FromMilliseconds(5),
            NotificationMode = s.NotificationMode,
            SoftStopTimeout = s.SoftStopTimeout,
            HardStopGrace = TimeSpan.FromSeconds(5),
        });
    }

    private static async Task<JobRow> WaitForStateAsync(IKopiDriver driver, long id, JobState state, TimeSpan limit)
    {
        var deadline = DateTime.UtcNow + limit;
        while (true)
        {
            var job = await GetAsync(driver, id);
            if (job.State == state)
            {
                return job;
            }

            Assert.True(DateTime.UtcNow < deadline, $"Job {id} is {job.State}, not {state}, after {limit}.");
            await Task.Delay(20, Ct);
        }
    }

    private static async Task<JobRow> GetAsync(IKopiDriver driver, long id) =>
        await driver.GetExecutor().JobGetByIdAsync(id, Ct) ?? throw new InvalidOperationException($"Job {id} is missing.");

    private sealed record Settings
    {
        public TimeSpan FetchPollInterval { get; init; } = TimeSpan.FromMilliseconds(100);

        public NotificationMode NotificationMode { get; init; } = NotificationMode.Auto;

        public TimeSpan? SoftStopTimeout { get; init; }
    }

    [JobKind("lifecycle")]
    public sealed record LifecycleArgs(string What) : IJobArgs;

    private sealed class Work : Worker<LifecycleArgs>
    {
        public TaskCompletionSource Started { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public TaskCompletionSource Release { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public bool SawCancellation { get; private set; }

        public override async Task WorkAsync(Job<LifecycleArgs> job, CancellationToken cancellationToken)
        {
            if (job.Args.What != "gated")
            {
                return;
            }

            Started.TrySetResult();
            try
            {
                await Release.Task.WaitAsync(cancellationToken);
            }
            catch (OperationCanceledException)
            {
                SawCancellation = true;
                throw;
            }
        }
    }
}
