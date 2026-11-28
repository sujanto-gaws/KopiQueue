using KopiQueue.Drivers;
using Xunit;

namespace KopiQueue.TestInfrastructure.Integration;

/// <summary>
/// The rescuer on a real database (plan task 3.4: FR-MNT-03, NFR-REL-02; design §5.7): a leader rescues a job a dead
/// client left running.
/// </summary>
public abstract class RescuerTests<TFixture>(TFixture db)
    where TFixture : IntegrationFixture
{
    /// <summary>How long a test waits for something to happen.</summary>
    protected static readonly TimeSpan Timeout = TimeSpan.FromSeconds(10);

    /// <summary>The driver's fixture.</summary>
    protected TFixture Fixture => db;

    /// <summary>The current test's cancellation token.</summary>
    protected static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Fact]
    public async Task A_leader_rescues_a_job_a_dead_client_left_running_and_runs_it_again()
    {
        await using var schema = await db.CreateSchemaAsync(Ct);
        var driver = schema.Driver;
        var left = await StartAsync(driver.GetExecutor(), "rescue_work", "dead-client", kind: "rescued_job");
        await Task.Delay(TimeSpan.FromSeconds(2.5), Ct); // older than the 2 s threshold before the client starts

        var worker = new RecordingWorker();
        var client = new KopiClient(driver, new KopiClientOptions
        {
            Queues = new Dictionary<string, QueueOptions> { ["rescue_work"] = new() { MaxWorkers = 1 } },
            Workers = new WorkerRegistry().Add(worker),
            ClientId = "rescuing-leader",
            JobTimeout = TimeSpan.FromSeconds(1),
            RescueStuckJobsAfter = TimeSpan.FromSeconds(2),
            ElectInterval = TimeSpan.FromMilliseconds(200),
            LeaderTtl = TimeSpan.FromSeconds(2),
        });
        await client.StartAsync(Ct);
        try
        {
            // The first batch runs as soon as the client leads; the retry is due within the scheduler interval, so the job is
            // available at once.
            var ran = await worker.Ran.Task.WaitAsync(Timeout, Ct);

            Assert.Equal((left.Id, 2), (ran.Id, ran.Attempt));
            Assert.Equal(["dead-client", "rescuing-leader"], ran.AttemptedBy);
            var error = Assert.Single(ran.Errors);
            Assert.Equal(1, error.Attempt);
            Assert.StartsWith("The job was still running 00:00:02 after this attempt started", error.Error, StringComparison.Ordinal);
        }
        finally
        {
            await client.StopAsync(Ct);
        }
    }

    /// <summary>Inserts a job in <paramref name="queue"/> and fetches it as client <paramref name="clientId"/>, so it is running.</summary>
    protected static async Task<JobRow> StartAsync(IKopiExecutor executor, string queue, string clientId, string kind = "stuck_job")
    {
        var id = (await executor.JobInsertManyAsync([Available(queue, kind)], Ct)).Single().Job.Id;
        var fetched = await executor.JobGetAvailableAsync(new JobGetAvailableParams { Queue = queue, Max = 100, ClientId = clientId }, Ct);
        return Assert.Single(fetched, j => j.Id == id);
    }

    /// <summary>An available job in <paramref name="queue"/>.</summary>
    protected static JobInsertParams Available(string queue, string kind = "stuck_job") => new()
    {
        Kind = kind, ArgsJson = "{}", Queue = queue, State = JobState.Available, Priority = 1, MaxAttempts = 25,
    };

    [JobKind("rescued_job")]
    public sealed record RescuedArgs : IJobArgs;

    private sealed class RecordingWorker : Worker<RescuedArgs>
    {
        public TaskCompletionSource<JobRow> Ran { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public override Task WorkAsync(Job<RescuedArgs> job, CancellationToken cancellationToken)
        {
            Ran.TrySetResult(job.Row);
            return Task.CompletedTask;
        }
    }
}
