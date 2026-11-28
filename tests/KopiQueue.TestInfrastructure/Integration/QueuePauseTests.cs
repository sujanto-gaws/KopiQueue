using System.Text.Json.Nodes;
using KopiQueue.Drivers;
using Xunit;

namespace KopiQueue.TestInfrastructure.Integration;

/// <summary>
/// Queues on a real database (plan task 3.6: FR-QUE-02..04, FR-ADM-04): clients create their queues' rows, a pause
/// reaches listening and polling clients alike while running jobs finish, a queue paused before a client starts is never
/// fetched, a local pause stops only its client, and the queue admin API reads, lists and updates queues.
/// </summary>
public abstract class QueuePauseTests<TFixture>(TFixture db)
    where TFixture : IntegrationFixture
{
    private const string Queue = "pausable";

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Fact]
    public async Task The_queue_api_reads_lists_and_updates_the_queues_clients_create()
    {
        await using var schema = await db.CreateSchemaAsync(Ct);
        var driver = schema.Driver;
        var worker = new PauseWorker();
        var client = await StartedAsync(driver, worker, "lister", NotificationMode.Auto, queues: ["q_a", "q_b", "q_c"]);
        try
        {
            var admin = new KopiClient(driver);
            var a = await admin.QueueGetAsync("q_a", Ct);
            Assert.NotNull(a);
            Assert.Null(a.PausedAt);
            Assert.Null(await admin.QueueGetAsync("q_missing", Ct));

            Assert.Equal(["q_a", "q_b", "q_c"], (await admin.QueueListAsync(cancellationToken: Ct)).Select(q => q.Name));
            var page = await admin.QueueListAsync(new QueueListParams { Limit = 2 }, Ct);
            Assert.Equal(["q_a", "q_b"], page.Select(q => q.Name));
            Assert.Equal(["q_c"], (await admin.QueueListAsync(new QueueListParams { Limit = 2, After = page[^1].Name }, Ct)).Select(q => q.Name));
            await Assert.ThrowsAsync<ArgumentOutOfRangeException>(() => admin.QueueListAsync(new QueueListParams { Limit = 0 }, Ct));

            var updated = await admin.QueueUpdateAsync("q_b", new JsonObject { ["owner"] = "billing" }, Ct);
            Assert.Equal("billing", JsonNode.Parse(updated.MetadataJson)!["owner"]!.GetValue<string>());

            await Assert.ThrowsAsync<KopiQueueNotFoundException>(() => admin.QueuePauseAsync("q_missing", cancellationToken: Ct));
            await Assert.ThrowsAsync<KopiQueueNotFoundException>(() => admin.QueueResumeAsync("q_missing", cancellationToken: Ct));
            await Assert.ThrowsAsync<KopiQueueNotFoundException>(() => admin.QueueUpdateAsync("q_missing", [], Ct));
            await Assert.ThrowsAsync<KopiQueueNotFoundException>(
                () => admin.QueuePauseAsync("q_a", new QueuePauseOptions { LocalOnly = true }, Ct)); // works no queues
        }
        finally
        {
            await client.StopAsync(Ct);
        }
    }

    [Theory]
    [InlineData(NotificationMode.Auto)]
    [InlineData(NotificationMode.PollOnly)]
    public async Task A_pause_reaches_every_client_within_the_poll_interval_and_running_jobs_finish(NotificationMode mode)
    {
        await using var schema = await db.CreateSchemaAsync(Ct);
        var driver = schema.Driver;
        var worker = new PauseWorker();
        var one = await StartedAsync(driver, worker, "one", mode);
        var two = await StartedAsync(driver, worker, "two", mode);
        try
        {
            var admin = new KopiClient(driver);
            var slow = await admin.InsertAsync(new PauseArgs("block"), new InsertOptions { Queue = Queue }, Ct);
            await worker.Started.Task.WaitAsync(TimeSpan.FromSeconds(10), Ct);

            await admin.QueuePauseAsync(Queue, cancellationToken: Ct);
            await Task.Delay(TimeSpan.FromSeconds(1.5), Ct); // one poll interval, and some
            for (var i = 0; i < 4; i++)
            {
                await admin.InsertAsync(new PauseArgs("ok"), new InsertOptions { Queue = Queue }, Ct);
            }

            await Task.Delay(TimeSpan.FromSeconds(2.5), Ct);
            Assert.Equal(4, await schema.CountAsync(j => j.State == JobState.Available, Ct));

            worker.Release.SetResult(); // the job running when the queue paused still finishes
            await schema.WaitForCountAsync(j => j.State == JobState.Completed, 1, TimeSpan.FromSeconds(15), Ct);
            Assert.Equal(JobState.Completed, (await driver.GetExecutor().JobGetByIdAsync(slow.Job.Id, Ct))!.State);
            Assert.Equal(4, await schema.CountAsync(j => j.State == JobState.Available, Ct));

            await admin.QueueResumeAsync(Queue, cancellationToken: Ct);
            await schema.WaitForCountAsync(j => j.State == JobState.Completed, 5, TimeSpan.FromSeconds(15), Ct);
        }
        finally
        {
            worker.Release.TrySetResult();
            await one.StopAsync(Ct);
            await two.StopAsync(Ct);
        }
    }

    [Fact]
    public async Task A_queue_paused_before_a_client_starts_is_never_fetched()
    {
        await using var schema = await db.CreateSchemaAsync(Ct);
        var driver = schema.Driver;
        await schema.InsertQueueAsync(Queue, TimeSpan.Zero, paused: true, Ct);

        var admin = new KopiClient(driver);
        await admin.InsertAsync(new PauseArgs("ok"), new InsertOptions { Queue = Queue }, Ct);
        var client = await StartedAsync(driver, new PauseWorker(), "late", NotificationMode.PollOnly);
        try
        {
            await Task.Delay(TimeSpan.FromSeconds(2.5), Ct);
            Assert.Equal(1, await schema.CountAsync(j => j.State == JobState.Available, Ct));

            await admin.QueueResumeAsync(Queue, cancellationToken: Ct);
            await schema.WaitForCountAsync(j => j.State == JobState.Completed, 1, TimeSpan.FromSeconds(15), Ct);
        }
        finally
        {
            await client.StopAsync(Ct);
        }
    }

    [Fact]
    public async Task A_local_pause_stops_only_its_own_client()
    {
        await using var schema = await db.CreateSchemaAsync(Ct);
        var driver = schema.Driver;
        var worker = new PauseWorker();
        var one = await StartedAsync(driver, worker, "one", NotificationMode.Auto);
        var two = await StartedAsync(driver, worker, "two", NotificationMode.Auto);
        try
        {
            var local = new QueuePauseOptions { LocalOnly = true };
            await one.QueuePauseAsync(Queue, local, Ct);
            await Task.Delay(200, Ct); // a fetch already sent may still complete
            for (var i = 0; i < 6; i++)
            {
                await one.InsertAsync(new PauseArgs("ok"), new InsertOptions { Queue = Queue }, Ct);
            }

            await schema.WaitForCountAsync(j => j.State == JobState.Completed, 6, TimeSpan.FromSeconds(15), Ct);
            Assert.Equal(["two"], await AttemptedByAsync(schema));
            Assert.Null((await one.QueueGetAsync(Queue, Ct))!.PausedAt); // nothing stored

            await two.QueuePauseAsync(Queue, local, Ct);
            await Task.Delay(200, Ct);
            await one.InsertAsync(new PauseArgs("ok"), new InsertOptions { Queue = Queue }, Ct);
            await Task.Delay(TimeSpan.FromSeconds(2.5), Ct);
            Assert.Equal(1, await schema.CountAsync(j => j.State == JobState.Available, Ct));

            await one.QueueResumeAsync(Queue, local, Ct);
            await schema.WaitForCountAsync(j => j.State == JobState.Completed, 7, TimeSpan.FromSeconds(15), Ct);
            Assert.Equal(["one", "two"], await AttemptedByAsync(schema));
            await Assert.ThrowsAsync<KopiQueueNotFoundException>(() => one.QueuePauseAsync("elsewhere", local, Ct));
        }
        finally
        {
            await one.StopAsync(Ct);
            await two.StopAsync(Ct);
        }
    }

    private static async Task<KopiClient> StartedAsync(IKopiDriver driver, PauseWorker worker, string id, NotificationMode mode, string[]? queues = null)
    {
        var client = new KopiClient(driver, new KopiClientOptions
        {
            Queues = (queues ?? [Queue]).ToDictionary(q => q, _ => new QueueOptions { MaxWorkers = 2 }),
            Workers = new WorkerRegistry().Add(worker),
            ClientId = id,
            NotificationMode = mode,
            FetchPollInterval = TimeSpan.FromSeconds(1),
        });
        await client.StartAsync(Ct);
        return client;
    }

    private static async Task<List<string>> AttemptedByAsync(IntegrationSchema schema) =>
        [.. (await schema.JobsAsync(Ct)).SelectMany(j => j.AttemptedBy).Distinct().Order(StringComparer.Ordinal)];

    [JobKind("pause_job")]
    public sealed record PauseArgs(string What) : IJobArgs;

    /// <summary>Completes <c>ok</c> jobs at once; a <c>block</c> job waits for <see cref="Release"/>.</summary>
    private sealed class PauseWorker : Worker<PauseArgs>
    {
        public TaskCompletionSource Started { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public TaskCompletionSource Release { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public override async Task WorkAsync(Job<PauseArgs> job, CancellationToken cancellationToken)
        {
            if (job.Args.What == "block")
            {
                Started.TrySetResult();
                await Release.Task.WaitAsync(cancellationToken);
            }
        }
    }
}
