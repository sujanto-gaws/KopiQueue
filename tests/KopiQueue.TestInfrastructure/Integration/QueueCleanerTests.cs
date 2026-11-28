using KopiQueue.Drivers;
using Xunit;

namespace KopiQueue.TestInfrastructure.Integration;

/// <summary>
/// The queue cleaner on a real database (plan task 3.5: FR-MNT-05, FR-MNT-07; design §5.7): stale queue rows go in
/// bounded batches, rows another transaction holds are skipped, jobs stay, and the leader runs it.
/// </summary>
public abstract class QueueCleanerTests<TFixture>(TFixture db)
    where TFixture : IntegrationFixture
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Fact]
    public async Task Queues_not_updated_within_the_retention_are_deleted_in_batches()
    {
        await using var schema = await db.CreateSchemaAsync(Ct);
        var driver = schema.Driver;
        await schema.InsertQueueAsync("stale_a", TimeSpan.FromDays(2), paused: false, Ct);
        await schema.InsertQueueAsync("stale_b", TimeSpan.FromDays(2), paused: false, Ct);
        await schema.InsertQueueAsync("stale_c", TimeSpan.FromHours(25), paused: false, Ct);
        await schema.InsertQueueAsync("fresh", TimeSpan.FromHours(23), paused: false, Ct);
        var executor = driver.GetExecutor();
        var oneDay = new QueueDeleteExpiredParams { Retention = TimeSpan.FromDays(1), Max = 2 };

        Assert.Equal(["stale_a", "stale_b"], await executor.QueueDeleteExpiredAsync(oneDay, Ct));
        Assert.Equal(["stale_c"], await executor.QueueDeleteExpiredAsync(oneDay, Ct));
        Assert.Empty(await executor.QueueDeleteExpiredAsync(oneDay, Ct));

        Assert.Equal(["fresh"], await schema.QueuesAsync(Ct));
    }

    [Fact]
    public async Task A_queue_row_another_transaction_holds_is_skipped_and_deleted_later()
    {
        await using var schema = await db.CreateSchemaAsync(Ct);
        var driver = schema.Driver;
        await schema.InsertQueueAsync("held", TimeSpan.FromDays(2), paused: false, Ct);
        await schema.InsertQueueAsync("free", TimeSpan.FromDays(2), paused: false, Ct);
        var oneDay = new QueueDeleteExpiredParams { Retention = TimeSpan.FromDays(1), Max = 10 };

        await using (await schema.LockQueueRowAsync("held", Ct))
        {
            Assert.Equal(["free"], await driver.GetExecutor().QueueDeleteExpiredAsync(oneDay, Ct).WaitAsync(TimeSpan.FromSeconds(10), Ct));
        }

        Assert.Equal(["held"], await driver.GetExecutor().QueueDeleteExpiredAsync(oneDay, Ct));
    }

    [Fact]
    public async Task Deleting_a_queue_leaves_its_jobs_alone()
    {
        await using var schema = await db.CreateSchemaAsync(Ct);
        var driver = schema.Driver;
        await schema.InsertQueueAsync("gone", TimeSpan.FromDays(2), paused: false, Ct);
        var job = await new KopiClient(driver).InsertAsync(new CleanerArgs(), new InsertOptions { Queue = "gone" }, Ct);

        await driver.GetExecutor().QueueDeleteExpiredAsync(new QueueDeleteExpiredParams { Retention = TimeSpan.FromDays(1), Max = 10 }, Ct);

        Assert.Empty(await schema.QueuesAsync(Ct));
        Assert.NotNull(await driver.GetExecutor().JobGetByIdAsync(job.Job.Id, Ct));
    }

    [Fact]
    public async Task The_leader_deletes_stale_queues_as_soon_as_it_wins()
    {
        await using var schema = await db.CreateSchemaAsync(Ct);
        var driver = schema.Driver;
        await schema.InsertQueueAsync("abandoned", TimeSpan.FromDays(2), paused: false, Ct);
        await schema.InsertQueueAsync("recent", TimeSpan.FromHours(1), paused: false, Ct);

        var client = await StartedAsync(driver, TimeSpan.FromDays(1));
        try
        {
            var deadline = DateTime.UtcNow.AddSeconds(10);
            while ((await schema.QueuesAsync(Ct)).Contains("abandoned"))
            {
                Assert.True(DateTime.UtcNow < deadline, "The abandoned queue is still there after 10 s.");
                await Task.Delay(50, Ct);
            }

            Assert.Contains("recent", await schema.QueuesAsync(Ct));
        }
        finally
        {
            await client.StopAsync(Ct);
        }
    }

    [Fact]
    public async Task No_queue_retention_keeps_every_queue()
    {
        await using var schema = await db.CreateSchemaAsync(Ct);
        var driver = schema.Driver;
        await schema.InsertQueueAsync("ancient", TimeSpan.FromDays(400), paused: false, Ct);

        var client = await StartedAsync(driver, queueRetention: null);
        try
        {
            await schema.WaitForLeaderAsync(l => l is not null, TimeSpan.FromSeconds(5), Ct);

            await Task.Delay(TimeSpan.FromSeconds(1), Ct);
            Assert.Contains("ancient", await schema.QueuesAsync(Ct));
        }
        finally
        {
            await client.StopAsync(Ct);
        }
    }

    private static async Task<KopiClient> StartedAsync(IKopiDriver driver, TimeSpan? queueRetention)
    {
        var client = new KopiClient(driver, new KopiClientOptions
        {
            Queues = new Dictionary<string, QueueOptions> { ["cleaner_work"] = new() { MaxWorkers = 1 } },
            Workers = new WorkerRegistry().Add(new CleanerWorker()),
            ClientId = "cleaner-client",
            QueueRetention = queueRetention,
        });
        await client.StartAsync(Ct);
        return client;
    }

    [JobKind("cleaner_job")]
    public sealed record CleanerArgs : IJobArgs;

    private sealed class CleanerWorker : Worker<CleanerArgs>
    {
        public override Task WorkAsync(Job<CleanerArgs> job, CancellationToken cancellationToken) => Task.CompletedTask;
    }
}
