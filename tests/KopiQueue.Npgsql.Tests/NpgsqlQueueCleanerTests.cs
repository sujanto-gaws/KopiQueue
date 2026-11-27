using KopiQueue.Drivers;
using KopiQueue.Migrations;
using KopiQueue.TestInfrastructure;
using Npgsql;

namespace KopiQueue.Npgsql.Tests;

/// <summary>
/// The queue cleaner on a real PostgreSQL (plan task 3.5: FR-MNT-05, FR-MNT-07; design §5.7): stale queue rows go in
/// bounded batches, rows another transaction holds are skipped, jobs stay, and the leader runs it.
/// </summary>
public sealed class NpgsqlQueueCleanerTests(PostgresFixture db) : IClassFixture<PostgresFixture>
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Fact]
    public async Task Queues_not_updated_within_the_retention_are_deleted_in_batches()
    {
        db.SkipIfUnavailable();
        await using var ds = db.CreateDataSource();
        var (driver, schema) = await MigratedAsync(ds);
        await AddQueueAsync(ds, schema, "stale_a", TimeSpan.FromDays(2));
        await AddQueueAsync(ds, schema, "stale_b", TimeSpan.FromDays(2));
        await AddQueueAsync(ds, schema, "stale_c", TimeSpan.FromHours(25));
        await AddQueueAsync(ds, schema, "fresh", TimeSpan.FromHours(23));
        var executor = driver.GetExecutor();
        var oneDay = new QueueDeleteExpiredParams { Retention = TimeSpan.FromDays(1), Max = 2 };

        Assert.Equal(["stale_a", "stale_b"], await executor.QueueDeleteExpiredAsync(oneDay, Ct));
        Assert.Equal(["stale_c"], await executor.QueueDeleteExpiredAsync(oneDay, Ct));
        Assert.Empty(await executor.QueueDeleteExpiredAsync(oneDay, Ct));

        Assert.Equal(["fresh"], await QueuesAsync(ds, schema));
    }

    [Fact]
    public async Task A_queue_row_another_transaction_holds_is_skipped_and_deleted_later()
    {
        db.SkipIfUnavailable();
        await using var ds = db.CreateDataSource();
        var (driver, schema) = await MigratedAsync(ds);
        await AddQueueAsync(ds, schema, "held", TimeSpan.FromDays(2));
        await AddQueueAsync(ds, schema, "free", TimeSpan.FromDays(2));
        var oneDay = new QueueDeleteExpiredParams { Retention = TimeSpan.FromDays(1), Max = 10 };

        await using (var holder = await ds.OpenConnectionAsync(Ct))
        await using (var transaction = await holder.BeginTransactionAsync(Ct))
        {
            await using (var cmd = new NpgsqlCommand($"SELECT 1 FROM {schema}.river_queue WHERE name = 'held' FOR UPDATE", holder, transaction))
            {
                await cmd.ExecuteNonQueryAsync(Ct);
            }

            Assert.Equal(["free"], await driver.GetExecutor().QueueDeleteExpiredAsync(oneDay, Ct).WaitAsync(TimeSpan.FromSeconds(10), Ct));
        }

        Assert.Equal(["held"], await driver.GetExecutor().QueueDeleteExpiredAsync(oneDay, Ct));
    }

    [Fact]
    public async Task Deleting_a_queue_leaves_its_jobs_alone()
    {
        db.SkipIfUnavailable();
        await using var ds = db.CreateDataSource();
        var (driver, schema) = await MigratedAsync(ds);
        await AddQueueAsync(ds, schema, "gone", TimeSpan.FromDays(2));
        var job = await new KopiClient(driver).InsertAsync(new CleanerArgs(), new InsertOptions { Queue = "gone" }, Ct);

        await driver.GetExecutor().QueueDeleteExpiredAsync(new QueueDeleteExpiredParams { Retention = TimeSpan.FromDays(1), Max = 10 }, Ct);

        Assert.Empty(await QueuesAsync(ds, schema));
        Assert.NotNull(await driver.GetExecutor().JobGetByIdAsync(job.Job.Id, Ct));
    }

    [Fact]
    public async Task The_leader_deletes_stale_queues_as_soon_as_it_wins()
    {
        db.SkipIfUnavailable();
        await using var ds = db.CreateDataSource();
        var (driver, schema) = await MigratedAsync(ds);
        await AddQueueAsync(ds, schema, "abandoned", TimeSpan.FromDays(2));
        await AddQueueAsync(ds, schema, "recent", TimeSpan.FromHours(1));

        var client = await StartedAsync(driver, TimeSpan.FromDays(1));
        try
        {
            var deadline = DateTime.UtcNow.AddSeconds(10);
            while ((await QueuesAsync(ds, schema)).Contains("abandoned"))
            {
                Assert.True(DateTime.UtcNow < deadline, "The abandoned queue is still there after 10 s.");
                await Task.Delay(50, Ct);
            }

            Assert.Contains("recent", await QueuesAsync(ds, schema));
        }
        finally
        {
            await client.StopAsync(Ct);
        }
    }

    [Fact]
    public async Task No_queue_retention_keeps_every_queue()
    {
        db.SkipIfUnavailable();
        await using var ds = db.CreateDataSource();
        var (driver, schema) = await MigratedAsync(ds);
        await AddQueueAsync(ds, schema, "ancient", TimeSpan.FromDays(400));

        var client = await StartedAsync(driver, queueRetention: null);
        try
        {
            var deadline = DateTime.UtcNow.AddSeconds(5);
            while (await LeaderAsync(ds, schema) is null)
            {
                Assert.True(DateTime.UtcNow < deadline, "The client did not become leader.");
                await Task.Delay(50, Ct);
            }

            await Task.Delay(TimeSpan.FromSeconds(1), Ct);
            Assert.Contains("ancient", await QueuesAsync(ds, schema));
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

    private static async Task AddQueueAsync(NpgsqlDataSource ds, string schema, string name, TimeSpan age)
    {
        await using var cmd = ds.CreateCommand($"INSERT INTO {schema}.river_queue (name, created_at, updated_at) VALUES ($1, now() - $2, now() - $2)");
        cmd.Parameters.Add(new NpgsqlParameter { Value = name });
        cmd.Parameters.Add(new NpgsqlParameter { Value = age });
        await cmd.ExecuteNonQueryAsync(Ct);
    }

    private static async Task<List<string>> QueuesAsync(NpgsqlDataSource ds, string schema)
    {
        await using var cmd = ds.CreateCommand($"SELECT name FROM {schema}.river_queue ORDER BY name");
        var names = new List<string>();
        await using var reader = await cmd.ExecuteReaderAsync(Ct);
        while (await reader.ReadAsync(Ct))
        {
            names.Add(reader.GetString(0));
        }

        return names;
    }

    private static async Task<string?> LeaderAsync(NpgsqlDataSource ds, string schema)
    {
        await using var cmd = ds.CreateCommand($"SELECT leader_id FROM {schema}.river_leader");
        return await cmd.ExecuteScalarAsync(Ct) as string;
    }

    private static async Task<(IKopiDriver Driver, string Schema)> MigratedAsync(NpgsqlDataSource ds)
    {
        var schema = "x_" + Guid.NewGuid().ToString("N")[..12];
        await using (var cmd = ds.CreateCommand($"CREATE SCHEMA {schema}"))
        {
            await cmd.ExecuteNonQueryAsync(Ct);
        }

        var driver = new NpgsqlDriver(ds, new NpgsqlDriverOptions { Schema = schema });
        await driver.CreateMigrator().MigrateAsync(MigrationDirection.Up, cancellationToken: Ct);
        return (driver, schema);
    }

    [JobKind("cleaner_job")]
    public sealed record CleanerArgs : IJobArgs;

    private sealed class CleanerWorker : Worker<CleanerArgs>
    {
        public override Task WorkAsync(Job<CleanerArgs> job, CancellationToken cancellationToken) => Task.CompletedTask;
    }
}
