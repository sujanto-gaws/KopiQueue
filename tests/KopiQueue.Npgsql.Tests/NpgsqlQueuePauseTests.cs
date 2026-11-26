using System.Text.Json.Nodes;
using KopiQueue.Drivers;
using KopiQueue.Migrations;
using KopiQueue.TestInfrastructure;
using Npgsql;

namespace KopiQueue.Npgsql.Tests;

/// <summary>
/// Queues on a real PostgreSQL (plan task 3.6: FR-QUE-02..04, FR-ADM-04): clients create their queues' rows, a pause
/// reaches listening and polling clients alike while running jobs finish, a queue paused before a client starts is never
/// fetched, a local pause stops only its client, and the queue admin API reads, lists and updates queues.
/// </summary>
public sealed class NpgsqlQueuePauseTests(PostgresFixture db) : IClassFixture<PostgresFixture>
{
    private const string Queue = "pausable";

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Fact]
    public async Task The_queue_api_reads_lists_and_updates_the_queues_clients_create()
    {
        db.SkipIfUnavailable();
        await using var ds = db.CreateDataSource();
        var (driver, _) = await MigratedAsync(ds);
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
        db.SkipIfUnavailable();
        await using var ds = db.CreateDataSource();
        var (driver, schema) = await MigratedAsync(ds);
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
            Assert.Equal(4, await CountAsync(ds, schema, "available"));

            worker.Release.SetResult(); // the job running when the queue paused still finishes
            await WaitForAsync(ds, schema, "completed", 1);
            Assert.Equal(JobState.Completed, (await driver.GetExecutor().JobGetByIdAsync(slow.Job.Id, Ct))!.State);
            Assert.Equal(4, await CountAsync(ds, schema, "available"));

            await admin.QueueResumeAsync(Queue, cancellationToken: Ct);
            await WaitForAsync(ds, schema, "completed", 5);
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
        db.SkipIfUnavailable();
        await using var ds = db.CreateDataSource();
        var (driver, schema) = await MigratedAsync(ds);
        await using (var cmd = ds.CreateCommand($"INSERT INTO {schema}.river_queue (name, paused_at, updated_at) VALUES ('{Queue}', now(), now())"))
        {
            await cmd.ExecuteNonQueryAsync(Ct);
        }

        var admin = new KopiClient(driver);
        await admin.InsertAsync(new PauseArgs("ok"), new InsertOptions { Queue = Queue }, Ct);
        var client = await StartedAsync(driver, new PauseWorker(), "late", NotificationMode.PollOnly);
        try
        {
            await Task.Delay(TimeSpan.FromSeconds(2.5), Ct);
            Assert.Equal(1, await CountAsync(ds, schema, "available"));

            await admin.QueueResumeAsync(Queue, cancellationToken: Ct);
            await WaitForAsync(ds, schema, "completed", 1);
        }
        finally
        {
            await client.StopAsync(Ct);
        }
    }

    [Fact]
    public async Task A_local_pause_stops_only_its_own_client()
    {
        db.SkipIfUnavailable();
        await using var ds = db.CreateDataSource();
        var (driver, schema) = await MigratedAsync(ds);
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

            await WaitForAsync(ds, schema, "completed", 6);
            Assert.Equal(["two"], await AttemptedByAsync(ds, schema));
            Assert.Null((await one.QueueGetAsync(Queue, Ct))!.PausedAt); // nothing stored

            await two.QueuePauseAsync(Queue, local, Ct);
            await Task.Delay(200, Ct);
            await one.InsertAsync(new PauseArgs("ok"), new InsertOptions { Queue = Queue }, Ct);
            await Task.Delay(TimeSpan.FromSeconds(2.5), Ct);
            Assert.Equal(1, await CountAsync(ds, schema, "available"));

            await one.QueueResumeAsync(Queue, local, Ct);
            await WaitForAsync(ds, schema, "completed", 7);
            Assert.Equal(["one", "two"], await AttemptedByAsync(ds, schema));
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

    private static async Task<long> CountAsync(NpgsqlDataSource ds, string schema, string state)
    {
        await using var cmd = ds.CreateCommand($"SELECT count(*) FROM {schema}.river_job WHERE state = '{state}'");
        return (long)(await cmd.ExecuteScalarAsync(Ct))!;
    }

    private static async Task WaitForAsync(NpgsqlDataSource ds, string schema, string state, long count)
    {
        var deadline = DateTime.UtcNow.AddSeconds(15);
        while (await CountAsync(ds, schema, state) < count)
        {
            Assert.True(DateTime.UtcNow < deadline, $"Fewer than {count} jobs are {state} after 15 s.");
            await Task.Delay(50, Ct);
        }
    }

    private static async Task<List<string>> AttemptedByAsync(NpgsqlDataSource ds, string schema)
    {
        await using var cmd = ds.CreateCommand($"SELECT DISTINCT unnest(attempted_by) AS c FROM {schema}.river_job ORDER BY c");
        var ids = new List<string>();
        await using var reader = await cmd.ExecuteReaderAsync(Ct);
        while (await reader.ReadAsync(Ct))
        {
            ids.Add(reader.GetString(0));
        }

        return ids;
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
