using KopiQueue.Drivers;
using KopiQueue.Migrations;
using KopiQueue.TestInfrastructure;
using Npgsql;

namespace KopiQueue.Npgsql.Tests;

/// <summary>
/// Periodic jobs on a real PostgreSQL (plan task 4.3: FR-PER-02, FR-PER-04; UC-3): a cluster inserts each tick once,
/// whichever client leads, and a change of leader neither repeats a tick nor skips more than one.
/// </summary>
public sealed class NpgsqlPeriodicTests(PostgresFixture db) : IClassFixture<PostgresFixture>
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Fact]
    public async Task A_cluster_inserts_each_tick_once_across_a_change_of_leader()
    {
        db.SkipIfUnavailable();
        await using var ds = db.CreateDataSource();
        var (driver, schema) = await MigratedAsync(ds);
        var started = DateTimeOffset.UtcNow;

        var a = await StartedAsync(driver, "node-a");
        var b = await StartedAsync(driver, "node-b");
        var stopped = false;
        try
        {
            await Task.Delay(TimeSpan.FromSeconds(3), Ct);
            var leader = await LeaderAsync(ds, schema);
            var (first, second) = leader == "node-a" ? (a, b) : (b, a);

            await first.StopAsync(Ct); // resigns; the other client takes over
            await Task.Delay(TimeSpan.FromSeconds(4), Ct);
            Assert.NotEqual(leader, await LeaderAsync(ds, schema));
            await second.StopAsync(Ct);
            stopped = true;
        }
        finally
        {
            if (!stopped)
            {
                await a.StopAsync(Ct);
                await b.StopAsync(Ct);
            }
        }

        var elapsed = DateTimeOffset.UtcNow - started;
        await using var cmd = ds.CreateCommand(
            $"""
            SELECT count(*), count(DISTINCT unique_key), bool_and((metadata->>'periodic')::boolean),
                   max(gap) FILTER (WHERE gap IS NOT NULL)
            FROM (
                SELECT unique_key, metadata, created_at - lag(created_at) OVER (ORDER BY created_at) AS gap
                FROM {schema}.river_job WHERE kind = 'every_second'
            ) AS j
            """);
        await using var reader = await cmd.ExecuteReaderAsync(Ct);
        Assert.True(await reader.ReadAsync(Ct));
        var (count, distinct, periodic, widest) = (reader.GetInt64(0), reader.GetInt64(1), reader.GetBoolean(2), reader.GetTimeSpan(3));

        Assert.Equal(count, distinct); // no tick twice (FR-PER-04)
        Assert.True(periodic);
        Assert.InRange(count, 4, (long)elapsed.TotalSeconds + 1);
        Assert.True(widest < TimeSpan.FromSeconds(3), $"The widest gap between ticks was {widest}: more than one tick was skipped.");
    }

    [Fact]
    public async Task A_new_leader_skips_a_tick_the_previous_leader_inserted()
    {
        db.SkipIfUnavailable();
        await using var ds = db.CreateDataSource();
        var (driver, schema) = await MigratedAsync(ds);

        // Every 10 minutes, with a catch-up window (the lease plus the election interval) over 10 minutes: the latest
        // tick is always recent enough to catch up, so each new leader inserts it, and only the first insert stays.
        var schedule = PeriodicSchedule.Every(TimeSpan.FromMinutes(10));
        var job = PeriodicJob.Create(schedule, () => new EverySecondArgs());
        var start = DateTimeOffset.UtcNow;
        var first = await StartedAsync(driver, "node-a", job, leaderTtl: TimeSpan.FromMinutes(10));
        await WaitForAsync(ds, schema, 1);
        await first.StopAsync(Ct);

        var second = await StartedAsync(driver, "node-b", job, leaderTtl: TimeSpan.FromMinutes(10));
        try
        {
            Assert.Equal("node-b", await WaitForLeaderAsync(ds, schema, "node-b"));
            await Task.Delay(TimeSpan.FromSeconds(1), Ct);
        }
        finally
        {
            await second.StopAsync(Ct);
        }

        // One job for the tick both leaders caught up, plus one for each tick that came due during the test (rarely any).
        var expected = 1L;
        for (var tick = schedule.NextAfter(start)!.Value; tick <= DateTimeOffset.UtcNow; tick = schedule.NextAfter(tick)!.Value)
        {
            expected++;
        }

        await using var cmd = ds.CreateCommand($"SELECT count(*) FROM {schema}.river_job WHERE kind = 'every_second'");
        Assert.Equal(expected, await cmd.ExecuteScalarAsync(Ct));
    }

    private static async Task<KopiClient> StartedAsync(IKopiDriver driver, string clientId, PeriodicJob? job = null, TimeSpan? leaderTtl = null)
    {
        var client = new KopiClient(driver, new KopiClientOptions
        {
            Queues = new Dictionary<string, QueueOptions> { ["periodic_work"] = new() { MaxWorkers = 1 } },
            Workers = new WorkerRegistry().Add(new IdleWorker()),
            ClientId = clientId,
            ElectInterval = TimeSpan.FromMilliseconds(200),
            LeaderTtl = leaderTtl ?? TimeSpan.FromSeconds(1),
            PeriodicJobs = [job ?? PeriodicJob.Create(PeriodicSchedule.Cron("* * * * * *"), () => new EverySecondArgs())],
        });
        await client.StartAsync(Ct);
        return client;
    }

    private static async Task WaitForAsync(NpgsqlDataSource ds, string schema, long jobs)
    {
        var deadline = DateTime.UtcNow.AddSeconds(10);
        await using var cmd = ds.CreateCommand($"SELECT count(*) FROM {schema}.river_job WHERE kind = 'every_second'");
        while ((long)(await cmd.ExecuteScalarAsync(Ct))! < jobs)
        {
            Assert.True(DateTime.UtcNow < deadline, $"Fewer than {jobs} periodic jobs after 10 s.");
            await Task.Delay(50, Ct);
        }
    }

    private static async Task<string?> WaitForLeaderAsync(NpgsqlDataSource ds, string schema, string expected)
    {
        var deadline = DateTime.UtcNow.AddSeconds(10);
        string? leader;
        while ((leader = await LeaderAsync(ds, schema)) != expected && DateTime.UtcNow < deadline)
        {
            await Task.Delay(50, Ct);
        }

        return leader;
    }

    private static async Task<string?> LeaderAsync(NpgsqlDataSource ds, string schema)
    {
        await using var cmd = ds.CreateCommand($"SELECT leader_id FROM {schema}.river_leader");
        return await cmd.ExecuteScalarAsync(Ct) as string;
    }

    private static async Task<(IKopiDriver Driver, string Schema)> MigratedAsync(NpgsqlDataSource ds)
    {
        var schema = "p_" + Guid.NewGuid().ToString("N")[..12];
        await using (var cmd = ds.CreateCommand($"CREATE SCHEMA {schema}"))
        {
            await cmd.ExecuteNonQueryAsync(Ct);
        }

        var driver = new NpgsqlDriver(ds, new NpgsqlDriverOptions { Schema = schema });
        await driver.CreateMigrator().MigrateAsync(MigrationDirection.Up, cancellationToken: Ct);
        return (driver, schema);
    }

    /// <summary>Inserted into the default queue, which no client works, so every inserted job stays to be counted.</summary>
    [JobKind("every_second")]
    public sealed record EverySecondArgs : IJobArgs;

    [JobKind("periodic_idle")]
    public sealed record IdleArgs : IJobArgs;

    private sealed class IdleWorker : Worker<IdleArgs>
    {
        public override Task WorkAsync(Job<IdleArgs> job, CancellationToken cancellationToken) => Task.CompletedTask;
    }
}
