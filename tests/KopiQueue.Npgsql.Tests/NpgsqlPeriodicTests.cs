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
        // Every second, so the test is short. How many ticks a change of leader skips depends on how long leadership is
        // vacant, which a loaded machine can stretch past a second, so this test checks what holds under any load: no
        // tick twice, and ticks from both leaders. The catch-up of a skipped tick is checked by the next test and the
        // enqueuer's unit tests.
        db.SkipIfUnavailable();
        await using var ds = db.CreateDataSource();
        var (driver, schema) = await MigratedAsync(ds);
        var started = DateTimeOffset.UtcNow;

        var a = await StartedAsync(driver, "node-a");
        var b = await StartedAsync(driver, "node-b");
        var stopped = false;
        try
        {
            var leader = await WaitForLeaderAsync(ds, schema, expected: null);
            await WaitForAsync(ds, schema, 2);
            var (first, second, next) = leader == "node-a" ? (a, b, "node-b") : (b, a, "node-a");

            var switchedAt = await DatabaseNowAsync(ds);
            await first.StopAsync(Ct); // resigns; the other client takes over
            Assert.Equal(next, await WaitForLeaderAsync(ds, schema, next));
            await WaitForAsync(ds, schema, 2, after: switchedAt);
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
            $"SELECT count(*), count(DISTINCT unique_key), bool_and((metadata->>'periodic')::boolean) FROM {schema}.river_job WHERE kind = 'every_second'");
        await using var reader = await cmd.ExecuteReaderAsync(Ct);
        Assert.True(await reader.ReadAsync(Ct));
        var (count, distinct, periodic) = (reader.GetInt64(0), reader.GetInt64(1), reader.GetBoolean(2));

        Assert.Equal(count, distinct); // no tick twice (FR-PER-04)
        Assert.True(periodic);
        Assert.InRange(count, 4, (long)Math.Ceiling(elapsed.TotalSeconds) + 1); // never more jobs than ticks
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

    /// <summary>Waits until at least <paramref name="jobs"/> periodic jobs exist, created after <paramref name="after"/> if given.</summary>
    private static async Task WaitForAsync(NpgsqlDataSource ds, string schema, long jobs, DateTimeOffset? after = null)
    {
        var deadline = DateTime.UtcNow.AddSeconds(20);
        await using var cmd = ds.CreateCommand(
            $"SELECT count(*) FROM {schema}.river_job WHERE kind = 'every_second' AND ($1::timestamptz IS NULL OR created_at >= $1)");
        cmd.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.TimestampTz, Value = (object?)after ?? DBNull.Value });
        while ((long)(await cmd.ExecuteScalarAsync(Ct))! < jobs)
        {
            Assert.True(DateTime.UtcNow < deadline, $"Fewer than {jobs} periodic jobs after 20 s.");
            await Task.Delay(50, Ct);
        }
    }

    private static async Task<DateTimeOffset> DatabaseNowAsync(NpgsqlDataSource ds)
    {
        await using var cmd = ds.CreateCommand("SELECT now()");
        await using var reader = await cmd.ExecuteReaderAsync(Ct);
        Assert.True(await reader.ReadAsync(Ct));
        return reader.GetFieldValue<DateTimeOffset>(0);
    }

    /// <summary>Waits until <paramref name="expected"/> leads, or until any client leads when it is <c>null</c>; returns the leader.</summary>
    private static async Task<string?> WaitForLeaderAsync(NpgsqlDataSource ds, string schema, string? expected)
    {
        var deadline = DateTime.UtcNow.AddSeconds(20);
        string? leader;
        while ((leader = await LeaderAsync(ds, schema)) is null || (expected is not null && leader != expected))
        {
            if (DateTime.UtcNow >= deadline)
            {
                break;
            }

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
