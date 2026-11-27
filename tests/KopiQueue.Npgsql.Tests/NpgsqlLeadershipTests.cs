using System.Diagnostics;
using KopiQueue.Migrations;
using KopiQueue.TestInfrastructure;
using Npgsql;

namespace KopiQueue.Npgsql.Tests;

/// <summary>
/// Leader election between working clients on a real PostgreSQL (plan task 3.1: FR-MNT-01; design §5.5): one leader at a
/// time, an immediate handover when the leader stops, and a takeover once a crashed leader's lease expires.
/// </summary>
public sealed class NpgsqlLeadershipTests(PostgresFixture db) : IClassFixture<PostgresFixture>
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Fact]
    public async Task Of_several_working_clients_one_leads_and_a_stopping_leader_hands_over_through_its_resignation()
    {
        db.SkipIfUnavailable();
        await using var ds = db.CreateDataSource();
        var (driver, schema) = await MigratedAsync(ds);

        // A 20 s interval: a follower taking over within a few seconds can only have been woken by the resignation.
        var clients = new Dictionary<string, KopiClient>(StringComparer.Ordinal);
        foreach (var id in new[] { "lead-a", "lead-b", "lead-c" })
        {
            clients[id] = await StartedAsync(driver, id, NotificationMode.Auto, TimeSpan.FromSeconds(20));
        }

        try
        {
            var first = await WaitForLeaderAsync(ds, schema, l => l is not null, TimeSpan.FromSeconds(5));
            Assert.Contains(first, clients.Keys);
            await Task.Delay(TimeSpan.FromSeconds(1), Ct);
            Assert.Equal(first, await LeaderAsync(ds, schema)); // nobody else took it

            // Time the handover, not the whole stop: the stop resigns first and only then closes the notifier, which may take
            // up to its DisposeTimeout (5 s) on a busy machine. So wait for the new leader while the stop is still running.
            var stopped = Stopwatch.StartNew();
            var stopping = clients[first!].StopAsync(Ct);
            var second = await WaitForLeaderAsync(ds, schema, l => l is not null && l != first, TimeSpan.FromSeconds(5));
            var handedOver = stopped.Elapsed;
            await stopping;

            Assert.Contains(second, clients.Keys);
            Assert.True(handedOver < TimeSpan.FromSeconds(5), $"Handed over after {handedOver}.");
        }
        finally
        {
            foreach (var client in clients.Values)
            {
                await client.StopAsync(Ct);
            }
        }

        Assert.Null(await LeaderAsync(ds, schema)); // the last leader resigned too
    }

    [Fact]
    public async Task In_poll_only_mode_a_follower_takes_over_at_its_next_attempt_after_the_leader_resigns()
    {
        db.SkipIfUnavailable();
        await using var ds = db.CreateDataSource();
        var (driver, schema) = await MigratedAsync(ds);
        var leader = await StartedAsync(driver, "poll-a", NotificationMode.PollOnly, TimeSpan.FromSeconds(1));
        await WaitForLeaderAsync(ds, schema, l => l == "poll-a", TimeSpan.FromSeconds(5));
        var follower = await StartedAsync(driver, "poll-b", NotificationMode.PollOnly, TimeSpan.FromSeconds(1));
        try
        {
            await Task.Delay(TimeSpan.FromSeconds(1.5), Ct);
            Assert.Equal("poll-a", await LeaderAsync(ds, schema));

            var stopped = Stopwatch.StartNew();
            await leader.StopAsync(Ct);
            await WaitForLeaderAsync(ds, schema, l => l == "poll-b", TimeSpan.FromSeconds(5));

            // The lease lasts 30 s; the follower won at its next 1 s attempt because the resignation freed the lease.
            Assert.True(stopped.Elapsed < TimeSpan.FromSeconds(3), $"Took over after {stopped.Elapsed}.");
        }
        finally
        {
            await leader.StopAsync(Ct);
            await follower.StopAsync(Ct);
        }
    }

    [Fact]
    public async Task A_crashed_leaders_lease_is_taken_over_only_once_it_expires()
    {
        db.SkipIfUnavailable();
        await using var ds = db.CreateDataSource();
        var (driver, schema) = await MigratedAsync(ds);

        // A leader that crashed: its lease runs for 3 more seconds and nobody will resign it.
        await using (var cmd = ds.CreateCommand(
            $"INSERT INTO {schema}.river_leader (leader_id, elected_at, expires_at) VALUES ('crashed', now(), now() + interval '3 seconds')"))
        {
            await cmd.ExecuteNonQueryAsync(Ct);
        }

        var started = Stopwatch.StartNew();
        var client = await StartedAsync(driver, "survivor", NotificationMode.Auto, TimeSpan.FromSeconds(1));
        try
        {
            await Task.Delay(TimeSpan.FromSeconds(1.5), Ct);
            Assert.Equal("crashed", await LeaderAsync(ds, schema));

            await WaitForLeaderAsync(ds, schema, l => l == "survivor", TimeSpan.FromSeconds(10));
            Assert.InRange(started.Elapsed, TimeSpan.FromSeconds(2.5), TimeSpan.FromSeconds(8));
        }
        finally
        {
            await client.StopAsync(Ct);
        }
    }

    [Fact]
    public async Task An_insert_only_client_never_competes_for_the_lease()
    {
        db.SkipIfUnavailable();
        await using var ds = db.CreateDataSource();
        var (driver, schema) = await MigratedAsync(ds);
        var client = new KopiClient(driver, new KopiClientOptions { ClientId = "insert-only", ElectInterval = TimeSpan.FromMilliseconds(100), LeaderTtl = TimeSpan.FromSeconds(1) });

        await client.StartAsync(Ct);
        await Task.Delay(TimeSpan.FromSeconds(0.5), Ct);
        await client.StopAsync(Ct);

        Assert.Null(await LeaderAsync(ds, schema));
    }

    private static async Task<KopiClient> StartedAsync(NpgsqlDriver driver, string clientId, NotificationMode mode, TimeSpan electInterval)
    {
        var client = new KopiClient(driver, new KopiClientOptions
        {
            Queues = new Dictionary<string, QueueOptions> { ["lead"] = new() { MaxWorkers = 1 } },
            Workers = new WorkerRegistry().Add(new IdleWorker()),
            ClientId = clientId,
            NotificationMode = mode,
            ElectInterval = electInterval,
            LeaderTtl = TimeSpan.FromSeconds(30) > electInterval * 2 ? TimeSpan.FromSeconds(30) : electInterval * 2,
        });
        await client.StartAsync(Ct);
        return client;
    }

    private static async Task<string?> LeaderAsync(NpgsqlDataSource ds, string schema)
    {
        await using var cmd = ds.CreateCommand($"SELECT leader_id FROM {schema}.river_leader");
        return await cmd.ExecuteScalarAsync(Ct) as string;
    }

    private static async Task<string?> WaitForLeaderAsync(NpgsqlDataSource ds, string schema, Func<string?, bool> condition, TimeSpan limit)
    {
        var deadline = DateTime.UtcNow + limit;
        while (true)
        {
            var leader = await LeaderAsync(ds, schema);
            if (condition(leader))
            {
                return leader;
            }

            Assert.True(DateTime.UtcNow < deadline, $"The leader is '{leader ?? "nobody"}' after {limit}.");
            await Task.Delay(20, Ct);
        }
    }

    private static async Task<(NpgsqlDriver Driver, string Schema)> MigratedAsync(NpgsqlDataSource ds)
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

    [JobKind("lead_idle")]
    public sealed record IdleArgs : IJobArgs;

    private sealed class IdleWorker : Worker<IdleArgs>
    {
        public override Task WorkAsync(Job<IdleArgs> job, CancellationToken cancellationToken) => Task.CompletedTask;
    }
}
