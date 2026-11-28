using System.Diagnostics;
using KopiQueue.Drivers;
using Xunit;

namespace KopiQueue.TestInfrastructure.Integration;

/// <summary>
/// Leader election between working clients on a real database (plan task 3.1: FR-MNT-01; design §5.5): one leader at a
/// time, an immediate handover when the leader stops, and a takeover once a crashed leader's lease expires.
/// </summary>
public abstract class LeadershipTests<TFixture>(TFixture db)
    where TFixture : IntegrationFixture
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Fact]
    public async Task Of_several_working_clients_one_leads_and_a_stopping_leader_hands_over_through_its_resignation()
    {
        await using var schema = await db.CreateSchemaAsync(Ct);
        Assert.SkipUnless(db.HasNotifications, "The driver has no notifications: the poll-only test covers its handover.");
        var driver = schema.Driver;

        // A 20 s interval: a follower taking over within a few seconds can only have been woken by the resignation.
        var clients = new Dictionary<string, KopiClient>(StringComparer.Ordinal);
        foreach (var id in new[] { "lead-a", "lead-b", "lead-c" })
        {
            clients[id] = await StartedAsync(driver, id, NotificationMode.Auto, TimeSpan.FromSeconds(20));
        }

        try
        {
            var first = await schema.WaitForLeaderAsync(l => l is not null, TimeSpan.FromSeconds(5), Ct);
            Assert.Contains(first, clients.Keys);
            await Task.Delay(TimeSpan.FromSeconds(1), Ct);
            Assert.Equal(first, await schema.LeaderAsync(Ct)); // nobody else took it

            // Time the handover, not the whole stop: the stop resigns first and only then closes the notifier, which may take
            // up to its DisposeTimeout (5 s) on a busy machine. So wait for the new leader while the stop is still running.
            var stopped = Stopwatch.StartNew();
            var stopping = clients[first!].StopAsync(Ct);
            var second = await schema.WaitForLeaderAsync(l => l is not null && l != first, TimeSpan.FromSeconds(5), Ct);
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

        Assert.Null(await schema.LeaderAsync(Ct)); // the last leader resigned too
    }

    [Fact]
    public async Task In_poll_only_mode_a_follower_takes_over_at_its_next_attempt_after_the_leader_resigns()
    {
        await using var schema = await db.CreateSchemaAsync(Ct);
        var driver = schema.Driver;
        var leader = await StartedAsync(driver, "poll-a", NotificationMode.PollOnly, TimeSpan.FromSeconds(1));
        await schema.WaitForLeaderAsync(l => l == "poll-a", TimeSpan.FromSeconds(5), Ct);
        var follower = await StartedAsync(driver, "poll-b", NotificationMode.PollOnly, TimeSpan.FromSeconds(1));
        try
        {
            await Task.Delay(TimeSpan.FromSeconds(1.5), Ct);
            Assert.Equal("poll-a", await schema.LeaderAsync(Ct));

            var stopped = Stopwatch.StartNew();
            await leader.StopAsync(Ct);
            await schema.WaitForLeaderAsync(l => l == "poll-b", TimeSpan.FromSeconds(5), Ct);

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
        await using var schema = await db.CreateSchemaAsync(Ct);
        var driver = schema.Driver;

        // A leader that crashed: its lease runs for 3 more seconds and nobody will resign it.
        await schema.SetLeaderAsync("crashed", TimeSpan.FromSeconds(3), Ct);

        var started = Stopwatch.StartNew();
        var client = await StartedAsync(driver, "survivor", NotificationMode.Auto, TimeSpan.FromSeconds(1));
        try
        {
            await Task.Delay(TimeSpan.FromSeconds(1.5), Ct);
            Assert.Equal("crashed", await schema.LeaderAsync(Ct));

            await schema.WaitForLeaderAsync(l => l == "survivor", TimeSpan.FromSeconds(10), Ct);
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
        await using var schema = await db.CreateSchemaAsync(Ct);
        var driver = schema.Driver;
        var client = new KopiClient(driver, new KopiClientOptions { ClientId = "insert-only", ElectInterval = TimeSpan.FromMilliseconds(100), LeaderTtl = TimeSpan.FromSeconds(1) });

        await client.StartAsync(Ct);
        await Task.Delay(TimeSpan.FromSeconds(0.5), Ct);
        await client.StopAsync(Ct);

        Assert.Null(await schema.LeaderAsync(Ct));
    }

    private static async Task<KopiClient> StartedAsync(IKopiDriver driver, string clientId, NotificationMode mode, TimeSpan electInterval)
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

    [JobKind("lead_idle")]
    public sealed record IdleArgs : IJobArgs;

    private sealed class IdleWorker : Worker<IdleArgs>
    {
        public override Task WorkAsync(Job<IdleArgs> job, CancellationToken cancellationToken) => Task.CompletedTask;
    }
}
