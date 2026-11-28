using KopiQueue.Drivers;
using Xunit;

namespace KopiQueue.TestInfrastructure.Integration;

/// <summary>
/// Periodic jobs on a real database (plan task 4.3: FR-PER-02, FR-PER-04; UC-3): a cluster inserts each tick once,
/// whichever client leads, and a change of leader neither repeats a tick nor skips more than one.
/// </summary>
public abstract class PeriodicTests<TFixture>(TFixture db)
    where TFixture : IntegrationFixture
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Fact]
    public async Task A_cluster_inserts_each_tick_once_across_a_change_of_leader()
    {
        // Every second, so the test is short. How many ticks a change of leader skips depends on how long leadership is
        // vacant, which a loaded machine can stretch past a second, so this test checks what holds under any load: no
        // tick twice, and ticks from both leaders. The catch-up of a skipped tick is checked by the next test and the
        // enqueuer's unit tests.
        await using var schema = await db.CreateSchemaAsync(Ct);
        var driver = schema.Driver;
        var started = DateTimeOffset.UtcNow;

        var a = await StartedAsync(driver, "node-a");
        var b = await StartedAsync(driver, "node-b");
        var stopped = false;
        try
        {
            var leader = await schema.WaitForLeaderAsync(l => l is not null, TimeSpan.FromSeconds(20), Ct);
            await WaitForAsync(schema, 2);
            var (first, second, next) = leader == "node-a" ? (a, b, "node-b") : (b, a, "node-a");

            var switchedAt = await schema.NowAsync(Ct);
            await first.StopAsync(Ct); // resigns; the other client takes over
            Assert.Equal(next, await schema.WaitForLeaderAsync(l => l == next, TimeSpan.FromSeconds(20), Ct));
            await WaitForAsync(schema, 2, after: switchedAt);
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
        var jobs = (await schema.JobsAsync(Ct)).Where(j => j.Kind == "every_second").ToList();
        var keys = new HashSet<string>();
        foreach (var job in jobs)
        {
            keys.Add(Convert.ToHexString((await schema.UniqueAsync(job.Id, Ct)).Key ?? []));
        }

        Assert.Equal(jobs.Count, keys.Count); // no tick twice (FR-PER-04)
        Assert.All(jobs, j => Assert.True(IsPeriodic(j)));
        Assert.InRange(jobs.Count, 4, (int)Math.Ceiling(elapsed.TotalSeconds) + 1); // never more jobs than ticks
    }

    [Fact]
    public async Task A_new_leader_skips_a_tick_the_previous_leader_inserted()
    {
        await using var schema = await db.CreateSchemaAsync(Ct);
        var driver = schema.Driver;

        // Every 10 minutes, with a catch-up window (the lease plus the election interval) over 10 minutes: the latest
        // tick is always recent enough to catch up, so each new leader inserts it, and only the first insert stays.
        var schedule = PeriodicSchedule.Every(TimeSpan.FromMinutes(10));
        var job = PeriodicJob.Create(schedule, () => new EverySecondArgs());
        var start = DateTimeOffset.UtcNow;
        var first = await StartedAsync(driver, "node-a", job, leaderTtl: TimeSpan.FromMinutes(10));
        await WaitForAsync(schema, 1);
        await first.StopAsync(Ct);

        var second = await StartedAsync(driver, "node-b", job, leaderTtl: TimeSpan.FromMinutes(10));
        try
        {
            Assert.Equal("node-b", await schema.WaitForLeaderAsync(l => l == "node-b", TimeSpan.FromSeconds(20), Ct));
            await Task.Delay(TimeSpan.FromSeconds(1), Ct);
        }
        finally
        {
            await second.StopAsync(Ct);
        }

        // One job for the tick both leaders caught up, plus one for each tick that came due during the test (rarely any).
        var expected = 1;
        for (var tick = schedule.NextAfter(start)!.Value; tick <= DateTimeOffset.UtcNow; tick = schedule.NextAfter(tick)!.Value)
        {
            expected++;
        }

        Assert.Equal(expected, await schema.CountAsync(j => j.Kind == "every_second", Ct));
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
    private static Task WaitForAsync(IntegrationSchema schema, int jobs, DateTimeOffset? after = null) => schema.WaitForCountAsync(
        j => j.Kind == "every_second" && (after is null || j.CreatedAt >= after), jobs, TimeSpan.FromSeconds(20), Ct);

    private static bool IsPeriodic(JobRow job)
    {
        using var metadata = System.Text.Json.JsonDocument.Parse(job.MetadataJson);
        return metadata.RootElement.TryGetProperty("periodic", out var periodic) && periodic.ValueKind == System.Text.Json.JsonValueKind.True;
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
