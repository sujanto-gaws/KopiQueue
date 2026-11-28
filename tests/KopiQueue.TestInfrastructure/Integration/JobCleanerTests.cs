using KopiQueue.Drivers;
using Xunit;

namespace KopiQueue.TestInfrastructure.Integration;

/// <summary>
/// The job cleaner on a real database (plan task 3.5: FR-MNT-04, FR-MNT-07; design §5.7): only the leader deletes
/// finalised jobs past their retention, no retention keeps everything, and deleting a job frees its unique slot.
/// </summary>
public abstract class JobCleanerTests<TFixture>(TFixture db)
    where TFixture : IntegrationFixture
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Fact]
    public async Task A_follower_deletes_nothing_and_the_leader_deletes_expired_jobs_as_soon_as_it_wins()
    {
        var schema = await db.CreateSchemaAsync(Ct);
        var driver = schema.Driver;
        await schema.SetLeaderAsync("someone-else", TimeSpan.FromHours(1), Ct);

        var executor = driver.GetExecutor();
        var expired = new[]
        {
            await FinalizedAsync(executor, JobState.Completed, TimeSpan.FromHours(25)),
            await FinalizedAsync(executor, JobState.Cancelled, TimeSpan.FromHours(25)),
            await FinalizedAsync(executor, JobState.Discarded, TimeSpan.FromDays(8)),
        };
        var kept = new[]
        {
            await FinalizedAsync(executor, JobState.Completed, TimeSpan.FromHours(1)),
            await FinalizedAsync(executor, JobState.Discarded, TimeSpan.FromDays(6)),
            (await executor.JobInsertManyAsync([Job("parked", scheduledAt: DateTimeOffset.UtcNow.AddYears(-1))], Ct)).Single().Job.Id,
        };

        var client = await StartedAsync(driver, "follower");
        try
        {
            // Many election attempts as a follower: nothing is deleted.
            await Task.Delay(TimeSpan.FromSeconds(1.5), Ct);
            Assert.Equal("someone-else", await schema.LeaderAsync(Ct));
            Assert.Equal([.. expired, .. kept], await PresentAsync(executor, [.. expired, .. kept]));

            await schema.DeleteLeaderAsync(Ct);

            var deadline = DateTime.UtcNow.AddSeconds(10);
            while ((await PresentAsync(executor, expired)).Count > 0)
            {
                Assert.True(DateTime.UtcNow < deadline, "The expired jobs are still there 10 s after the lease was freed.");
                await Task.Delay(50, Ct);
            }

            Assert.Equal("follower", await schema.LeaderAsync(Ct));
            Assert.Equal(kept, await PresentAsync(executor, kept));
        }
        finally
        {
            await client.StopAsync(Ct);
        }
    }

    [Fact]
    public async Task An_insert_only_client_deletes_nothing()
    {
        var schema = await db.CreateSchemaAsync(Ct);
        var driver = schema.Driver;
        var executor = driver.GetExecutor();
        var expired = await FinalizedAsync(executor, JobState.Completed, TimeSpan.FromDays(2));

        var client = new KopiClient(driver);
        await client.StartAsync(Ct);
        try
        {
            await Task.Delay(TimeSpan.FromSeconds(1), Ct);

            Assert.NotNull(await executor.JobGetByIdAsync(expired, Ct));
            Assert.Null(await schema.LeaderAsync(Ct));
        }
        finally
        {
            await client.StopAsync(Ct);
        }
    }

    [Fact]
    public async Task With_no_retention_the_leader_keeps_every_finalised_job()
    {
        var schema = await db.CreateSchemaAsync(Ct);
        var driver = schema.Driver;
        var executor = driver.GetExecutor();
        long[] ancient =
        [
            await FinalizedAsync(executor, JobState.Completed, TimeSpan.FromDays(400)),
            await FinalizedAsync(executor, JobState.Cancelled, TimeSpan.FromDays(400)),
            await FinalizedAsync(executor, JobState.Discarded, TimeSpan.FromDays(400)),
        ];

        var client = await StartedAsync(driver, "keeper", keepAll: true);
        try
        {
            await schema.WaitForLeaderAsync(l => l is not null, TimeSpan.FromSeconds(5), Ct);

            await Task.Delay(TimeSpan.FromSeconds(1), Ct);
            Assert.Equal(ancient, await PresentAsync(executor, ancient));
        }
        finally
        {
            await client.StopAsync(Ct);
        }
    }

    [Fact]
    public async Task Deleting_a_completed_unique_job_frees_its_slot_for_a_duplicate()
    {
        var schema = await db.CreateSchemaAsync(Ct);
        var driver = schema.Driver;
        var executor = driver.GetExecutor();

        // Unique inserts arrive with M4 (#42), so the keys are set directly; the pinned schema's unique index enforces them.
        var completed = await FinalizedAsync(executor, JobState.Completed, TimeSpan.FromDays(2));
        await SetUniqueAsync(schema, completed);
        var duplicate = (await executor.JobInsertManyAsync([Job("cleaner_work")], Ct)).Single().Job.Id;

        var taken = await Assert.ThrowsAnyAsync<Exception>(() => SetUniqueAsync(schema, duplicate));
        Assert.True(schema.IsUniqueViolation(taken), taken.Message); // the completed job holds the slot

        Assert.Equal([completed], await executor.JobDeleteFinalizedAsync(new JobDeleteFinalizedParams { CompletedRetention = TimeSpan.FromDays(1), Max = 10 }, Ct));

        await SetUniqueAsync(schema, duplicate);
    }

    /// <summary>Gives job <paramref name="id"/> a fixed unique key under the default unique states (design §5.6), which include completed.</summary>
    private static Task SetUniqueAsync(IntegrationSchema schema, long id) =>
        schema.SetUniqueAsync(id, [.. Enumerable.Range(1, 32).Select(i => (byte)i)], 0b1111_0101, Ct);

    private static async Task<KopiClient> StartedAsync(IKopiDriver driver, string clientId, bool keepAll = false)
    {
        var client = new KopiClient(driver, new KopiClientOptions
        {
            Queues = new Dictionary<string, QueueOptions> { ["cleaner_work"] = new() { MaxWorkers = 1 } },
            Workers = new WorkerRegistry().Add(new CleanerWorker()),
            ClientId = clientId,
            ElectInterval = TimeSpan.FromMilliseconds(200),
            LeaderTtl = TimeSpan.FromSeconds(2),
            CompletedJobRetention = keepAll ? null : TimeSpan.FromHours(24),
            CancelledJobRetention = keepAll ? null : TimeSpan.FromHours(24),
            DiscardedJobRetention = keepAll ? null : TimeSpan.FromDays(7),
        });
        await client.StartAsync(Ct);
        return client;
    }

    /// <summary>Inserts a job in a queue of its own, fetches it and finalises it as <paramref name="state"/>, <paramref name="age"/> ago.</summary>
    private static async Task<long> FinalizedAsync(IKopiExecutor executor, JobState state, TimeSpan age)
    {
        var queue = "q_" + Guid.NewGuid().ToString("N")[..12];
        var id = (await executor.JobInsertManyAsync([Job(queue)], Ct)).Single().Job.Id;
        Assert.Single(await executor.JobGetAvailableAsync(new JobGetAvailableParams { Queue = queue, Max = 1, ClientId = "finisher" }, Ct));
        var row = Assert.Single(await executor.JobSetStateIfRunningManyAsync(
            [new JobSetStateParams { Id = id, State = state, FinalizedAt = DateTimeOffset.UtcNow - age }],
            Ct));
        Assert.Equal(state, row.State);
        return id;
    }

    private static JobInsertParams Job(string queue, DateTimeOffset? scheduledAt = null) => new()
    {
        Kind = "cleaner_job", ArgsJson = "{}", Queue = queue, State = scheduledAt is null ? JobState.Available : JobState.Scheduled,
        Priority = 1, MaxAttempts = 25, ScheduledAt = scheduledAt ?? DateTimeOffset.UtcNow.AddSeconds(-1),
    };

    /// <summary>Which of <paramref name="ids"/> still exist, in the given order.</summary>
    private static async Task<List<long>> PresentAsync(IKopiExecutor executor, long[] ids)
    {
        var present = new List<long>();
        foreach (var id in ids)
        {
            if (await executor.JobGetByIdAsync(id, Ct) is not null)
            {
                present.Add(id);
            }
        }

        return present;
    }

    [JobKind("cleaner_job")]
    public sealed record CleanerArgs : IJobArgs;

    private sealed class CleanerWorker : Worker<CleanerArgs>
    {
        public override Task WorkAsync(Job<CleanerArgs> job, CancellationToken cancellationToken) => Task.CompletedTask;
    }
}
