using System.Collections.Concurrent;
using KopiQueue.Drivers;
using Xunit;

namespace KopiQueue.TestInfrastructure.Conformance;

/// <summary>Fetch cases (plan task 2.3: FR-EXE-01, FR-EXE-02).</summary>
public abstract partial class DriverConformanceTests<TFixture>
{
    [Fact]
    public async Task Fetch_starts_the_due_available_jobs_of_one_queue_by_priority_then_time_then_id()
    {
        var schema = await MigratedAsync();
        await using var tx = await schema.BeginTransactionAsync(Ct);
        var executor = schema.Driver.GetExecutor(tx.Transaction);
        var queue = UniqueQueue();
        var kind = UniqueKind();
        var past = Microseconds(DateTimeOffset.UtcNow.AddHours(-2));

        JobInsertParams At(int priority, DateTimeOffset at, string? inQueue = null, JobState state = JobState.Available) =>
            new() { Kind = kind, ArgsJson = "{}", Queue = inQueue ?? queue, State = state, Priority = priority, MaxAttempts = 25, ScheduledAt = at };

        var inserted = (await executor.JobInsertManyAsync(
            [
                At(2, past),                            // 0: lower priority
                At(1, past.AddMinutes(30)),             // 1: later
                At(1, past),                            // 2: first by id among equals
                At(1, past),                            // 3
                At(1, DateTimeOffset.UtcNow.AddHours(1)), // not due yet
                At(1, past, state: JobState.Retryable), // not available
                At(1, past, state: JobState.Scheduled), // not available
                At(1, past, inQueue: UniqueQueue()),    // another queue
            ],
            Ct)).Select(r => r.Job.Id).ToArray();

        var fetched = await executor.JobGetAvailableAsync(new JobGetAvailableParams { Queue = queue, Max = 100, ClientId = "client-a" }, Ct);

        Assert.Equal([inserted[2], inserted[3], inserted[1], inserted[0]], fetched.Select(j => j.Id));
        Assert.All(fetched, job =>
        {
            Assert.Equal((JobState.Running, 1, queue), (job.State, job.Attempt, job.Queue));
            Assert.NotNull(job.AttemptedAt);
            Assert.InRange(job.AttemptedAt.Value, DateTimeOffset.UtcNow.AddMinutes(-5), DateTimeOffset.UtcNow.AddMinutes(5));
            Assert.Equal(["client-a"], job.AttemptedBy);
            Assert.Null(job.FinalizedAt);
        });
        var stored = await executor.JobGetByIdAsync(inserted[2], Ct);
        Assert.Equal((JobState.Running, 1), (stored!.State, stored.Attempt));
        Assert.Equal(JobState.Available, (await executor.JobGetByIdAsync(inserted[4], Ct))!.State);
        Assert.Empty(await executor.JobGetAvailableAsync(new JobGetAvailableParams { Queue = queue, Max = 100, ClientId = "client-a" }, Ct));
    }

    [Fact]
    public async Task Fetch_takes_at_most_max_jobs_and_leaves_the_rest_for_the_next_fetch()
    {
        var schema = await MigratedAsync();
        await using var tx = await schema.BeginTransactionAsync(Ct);
        var executor = schema.Driver.GetExecutor(tx.Transaction);
        var queue = UniqueQueue();
        var kind = UniqueKind();
        var ids = (await executor.JobInsertManyAsync([.. Enumerable.Range(0, 5).Select(_ => Job(kind, queue))], Ct)).Select(r => r.Job.Id).ToArray();
        var fetch = new JobGetAvailableParams { Queue = queue, Max = 3, ClientId = "c" };

        var first = await executor.JobGetAvailableAsync(fetch, Ct);
        var second = await executor.JobGetAvailableAsync(fetch, Ct);
        var third = await executor.JobGetAvailableAsync(fetch, Ct);

        Assert.Equal(ids[..3], first.Select(j => j.Id));
        Assert.Equal(ids[3..], second.Select(j => j.Id));
        Assert.Empty(third);
    }

    [Fact]
    public async Task Concurrent_fetchers_never_get_the_same_job()
    {
        var schema = await MigratedAsync();
        var queue = UniqueQueue();
        var kind = UniqueKind();
        const int Jobs = 300;
        await schema.Driver.GetExecutor().JobInsertManyAsync([.. Enumerable.Range(0, Jobs).Select(_ => Job(kind, queue))], Ct);
        var fetched = new ConcurrentBag<long>();

        await Parallel.ForEachAsync(Enumerable.Range(0, 8), Ct, async (fetcher, ct) =>
        {
            var parameters = new JobGetAvailableParams { Queue = queue, Max = 7, ClientId = $"fetcher-{fetcher}" };
            while (true)
            {
                var jobs = await schema.Driver.GetExecutor().JobGetAvailableAsync(parameters, ct);
                if (jobs.Count == 0)
                {
                    return;
                }

                foreach (var job in jobs)
                {
                    fetched.Add(job.Id);
                }
            }
        });

        Assert.Equal(Jobs, fetched.Count);
        Assert.Equal(Jobs, fetched.Distinct().Count());
    }

    [Theory]
    [InlineData(0, 100)]
    [InlineData(1, 0)]
    public async Task Fetch_rejects_a_max_or_attempted_by_limit_below_one(int max, int maxAttemptedBy)
    {
        var schema = await MigratedAsync();

        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(() => schema.Driver.GetExecutor().JobGetAvailableAsync(
            new JobGetAvailableParams { Queue = "default", Max = max, ClientId = "c", MaxAttemptedBy = maxAttemptedBy }, Ct));
    }

    /// <summary>A queue no other test uses: fetch cases share the migrated schema.</summary>
    protected static string UniqueQueue() => "q" + Guid.NewGuid().ToString("N")[..16];
}
