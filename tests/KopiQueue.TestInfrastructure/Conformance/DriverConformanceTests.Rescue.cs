using KopiQueue.Drivers;
using Xunit;

namespace KopiQueue.TestInfrastructure.Conformance;

/// <summary>Rescuer cases (plan task 3.4: FR-MNT-03, NFR-REL-02; design §5.7).</summary>
/// <remarks>
/// Listing looks at every running job of the schema, so these cases run in a schema of their own. A job's attempt
/// starts when it is fetched, by the database's clock, so a case makes a job "old" by waiting past a 1 s threshold.
/// </remarks>
public abstract partial class DriverConformanceTests<TFixture>
{
    private static readonly TimeSpan StuckAfter = TimeSpan.FromSeconds(1);

    [Fact]
    public async Task Stuck_candidates_are_running_jobs_whose_attempt_is_older_than_the_threshold_oldest_first()
    {
        var executor = (await IsolatedSchemaAsync()).Driver.GetExecutor();
        var kind = UniqueKind();
        var (queue, other) = (UniqueQueue(), UniqueQueue());
        var first = await StartOneAsync(executor, kind, queue);
        var second = await StartOneAsync(executor, kind, queue);
        var completed = await StartOneAsync(executor, kind, other);
        await executor.JobSetStateIfRunningManyAsync(
            [new JobSetStateParams { Id = completed.Id, State = JobState.Completed, FinalizedAt = Microseconds(DateTimeOffset.UtcNow) }],
            Ct);
        var past = Microseconds(DateTimeOffset.UtcNow.AddHours(-2));
        await executor.JobInsertManyAsync(
            [At(kind, other, JobState.Available, past), At(kind, other, JobState.Retryable, past), At(kind, other, JobState.Scheduled, past)],
            Ct);
        await Task.Delay(StuckAfter + TimeSpan.FromMilliseconds(300), Ct);
        var fresh = await StartOneAsync(executor, kind, other);

        var all = await executor.JobListStuckAsync(new JobListStuckParams { StuckAfter = StuckAfter, Max = 100 }, Ct);
        var one = await executor.JobListStuckAsync(new JobListStuckParams { StuckAfter = StuckAfter, Max = 1 }, Ct);

        Assert.Equal([first.Id, second.Id], all.Jobs.Select(j => j.Id));
        Assert.All(all.Jobs, job => Assert.Equal(JobState.Running, job.State));
        Assert.Equal(first.AttemptedAt, all.Jobs[0].AttemptedAt);
        Assert.Equal([first.Id], one.Jobs.Select(j => j.Id));
        Assert.InRange(all.Now, fresh.AttemptedAt!.Value, fresh.AttemptedAt.Value.AddMinutes(1));
        Assert.Equal(JobState.Running, (await executor.JobGetByIdAsync(first.Id, Ct))!.State); // listing changes nothing
    }

    [Fact]
    public async Task With_nothing_stuck_the_list_is_empty_and_still_gives_the_databases_now()
    {
        var executor = (await IsolatedSchemaAsync()).Driver.GetExecutor();

        var stuck = await executor.JobListStuckAsync(new JobListStuckParams { StuckAfter = StuckAfter, Max = 10 }, Ct);

        Assert.Empty(stuck.Jobs);
        Assert.InRange(stuck.Now, DateTimeOffset.UtcNow.AddMinutes(-5), DateTimeOffset.UtcNow.AddMinutes(5));
    }

    [Fact]
    public async Task A_rescue_leaves_alone_a_job_that_finished_or_was_fetched_again_after_it_was_read()
    {
        var executor = (await IsolatedSchemaAsync()).Driver.GetExecutor();
        var kind = UniqueKind();
        var queue = UniqueQueue();
        var finished = await StartOneAsync(executor, kind, queue);
        var refetched = await StartOneAsync(executor, kind, queue);
        var stuck = await StartOneAsync(executor, kind, queue);
        await Task.Delay(StuckAfter + TimeSpan.FromMilliseconds(300), Ct);
        var read = await executor.JobListStuckAsync(new JobListStuckParams { StuckAfter = StuckAfter, Max = 100 }, Ct);
        Assert.Equal([finished.Id, refetched.Id, stuck.Id], read.Jobs.Select(j => j.Id));

        // After the read: one job completes, another is interrupted and fetched again, so its attempt starts anew.
        await executor.JobSetStateIfRunningManyAsync(
            [
                new JobSetStateParams { Id = finished.Id, State = JobState.Completed, FinalizedAt = Microseconds(DateTimeOffset.UtcNow) },
                new JobSetStateParams { Id = refetched.Id, State = JobState.Available, Attempt = 0 },
            ],
            Ct);
        var again = Assert.Single(await executor.JobGetAvailableAsync(new JobGetAvailableParams { Queue = queue, Max = 10, ClientId = "c2" }, Ct));
        Assert.Equal(refetched.Id, again.Id);

        var rescued = await executor.JobRescueManyAsync([.. read.Jobs.Select(j => Retry(j, read.Now))], Ct);

        Assert.Equal([stuck.Id], rescued.Select(j => j.Id));
        Assert.Equal(JobState.Completed, (await executor.JobGetByIdAsync(finished.Id, Ct))!.State);
        var running = (await executor.JobGetByIdAsync(refetched.Id, Ct))!;
        Assert.Equal((JobState.Running, again.AttemptedAt), (running.State, running.AttemptedAt));
        Assert.Empty(running.Errors);
    }

    [Fact]
    public async Task A_rescue_retries_or_discards_writing_the_right_columns_and_exactly_one_error()
    {
        var executor = (await IsolatedSchemaAsync()).Driver.GetExecutor();
        var kind = UniqueKind();
        var queue = UniqueQueue();
        var jobs = new[] { await StartOneAsync(executor, kind, queue), await StartOneAsync(executor, kind, queue), await StartOneAsync(executor, kind, queue) };
        var now = Microseconds(DateTimeOffset.UtcNow);
        var later = Microseconds(now.AddMinutes(30));
        var soon = Microseconds(now.AddSeconds(2));
        AttemptError Stuck(JobRow job) => new(now, job.Attempt, "rescued: still running after the threshold", null);

        var rescued = await executor.JobRescueManyAsync(
            [
                new JobRescueParams { Id = jobs[0].Id, AttemptedAt = jobs[0].AttemptedAt!.Value, State = JobState.Retryable, ScheduledAt = later, Error = Stuck(jobs[0]) },
                new JobRescueParams { Id = jobs[1].Id, AttemptedAt = jobs[1].AttemptedAt!.Value, State = JobState.Available, ScheduledAt = soon, Error = Stuck(jobs[1]) },
                new JobRescueParams { Id = jobs[2].Id, AttemptedAt = jobs[2].AttemptedAt!.Value, State = JobState.Discarded, Error = Stuck(jobs[2]) },
            ],
            Ct);

        Assert.Equal(jobs.Select(j => j.Id), rescued.Select(j => j.Id));
        Assert.Equal(
            [(JobState.Retryable, later), (JobState.Available, soon), (JobState.Discarded, jobs[2].ScheduledAt)],
            rescued.Select(j => (j.State, j.ScheduledAt)));
        Assert.All(rescued.Take(2), j => Assert.Null(j.FinalizedAt));
        Assert.InRange(rescued[2].FinalizedAt!.Value, now.AddMinutes(-5), now.AddMinutes(5));
        for (var i = 0; i < jobs.Length; i++)
        {
            var stored = (await executor.JobGetByIdAsync(jobs[i].Id, Ct))!;
            Assert.Equal(rescued[i].State, stored.State);
            Assert.Equal((jobs[i].Attempt, jobs[i].AttemptedAt), (stored.Attempt, stored.AttemptedAt)); // the stuck attempt counts
            Assert.Equal(jobs[i].AttemptedBy, stored.AttemptedBy);
            Assert.Equal([Stuck(jobs[i])], stored.Errors);
        }

        // Already rescued: none is running any more, so a second rescue changes nothing.
        Assert.Empty(await executor.JobRescueManyAsync([Retry(jobs[0], now)], Ct));
    }

    [Fact]
    public async Task A_rescue_skips_a_job_another_transaction_holds_without_waiting()
    {
        var schema = await IsolatedSchemaAsync();
        var executor = schema.Driver.GetExecutor();
        var kind = UniqueKind();
        var queue = UniqueQueue();
        var held = await StartOneAsync(executor, kind, queue);
        var free = await StartOneAsync(executor, kind, queue);
        var now = Microseconds(DateTimeOffset.UtcNow);

        await using (var holder = await schema.BeginTransactionAsync(Ct))
        {
            // A completion in flight: it locks the row until it commits or rolls back.
            await schema.Driver.GetExecutor(holder.Transaction).JobSetStateIfRunningManyAsync(
                [new JobSetStateParams { Id = held.Id, State = JobState.Completed, FinalizedAt = now }],
                Ct);

            var whileHeld = await executor.JobRescueManyAsync([Retry(held, now), Retry(free, now)], Ct).WaitAsync(TimeSpan.FromSeconds(10), Ct);

            Assert.Equal([free.Id], whileHeld.Select(j => j.Id));
        }

        // The completion rolled back, so the job is still running the same attempt and can be rescued.
        Assert.Equal([held.Id], (await executor.JobRescueManyAsync([Retry(held, now)], Ct)).Select(j => j.Id));
    }

    [Theory]
    [InlineData(JobState.Running)]
    [InlineData(JobState.Completed)]
    [InlineData(JobState.Scheduled)]
    public async Task A_rescue_to_another_state_is_rejected(JobState state)
    {
        var schema = await MigratedAsync();

        await Assert.ThrowsAsync<ArgumentException>(() => schema.Driver.GetExecutor().JobRescueManyAsync(
            [new JobRescueParams { Id = 1, AttemptedAt = DateTimeOffset.UtcNow, State = state, Error = new AttemptError(DateTimeOffset.UtcNow, 1, "x", null) }],
            Ct));
    }

    /// <summary>Inserts one job into <paramref name="queue"/> and fetches it, so it is running its first attempt.</summary>
    private static async Task<JobRow> StartOneAsync(IKopiExecutor executor, string kind, string queue)
    {
        var id = (await executor.JobInsertManyAsync([Job(kind, queue)], Ct)).Single().Job.Id;
        var fetched = await executor.JobGetAvailableAsync(new JobGetAvailableParams { Queue = queue, Max = 100, ClientId = "c1" }, Ct);
        return Assert.Single(fetched, j => j.Id == id);
    }

    private static JobRescueParams Retry(JobRow job, DateTimeOffset now) => new()
    {
        Id = job.Id,
        AttemptedAt = job.AttemptedAt!.Value,
        State = JobState.Retryable,
        ScheduledAt = now.AddMinutes(10),
        Error = new AttemptError(now, job.Attempt, "rescued", null),
    };
}
