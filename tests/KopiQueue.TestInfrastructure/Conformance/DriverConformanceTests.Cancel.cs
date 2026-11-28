using KopiQueue.Drivers;
using Xunit;

namespace KopiQueue.TestInfrastructure.Conformance;

/// <summary>External cancel cases (plan task 3.7: FR-ERR-06, FR-ADM-05, FR-DB-05; design §5.8).</summary>
public abstract partial class DriverConformanceTests<TFixture>
{
    [Theory]
    [InlineData(JobState.Available)]
    [InlineData(JobState.Scheduled)]
    [InlineData(JobState.Retryable)]
    [InlineData(JobState.Pending)]
    public async Task A_job_that_has_not_started_is_cancelled_at_once_without_an_error(JobState state)
    {
        var executor = (await MigratedAsync()).Driver.GetExecutor();
        var queue = UniqueQueue();
        var job = (await executor.JobInsertManyAsync(
            [At(UniqueKind(), queue, state, DateTimeOffset.UtcNow.AddSeconds(-1), metadata: """{"tenant": "a"}""")],
            Ct)).Single().Job;

        var cancelled = await executor.JobCancelAsync(job.Id, Ct);

        Assert.NotNull(cancelled);
        Assert.Equal((job.Id, JobState.Cancelled, job.Attempt, job.ScheduledAt), (cancelled.Id, cancelled.State, cancelled.Attempt, cancelled.ScheduledAt));
        Assert.InRange(cancelled.FinalizedAt!.Value, DateTimeOffset.UtcNow.AddMinutes(-1), DateTimeOffset.UtcNow.AddMinutes(1));
        Assert.Empty(cancelled.Errors);
        Assert.Equal("a", Metadata(cancelled, "tenant"));
        Assert.Null(Metadata(cancelled, "cancel_attempted_at"));
        Assert.Equal(JobState.Cancelled, (await executor.JobGetByIdAsync(job.Id, Ct))!.State);
        Assert.Empty(await executor.JobGetAvailableAsync(new JobGetAvailableParams { Queue = queue, Max = 10, ClientId = "c" }, Ct));
    }

    [Theory]
    [InlineData(JobState.Completed)]
    [InlineData(JobState.Cancelled)]
    [InlineData(JobState.Discarded)]
    public async Task A_finalised_job_is_returned_as_it_is(JobState state)
    {
        var executor = (await MigratedAsync()).Driver.GetExecutor();
        var job = await StartOneAsync(executor, UniqueKind(), UniqueQueue());
        var at = Microseconds(DateTimeOffset.UtcNow.AddMinutes(-5));
        var finalized = Assert.Single(await executor.JobSetStateIfRunningManyAsync([new JobSetStateParams { Id = job.Id, State = state, FinalizedAt = at }], Ct));

        var after = await executor.JobCancelAsync(job.Id, Ct);

        Assert.NotNull(after);
        Assert.Equal(
            (finalized.State, finalized.FinalizedAt, finalized.Attempt, finalized.MetadataJson, finalized.Errors.Count),
            (after.State, after.FinalizedAt, after.Attempt, after.MetadataJson, after.Errors.Count));
    }

    [Fact]
    public async Task A_running_job_stays_running_and_is_marked_with_cancel_attempted_at()
    {
        var executor = (await MigratedAsync()).Driver.GetExecutor();
        var queue = UniqueQueue();
        await executor.JobInsertManyAsync([At(UniqueKind(), queue, JobState.Available, DateTimeOffset.UtcNow.AddSeconds(-1), metadata: """{"tenant": "a"}""")], Ct);
        var job = Assert.Single(await executor.JobGetAvailableAsync(new JobGetAvailableParams { Queue = queue, Max = 1, ClientId = "c" }, Ct));

        var marked = await executor.JobCancelAsync(job.Id, Ct);

        Assert.NotNull(marked);
        Assert.Equal((JobState.Running, job.Attempt, job.AttemptedAt, (DateTimeOffset?)null), (marked.State, marked.Attempt, marked.AttemptedAt, marked.FinalizedAt));
        Assert.Empty(marked.Errors);
        Assert.Equal("a", Metadata(marked, "tenant"));
        var asked = DateTimeOffset.Parse(Metadata(marked, "cancel_attempted_at")!, System.Globalization.CultureInfo.InvariantCulture);
        Assert.InRange(asked, DateTimeOffset.UtcNow.AddMinutes(-1), DateTimeOffset.UtcNow.AddMinutes(1));
    }

    [Fact]
    public async Task Cancelling_an_unknown_job_gives_nothing()
    {
        var executor = (await MigratedAsync()).Driver.GetExecutor();

        Assert.Null(await executor.JobCancelAsync(long.MaxValue, Ct));
    }

    [Fact]
    public async Task A_cancel_in_a_transaction_that_rolls_back_changes_nothing()
    {
        var schema = await MigratedAsync();
        var executor = schema.Driver.GetExecutor();
        var waiting = (await executor.JobInsertManyAsync([At(UniqueKind(), UniqueQueue(), JobState.Available, DateTimeOffset.UtcNow.AddSeconds(-1))], Ct)).Single().Job;
        var running = await StartOneAsync(executor, UniqueKind(), UniqueQueue());

        await using (var tx = await schema.BeginTransactionAsync(Ct))
        {
            var inside = schema.Driver.GetExecutor(tx.Transaction);
            Assert.Equal(JobState.Cancelled, (await inside.JobCancelAsync(waiting.Id, Ct))!.State);
            Assert.NotNull(Metadata((await inside.JobCancelAsync(running.Id, Ct))!, "cancel_attempted_at"));
        }

        Assert.Equal(JobState.Available, (await executor.JobGetByIdAsync(waiting.Id, Ct))!.State);
        Assert.Null(Metadata((await executor.JobGetByIdAsync(running.Id, Ct))!, "cancel_attempted_at"));
        Assert.Empty(await executor.JobListCancelAttemptedAsync([running.Id], Ct));
    }

    [Fact]
    public async Task Only_running_jobs_asked_to_cancel_are_listed()
    {
        var executor = (await MigratedAsync()).Driver.GetExecutor();
        var kind = UniqueKind();
        var asked = await StartOneAsync(executor, kind, UniqueQueue());
        var notAsked = await StartOneAsync(executor, kind, UniqueQueue());
        var askedThenDone = await StartOneAsync(executor, kind, UniqueQueue());
        var cancelledBeforeStart = (await executor.JobInsertManyAsync([At(kind, UniqueQueue(), JobState.Available, DateTimeOffset.UtcNow.AddSeconds(-1))], Ct)).Single().Job;
        foreach (var id in new[] { asked.Id, askedThenDone.Id, cancelledBeforeStart.Id })
        {
            await executor.JobCancelAsync(id, Ct);
        }

        await executor.JobSetStateIfRunningManyAsync([new JobSetStateParams { Id = askedThenDone.Id, State = JobState.Completed, FinalizedAt = DateTimeOffset.UtcNow }], Ct);

        Assert.Equal(
            [asked.Id],
            await executor.JobListCancelAttemptedAsync([askedThenDone.Id, notAsked.Id, asked.Id, cancelledBeforeStart.Id, long.MaxValue], Ct));
        Assert.Empty(await executor.JobListCancelAttemptedAsync([], Ct));
    }

    [Fact]
    public async Task A_cancel_ask_is_the_top_level_key_whatever_its_value()
    {
        // As PostgreSQL's jsonb ? operator: the key's presence at the top level counts, even with a null value (written
        // by another tool), and the same key nested deeper does not.
        var executor = (await MigratedAsync()).Driver.GetExecutor();
        var kind = UniqueKind();
        var queue = UniqueQueue();
        var past = DateTimeOffset.UtcNow.AddSeconds(-1);
        var nullValue = (await executor.JobInsertManyAsync([At(kind, queue, JobState.Available, past, metadata: """{"cancel_attempted_at": null}""")], Ct)).Single().Job;
        await executor.JobInsertManyAsync([At(kind, queue, JobState.Available, past, metadata: """{"x": {"cancel_attempted_at": "2026-01-01T00:00:00Z"}}""")], Ct);
        var running = await executor.JobGetAvailableAsync(new JobGetAvailableParams { Queue = queue, Max = 10, ClientId = "c" }, Ct);

        Assert.Equal([nullValue.Id], await executor.JobListCancelAttemptedAsync([.. running.Select(j => j.Id).Order()], Ct));
    }

    [Fact]
    public async Task After_a_cancel_a_completed_cancelled_or_discarded_outcome_stands_and_a_retry_becomes_a_cancel()
    {
        var executor = (await MigratedAsync()).Driver.GetExecutor();
        var kind = UniqueKind();
        var jobs = new List<JobRow>();
        for (var i = 0; i < 4; i++)
        {
            var job = await StartOneAsync(executor, kind, UniqueQueue());
            await executor.JobCancelAsync(job.Id, Ct);
            jobs.Add(job);
        }

        var at = Microseconds(DateTimeOffset.UtcNow);
        var error = new AttemptError(at, 1, "boom", null);
        var rows = await executor.JobSetStateIfRunningManyAsync(
            [
                new JobSetStateParams { Id = jobs[0].Id, State = JobState.Completed, FinalizedAt = at },
                new JobSetStateParams { Id = jobs[1].Id, State = JobState.Cancelled, FinalizedAt = at, Error = error },
                new JobSetStateParams { Id = jobs[2].Id, State = JobState.Discarded, FinalizedAt = at, Error = error },
                new JobSetStateParams { Id = jobs[3].Id, State = JobState.Retryable, ScheduledAt = Later, Error = error },
            ],
            Ct);

        Assert.Equal([JobState.Completed, JobState.Cancelled, JobState.Discarded, JobState.Cancelled], rows.Select(r => r.State));
        Assert.All(rows.Take(3), r => Assert.Equal(at, r.FinalizedAt));
        Assert.NotNull(rows[3].FinalizedAt);
        Assert.Equal([error], rows[3].Errors);
    }
}
