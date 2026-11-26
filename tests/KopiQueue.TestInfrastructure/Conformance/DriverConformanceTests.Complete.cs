using KopiQueue.Drivers;
using Xunit;

namespace KopiQueue.TestInfrastructure.Conformance;

/// <summary>Completion cases (plan task 2.6: FR-EXE-06, NFR-REL-03; design §5.3, §5.8).</summary>
public abstract partial class DriverConformanceTests<TFixture>
{
    private static readonly DateTimeOffset Later = new(2031, 2, 3, 4, 5, 6, TimeSpan.Zero);

    [Fact]
    public async Task Complete_finalises_running_jobs_and_keeps_their_attempt_and_errors()
    {
        var (executor, tx, jobs) = await RunningAsync(1);
        await using var _ = tx;
        var finalized = Microseconds(DateTimeOffset.UtcNow);

        var row = Assert.Single(await executor.JobSetStateIfRunningManyAsync(
            [new JobSetStateParams { Id = jobs[0].Id, State = JobState.Completed, FinalizedAt = finalized }], Ct));

        Assert.Equal((jobs[0].Id, JobState.Completed, finalized, 1), (row.Id, row.State, row.FinalizedAt!.Value, row.Attempt));
        Assert.Empty(row.Errors);
        Assert.Equal(jobs[0].ScheduledAt, row.ScheduledAt);
        Assert.Equal(row.State, (await executor.JobGetByIdAsync(row.Id, Ct))!.State);
    }

    [Fact]
    public async Task A_failed_attempt_appends_its_error_and_reschedules_the_job()
    {
        var (executor, tx, jobs) = await RunningAsync(2);
        await using var _ = tx;
        var at = Microseconds(DateTimeOffset.UtcNow);
        var withTrace = new AttemptError(at, 1, "boom \"quoted\" ü", "at Worker.WorkAsync()\n  at Executor");
        var withoutTrace = new AttemptError(at, 1, "timed out", null);

        var rows = await executor.JobSetStateIfRunningManyAsync(
            [
                new JobSetStateParams { Id = jobs[0].Id, State = JobState.Retryable, Error = withTrace, ScheduledAt = Later },
                new JobSetStateParams { Id = jobs[1].Id, State = JobState.Discarded, Error = withoutTrace, FinalizedAt = at },
            ],
            Ct);

        Assert.Equal([jobs[0].Id, jobs[1].Id], rows.Select(r => r.Id));
        Assert.Equal((JobState.Retryable, Later, (DateTimeOffset?)null), (rows[0].State, rows[0].ScheduledAt, rows[0].FinalizedAt));
        Assert.Equal([withTrace], rows[0].Errors);
        Assert.Equal((JobState.Discarded, at), (rows[1].State, rows[1].FinalizedAt!.Value));
        Assert.Equal([withoutTrace], rows[1].Errors);
    }

    [Fact]
    public async Task An_outcome_can_set_the_attempt_and_merge_metadata()
    {
        var (executor, tx, jobs) = await RunningAsync(1, metadata: """{"tenant": "a", "snoozes": 1}""");
        await using var _ = tx;

        var row = Assert.Single(await executor.JobSetStateIfRunningManyAsync(
            [new JobSetStateParams { Id = jobs[0].Id, State = JobState.Scheduled, Attempt = 0, ScheduledAt = Later, MetadataUpdates = """{"snoozes": 2}""" }],
            Ct));

        Assert.Equal((JobState.Scheduled, 0, Later), (row.State, row.Attempt, row.ScheduledAt));
        Assert.Equal("a", Metadata(row, "tenant"));
        Assert.Equal("2", Metadata(row, "snoozes"));
        Assert.Empty(row.Errors);
    }

    [Fact]
    public async Task A_job_that_is_no_longer_running_is_returned_unchanged_except_for_merged_metadata()
    {
        var (executor, tx, jobs) = await RunningAsync(2);
        await using var _ = tx;
        var at = Microseconds(DateTimeOffset.UtcNow);
        await executor.JobSetStateIfRunningManyAsync([new JobSetStateParams { Id = jobs[0].Id, State = JobState.Completed, FinalizedAt = at }], Ct);
        var available = (await executor.JobInsertManyAsync([Job(UniqueKind(), UniqueQueue())], Ct))[0].Job;

        var rows = await executor.JobSetStateIfRunningManyAsync(
            [
                new JobSetStateParams { Id = jobs[0].Id, State = JobState.Retryable, Error = new AttemptError(at, 1, "late", null), ScheduledAt = Later },
                new JobSetStateParams { Id = available.Id, State = JobState.Completed, FinalizedAt = at, MetadataUpdates = """{"note": "kept"}""" },
            ],
            Ct);

        Assert.Equal((JobState.Completed, at), (rows[0].State, rows[0].FinalizedAt!.Value));
        Assert.Empty(rows[0].Errors);
        Assert.Equal((JobState.Available, (DateTimeOffset?)null), (rows[1].State, rows[1].FinalizedAt));
        Assert.Equal("kept", Metadata(rows[1], "note"));
    }

    [Fact]
    public async Task A_job_cancelled_while_running_is_cancelled_instead_of_run_again()
    {
        var (executor, tx, jobs) = await RunningAsync(4, metadata: """{"cancel_attempted_at": "2026-11-26T00:00:00Z"}""");
        await using var _ = tx;
        var at = Microseconds(DateTimeOffset.UtcNow);
        var error = new AttemptError(at, 1, "interrupted", null);

        var rows = await executor.JobSetStateIfRunningManyAsync(
            [
                new JobSetStateParams { Id = jobs[0].Id, State = JobState.Retryable, Error = error, ScheduledAt = Later },
                new JobSetStateParams { Id = jobs[1].Id, State = JobState.Available, Attempt = 0, ScheduledAt = Later },
                new JobSetStateParams { Id = jobs[2].Id, State = JobState.Scheduled, Attempt = 0, ScheduledAt = Later },
                new JobSetStateParams { Id = jobs[3].Id, State = JobState.Completed, FinalizedAt = at },
            ],
            Ct);

        Assert.All(rows.Take(3), row =>
        {
            Assert.Equal((JobState.Cancelled, 1), (row.State, row.Attempt));
            Assert.NotNull(row.FinalizedAt);
            Assert.NotEqual(Later, row.ScheduledAt);
        });
        Assert.Equal([error], rows[0].Errors);
        Assert.Equal((JobState.Completed, at), (rows[3].State, rows[3].FinalizedAt!.Value));
    }

    [Fact]
    public async Task Missing_jobs_are_left_out_and_rows_come_back_in_id_order()
    {
        var (executor, tx, jobs) = await RunningAsync(3);
        await using var _ = tx;
        var at = Microseconds(DateTimeOffset.UtcNow);

        var rows = await executor.JobSetStateIfRunningManyAsync(
            [.. new[] { jobs[2].Id, long.MaxValue, jobs[0].Id, jobs[1].Id }.Select(id => new JobSetStateParams { Id = id, State = JobState.Completed, FinalizedAt = at })],
            Ct);

        Assert.Equal(jobs.Select(j => j.Id), rows.Select(r => r.Id));
        Assert.Empty(await executor.JobSetStateIfRunningManyAsync([], Ct));
    }

    [Theory]
    [InlineData(JobState.Running)]
    [InlineData(JobState.Pending)]
    public async Task An_outcome_cannot_set_running_or_pending(JobState state)
    {
        var schema = await MigratedAsync();

        await Assert.ThrowsAsync<ArgumentException>(() => schema.Driver.GetExecutor().JobSetStateIfRunningManyAsync(
            [new JobSetStateParams { Id = 1, State = state }], Ct));
    }

    /// <summary>Inserts <paramref name="count"/> jobs in a caller transaction and fetches them, so they are running.</summary>
    private async Task<(IKopiExecutor Executor, CallerTransaction Transaction, IReadOnlyList<JobRow> Jobs)> RunningAsync(int count, string metadata = "{}")
    {
        var schema = await MigratedAsync();
        var tx = await schema.BeginTransactionAsync(Ct);
        var executor = schema.Driver.GetExecutor(tx.Transaction);
        var queue = UniqueQueue();
        var kind = UniqueKind();
        await executor.JobInsertManyAsync(
            [.. Enumerable.Range(0, count).Select(_ => new JobInsertParams
            {
                Kind = kind, ArgsJson = "{}", Queue = queue, State = JobState.Available, Priority = 1, MaxAttempts = 25, MetadataJson = metadata,
            })],
            Ct);
        var jobs = await executor.JobGetAvailableAsync(new JobGetAvailableParams { Queue = queue, Max = count, ClientId = "c" }, Ct);
        Assert.Equal(count, jobs.Count);
        return (executor, tx, jobs);
    }

    private static string? Metadata(JobRow row, string key)
    {
        using var json = System.Text.Json.JsonDocument.Parse(row.MetadataJson);
        return json.RootElement.TryGetProperty(key, out var value) ? value.ToString() : null;
    }
}
