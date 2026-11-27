using System.Text.Json.Nodes;
using KopiQueue.Drivers;
using Xunit;

namespace KopiQueue.TestInfrastructure.Conformance;

/// <summary>Admin cases (plan task 4.4: FR-ADM-01..05, FR-ERR-07): list, delete, delete many and retry.</summary>
public abstract partial class DriverConformanceTests<TFixture>
{
    private static readonly DateTimeOffset Future = new(2031, 2, 3, 4, 5, 6, TimeSpan.Zero);

    [Fact]
    public async Task A_list_returns_the_jobs_that_pass_every_filter()
    {
        var executor = (await MigratedAsync()).Driver.GetExecutor();
        var (kind, other, queue) = (UniqueKind(), UniqueKind(), UniqueQueue());
        var rows = (await executor.JobInsertManyAsync(
            [
                At(kind, queue, JobState.Available, DateTimeOffset.UtcNow, priority: 1, metadata: """{"tenant": "a", "tags": {"x": 1, "y": 2}}"""),
                At(kind, queue, JobState.Scheduled, Future, priority: 2, metadata: """{"tenant": "b"}"""),
                At(kind, UniqueQueue(), JobState.Available, DateTimeOffset.UtcNow, priority: 1, metadata: """{"tenant": "a"}"""),
                At(other, queue, JobState.Available, DateTimeOffset.UtcNow, priority: 1, metadata: """{"tenant": "a"}"""),
            ],
            Ct)).Select(r => r.Job.Id).ToArray();

        Task<IReadOnlyList<JobRow>> ListAsync(JobListParams filter) => executor.JobListAsync(new JobListQuery { Filter = filter, Max = 100 }, Ct);
        async Task<long[]> IdsAsync(JobListParams filter) => [.. (await ListAsync(filter)).Select(j => j.Id)];

        Assert.Equal(rows[..3], await IdsAsync(new() { Kinds = [kind] }));
        Assert.Equal(rows, await IdsAsync(new() { Kinds = [kind, other] }));
        Assert.Equal([rows[0], rows[1]], await IdsAsync(new() { Kinds = [kind], Queues = [queue] }));
        Assert.Equal([rows[1]], await IdsAsync(new() { Kinds = [kind], States = [JobState.Scheduled, JobState.Completed] }));
        Assert.Equal([rows[1]], await IdsAsync(new() { Kinds = [kind], Priorities = [2, 3] }));
        Assert.Equal([rows[0], rows[2]], await IdsAsync(new() { Kinds = [kind], MetadataContains = new JsonObject { ["tenant"] = "a" } }));
        Assert.Equal([rows[0]], await IdsAsync(new() { Kinds = [kind], MetadataContains = new JsonObject { ["tags"] = new JsonObject { ["y"] = 2 } } }));
        Assert.Empty(await IdsAsync(new() { Kinds = [kind], MetadataContains = new JsonObject { ["tenant"] = "c" } }));
        Assert.Equal([rows[2], rows[3]], await IdsAsync(new() { Ids = [rows[3], rows[2], long.MaxValue] }));
        Assert.Empty(await IdsAsync(new() { Kinds = [kind], Ids = [] }));

        var job = Assert.Single(await ListAsync(new() { Ids = [rows[1]] }));
        Assert.Equal((kind, queue, JobState.Scheduled, 2, Future), (job.Kind, job.Queue, job.State, job.Priority, job.ScheduledAt));
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task A_list_by_id_pages_through_every_job_once(bool descending)
    {
        var executor = (await MigratedAsync()).Driver.GetExecutor();
        var kind = UniqueKind();
        var ids = (await executor.JobInsertManyAsync([.. Enumerable.Range(0, 5).Select(_ => Job(kind))], Ct)).Select(r => r.Job.Id).ToList();
        var expected = descending ? Enumerable.Reverse(ids).ToList() : ids;

        var seen = new List<long>();
        JobListPosition? after = null;
        while (true)
        {
            var page = await executor.JobListAsync(new JobListQuery { Filter = new JobListParams { Kinds = [kind] }, Descending = descending, After = after, Max = 2 }, Ct);
            if (page.Count == 0)
            {
                break;
            }

            Assert.InRange(page.Count, 1, 2);
            seen.AddRange(page.Select(j => j.Id));
            after = new JobListPosition(page[^1].Id, null);
        }

        Assert.Equal(expected, seen);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task A_list_by_a_time_sorts_ties_by_id_and_jobs_without_one_last_ascending(bool descending)
    {
        var executor = (await MigratedAsync()).Driver.GetExecutor();
        var kind = UniqueKind();
        var queue = UniqueQueue();
        var (early, late) = (Microseconds(DateTimeOffset.UtcNow.AddHours(-2)), Microseconds(DateTimeOffset.UtcNow.AddHours(-1)));

        // Five running jobs: two finalised late, one early, two left running (no finalized_at).
        await executor.JobInsertManyAsync([.. Enumerable.Range(0, 5).Select(_ => Job(kind, queue))], Ct);
        var running = (await executor.JobGetAvailableAsync(new JobGetAvailableParams { Queue = queue, Max = 5, ClientId = "c" }, Ct)).OrderBy(j => j.Id).ToArray();
        Assert.Equal(5, running.Length);
        await executor.JobSetStateIfRunningManyAsync(
            [
                new JobSetStateParams { Id = running[0].Id, State = JobState.Completed, FinalizedAt = late },
                new JobSetStateParams { Id = running[1].Id, State = JobState.Completed, FinalizedAt = early },
                new JobSetStateParams { Id = running[3].Id, State = JobState.Completed, FinalizedAt = late },
            ],
            Ct);
        long[] ascending = [running[1].Id, running[0].Id, running[3].Id, running[2].Id, running[4].Id];
        long[] expected = descending ? [.. ascending.Reverse()] : ascending;

        var all = await executor.JobListAsync(
            new JobListQuery { Filter = new JobListParams { Kinds = [kind] }, OrderBy = JobListOrderBy.FinalizedAt, Descending = descending, Max = 10 }, Ct);
        Assert.Equal(expected, all.Select(j => j.Id));

        var seen = new List<long>();
        JobListPosition? after = null;
        for (var page = 0; page < 10; page++)
        {
            var rows = await executor.JobListAsync(
                new JobListQuery { Filter = new JobListParams { Kinds = [kind] }, OrderBy = JobListOrderBy.FinalizedAt, Descending = descending, After = after, Max = 1 }, Ct);
            if (rows.Count == 0)
            {
                break;
            }

            seen.Add(rows[0].Id);
            after = new JobListPosition(rows[0].Id, rows[0].FinalizedAt);
        }

        Assert.Equal(expected, seen);
    }

    [Fact]
    public async Task A_list_by_scheduled_at_follows_the_schedule()
    {
        var executor = (await MigratedAsync()).Driver.GetExecutor();
        var kind = UniqueKind();
        var now = Microseconds(DateTimeOffset.UtcNow);
        var ids = (await executor.JobInsertManyAsync(
            [
                At(kind, "default", JobState.Scheduled, now.AddMinutes(30)),
                At(kind, "default", JobState.Scheduled, now.AddMinutes(10)),
                At(kind, "default", JobState.Scheduled, now.AddMinutes(20)),
            ],
            Ct)).Select(r => r.Job.Id).ToArray();

        var rows = await executor.JobListAsync(new JobListQuery { Filter = new JobListParams { Kinds = [kind] }, OrderBy = JobListOrderBy.ScheduledAt, Max = 10 }, Ct);
        var after = await executor.JobListAsync(
            new JobListQuery { Filter = new JobListParams { Kinds = [kind] }, OrderBy = JobListOrderBy.ScheduledAt, After = new JobListPosition(ids[1], now.AddMinutes(10)), Max = 10 },
            Ct);

        Assert.Equal([ids[1], ids[2], ids[0]], rows.Select(j => j.Id));
        Assert.Equal([ids[2], ids[0]], after.Select(j => j.Id));
    }

    [Fact]
    public async Task A_job_that_is_not_running_is_deleted_and_returned_as_it_was()
    {
        var executor = (await MigratedAsync()).Driver.GetExecutor();
        var job = (await executor.JobInsertManyAsync([At(UniqueKind(), UniqueQueue(), JobState.Scheduled, Future, metadata: """{"tenant": "a"}""")], Ct)).Single().Job;

        var result = await executor.JobDeleteAsync(job.Id, Ct);

        Assert.NotNull(result);
        Assert.True(result.Deleted);
        Assert.Equal((job.Id, JobState.Scheduled, job.MetadataJson), (result.Job.Id, result.Job.State, result.Job.MetadataJson));
        Assert.Null(await executor.JobGetByIdAsync(job.Id, Ct));
        Assert.Null(await executor.JobDeleteAsync(job.Id, Ct));
    }

    [Fact]
    public async Task A_running_job_is_not_deleted()
    {
        var executor = (await MigratedAsync()).Driver.GetExecutor();
        var job = await StartOneAsync(executor, UniqueKind(), UniqueQueue());

        var result = await executor.JobDeleteAsync(job.Id, Ct);

        Assert.NotNull(result);
        Assert.False(result.Deleted);
        Assert.Equal((job.Id, JobState.Running), (result.Job.Id, result.Job.State));
        Assert.Equal(JobState.Running, (await executor.JobGetByIdAsync(job.Id, Ct))!.State);
    }

    [Fact]
    public async Task A_delete_many_deletes_matching_jobs_in_id_order_up_to_the_most_and_never_a_running_one()
    {
        var executor = (await MigratedAsync()).Driver.GetExecutor();
        var (kind, queue) = (UniqueKind(), UniqueQueue());
        var running = await StartOneAsync(executor, kind, queue);
        var waiting = (await executor.JobInsertManyAsync(
            [
                At(kind, queue, JobState.Available, DateTimeOffset.UtcNow.AddMinutes(1), metadata: """{"tenant": "a"}"""),
                At(kind, queue, JobState.Scheduled, Future, metadata: """{"tenant": "a"}"""),
                At(kind, queue, JobState.Scheduled, Future, metadata: """{"tenant": "b"}"""),
                At(kind, queue, JobState.Retryable, Future, metadata: """{"tenant": "a"}"""),
            ],
            Ct)).Select(r => r.Job.Id).ToArray();

        var first = await executor.JobDeleteManyAsync(
            new JobDeleteManyQuery { Filter = new JobDeleteManyParams { Kinds = [kind], MetadataContains = new JsonObject { ["tenant"] = "a" } }, Max = 2 }, Ct);
        var rest = await executor.JobDeleteManyAsync(new JobDeleteManyQuery { Filter = new JobDeleteManyParams { Kinds = [kind] }, Max = 100 }, Ct);

        Assert.Equal([waiting[0], waiting[1]], first.Select(j => j.Id));
        Assert.Equal([waiting[2], waiting[3]], rest.Select(j => j.Id));
        Assert.Equal(JobState.Running, (await executor.JobGetByIdAsync(running.Id, Ct))!.State);
        Assert.Empty(await executor.JobDeleteManyAsync(new JobDeleteManyQuery { Filter = new JobDeleteManyParams { Kinds = [kind] }, Max = 100 }, Ct));
    }

    [Fact]
    public async Task Deletes_in_a_transaction_that_rolls_back_change_nothing()
    {
        var schema = await MigratedAsync();
        var executor = schema.Driver.GetExecutor();
        var kind = UniqueKind();
        var ids = (await executor.JobInsertManyAsync([Job(kind), Job(kind)], Ct)).Select(r => r.Job.Id).ToArray();

        await using (var tx = await schema.BeginTransactionAsync(Ct))
        {
            var inside = schema.Driver.GetExecutor(tx.Transaction);
            Assert.True((await inside.JobDeleteAsync(ids[0], Ct))!.Deleted);
            Assert.Equal([ids[1]], (await inside.JobDeleteManyAsync(new JobDeleteManyQuery { Filter = new JobDeleteManyParams { Kinds = [kind] }, Max = 10 }, Ct)).Select(j => j.Id));
        }

        Assert.Equal(2, await schema.CountJobsAsync(kind, Ct));
    }

    [Theory]
    [InlineData(JobState.Scheduled)]
    [InlineData(JobState.Retryable)]
    public async Task A_scheduled_or_retryable_job_is_made_available_now(JobState state)
    {
        var executor = (await MigratedAsync()).Driver.GetExecutor();
        var queue = UniqueQueue();
        var job = (await executor.JobInsertManyAsync([At(UniqueKind(), queue, state, Future)], Ct)).Single().Job;

        var retried = await executor.JobRetryAsync(job.Id, Ct);

        Assert.NotNull(retried);
        Assert.Equal((JobState.Available, job.Attempt, (DateTimeOffset?)null), (retried.State, retried.Attempt, retried.FinalizedAt));
        Assert.InRange(retried.ScheduledAt, DateTimeOffset.UtcNow.AddMinutes(-1), DateTimeOffset.UtcNow.AddMinutes(1));
        Assert.Equal(job.Id, Assert.Single(await executor.JobGetAvailableAsync(new JobGetAvailableParams { Queue = queue, Max = 10, ClientId = "c" }, Ct)).Id);
    }

    [Theory]
    [InlineData(JobState.Completed)]
    [InlineData(JobState.Cancelled)]
    [InlineData(JobState.Discarded)]
    public async Task A_finalised_job_is_made_available_with_its_attempts_and_errors_and_without_an_old_cancel(JobState state)
    {
        var executor = (await MigratedAsync()).Driver.GetExecutor();
        var queue = UniqueQueue();
        var job = await StartOneAsync(executor, UniqueKind(), queue);
        await executor.JobCancelAsync(job.Id, Ct); // marks it with cancel_attempted_at
        var error = new AttemptError(Microseconds(DateTimeOffset.UtcNow), 1, "boom", null);
        var finalized = Assert.Single(await executor.JobSetStateIfRunningManyAsync(
            [new JobSetStateParams { Id = job.Id, State = state, FinalizedAt = DateTimeOffset.UtcNow, Error = state == JobState.Completed ? null : error }], Ct));
        Assert.Equal(state, finalized.State);

        var retried = await executor.JobRetryAsync(job.Id, Ct);

        Assert.NotNull(retried);
        Assert.Equal((JobState.Available, 1, (DateTimeOffset?)null), (retried.State, retried.Attempt, retried.FinalizedAt));
        Assert.Equal(finalized.Errors, retried.Errors);
        Assert.Null(Metadata(retried, "cancel_attempted_at"));
        var again = Assert.Single(await executor.JobGetAvailableAsync(new JobGetAvailableParams { Queue = queue, Max = 10, ClientId = "c" }, Ct));
        Assert.Equal((job.Id, 2), (again.Id, again.Attempt));
        Assert.Empty(await executor.JobListCancelAttemptedAsync([job.Id], Ct));
    }

    [Theory]
    [InlineData(JobState.Available)]
    [InlineData(JobState.Pending)]
    [InlineData(JobState.Running)]
    public async Task An_available_pending_or_running_job_is_left_as_it_is(JobState state)
    {
        var executor = (await MigratedAsync()).Driver.GetExecutor();
        var job = state == JobState.Running
            ? await StartOneAsync(executor, UniqueKind(), UniqueQueue())
            : (await executor.JobInsertManyAsync([At(UniqueKind(), UniqueQueue(), state, Future)], Ct)).Single().Job;

        var after = await executor.JobRetryAsync(job.Id, Ct);

        Assert.NotNull(after);
        Assert.Equal((job.State, job.ScheduledAt, job.Attempt, job.FinalizedAt), (after.State, after.ScheduledAt, after.Attempt, after.FinalizedAt));
    }

    [Fact]
    public async Task Retrying_or_deleting_an_unknown_job_gives_nothing()
    {
        var executor = (await MigratedAsync()).Driver.GetExecutor();

        Assert.Null(await executor.JobRetryAsync(long.MaxValue, Ct));
        Assert.Null(await executor.JobDeleteAsync(long.MaxValue, Ct));
    }

    [Fact]
    public async Task A_retry_in_a_transaction_that_rolls_back_changes_nothing()
    {
        var schema = await MigratedAsync();
        var executor = schema.Driver.GetExecutor();
        var job = (await executor.JobInsertManyAsync([At(UniqueKind(), UniqueQueue(), JobState.Scheduled, Future)], Ct)).Single().Job;

        await using (var tx = await schema.BeginTransactionAsync(Ct))
        {
            Assert.Equal(JobState.Available, (await schema.Driver.GetExecutor(tx.Transaction).JobRetryAsync(job.Id, Ct))!.State);
        }

        var after = await executor.JobGetByIdAsync(job.Id, Ct);
        Assert.Equal((JobState.Scheduled, Future), (after!.State, after.ScheduledAt));
    }

    [Fact]
    public async Task A_unique_job_whose_key_another_job_holds_cannot_be_retried()
    {
        var executor = (await MigratedAsync()).Driver.GetExecutor();
        var (kind, queue, key) = (UniqueKind(), UniqueQueue(), NewUniqueKey());
        var first = (await executor.JobInsertManyAsync([Keyed(Job(kind, queue), key)], Ct)).Single().Job;
        var running = Assert.Single(await executor.JobGetAvailableAsync(new JobGetAvailableParams { Queue = queue, Max = 1, ClientId = "c" }, Ct));
        Assert.Equal(first.Id, running.Id);
        await executor.JobSetStateIfRunningManyAsync([new JobSetStateParams { Id = first.Id, State = JobState.Discarded, FinalizedAt = DateTimeOffset.UtcNow }], Ct);
        var second = (await executor.JobInsertManyAsync([Keyed(Job(kind, queue), key)], Ct)).Single(); // discarded is outside the default states
        Assert.False(second.UniqueSkippedAsDuplicate);

        await Assert.ThrowsAsync<InvalidOperationException>(() => executor.JobRetryAsync(first.Id, Ct));

        Assert.Equal(JobState.Discarded, (await executor.JobGetByIdAsync(first.Id, Ct))!.State);
    }

    [Fact]
    public async Task A_committed_retry_reaches_a_listener_as_an_insert_notification()
    {
        var schema = await MigratedAsync();
        var listener = schema.Driver.CreateListener();
        if (listener is null)
        {
            return;
        }

        var queue = UniqueQueue();
        var job = (await schema.Driver.GetExecutor().JobInsertManyAsync([At(UniqueKind(), queue, JobState.Scheduled, Future)], Ct)).Single().Job;
        await using var connection = await listener.ConnectAsync(Ct);
        await schema.Driver.GetExecutor().JobRetryAsync(job.Id, Ct);

        KopiNotification notification;
        do
        {
            notification = await connection.WaitForNotificationAsync(Ct).WaitAsync(TimeSpan.FromSeconds(10), Ct);
        }
        while (notification.Topic != NotificationTopic.Insert || QueueOf(notification) != queue);
    }

    private static JobInsertParams Keyed(JobInsertParams job, byte[] key) => new()
    {
        Kind = job.Kind,
        ArgsJson = job.ArgsJson,
        Queue = job.Queue,
        State = job.State,
        Priority = job.Priority,
        MaxAttempts = job.MaxAttempts,
        UniqueKey = key,
        UniqueStates = DefaultUniqueStates,
    };
}
