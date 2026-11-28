using System.Collections.Concurrent;
using System.Text.Json.Nodes;
using KopiQueue.Drivers;
using Xunit;

namespace KopiQueue.TestInfrastructure.Integration;

/// <summary>
/// The admin API on a real database (plan task 4.4: FR-ADM-01..05, FR-ERR-07): keyset pages through the client's
/// cursors, a retried job running again on a working client, a running job that cannot be deleted, and every
/// transaction variant committing or rolling back with the caller's transaction.
/// </summary>
public abstract class AdminTests<TFixture>(TFixture db)
    where TFixture : IntegrationFixture
{
    private const string Queue = "admin_work";

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Fact]
    public async Task Cursors_page_through_every_matching_job_once_in_each_order()
    {
        var schema = await db.CreateSchemaAsync(Ct);
        var driver = schema.Driver;
        var client = new KopiClient(driver);
        var now = DateTimeOffset.UtcNow;
        var inserted = await client.InsertManyAsync(
            [.. Enumerable.Range(0, 25).Select(i => new InsertRequest(
                new AdminArgs(i),
                new InsertOptions { ScheduledAt = now.AddMinutes(i % 5), Metadata = new JsonObject { ["batch"] = i < 20 ? "a" : "b" } }))],
            cancellationToken: Ct);
        var ids = inserted.Select(r => r.Job.Id).ToArray();

        async Task<List<JobRow>> AllAsync(JobListOrderBy orderBy, bool descending)
        {
            var jobs = new List<JobRow>();
            string? after = null;
            var pages = 0;
            do
            {
                var page = await client.JobListAsync(
                    new JobListParams { Kinds = ["admin_job"], MetadataContains = new JsonObject { ["batch"] = "a" }, OrderBy = orderBy, Descending = descending, Limit = 7, After = after },
                    Ct);
                jobs.AddRange(page.Jobs);
                after = page.NextCursor;
                Assert.True(++pages <= 4);
            }
            while (after is not null);

            return jobs;
        }

        var byId = await AllAsync(JobListOrderBy.Id, descending: false);
        var byScheduleDown = await AllAsync(JobListOrderBy.ScheduledAt, descending: true);

        Assert.Equal(ids[..20], byId.Select(j => j.Id));
        Assert.Equal(
            ids[..20].Select((id, i) => (id, i)).OrderByDescending(x => x.i % 5).ThenByDescending(x => x.id).Select(x => x.id),
            byScheduleDown.Select(j => j.Id));
        Assert.Equal(ids[^1], (await client.JobGetAsync(ids[^1], Ct))!.Id);
    }

    [Fact]
    public async Task Every_sort_direction_and_page_size_pages_exactly_as_a_full_sort_would()
    {
        // Plan task 4.6: 300 jobs whose times tie often and are often missing (never attempted, not finalised), paged in
        // every order, both ways, by 1, 7 and 64, against the same jobs sorted in memory (a missing time last ascending).
        var schema = await db.CreateSchemaAsync(Ct);
        var driver = schema.Driver;
        var client = new KopiClient(driver);
        var executor = driver.GetExecutor();
        var now = DateTimeOffset.UtcNow;
        await client.InsertManyAsync(
            [.. Enumerable.Range(0, 300).Select(i => new InsertRequest(new AdminArgs(i), new InsertOptions { Queue = "paged", ScheduledAt = now.AddMinutes(-(i % 5) - 1) }))],
            cancellationToken: Ct);
        var finalizedAt = new[] { now.AddHours(-3), now.AddHours(-2), now.AddHours(-1) };
        for (var fetch = 0; fetch < 4; fetch++)
        {
            // Each fetch gives its jobs one attempted_at; some are then finalised at one of three times.
            var running = await executor.JobGetAvailableAsync(new JobGetAvailableParams { Queue = "paged", Max = 40, ClientId = "c" }, Ct);
            await executor.JobSetStateIfRunningManyAsync(
                [.. running.Where((_, i) => i % 2 == 0).Select((j, i) => new JobSetStateParams { Id = j.Id, State = JobState.Completed, FinalizedAt = finalizedAt[i % 3] })],
                Ct);
            await Task.Delay(10, Ct);
        }

        var all = (await client.JobListAsync(new JobListParams { Limit = 10_000 }, Ct)).Jobs;
        Assert.Equal(300, all.Count);
        Assert.Equal(all.Select(j => j.Id).Order(), all.Select(j => j.Id));
        foreach (var orderBy in Enum.GetValues<JobListOrderBy>())
        {
            Func<JobRow, DateTimeOffset?> value = orderBy switch
            {
                JobListOrderBy.Id => _ => DateTimeOffset.MinValue,
                JobListOrderBy.CreatedAt => j => j.CreatedAt,
                JobListOrderBy.ScheduledAt => j => j.ScheduledAt,
                JobListOrderBy.AttemptedAt => j => j.AttemptedAt,
                _ => j => j.FinalizedAt,
            };
            var ascending = all.OrderBy(j => value(j) ?? DateTimeOffset.MaxValue).ThenBy(j => j.Id).Select(j => j.Id).ToList();
            foreach (var descending in new[] { false, true })
            {
                var expected = descending ? Enumerable.Reverse(ascending).ToList() : ascending;
                foreach (var limit in new[] { 1, 7, 64 })
                {
                    var seen = new List<long>();
                    string? after = null;
                    do
                    {
                        var page = await client.JobListAsync(new JobListParams { OrderBy = orderBy, Descending = descending, Limit = limit, After = after }, Ct);
                        Assert.InRange(page.Jobs.Count, 1, limit);
                        seen.AddRange(page.Jobs.Select(j => j.Id));
                        after = page.NextCursor;
                    }
                    while (after is not null);

                    Assert.True(expected.SequenceEqual(seen), $"{orderBy}, descending {descending}, by {limit}: the pages differ from the full sort.");
                }
            }
        }
    }

    [Fact]
    public async Task Paging_by_id_while_jobs_are_inserted_and_deleted_returns_no_job_twice_and_every_job_that_stays()
    {
        var schema = await db.CreateSchemaAsync(Ct);
        var driver = schema.Driver;
        var client = new KopiClient(driver);
        var kept = (await client.InsertManyAsync([.. Enumerable.Range(0, 200).Select(i => new InsertRequest(new AdminArgs(i)))], cancellationToken: Ct))
            .Select(r => r.Job.Id).ToHashSet();
        await client.InsertManyAsync([.. Enumerable.Range(0, 200).Select(i => new InsertRequest(new AdminArgs(i), new InsertOptions { Queue = "doomed" }))], cancellationToken: Ct);

        using var stop = CancellationTokenSource.CreateLinkedTokenSource(Ct);
        var churn = Task.Run(
            async () =>
            {
                var other = new KopiClient(driver);
                while (!stop.IsCancellationRequested)
                {
                    await other.InsertAsync(new AdminArgs(1), new InsertOptions { Queue = "doomed" }, CancellationToken.None);
                    await other.JobDeleteManyAsync(new JobDeleteManyParams { Queues = ["doomed"], Limit = 3 }, CancellationToken.None);
                }
            },
            CancellationToken.None);

        var seen = new List<long>();
        string? after = null;
        do
        {
            var page = await client.JobListAsync(new JobListParams { Limit = 10, After = after }, Ct);
            seen.AddRange(page.Jobs.Select(j => j.Id));
            after = page.NextCursor;
        }
        while (after is not null);

        await stop.CancelAsync();
        await churn;

        Assert.Equal(seen.Order(), seen); // strictly ascending, so no job twice
        Assert.Equal(seen.Count, seen.Distinct().Count());
        Assert.Subset(seen.ToHashSet(), kept); // every job that was never deleted
    }

    [Fact]
    public async Task A_retried_discarded_job_runs_again_on_a_working_client_with_one_more_attempt()
    {
        var schema = await db.CreateSchemaAsync(Ct);
        var driver = schema.Driver;
        var worker = new AdminWorker();
        var working = await StartedAsync(driver, worker);
        try
        {
            var admin = new KopiClient(driver);
            var job = (await admin.InsertAsync(new AdminArgs(-1), new InsertOptions { Queue = Queue, MaxAttempts = 1 }, Ct)).Job;
            var discarded = await WaitForStateAsync(driver, job.Id, JobState.Discarded);

            var retried = await admin.JobRetryAsync(job.Id, Ct);
            var completed = await WaitForStateAsync(driver, job.Id, JobState.Completed);

            Assert.Equal((JobState.Available, 1), (retried.State, retried.Attempt));
            Assert.Equal(2, completed.Attempt);
            Assert.Equal(discarded.Errors, completed.Errors);
            Assert.Equal([1, 2], worker.Attempts(job.Id));
        }
        finally
        {
            await working.StopAsync(Ct);
        }
    }

    [Fact]
    public async Task A_running_job_cannot_be_deleted_until_it_has_stopped()
    {
        var schema = await db.CreateSchemaAsync(Ct);
        var driver = schema.Driver;
        var worker = new AdminWorker();
        var working = await StartedAsync(driver, worker);
        try
        {
            var admin = new KopiClient(driver);
            var job = (await admin.InsertAsync(new AdminArgs(0), new InsertOptions { Queue = Queue }, Ct)).Job;
            await worker.Started(job.Id).WaitAsync(TimeSpan.FromSeconds(10), Ct);

            await Assert.ThrowsAsync<KopiQueueJobRunningException>(() => admin.JobDeleteAsync(job.Id, Ct));
            Assert.Empty(await admin.JobDeleteManyAsync(new JobDeleteManyParams { Ids = [job.Id] }, Ct));
            worker.Release(job.Id);
            await WaitForStateAsync(driver, job.Id, JobState.Completed);

            Assert.Equal(job.Id, (await admin.JobDeleteAsync(job.Id, Ct))!.Id);
            Assert.Null(await admin.JobGetAsync(job.Id, Ct));
        }
        finally
        {
            await working.StopAsync(Ct);
        }
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Every_transaction_variant_commits_or_rolls_back_with_the_callers_transaction(bool commit)
    {
        var schema = await db.CreateSchemaAsync(Ct);
        var driver = schema.Driver;
        var client = new KopiClient(driver);
        var scheduled = DateTimeOffset.UtcNow.AddHours(1);
        var jobs = (await client.InsertManyAsync(
            [.. Enumerable.Range(0, 4).Select(i => new InsertRequest(new AdminArgs(i), new InsertOptions { ScheduledAt = scheduled }))],
            cancellationToken: Ct)).Select(r => r.Job.Id).ToArray();
        await schema.InsertQueueAsync("q1", TimeSpan.Zero, paused: false, Ct);
        await schema.InsertQueueAsync("q2", TimeSpan.Zero, paused: false, Ct);

        await using (var caller = await schema.BeginTransactionAsync(Ct))
        {
            var tx = caller.Transaction;
            Assert.Equal(JobState.Available, (await client.JobRetryAsync(jobs[0], tx, Ct)).State);
            Assert.Equal(jobs[1], (await client.JobDeleteAsync(jobs[1], tx, Ct))!.Id);
            Assert.Equal([jobs[2]], (await client.JobDeleteManyAsync(new JobDeleteManyParams { Ids = [jobs[2]] }, tx, Ct)).Select(j => j.Id));
            await client.QueuePauseAsync("q1", tx, Ct);
            await client.QueueUpdateAsync("q2", new JsonObject { ["owner"] = "ops" }, tx, Ct);
            await client.QueuePauseAsync("q2", tx, Ct);
            await client.QueueResumeAsync("q2", tx, Ct);
            await Assert.ThrowsAsync<KopiQueueNotFoundException>(() => client.QueuePauseAsync("missing", tx, Ct));
            if (commit)
            {
                await tx.CommitAsync(Ct);
            }
        }

        var retried = (await client.JobGetAsync(jobs[0], Ct))!;
        Assert.Equal(commit ? JobState.Available : JobState.Scheduled, retried.State);
        Assert.Equal(!commit, await client.JobGetAsync(jobs[1], Ct) is not null);
        Assert.Equal(!commit, await client.JobGetAsync(jobs[2], Ct) is not null);
        Assert.Equal(JobState.Scheduled, (await client.JobGetAsync(jobs[3], Ct))!.State);
        Assert.Equal(commit, (await client.QueueGetAsync("q1", Ct))!.PausedAt is not null);
        var q2 = (await client.QueueGetAsync("q2", Ct))!;
        Assert.Null(q2.PausedAt);
        Assert.Equal(commit ? """{"owner":"ops"}""" : "{}", JsonNode.Parse(q2.MetadataJson)!.ToJsonString());
    }

    [Fact]
    public async Task A_unique_job_whose_key_a_newer_job_holds_cannot_be_retried()
    {
        var schema = await db.CreateSchemaAsync(Ct);
        var driver = schema.Driver;
        var worker = new AdminWorker();
        var working = await StartedAsync(driver, worker);
        try
        {
            var admin = new KopiClient(driver);
            var unique = new InsertOptions { Queue = Queue, MaxAttempts = 1, Unique = new() { ByArgs = true } };
            var first = (await admin.InsertAsync(new AdminArgs(-1), unique, Ct)).Job;
            await WaitForStateAsync(driver, first.Id, JobState.Discarded); // discarded is outside the default unique states
            await working.StopAsync(Ct);
            var second = await admin.InsertAsync(new AdminArgs(-1), unique, Ct);
            Assert.False(second.UniqueSkippedAsDuplicate);

            var e = await Assert.ThrowsAsync<InvalidOperationException>(() => admin.JobRetryAsync(first.Id, Ct));

            Assert.Contains("unique key", e.Message, StringComparison.Ordinal);
            Assert.Equal(JobState.Discarded, (await admin.JobGetAsync(first.Id, Ct))!.State);
        }
        finally
        {
            await working.StopAsync(Ct);
        }
    }

    private static async Task<KopiClient> StartedAsync(IKopiDriver driver, AdminWorker worker)
    {
        var client = new KopiClient(driver, new KopiClientOptions
        {
            Queues = new Dictionary<string, QueueOptions> { [Queue] = new() { MaxWorkers = 4 } },
            Workers = new WorkerRegistry().Add(worker),
            ClientId = "admin-client",
            FetchPollInterval = TimeSpan.FromSeconds(5), // the retry's wake-up, not the poll, starts the job again
        });
        await client.StartAsync(Ct);
        return client;
    }

    private static async Task<JobRow> WaitForStateAsync(IKopiDriver driver, long id, JobState state)
    {
        var deadline = DateTime.UtcNow.AddSeconds(15);
        while (true)
        {
            var row = (await driver.GetExecutor().JobGetByIdAsync(id, Ct))!;
            if (row.State == state)
            {
                return row;
            }

            Assert.True(DateTime.UtcNow < deadline, $"Job {id} is {row.State}, not {state}, after 15 s.");
            await Task.Delay(20, Ct);
        }
    }

    /// <summary><c>N</c> below 0: fails its first attempt. 0: runs until released. Above 0: completes.</summary>
    [JobKind("admin_job")]
    public sealed record AdminArgs(int N) : IJobArgs;

    private sealed class AdminWorker : Worker<AdminArgs>
    {
        private readonly ConcurrentDictionary<long, TaskCompletionSource> _started = new();
        private readonly ConcurrentDictionary<long, TaskCompletionSource> _released = new();
        private readonly ConcurrentDictionary<long, ConcurrentQueue<int>> _attempts = new();

        public Task Started(long id) => Gate(_started, id).Task;

        public void Release(long id) => Gate(_released, id).TrySetResult();

        public int[] Attempts(long id) => [.. _attempts.GetOrAdd(id, _ => new())];

        public override async Task WorkAsync(Job<AdminArgs> job, CancellationToken cancellationToken)
        {
            _attempts.GetOrAdd(job.Row.Id, _ => new()).Enqueue(job.Row.Attempt);
            Gate(_started, job.Row.Id).TrySetResult();
            if (job.Args.N < 0 && job.Row.Attempt == 1)
            {
                throw new InvalidOperationException("first attempt fails");
            }

            if (job.Args.N == 0)
            {
                await Gate(_released, job.Row.Id).Task.WaitAsync(cancellationToken);
            }
        }

        private static TaskCompletionSource Gate(ConcurrentDictionary<long, TaskCompletionSource> gates, long id) =>
            gates.GetOrAdd(id, _ => new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously));
    }
}
