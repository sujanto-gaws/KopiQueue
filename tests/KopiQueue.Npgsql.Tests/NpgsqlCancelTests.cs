using System.Collections.Concurrent;
using System.Diagnostics;
using KopiQueue.Drivers;
using KopiQueue.Migrations;
using KopiQueue.TestInfrastructure;
using Npgsql;

namespace KopiQueue.Npgsql.Tests;

/// <summary>
/// External cancel on a real PostgreSQL (plan task 3.7: FR-ERR-06, FR-ADM-05, FR-DB-05; design §5.8): a cancel reaches a
/// job running on another client through the notification or, without notifications, within the poll interval; the
/// transactional variant waits for commit; and the worker's outcome decides how the job ends.
/// </summary>
public sealed class NpgsqlCancelTests(PostgresFixture db) : IClassFixture<PostgresFixture>
{
    private const string Queue = "cancel_work";

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Fact]
    public async Task A_cancel_reaches_a_job_running_on_another_client_through_the_notification_and_cancels_it()
    {
        db.SkipIfUnavailable();
        await using var ds = db.CreateDataSource();
        var (driver, _) = await MigratedAsync(ds);
        var worker = new CancelWorker();
        var working = await StartedAsync(driver, worker, NotificationMode.Auto, TimeSpan.FromSeconds(5)); // polls far too rarely to matter
        try
        {
            var admin = new KopiClient(driver);
            await Task.Delay(TimeSpan.FromMilliseconds(500), Ct); // past the first fetch: only the insert notification starts the job
            var job = (await admin.InsertAsync(new CancelArgs("wait"), new InsertOptions { Queue = Queue }, Ct)).Job;
            await worker.Started(job.Id).WaitAsync(TimeSpan.FromSeconds(3), Ct);

            var clock = Stopwatch.StartNew();
            var marked = await admin.JobCancelAsync(job.Id, Ct);
            var done = await WaitForStateAsync(driver, job.Id, JobState.Cancelled);
            clock.Stop();

            Assert.Equal(JobState.Running, marked.State);
            Assert.True(clock.Elapsed < TimeSpan.FromSeconds(2), $"took {clock.Elapsed}");
            Assert.Equal(1, done.Attempt);
            var error = Assert.Single(done.Errors);
            Assert.Contains(nameof(TaskCanceledException), error.Trace, StringComparison.Ordinal); // the worker's own exception
            Assert.NotNull(done.FinalizedAt);
        }
        finally
        {
            await working.StopAsync(Ct);
        }
    }

    [Fact]
    public async Task Without_notifications_a_cancel_reaches_a_job_on_another_client_within_the_poll_interval()
    {
        db.SkipIfUnavailable();
        await using var ds = db.CreateDataSource();
        var (driver, _) = await MigratedAsync(ds);
        var worker = new CancelWorker();
        var working = await StartedAsync(driver, worker, NotificationMode.PollOnly, TimeSpan.FromSeconds(1));
        try
        {
            var admin = new KopiClient(driver);
            var job = (await admin.InsertAsync(new CancelArgs("wait"), new InsertOptions { Queue = Queue }, Ct)).Job;
            await worker.Started(job.Id).WaitAsync(TimeSpan.FromSeconds(5), Ct);

            var clock = Stopwatch.StartNew();
            await admin.JobCancelAsync(job.Id, Ct);
            await WaitForStateAsync(driver, job.Id, JobState.Cancelled);

            Assert.True(clock.Elapsed < TimeSpan.FromSeconds(2.5), $"took {clock.Elapsed}"); // one poll interval, and some
        }
        finally
        {
            await working.StopAsync(Ct);
        }
    }

    [Fact]
    public async Task A_client_cancelling_a_job_it_runs_itself_cancels_it_at_once_even_without_notifications()
    {
        db.SkipIfUnavailable();
        await using var ds = db.CreateDataSource();
        var (driver, _) = await MigratedAsync(ds);
        var worker = new CancelWorker();
        var working = await StartedAsync(driver, worker, NotificationMode.PollOnly, TimeSpan.FromSeconds(10));
        try
        {
            var job = (await working.InsertAsync(new CancelArgs("wait"), new InsertOptions { Queue = Queue }, Ct)).Job;
            await worker.Started(job.Id).WaitAsync(TimeSpan.FromSeconds(5), Ct);

            var clock = Stopwatch.StartNew();
            await working.JobCancelAsync(job.Id, Ct);
            await WaitForStateAsync(driver, job.Id, JobState.Cancelled);

            Assert.True(clock.Elapsed < TimeSpan.FromSeconds(2), $"took {clock.Elapsed}");
        }
        finally
        {
            await working.StopAsync(Ct);
        }
    }

    [Fact]
    public async Task A_cancel_in_a_transaction_has_no_effect_until_it_commits_and_none_after_a_rollback()
    {
        db.SkipIfUnavailable();
        await using var ds = db.CreateDataSource();
        var (driver, _) = await MigratedAsync(ds);
        var worker = new CancelWorker();
        var working = await StartedAsync(driver, worker, NotificationMode.Auto, TimeSpan.FromSeconds(1));
        try
        {
            var admin = new KopiClient(driver);
            var running = (await admin.InsertAsync(new CancelArgs("wait"), new InsertOptions { Queue = Queue }, Ct)).Job;
            var later = (await admin.InsertAsync(new CancelArgs("wait"), new InsertOptions { Queue = Queue, ScheduledAt = DateTimeOffset.UtcNow.AddHours(1) }, Ct)).Job;
            await worker.Started(running.Id).WaitAsync(TimeSpan.FromSeconds(5), Ct);

            await using (var connection = await ds.OpenConnectionAsync(Ct))
            await using (var rolledBack = await connection.BeginTransactionAsync(Ct))
            {
                Assert.Equal(JobState.Running, (await admin.JobCancelAsync(running.Id, rolledBack, Ct)).State);
                Assert.Equal(JobState.Cancelled, (await admin.JobCancelAsync(later.Id, rolledBack, Ct)).State);
                await Task.Delay(TimeSpan.FromSeconds(1.5), Ct); // a notification and a poll would both have arrived by now
                Assert.False(worker.TokenCancelled(running.Id));
                Assert.Equal(JobState.Scheduled, (await driver.GetExecutor().JobGetByIdAsync(later.Id, Ct))!.State);
                await rolledBack.RollbackAsync(Ct);
            }

            await Task.Delay(TimeSpan.FromSeconds(1.5), Ct);
            Assert.False(worker.TokenCancelled(running.Id));
            Assert.Equal(JobState.Scheduled, (await driver.GetExecutor().JobGetByIdAsync(later.Id, Ct))!.State);

            await using (var connection = await ds.OpenConnectionAsync(Ct))
            await using (var committed = await connection.BeginTransactionAsync(Ct))
            {
                await admin.JobCancelAsync(running.Id, committed, Ct);
                await committed.CommitAsync(Ct);
            }

            await WaitForStateAsync(driver, running.Id, JobState.Cancelled);
            Assert.True(worker.TokenCancelled(running.Id));
        }
        finally
        {
            await working.StopAsync(Ct);
        }
    }

    [Fact]
    public async Task A_worker_that_ignores_the_cancel_and_succeeds_completes_its_job()
    {
        db.SkipIfUnavailable();
        await using var ds = db.CreateDataSource();
        var (driver, _) = await MigratedAsync(ds);
        var worker = new CancelWorker();
        var working = await StartedAsync(driver, worker, NotificationMode.Auto, TimeSpan.FromSeconds(1));
        try
        {
            var admin = new KopiClient(driver);
            var job = (await admin.InsertAsync(new CancelArgs("ignore"), new InsertOptions { Queue = Queue }, Ct)).Job;
            await worker.Started(job.Id).WaitAsync(TimeSpan.FromSeconds(5), Ct);

            await admin.JobCancelAsync(job.Id, Ct);
            await WaitUntilAsync(() => worker.TokenCancelled(job.Id));
            worker.Release(job.Id);

            var done = await WaitForStateAsync(driver, job.Id, JobState.Completed);
            Assert.Empty(done.Errors);
            Assert.Contains("cancel_attempted_at", done.MetadataJson, StringComparison.Ordinal);
        }
        finally
        {
            await working.StopAsync(Ct);
        }
    }

    [Fact]
    public async Task Cancelling_jobs_that_have_not_started_finished_or_do_not_exist()
    {
        db.SkipIfUnavailable();
        await using var ds = db.CreateDataSource();
        var (driver, _) = await MigratedAsync(ds);
        var admin = new KopiClient(driver);
        var waiting = (await admin.InsertAsync(new CancelArgs("wait"), new InsertOptions { Queue = Queue }, Ct)).Job;

        var cancelled = await admin.JobCancelAsync(waiting.Id, Ct);
        var again = await admin.JobCancelAsync(waiting.Id, Ct);

        Assert.Equal(JobState.Cancelled, cancelled.State);
        Assert.Equal((JobState.Cancelled, cancelled.FinalizedAt), (again.State, again.FinalizedAt)); // finalised: left as it is
        await Assert.ThrowsAsync<KopiQueueNotFoundException>(() => admin.JobCancelAsync(long.MaxValue, Ct));
    }

    private static async Task<KopiClient> StartedAsync(IKopiDriver driver, CancelWorker worker, NotificationMode mode, TimeSpan pollInterval)
    {
        var client = new KopiClient(driver, new KopiClientOptions
        {
            Queues = new Dictionary<string, QueueOptions> { [Queue] = new() { MaxWorkers = 4 } },
            Workers = new WorkerRegistry().Add(worker),
            ClientId = "worker-client",
            NotificationMode = mode,
            FetchPollInterval = pollInterval,
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

    private static async Task WaitUntilAsync(Func<bool> condition)
    {
        var deadline = DateTime.UtcNow.AddSeconds(15);
        while (!condition())
        {
            Assert.True(DateTime.UtcNow < deadline, "The condition was not met within 15 s.");
            await Task.Delay(20, Ct);
        }
    }

    private static async Task<(IKopiDriver Driver, string Schema)> MigratedAsync(NpgsqlDataSource ds)
    {
        var schema = "k_" + Guid.NewGuid().ToString("N")[..12];
        await using (var cmd = ds.CreateCommand($"CREATE SCHEMA {schema}"))
        {
            await cmd.ExecuteNonQueryAsync(Ct);
        }

        var driver = new NpgsqlDriver(ds, new NpgsqlDriverOptions { Schema = schema });
        await driver.CreateMigrator().MigrateAsync(MigrationDirection.Up, cancellationToken: Ct);
        return (driver, schema);
    }

    /// <summary>"wait": runs until its token is cancelled, then lets the cancellation escape. "ignore": runs until released.</summary>
    [JobKind("cancel_job")]
    public sealed record CancelArgs(string Mode) : IJobArgs;

    private sealed class CancelWorker : Worker<CancelArgs>
    {
        private readonly ConcurrentDictionary<long, TaskCompletionSource> _started = new();
        private readonly ConcurrentDictionary<long, TaskCompletionSource> _released = new();
        private readonly ConcurrentDictionary<long, CancellationToken> _tokens = new();

        public Task Started(long id) => Gate(_started, id).Task;

        public void Release(long id) => Gate(_released, id).TrySetResult();

        public bool TokenCancelled(long id) => _tokens.TryGetValue(id, out var token) && token.IsCancellationRequested;

        public override async Task WorkAsync(Job<CancelArgs> job, CancellationToken cancellationToken)
        {
            _tokens[job.Id] = cancellationToken;
            Gate(_started, job.Id).TrySetResult();
            if (job.Args.Mode == "ignore")
            {
                await Gate(_released, job.Id).Task;
            }
            else
            {
                await Task.Delay(System.Threading.Timeout.Infinite, cancellationToken);
            }
        }

        private static TaskCompletionSource Gate(ConcurrentDictionary<long, TaskCompletionSource> gates, long id) =>
            gates.GetOrAdd(id, _ => new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously));
    }
}
