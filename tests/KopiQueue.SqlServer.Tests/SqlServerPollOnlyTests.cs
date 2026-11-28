using System.Collections.Concurrent;
using System.Diagnostics;
using KopiQueue.Drivers;
using KopiQueue.EntityFrameworkCore;
using KopiQueue.Execution;
using KopiQueue.Migrations;
using KopiQueue.SqlServer.Tests.Migrations;
using KopiQueue.TestInfrastructure;
using Microsoft.Data.SqlClient;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Storage;

namespace KopiQueue.SqlServer.Tests;

/// <summary>
/// SQL Server has no LISTEN/NOTIFY, so clients work by polling (plan task 5.7: FR-DB-04, FR-DB-05; design §3.3, §6.3):
/// a cancel or a pause from another client takes effect within the poll interval, and the EF Core commit interceptor
/// wakes the inserting client's producers as soon as a context's transaction with jobs commits.
/// </summary>
public sealed class SqlServerPollOnlyTests(SqlServerFixture db) : IClassFixture<SqlServerFixture>
{
    private const string Queue = "poll_work";

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Fact]
    public async Task A_cancel_reaches_a_job_running_on_another_client_within_the_poll_interval()
    {
        var (driver, _) = await MigratedAsync();
        var worker = new PollWorker();
        var working = await StartedAsync(driver, worker, TimeSpan.FromMilliseconds(500));
        try
        {
            var admin = new KopiClient(driver);
            var job = (await admin.InsertAsync(new PollArgs("wait"), new InsertOptions { Queue = Queue }, Ct)).Job;
            await worker.Started(job.Id).WaitAsync(TimeSpan.FromSeconds(5), Ct);

            var clock = Stopwatch.StartNew();
            Assert.Equal(JobState.Running, (await admin.JobCancelAsync(job.Id, Ct)).State);
            var done = await WaitForStateAsync(driver, job.Id, JobState.Cancelled);

            // Within a few 0.5 s polls, where without the poll it would never arrive; a loaded CI runner needs the slack.
            Assert.True(clock.Elapsed < TimeSpan.FromSeconds(3), $"took {clock.Elapsed}");
            Assert.Contains(nameof(TaskCanceledException), Assert.Single(done.Errors).Trace, StringComparison.Ordinal);
        }
        finally
        {
            await working.StopAsync(Ct);
        }
    }

    [Fact]
    public async Task A_pause_and_a_resume_reach_another_client_within_the_poll_interval()
    {
        var (driver, schema) = await MigratedAsync();
        var working = await StartedAsync(driver, new PollWorker(), TimeSpan.FromMilliseconds(500));
        try
        {
            var admin = new KopiClient(driver);
            await admin.QueuePauseAsync(Queue, cancellationToken: Ct);
            await Task.Delay(TimeSpan.FromSeconds(1.5), Ct); // a few polls
            for (var i = 0; i < 3; i++)
            {
                await admin.InsertAsync(new PollArgs("ok"), new InsertOptions { Queue = Queue }, Ct);
            }

            await Task.Delay(TimeSpan.FromSeconds(1.5), Ct);
            Assert.Equal(3, await CountAsync(schema, "available"));

            var clock = Stopwatch.StartNew();
            await admin.QueueResumeAsync(Queue, cancellationToken: Ct);
            await WaitForCountAsync(schema, "completed", 3);
            Assert.True(clock.Elapsed < TimeSpan.FromSeconds(3), $"took {clock.Elapsed}");
        }
        finally
        {
            await working.StopAsync(Ct);
        }
    }

    [Fact]
    public async Task With_the_interceptor_jobs_inserted_in_an_EF_transaction_start_when_it_commits()
    {
        var (driver, schema) = await MigratedAsync();
        var working = await StartedAsync(driver, new PollWorker(), TimeSpan.FromSeconds(30)); // polls far too rarely to matter
        try
        {
            await Task.Delay(TimeSpan.FromMilliseconds(500), Ct); // past the first fetch: only the commit can wake the queue
            await using var context = Context(interceptor: true);
            await using (var tx = await context.Database.BeginTransactionAsync(Ct))
            {
                await working.InsertAsync(new PollArgs("ok"), context, new InsertOptions { Queue = Queue }, Ct);
                await working.InsertManyAsync([Ok(), Ok()], context, Ct);
                await working.InsertManyFastAsync([Ok(), Ok()], context, Ct);
                Assert.True(TransactionWakes.IsPending(tx.GetDbTransaction()));
                await Task.Delay(TimeSpan.FromMilliseconds(500), Ct);
                Assert.Equal(0, await CountAsync(schema, "completed")); // nothing is woken before the commit

                var clock = Stopwatch.StartNew();
                await tx.CommitAsync(Ct);
                await WaitForCountAsync(schema, "completed", 5);
                Assert.True(clock.Elapsed < TimeSpan.FromSeconds(5), $"took {clock.Elapsed}");
                Assert.False(TransactionWakes.IsPending(tx.GetDbTransaction()));
            }
        }
        finally
        {
            await working.StopAsync(Ct);
        }
    }

    [Fact]
    public async Task Without_the_interceptor_or_after_a_rollback_no_producer_is_woken()
    {
        var (driver, schema) = await MigratedAsync();
        var working = await StartedAsync(driver, new PollWorker(), TimeSpan.FromSeconds(30));
        try
        {
            await Task.Delay(TimeSpan.FromMilliseconds(500), Ct);
            await using (var context = Context(interceptor: true))
            await using (var tx = await context.Database.BeginTransactionAsync(Ct))
            {
                await working.InsertAsync(new PollArgs("ok"), context, new InsertOptions { Queue = Queue }, Ct);
                await tx.RollbackAsync(Ct);
                Assert.False(TransactionWakes.IsPending(tx.GetDbTransaction()));
            }

            await using (var context = Context(interceptor: false))
            await using (var tx = await context.Database.BeginTransactionAsync(Ct))
            {
                await working.InsertAsync(new PollArgs("ok"), context, new InsertOptions { Queue = Queue }, Ct);
                await tx.CommitAsync(Ct);
            }

            await Task.Delay(TimeSpan.FromSeconds(2), Ct);
            Assert.Equal(1, await CountAsync(schema, "available")); // found at the next 30 s poll, not before
        }
        finally
        {
            await working.StopAsync(Ct);
        }
    }

    private static InsertRequest Ok() => new(new PollArgs("ok"), new InsertOptions { Queue = Queue });

    private DbContext Context(bool interceptor)
    {
        var options = new DbContextOptionsBuilder<DbContext>().UseSqlServer(db.ConnectionString);
        if (interceptor)
        {
            options.AddKopiQueueCommitInterceptor();
        }

        return new DbContext(options.Options);
    }

    private static async Task<KopiClient> StartedAsync(IKopiDriver driver, PollWorker worker, TimeSpan pollInterval)
    {
        var client = new KopiClient(driver, new KopiClientOptions
        {
            Queues = new Dictionary<string, QueueOptions> { [Queue] = new() { MaxWorkers = 4 } },
            Workers = new WorkerRegistry().Add(worker),
            FetchPollInterval = pollInterval,
        });
        await client.StartAsync(Ct);
        return client;
    }

    private async Task<(IKopiDriver Driver, string Schema)> MigratedAsync()
    {
        db.SkipIfUnavailable();
        var schema = await db.CreateSchemaAsync();
        var driver = new SqlServerDriver(db.ConnectionString, new SqlServerDriverOptions { Schema = schema });
        await driver.CreateMigrator().MigrateAsync(MigrationDirection.Up, cancellationToken: Ct);
        return (driver, schema);
    }

    private async Task<int> CountAsync(string schema, string state)
    {
        await using var connection = await db.OpenConnectionAsync(Ct);
        // READPAST: the open transaction of a test holds its uncommitted inserts, which a locking read would wait for.
        await using var cmd = new SqlCommand($"SELECT COUNT(*) FROM [{schema}].kopi_job WITH (READPAST) WHERE state = @state", connection);
        cmd.Parameters.AddWithValue("@state", state);
        return (int)(await cmd.ExecuteScalarAsync(Ct))!;
    }

    private async Task WaitForCountAsync(string schema, string state, int count)
    {
        var deadline = DateTime.UtcNow.AddSeconds(15);
        while (await CountAsync(schema, state) < count)
        {
            Assert.True(DateTime.UtcNow < deadline, $"Fewer than {count} jobs are {state} after 15 s.");
            await Task.Delay(50, Ct);
        }
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

    /// <summary>"ok": completes at once. "wait": runs until its token is cancelled, then lets the cancellation escape.</summary>
    [JobKind("poll_job")]
    public sealed record PollArgs(string Mode) : IJobArgs;

    private sealed class PollWorker : Worker<PollArgs>
    {
        private readonly ConcurrentDictionary<long, TaskCompletionSource> _started = new();

        public Task Started(long id) => Gate(id).Task;

        public override async Task WorkAsync(Job<PollArgs> job, CancellationToken cancellationToken)
        {
            Gate(job.Id).TrySetResult();
            if (job.Args.Mode == "wait")
            {
                await Task.Delay(System.Threading.Timeout.Infinite, cancellationToken);
            }
        }

        private TaskCompletionSource Gate(long id) =>
            _started.GetOrAdd(id, _ => new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously));
    }
}
