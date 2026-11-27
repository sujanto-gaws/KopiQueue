using System.Collections.Concurrent;
using KopiQueue.Drivers;
using KopiQueue.Execution;
using KopiQueue.Migrations;
using KopiQueue.Serialization;
using KopiQueue.TestInfrastructure;
using Npgsql;

namespace KopiQueue.Npgsql.Tests;

/// <summary>
/// Producer, executor and completer together on a real PostgreSQL: jobs run and their outcomes are stored as River
/// stores them (plan tasks 2.3–2.6: FR-EXE-01..08, FR-ERR-01..04, NFR-REL-03).
/// </summary>
public sealed class NpgsqlExecutionTests(PostgresFixture db) : IClassFixture<PostgresFixture>
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Fact]
    public async Task Jobs_run_and_every_outcome_is_stored()
    {
        db.SkipIfUnavailable();
        await using var ds = db.CreateDataSource();
        var (driver, schema) = await MigratedAsync(ds);
        var client = new KopiClient(driver);
        var ids = new Dictionary<string, long>();
        foreach (var what in new[] { "complete", "fail", "fail-last", "snooze", "cancel" })
        {
            var options = new InsertOptions { Queue = "work", MaxAttempts = what == "fail-last" ? 1 : 25 };
            ids[what] = (await client.InsertAsync(new TaskArgs(what), options, Ct)).Job.Id;
        }

        await using var pipeline = await Pipeline.StartAsync(driver, new WorkerRegistry().Add(new TaskWorker()));
        await pipeline.WaitForAsync(ids.Count);

        var complete = await GetAsync(driver, ids["complete"]);
        Assert.Equal((JobState.Completed, 1), (complete.State, complete.Attempt));
        Assert.NotNull(complete.FinalizedAt);

        var fail = await GetAsync(driver, ids["fail"]);
        Assert.Equal((JobState.Retryable, 1), (fail.State, fail.Attempt));
        Assert.InRange(fail.ScheduledAt - DateTimeOffset.UtcNow, TimeSpan.FromMinutes(9), TimeSpan.FromMinutes(11)); // the worker's NextRetry
        var error = Assert.Single(fail.Errors);
        Assert.Equal((1, "boom"), (error.Attempt, error.Error));
        Assert.Contains("InvalidOperationException", error.Trace, StringComparison.Ordinal);
        Assert.Null(fail.FinalizedAt);

        var last = await GetAsync(driver, ids["fail-last"]);
        Assert.Equal(JobState.Discarded, last.State);
        Assert.NotNull(last.FinalizedAt);

        var snooze = await GetAsync(driver, ids["snooze"]);
        Assert.Equal((JobState.Scheduled, 0), (snooze.State, snooze.Attempt));
        Assert.Empty(snooze.Errors);
        Assert.Contains("\"snoozes\": 1", snooze.MetadataJson, StringComparison.Ordinal);
        Assert.InRange(snooze.ScheduledAt - DateTimeOffset.UtcNow, TimeSpan.FromMinutes(9), TimeSpan.FromMinutes(11));

        var cancel = await GetAsync(driver, ids["cancel"]);
        Assert.Equal(JobState.Cancelled, cancel.State);
        Assert.Equal("no longer wanted", Assert.Single(cancel.Errors).Error);

        await using var cmd = ds.CreateCommand($"SELECT count(*) FROM {schema}.river_job WHERE state = 'running'");
        Assert.Equal(0L, (long)(await cmd.ExecuteScalarAsync(Ct))!);
    }

    [Fact]
    public async Task A_job_cancelled_from_outside_while_running_ends_cancelled_even_though_its_worker_failed()
    {
        db.SkipIfUnavailable();
        await using var ds = db.CreateDataSource();
        var (driver, schema) = await MigratedAsync(ds);
        var started = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var worker = new GatedWorker(started, release);
        var id = (await new KopiClient(driver).InsertAsync(new TaskArgs("gated"), new InsertOptions { Queue = "work" }, Ct)).Job.Id;

        await using var pipeline = await Pipeline.StartAsync(driver, new WorkerRegistry().Add(worker));
        await started.Task.WaitAsync(TimeSpan.FromSeconds(10), Ct);
        await using (var cmd = ds.CreateCommand(
            $"UPDATE {schema}.river_job SET metadata = metadata || jsonb_build_object('cancel_attempted_at', now()) WHERE id = {id}"))
        {
            await cmd.ExecuteNonQueryAsync(Ct);
        }

        release.SetResult();
        var completed = await pipeline.WaitForAsync(1);

        Assert.Equal(JobState.Cancelled, Assert.Single(completed).Row!.State);
        var stored = await GetAsync(driver, id);
        Assert.Equal((JobState.Cancelled, 1), (stored.State, stored.Attempt));
        Assert.NotNull(stored.FinalizedAt);
        Assert.Equal("worker failed", Assert.Single(stored.Errors).Error);
    }

    [Fact]
    public async Task Many_jobs_complete_through_batched_writes()
    {
        db.SkipIfUnavailable();
        await using var ds = db.CreateDataSource();
        var (driver, schema) = await MigratedAsync(ds);
        const int Jobs = 500;
        await driver.GetExecutor().JobInsertFastAsync(
            [.. Enumerable.Range(0, Jobs).Select(_ => new JobInsertParams
            {
                Kind = "task", ArgsJson = """{"What":"complete"}""", Queue = "work", State = JobState.Available, Priority = 1, MaxAttempts = 25,
            })],
            Ct);

        await using var pipeline = await Pipeline.StartAsync(driver, new WorkerRegistry().Add(new TaskWorker()), maxWorkers: 50);
        await pipeline.WaitForAsync(Jobs, TimeSpan.FromSeconds(60));

        await using var cmd = ds.CreateCommand($"SELECT count(*) FROM {schema}.river_job WHERE state = 'completed' AND finalized_at IS NOT NULL");
        Assert.Equal((long)Jobs, (long)(await cmd.ExecuteScalarAsync(Ct))!);
        Assert.True(pipeline.Batches < Jobs / 2, $"{pipeline.Batches} writes for {Jobs} jobs: completions were not batched.");
    }

    private static async Task<JobRow> GetAsync(IKopiDriver driver, long id) =>
        await driver.GetExecutor().JobGetByIdAsync(id, Ct) ?? throw new InvalidOperationException($"Job {id} is missing.");

    private static async Task<(IKopiDriver Driver, string Schema)> MigratedAsync(NpgsqlDataSource ds)
    {
        var schema = "x_" + Guid.NewGuid().ToString("N")[..12];
        await using (var cmd = ds.CreateCommand($"CREATE SCHEMA {schema}"))
        {
            await cmd.ExecuteNonQueryAsync(Ct);
        }

        var driver = new NpgsqlDriver(ds, new NpgsqlDriverOptions { Schema = schema });
        await driver.CreateMigrator().MigrateAsync(MigrationDirection.Up, cancellationToken: Ct);
        return (driver, schema);
    }

    [JobKind("task")]
    public sealed record TaskArgs(string What) : IJobArgs;

    private sealed class TaskWorker : Worker<TaskArgs>
    {
        /// <summary>Long enough that a failed job is not fetched again during the test.</summary>
        public override TimeSpan? NextRetry(Job<TaskArgs> job) => TimeSpan.FromMinutes(10);

        public override Task WorkAsync(Job<TaskArgs> job, CancellationToken cancellationToken) => job.Args.What switch
        {
            "complete" => Task.CompletedTask,
            "fail" or "fail-last" => throw new InvalidOperationException("boom"),
            "snooze" => throw JobControl.Snooze(TimeSpan.FromMinutes(10)),
            "cancel" => throw JobControl.Cancel("no longer wanted"),
            _ => throw new ArgumentOutOfRangeException(nameof(job), job.Args.What, null),
        };
    }

    private sealed class GatedWorker(TaskCompletionSource started, TaskCompletionSource release) : Worker<TaskArgs>
    {
        public override async Task WorkAsync(Job<TaskArgs> job, CancellationToken cancellationToken)
        {
            started.TrySetResult();
            await release.Task.WaitAsync(cancellationToken);
            throw new InvalidOperationException("worker failed");
        }
    }

    /// <summary>A producer of queue <c>work</c> running its jobs through an executor into a batch completer.</summary>
    private sealed class Pipeline : IAsyncDisposable
    {
        private readonly ConcurrentQueue<CompletedJob> _completed = new();
        private int _batches;
        private JobCompleter _completer = null!;
        private Producer _producer = null!;

        public int Batches => Volatile.Read(ref _batches);

        public static async Task<Pipeline> StartAsync(IKopiDriver driver, WorkerRegistry workers, int maxWorkers = 10)
        {
            var pipeline = new Pipeline();
            pipeline._completer = JobCompleter.Create(CompleterKind.Batch, driver, onCompleted: batch =>
            {
                Interlocked.Increment(ref pipeline._batches);
                foreach (var c in batch)
                {
                    pipeline._completed.Enqueue(c);
                }
            });
            var executor = new JobExecutor(workers, SystemTextJsonArgsSerializer.Default, pipeline._completer);
            pipeline._producer = new Producer(
                driver,
                new ProducerOptions
                {
                    Queue = "work", MaxWorkers = maxWorkers, ClientId = "test-client",
                    FetchPollInterval = TimeSpan.FromMilliseconds(100), FetchCooldown = TimeSpan.FromMilliseconds(5),
                },
                (job, hardStop, cancel) => executor.ExecuteAsync(job, hardStop, cancel));
            await pipeline._completer.StartAsync(Ct);
            await pipeline._producer.StartAsync(Ct);
            return pipeline;
        }

        public async Task<IReadOnlyList<CompletedJob>> WaitForAsync(int count, TimeSpan? limit = null)
        {
            var deadline = DateTime.UtcNow + (limit ?? TimeSpan.FromSeconds(15));
            while (_completed.Count < count)
            {
                Assert.True(DateTime.UtcNow < deadline, $"{_completed.Count} of {count} jobs completed in time.");
                await Task.Delay(20, Ct);
            }

            return [.. _completed];
        }

        public async ValueTask DisposeAsync()
        {
            await _producer.StopAsync(Ct);
            await _completer.StopAsync(Ct);
            await _producer.DisposeAsync();
            await _completer.DisposeAsync();
        }
    }
}
