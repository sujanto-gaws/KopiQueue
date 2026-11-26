using System.Collections.Concurrent;
using System.Diagnostics;
using KopiQueue.Drivers;
using KopiQueue.Execution;
using KopiQueue.Migrations;
using KopiQueue.TestInfrastructure;
using Npgsql;

namespace KopiQueue.Npgsql.Tests;

/// <summary>
/// Producers fetching from a real PostgreSQL, woken by notifications, by their own client, or by the poll timer (plan
/// task 2.3: FR-EXE-01, FR-EXE-03, FR-EXE-04).
/// </summary>
public sealed class NpgsqlProducerTests(PostgresFixture db) : IClassFixture<PostgresFixture>
{
    private static readonly TimeSpan Timeout = TimeSpan.FromSeconds(10);

    /// <summary>Long enough that a test passing within <see cref="Timeout"/> was not helped by a poll.</summary>
    private static readonly TimeSpan NoPoll = TimeSpan.FromHours(1);

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Fact]
    public async Task An_insert_notification_from_another_client_wakes_the_producer()
    {
        db.SkipIfUnavailable();
        await using var ds = db.CreateDataSource();
        var (driver, _) = await MigratedAsync(ds);
        var started = new ConcurrentQueue<JobRow>();
        await using var notifier = new Notifier(driver.CreateListener()!);
        await using var producer = new Producer(driver, Options("email", NoPoll), Record(started), notifier: notifier);
        await notifier.StartAsync(Ct);
        await Until(() => notifier.IsConnected);
        await producer.StartAsync(Ct);
        await Task.Delay(300, Ct); // past the first fetch and its cooldown

        var watch = Stopwatch.StartNew();
        var id = (await new KopiClient(driver).InsertAsync(new EmailArgs("a@example.com"), new InsertOptions { Queue = "email" }, Ct)).Job.Id;
        await Until(() => !started.IsEmpty);

        var job = Assert.Single(started);
        Assert.Equal((id, JobState.Running, 1), (job.Id, job.State, job.Attempt));
        Assert.Equal(["producer-email"], job.AttemptedBy);
        Assert.True(watch.Elapsed < TimeSpan.FromSeconds(2), $"Picked up after {watch.Elapsed}.");
    }

    [Fact]
    public async Task An_insert_by_its_own_client_wakes_the_producer_without_notifications()
    {
        db.SkipIfUnavailable();
        await using var ds = db.CreateDataSource();
        var (driver, _) = await MigratedAsync(ds);
        var client = new KopiClient(driver);
        var started = new ConcurrentQueue<JobRow>();
        await using var producer = new Producer(driver, Options("email", NoPoll), Record(started), localWaker: client.LocalWaker);
        await producer.StartAsync(Ct);
        await Task.Delay(300, Ct);

        var id = (await client.InsertAsync(new EmailArgs("b@example.com"), new InsertOptions { Queue = "email" }, Ct)).Job.Id;
        await Until(() => !started.IsEmpty);

        Assert.Equal(id, Assert.Single(started).Id);
    }

    [Fact]
    public async Task Poll_only_producers_pick_up_jobs_at_the_poll_interval()
    {
        db.SkipIfUnavailable();
        await using var ds = db.CreateDataSource();
        var (driver, _) = await MigratedAsync(ds);
        var started = new ConcurrentQueue<JobRow>();
        await using var producer = new Producer(driver, Options("email", TimeSpan.FromMilliseconds(200)), Record(started));
        await producer.StartAsync(Ct);
        await Task.Delay(300, Ct);

        await driver.GetExecutor().JobInsertManyAsync([Job("email"), Job("email"), Job("other")], Ct);
        await Until(() => started.Count == 2);
        await Task.Delay(500, Ct);

        Assert.Equal(2, started.Count);
        Assert.All(started, j => Assert.Equal("email", j.Queue));
    }

    [Fact]
    public async Task Producers_of_several_clients_share_a_queue_without_running_a_job_twice()
    {
        db.SkipIfUnavailable();
        await using var ds = db.CreateDataSource();
        var (driver, schema) = await MigratedAsync(ds);
        const int Jobs = 400;
        var started = new ConcurrentQueue<JobRow>();
        var peaks = new int[3];
        var producers = new List<Producer>();
        for (var i = 0; i < 3; i++)
        {
            var client = i;
            var running = 0;
            var producer = new Producer(
                driver,
                new ProducerOptions
                {
                    Queue = "bulk", MaxWorkers = 5, ClientId = $"client-{client}", FetchBatchMax = 4,
                    FetchPollInterval = TimeSpan.FromMilliseconds(200), FetchCooldown = TimeSpan.FromMilliseconds(5),
                },
                async (job, ct) =>
                {
                    var now = Interlocked.Increment(ref running);
                    InterlockedMax(ref peaks[client], now);
                    started.Enqueue(job);
                    await Task.Delay(Random.Shared.Next(2, 6), ct); // long enough that one client cannot drain the queue alone
                    Interlocked.Decrement(ref running);
                });
            producers.Add(producer);
        }

        foreach (var producer in producers)
        {
            await producer.StartAsync(Ct);
        }

        // Insert once every producer is running, so none of them starts late and finds the queue drained under load.
        await driver.GetExecutor().JobInsertFastAsync([.. Enumerable.Range(0, Jobs).Select(_ => Job("bulk"))], Ct);
        await Until(() => started.Count >= Jobs, TimeSpan.FromSeconds(60));
        foreach (var producer in producers)
        {
            await producer.DisposeAsync();
        }

        Assert.Equal(Jobs, started.Count);
        Assert.Equal(Jobs, started.Select(j => j.Id).Distinct().Count());
        Assert.All(peaks, peak => Assert.InRange(peak, 0, 5)); // never more than MaxWorkers at once per client
        Assert.All(started, j => Assert.Equal(1, j.Attempt));
        Assert.True(started.Select(j => j.AttemptedBy[0]).Distinct().Count() > 1, "One client took every job.");
        await using var cmd = ds.CreateCommand($"SELECT count(*) FROM {schema}.river_job WHERE state = 'running' AND attempt = 1 AND cardinality(attempted_by) = 1");
        Assert.Equal(Jobs, (long)(await cmd.ExecuteScalarAsync(Ct))!);
    }

    private static ProducerOptions Options(string queue, TimeSpan poll) =>
        new() { Queue = queue, MaxWorkers = 10, ClientId = $"producer-{queue}", FetchPollInterval = poll };

    private static Func<JobRow, CancellationToken, Task> Record(ConcurrentQueue<JobRow> started) => (job, _) =>
    {
        started.Enqueue(job);
        return Task.CompletedTask;
    };

    private static JobInsertParams Job(string queue) =>
        new() { Kind = "email_send", ArgsJson = "{}", Queue = queue, State = JobState.Available, Priority = 1, MaxAttempts = 25 };

    private static void InterlockedMax(ref int target, int value)
    {
        int current;
        while ((current = Volatile.Read(ref target)) < value && Interlocked.CompareExchange(ref target, value, current) != current)
        {
        }
    }

    private static async Task Until(Func<bool> condition, TimeSpan? limit = null)
    {
        var deadline = DateTime.UtcNow + (limit ?? Timeout);
        while (!condition())
        {
            Assert.True(DateTime.UtcNow < deadline, "The condition was not met in time.");
            await Task.Delay(10, Ct);
        }
    }

    private static async Task<(IKopiDriver Driver, string Schema)> MigratedAsync(NpgsqlDataSource ds)
    {
        var schema = "p_" + Guid.NewGuid().ToString("N")[..12];
        await using (var cmd = ds.CreateCommand($"CREATE SCHEMA {schema}"))
        {
            await cmd.ExecuteNonQueryAsync(Ct);
        }

        var driver = new NpgsqlDriver(ds, new NpgsqlDriverOptions { Schema = schema });
        await driver.CreateMigrator().MigrateAsync(MigrationDirection.Up, cancellationToken: Ct);
        return (driver, schema);
    }

    [JobKind("email_send")]
    private sealed record EmailArgs(string To) : IJobArgs;
}
