using System.Collections.Concurrent;
using System.Diagnostics;
using System.Text.Json;
using KopiQueue.Drivers;
using KopiQueue.Migrations;
using KopiQueue.TestInfrastructure;
using Npgsql;

namespace KopiQueue.Npgsql.Tests;

/// <summary>
/// The scheduler on a real PostgreSQL (plan task 3.3: FR-SCH-01, FR-SCH-02, FR-MNT-02; design §5.7): only the leader
/// schedules, each affected queue is notified once and only on commit, and a scheduled job runs soon after it is due.
/// </summary>
/// <remarks>
/// "No notification" is proven without sleeping, as in <see cref="KopiClientInsertTests"/>: a sentinel job is committed
/// afterwards, and it must be the next notification to arrive, because PostgreSQL delivers notifications in commit order.
/// </remarks>
public sealed class NpgsqlSchedulerTests(PostgresFixture db) : IClassFixture<PostgresFixture>
{
    private static readonly TimeSpan Timeout = TimeSpan.FromSeconds(10);

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Fact]
    public async Task A_batch_notifies_each_queue_that_got_jobs_once_and_only_when_it_commits()
    {
        db.SkipIfUnavailable();
        await using var ds = db.CreateDataSource();
        var (driver, schema) = await MigratedAsync(ds);
        var past = DateTimeOffset.UtcNow.AddMinutes(-5);
        var jobs = await driver.GetExecutor().JobInsertManyAsync(
            [
                Due("alpha", JobState.Scheduled, past), Due("alpha", JobState.Retryable, past), Due("alpha", JobState.Scheduled, past),
                Due("beta", JobState.Retryable, past), Due("beta", JobState.Scheduled, past),
                Due("gamma", JobState.Available, past), // holds the unique slot
                Due("gamma", JobState.Retryable, past), // conflicts, so it is discarded, which notifies nothing
            ],
            Ct);
        await SetUniqueAsync(ds, schema, jobs[5].Job.Id, states: "11110101");
        await SetUniqueAsync(ds, schema, jobs[6].Job.Id, states: "11010001");
        await using var listener = await Listener.StartAsync(db.ConnectionString, schema);

        await using (var connection = await ds.OpenConnectionAsync(Ct))
        await using (var rolledBack = await connection.BeginTransactionAsync(Ct))
        {
            Assert.Equal(6, (await driver.GetExecutor(rolledBack).JobScheduleAsync(new JobScheduleParams { Max = 100 }, Ct)).Count);
            await rolledBack.RollbackAsync(Ct);
        }

        await InsertSentinelAsync(driver, "sentinel_1");
        Assert.Equal("sentinel_1", Queue(await listener.NextAsync()));

        var changed = await driver.GetExecutor().JobScheduleAsync(new JobScheduleParams { Max = 100 }, Ct);
        await InsertSentinelAsync(driver, "sentinel_2");

        Assert.Equal(6, changed.Count);
        Assert.Equal(JobState.Discarded, Assert.Single(changed, c => c.Queue == "gamma").State);
        var received = new List<string>();
        for (var next = Queue(await listener.NextAsync()); next != "sentinel_2"; next = Queue(await listener.NextAsync()))
        {
            received.Add(next);
        }

        Assert.Equal(["alpha", "beta"], received.Order(StringComparer.Ordinal));
        Assert.All(listener.Channels, channel => Assert.Equal($"{schema}.river_insert", channel));
    }

    [Fact]
    public async Task A_follower_does_not_schedule_and_the_leader_does_as_soon_as_it_wins()
    {
        db.SkipIfUnavailable();
        await using var ds = db.CreateDataSource();
        var (driver, schema) = await MigratedAsync(ds);
        await using (var cmd = ds.CreateCommand(
            $"INSERT INTO {schema}.river_leader (leader_id, elected_at, expires_at) VALUES ('someone-else', now(), now() + interval '1 hour')"))
        {
            await cmd.ExecuteNonQueryAsync(Ct);
        }

        var job = (await driver.GetExecutor().JobInsertManyAsync([Due("sched_work", JobState.Scheduled, DateTimeOffset.UtcNow.AddMinutes(-1))], Ct)).Single().Job;
        var worker = new RecordingWorker();
        var client = await StartedAsync(driver, worker, "follower", TimeSpan.FromMilliseconds(200));
        try
        {
            // Many scheduler intervals and election attempts: the follower never promotes the job.
            await Task.Delay(TimeSpan.FromSeconds(1.5), Ct);
            Assert.Equal(JobState.Scheduled, (await driver.GetExecutor().JobGetByIdAsync(job.Id, Ct))!.State);
            Assert.Equal("someone-else", await LeaderAsync(ds, schema));

            await using (var cmd = ds.CreateCommand($"DELETE FROM {schema}.river_leader"))
            {
                await cmd.ExecuteNonQueryAsync(Ct);
            }

            await worker.Ran.Task.WaitAsync(Timeout, Ct);
            Assert.Equal("follower", await LeaderAsync(ds, schema));
        }
        finally
        {
            await client.StopAsync(Ct);
        }
    }

    [Fact]
    public async Task An_insert_only_client_does_not_schedule()
    {
        db.SkipIfUnavailable();
        await using var ds = db.CreateDataSource();
        var (driver, schema) = await MigratedAsync(ds);
        var client = new KopiClient(driver, new KopiClientOptions { SchedulerInterval = TimeSpan.FromMilliseconds(100) });
        await client.StartAsync(Ct);
        try
        {
            var job = await client.InsertAsync(new ScheduledArgs(), new InsertOptions { Queue = "sched_work", ScheduledAt = DateTimeOffset.UtcNow.AddMilliseconds(200) }, Ct);
            Assert.Equal(JobState.Scheduled, job.Job.State);

            await Task.Delay(TimeSpan.FromSeconds(1), Ct);

            Assert.Equal(JobState.Scheduled, (await driver.GetExecutor().JobGetByIdAsync(job.Job.Id, Ct))!.State);
            Assert.Null(await LeaderAsync(ds, schema));
        }
        finally
        {
            await client.StopAsync(Ct);
        }
    }

    [Fact]
    public async Task A_job_scheduled_a_few_seconds_ahead_runs_within_the_scheduler_interval_of_its_due_time()
    {
        db.SkipIfUnavailable();
        await using var ds = db.CreateDataSource();
        var (driver, schema) = await MigratedAsync(ds);
        var interval = TimeSpan.FromSeconds(1);
        var worker = new RecordingWorker();
        var client = await StartedAsync(driver, worker, "on-time", interval);
        try
        {
            var deadline = Stopwatch.StartNew();
            while (await LeaderAsync(ds, schema) is null)
            {
                Assert.True(deadline.Elapsed < Timeout, "The client did not become leader.");
                await Task.Delay(50, Ct);
            }

            var dueAt = DateTimeOffset.UtcNow.AddSeconds(3);
            var job = await client.InsertAsync(new ScheduledArgs(), new InsertOptions { Queue = "sched_work", ScheduledAt = dueAt }, Ct);
            Assert.Equal(JobState.Scheduled, job.Job.State);

            var ranAt = await worker.Ran.Task.WaitAsync(Timeout, Ct);

            // The scheduler runs every interval ±10%; the notification then wakes the producer. Allow a little for the
            // fetch and for a loaded machine, but far less than the 1 s poll interval would add on its own twice over.
            var late = ranAt - dueAt;
            Assert.True(late >= TimeSpan.FromMilliseconds(-100), $"Ran {-late} before it was due.");
            Assert.True(late <= (interval * 1.1) + TimeSpan.FromMilliseconds(750), $"Ran {late} after it was due.");
        }
        finally
        {
            await client.StopAsync(Ct);
        }
    }

    private static JobInsertParams Due(string queue, JobState state, DateTimeOffset scheduledAt) => new()
    {
        Kind = "scheduled_job",
        ArgsJson = "{}",
        Queue = queue,
        State = state,
        Priority = 1,
        MaxAttempts = 25,
        ScheduledAt = scheduledAt,
    };

    private static async Task InsertSentinelAsync(IKopiDriver driver, string queue) =>
        await driver.GetExecutor().JobInsertManyAsync([Due(queue, JobState.Available, DateTimeOffset.UtcNow)], Ct);

    private static async Task SetUniqueAsync(NpgsqlDataSource ds, string schema, long id, string states)
    {
        await using var cmd = ds.CreateCommand($"UPDATE {schema}.river_job SET unique_key = $2, unique_states = $3::bit(8) WHERE id = $1");
        cmd.Parameters.Add(new NpgsqlParameter { Value = id });
        cmd.Parameters.Add(new NpgsqlParameter { Value = new byte[] { 1, 2, 3 } });
        cmd.Parameters.Add(new NpgsqlParameter { Value = states });
        await cmd.ExecuteNonQueryAsync(Ct);
    }

    private static async Task<KopiClient> StartedAsync(IKopiDriver driver, RecordingWorker worker, string clientId, TimeSpan schedulerInterval)
    {
        var client = new KopiClient(driver, new KopiClientOptions
        {
            Queues = new Dictionary<string, QueueOptions> { ["sched_work"] = new() { MaxWorkers = 1 } },
            Workers = new WorkerRegistry().Add(worker),
            ClientId = clientId,
            ElectInterval = TimeSpan.FromMilliseconds(200),
            LeaderTtl = TimeSpan.FromSeconds(2),
            SchedulerInterval = schedulerInterval,
        });
        await client.StartAsync(Ct);
        return client;
    }

    private static string Queue(NpgsqlNotificationEventArgs notification)
    {
        using var payload = JsonDocument.Parse(notification.Payload);
        return payload.RootElement.GetProperty("queue").GetString()!;
    }

    private static async Task<string?> LeaderAsync(NpgsqlDataSource ds, string schema)
    {
        await using var cmd = ds.CreateCommand($"SELECT leader_id FROM {schema}.river_leader");
        return await cmd.ExecuteScalarAsync(Ct) as string;
    }

    private static async Task<(IKopiDriver Driver, string Schema)> MigratedAsync(NpgsqlDataSource ds)
    {
        var schema = "s_" + Guid.NewGuid().ToString("N")[..12];
        await using (var cmd = ds.CreateCommand($"CREATE SCHEMA {schema}"))
        {
            await cmd.ExecuteNonQueryAsync(Ct);
        }

        var driver = new NpgsqlDriver(ds, new NpgsqlDriverOptions { Schema = schema });
        await driver.CreateMigrator().MigrateAsync(MigrationDirection.Up, cancellationToken: Ct);
        return (driver, schema);
    }

    [JobKind("scheduled_job")]
    public sealed record ScheduledArgs : IJobArgs;

    private sealed class RecordingWorker : Worker<ScheduledArgs>
    {
        public TaskCompletionSource<DateTimeOffset> Ran { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public override Task WorkAsync(Job<ScheduledArgs> job, CancellationToken cancellationToken)
        {
            Ran.TrySetResult(DateTimeOffset.UtcNow);
            return Task.CompletedTask;
        }
    }

    /// <summary>A dedicated connection listening on a schema's insert channel.</summary>
    private sealed class Listener : IAsyncDisposable
    {
        private readonly NpgsqlConnection _connection;
        private readonly ConcurrentQueue<NpgsqlNotificationEventArgs> _received = new();

        private Listener(NpgsqlConnection connection)
        {
            _connection = connection;
            _connection.Notification += (_, e) => _received.Enqueue(e);
        }

        public ConcurrentBag<string> Channels { get; } = [];

        public static async Task<Listener> StartAsync(string connectionString, string schema)
        {
            var connection = new NpgsqlConnection(new NpgsqlConnectionStringBuilder(connectionString) { Pooling = false }.ConnectionString);
            await connection.OpenAsync(Ct);
            var listener = new Listener(connection);
            await using var cmd = new NpgsqlCommand($"LISTEN \"{schema}.river_insert\"", connection);
            await cmd.ExecuteNonQueryAsync(Ct);
            return listener;
        }

        public async Task<NpgsqlNotificationEventArgs> NextAsync()
        {
            var deadline = DateTime.UtcNow + Timeout;
            NpgsqlNotificationEventArgs? next;
            while (!_received.TryDequeue(out next))
            {
                var remaining = deadline - DateTime.UtcNow;
                if (remaining <= TimeSpan.Zero || !await _connection.WaitAsync((int)remaining.TotalMilliseconds, Ct))
                {
                    throw new TimeoutException("No notification arrived.");
                }
            }

            Channels.Add(next.Channel);
            return next;
        }

        public ValueTask DisposeAsync() => _connection.DisposeAsync();
    }
}
