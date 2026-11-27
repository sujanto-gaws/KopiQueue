using System.Collections.Concurrent;
using System.Text.Json;
using KopiQueue.Drivers;
using KopiQueue.Migrations;
using KopiQueue.TestInfrastructure;
using Npgsql;

namespace KopiQueue.Npgsql.Tests;

/// <summary>
/// The rescuer on a real PostgreSQL (plan task 3.4: FR-MNT-03, NFR-REL-02; design §5.7): a rescue notifies the queues of
/// jobs it made available, once each and only on commit, and a leader rescues a job a dead client left running.
/// </summary>
/// <remarks>
/// "No notification" is proven without sleeping, as in <see cref="KopiClientInsertTests"/>: a sentinel job is committed
/// afterwards, and it must be the next notification to arrive, because PostgreSQL delivers notifications in commit order.
/// </remarks>
public sealed class NpgsqlRescuerTests(PostgresFixture db) : IClassFixture<PostgresFixture>
{
    private static readonly TimeSpan Timeout = TimeSpan.FromSeconds(10);

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Fact]
    public async Task A_rescue_notifies_each_queue_that_got_an_available_job_once_and_only_when_it_commits()
    {
        db.SkipIfUnavailable();
        await using var ds = db.CreateDataSource();
        var (driver, schema) = await MigratedAsync(ds);
        var executor = driver.GetExecutor();
        var jobs = new List<(JobRow Job, JobState To)>();
        foreach (var (queue, to) in new[] { ("alpha", JobState.Available), ("alpha", JobState.Available), ("beta", JobState.Retryable), ("gamma", JobState.Discarded) })
        {
            jobs.Add((await StartAsync(executor, queue, "dead-client"), to));
        }

        var now = DateTimeOffset.UtcNow;
        JobRescueParams[] rescues =
        [
            .. jobs.Select(j => new JobRescueParams
            {
                Id = j.Job.Id,
                AttemptedAt = j.Job.AttemptedAt!.Value,
                State = j.To,
                ScheduledAt = j.To == JobState.Discarded ? null : now.AddSeconds(1),
                Error = new AttemptError(now, 1, "stuck", null),
            }),
        ];
        await using var listener = await Listener.StartAsync(db.ConnectionString, schema);

        await using (var connection = await ds.OpenConnectionAsync(Ct))
        await using (var rolledBack = await connection.BeginTransactionAsync(Ct))
        {
            Assert.Equal(4, (await driver.GetExecutor(rolledBack).JobRescueManyAsync(rescues, Ct)).Count);
            await rolledBack.RollbackAsync(Ct);
        }

        await InsertSentinelAsync(executor, "sentinel_1");
        Assert.Equal("sentinel_1", Queue(await listener.NextAsync()));

        Assert.Equal(4, (await executor.JobRescueManyAsync(rescues, Ct)).Count);
        await InsertSentinelAsync(executor, "sentinel_2");

        Assert.Equal("alpha", Queue(await listener.NextAsync()));
        Assert.Equal("sentinel_2", Queue(await listener.NextAsync()));
    }

    [Fact]
    public async Task A_leader_rescues_a_job_a_dead_client_left_running_and_runs_it_again()
    {
        db.SkipIfUnavailable();
        await using var ds = db.CreateDataSource();
        var (driver, schema) = await MigratedAsync(ds);
        var left = await StartAsync(driver.GetExecutor(), "rescue_work", "dead-client", kind: "rescued_job");
        await Task.Delay(TimeSpan.FromSeconds(2.5), Ct); // older than the 2 s threshold before the client starts

        var worker = new RecordingWorker();
        var client = new KopiClient(driver, new KopiClientOptions
        {
            Queues = new Dictionary<string, QueueOptions> { ["rescue_work"] = new() { MaxWorkers = 1 } },
            Workers = new WorkerRegistry().Add(worker),
            ClientId = "rescuing-leader",
            JobTimeout = TimeSpan.FromSeconds(1),
            RescueStuckJobsAfter = TimeSpan.FromSeconds(2),
            ElectInterval = TimeSpan.FromMilliseconds(200),
            LeaderTtl = TimeSpan.FromSeconds(2),
        });
        await client.StartAsync(Ct);
        try
        {
            // The first batch runs as soon as the client leads; the retry is due within the scheduler interval, so the job is
            // available at once.
            var ran = await worker.Ran.Task.WaitAsync(Timeout, Ct);

            Assert.Equal((left.Id, 2), (ran.Id, ran.Attempt));
            Assert.Equal(["dead-client", "rescuing-leader"], ran.AttemptedBy);
            var error = Assert.Single(ran.Errors);
            Assert.Equal(1, error.Attempt);
            Assert.StartsWith("The job was still running 00:00:02 after this attempt started", error.Error, StringComparison.Ordinal);
        }
        finally
        {
            await client.StopAsync(Ct);
        }
    }

    private static async Task<JobRow> StartAsync(IKopiExecutor executor, string queue, string clientId, string kind = "stuck_job")
    {
        var id = (await executor.JobInsertManyAsync([Available(queue, kind)], Ct)).Single().Job.Id;
        var fetched = await executor.JobGetAvailableAsync(new JobGetAvailableParams { Queue = queue, Max = 100, ClientId = clientId }, Ct);
        return Assert.Single(fetched, j => j.Id == id);
    }

    private static JobInsertParams Available(string queue, string kind = "stuck_job") => new()
    {
        Kind = kind, ArgsJson = "{}", Queue = queue, State = JobState.Available, Priority = 1, MaxAttempts = 25,
    };

    private static async Task InsertSentinelAsync(IKopiExecutor executor, string queue) =>
        await executor.JobInsertManyAsync([Available(queue)], Ct);

    private static string Queue(NpgsqlNotificationEventArgs notification)
    {
        using var payload = JsonDocument.Parse(notification.Payload);
        return payload.RootElement.GetProperty("queue").GetString()!;
    }

    private static async Task<(IKopiDriver Driver, string Schema)> MigratedAsync(NpgsqlDataSource ds)
    {
        var schema = "r_" + Guid.NewGuid().ToString("N")[..12];
        await using (var cmd = ds.CreateCommand($"CREATE SCHEMA {schema}"))
        {
            await cmd.ExecuteNonQueryAsync(Ct);
        }

        var driver = new NpgsqlDriver(ds, new NpgsqlDriverOptions { Schema = schema });
        await driver.CreateMigrator().MigrateAsync(MigrationDirection.Up, cancellationToken: Ct);
        return (driver, schema);
    }

    [JobKind("rescued_job")]
    public sealed record RescuedArgs : IJobArgs;

    private sealed class RecordingWorker : Worker<RescuedArgs>
    {
        public TaskCompletionSource<JobRow> Ran { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public override Task WorkAsync(Job<RescuedArgs> job, CancellationToken cancellationToken)
        {
            Ran.TrySetResult(job.Row);
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

            return next;
        }

        public ValueTask DisposeAsync() => _connection.DisposeAsync();
    }
}
