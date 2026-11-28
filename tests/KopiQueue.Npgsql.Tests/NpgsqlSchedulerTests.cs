using System.Collections.Concurrent;
using System.Text.Json;
using KopiQueue.Drivers;
using KopiQueue.TestInfrastructure.Integration;
using Npgsql;

namespace KopiQueue.Npgsql.Tests;

/// <summary>
/// The shared scheduler suite on PostgreSQL, and its notifications: each affected queue is notified once and only on
/// commit (plan task 3.3: FR-SCH-02).
/// </summary>
/// <remarks>
/// "No notification" is proven without sleeping, as in <see cref="KopiClientInsertTests"/>: a sentinel job is committed
/// afterwards, and it must be the next notification to arrive, because PostgreSQL delivers notifications in commit order.
/// </remarks>
public sealed class NpgsqlSchedulerTests(NpgsqlIntegrationFixture db)
    : SchedulerTests<NpgsqlIntegrationFixture>(db), IClassFixture<NpgsqlIntegrationFixture>
{
    [Fact]
    public async Task A_batch_notifies_each_queue_that_got_jobs_once_and_only_when_it_commits()
    {
        var schema = await Fixture.CreateSchemaAsync(Ct);
        var driver = schema.Driver;
        var past = DateTimeOffset.UtcNow.AddMinutes(-5);
        var jobs = await driver.GetExecutor().JobInsertManyAsync(
            [
                Due("alpha", JobState.Scheduled, past), Due("alpha", JobState.Retryable, past), Due("alpha", JobState.Scheduled, past),
                Due("beta", JobState.Retryable, past), Due("beta", JobState.Scheduled, past),
                Due("gamma", JobState.Available, past), // holds the unique slot
                Due("gamma", JobState.Retryable, past), // conflicts, so it is discarded, which notifies nothing
            ],
            Ct);
        await schema.SetUniqueAsync(jobs[5].Job.Id, [1, 2, 3], 0b1111_0101, Ct);
        await schema.SetUniqueAsync(jobs[6].Job.Id, [1, 2, 3], 0b1101_0001, Ct);
        await using var listener = await Listener.StartAsync(Fixture.ConnectionString, schema.Name);

        await using (var rolledBack = await schema.BeginTransactionAsync(Ct))
        {
            Assert.Equal(6, (await driver.GetExecutor(rolledBack.Transaction).JobScheduleAsync(new JobScheduleParams { Max = 100 }, Ct)).Count);
            await rolledBack.Transaction.RollbackAsync(Ct);
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
        Assert.All(listener.Channels, channel => Assert.Equal($"{schema.Name}.river_insert", channel));
    }

    private static async Task InsertSentinelAsync(IKopiDriver driver, string queue) =>
        await driver.GetExecutor().JobInsertManyAsync([Due(queue, JobState.Available, DateTimeOffset.UtcNow)], Ct);

    private static string Queue(NpgsqlNotificationEventArgs notification)
    {
        using var payload = JsonDocument.Parse(notification.Payload);
        return payload.RootElement.GetProperty("queue").GetString()!;
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
