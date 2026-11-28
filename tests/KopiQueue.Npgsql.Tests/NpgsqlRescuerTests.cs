using System.Collections.Concurrent;
using System.Text.Json;
using KopiQueue.Drivers;
using KopiQueue.TestInfrastructure.Integration;
using Npgsql;

namespace KopiQueue.Npgsql.Tests;

/// <summary>
/// The shared rescuer suite on PostgreSQL, and its notifications: a rescue notifies the queues of jobs it made available,
/// once each and only on commit (plan task 3.4).
/// </summary>
/// <remarks>
/// "No notification" is proven without sleeping, as in <see cref="KopiClientInsertTests"/>: a sentinel job is committed
/// afterwards, and it must be the next notification to arrive, because PostgreSQL delivers notifications in commit order.
/// </remarks>
public sealed class NpgsqlRescuerTests(NpgsqlIntegrationFixture db)
    : RescuerTests<NpgsqlIntegrationFixture>(db), IClassFixture<NpgsqlIntegrationFixture>
{
    [Fact]
    public async Task A_rescue_notifies_each_queue_that_got_an_available_job_once_and_only_when_it_commits()
    {
        await using var schema = await Fixture.CreateSchemaAsync(Ct);
        var driver = schema.Driver;
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
        await using var listener = await Listener.StartAsync(Fixture.ConnectionString, schema.Name);

        await using (var rolledBack = await schema.BeginTransactionAsync(Ct))
        {
            Assert.Equal(4, (await driver.GetExecutor(rolledBack.Transaction).JobRescueManyAsync(rescues, Ct)).Count);
            await rolledBack.Transaction.RollbackAsync(Ct);
        }

        await InsertSentinelAsync(executor, "sentinel_1");
        Assert.Equal("sentinel_1", Queue(await listener.NextAsync()));

        Assert.Equal(4, (await executor.JobRescueManyAsync(rescues, Ct)).Count);
        await InsertSentinelAsync(executor, "sentinel_2");

        Assert.Equal("alpha", Queue(await listener.NextAsync()));
        Assert.Equal("sentinel_2", Queue(await listener.NextAsync()));
    }

    private static async Task InsertSentinelAsync(IKopiExecutor executor, string queue) =>
        await executor.JobInsertManyAsync([Available(queue)], Ct);

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
