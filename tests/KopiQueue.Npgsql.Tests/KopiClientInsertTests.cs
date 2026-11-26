using System.Collections.Concurrent;
using System.Text.Json;
using KopiQueue.Drivers;
using KopiQueue.Migrations;
using KopiQueue.TestInfrastructure;
using Npgsql;

namespace KopiQueue.Npgsql.Tests;

/// <summary>
/// <c>InsertAsync</c> and its insert notification against a real PostgreSQL (plan task 1.6: FR-INS-01, FR-INS-02, FR-INS-07).
/// </summary>
/// <remarks>
/// "No notification" is proven without sleeping: a sentinel job is committed afterwards, and it must be the next
/// notification to arrive, because PostgreSQL delivers notifications in commit order.
/// </remarks>
public sealed class KopiClientInsertTests(PostgresFixture db) : IClassFixture<PostgresFixture>
{
    private static readonly TimeSpan Timeout = TimeSpan.FromSeconds(10);

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Fact]
    public async Task A_pooled_insert_is_stored_and_notifies_its_queue()
    {
        db.SkipIfUnavailable();
        await using var ds = db.CreateDataSource();
        var (client, schema) = await ClientAsync(ds);
        await using var listener = await Listener.StartAsync(db.ConnectionString, schema);

        var result = await client.InsertAsync(new EmailArgs("a@example.com"), new InsertOptions { Queue = "email" }, Ct);

        var notification = await listener.NextAsync();
        Assert.Equal($"{schema}.river_insert", notification.Channel);
        Assert.Equal("email", Queue(notification));
        Assert.Equal(("email_send", "email", JobState.Available), (result.Job.Kind, result.Job.Queue, result.Job.State));
        Assert.Equal("""{"to": "a@example.com"}""", result.Job.ArgsJson);
        Assert.Equal(result.Job.Id, (await StoredAsync(ds, schema, result.Job.Id))?.Id);
    }

    [Fact]
    public async Task A_transaction_insert_notifies_only_when_it_commits()
    {
        db.SkipIfUnavailable();
        await using var ds = db.CreateDataSource();
        var (client, schema) = await ClientAsync(ds);
        await using var listener = await Listener.StartAsync(db.ConnectionString, schema);
        await using var connection = await ds.OpenConnectionAsync(Ct);
        await using var tx = await connection.BeginTransactionAsync(Ct);

        var result = await client.InsertAsync(new EmailArgs("b@example.com"), tx, new InsertOptions { Queue = "email" }, Ct);
        Assert.Null(await StoredAsync(ds, schema, result.Job.Id));
        await client.InsertAsync(new EmailArgs("sentinel"), new InsertOptions { Queue = "sentinel" }, Ct);
        Assert.Equal("sentinel", Queue(await listener.NextAsync()));

        await tx.CommitAsync(Ct);

        Assert.Equal("email", Queue(await listener.NextAsync()));
        Assert.NotNull(await StoredAsync(ds, schema, result.Job.Id));
    }

    [Fact]
    public async Task A_rolled_back_insert_neither_stores_nor_notifies()
    {
        db.SkipIfUnavailable();
        await using var ds = db.CreateDataSource();
        var (client, schema) = await ClientAsync(ds);
        await using var listener = await Listener.StartAsync(db.ConnectionString, schema);

        long id;
        await using (var connection = await ds.OpenConnectionAsync(Ct))
        await using (var tx = await connection.BeginTransactionAsync(Ct))
        {
            id = (await client.InsertAsync(new EmailArgs("gone"), tx, new InsertOptions { Queue = "gone" }, Ct)).Job.Id;
            await tx.RollbackAsync(Ct);
        }

        await client.InsertAsync(new EmailArgs("sentinel"), new InsertOptions { Queue = "sentinel" }, Ct);

        Assert.Equal("sentinel", Queue(await listener.NextAsync()));
        Assert.Null(await StoredAsync(ds, schema, id));
    }

    [Fact]
    public async Task A_scheduled_job_does_not_notify()
    {
        db.SkipIfUnavailable();
        await using var ds = db.CreateDataSource();
        var (client, schema) = await ClientAsync(ds);
        await using var listener = await Listener.StartAsync(db.ConnectionString, schema);

        var later = await client.InsertAsync(new EmailArgs("later"), new InsertOptions { Queue = "later", ScheduledAt = DateTimeOffset.UtcNow.AddHours(1) }, Ct);
        await client.InsertAsync(new EmailArgs("sentinel"), new InsertOptions { Queue = "sentinel" }, Ct);

        Assert.Equal(JobState.Scheduled, later.Job.State);
        Assert.Equal("sentinel", Queue(await listener.NextAsync()));
    }

    [Fact]
    public async Task A_batch_notifies_each_queue_with_available_jobs_once()
    {
        db.SkipIfUnavailable();
        await using var ds = db.CreateDataSource();
        var (client, schema) = await ClientAsync(ds);
        IKopiDriver driver = new NpgsqlDriver(ds, new NpgsqlDriverOptions { Schema = schema });
        await using var listener = await Listener.StartAsync(db.ConnectionString, schema);
        var now = DateTimeOffset.UtcNow;

        await driver.GetExecutor().JobInsertManyAsync(
            [
                JobValidation.ToInsertParams("k", "{}", new InsertOptions { Queue = "a" }, now),
                JobValidation.ToInsertParams("k", "{}", new InsertOptions { Queue = "b" }, now),
                JobValidation.ToInsertParams("k", "{}", new InsertOptions { Queue = "a" }, now),
                JobValidation.ToInsertParams("k", "{}", new InsertOptions { Queue = "c", ScheduledAt = now.AddHours(1) }, now),
                JobValidation.ToInsertParams("k", "{}", new InsertOptions { Queue = "a" }, now),
            ],
            Ct);
        await client.InsertAsync(new EmailArgs("sentinel"), new InsertOptions { Queue = "sentinel" }, Ct);

        var queues = new[] { Queue(await listener.NextAsync()), Queue(await listener.NextAsync()) };
        Assert.Equal(["a", "b"], queues.Order(StringComparer.Ordinal));
        Assert.Equal("sentinel", Queue(await listener.NextAsync()));
    }

    [Fact]
    public async Task A_failed_insert_does_not_notify()
    {
        db.SkipIfUnavailable();
        await using var ds = db.CreateDataSource();
        var (client, schema) = await ClientAsync(ds);
        IKopiDriver driver = new NpgsqlDriver(ds, new NpgsqlDriverOptions { Schema = schema });
        await using var listener = await Listener.StartAsync(db.ConnectionString, schema);

        // Priority 9 passes the driver but fails the table's CHECK, after the notify was queued in the same batch.
        await Assert.ThrowsAsync<PostgresException>(() => driver.GetExecutor().JobInsertManyAsync(
            [new JobInsertParams { Kind = "k", ArgsJson = "{}", Queue = "bad", State = JobState.Available, Priority = 9, MaxAttempts = 1 }], Ct));
        await client.InsertAsync(new EmailArgs("sentinel"), new InsertOptions { Queue = "sentinel" }, Ct);

        Assert.Equal("sentinel", Queue(await listener.NextAsync()));
    }

    [Fact]
    public async Task Without_a_schema_the_channel_uses_the_current_schema()
    {
        db.SkipIfUnavailable();
        string schema;
        await using (var setup = db.CreateDataSource())
        {
            schema = await CreateSchemaAsync(setup);
        }

        await using var ds = NpgsqlDataSource.Create(new NpgsqlConnectionStringBuilder(db.ConnectionString) { SearchPath = schema }.ConnectionString);
        var driver = new NpgsqlDriver(ds);
        await driver.CreateMigrator().MigrateAsync(MigrationDirection.Up, cancellationToken: Ct);
        await using var listener = await Listener.StartAsync(db.ConnectionString, schema);

        await new KopiClient(driver).InsertAsync(new EmailArgs("x"), cancellationToken: Ct);

        var notification = await listener.NextAsync();
        Assert.Equal(($"{schema}.river_insert", "default"), (notification.Channel, Queue(notification)));
    }

    [Fact]
    public async Task A_schema_too_long_for_a_channel_still_inserts()
    {
        db.SkipIfUnavailable();
        await using var ds = db.CreateDataSource();
        var schema = await CreateSchemaAsync(ds, "l_" + new string('x', 52) + Guid.NewGuid().ToString("N")[..8]);
        var driver = new NpgsqlDriver(ds, new NpgsqlDriverOptions { Schema = schema });
        await driver.CreateMigrator().MigrateAsync(MigrationDirection.Up, cancellationToken: Ct);

        var result = await new KopiClient(driver).InsertAsync(new EmailArgs("long"), cancellationToken: Ct);

        Assert.Equal(62, schema.Length);
        Assert.NotNull(await StoredAsync(ds, schema, result.Job.Id));
    }

    private static string Queue(NpgsqlNotificationEventArgs notification)
    {
        using var payload = JsonDocument.Parse(notification.Payload);
        return payload.RootElement.GetProperty("queue").GetString()!;
    }

    private static async Task<JobRow?> StoredAsync(NpgsqlDataSource ds, string schema, long id) =>
        await ((IKopiDriver)new NpgsqlDriver(ds, new NpgsqlDriverOptions { Schema = schema })).GetExecutor().JobGetByIdAsync(id, Ct);

    private static async Task<(KopiClient Client, string Schema)> ClientAsync(NpgsqlDataSource ds)
    {
        var schema = await CreateSchemaAsync(ds);
        var driver = new NpgsqlDriver(ds, new NpgsqlDriverOptions { Schema = schema });
        await driver.CreateMigrator().MigrateAsync(MigrationDirection.Up, cancellationToken: Ct);
        return (new KopiClient(driver), schema);
    }

    private static async Task<string> CreateSchemaAsync(NpgsqlDataSource ds, string? name = null)
    {
        name ??= "i_" + Guid.NewGuid().ToString("N")[..12];
        await using var cmd = ds.CreateCommand($"CREATE SCHEMA {name}");
        await cmd.ExecuteNonQueryAsync(Ct);
        return name;
    }

    [JobKind("email_send")]
    private sealed record EmailArgs(string To) : IJobArgs;

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
