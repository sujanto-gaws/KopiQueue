using System.Diagnostics;
using System.Text.Json;
using System.Threading.Channels;
using KopiQueue.Drivers;
using KopiQueue.Execution;
using KopiQueue.Migrations;
using KopiQueue.TestInfrastructure;
using Npgsql;

namespace KopiQueue.Npgsql.Tests;

/// <summary>
/// The notifier on a real PostgreSQL through <see cref="NpgsqlListener"/> (plan task 2.2: FR-EXE-03, NFR-REL-04a;
/// spike 0.7 D1-D4, D7).
/// </summary>
public sealed class NpgsqlNotifierTests(PostgresFixture db) : IClassFixture<PostgresFixture>
{
    private static readonly TimeSpan Timeout = TimeSpan.FromSeconds(10);

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Fact]
    public async Task Inserts_reach_the_notifier_through_a_connection_cloned_from_the_data_source()
    {
        db.SkipIfUnavailable();
        await using var ds = db.CreateDataSource();
        if (new NpgsqlConnectionStringBuilder(db.ConnectionString).Password is not null)
        {
            Assert.Null(new NpgsqlConnectionStringBuilder(ds.ConnectionString).Password);
        }

        var (client, driver, schema) = await ClientAsync(ds);
        await using var notifier = new Notifier(driver.CreateListener()!);
        var inserts = Events();
        var others = Events();
        using var a = notifier.Subscribe(NotificationTopic.Insert, p => inserts.Writer.TryWrite(Queue(p)), () => inserts.Writer.TryWrite("resync"));
        using var b = notifier.Subscribe(NotificationTopic.Control, p => others.Writer.TryWrite("control " + p), () => { });
        using var c = notifier.Subscribe(NotificationTopic.Leadership, p => others.Writer.TryWrite("leadership " + p), () => { });

        await notifier.StartAsync(Ct);
        Assert.Equal("resync", await NextAsync(inserts));
        await client.InsertAsync(new EmailArgs("a@example.com"), new InsertOptions { Queue = "email" }, Ct);
        await NotifyAsync(ds, $"{schema}.river_control", """{"action":"pause","queue":"*"}""");
        await NotifyAsync(ds, $"{schema}.river_leadership", """{"action":"resigned"}""");

        Assert.Equal("email", await NextAsync(inserts));
        Assert.Equal("""control {"action":"pause","queue":"*"}""", await NextAsync(others));
        Assert.Equal("""leadership {"action":"resigned"}""", await NextAsync(others));
        Assert.True(notifier.IsConnected);
    }

    [Fact]
    public async Task The_notifier_uses_its_own_unpooled_session_and_closes_it_on_stop()
    {
        db.SkipIfUnavailable();
        await using var ds = db.CreateDataSource();
        var (_, driver, schema) = await ClientAsync(ds);
        var notifier = new Notifier(driver.CreateListener()!);
        await notifier.StartAsync(Ct);
        await Until(() => notifier.IsConnected);

        var pid = await ListenerPidAsync(ds, schema);
        Assert.NotNull(pid);
        await notifier.DisposeAsync();

        await UntilAsync(async () => await ListenerPidAsync(ds, schema) is null);
    }

    [Fact]
    public async Task Without_a_schema_the_channels_use_the_current_schema()
    {
        db.SkipIfUnavailable();
        await using var setup = db.CreateDataSource();
        var (_, _, schema) = await ClientAsync(setup);
        await using var ds = NpgsqlDataSource.Create(new NpgsqlConnectionStringBuilder(db.ConnectionString) { SearchPath = schema }.ConnectionString);
        var driver = (IKopiDriver)new NpgsqlDriver(ds);
        var client = new KopiClient(driver);
        await using var notifier = new Notifier(driver.CreateListener()!);
        var inserts = Events();
        using var subscription = notifier.Subscribe(NotificationTopic.Insert, p => inserts.Writer.TryWrite(Queue(p)), () => inserts.Writer.TryWrite("resync"));

        await notifier.StartAsync(Ct);
        Assert.Equal("resync", await NextAsync(inserts));
        await client.InsertAsync(new EmailArgs("b@example.com"), cancellationToken: Ct);

        Assert.Equal("default", await NextAsync(inserts));
    }

    [Fact]
    public async Task A_connection_closed_by_the_server_is_reconnected_and_every_subscriber_resyncs()
    {
        db.SkipIfUnavailable();
        await using var ds = db.CreateDataSource();
        var (client, driver, schema) = await ClientAsync(ds);
        await using var notifier = new Notifier(driver.CreateListener()!);
        var inserts = Events();
        var leadership = Events();
        using var a = notifier.Subscribe(NotificationTopic.Insert, p => inserts.Writer.TryWrite(Queue(p)), () => inserts.Writer.TryWrite("resync"));
        using var b = notifier.Subscribe(NotificationTopic.Leadership, _ => { }, () => leadership.Writer.TryWrite("resync"));
        await notifier.StartAsync(Ct);
        Assert.Equal("resync", await NextAsync(inserts));
        Assert.Equal("resync", await NextAsync(leadership));
        var before = await ListenerPidAsync(ds, schema);

        await using (var cmd = ds.CreateCommand($"SELECT pg_terminate_backend({before})"))
        {
            await cmd.ExecuteNonQueryAsync(Ct);
        }

        Assert.Equal("resync", await NextAsync(inserts));
        Assert.Equal("resync", await NextAsync(leadership));
        var after = await ListenerPidAsync(ds, schema);
        Assert.NotNull(after);
        Assert.NotEqual(before, after);
        await client.InsertAsync(new EmailArgs("c@example.com"), new InsertOptions { Queue = "after" }, Ct);
        Assert.Equal("after", await NextAsync(inserts));
    }

    [Fact]
    public async Task A_silently_dead_connection_is_found_by_the_keep_alive_and_replaced()
    {
        db.SkipIfUnavailable();
        await using var ds = db.CreateDataSource();
        var (client, _, schema) = await ClientAsync(ds);
        var target = new NpgsqlConnectionStringBuilder(db.ConnectionString);
        await using var proxy = new BlackholeProxy(target.Host!, target.Port);
        var throughProxy = new NpgsqlConnectionStringBuilder(db.ConnectionString) { Host = "127.0.0.1", Port = proxy.Port }.ConnectionString;
        var driver = (IKopiDriver)new NpgsqlDriver(ds, new NpgsqlDriverOptions
        {
            Schema = schema,
            NotifierConnectionString = throughProxy,
            NotifierKeepAlive = TimeSpan.FromSeconds(1),
        });
        await using var notifier = new Notifier(driver.CreateListener()!);
        var inserts = Events();
        using var subscription = notifier.Subscribe(NotificationTopic.Insert, p => inserts.Writer.TryWrite(Queue(p)), () => inserts.Writer.TryWrite("resync"));
        await notifier.StartAsync(Ct);
        Assert.Equal("resync", await NextAsync(inserts));

        proxy.Blackholed = true;
        var stopwatch = Stopwatch.StartNew();
        await Until(() => !notifier.IsConnected, TimeSpan.FromSeconds(15));
        var detected = stopwatch.Elapsed;
        proxy.Blackholed = false;

        Assert.True(detected < TimeSpan.FromSeconds(8), $"Detected after {detected}; Keepalive=1 should take about 2 s.");
        Assert.Equal("resync", await NextAsync(inserts));
        await client.InsertAsync(new EmailArgs("d@example.com"), new InsertOptions { Queue = "revived" }, Ct);
        Assert.Equal("revived", await NextAsync(inserts));
    }

    [Fact]
    public async Task A_schema_too_long_for_its_channels_is_a_configuration_error()
    {
        await using var ds = NpgsqlDataSource.Create("Host=localhost;Username=nobody");
        var longest = new string('s', 46);
        var tooLong = new string('s', 47);

        Assert.Equal(3, NpgsqlListener.Channels(longest).Count);
        var listener = ((IKopiDriver)new NpgsqlDriver(ds, new NpgsqlDriverOptions { Schema = tooLong })).CreateListener()!;
        var ex = await Assert.ThrowsAsync<KopiQueueConfigurationException>(() => listener.ConnectAsync(Ct));
        Assert.Contains("at most 46 bytes", ex.Message, StringComparison.Ordinal);
        Assert.Contains($"\"{tooLong}.river_leadership\" is 64 bytes", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void The_notifier_connection_is_unpooled_unenlisted_and_kept_alive()
    {
        var listener = new NpgsqlListener(NpgsqlDataSource.Create("Host=h"), null, null, TimeSpan.FromMilliseconds(2500));

        var settings = new NpgsqlConnectionStringBuilder(listener.ConnectionString("Host=h;Pooling=true;Keepalive=60;Enlist=true;Maximum Pool Size=3"));

        Assert.Equal((false, 3, false), (settings.Pooling, settings.KeepAlive, settings.Enlist));
        Assert.Equal("\"a.\"\"b\"", NpgsqlListenerConnection.Quote("a.\"b"));
    }

    [Theory]
    [InlineData(0.5)]
    [InlineData(86_401)]
    public void The_keep_alive_must_be_from_1_second_to_1_day(double seconds)
    {
        var ds = NpgsqlDataSource.Create("Host=h");

        Assert.Throws<ArgumentOutOfRangeException>(() => new NpgsqlDriver(ds, new NpgsqlDriverOptions { NotifierKeepAlive = TimeSpan.FromSeconds(seconds) }));
    }

    private static Channel<string> Events() => Channel.CreateUnbounded<string>();

    private static async Task<string> NextAsync(Channel<string> events) =>
        await events.Reader.ReadAsync(Ct).AsTask().WaitAsync(Timeout, Ct);

    private static string Queue(string payload)
    {
        using var json = JsonDocument.Parse(payload);
        return json.RootElement.GetProperty("queue").GetString()!;
    }

    private static async Task NotifyAsync(NpgsqlDataSource ds, string channel, string payload)
    {
        await using var cmd = ds.CreateCommand("SELECT pg_notify($1, $2)");
        cmd.Parameters.Add(new NpgsqlParameter { Value = channel });
        cmd.Parameters.Add(new NpgsqlParameter { Value = payload });
        await cmd.ExecuteNonQueryAsync(Ct);
    }

    /// <summary>The backend of the session that last ran a LISTEN on one of <paramref name="schema"/>'s channels.</summary>
    private static async Task<int?> ListenerPidAsync(NpgsqlDataSource ds, string schema)
    {
        await using var cmd = ds.CreateCommand(
            "SELECT pid FROM pg_stat_activity WHERE pid <> pg_backend_pid() AND state IS NOT NULL AND query LIKE $1");
        cmd.Parameters.Add(new NpgsqlParameter { Value = $"%LISTEN \"{schema}.%" });
        return await cmd.ExecuteScalarAsync(Ct) as int?;
    }

    private static async Task Until(Func<bool> condition, TimeSpan? limit = null)
    {
        var deadline = DateTime.UtcNow + (limit ?? Timeout);
        while (!condition())
        {
            Assert.True(DateTime.UtcNow < deadline, "The condition was not met in time.");
            await Task.Delay(20, Ct);
        }
    }

    private static async Task UntilAsync(Func<Task<bool>> condition)
    {
        var deadline = DateTime.UtcNow + Timeout;
        while (!await condition())
        {
            Assert.True(DateTime.UtcNow < deadline, "The condition was not met in time.");
            await Task.Delay(50, Ct);
        }
    }

    private static async Task<(KopiClient Client, IKopiDriver Driver, string Schema)> ClientAsync(NpgsqlDataSource ds)
    {
        var schema = "n_" + Guid.NewGuid().ToString("N")[..12];
        await using (var cmd = ds.CreateCommand($"CREATE SCHEMA {schema}"))
        {
            await cmd.ExecuteNonQueryAsync(Ct);
        }

        var driver = new NpgsqlDriver(ds, new NpgsqlDriverOptions { Schema = schema });
        await driver.CreateMigrator().MigrateAsync(MigrationDirection.Up, cancellationToken: Ct);
        return (new KopiClient(driver), driver, schema);
    }

    [JobKind("email_send")]
    private sealed record EmailArgs(string To) : IJobArgs;
}
