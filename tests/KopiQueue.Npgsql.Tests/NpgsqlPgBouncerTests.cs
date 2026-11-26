using System.Collections.Concurrent;
using KopiQueue.Drivers;
using KopiQueue.Migrations;
using KopiQueue.TestInfrastructure;
using Npgsql;

namespace KopiQueue.Npgsql.Tests;

/// <summary>
/// KopiQueue behind PgBouncer (plan task 2.11, NFR-COMP-04, FR-EXE-03): transaction mode in poll-only mode, the notifier
/// bypassing PgBouncer through <see cref="NpgsqlDriverOptions.NotifierConnectionString"/>, and session mode.
/// </summary>
/// <remarks>
/// The PgBouncer connection strings come from <c>KOPI_TEST_PGBOUNCER_TX</c> and <c>KOPI_TEST_PGBOUNCER_SESSION</c>
/// (CI starts both with <c>tests/pgbouncer/start.sh</c>). Without them the tests skip, unless
/// <c>KOPI_REQUIRE_PGBOUNCER=1</c> turns the skip into a failure.
/// </remarks>
public sealed class NpgsqlPgBouncerTests(PostgresFixture db) : IClassFixture<PostgresFixture>
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Fact]
    public async Task Transaction_mode_inserts_every_way_and_works_jobs_in_poll_only_mode()
    {
        db.SkipIfUnavailable();
        var schema = await MigratedDirectlyAsync();
        await using var pooled = NpgsqlDataSource.Create(Through("KOPI_TEST_PGBOUNCER_TX"));
        var driver = new NpgsqlDriver(pooled, new NpgsqlDriverOptions { Schema = schema });
        var worker = new BouncerWorker();
        var client = Client(driver, worker, NotificationMode.PollOnly, TimeSpan.FromMilliseconds(200));
        await client.StartAsync(Ct);
        try
        {
            var ids = new List<long> { (await client.InsertAsync(new BouncerArgs(1), new InsertOptions { Queue = "bouncer" }, Ct)).Job.Id };
            ids.AddRange((await client.InsertManyAsync([new InsertRequest(new BouncerArgs(2), new InsertOptions { Queue = "bouncer" })], cancellationToken: Ct)).Select(r => r.Job.Id));
            Assert.Equal(1, await client.InsertManyFastAsync([new InsertRequest(new BouncerArgs(3), new InsertOptions { Queue = "bouncer" })], cancellationToken: Ct));
            await using (var connection = await pooled.OpenConnectionAsync(Ct))
            await using (var transaction = await connection.BeginTransactionAsync(Ct))
            {
                ids.Add((await client.InsertAsync(new BouncerArgs(4), transaction, new InsertOptions { Queue = "bouncer" }, Ct)).Job.Id);
                await transaction.CommitAsync(Ct);
            }

            await WaitAsync(() => worker.Ran.Count >= 4, "every job ran");
        }
        finally
        {
            await client.StopAsync(Ct);
        }

        Assert.Equal([1, 2, 3, 4], worker.Ran.Order());
        Assert.Equal(4L, await CountAsync(schema, "state = 'completed'"));
    }

    [Fact]
    public async Task Inserts_through_transaction_mode_still_notify_on_commit()
    {
        db.SkipIfUnavailable();
        var schema = await MigratedDirectlyAsync();
        await using var direct = db.CreateDataSource();
        await using var listen = await direct.OpenConnectionAsync(Ct);
        var heard = new ConcurrentQueue<string>();
        listen.Notification += (_, e) => heard.Enqueue(e.Payload);
        await using (var cmd = new NpgsqlCommand($"LISTEN \"{schema}.river_insert\"", listen))
        {
            await cmd.ExecuteNonQueryAsync(Ct);
        }

        await using var pooled = NpgsqlDataSource.Create(Through("KOPI_TEST_PGBOUNCER_TX"));
        await new KopiClient(new NpgsqlDriver(pooled, new NpgsqlDriverOptions { Schema = schema }))
            .InsertAsync(new BouncerArgs(1), new InsertOptions { Queue = "bouncer" }, Ct);

        using var wait = CancellationTokenSource.CreateLinkedTokenSource(Ct);
        wait.CancelAfter(TimeSpan.FromSeconds(5));
        await listen.WaitAsync(wait.Token);
        Assert.Contains("bouncer", Assert.Single(heard), StringComparison.Ordinal);
    }

    [Fact]
    public async Task Transaction_mode_with_the_notifier_bypassing_PgBouncer_picks_jobs_up_through_listen()
    {
        db.SkipIfUnavailable();
        var schema = await MigratedDirectlyAsync();
        await using var pooled = NpgsqlDataSource.Create(Through("KOPI_TEST_PGBOUNCER_TX"));
        var driver = new NpgsqlDriver(pooled, new NpgsqlDriverOptions { Schema = schema, NotifierConnectionString = db.ConnectionString });
        var worker = new BouncerWorker();
        var client = Client(driver, worker, NotificationMode.ListenNotify, TimeSpan.FromMinutes(5)); // only LISTEN can be in time
        await client.StartAsync(Ct);
        try
        {
            await Task.Delay(300, Ct);
            await new KopiClient(new NpgsqlDriver(pooled, new NpgsqlDriverOptions { Schema = schema }))
                .InsertAsync(new BouncerArgs(1), new InsertOptions { Queue = "bouncer" }, Ct);

            await WaitAsync(() => !worker.Ran.IsEmpty, "the job ran", TimeSpan.FromSeconds(10));
        }
        finally
        {
            await client.StopAsync(Ct);
        }
    }

    [Fact]
    public async Task Session_mode_migrates_and_listens_like_a_direct_connection()
    {
        db.SkipIfUnavailable();
        await using var session = NpgsqlDataSource.Create(Through("KOPI_TEST_PGBOUNCER_SESSION"));
        var schema = await CreateSchemaAsync();
        var driver = new NpgsqlDriver(session, new NpgsqlDriverOptions { Schema = schema });
        await driver.CreateMigrator().MigrateAsync(MigrationDirection.Up, cancellationToken: Ct); // the advisory lock holds in session mode
        var worker = new BouncerWorker();
        var client = Client(driver, worker, NotificationMode.ListenNotify, TimeSpan.FromMinutes(5));
        await client.StartAsync(Ct);
        try
        {
            await Task.Delay(300, Ct);
            await new KopiClient(new NpgsqlDriver(session, new NpgsqlDriverOptions { Schema = schema }))
                .InsertAsync(new BouncerArgs(1), new InsertOptions { Queue = "bouncer" }, Ct);

            await WaitAsync(() => !worker.Ran.IsEmpty, "the job ran", TimeSpan.FromSeconds(10));
        }
        finally
        {
            await client.StopAsync(Ct);
        }
    }

    /// <summary>
    /// The PgBouncer connection string in <paramref name="variable"/>, aimed at this fixture's database (PgBouncer's
    /// <c>*</c> entry forwards any database name), or skips the test.
    /// </summary>
    private string Through(string variable)
    {
        var value = Environment.GetEnvironmentVariable(variable);
        if (string.IsNullOrEmpty(value))
        {
            var message = $"PgBouncer not configured: set {variable} (CI starts it with tests/pgbouncer/start.sh).";
            if (Environment.GetEnvironmentVariable("KOPI_REQUIRE_PGBOUNCER") == "1")
            {
                Assert.Fail(message);
            }

            Assert.Skip(message);
        }

        // Npgsql's advice for PgBouncer: don't send DISCARD ALL when a connection goes back to the pool.
        return new NpgsqlConnectionStringBuilder(value)
        {
            Database = new NpgsqlConnectionStringBuilder(db.ConnectionString).Database,
            NoResetOnClose = true,
        }.ConnectionString;
    }

    private static KopiClient Client(IKopiDriver driver, BouncerWorker worker, NotificationMode mode, TimeSpan poll) => new(driver, new KopiClientOptions
    {
        Queues = new Dictionary<string, QueueOptions> { ["bouncer"] = new() { MaxWorkers = 4 } },
        Workers = new WorkerRegistry().Add(worker),
        NotificationMode = mode,
        FetchPollInterval = poll,
        FetchCooldown = TimeSpan.FromMilliseconds(5),
    });

    /// <summary>Migrations take a session-level advisory lock, so they run directly, not through transaction pooling.</summary>
    private async Task<string> MigratedDirectlyAsync()
    {
        var schema = await CreateSchemaAsync();
        await using var direct = db.CreateDataSource();
        await new NpgsqlDriver(direct, new NpgsqlDriverOptions { Schema = schema }).CreateMigrator().MigrateAsync(MigrationDirection.Up, cancellationToken: Ct);
        return schema;
    }

    private async Task<string> CreateSchemaAsync()
    {
        var schema = "x_" + Guid.NewGuid().ToString("N")[..12];
        await using var direct = db.CreateDataSource();
        await using var cmd = direct.CreateCommand($"CREATE SCHEMA {schema}");
        await cmd.ExecuteNonQueryAsync(Ct);
        return schema;
    }

    private async Task<long> CountAsync(string schema, string where)
    {
        await using var direct = db.CreateDataSource();
        await using var cmd = direct.CreateCommand($"SELECT count(*) FROM {schema}.river_job WHERE {where}");
        return (long)(await cmd.ExecuteScalarAsync(Ct))!;
    }

    private static async Task WaitAsync(Func<bool> condition, string what, TimeSpan? limit = null)
    {
        var deadline = DateTime.UtcNow + (limit ?? TimeSpan.FromSeconds(20));
        while (!condition())
        {
            Assert.True(DateTime.UtcNow < deadline, $"Not met in time: {what}.");
            await Task.Delay(20, Ct);
        }
    }

    [JobKind("bouncer")]
    public sealed record BouncerArgs(int N) : IJobArgs;

    private sealed class BouncerWorker : Worker<BouncerArgs>
    {
        public ConcurrentBag<int> Ran { get; } = [];

        public override Task WorkAsync(Job<BouncerArgs> job, CancellationToken cancellationToken)
        {
            Ran.Add(job.Args.N);
            return Task.CompletedTask;
        }
    }
}
