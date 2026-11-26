using KopiQueue.Migrations;
using KopiQueue.TestInfrastructure;
using Npgsql;

namespace KopiQueue.Npgsql.Tests;

/// <summary>
/// Insert-only use, as from a web front-end: no queues, no workers, no start step (plan task 1.8: FR-INS-08).
/// </summary>
/// <remarks>
/// Each test gives its data source a unique <c>Application Name</c>, so the client's sessions can be counted in
/// <c>pg_stat_activity</c>.
/// </remarks>
public sealed class InsertOnlyClientTests(PostgresFixture db) : IClassFixture<PostgresFixture>
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Fact]
    public async Task A_new_client_inserts_without_being_started_and_keeps_no_connection()
    {
        db.SkipIfUnavailable();
        var schema = await MigratedSchemaAsync();
        var application = Application();

        // Pooling off: any connection the client kept open, for listening or anything else, would still be visible.
        await using (var ds = DataSource(application, pooling: false))
        {
            var client = new KopiClient(new NpgsqlDriver(ds, new NpgsqlDriverOptions { Schema = schema }));

            await client.InsertAsync(new PingArgs(1), cancellationToken: Ct);
            await client.InsertManyAsync([new(new PingArgs(2)), new(new PingArgs(3))], cancellationToken: Ct);
            await client.InsertManyFastAsync([new(new PingArgs(4))], cancellationToken: Ct);

            // A closed backend leaves pg_stat_activity a moment after its connection closes.
            Assert.Equal(0L, await EventuallyAsync(() => SessionsAsync(application), 0L));
        }

        Assert.Equal(4L, await JobCountAsync(schema));
    }

    [Fact]
    public async Task One_shared_client_handles_many_parallel_inserts_within_the_pool()
    {
        db.SkipIfUnavailable();
        var schema = await MigratedSchemaAsync();
        var application = Application();
        await using var ds = DataSource(application, pooling: true, maxPoolSize: 5);
        var client = new KopiClient(new NpgsqlDriver(ds, new NpgsqlDriverOptions { Schema = schema }));

        var results = await Task.WhenAll(Enumerable.Range(0, 200).Select(i => client.InsertAsync(new PingArgs(i), cancellationToken: Ct)));

        Assert.Equal(200, results.Select(r => r.Job.Id).Distinct().Count());
        Assert.Equal(Enumerable.Range(0, 200), results.Select(r => r.Args.N).Order());
        Assert.Equal(200L, await JobCountAsync(schema));
        Assert.InRange(await SessionsAsync(application), 1L, 5L);
        Assert.Equal(0L, await SessionsAsync(application, "state <> 'idle' OR query ILIKE 'LISTEN%'"));
    }

    [Fact]
    public async Task Clients_are_cheap_and_independent()
    {
        db.SkipIfUnavailable();
        var schema = await MigratedSchemaAsync();
        await using var ds = DataSource(Application(), pooling: true);
        var driver = new NpgsqlDriver(ds, new NpgsqlDriverOptions { Schema = schema });

        // A client per request, as a front-end without DI might do: nothing to start, stop or dispose.
        for (var i = 0; i < 20; i++)
        {
            await new KopiClient(driver).InsertAsync(new PingArgs(i), cancellationToken: Ct);
        }

        Assert.Equal(20L, await JobCountAsync(schema));
    }

    private NpgsqlDataSource DataSource(string application, bool pooling, int maxPoolSize = 100) =>
        NpgsqlDataSource.Create(new NpgsqlConnectionStringBuilder(db.ConnectionString)
        {
            ApplicationName = application,
            Pooling = pooling,
            MaxPoolSize = maxPoolSize,
        }.ConnectionString);

    private static string Application() => "kq-insert-only-" + Guid.NewGuid().ToString("N")[..12];

    private async Task<long> SessionsAsync(string application, string where = "true")
    {
        await using var ds = db.CreateDataSource();
        await using var cmd = ds.CreateCommand($"SELECT count(*) FROM pg_stat_activity WHERE application_name = $1 AND ({where})");
        cmd.Parameters.Add(new NpgsqlParameter { Value = application });
        return (long)(await cmd.ExecuteScalarAsync(Ct))!;
    }

    private static async Task<long> EventuallyAsync(Func<Task<long>> read, long expected)
    {
        var deadline = DateTime.UtcNow.AddSeconds(5);
        long value;
        while ((value = await read()) != expected && DateTime.UtcNow < deadline)
        {
            await Task.Delay(50, Ct);
        }

        return value;
    }

    private async Task<long> JobCountAsync(string schema)
    {
        await using var ds = db.CreateDataSource();
        await using var cmd = ds.CreateCommand($"SELECT count(*) FROM {schema}.river_job WHERE kind = 'ping'");
        return (long)(await cmd.ExecuteScalarAsync(Ct))!;
    }

    private async Task<string> MigratedSchemaAsync()
    {
        await using var ds = db.CreateDataSource();
        var schema = "o_" + Guid.NewGuid().ToString("N")[..12];
        await using (var cmd = ds.CreateCommand($"CREATE SCHEMA {schema}"))
        {
            await cmd.ExecuteNonQueryAsync(Ct);
        }

        await new NpgsqlDriver(ds, new NpgsqlDriverOptions { Schema = schema }).CreateMigrator().MigrateAsync(MigrationDirection.Up, cancellationToken: Ct);
        return schema;
    }

    [JobKind("ping")]
    private sealed record PingArgs(int N) : IJobArgs;
}
