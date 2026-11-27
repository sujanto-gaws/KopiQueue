using KopiQueue.Migrations;
using KopiQueue.TestInfrastructure;
using Npgsql;

namespace KopiQueue.Npgsql.Tests;

/// <summary>
/// Unique inserts on a real PostgreSQL against River (plan task 4.2: FR-UNQ-02, FR-UNQ-03, FR-UNQ-06): the client stores
/// the unique key and states River v0.51.2 computed for the same job (spike 0.8's golden vectors), so a job a River
/// producer inserted is a duplicate of the same job inserted by KopiQueue.
/// </summary>
public sealed class NpgsqlUniqueTests(PostgresFixture db) : IClassFixture<PostgresFixture>
{
    /// <summary>The vector "field tags": kind golden_tagged, unique by args, members region and customer.</summary>
    private const string FieldTagsKey = "f0fa6311cccc42fd4aadc7891f279bd48b2180c8d462a2158da9b28256c97434";

    /// <summary>The vector "all dims": kind golden_gen, unique by args, a 1 h period and the queue.</summary>
    private const string AllDimsKey = "a0439f8b3436dd471e8277e3d99ca48585c5a4737d11a73946b802d3c5f2120f";

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Fact]
    public async Task The_client_stores_Rivers_key_for_marked_members()
    {
        db.SkipIfUnavailable();
        await using var ds = db.CreateDataSource();
        var (driver, schema) = await MigratedAsync(ds);
        var client = new KopiClient(driver);

        var result = await client.InsertAsync(
            new GoldenTaggedArgs("eu", new GoldenCustomer("x", 1), "ignored"), new InsertOptions { Unique = new() { ByArgs = true } }, Ct);

        Assert.Equal((FieldTagsKey, "11110101"), await UniqueOfAsync(ds, schema, result.Job.Id));
    }

    [Fact]
    public async Task The_client_stores_Rivers_key_by_args_period_and_queue()
    {
        db.SkipIfUnavailable();
        await using var ds = db.CreateDataSource();
        var (driver, schema) = await MigratedAsync(ds);
        var time = new FixedTime(DateTimeOffset.Parse("2026-11-26T01:06:14.88365Z", System.Globalization.CultureInfo.InvariantCulture));
        var client = new KopiClient(driver, new KopiClientOptions { TimeProvider = time });
        var options = new InsertOptions { Queue = "q_all", Unique = new() { ByArgs = true, ByPeriod = TimeSpan.FromHours(1), ByQueue = true } };

        var result = await client.InsertAsync(new GoldenGenArgs("all dims", new GoldenValue("all")), options, Ct);

        Assert.Equal((AllDimsKey, "11110101"), await UniqueOfAsync(ds, schema, result.Job.Id));
    }

    [Fact]
    public async Task A_job_a_River_producer_inserted_is_a_duplicate_of_the_same_job_from_KopiQueue()
    {
        db.SkipIfUnavailable();
        await using var ds = db.CreateDataSource();
        var (driver, schema) = await MigratedAsync(ds);
        long riverJob;
        await using (var cmd = ds.CreateCommand(
            $"""
            INSERT INTO {schema}.river_job (args, kind, max_attempts, queue, state, unique_key, unique_states)
            VALUES ($2::jsonb, 'golden_tagged', 25, 'default', 'available', $1, B'11110101')
            RETURNING id
            """))
        {
            cmd.Parameters.Add(new NpgsqlParameter { Value = Convert.FromHexString(FieldTagsKey) });
            cmd.Parameters.Add(new NpgsqlParameter { Value = """{"region":"eu","customer":{"b":"x","a":1},"note":"from go"}""" });
            riverJob = (long)(await cmd.ExecuteScalarAsync(Ct))!;
        }

        var client = new KopiClient(driver);
        var result = await client.InsertAsync(
            new GoldenTaggedArgs("eu", new GoldenCustomer("x", 1), "from dotnet"), new InsertOptions { Unique = new() { ByArgs = true } }, Ct);

        Assert.True(result.UniqueSkippedAsDuplicate);
        Assert.Equal(riverJob, result.Job.Id);
        Assert.Contains("from go", result.Job.ArgsJson, StringComparison.Ordinal);
    }

    [Fact]
    public async Task At_most_one_job_per_customer_waits_however_many_clients_insert_at_once()
    {
        // UC-4: concurrent inserts from separate clients, each on its own connection, store one job.
        db.SkipIfUnavailable();
        await using var ds = db.CreateDataSource();
        var (driver, schema) = await MigratedAsync(ds);
        var unique = new InsertOptions { Unique = new() { ByArgs = true } };

        var results = await Task.WhenAll(Enumerable.Range(0, 16).Select(i => Task.Run(
            () => new KopiClient(driver).InsertAsync(new GoldenTaggedArgs("eu", new GoldenCustomer("c", 42), $"request {i}"), unique, Ct),
            Ct)));

        Assert.Single(results, r => !r.UniqueSkippedAsDuplicate);
        Assert.Single(results.Select(r => r.Job.Id).Distinct());
        await using var count = ds.CreateCommand($"SELECT count(*) FROM {schema}.river_job");
        Assert.Equal(1L, await count.ExecuteScalarAsync(Ct));
    }

    [Fact]
    public async Task Uniqueness_from_the_args_type_keeps_one_pending_job_per_customer_without_options_at_the_call_site()
    {
        // UC-4 with args-level defaults (plan task 4.5, FR-DEF-05): the args type declares its queue and uniqueness.
        db.SkipIfUnavailable();
        await using var ds = db.CreateDataSource();
        var (driver, schema) = await MigratedAsync(ds);
        var client = new KopiClient(driver);

        var first = await client.InsertAsync(new SyncCustomerArgs(42, "web"), cancellationToken: Ct);
        var again = await client.InsertAsync(new SyncCustomerArgs(42, "web"), cancellationToken: Ct);
        var other = await client.InsertAsync(new SyncCustomerArgs(43, "web"), cancellationToken: Ct);
        var many = await client.InsertManyAsync([new(new SyncCustomerArgs(42, "web")), new(new SyncCustomerArgs(44, "web"))], cancellationToken: Ct);

        Assert.Equal(("sync", 3), (first.Job.Queue, first.Job.MaxAttempts));
        Assert.True(again.UniqueSkippedAsDuplicate);
        Assert.Equal(first.Job.Id, again.Job.Id);
        Assert.False(other.UniqueSkippedAsDuplicate);
        Assert.Equal([true, false], many.Select(r => r.UniqueSkippedAsDuplicate));
        await using var cmd = ds.CreateCommand($"SELECT count(*) FROM {schema}.river_job WHERE kind = 'sync_customer_defaults'");
        Assert.Equal(3L, await cmd.ExecuteScalarAsync(Ct));
        Assert.Equal("11110101", (await UniqueOfAsync(ds, schema, first.Job.Id)).States);
    }

    private static async Task<(string Key, string States)> UniqueOfAsync(NpgsqlDataSource ds, string schema, long id)
    {
        await using var cmd = ds.CreateCommand($"SELECT encode(unique_key, 'hex'), unique_states::text FROM {schema}.river_job WHERE id = $1");
        cmd.Parameters.Add(new NpgsqlParameter { Value = id });
        await using var reader = await cmd.ExecuteReaderAsync(Ct);
        Assert.True(await reader.ReadAsync(Ct));
        return (reader.GetString(0), reader.GetString(1));
    }

    private static async Task<(NpgsqlDriver Driver, string Schema)> MigratedAsync(NpgsqlDataSource ds)
    {
        var schema = "u_" + Guid.NewGuid().ToString("N")[..12];
        await using (var cmd = ds.CreateCommand($"CREATE SCHEMA {schema}"))
        {
            await cmd.ExecuteNonQueryAsync(Ct);
        }

        var driver = new NpgsqlDriver(ds, new NpgsqlDriverOptions { Schema = schema });
        await driver.CreateMigrator().MigrateAsync(MigrationDirection.Up, cancellationToken: Ct);
        return (driver, schema);
    }

    /// <summary>The golden vectors' <c>golden_tagged</c> args; Go tagged region, customer and omit as unique.</summary>
    [JobKind("sync_customer_defaults")]
    public sealed record SyncCustomerArgs(long CustomerId, string RequestedBy) : IJobArgsWithInsertOptions
    {
        public InsertOptions GetInsertOptions() => new() { Queue = "sync", MaxAttempts = 3, Unique = new() { ByArgs = true } };
    }

    [JobKind("golden_tagged")]
    public sealed record GoldenTaggedArgs([property: UniqueKey] string Region, [property: UniqueKey] GoldenCustomer Customer, string Note) : IJobArgs;

    public sealed record GoldenCustomer(string B, int A);

    /// <summary>The golden vectors' <c>golden_gen</c> args.</summary>
    [JobKind("golden_gen")]
    public sealed record GoldenGenArgs(string V, GoldenValue P) : IJobArgs;

    public sealed record GoldenValue(string V);

    private sealed class FixedTime(DateTimeOffset now) : TimeProvider
    {
        public override DateTimeOffset GetUtcNow() => now;
    }
}
