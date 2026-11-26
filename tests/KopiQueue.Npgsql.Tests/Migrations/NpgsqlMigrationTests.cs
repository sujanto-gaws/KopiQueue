using KopiQueue.Migrations;
using KopiQueue.Npgsql.Migrations;
using KopiQueue.TestInfrastructure;
using Npgsql;

namespace KopiQueue.Npgsql.Tests.Migrations;

/// <summary>The migration framework against a real PostgreSQL (plan task 1.1: FR-MIG-01, FR-MIG-06).</summary>
public sealed class NpgsqlMigrationTests(PostgresFixture db) : IClassFixture<PostgresFixture>
{
    private static readonly IReadOnlyList<Migration> TestLine =
        MigrationLoader.FromAssembly(typeof(NpgsqlMigrationTests).Assembly)["main"];

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Fact]
    public async Task Up_applies_every_version_and_records_it()
    {
        db.SkipIfUnavailable();
        await using var ds = db.CreateDataSource();
        var schema = await CreateSchemaAsync(ds);
        var driver = new NpgsqlMigrationDriver(ds, schema);

        var result = await new MigrationRunner(driver).RunAsync(Up(), Ct);

        Assert.Equal((0, 3, 3), (result.VersionBefore, result.VersionAfter, result.LatestVersion));
        Assert.Equal([1L, 2L, 3L], await RecordedAsync(ds, driver));
        Assert.Equal(["river_migration", "river_widget"], await TablesAsync(ds, schema));
        Assert.True(await IndexExistsAsync(ds, schema, "river_widget_name_idx"));
    }

    [Fact]
    public async Task Running_up_twice_is_a_no_op()
    {
        db.SkipIfUnavailable();
        await using var ds = db.CreateDataSource();
        var driver = new NpgsqlMigrationDriver(ds, await CreateSchemaAsync(ds));
        await new MigrationRunner(driver).RunAsync(Up(), Ct);

        var again = await new MigrationRunner(driver).RunAsync(Up(), Ct);

        Assert.Empty(again.Steps);
        Assert.Equal((3, 3), (again.VersionBefore, again.VersionAfter));
    }

    [Fact]
    public async Task Down_to_zero_removes_everything_and_up_works_again()
    {
        db.SkipIfUnavailable();
        await using var ds = db.CreateDataSource();
        var schema = await CreateSchemaAsync(ds);
        var driver = new NpgsqlMigrationDriver(ds, schema);
        var runner = new MigrationRunner(driver);
        await runner.RunAsync(Up(), Ct);

        var down = await runner.RunAsync(new(TestLine, "main", MigrationDirection.Down, TargetVersion: 0), Ct);

        Assert.Equal([3, 2, 1], down.Steps.Select(s => s.Version));
        Assert.Empty(await TablesAsync(ds, schema));
        var downFromNothing = await runner.RunAsync(new(TestLine, "main", MigrationDirection.Down), Ct);
        Assert.Empty(downFromNothing.Steps);

        var upAgain = await runner.RunAsync(Up(), Ct);
        Assert.Equal(3, upAgain.Steps.Count);
    }

    [Fact]
    public async Task Up_and_down_by_target_version()
    {
        db.SkipIfUnavailable();
        await using var ds = db.CreateDataSource();
        var schema = await CreateSchemaAsync(ds);
        var driver = new NpgsqlMigrationDriver(ds, schema);
        var runner = new MigrationRunner(driver);

        await runner.RunAsync(Up(target: 2), Ct);
        Assert.Equal([1L, 2L], await RecordedAsync(ds, driver));
        Assert.False(await IndexExistsAsync(ds, schema, "river_widget_name_idx"));

        await runner.RunAsync(Up(), Ct);
        var down = await runner.RunAsync(new(TestLine, "main", MigrationDirection.Down), Ct);
        Assert.Equal([3], down.Steps.Select(s => s.Version));
        Assert.Equal([1L, 2L], await RecordedAsync(ds, driver));
        Assert.False(await IndexExistsAsync(ds, schema, "river_widget_name_idx"));
    }

    [Fact]
    public async Task A_failing_migration_rolls_back_only_itself()
    {
        db.SkipIfUnavailable();
        await using var ds = db.CreateDataSource();
        var schema = await CreateSchemaAsync(ds);
        var driver = new NpgsqlMigrationDriver(ds, schema);
        Migration[] line =
        [
            TestLine[0],
            new("main", 2, "half done",
                new MigrationScript("CREATE TABLE {{schema}}{{prefix}}half (id int);\nSELECT 1 / 0;", true),
                new MigrationScript("DROP TABLE {{schema}}{{prefix}}half;", true)),
        ];

        var ex = await Assert.ThrowsAsync<KopiQueueMigrationException>(
            () => new MigrationRunner(driver).RunAsync(new(line, "main", MigrationDirection.Up), Ct));

        Assert.Equal(PostgresErrorCodes.DivisionByZero, Assert.IsType<PostgresException>(ex.InnerException).SqlState);
        Assert.Equal([1L], await RecordedAsync(ds, driver));
        Assert.Equal(["river_migration"], await TablesAsync(ds, schema));
        Assert.True(await LockIsFreeAsync(ds, driver));
    }

    [Fact]
    public async Task Concurrent_runners_apply_each_version_exactly_once()
    {
        db.SkipIfUnavailable();
        await using var ds = db.CreateDataSource();
        var driver = new NpgsqlMigrationDriver(ds, await CreateSchemaAsync(ds));

        var results = await Task.WhenAll(Enumerable.Range(0, 8).Select(
            _ => Task.Run(() => new MigrationRunner(driver).RunAsync(Up(), Ct), Ct)));

        Assert.Equal(3, results.Sum(r => r.Steps.Count));
        Assert.All(results, r => Assert.Equal(3, r.VersionAfter));
        Assert.Equal([1L, 2L, 3L], await RecordedAsync(ds, driver));
    }

    [Fact]
    public async Task Waits_for_and_then_times_out_on_a_lock_held_elsewhere()
    {
        db.SkipIfUnavailable();
        await using var ds = db.CreateDataSource();
        var driver = new NpgsqlMigrationDriver(ds, await CreateSchemaAsync(ds));
        await using var holder = await ds.OpenConnectionAsync(Ct);
        await using (var take = new NpgsqlCommand("SELECT pg_advisory_lock($1)", holder) { Parameters = { new() { Value = driver.AdvisoryLockKey } } })
        {
            await take.ExecuteNonQueryAsync(Ct);
        }

        var ex = await Assert.ThrowsAsync<KopiQueueMigrationException>(
            () => new MigrationRunner(driver).RunAsync(Up(lockTimeout: TimeSpan.FromSeconds(1)), Ct));

        Assert.Contains("waiting for the migration lock", ex.Message, StringComparison.Ordinal);
        Assert.Empty(await RecordedAsync(ds, driver));
    }

    [Fact]
    public async Task The_lock_is_released_after_a_run()
    {
        db.SkipIfUnavailable();
        await using var ds = db.CreateDataSource();
        var driver = new NpgsqlMigrationDriver(ds, await CreateSchemaAsync(ds));

        await new MigrationRunner(driver).RunAsync(Up(), Ct);

        Assert.True(await LockIsFreeAsync(ds, driver));
    }

    [Fact]
    public async Task Schemas_and_prefixes_migrate_independently()
    {
        db.SkipIfUnavailable();
        await using var ds = db.CreateDataSource();
        var schemaA = await CreateSchemaAsync(ds);
        var schemaB = await CreateSchemaAsync(ds);

        await new MigrationRunner(new NpgsqlMigrationDriver(ds, schemaA)).RunAsync(Up(target: 1), Ct);
        await new MigrationRunner(new NpgsqlMigrationDriver(ds, schemaB)).RunAsync(Up(), Ct);
        await new MigrationRunner(new NpgsqlMigrationDriver(ds, schemaB, "kopi_")).RunAsync(Up(target: 2), Ct);

        Assert.Equal(["river_migration"], await TablesAsync(ds, schemaA));
        Assert.Equal(["kopi_migration", "kopi_widget", "river_migration", "river_widget"], await TablesAsync(ds, schemaB));
        Assert.NotEqual(NpgsqlMigrationDriver.LockKey(schemaA, "river_"), NpgsqlMigrationDriver.LockKey(schemaB, "river_"));
        Assert.NotEqual(NpgsqlMigrationDriver.LockKey(schemaB, "river_"), NpgsqlMigrationDriver.LockKey(schemaB, "kopi_"));
    }

    [Fact]
    public async Task Without_a_schema_the_search_path_is_used()
    {
        db.SkipIfUnavailable();
        var schema = "sp_" + Guid.NewGuid().ToString("N")[..12];
        await using (var setup = db.CreateDataSource())
        {
            await CreateSchemaAsync(setup, schema);
        }

        var builder = new NpgsqlConnectionStringBuilder(db.ConnectionString) { SearchPath = schema };
        await using var ds = NpgsqlDataSource.Create(builder.ConnectionString);
        var driver = new NpgsqlMigrationDriver(ds);

        await new MigrationRunner(driver).RunAsync(Up(), Ct);

        Assert.Equal(string.Empty, driver.SchemaQualifier);
        Assert.Equal(["river_migration", "river_widget"], await TablesAsync(ds, schema));
    }

    [Fact]
    public async Task A_database_newer_than_the_library_is_left_alone_on_up()
    {
        db.SkipIfUnavailable();
        await using var ds = db.CreateDataSource();
        var driver = new NpgsqlMigrationDriver(ds, await CreateSchemaAsync(ds));
        await new MigrationRunner(driver).RunAsync(Up(), Ct);
        await using (var cmd = ds.CreateCommand($"INSERT INTO {driver.MigrationTable} (line, version) VALUES ('main', 4)"))
        {
            await cmd.ExecuteNonQueryAsync(Ct);
        }

        var result = await new MigrationRunner(driver).RunAsync(Up(), Ct);

        Assert.Empty(result.Steps);
        Assert.Equal((4, 3), (result.VersionBefore, result.LatestVersion));
        await Assert.ThrowsAsync<KopiQueueMigrationException>(
            () => new MigrationRunner(driver).RunAsync(new(TestLine, "main", MigrationDirection.Down), Ct));
    }

    [Fact]
    public async Task Other_lines_are_tracked_separately()
    {
        db.SkipIfUnavailable();
        await using var ds = db.CreateDataSource();
        var schema = await CreateSchemaAsync(ds);
        var driver = new NpgsqlMigrationDriver(ds, schema);
        await new MigrationRunner(driver).RunAsync(Up(target: 1), Ct);
        Migration[] extra =
        [
            new("extra", 1, "extra table",
                new MigrationScript("CREATE TABLE {{schema}}{{prefix}}extra (id int);", true),
                new MigrationScript("DROP TABLE {{schema}}{{prefix}}extra;", true)),
        ];

        var result = await new MigrationRunner(driver).RunAsync(new(extra, "extra", MigrationDirection.Up), Ct);

        Assert.Equal(1, result.VersionAfter);
        Assert.Equal([1L], await RecordedAsync(ds, driver, "extra"));
        Assert.Equal([1L], await RecordedAsync(ds, driver, "main"));
    }

    [Fact]
    public void Rejects_unsafe_identifiers()
    {
        using var ds = NpgsqlDataSource.Create("Host=unused");
        Assert.Throws<ArgumentException>(() => new NpgsqlMigrationDriver(ds, "x\"; DROP SCHEMA public; --"));
        Assert.Throws<ArgumentException>(() => new NpgsqlMigrationDriver(ds, "app", "Bad-Prefix"));
    }

    private static MigrationRequest Up(int? target = null, TimeSpan? lockTimeout = null) =>
        new(TestLine, "main", MigrationDirection.Up, target, lockTimeout);

    private static async Task<string> CreateSchemaAsync(NpgsqlDataSource ds, string? name = null)
    {
        name ??= "m_" + Guid.NewGuid().ToString("N")[..12];
        await using var cmd = ds.CreateCommand($"CREATE SCHEMA \"{name}\"");
        await cmd.ExecuteNonQueryAsync(Ct);
        return name;
    }

    private static async Task<List<long>> RecordedAsync(NpgsqlDataSource ds, NpgsqlMigrationDriver driver, string line = "main")
    {
        await using var cmd = ds.CreateCommand(
            $"SELECT version FROM {driver.MigrationTable} WHERE line = $1 ORDER BY version");
        cmd.Parameters.Add(new() { Value = line });
        try
        {
            await using var reader = await cmd.ExecuteReaderAsync(Ct);
            var versions = new List<long>();
            while (await reader.ReadAsync(Ct))
            {
                versions.Add(reader.GetInt64(0));
            }

            return versions;
        }
        catch (PostgresException e) when (e.SqlState == PostgresErrorCodes.UndefinedTable)
        {
            return [];
        }
    }

    private static async Task<List<string>> TablesAsync(NpgsqlDataSource ds, string schema)
    {
        await using var cmd = ds.CreateCommand("SELECT tablename FROM pg_tables WHERE schemaname = $1 ORDER BY tablename COLLATE \"C\"");
        cmd.Parameters.Add(new() { Value = schema });
        await using var reader = await cmd.ExecuteReaderAsync(Ct);
        var tables = new List<string>();
        while (await reader.ReadAsync(Ct))
        {
            tables.Add(reader.GetString(0));
        }

        return tables;
    }

    private static async Task<bool> IndexExistsAsync(NpgsqlDataSource ds, string schema, string index)
    {
        await using var cmd = ds.CreateCommand("SELECT EXISTS (SELECT 1 FROM pg_indexes WHERE schemaname = $1 AND indexname = $2)");
        cmd.Parameters.Add(new() { Value = schema });
        cmd.Parameters.Add(new() { Value = index });
        return (bool)(await cmd.ExecuteScalarAsync(Ct))!;
    }

    private static async Task<bool> LockIsFreeAsync(NpgsqlDataSource ds, NpgsqlMigrationDriver driver)
    {
        await using var conn = await ds.OpenConnectionAsync(Ct);
        await using var cmd = new NpgsqlCommand("SELECT pg_try_advisory_lock($1)", conn) { Parameters = { new() { Value = driver.AdvisoryLockKey } } };
        var free = (bool)(await cmd.ExecuteScalarAsync(Ct))!;
        if (free)
        {
            cmd.CommandText = "SELECT pg_advisory_unlock($1)";
            await cmd.ExecuteNonQueryAsync(Ct);
        }

        return free;
    }
}
