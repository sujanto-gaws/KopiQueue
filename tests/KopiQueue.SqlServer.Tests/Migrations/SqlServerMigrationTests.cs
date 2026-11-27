using KopiQueue.Migrations;
using KopiQueue.SqlServer.Migrations;
using KopiQueue.TestInfrastructure;
using Microsoft.Data.SqlClient;

namespace KopiQueue.SqlServer.Tests.Migrations;

/// <summary>The migration framework against a real SQL Server, on a test line (plan task 5.1: FR-MIG-01, FR-MIG-05, FR-MIG-06).</summary>
public sealed class SqlServerMigrationTests(SqlServerFixture db) : IClassFixture<SqlServerFixture>
{
    private static readonly IReadOnlyList<Migration> TestLine =
        MigrationLoader.FromAssembly(typeof(SqlServerMigrationTests).Assembly)["main"];

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Fact]
    public async Task Up_applies_every_version_and_records_it()
    {
        db.SkipIfUnavailable();
        var schema = await db.CreateSchemaAsync();
        var driver = db.MigrationDriver(schema);

        var result = await new MigrationRunner(driver).RunAsync(Up(), Ct);

        Assert.Equal((0, 3, 3), (result.VersionBefore, result.VersionAfter, result.LatestVersion));
        Assert.Equal([1L, 2L, 3L], await db.RecordedAsync(driver));
        Assert.Equal(["kopi_migration", "kopi_widget", "kopi_widget_log"], await db.TablesAsync(schema));
        Assert.Equal(0L, await db.ScalarAsync($"SELECT n FROM [{schema}].kopi_widget_count"));
    }

    [Fact]
    public async Task Steps_run_in_a_transaction_unless_the_script_opts_out()
    {
        db.SkipIfUnavailable();
        var schema = await db.CreateSchemaAsync();

        await new MigrationRunner(db.MigrationDriver(schema)).RunAsync(Up(), Ct);

        Assert.Equal(
            ["2:1", "3:0"],
            await db.StringsAsync($"SELECT CONCAT(step, ':', trancount) FROM [{schema}].kopi_widget_log ORDER BY step"));
    }

    [Fact]
    public async Task Running_up_twice_is_a_no_op()
    {
        db.SkipIfUnavailable();
        var driver = db.MigrationDriver(await db.CreateSchemaAsync());
        await new MigrationRunner(driver).RunAsync(Up(), Ct);

        var again = await new MigrationRunner(driver).RunAsync(Up(), Ct);

        Assert.Empty(again.Steps);
        Assert.Equal((3, 3), (again.VersionBefore, again.VersionAfter));
    }

    [Fact]
    public async Task Down_to_zero_removes_everything_and_up_works_again()
    {
        db.SkipIfUnavailable();
        var schema = await db.CreateSchemaAsync();
        var runner = new MigrationRunner(db.MigrationDriver(schema));
        await runner.RunAsync(Up(), Ct);

        var down = await runner.RunAsync(new(TestLine, "main", MigrationDirection.Down, TargetVersion: 0), Ct);

        Assert.Equal([3, 2, 1], down.Steps.Select(s => s.Version));
        Assert.Equal(0, await db.ObjectCountAsync(schema));
        Assert.Empty((await runner.RunAsync(new(TestLine, "main", MigrationDirection.Down), Ct)).Steps);
        Assert.Equal(3, (await runner.RunAsync(Up(), Ct)).Steps.Count);
    }

    [Fact]
    public async Task Up_and_down_by_target_version()
    {
        db.SkipIfUnavailable();
        var schema = await db.CreateSchemaAsync();
        var driver = db.MigrationDriver(schema);
        var runner = new MigrationRunner(driver);

        await runner.RunAsync(Up(target: 2), Ct);
        Assert.Equal([1L, 2L], await db.RecordedAsync(driver));
        Assert.False(await IndexExistsAsync(schema));

        await runner.RunAsync(Up(), Ct);
        Assert.True(await IndexExistsAsync(schema));
        var down = await runner.RunAsync(new(TestLine, "main", MigrationDirection.Down), Ct);
        Assert.Equal([3], down.Steps.Select(s => s.Version));
        Assert.Equal([1L, 2L], await db.RecordedAsync(driver));
        Assert.False(await IndexExistsAsync(schema));
    }

    [Fact]
    public async Task A_failing_migration_rolls_back_only_itself()
    {
        db.SkipIfUnavailable();
        var schema = await db.CreateSchemaAsync();
        var driver = db.MigrationDriver(schema);
        Migration[] line =
        [
            TestLine[0],
            new("main", 2, "half done",
                new MigrationScript("CREATE TABLE {{schema}}{{prefix}}half (id int);\nGO\nSELECT 1 / 0;", true),
                new MigrationScript("DROP TABLE {{schema}}{{prefix}}half;", true)),
        ];

        var ex = await Assert.ThrowsAsync<KopiQueueMigrationException>(
            () => new MigrationRunner(driver).RunAsync(new(line, "main", MigrationDirection.Up), Ct));

        Assert.Equal(8134, Assert.IsType<SqlException>(ex.InnerException).Number); // divide by zero
        Assert.Equal([1L], await db.RecordedAsync(driver));
        Assert.Equal(["kopi_migration"], await db.TablesAsync(schema));
        Assert.True(await db.LockIsFreeAsync(driver));
    }

    [Fact]
    public async Task Concurrent_runners_apply_each_version_exactly_once()
    {
        db.SkipIfUnavailable();
        var driver = db.MigrationDriver(await db.CreateSchemaAsync());

        var results = await Task.WhenAll(Enumerable.Range(0, 8).Select(
            _ => Task.Run(() => new MigrationRunner(driver).RunAsync(Up(), Ct), Ct)));

        Assert.Equal(3, results.Sum(r => r.Steps.Count));
        Assert.All(results, r => Assert.Equal(3, r.VersionAfter));
        Assert.Equal([1L, 2L, 3L], await db.RecordedAsync(driver));
    }

    [Fact]
    public async Task Waits_for_and_then_times_out_on_a_lock_held_elsewhere()
    {
        db.SkipIfUnavailable();
        var driver = db.MigrationDriver(await db.CreateSchemaAsync());
        await using var holder = await db.OpenConnectionAsync(Ct);
        await using (var take = new SqlCommand(
            "EXEC sys.sp_getapplock @Resource = @resource, @LockMode = 'Exclusive', @LockOwner = 'Session', @LockTimeout = 0", holder))
        {
            take.Parameters.AddWithValue("@resource", driver.LockResource);
            await take.ExecuteNonQueryAsync(Ct);
        }

        var ex = await Assert.ThrowsAsync<KopiQueueMigrationException>(
            () => new MigrationRunner(driver).RunAsync(Up(lockTimeout: TimeSpan.FromSeconds(1)), Ct));

        Assert.Contains("waiting for the migration lock", ex.Message, StringComparison.Ordinal);
        Assert.Empty(await db.RecordedAsync(driver));
    }

    [Fact]
    public async Task The_lock_is_released_after_a_run_and_held_during_it()
    {
        db.SkipIfUnavailable();
        var driver = db.MigrationDriver(await db.CreateSchemaAsync());

        await using (var session = await driver.OpenSessionAsync(Ct))
        {
            Assert.True(await session.TryAcquireLockAsync(Ct));
            Assert.False(await db.LockIsFreeAsync(driver));
        }

        Assert.True(await db.LockIsFreeAsync(driver));
        await new MigrationRunner(driver).RunAsync(Up(), Ct);
        Assert.True(await db.LockIsFreeAsync(driver));
    }

    [Fact]
    public async Task Schemas_and_prefixes_migrate_independently()
    {
        db.SkipIfUnavailable();
        var schemaA = await db.CreateSchemaAsync();
        var schemaB = await db.CreateSchemaAsync();

        await new MigrationRunner(db.MigrationDriver(schemaA)).RunAsync(Up(target: 1), Ct);
        await new MigrationRunner(db.MigrationDriver(schemaB)).RunAsync(Up(), Ct);
        await new MigrationRunner(db.MigrationDriver(schemaB, "other_")).RunAsync(Up(target: 2), Ct);

        Assert.Equal(["kopi_migration"], await db.TablesAsync(schemaA));
        Assert.Equal(
            ["kopi_migration", "kopi_widget", "kopi_widget_log", "other_migration", "other_widget", "other_widget_log"],
            await db.TablesAsync(schemaB));
        Assert.NotEqual(db.MigrationDriver(schemaA).LockResource, db.MigrationDriver(schemaB).LockResource);
        Assert.NotEqual(db.MigrationDriver(schemaB).LockResource, db.MigrationDriver(schemaB, "other_").LockResource);
    }

    [Fact]
    public async Task A_database_newer_than_the_library_is_left_alone_on_up()
    {
        db.SkipIfUnavailable();
        var driver = db.MigrationDriver(await db.CreateSchemaAsync());
        await new MigrationRunner(driver).RunAsync(Up(), Ct);
        await db.ExecuteAsync($"INSERT INTO {driver.MigrationTable} (line, version) VALUES (N'main', 4)");

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
        var driver = db.MigrationDriver(await db.CreateSchemaAsync());
        await new MigrationRunner(driver).RunAsync(Up(target: 1), Ct);
        Migration[] extra =
        [
            new("extra", 1, "extra table",
                new MigrationScript("CREATE TABLE {{schema}}{{prefix}}extra (id int);", true),
                new MigrationScript("DROP TABLE {{schema}}{{prefix}}extra;", true)),
        ];

        var result = await new MigrationRunner(driver).RunAsync(new(extra, "extra", MigrationDirection.Up), Ct);

        Assert.Equal(1, result.VersionAfter);
        Assert.Equal([1L], await db.RecordedAsync(driver, "extra"));
        Assert.Equal([1L], await db.RecordedAsync(driver, "main"));
    }

    [Fact]
    public async Task A_line_before_the_migration_table_exists_is_refused()
    {
        db.SkipIfUnavailable();
        var schema = await db.CreateSchemaAsync();
        var driver = db.MigrationDriver(schema);
        Migration[] extra =
        [
            new("extra", 1, "extra table",
                new MigrationScript("CREATE TABLE {{schema}}{{prefix}}extra (id int);", true),
                new MigrationScript("DROP TABLE {{schema}}{{prefix}}extra;", true)),
        ];

        var ex = await Assert.ThrowsAsync<KopiQueueMigrationException>(
            () => new MigrationRunner(driver).RunAsync(new(extra, "extra", MigrationDirection.Up), Ct));

        Assert.Contains("does not exist", ex.Message, StringComparison.Ordinal);
        Assert.Empty(await db.TablesAsync(schema));
    }

    private static MigrationRequest Up(int? target = null, TimeSpan? lockTimeout = null) =>
        new(TestLine, "main", MigrationDirection.Up, target, lockTimeout);

    private async Task<bool> IndexExistsAsync(string schema) => (int)(await db.ScalarAsync(
        "SELECT COUNT(*) FROM sys.indexes WHERE name = 'kopi_widget_name_idx' AND object_id = OBJECT_ID(@table)",
        new SqlParameter("@table", $"[{schema}].kopi_widget")))! == 1;
}
