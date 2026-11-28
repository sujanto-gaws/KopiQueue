using KopiQueue.Migrations;
using KopiQueue.TestInfrastructure;
using Microsoft.Data.SqlClient;

namespace KopiQueue.SqlServer.Tests.Migrations;

/// <summary>The public migrator API, created from <see cref="SqlServerDriver"/>, against a real SQL Server (plan task 5.1: FR-MIG-02).</summary>
public sealed class SqlServerMigratorApiTests(SqlServerFixture db) : IClassFixture<SqlServerFixture>
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Fact]
    public async Task Validate_migrate_and_list_versions_end_to_end()
    {
        db.SkipIfUnavailable();
        var schema = await db.CreateSchemaAsync();
        var migrator = new SqlServerDriver(db.ConnectionString, new SqlServerDriverOptions { Schema = schema }).CreateMigrator();

        var before = await migrator.ValidateAsync(Ct);
        Assert.Equal((0, 1, false), (before.CurrentVersion, before.LatestVersion, before.IsCurrent));
        Assert.Equal([1], before.PendingVersions);

        var started = DateTimeOffset.UtcNow.AddMinutes(-1);
        var result = await migrator.MigrateAsync(MigrationDirection.Up, cancellationToken: Ct);
        Assert.Equal((0, 1), (result.VersionBefore, result.VersionAfter));
        Assert.Equal("initial schema", result.Steps[0].Name);
        Assert.Contains($"CREATE TABLE [{schema}].kopi_job (", result.Steps[0].Sql, StringComparison.Ordinal);

        Assert.True((await migrator.ValidateAsync(Ct)).IsCurrent);
        var versions = await migrator.GetVersionsAsync(Ct);
        var version = Assert.Single(versions);
        Assert.Equal((1, "initial schema"), (version.Version, version.Name));
        Assert.InRange(version.AppliedAt!.Value, started, DateTimeOffset.UtcNow.AddMinutes(1));
        Assert.Equal(TimeSpan.Zero, version.AppliedAt.Value.Offset);
    }

    [Fact]
    public async Task Down_and_dry_run()
    {
        db.SkipIfUnavailable();
        var schema = await db.CreateSchemaAsync();
        var migrator = new SqlServerDriver(db.ConnectionString, new SqlServerDriverOptions { Schema = schema }).CreateMigrator();
        await migrator.MigrateAsync(MigrationDirection.Up, cancellationToken: Ct);

        var planned = await migrator.MigrateAsync(MigrationDirection.Down, new MigrateOptions { TargetVersion = 0, DryRun = true }, Ct);
        Assert.Equal([1], planned.Steps.Select(s => s.Version));
        Assert.Contains($"DROP TABLE [{schema}].kopi_job;", planned.Steps[0].Sql, StringComparison.Ordinal);
        Assert.True((await migrator.ValidateAsync(Ct)).IsCurrent);

        var down = await migrator.MigrateAsync(MigrationDirection.Down, cancellationToken: Ct);
        Assert.Equal((1, 0), (down.VersionBefore, down.VersionAfter));
        Assert.Equal(0, await db.ObjectCountAsync(schema));
    }

    [Fact]
    public async Task A_newer_database_is_reported_and_left_alone()
    {
        db.SkipIfUnavailable();
        var schema = await db.CreateSchemaAsync();
        var migrator = new SqlServerDriver(db.ConnectionString, new SqlServerDriverOptions { Schema = schema }).CreateMigrator();
        await migrator.MigrateAsync(MigrationDirection.Up, cancellationToken: Ct);
        await db.ExecuteAsync($"INSERT INTO [{schema}].kopi_migration (line, version) VALUES (N'main', 2)");

        var status = await migrator.ValidateAsync(Ct);
        var versions = await migrator.GetVersionsAsync(Ct);
        var up = await migrator.MigrateAsync(MigrationDirection.Up, cancellationToken: Ct);

        Assert.Equal((2, false, true), (status.CurrentVersion, status.IsCurrent, status.IsDatabaseNewer));
        Assert.Equal((2, false, true), (versions[^1].Version, versions[^1].IsKnown, versions[^1].IsApplied));
        Assert.Empty(up.Steps);
    }

    [Fact]
    public async Task The_default_options_use_dbo_and_the_kopi_prefix()
    {
        db.SkipIfUnavailable();
        var driver = new SqlServerDriver(db.ConnectionString);
        var migrator = driver.CreateMigrator();
        Assert.Equal(("dbo", "kopi_"), (driver.Schema, driver.TablePrefix));

        await migrator.MigrateAsync(MigrationDirection.Up, cancellationToken: Ct);
        try
        {
            Assert.Equal(["kopi_job", "kopi_leader", "kopi_migration", "kopi_queue"], await db.TablesAsync("dbo"));
        }
        finally
        {
            // Other tests of this class work in their own schemas, but leave dbo as the fixture made it.
            await migrator.MigrateAsync(MigrationDirection.Down, new MigrateOptions { TargetVersion = 0 }, CancellationToken.None);
        }

        Assert.Empty(await db.TablesAsync("dbo"));
    }

    [Fact]
    public async Task Connections_are_named_KopiQueue_unless_the_connection_string_names_them()
    {
        db.SkipIfUnavailable();

        Assert.Equal("KopiQueue", await AppNameAsync(db.ConnectionString));
        var named = new SqlConnectionStringBuilder(db.ConnectionString) { ApplicationName = "Billing" }.ConnectionString;
        Assert.Equal("Billing", await AppNameAsync(named));

        static async Task<string> AppNameAsync(string connectionString)
        {
            var driver = new SqlServerDriver(connectionString);
            await using var conn = await driver.Connections.OpenAsync(Ct);
            await using var cmd = new SqlCommand("SELECT APP_NAME()", conn);
            return (string)(await cmd.ExecuteScalarAsync(Ct))!;
        }
    }
}
