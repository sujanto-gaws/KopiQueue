using KopiQueue.Migrations;
using KopiQueue.TestInfrastructure;
using Npgsql;

namespace KopiQueue.Npgsql.Tests.Migrations;

/// <summary>The public migrator API against a real PostgreSQL (plan task 1.3: FR-MIG-02).</summary>
public sealed class NpgsqlKopiMigratorTests(PostgresFixture db) : IClassFixture<PostgresFixture>
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Fact]
    public async Task Validate_migrate_and_list_versions_end_to_end()
    {
        db.SkipIfUnavailable();
        await using var ds = db.CreateDataSource();
        var migrator = NpgsqlKopiMigrator.Create(ds, new NpgsqlMigratorOptions { Schema = await CreateSchemaAsync(ds) });

        var before = await migrator.ValidateAsync(Ct);
        Assert.Equal((0, 8, false), (before.CurrentVersion, before.LatestVersion, before.IsCurrent));
        Assert.Equal(Enumerable.Range(1, 8), before.PendingVersions);

        var started = DateTimeOffset.UtcNow.AddMinutes(-1);
        var result = await migrator.MigrateAsync(MigrationDirection.Up, cancellationToken: Ct);
        Assert.Equal((0, 8), (result.VersionBefore, result.VersionAfter));
        Assert.Equal("initial schema", result.Steps[1].Name);
        Assert.Contains("CREATE TABLE \"", result.Steps[1].Sql, StringComparison.Ordinal);

        var after = await migrator.ValidateAsync(Ct);
        Assert.True(after.IsCurrent);
        Assert.Empty(after.PendingVersions);

        var versions = await migrator.GetVersionsAsync(Ct);
        Assert.Equal(Enumerable.Range(1, 8), versions.Select(v => v.Version));
        Assert.Equal(
            ["create river migration", "initial schema", "river job tags non null", "pending and more",
             "migration unique client", "bulk unique", "notification outbox sqlite jsonb and sql cleanup", "job id autoincrement"],
            versions.Select(v => v.Name));
        Assert.All(versions, v => Assert.True(v.AppliedAt > started, $"version {v.Version} applied at {v.AppliedAt}"));
    }

    [Fact]
    public async Task Applied_times_survive_the_change_of_migration_table_in_version_5()
    {
        db.SkipIfUnavailable();
        await using var ds = db.CreateDataSource();
        var schema = await CreateSchemaAsync(ds);
        var migrator = NpgsqlKopiMigrator.Create(ds, new NpgsqlMigratorOptions { Schema = schema });

        await migrator.MigrateAsync(MigrationDirection.Up, new MigrateOptions { TargetVersion = 2 }, Ct);
        var original = await migrator.GetVersionsAsync(Ct);
        await migrator.MigrateAsync(MigrationDirection.Up, cancellationToken: Ct);
        var current = await migrator.GetVersionsAsync(Ct);

        Assert.Equal([true, true, false, false, false, false, false, false], original.Select(v => v.IsApplied));
        Assert.Equal(original[0].AppliedAt, current[0].AppliedAt);
        Assert.Equal(original[1].AppliedAt, current[1].AppliedAt);
    }

    [Fact]
    public async Task Down_and_dry_run()
    {
        db.SkipIfUnavailable();
        await using var ds = db.CreateDataSource();
        var migrator = NpgsqlKopiMigrator.Create(ds, new NpgsqlMigratorOptions { Schema = await CreateSchemaAsync(ds) });
        await migrator.MigrateAsync(MigrationDirection.Up, cancellationToken: Ct);

        var planned = await migrator.MigrateAsync(MigrationDirection.Down, new MigrateOptions { TargetVersion = 0, DryRun = true }, Ct);
        Assert.Equal(Enumerable.Range(1, 8).Reverse(), planned.Steps.Select(s => s.Version));
        Assert.True((await migrator.ValidateAsync(Ct)).IsCurrent);

        var oneDown = await migrator.MigrateAsync(MigrationDirection.Down, cancellationToken: Ct);
        Assert.Equal((8, 7), (oneDown.VersionBefore, oneDown.VersionAfter));
        Assert.Equal([8], (await migrator.ValidateAsync(Ct)).PendingVersions);
    }

    [Fact]
    public async Task A_newer_database_is_reported_and_left_alone()
    {
        db.SkipIfUnavailable();
        await using var ds = db.CreateDataSource();
        var schema = await CreateSchemaAsync(ds);
        var migrator = NpgsqlKopiMigrator.Create(ds, new NpgsqlMigratorOptions { Schema = schema });
        await migrator.MigrateAsync(MigrationDirection.Up, cancellationToken: Ct);
        await using (var cmd = ds.CreateCommand($"INSERT INTO {schema}.river_migration (line, version) VALUES ('main', 9)"))
        {
            await cmd.ExecuteNonQueryAsync(Ct);
        }

        var status = await migrator.ValidateAsync(Ct);
        var versions = await migrator.GetVersionsAsync(Ct);
        var up = await migrator.MigrateAsync(MigrationDirection.Up, cancellationToken: Ct);

        Assert.Equal((9, false, true), (status.CurrentVersion, status.IsCurrent, status.IsDatabaseNewer));
        Assert.Equal((9, false, true), (versions[^1].Version, versions[^1].IsKnown, versions[^1].IsApplied));
        Assert.Empty(up.Steps);
    }

    [Fact]
    public async Task The_default_options_use_the_search_path_and_the_river_prefix()
    {
        db.SkipIfUnavailable();
        string schema;
        await using (var setup = db.CreateDataSource())
        {
            schema = await CreateSchemaAsync(setup);
        }

        await using var ds = NpgsqlDataSource.Create(new NpgsqlConnectionStringBuilder(db.ConnectionString) { SearchPath = schema }.ConnectionString);
        var migrator = NpgsqlKopiMigrator.Create(ds);

        await migrator.MigrateAsync(MigrationDirection.Up, cancellationToken: Ct);

        await using var cmd = ds.CreateCommand($"SELECT count(*) FROM pg_tables WHERE schemaname = '{schema}' AND tablename LIKE 'river\\_%'");
        Assert.Equal(5L, await cmd.ExecuteScalarAsync(Ct));
    }

    [Fact]
    public void Create_rejects_unsafe_options()
    {
        using var ds = NpgsqlDataSource.Create("Host=unused");

        Assert.Throws<ArgumentException>(() => NpgsqlKopiMigrator.Create(ds, new NpgsqlMigratorOptions { Schema = "a;b" }));
        Assert.Throws<ArgumentException>(() => NpgsqlKopiMigrator.Create(ds, new NpgsqlMigratorOptions { TablePrefix = "Bad" }));
        Assert.Throws<ArgumentNullException>(() => NpgsqlKopiMigrator.Create(null!));
    }

    private static async Task<string> CreateSchemaAsync(NpgsqlDataSource ds)
    {
        var name = "k_" + Guid.NewGuid().ToString("N")[..12];
        await using var cmd = ds.CreateCommand($"CREATE SCHEMA {name}");
        await cmd.ExecuteNonQueryAsync(Ct);
        return name;
    }
}
