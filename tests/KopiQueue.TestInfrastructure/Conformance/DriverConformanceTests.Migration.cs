using KopiQueue.Migrations;
using Xunit;

namespace KopiQueue.TestInfrastructure.Conformance;

/// <summary>Migration cases (plan tasks 1.1-1.3: FR-MIG-01, -02, -06).</summary>
public abstract partial class DriverConformanceTests<TFixture>
{
    private async Task<KopiMigrator> EmptySchemaMigratorAsync()
    {
        Fixture.SkipIfUnavailable();
        var schema = await Fixture.CreateSchemaAsync(Ct);
        return schema.Driver.CreateMigrator();
    }

    [Fact]
    public async Task A_new_schema_has_every_version_pending()
    {
        var migrator = await EmptySchemaMigratorAsync();

        var status = await migrator.ValidateAsync(Ct);
        var versions = await migrator.GetVersionsAsync(Ct);

        Assert.True(migrator.LatestVersion > 0);
        Assert.Equal((0, migrator.LatestVersion, false, false), (status.CurrentVersion, status.LatestVersion, status.IsCurrent, status.IsDatabaseNewer));
        Assert.Equal(Enumerable.Range(1, migrator.LatestVersion), status.PendingVersions);
        Assert.Equal(Enumerable.Range(1, migrator.LatestVersion), versions.Select(v => v.Version));
        Assert.All(versions, v => Assert.False(v.IsApplied));
        Assert.All(versions, v => Assert.True(v.IsKnown && !string.IsNullOrEmpty(v.Name)));
    }

    [Fact]
    public async Task Migrating_up_applies_every_version_once()
    {
        var migrator = await EmptySchemaMigratorAsync();
        var before = DateTimeOffset.UtcNow.AddMinutes(-5);

        var up = await migrator.MigrateAsync(MigrationDirection.Up, cancellationToken: Ct);
        var again = await migrator.MigrateAsync(MigrationDirection.Up, cancellationToken: Ct);

        Assert.Equal((0, migrator.LatestVersion), (up.VersionBefore, up.VersionAfter));
        Assert.Equal(Enumerable.Range(1, migrator.LatestVersion), up.Steps.Select(s => s.Version));
        Assert.Empty(again.Steps);
        Assert.True((await migrator.ValidateAsync(Ct)).IsCurrent);
        Assert.All(await migrator.GetVersionsAsync(Ct), v => Assert.True(v.IsApplied && v.AppliedAt > before));
    }

    [Fact]
    public async Task Migrating_down_to_zero_and_up_again_round_trips()
    {
        var migrator = await EmptySchemaMigratorAsync();
        await migrator.MigrateAsync(MigrationDirection.Up, cancellationToken: Ct);

        var down = await migrator.MigrateAsync(MigrationDirection.Down, new MigrateOptions { TargetVersion = 0 }, Ct);
        var status = await migrator.ValidateAsync(Ct);
        var up = await migrator.MigrateAsync(MigrationDirection.Up, cancellationToken: Ct);

        Assert.Equal(Enumerable.Range(1, migrator.LatestVersion).Reverse(), down.Steps.Select(s => s.Version));
        Assert.Equal(0, status.CurrentVersion);
        Assert.Equal(migrator.LatestVersion, up.VersionAfter);
        Assert.True((await migrator.ValidateAsync(Ct)).IsCurrent);
    }

    [Fact]
    public async Task Migrating_step_by_step_reaches_the_same_version()
    {
        var migrator = await EmptySchemaMigratorAsync();

        for (var version = 1; version <= migrator.LatestVersion; version++)
        {
            var step = await migrator.MigrateAsync(MigrationDirection.Up, new MigrateOptions { TargetVersion = version }, Ct);
            Assert.Equal(version, step.VersionAfter);
            Assert.Equal(version, (await migrator.ValidateAsync(Ct)).CurrentVersion);
        }

        Assert.True((await migrator.ValidateAsync(Ct)).IsCurrent);
    }

    [Fact]
    public async Task A_dry_run_changes_nothing()
    {
        var migrator = await EmptySchemaMigratorAsync();

        var plan = await migrator.MigrateAsync(MigrationDirection.Up, new MigrateOptions { DryRun = true }, Ct);

        Assert.True(plan.DryRun);
        Assert.Equal(migrator.LatestVersion, plan.Steps.Count);
        Assert.All(plan.Steps, s => Assert.False(string.IsNullOrWhiteSpace(s.Sql)));
        Assert.Equal(0, (await migrator.ValidateAsync(Ct)).CurrentVersion);
    }

    [Fact]
    public async Task Concurrent_migrators_apply_each_version_once()
    {
        Fixture.SkipIfUnavailable();
        var schema = await Fixture.CreateSchemaAsync(Ct);

        var results = await Task.WhenAll(Enumerable.Range(0, 4).Select(_ => schema.Driver.CreateMigrator().MigrateAsync(MigrationDirection.Up, cancellationToken: Ct)));

        var migrator = schema.Driver.CreateMigrator();
        Assert.Equal(Enumerable.Range(1, migrator.LatestVersion), results.SelectMany(r => r.Steps).Select(s => s.Version).Order());
        Assert.All(results, r => Assert.Equal(migrator.LatestVersion, r.VersionAfter));
        Assert.True((await migrator.ValidateAsync(Ct)).IsCurrent);
    }

    [Fact]
    public async Task Inserts_work_on_a_schema_migrated_by_the_driver()
    {
        Fixture.SkipIfUnavailable();
        var schema = await Fixture.CreateSchemaAsync(Ct);
        await schema.Driver.CreateMigrator().MigrateAsync(MigrationDirection.Up, cancellationToken: Ct);
        var kind = UniqueKind();

        await schema.Driver.GetExecutor().JobInsertManyAsync([Job(kind)], Ct);

        Assert.Equal(1, await schema.CountJobsAsync(kind, Ct));
    }
}
