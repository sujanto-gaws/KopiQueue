using KopiQueue.Migrations;

namespace KopiQueue.Tests.Migrations;

public sealed class KopiMigratorTests
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Fact]
    public async Task MigrateAsync_up_by_default_applies_everything()
    {
        var driver = new FakeDriver();
        var migrator = Migrator(driver, 3);

        var result = await migrator.MigrateAsync(MigrationDirection.Up, cancellationToken: Ct);

        Assert.Equal(("main", MigrationDirection.Up, 0, 3, 3, false), (result.Line, result.Direction, result.VersionBefore, result.VersionAfter, result.LatestVersion, result.DryRun));
        Assert.Equal(
            [(1, "v1", MigrationDirection.Up, "UP p_1"), (2, "v2", MigrationDirection.Up, "UP p_2"), (3, "v3", MigrationDirection.Up, "UP p_3")],
            result.Steps.Select(s => (s.Version, s.Name, s.Direction, s.Sql)));
        Assert.All(result.Steps, s => Assert.True(s.Duration >= TimeSpan.Zero));
    }

    [Fact]
    public async Task MigrateAsync_passes_target_and_dry_run()
    {
        var driver = new FakeDriver { Applied = [1, 2, 3] };
        var migrator = Migrator(driver, 3);

        var result = await migrator.MigrateAsync(MigrationDirection.Down, new MigrateOptions { TargetVersion = 1, DryRun = true }, Ct);

        Assert.True(result.DryRun);
        Assert.Equal([3, 2], result.Steps.Select(s => s.Version));
        Assert.All(result.Steps, s => Assert.Equal(TimeSpan.Zero, s.Duration));
        Assert.Equal([1, 2, 3], driver.Applied);
    }

    [Fact]
    public async Task MigrateAsync_uses_the_lock_timeout()
    {
        var driver = new FakeDriver { LockBusyAttempts = int.MaxValue };
        var migrator = Migrator(driver, 1);

        var ex = await Assert.ThrowsAsync<KopiQueueMigrationException>(() =>
            migrator.MigrateAsync(MigrationDirection.Up, new MigrateOptions { LockTimeout = TimeSpan.FromMilliseconds(300) }, Ct));

        Assert.Contains("0.3 s", ex.Message, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData(0)]
    [InlineData(-1)]
    public async Task MigrateAsync_rejects_a_non_positive_lock_timeout(int milliseconds)
    {
        var migrator = Migrator(new FakeDriver(), 1);

        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(() =>
            migrator.MigrateAsync(MigrationDirection.Up, new MigrateOptions { LockTimeout = TimeSpan.FromMilliseconds(milliseconds) }, Ct));
    }

    [Fact]
    public async Task MigrateAsync_rejects_an_undefined_direction()
    {
        var migrator = Migrator(new FakeDriver(), 1);

        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(() => migrator.MigrateAsync((MigrationDirection)7, cancellationToken: Ct));
    }

    [Fact]
    public async Task GetVersionsAsync_lists_known_versions_with_their_applied_time()
    {
        var driver = new FakeDriver { Applied = [1, 2] };

        var versions = await Migrator(driver, 3).GetVersionsAsync(Ct);

        Assert.Equal(
            [(1, "v1", true, true, FakeDriver.AppliedAt(1)), (2, "v2", true, true, FakeDriver.AppliedAt(2)), (3, "v3", false, true, (DateTimeOffset?)null)],
            versions.Select(v => (v.Version, v.Name, v.IsApplied, v.IsKnown, v.AppliedAt)));
        Assert.DoesNotContain("lock", driver.Log);
        Assert.Equal("dispose", driver.Log[^1]);
    }

    [Fact]
    public async Task GetVersionsAsync_includes_applied_versions_this_library_does_not_know()
    {
        var driver = new FakeDriver { Applied = [1, 2, 3, 4] };

        var versions = await Migrator(driver, 2).GetVersionsAsync(Ct);

        Assert.Equal([1, 2, 3, 4], versions.Select(v => v.Version));
        Assert.Equal([true, true, false, false], versions.Select(v => v.IsKnown));
        Assert.Null(versions[3].Name);
        Assert.All(versions, v => Assert.True(v.IsApplied));
    }

    [Theory]
    [InlineData(new int[0], 0, false, false, new[] { 1, 2, 3 }, "is at version 0; versions 1 to 3 are pending.")]
    [InlineData(new[] { 1 }, 1, false, false, new[] { 2, 3 }, "is at version 1; versions 2 to 3 are pending.")]
    [InlineData(new[] { 1, 2, 3 }, 3, true, false, new int[0], "is current at version 3.")]
    [InlineData(new[] { 1, 2, 3, 4 }, 4, false, true, new int[0], "is at version 4, newer than the latest version 3 this library knows.")]
    public async Task ValidateAsync_reports_the_current_and_pending_versions(
        int[] applied, int current, bool isCurrent, bool isNewer, int[] pending, string message)
    {
        var driver = new FakeDriver { Applied = [.. applied] };

        var status = await Migrator(driver, 3).ValidateAsync(Ct);

        Assert.Equal(("main", current, 3, isCurrent, isNewer), (status.Line, status.CurrentVersion, status.LatestVersion, status.IsCurrent, status.IsDatabaseNewer));
        Assert.Equal(pending, status.PendingVersions);
        Assert.Equal("Migration line 'main' " + message, status.Message);
        Assert.DoesNotContain("lock", driver.Log);
    }

    [Fact]
    public async Task ValidateAsync_rejects_gaps()
    {
        var migrator = Migrator(new FakeDriver { Applied = [1, 3] }, 3);

        await Assert.ThrowsAsync<KopiQueueMigrationException>(() => migrator.ValidateAsync(Ct));
    }

    [Fact]
    public void LatestVersion_is_the_number_of_known_migrations()
    {
        Assert.Equal(5, Migrator(new FakeDriver(), 5).LatestVersion);
    }

    private static KopiMigrator Migrator(FakeDriver driver, int versions) =>
        new(driver, "main", MigrationPlannerTests.Line(versions));
}
