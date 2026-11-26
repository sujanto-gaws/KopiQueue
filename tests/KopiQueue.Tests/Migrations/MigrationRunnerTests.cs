using KopiQueue.Migrations;

namespace KopiQueue.Tests.Migrations;

public sealed class MigrationRunnerTests
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Fact]
    public async Task Up_executes_rendered_steps_in_order_under_the_lock()
    {
        var driver = new FakeDriver();

        var result = await new MigrationRunner(driver).RunAsync(new(MigrationPlannerTests.Line(3), "main", MigrationDirection.Up), Ct);

        Assert.Equal(
            ["lock", "applied main", "exec up 1: UP p_1", "exec up 2: UP p_2", "exec up 3: UP p_3", "unlock", "dispose"],
            driver.Log);
        Assert.Equal((0, 3, 3), (result.VersionBefore, result.VersionAfter, result.LatestVersion));
        Assert.Equal([1, 2, 3], result.Steps.Select(s => s.Version));
        Assert.Equal([1, 2, 3], driver.Applied);
    }

    [Fact]
    public async Task Down_without_a_target_reverts_one_version()
    {
        var driver = new FakeDriver { Applied = [1, 2, 3] };

        var result = await new MigrationRunner(driver).RunAsync(new(MigrationPlannerTests.Line(3), "main", MigrationDirection.Down), Ct);

        Assert.Contains("exec down 3: DOWN p_3", driver.Log);
        Assert.Equal((3, 2), (result.VersionBefore, result.VersionAfter));
        Assert.Equal([1, 2], driver.Applied);
    }

    [Fact]
    public async Task Down_to_zero_reports_version_zero()
    {
        var driver = new FakeDriver { Applied = [1, 2] };

        var result = await new MigrationRunner(driver).RunAsync(new(MigrationPlannerTests.Line(3), "main", MigrationDirection.Down, 0), Ct);

        Assert.Equal((2, 0), (result.VersionBefore, result.VersionAfter));
        Assert.Empty(driver.Applied);
    }

    [Fact]
    public async Task Nothing_to_do_still_releases_the_lock()
    {
        var driver = new FakeDriver { Applied = [1, 2, 3] };

        var result = await new MigrationRunner(driver).RunAsync(new(MigrationPlannerTests.Line(3), "main", MigrationDirection.Up), Ct);

        Assert.Empty(result.Steps);
        Assert.Equal(["lock", "applied main", "unlock", "dispose"], driver.Log);
    }

    [Fact]
    public async Task A_failing_step_stops_the_run_wraps_the_error_and_releases_the_lock()
    {
        var driver = new FakeDriver { FailOn = 2 };

        var ex = await Assert.ThrowsAsync<KopiQueueMigrationException>(
            () => new MigrationRunner(driver).RunAsync(new(MigrationPlannerTests.Line(3), "main", MigrationDirection.Up), Ct));

        Assert.IsType<FakeDbException>(ex.InnerException);
        Assert.Contains("main 2 (v2) failed while migrating up", ex.Message, StringComparison.Ordinal);
        Assert.Contains("rolled back", ex.Message, StringComparison.Ordinal);
        Assert.Equal([1], driver.Applied);
        Assert.Equal(["unlock", "dispose"], driver.Log[^2..]);
        Assert.DoesNotContain(driver.Log, l => l.StartsWith("exec up 3", StringComparison.Ordinal));
    }

    [Fact]
    public async Task A_failing_step_without_a_transaction_warns_about_a_partial_migration()
    {
        var migrations = new[]
        {
            new Migration("main", 1, "concurrent", new MigrationScript("X", InTransaction: false), new MigrationScript("Y", false)),
        };
        var driver = new FakeDriver { FailOn = 1 };

        var ex = await Assert.ThrowsAsync<KopiQueueMigrationException>(
            () => new MigrationRunner(driver).RunAsync(new(migrations, "main", MigrationDirection.Up), Ct));

        Assert.Contains("partially migrated", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task Waits_for_the_lock_then_runs()
    {
        var driver = new FakeDriver { LockBusyAttempts = 2 };

        var result = await new MigrationRunner(driver).RunAsync(new(MigrationPlannerTests.Line(1), "main", MigrationDirection.Up), Ct);

        Assert.Single(result.Steps);
        Assert.Equal(3, driver.LockAttempts);
    }

    [Fact]
    public async Task Times_out_when_the_lock_stays_busy()
    {
        var driver = new FakeDriver { LockBusyAttempts = int.MaxValue };

        var ex = await Assert.ThrowsAsync<KopiQueueMigrationException>(
            () => new MigrationRunner(driver).RunAsync(
                new(MigrationPlannerTests.Line(1), "main", MigrationDirection.Up, LockTimeout: TimeSpan.FromMilliseconds(500)), Ct));

        Assert.Contains("waiting for the migration lock", ex.Message, StringComparison.Ordinal);
        Assert.DoesNotContain("unlock", driver.Log);
        Assert.Equal("dispose", driver.Log[^1]);
    }

    [Fact]
    public async Task A_dry_run_plans_and_renders_but_executes_nothing()
    {
        var driver = new FakeDriver { Applied = [1] };

        var result = await new MigrationRunner(driver).RunAsync(
            new(MigrationPlannerTests.Line(3), "main", MigrationDirection.Up, DryRun: true), Ct);

        Assert.True(result.DryRun);
        Assert.Equal(["UP p_2", "UP p_3"], result.Steps.Select(s => s.Sql));
        Assert.Equal((1, 3), (result.VersionBefore, result.VersionAfter));
        Assert.DoesNotContain(driver.Log, l => l.StartsWith("exec", StringComparison.Ordinal));
        Assert.Equal([1], driver.Applied);
    }

    [Fact]
    public async Task A_lock_release_failure_does_not_hide_the_step_failure()
    {
        var driver = new FakeDriver { FailOn = 1, FailUnlock = true };

        var ex = await Assert.ThrowsAsync<KopiQueueMigrationException>(
            () => new MigrationRunner(driver).RunAsync(new(MigrationPlannerTests.Line(1), "main", MigrationDirection.Up), Ct));

        Assert.IsType<FakeDbException>(ex.InnerException);
    }
}
