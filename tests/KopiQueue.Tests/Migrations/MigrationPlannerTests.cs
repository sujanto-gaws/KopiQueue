using KopiQueue.Migrations;

namespace KopiQueue.Tests.Migrations;

public sealed class MigrationPlannerTests
{
    private static readonly IReadOnlyList<Migration> Five = Line(5);

    [Theory]
    [InlineData(new int[0], null, new[] { 1, 2, 3, 4, 5 })]
    [InlineData(new[] { 1, 2 }, null, new[] { 3, 4, 5 })]
    [InlineData(new[] { 1, 2 }, 4, new[] { 3, 4 })]
    [InlineData(new[] { 1, 2, 3, 4, 5 }, null, new int[0])]
    [InlineData(new[] { 1, 2, 3 }, 3, new int[0])]
    [InlineData(new[] { 3, 1, 2 }, null, new[] { 4, 5 })] // order of the recorded rows does not matter
    public void Up_applies_missing_versions_in_ascending_order(int[] applied, int? target, int[] expected)
    {
        Assert.Equal(expected, Versions(MigrationPlanner.Plan(Five, applied, MigrationDirection.Up, target)));
    }

    [Theory]
    [InlineData(new[] { 1, 2, 3 }, null, new[] { 3 })]
    [InlineData(new[] { 1, 2, 3 }, 1, new[] { 3, 2 })]
    [InlineData(new[] { 1, 2, 3 }, 0, new[] { 3, 2, 1 })]
    [InlineData(new[] { 1, 2, 3 }, 3, new int[0])]
    [InlineData(new int[0], null, new int[0])]
    [InlineData(new int[0], 0, new int[0])]
    public void Down_reverts_versions_in_descending_order(int[] applied, int? target, int[] expected)
    {
        Assert.Equal(expected, Versions(MigrationPlanner.Plan(Five, applied, MigrationDirection.Down, target)));
    }

    [Theory]
    [InlineData(0)]
    [InlineData(6)]
    public void Up_target_outside_known_versions_is_rejected(int target)
    {
        Assert.Throws<ArgumentOutOfRangeException>(() => MigrationPlanner.Plan(Five, [], MigrationDirection.Up, target));
    }

    [Fact]
    public void Down_target_below_zero_is_rejected()
    {
        Assert.Throws<ArgumentOutOfRangeException>(() => MigrationPlanner.Plan(Five, [1], MigrationDirection.Down, -1));
    }

    [Fact]
    public void Up_to_a_version_below_the_current_one_is_rejected()
    {
        var ex = Assert.Throws<KopiQueueMigrationException>(() => MigrationPlanner.Plan(Five, [1, 2, 3], MigrationDirection.Up, 2));
        Assert.Contains("Migrate down", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void Down_to_a_version_above_the_current_one_is_rejected()
    {
        var ex = Assert.Throws<KopiQueueMigrationException>(() => MigrationPlanner.Plan(Five, [1, 2], MigrationDirection.Down, 4));
        Assert.Contains("Migrate up", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void A_database_newer_than_the_library_needs_no_up_migration()
    {
        Assert.Empty(MigrationPlanner.Plan(Five, [1, 2, 3, 4, 5, 6, 7], MigrationDirection.Up, null));
    }

    [Fact]
    public void A_database_newer_than_the_library_cannot_be_migrated_down()
    {
        var ex = Assert.Throws<KopiQueueMigrationException>(
            () => MigrationPlanner.Plan(Five, [1, 2, 3, 4, 5, 6, 7], MigrationDirection.Down, null));
        Assert.Contains("only knows versions up to 5", ex.Message, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData(new[] { 1, 3 })]
    [InlineData(new[] { 2, 3 })]
    [InlineData(new[] { 1, 1, 2 })]
    public void Gaps_in_recorded_versions_are_rejected(int[] applied)
    {
        var ex = Assert.Throws<KopiQueueMigrationException>(() => MigrationPlanner.Plan(Five, applied, MigrationDirection.Up, null));
        Assert.Contains("inconsistent", ex.Message, StringComparison.Ordinal);
    }

    internal static IReadOnlyList<Migration> Line(int count, string line = "main") =>
        [.. Enumerable.Range(1, count).Select(v => new Migration(
            line, v, $"v{v}", new MigrationScript($"UP {{{{prefix}}}}{v}", true), new MigrationScript($"DOWN {{{{prefix}}}}{v}", true)))];

    private static int[] Versions(IEnumerable<Migration> plan) => [.. plan.Select(m => m.Version)];
}
