using KopiQueue.Migrations;

namespace KopiQueue.Tests.Migrations;

public sealed class MigrationLoaderTests
{
    [Fact]
    public void Loads_embedded_resources_with_names_order_and_directives()
    {
        var lines = MigrationLoader.FromAssembly(typeof(MigrationLoaderTests).Assembly);

        var sample = Assert.Single(lines).Value;
        Assert.Collection(
            sample,
            m =>
            {
                Assert.Equal(("sample", 1, "first table"), (m.Line, m.Version, m.Name));
                Assert.True(m.Up.InTransaction);
                Assert.Contains("CREATE TABLE {{schema}}{{prefix}}first", m.Up.Sql, StringComparison.Ordinal);
            },
            m =>
            {
                Assert.Equal((2, "first index"), (m.Version, m.Name));
                Assert.False(m.Up.InTransaction);
                Assert.False(m.Down.InTransaction);
            });
    }

    [Fact]
    public void Groups_lines_and_sorts_versions_numerically()
    {
        var lines = MigrationLoader.Parse(
        [
            ("main/010_ten.up.sql", "u10"), ("main/010_ten.down.sql", "d10"),
            ..Pairs("main", 1, 9),
            ("extra/001_one.up.sql", "u"), ("extra/001_one.down.sql", "d"),
        ]);

        Assert.Equal(["extra", "main"], lines.Keys.Order());
        Assert.Equal(Enumerable.Range(1, 10), lines["main"].Select(m => m.Version));
        Assert.Equal("u10", lines["main"][9].Up.Sql);
    }

    [Theory]
    [InlineData("main/1_short.up.sql")]
    [InlineData("main/001-dash.up.sql")]
    [InlineData("main/001_UPPER.up.sql")]
    [InlineData("main/001_x.sideways.sql")]
    [InlineData("001_no_line.up.sql")]
    [InlineData("Main/001_x.up.sql")]
    public void Rejects_malformed_resource_names(string path)
    {
        var ex = Assert.Throws<InvalidOperationException>(() => MigrationLoader.Parse([(path, "SELECT 1")]));
        Assert.Contains(path, ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void Rejects_a_missing_down_script()
    {
        var ex = Assert.Throws<InvalidOperationException>(() => MigrationLoader.Parse([("main/001_one.up.sql", "u")]));
        Assert.Contains("needs both", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void Rejects_a_gap_in_versions()
    {
        var ex = Assert.Throws<InvalidOperationException>(() => MigrationLoader.Parse([.. Pairs("main", 1, 1), .. Pairs("main", 3, 3)]));
        Assert.Contains("version 2 is missing", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void Rejects_version_zero()
    {
        Assert.Throws<InvalidOperationException>(() => MigrationLoader.Parse([("main/000_zero.up.sql", "u"), ("main/000_zero.down.sql", "d")]));
    }

    [Fact]
    public void Rejects_two_names_for_one_version()
    {
        var ex = Assert.Throws<InvalidOperationException>(
            () => MigrationLoader.Parse([("main/001_one.up.sql", "u"), ("main/001_uno.down.sql", "d")]));
        Assert.Contains("two names", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void Rejects_duplicates()
    {
        Assert.Throws<InvalidOperationException>(
            () => MigrationLoader.Parse([("main/001_one.up.sql", "u"), ("main/001_one.up.sql", "u2"), ("main/001_one.down.sql", "d")]));
    }

    [Theory]
    [InlineData("-- kopi:no-transaction\nCREATE INDEX CONCURRENTLY i ON t (c);", false)]
    [InlineData("\n  -- a comment\n   -- kopi:no-transaction  \nSELECT 1;", false)]
    [InlineData("SELECT 1;\n-- kopi:no-transaction", true)] // only the leading comment block counts
    [InlineData("-- kopi:no-transactions\nSELECT 1;", true)]
    [InlineData("", true)]
    public void Reads_the_no_transaction_directive_from_the_leading_comments(string sql, bool inTransaction)
    {
        Assert.Equal(inTransaction, MigrationLoader.ParseScript(sql).InTransaction);
    }

    private static IEnumerable<(string, string)> Pairs(string line, int from, int to)
    {
        for (var v = from; v <= to; v++)
        {
            yield return ($"{line}/{v:000}_v{v}.up.sql", $"u{v}");
            yield return ($"{line}/{v:000}_v{v}.down.sql", $"d{v}");
        }
    }
}
