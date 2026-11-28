using KopiQueue.SqlServer.Migrations;
using KopiQueue.TestInfrastructure;
using Microsoft.Data.SqlClient;
using static KopiQueue.SqlServer.SqlServerSessionCheck;

namespace KopiQueue.SqlServer.Tests;

/// <summary>The check of a session's SET options and compatibility level (design §4.4, plan task 5.1).</summary>
public sealed class SqlServerSessionCheckTests(SqlServerFixture db) : IClassFixture<SqlServerFixture>
{
    private static readonly SessionSettings Good = new(
        AnsiNulls: true, AnsiPadding: true, AnsiWarnings: true, ConcatNullYieldsNull: true, QuotedIdentifier: true,
        NumericRoundAbort: false, CompatibilityLevel: 150);

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Fact]
    public void Good_settings_pass()
    {
        Assert.Empty(Evaluate(Good));
        Assert.Empty(Evaluate(Good with { CompatibilityLevel = 170 }));
        Assert.Empty(Evaluate(Good with { CompatibilityLevel = null }));
    }

    [Fact]
    public void Each_wrong_setting_is_named()
    {
        var problems = Evaluate(new SessionSettings(
            AnsiNulls: false, AnsiPadding: false, AnsiWarnings: false, ConcatNullYieldsNull: false, QuotedIdentifier: false,
            NumericRoundAbort: true, CompatibilityLevel: 140));

        Assert.Equal(
        [
            "ANSI_NULLS is OFF but must be ON",
            "ANSI_PADDING is OFF but must be ON",
            "ANSI_WARNINGS is OFF but must be ON",
            "CONCAT_NULL_YIELDS_NULL is OFF but must be ON",
            "QUOTED_IDENTIFIER is OFF but must be ON",
            "NUMERIC_ROUNDABORT is ON but must be OFF",
            "the database compatibility level is 140, but KopiQueue needs 150 or later",
        ], problems);
    }

    [Fact]
    public async Task A_default_SqlClient_session_passes()
    {
        db.SkipIfUnavailable();
        await using var conn = await db.OpenConnectionAsync(Ct);

        var settings = await ReadAsync(conn, null, Ct);
        await EnsureAsync(conn, null, Ct);

        Assert.True(settings.CompatibilityLevel >= MinimumCompatibilityLevel);
        Assert.Empty(Evaluate(settings));
    }

    [Theory]
    [InlineData("SET ANSI_WARNINGS OFF", "ANSI_WARNINGS")]
    [InlineData("SET NUMERIC_ROUNDABORT ON", "NUMERIC_ROUNDABORT")]
    [InlineData("SET QUOTED_IDENTIFIER OFF", "QUOTED_IDENTIFIER")]
    public async Task A_session_changed_by_other_code_fails_fast(string set, string named)
    {
        db.SkipIfUnavailable();

        // Not pooled, so the changed session is not handed to other tests.
        await using var conn = new SqlConnection(new SqlConnectionStringBuilder(db.ConnectionString) { Pooling = false }.ConnectionString);
        await conn.OpenAsync(Ct);
        await using (var cmd = new SqlCommand(set, conn))
        {
            await cmd.ExecuteNonQueryAsync(Ct);
        }

        var ex = await Assert.ThrowsAsync<KopiQueueConfigurationException>(() => EnsureAsync(conn, null, Ct));

        Assert.Contains(named, ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task The_migrator_checks_its_session_before_it_reads_or_migrates()
    {
        db.SkipIfUnavailable();
        var driver = new SqlServerMigrationDriver(new SqlServerConnectionFactory(db.ConnectionString));

        await using var session = await driver.OpenSessionAsync(Ct);

        Assert.Empty(await session.GetAppliedVersionsAsync("main", Ct));
    }
}
