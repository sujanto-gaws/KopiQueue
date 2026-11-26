using KopiQueue.TestInfrastructure;
using Microsoft.Data.SqlClient;

namespace KopiQueue.SqlServer.Tests;

/// <summary>Proves the SQL Server test fixture works (M0 task 0.4). Replaced by the conformance suite from M1.</summary>
public sealed class InfrastructureSmokeTests(SqlServerFixture db) : IClassFixture<SqlServerFixture>
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Fact]
    public async Task Server_version_is_supported()
    {
        db.SkipIfUnavailable();
        await using var conn = await db.OpenConnectionAsync(Ct);
        await using var cmd = new SqlCommand("SELECT CAST(SERVERPROPERTY('ProductMajorVersion') AS int)", conn);

        var major = (int)(await cmd.ExecuteScalarAsync(Ct))!;

        Assert.True(major >= 15, $"SQL Server 2019 (v15)+ required (NFR-COMP-02a), got v{major}");
    }

    [Fact]
    public async Task Fixture_database_is_fresh_isolated_and_compatible()
    {
        db.SkipIfUnavailable();
        await using var conn = await db.OpenConnectionAsync(Ct);
        await using var cmd = new SqlCommand(
            """
            SELECT DB_NAME(),
                   (SELECT COUNT(*) FROM sys.tables),
                   (SELECT compatibility_level FROM sys.databases WHERE database_id = DB_ID())
            """, conn);
        await using var reader = await cmd.ExecuteReaderAsync(Ct);
        Assert.True(await reader.ReadAsync(Ct));

        Assert.Equal(db.DatabaseName, reader.GetString(0));
        Assert.Equal(0, reader.GetInt32(1));
        Assert.True(reader.GetByte(2) >= 150, $"compatibility level 150+ required, got {reader.GetByte(2)}");
    }

    [Fact]
    public async Task Default_sqlclient_session_can_write_filtered_and_computed_column_indexes()
    {
        // Design §4.4 / ADR-011. SqlClient leaves ARITHABORT OFF, which is fine only because ANSI_WARNINGS ON
        // implies ARITHABORT at compatibility level >= 90. Prove the real requirement: DML succeeds.
        db.SkipIfUnavailable();
        await using var conn = await db.OpenConnectionAsync(Ct);
        await using (var setup = new SqlCommand(
            """
            CREATE TABLE dbo.smoke_slot (
              id bigint IDENTITY PRIMARY KEY, state varchar(16) NOT NULL, k varbinary(32) NULL,
              slot AS CAST(CASE WHEN k IS NOT NULL AND state = 'available' THEN 0 ELSE id END AS bigint) PERSISTED);
            CREATE INDEX smoke_slot_filtered ON dbo.smoke_slot (id) WHERE state = 'available';
            CREATE UNIQUE INDEX smoke_slot_unique ON dbo.smoke_slot (k, slot);
            """, conn))
        {
            await setup.ExecuteNonQueryAsync(Ct);
        }

        // Tests in a class share one database and run in any order: leave it empty for the freshness check.
        try
        {
            // Non-occupied duplicates and NULL keys are allowed; then a second occupied row with the same key is rejected.
            await using (var ok = new SqlCommand(
                """
                INSERT INTO dbo.smoke_slot (state, k) VALUES
                  ('available', 0x01), ('completed', 0x01), ('completed', 0x01), ('available', NULL), ('available', NULL);
                """, conn))
            {
                Assert.Equal(5, await ok.ExecuteNonQueryAsync(Ct));
            }

            await using var duplicate = new SqlCommand("INSERT INTO dbo.smoke_slot (state, k) VALUES ('available', 0x01)", conn);
            var ex = await Assert.ThrowsAsync<SqlException>(() => duplicate.ExecuteNonQueryAsync(Ct));
            Assert.Equal(2601, ex.Number);
        }
        finally
        {
            await using var drop = new SqlCommand("DROP TABLE IF EXISTS dbo.smoke_slot", conn);
            await drop.ExecuteNonQueryAsync(CancellationToken.None);
        }
    }
}
