using KopiQueue.SqlServer.Migrations;
using KopiQueue.TestInfrastructure;
using Microsoft.Data.SqlClient;

namespace KopiQueue.SqlServer.Tests.Migrations;

/// <summary>Helpers for tests that run against the fixture's database. Each test works in a schema of its own.</summary>
internal static class SqlServerTestDb
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    public static async Task<string> CreateSchemaAsync(this SqlServerFixture db, string? name = null)
    {
        name ??= "m_" + Guid.NewGuid().ToString("N")[..12];
        await db.ExecuteAsync($"CREATE SCHEMA [{name}]");
        return name;
    }

    public static async Task<int> ExecuteAsync(this SqlServerFixture db, string sql, params SqlParameter[] parameters)
    {
        await using var conn = await db.OpenConnectionAsync(Ct);
        await using var cmd = new SqlCommand(sql, conn);
        cmd.Parameters.AddRange(parameters);
        return await cmd.ExecuteNonQueryAsync(Ct);
    }

    public static async Task<object?> ScalarAsync(this SqlServerFixture db, string sql, params SqlParameter[] parameters)
    {
        await using var conn = await db.OpenConnectionAsync(Ct);
        await using var cmd = new SqlCommand(sql, conn);
        cmd.Parameters.AddRange(parameters);
        var value = await cmd.ExecuteScalarAsync(Ct);
        return value is DBNull ? null : value;
    }

    public static async Task<List<string>> StringsAsync(this SqlServerFixture db, string sql, params SqlParameter[] parameters)
    {
        await using var conn = await db.OpenConnectionAsync(Ct);
        await using var cmd = new SqlCommand(sql, conn);
        cmd.Parameters.AddRange(parameters);
        await using var reader = await cmd.ExecuteReaderAsync(Ct);
        var values = new List<string>();
        while (await reader.ReadAsync(Ct))
        {
            values.Add(reader.GetString(0));
        }

        return values;
    }

    /// <summary>The tables of <paramref name="schema"/>, in ordinal order.</summary>
    public static Task<List<string>> TablesAsync(this SqlServerFixture db, string schema) => db.StringsAsync(
        "SELECT t.name FROM sys.tables t WHERE t.schema_id = SCHEMA_ID(@schema) ORDER BY t.name COLLATE Latin1_General_100_BIN2",
        new SqlParameter("@schema", schema));

    /// <summary>Every object of <paramref name="schema"/>: tables, views, constraints, and so on.</summary>
    public static async Task<int> ObjectCountAsync(this SqlServerFixture db, string schema) => (int)(await db.ScalarAsync(
        "SELECT COUNT(*) FROM sys.objects WHERE schema_id = SCHEMA_ID(@schema)", new SqlParameter("@schema", schema)))!;

    public static async Task<List<long>> RecordedAsync(this SqlServerFixture db, SqlServerMigrationDriver driver, string line = "main")
    {
        await using var conn = await db.OpenConnectionAsync(Ct);
        await using var cmd = new SqlCommand($"SELECT version FROM {driver.MigrationTable} WHERE line = @line ORDER BY version", conn);
        cmd.Parameters.AddWithValue("@line", line);
        try
        {
            await using var reader = await cmd.ExecuteReaderAsync(Ct);
            var versions = new List<long>();
            while (await reader.ReadAsync(Ct))
            {
                versions.Add(reader.GetInt64(0));
            }

            return versions;
        }
        catch (SqlException e) when (e.Number == 208)
        {
            return [];
        }
    }

    /// <summary>Whether another session could take the migration lock now. Takes nothing itself.</summary>
    public static async Task<bool> LockIsFreeAsync(this SqlServerFixture db, SqlServerMigrationDriver driver) => (int)(await db.ScalarAsync(
        "SELECT APPLOCK_TEST('public', @resource, 'Exclusive', 'Session')", new SqlParameter("@resource", driver.LockResource)))! == 1;

    public static SqlServerMigrationDriver MigrationDriver(this SqlServerFixture db, string schema, string prefix = SqlServerMigrationDriver.DefaultTablePrefix) =>
        new(new SqlServerConnectionFactory(db.ConnectionString), schema, prefix);
}
