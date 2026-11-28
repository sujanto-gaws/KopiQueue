using System.Globalization;
using System.Text.RegularExpressions;
using KopiQueue.Drivers;
using KopiQueue.Migrations;
using KopiQueue.SqlServer;
using Microsoft.Data.SqlClient;

namespace KopiQueue.Benchmarks;

/// <summary>A throwaway database, migrated, and dropped on dispose unless kept.</summary>
internal sealed partial class BenchDatabase : IAsyncDisposable
{
    public const string Job = "[dbo].[kopi_job]";

    private readonly string _adminConnectionString;
    private readonly bool _keep;

    private BenchDatabase(string adminConnectionString, string name, bool keep)
    {
        _adminConnectionString = adminConnectionString;
        _keep = keep;
        Name = name;
        ConnectionString = new SqlConnectionStringBuilder(adminConnectionString) { InitialCatalog = name }.ConnectionString;
        Driver = new SqlServerDriver(ConnectionString);
    }

    public string Name { get; }

    public string ConnectionString { get; }

    public IKopiDriver Driver { get; }

    public static async Task<BenchDatabase> CreateAsync(string adminConnectionString, BenchSettings settings)
    {
        var name = $"kopi_bench_{DateTime.UtcNow:yyyyMMddHHmmss}";
        await using (var admin = await OpenWhenReadyAsync(adminConnectionString))
        {
            await ExecAsync(admin, $"CREATE DATABASE [{name}]");
            if (settings.Rcsi)
            {
                await ExecAsync(admin, $"ALTER DATABASE [{name}] SET READ_COMMITTED_SNAPSHOT ON WITH ROLLBACK IMMEDIATE");
            }
        }

        var database = new BenchDatabase(adminConnectionString, name, settings.Keep);
        await database.Driver.CreateMigrator().MigrateAsync(MigrationDirection.Up);
        return database;
    }

    /// <summary>Opens a connection, waiting up to a minute for a server that is still starting, as in CI.</summary>
    private static async Task<SqlConnection> OpenWhenReadyAsync(string connectionString)
    {
        for (var attempt = 1; ; attempt++)
        {
            var connection = new SqlConnection(connectionString);
            try
            {
                await connection.OpenAsync();
                return connection;
            }
            catch (SqlException) when (attempt < 30)
            {
                await connection.DisposeAsync();
                await Task.Delay(TimeSpan.FromSeconds(2));
            }
        }
    }

    public async ValueTask DisposeAsync()
    {
        SqlConnection.ClearAllPools();
        if (_keep)
        {
            Report.Line($"\nKept database {Name}.");
            return;
        }

        await using var admin = new SqlConnection(_adminConnectionString);
        await admin.OpenAsync();
        await ExecAsync(admin, $"ALTER DATABASE [{Name}] SET SINGLE_USER WITH ROLLBACK IMMEDIATE; DROP DATABASE [{Name}];");
    }

    public async Task<SqlConnection> OpenAsync()
    {
        var connection = new SqlConnection(ConnectionString);
        await connection.OpenAsync();
        return connection;
    }

    public async Task ExecAsync(string sql, params (string Name, object Value)[] parameters)
    {
        await using var connection = await OpenAsync();
        await ExecAsync(connection, sql, parameters);
    }

    public async Task<T> ScalarAsync<T>(string sql, params (string Name, object Value)[] parameters)
    {
        await using var connection = await OpenAsync();
        await using var command = Command(connection, sql, parameters);
        return (T)Convert.ChangeType((await command.ExecuteScalarAsync())!, typeof(T), CultureInfo.InvariantCulture);
    }

    /// <summary>Inserts <paramref name="count"/> jobs in one statement: completed history, or due available jobs.</summary>
    public Task SeedAsync(int count, string state, string queue) => ExecAsync($$"""
        WITH n AS (
            SELECT TOP (@count) ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS i
            FROM sys.all_objects a CROSS JOIN sys.all_objects b CROSS JOIN sys.all_objects c
        )
        INSERT INTO {{Job}} (state, attempt, attempted_at, attempted_by, finalized_at, scheduled_at, args, kind, queue)
        SELECT @state,
               CASE @state WHEN 'completed' THEN 1 ELSE 0 END,
               CASE @state WHEN 'completed' THEN SYSUTCDATETIME() END,
               CASE @state WHEN 'completed' THEN N'["seed"]' END,
               CASE @state WHEN 'completed' THEN SYSUTCDATETIME() END,
               DATEADD(minute, -1, SYSUTCDATETIME()),
               N'{"n":' + CAST(i AS nvarchar(20)) + N'}', N'bench', @queue
        FROM n;
        """, ("@count", count), ("@state", state), ("@queue", queue));

    /// <summary>The statements this database has run, from the plan cache: subtract two snapshots for a step's cost.</summary>
    public async Task<StatementStats> SnapshotAsync()
    {
        await using var connection = await OpenAsync();
        await using var command = Command(connection, """
            SELECT SUBSTRING(st.text, qs.statement_start_offset / 2 + 1,
                       (CASE qs.statement_end_offset WHEN -1 THEN DATALENGTH(st.text) ELSE qs.statement_end_offset END
                        - qs.statement_start_offset) / 2 + 1),
                   qs.execution_count, qs.total_logical_reads, qs.total_logical_writes, qs.total_worker_time,
                   tp.query_plan,
                   CONVERT(nvarchar(130), qs.plan_handle, 2) + N':' + CAST(qs.statement_start_offset AS nvarchar(12))
                       + N':' + CONVERT(nvarchar(30), qs.creation_time, 126)
            FROM sys.dm_exec_query_stats AS qs
            CROSS APPLY sys.dm_exec_sql_text(qs.sql_handle) AS st
            CROSS APPLY (SELECT CAST(pa.value AS int) AS dbid FROM sys.dm_exec_plan_attributes(qs.plan_handle) AS pa
                         WHERE pa.attribute = 'dbid') AS db
            OUTER APPLY sys.dm_exec_text_query_plan(qs.plan_handle, qs.statement_start_offset, qs.statement_end_offset) AS tp
            WHERE db.dbid = DB_ID() AND st.text NOT LIKE N'%dm_exec_query_stats%';
            """, []);
        var entries = new Dictionary<string, (string Label, StatementCost Cost)>();
        await using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            entries[reader.GetString(6)] = (Label(reader.GetString(0)), new StatementCost(
                reader.GetInt64(1), reader.GetInt64(2), reader.GetInt64(3), reader.GetInt64(4),
                reader.IsDBNull(5) ? "" : Operators(reader.GetString(5))));
        }

        return new StatementStats(entries);
    }

    private static async Task ExecAsync(SqlConnection connection, string sql, params (string Name, object Value)[] parameters)
    {
        await using var command = Command(connection, sql, parameters);
        await command.ExecuteNonQueryAsync();
    }

    private static SqlCommand Command(SqlConnection connection, string sql, (string Name, object Value)[] parameters)
    {
#pragma warning disable CA2100 // The benchmark's own statements; values go in parameters.
        var command = new SqlCommand(sql, connection) { CommandTimeout = 600 };
#pragma warning restore CA2100
        foreach (var (name, value) in parameters)
        {
            command.Parameters.AddWithValue(name, value);
        }

        return command;
    }

    /// <summary>The statement without comments, on one line, shortened to its head and tail: enough to tell statements apart.</summary>
    private static string Label(string statement)
    {
        var text = Whitespace().Replace(LineComment().Replace(statement, " "), " ").Trim();
        return text.Length <= 110 ? text : $"{text[..70]} … {text[^36..]}"; // the head alone can be shared
    }

    /// <summary>The plan's physical operators with counts, for example "Clustered Index Update ×1, Table Spool ×1".</summary>
    private static string Operators(string plan) =>
        string.Join(", ", PhysicalOp().Matches(plan).Select(m => m.Groups[1].Value)
            .GroupBy(op => op).OrderBy(g => g.Key, StringComparer.Ordinal).Select(g => $"{g.Key} ×{g.Count()}"));

    [GeneratedRegex("--[^\r\n]*")]
    private static partial Regex LineComment();

    [GeneratedRegex(@"\s+")]
    private static partial Regex Whitespace();

    [GeneratedRegex("PhysicalOp=\"([^\"]+)\"")]
    private static partial Regex PhysicalOp();
}

internal sealed record StatementCost(long Executions, long Reads, long Writes, long CpuMicroseconds, string Operators)
{
    public StatementCost Add(StatementCost other) =>
        new(Executions + other.Executions, Reads + other.Reads, Writes + other.Writes, CpuMicroseconds + other.CpuMicroseconds,
            Operators.Length >= other.Operators.Length ? Operators : other.Operators);
}

/// <summary>
/// The plan cache's entries for this database, keyed by plan, statement and compile time. A recompiled statement gets a
/// new entry whose counters start again, so deltas are taken per entry and then summed per statement. Executions of an
/// entry evicted between two snapshots are lost; for a busy statement that is rare.
/// </summary>
internal sealed class StatementStats(IReadOnlyDictionary<string, (string Label, StatementCost Cost)> entries)
{
    private IReadOnlyDictionary<string, (string Label, StatementCost Cost)> Entries { get; } = entries;

    /// <summary>What ran between <paramref name="before"/> and this snapshot, the most reads first.</summary>
    public IReadOnlyList<(string Statement, StatementCost Cost)> Since(StatementStats before) =>
        [.. Entries
            .Select(e => (e.Value.Label, Cost: before.Entries.TryGetValue(e.Key, out var b)
                ? new StatementCost(e.Value.Cost.Executions - b.Cost.Executions, e.Value.Cost.Reads - b.Cost.Reads,
                    e.Value.Cost.Writes - b.Cost.Writes, e.Value.Cost.CpuMicroseconds - b.Cost.CpuMicroseconds, e.Value.Cost.Operators)
                : e.Value.Cost))
            .Where(e => e.Cost.Executions > 0)
            .GroupBy(e => e.Label, e => e.Cost)
            .Select(g => (g.Key, g.Aggregate((a, c) => a.Add(c))))
            .OrderByDescending(s => s.Item2.Reads)];
}
