using System.Diagnostics;
using KopiQueue.Drivers;
using KopiQueue.Migrations;
using KopiQueue.SqlServer.Tests.Migrations;
using KopiQueue.TestInfrastructure;
using Microsoft.Data.SqlClient;

namespace KopiQueue.SqlServer.Tests;

/// <summary>
/// Large inserts on SQL Server (plan task 5.5: FR-INS-04, ADR-014, NFR-PERF-07): batched <c>OPENJSON</c> statements of at
/// most 1,000 rows instead of <c>SqlBulkCopy</c>, so a caller's open transaction holds row locks only and never stalls
/// other producers and fetchers (spike 0.10).
/// </summary>
public sealed class SqlServerBulkInsertTests(SqlServerFixture db) : IClassFixture<SqlServerFixture>
{
    private const int Large = 50_000;

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Fact]
    public async Task Large_batches_insert_every_job_on_both_paths()
    {
        var (driver, _) = await MigratedAsync();

        var fast = await TimedAsync(() => driver.Executor().JobInsertFastAsync(Jobs("fast", Large), Ct));
        var many = await TimedAsync(async () => (long)(await driver.Executor().JobInsertManyAsync(Jobs("many", Large), Ct)).Count);

        // Reported, not asserted: NFR-PERF-07 (>= 30,000 jobs/s) is measured on CI hardware in M7. Spike 0.10 measured
        // 41,825 rows/s for the statement that InsertManyAsync uses.
        Report("InsertManyFastAsync", fast);
        Report("InsertManyAsync", many);
        Assert.Equal(Large, fast.Count);
        Assert.Equal(Large, many.Count);
        Assert.Equal(Large, await CountAsync(driver, "fast"));
        Assert.Equal(Large, await CountAsync(driver, "many"));
    }

    [Fact]
    public async Task A_large_insert_in_an_open_caller_transaction_holds_row_locks_and_blocks_no_fetcher()
    {
        // ADR-014: SqlBulkCopy held PAGE and EXTENT X locks that READPAST cannot skip, stalling fetchers of every queue.
        var (driver, schema) = await MigratedAsync();
        await driver.Executor().JobInsertManyAsync([Job("ready", "other")], Ct);
        await using var connection = await db.OpenConnectionAsync(Ct);
        await using var tx = (SqlTransaction)await connection.BeginTransactionAsync(Ct);

        var inserted = await ((IKopiDriver)driver).GetExecutor(tx).JobInsertFastAsync(Jobs("pending", 10_000, queue: "bulk"), Ct);
        var locks = await LockModesAsync(connection, tx);
        var watch = Stopwatch.StartNew();
        var fetched = await driver.Executor().JobGetAvailableAsync(new JobGetAvailableParams { Queue = "other", Max = 10, ClientId = "c" }, Ct);
        var fetchTime = watch.Elapsed;
        var bulkFetched = await driver.Executor().JobGetAvailableAsync(new JobGetAvailableParams { Queue = "bulk", Max = 10, ClientId = "c" }, Ct);
        await tx.RollbackAsync(Ct);

        Assert.Equal(10_000, inserted);
        Assert.DoesNotContain(locks, l => l.Type is "PAGE" or "EXTENT" or "OBJECT" && l.Mode is "X");
        Assert.Contains(locks, l => l.Type == "KEY" && l.Mode == "X");
        Assert.Equal("ready", Assert.Single(fetched).Kind);
        Assert.True(fetchTime < TimeSpan.FromSeconds(5), $"The fetch waited {fetchTime}.");
        Assert.Empty(bulkFetched); // the uncommitted rows are skipped, not waited for
        Assert.Equal(0, await CountAsync(driver, "pending"));
        Assert.Equal(1, (int)(await db.ScalarAsync($"SELECT COUNT(*) FROM [{schema}].kopi_job"))!);
    }

    [Fact]
    public async Task A_fast_insert_counts_every_statement_of_a_batch()
    {
        var (driver, _) = await MigratedAsync();

        var count = await driver.Executor().JobInsertFastAsync(Jobs("counted", 2_345), Ct);

        Assert.Equal(2_345, count);
        Assert.Equal(2_345, await CountAsync(driver, "counted"));
    }

    private static async Task<(int Count, TimeSpan Elapsed)> TimedAsync(Func<Task<long>> insert)
    {
        var watch = Stopwatch.StartNew();
        var count = await insert();
        return ((int)count, watch.Elapsed);
    }

    private static void Report(string path, (int Count, TimeSpan Elapsed) run) =>
        TestContext.Current.TestOutputHelper?.WriteLine(
            $"{path}: {run.Count:N0} jobs in {run.Elapsed.TotalMilliseconds:N0} ms = {run.Count / run.Elapsed.TotalSeconds:N0} jobs/s");

    /// <summary>The lock types and modes the transaction's session holds in the test database.</summary>
    private static async Task<List<(string Type, string Mode)>> LockModesAsync(SqlConnection connection, SqlTransaction tx)
    {
        await using var cmd = new SqlCommand(
            """
            SELECT DISTINCT resource_type, request_mode
            FROM sys.dm_tran_locks
            WHERE request_session_id = @@SPID AND resource_database_id = DB_ID()
            """,
            connection,
            tx);
        await using var reader = await cmd.ExecuteReaderAsync(Ct);
        var locks = new List<(string, string)>();
        while (await reader.ReadAsync(Ct))
        {
            locks.Add((reader.GetString(0).Trim(), reader.GetString(1).Trim()));
        }

        return locks;
    }

    private async Task<(SqlServerDriver Driver, string Schema)> MigratedAsync()
    {
        db.SkipIfUnavailable();
        var schema = await db.CreateSchemaAsync();
        var driver = new SqlServerDriver(db.ConnectionString, new SqlServerDriverOptions { Schema = schema });
        await driver.CreateMigrator().MigrateAsync(MigrationDirection.Up, cancellationToken: Ct);
        return (driver, schema);
    }

    private static async Task<long> CountAsync(SqlServerDriver driver, string kind)
    {
        await using var connection = await driver.Connections.OpenAsync(Ct);
        await using var cmd = new SqlCommand($"SELECT COUNT_BIG(*) FROM [{driver.Schema}].kopi_job WITH (READPAST) WHERE kind = @kind", connection);
        cmd.Parameters.AddWithValue("@kind", kind);
        return (long)(await cmd.ExecuteScalarAsync(Ct))!;
    }

    private static List<JobInsertParams> Jobs(string kind, int count, string queue = "default") =>
        [.. Enumerable.Range(0, count).Select(i => Job(kind, queue, $$"""{"n":{{i}}}"""))];

    private static JobInsertParams Job(string kind, string queue, string args = "{}") => new()
    {
        Kind = kind, ArgsJson = args, Queue = queue, State = JobState.Available, Priority = 1, MaxAttempts = 25,
    };
}
