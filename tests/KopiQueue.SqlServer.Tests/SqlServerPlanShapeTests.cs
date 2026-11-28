using System.Xml.Linq;
using KopiQueue.Drivers;
using KopiQueue.Migrations;
using KopiQueue.SqlServer.Tests.Migrations;
using KopiQueue.TestInfrastructure;
using Microsoft.Data.SqlClient;

namespace KopiQueue.SqlServer.Tests;

/// <summary>
/// Plan-shape tests (plan task 5.8, design §8.1 rules 3–5, spike 0.9 D5): every statement the driver runs against the job
/// table reaches it through seeks, at small and at full (1,000-row) sizes on a table far larger than one statement, and
/// no statement's locks escalate. Without <c>FORCESEEK</c>, spike 0.9 saw the fetch flip to a clustered scan at larger
/// limits, read every row under <c>UPDLOCK</c> and escalate to a table lock that blocked every other producer.
/// </summary>
/// <remarks>
/// Query Store records the plan of every statement, including those compiled with <c>OPTION (RECOMPILE)</c>, which the
/// plan cache does not keep. The lock promotions come from <c>sys.dm_db_index_operational_stats</c>.
/// </remarks>
public sealed class SqlServerPlanShapeTests(SqlServerFixture db) : IClassFixture<SqlServerFixture>
{
    private const int Full = 1_000;

    /// <summary>The test's own statement, which Query Store records too (it drops comments, so a marker wouldn't do).</summary>
    private const string TestSetup = "SET finalized_at = DATEADD(day, -2, finalized_at)";

    /// <summary>
    /// Marks the reads that may scan: the admin list and delete-many's first step, compiled per call for their arbitrary
    /// filters. They read with <c>READPAST</c> or plain READ COMMITTED and lock nothing they don't delete.
    /// </summary>
    private const string FilteredRead = "OPTION (RECOMPILE)";

    private static readonly XNamespace Showplan = "http://schemas.microsoft.com/sqlserver/2004/07/showplan";

    private static readonly DateTimeOffset Past = DateTimeOffset.UtcNow.AddHours(-1);

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Fact]
    public async Task Every_job_statement_seeks_and_none_escalates()
    {
        db.SkipIfUnavailable();
        await db.ExecuteAsync(
            "ALTER DATABASE CURRENT SET QUERY_STORE = ON (OPERATION_MODE = READ_WRITE, QUERY_CAPTURE_MODE = ALL)");
        var schema = await db.CreateSchemaAsync();
        var driver = new SqlServerDriver(db.ConnectionString, new SqlServerDriverOptions { Schema = schema });
        await driver.CreateMigrator().MigrateAsync(MigrationDirection.Up, cancellationToken: Ct);
        var executor = driver.Executor();

        await RunEveryJobStatementAsync(executor, schema);

        var plans = await PlansAsync(schema);
        Assert.True(plans.Count >= 20, $"Query Store recorded only {plans.Count} plans.");
        var scans = plans
            .Where(p => !p.Text.Contains(TestSetup, StringComparison.Ordinal) && !p.Text.Contains(FilteredRead, StringComparison.Ordinal))
            .SelectMany(p => JobTableAccess(p.Plan, schema).Where(op => !op.EndsWith("Seek", StringComparison.Ordinal)).Select(op => $"{op}: {Shorten(p.Text)}"))
            .Distinct()
            .ToList();
        Assert.True(scans.Count == 0, "Statements that scan the job table:\n" + string.Join("\n", scans));
        Assert.Equal(0L, await LockPromotionsAsync(schema));
    }

    /// <summary>Runs each statement that touches the job table, with as many rows as one statement may touch.</summary>
    private async Task RunEveryJobStatementAsync(IKopiExecutor executor, string schema)
    {
        // Far more rows than one statement touches, in several states, so a scan would show and a scan's locks escalate.
        for (var i = 0; i < 4; i++)
        {
            await executor.JobInsertFastAsync([.. Enumerable.Range(0, Full).Select(_ => Job(JobState.Available, queue: "bulk"))], Ct);
        }

        await executor.JobInsertManyAsync([.. Enumerable.Range(0, Full).Select(_ => Job(JobState.Scheduled))], Ct);
        await executor.JobInsertManyAsync([.. Enumerable.Range(0, Full).Select(i => Job(JobState.Available, key: Key(i)))], Ct);
        await executor.JobInsertManyAsync([.. Enumerable.Range(0, 10).Select(i => Job(JobState.Available, key: Key(i)))], Ct); // duplicates

        foreach (var max in new[] { 10, Full })
        {
            await executor.JobScheduleAsync(new JobScheduleParams { Max = max }, Ct);
        }

        var running = new List<JobRow>();
        foreach (var max in new[] { 10, Full })
        {
            running.AddRange(await executor.JobGetAvailableAsync(new JobGetAvailableParams { Queue = "default", Max = max, ClientId = "c" }, Ct));
        }

        Assert.Equal(10 + Full, running.Count);
        await executor.JobListCancelAttemptedAsync([.. running.Take(Full).Select(j => j.Id).Order()], Ct);
        await executor.JobCancelAsync(running[0].Id, Ct);

        var stuck = await executor.JobListStuckAsync(new JobListStuckParams { StuckAfter = TimeSpan.FromTicks(10), Max = Full }, Ct);
        await executor.JobRescueManyAsync(
            [.. stuck.Jobs.Take(500).Select(j => new JobRescueParams
            {
                Id = j.Id, AttemptedAt = j.AttemptedAt!.Value, State = JobState.Retryable, ScheduledAt = Past,
                Error = new AttemptError(stuck.Now, j.Attempt, "stuck", null),
            })], Ct);
        await executor.JobSetStateIfRunningManyAsync(
            [.. running.Skip(500).Take(Full).Select(j => new JobSetStateParams { Id = j.Id, State = JobState.Completed, FinalizedAt = DateTimeOffset.UtcNow })], Ct);
        await executor.JobScheduleAsync(new JobScheduleParams { Max = Full }, Ct); // the rescued retryable jobs

        await db.ExecuteAsync($"UPDATE [{schema}].kopi_job {TestSetup} WHERE state = 'completed'");
        foreach (var max in new[] { 10, Full })
        {
            await executor.JobDeleteFinalizedAsync(new JobDeleteFinalizedParams { CompletedRetention = TimeSpan.FromDays(1), Max = max }, Ct);
        }

        var one = running[^1].Id;
        await executor.JobGetByIdAsync(one, Ct);
        await executor.JobRetryAsync(one, Ct);
        await executor.JobDeleteAsync(one, Ct);
        await executor.JobListAsync(new JobListQuery { Filter = new JobListParams { Queues = ["bulk"] }, Max = 100 }, Ct);
        await executor.JobListAsync(new JobListQuery { Filter = new JobListParams { Queues = ["bulk"] }, OrderBy = JobListOrderBy.ScheduledAt, Max = 100 }, Ct);
        foreach (var max in new[] { 10, Full })
        {
            await executor.JobDeleteManyAsync(new JobDeleteManyQuery { Filter = new JobDeleteManyParams { Queues = ["bulk"] }, Max = max }, Ct);
        }
    }

    /// <summary>The text and plan of every statement Query Store recorded on <paramref name="schema"/>'s tables.</summary>
    private async Task<List<(string Text, string Plan)>> PlansAsync(string schema)
    {
        await using var connection = await db.OpenConnectionAsync(Ct);
        await using var cmd = new SqlCommand(
            """
            SELECT t.query_sql_text, p.query_plan
            FROM sys.query_store_query_text AS t
            JOIN sys.query_store_query AS q ON q.query_text_id = t.query_text_id
            JOIN sys.query_store_plan AS p ON p.query_id = q.query_id
            WHERE t.query_sql_text LIKE @schema
            """,
            connection);
        cmd.Parameters.AddWithValue("@schema", "%" + schema + "%");
        await using var reader = await cmd.ExecuteReaderAsync(Ct);
        var plans = new List<(string, string)>();
        while (await reader.ReadAsync(Ct))
        {
            plans.Add((reader.GetString(0), reader.GetString(1)));
        }

        return plans;
    }

    /// <summary>The physical operators with which <paramref name="plan"/> reads the job table's indexes.</summary>
    private static IEnumerable<string> JobTableAccess(string plan, string schema) =>
        XDocument.Parse(plan)
            .Descendants(Showplan + "RelOp")
            .Where(op => op.Elements().Any(e => e.Name.LocalName is "IndexScan" or "TableScan" && e.Elements(Showplan + "Object").Any(o =>
                (string?)o.Attribute("Schema") == $"[{schema}]" && (string?)o.Attribute("Table") == "[kopi_job]")))
            .Select(op => (string)op.Attribute("PhysicalOp")!);

    private async Task<long> LockPromotionsAsync(string schema) => (long)(await db.ScalarAsync(
        $"SELECT CAST(ISNULL(SUM(index_lock_promotion_count), 0) AS bigint) FROM sys.dm_db_index_operational_stats(DB_ID(), OBJECT_ID(N'[{schema}].kopi_job'), NULL, NULL)"))!;

    private static string Shorten(string text)
    {
        var flat = string.Join(' ', text.Split((char[])['\r', '\n', '\t', ' '], StringSplitOptions.RemoveEmptyEntries));
        return flat.Length <= 160 ? flat : flat[..160] + "…";
    }

    private static byte[] Key(int i)
    {
        var key = new byte[32];
        BitConverter.TryWriteBytes(key, i);
        return key;
    }

    private static JobInsertParams Job(JobState state, byte[]? key = null, string queue = "default") => new()
    {
        Kind = "k",
        ArgsJson = "{}",
        Queue = queue,
        State = state,
        Priority = 1,
        MaxAttempts = 25,
        ScheduledAt = Past,
        UniqueKey = key,
        UniqueStates = key is null ? null : (byte)0b1111_0101,
    };
}
