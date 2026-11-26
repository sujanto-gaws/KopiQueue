using System.Collections.Concurrent;
using System.Data;
using System.Diagnostics;
using System.Threading.Channels;
using Microsoft.Data.SqlClient;

namespace SqlServerSpikes;

/// <summary>Spike 0.9: fetch correctness and locking (NFR-REL-06, FR-EXE-01).</summary>
internal static class Spike09
{
    /// <summary>N jobs, W concurrent fetchers, C concurrent OPENJSON completers. Every job must run exactly once.</summary>
    public static async Task ConcurrentFetchAsync(string db, int jobs = 20_000, int fetchers = 16, int completers = 2, int limit = 25)
    {
        Report.Section($"0.9a Concurrent fetch + complete: {db} ({jobs:N0} jobs, {fetchers} fetchers x limit {limit}, {completers} completers)");
        await Db.TruncateAsync(db);
        await Db.SeedAsync(db, jobs, "available", "default");

        var seen = new ConcurrentDictionary<long, byte>();
        int duplicates = 0, fetchDeadlocks = 0, completeDeadlocks = 0, completed = 0;
        var errors = new ConcurrentBag<string>();
        var channel = Channel.CreateUnbounded<long>();
        var sw = Stopwatch.StartNew();

        var fetchTasks = Enumerable.Range(0, fetchers).Select(i => Task.Run(async () =>
        {
            await using var c = await Db.OpenAsync(db);
            var empty = 0;
            while (empty < 3)
            {
                List<long> ids;
                try
                {
                    ids = await Db.FetchAsync(c, "default", limit, $"spike-{i}");
                }
                catch (SqlException e) when (e.Number == 1205)
                {
                    Interlocked.Increment(ref fetchDeadlocks);
                    continue;
                }
                catch (Exception e)
                {
                    errors.Add(Report.Err(e));
                    return;
                }
                if (ids.Count == 0)
                {
                    empty++;
                    await Task.Delay(20);
                    continue;
                }
                empty = 0;
                foreach (var id in ids)
                {
                    if (!seen.TryAdd(id, 0))
                    {
                        Interlocked.Increment(ref duplicates);
                    }
                    await channel.Writer.WriteAsync(id);
                }
            }
        })).ToArray();

        var completeTasks = Enumerable.Range(0, completers).Select(_ => Task.Run(async () =>
        {
            await using var c = await Db.OpenAsync(db);
            var batch = new List<long>(500);
            while (await channel.Reader.WaitToReadAsync())
            {
                batch.Clear();
                while (batch.Count < 500 && channel.Reader.TryRead(out var id))
                {
                    batch.Add(id);
                }
                if (batch.Count == 0)
                {
                    continue;
                }
                while (true)
                {
                    try
                    {
                        Interlocked.Add(ref completed, await Db.CompleteAsync(c, batch));
                        break;
                    }
                    catch (SqlException e) when (e.Number == 1205)
                    {
                        Interlocked.Increment(ref completeDeadlocks);
                    }
                }
            }
        })).ToArray();

        await Task.WhenAll(fetchTasks);
        channel.Writer.Complete();
        await Task.WhenAll(completeTasks);
        sw.Stop();

        await using var v = await Db.OpenAsync(db);
        await using var cmd = Db.Cmd(v, """
            SELECT COUNT(*), SUM(CASE WHEN state = 'completed' THEN 1 ELSE 0 END), MIN(attempt), MAX(attempt)
            FROM dbo.kopi_job;
            """);
        await using var r = await cmd.ExecuteReaderAsync();
        await r.ReadAsync();
        var (total, done, minAttempt, maxAttempt) = (r.GetInt32(0), r.GetInt32(1), r.GetInt16(2), r.GetInt16(3));

        Report.Line("result", total == jobs && done == jobs && minAttempt == 1 && maxAttempt == 1 && duplicates == 0
            ? "PASS: every job fetched exactly once and completed"
            : "FAIL");
        Report.Line("rows", $"total={total:N0} completed={done:N0} attempt min/max={minAttempt}/{maxAttempt} fetched-distinct={seen.Count:N0} duplicates={duplicates} completer-updated={completed:N0}");
        Report.Line("deadlocks", $"fetch={fetchDeadlocks} complete={completeDeadlocks}");
        Report.Line("other errors", errors.IsEmpty ? "none" : string.Join(" | ", errors.Distinct()));
        Report.Line("throughput", $"{jobs / sw.Elapsed.TotalSeconds:N0} jobs/s end to end ({sw.Elapsed.TotalSeconds:F1} s, no-op work)");
    }

    /// <summary>READPAST semantics, pooled-session isolation leakage and SNAPSHOT sessions.</summary>
    public static async Task IsolationAsync(string db)
    {
        Report.Section($"0.9b READPAST and isolation: {db}");
        await Db.TruncateAsync(db);
        await Db.SeedAsync(db, 20, "available", "default");

        // 1. Rows locked by an in-flight (uncommitted) fetch are skipped, not waited on.
        await using var a = await Db.OpenAsync(db);
        await using var b = await Db.OpenAsync(db);
        var txA = a.BeginTransaction(IsolationLevel.ReadCommitted);
        var idsA = await Db.FetchAsync(a, "default", 10, "A", txA);
        var sw = Stopwatch.StartNew();
        var idsB = await Db.FetchAsync(b, "default", 10, "B", timeout: 5);
        var tB = sw.ElapsedMilliseconds;
        var idsB2 = await Db.FetchAsync(b, "default", 10, "B", timeout: 5);
        Report.Line("locked rows skipped",
            $"A took {idsA.Count} (uncommitted); B then got {idsB.Count} in {tB} ms, overlap={idsA.Intersect(idsB).Count()}; B again got {idsB2.Count}");
        txA.Rollback();
        var idsB3 = await Db.FetchAsync(b, "default", 10, "B", timeout: 5);
        Report.Line("after A rolls back", $"B got {idsB3.Count}, same rows A had: {idsB3.ToHashSet().SetEquals(idsA)}");

        // 2. Jobs inserted by an uncommitted caller transaction are invisible and do not block fetchers.
        var txI = a.BeginTransaction();
        await Db.SeedAsync(a, 5, "available", "q2", txI);
        sw.Restart();
        string before;
        try
        {
            before = $"{(await Db.FetchAsync(b, "q2", 10, "B", timeout: 5)).Count} rows in {sw.ElapsedMilliseconds} ms";
        }
        catch (Exception e)
        {
            before = $"{Report.Err(e)} after {sw.ElapsedMilliseconds} ms";
        }
        txI.Commit();
        var after = await Db.FetchAsync(b, "q2", 10, "B", timeout: 5);
        Report.Line("uncommitted insert", $"fetch before commit: {before}; after commit: {after.Count} rows");

        // 3. Does a SNAPSHOT isolation level leak through the connection pool?
        var cs = Db.For(db);
        int spid1, spid2;
        short level;
        await using (var c = new SqlConnection(cs))
        {
            await c.OpenAsync();
            await Db.ExecAsync(c, "SET TRANSACTION ISOLATION LEVEL SNAPSHOT;");
            spid1 = await Db.ScalarAsync<short>(c, "SELECT @@SPID;");
        }
        await using (var c = new SqlConnection(cs))
        {
            await c.OpenAsync();
            spid2 = await Db.ScalarAsync<short>(c, "SELECT @@SPID;");
            level = await Db.ScalarAsync<short>(c, "SELECT transaction_isolation_level FROM sys.dm_exec_sessions WHERE session_id = @@SPID;");
        }
        Report.Line("pool leak (SET ... SNAPSHOT)", $"same pooled session: {spid1 == spid2}; isolation after reuse: {LevelName(level)}");

        await using (var c = new SqlConnection(cs))
        {
            await c.OpenAsync();
            using var t = c.BeginTransaction(IsolationLevel.Snapshot);
            await Db.ExecAsync(c, "SELECT 1;", t);
            t.Commit();
            level = await Db.ScalarAsync<short>(c, "SELECT transaction_isolation_level FROM sys.dm_exec_sessions WHERE session_id = @@SPID;");
        }
        short pooled;
        await using (var c = new SqlConnection(cs))
        {
            await c.OpenAsync();
            pooled = await Db.ScalarAsync<short>(c, "SELECT transaction_isolation_level FROM sys.dm_exec_sessions WHERE session_id = @@SPID;");
        }
        Report.Line("pool leak (SqlTransaction Snapshot)", $"same connection after commit: {LevelName(level)}; next pooled open: {LevelName(pooled)}");

        // 4. Fetch on a session whose isolation level is SNAPSHOT.
        await Db.TruncateAsync(db);
        await Db.SeedAsync(db, 5, "available", "default");
        await using var s = await Db.OpenAsync(db);
        await Db.ExecAsync(s, "SET TRANSACTION ISOLATION LEVEL SNAPSHOT;");
        string plain, guarded;
        try
        {
            plain = $"OK, {(await Db.FetchAsync(s, "default", 2, "S")).Count} rows";
        }
        catch (Exception e)
        {
            plain = Report.Err(e);
        }
        try
        {
            guarded = $"OK, {(await Db.FetchAsync(s, "default", 2, "S", prefix: "SET TRANSACTION ISOLATION LEVEL READ COMMITTED;\n")).Count} rows";
        }
        catch (Exception e)
        {
            guarded = Report.Err(e);
        }
        var levelAfter = await Db.ScalarAsync<short>(s, "SELECT transaction_isolation_level FROM sys.dm_exec_sessions WHERE session_id = @@SPID;");
        Report.Line("fetch on SNAPSHOT session", plain);
        Report.Line("fetch with SET READ COMMITTED prefix", $"{guarded}; session level afterwards: {LevelName(levelAfter)}");
    }

    /// <summary>Lock counts held by one completer / fetch statement inside a transaction, by batch size.</summary>
    public static async Task EscalationAsync(string db)
    {
        Report.Section($"0.9c Lock escalation: {db}");
        await Db.TruncateAsync(db);
        await Db.SeedAsync(db, 12_000, "running", "default");
        await Db.SeedAsync(db, 12_000, "available", "default");

        foreach (var n in new[] { 1_000, 2_000, 4_000, 4_900, 6_000, 10_000 })
        {
            await using var c = await Db.OpenAsync(db);
            var tx = c.BeginTransaction(IsolationLevel.ReadCommitted);
            var ids = new List<long>(n);
            await using (var cmd = Db.Cmd(c, $"SELECT TOP ({n}) id FROM dbo.kopi_job WHERE state = 'running' ORDER BY id;", tx))
            await using (var r = await cmd.ExecuteReaderAsync())
            {
                while (await r.ReadAsync())
                {
                    ids.Add(r.GetInt64(0));
                }
            }
            await Db.CompleteAsync(c, ids, tx);
            var (summary, escalated) = await Db.LocksAsync(c, tx);
            tx.Rollback();
            Report.Line($"complete batch {n,6:N0}", $"{(escalated ? "ESCALATED" : "row locks")}: {summary}");
        }

        foreach (var n in new[] { 1_000, 4_900, 6_000 })
        {
            await using var c = await Db.OpenAsync(db);
            var tx = c.BeginTransaction(IsolationLevel.ReadCommitted);
            await Db.FetchAsync(c, "default", n, "esc", tx);
            var (summary, escalated) = await Db.LocksAsync(c, tx);
            tx.Rollback();
            Report.Line($"fetch limit {n,6:N0}", $"{(escalated ? "ESCALATED" : "row locks")}: {summary}");
        }
    }

    /// <summary>Actual execution plan and locks of the fetch statement for a few limits (diagnoses 0.9c).</summary>
    public static async Task FetchPlanAsync(string db, string extraHint = "")
    {
        Report.Section($"0.9d Fetch plan diagnosis: {db}{(extraHint.Length > 0 ? $" (variant: {extraHint})" : "")}");
        await Db.TruncateAsync(db);
        await Db.SeedAsync(db, 12_000, "running", "default");
        await Db.SeedAsync(db, 12_000, "available", "default");
        await Db.SeedAsync(db, 12_000, "available", "other");

        foreach (var n in new[] { 25, 1_000, 4_000 })
        {
            await using var c = await Db.OpenAsync(db);
            var tx = c.BeginTransaction(IsolationLevel.ReadCommitted);
            await Db.ExecAsync(c, "SET STATISTICS XML ON;", tx);
            var sql = Db.FetchSql.Replace("/*tag*/", $"/* plan-{n} */", StringComparison.Ordinal);
            if (extraHint.Length == 0)
            {
                sql = sql.Replace(", FORCESEEK(kopi_job_fetch_idx(queue)))", ")", StringComparison.Ordinal);
            }
            else
            {
                sql = sql.Replace(", FORCESEEK(kopi_job_fetch_idx(queue)))", $", {extraHint})", StringComparison.Ordinal);
            }
            await using var cmd = Db.Cmd(c, sql, tx);
            cmd.Parameters.Add("@limit", SqlDbType.Int).Value = n;
            cmd.Parameters.Add("@queue", SqlDbType.NVarChar, 128).Value = "default";
            cmd.Parameters.Add("@client_id", SqlDbType.NVarChar, 128).Value = "plan";
            string plan = "";
            var rows = 0;
            await using (var r = await cmd.ExecuteReaderAsync())
            {
                do
                {
                    while (await r.ReadAsync())
                    {
                        if (r.GetName(0).StartsWith("Microsoft SQL Server", StringComparison.Ordinal) && r.GetValue(0) is string xml)
                        {
                            plan = xml;
                        }
                        else
                        {
                            rows++;
                        }
                    }
                }
                while (await r.NextResultAsync());
            }
            await Db.ExecAsync(c, "SET STATISTICS XML OFF;", tx);
            var (locks, escalated) = await Db.LocksAsync(c, tx);
            tx.Rollback();

            var ops = System.Text.RegularExpressions.Regex.Matches(plan,
                    "PhysicalOp=\"([^\"]+)\"[^>]*?EstimateRows=\"([^\"]+)\"")
                .Select(m => $"{m.Groups[1].Value}(est {double.Parse(m.Groups[2].Value, System.Globalization.CultureInfo.InvariantCulture):N0})");
            var objects = System.Text.RegularExpressions.Regex.Matches(plan, "Index=\"\\[([^\\]]+)\\]\"")
                .Select(m => m.Groups[1].Value).Distinct();
            var actualRead = System.Text.RegularExpressions.Regex.Matches(plan, "ActualRowsRead=\"(\\d+)\"")
                .Select(m => long.Parse(m.Groups[1].Value, System.Globalization.CultureInfo.InvariantCulture)).DefaultIfEmpty(0).Max();
            Report.Line($"limit {n,5:N0}", $"{rows} rows; {(escalated ? "ESCALATED" : "row locks")}: {locks}");
            Report.Line($"limit {n,5:N0} plan", $"ops: {string.Join(" > ", ops)}; indexes: {string.Join(", ", objects)}; max ActualRowsRead={actualRead:N0}");
        }
    }

    private static string LevelName(short level) => level switch
    {
        1 => "READ UNCOMMITTED",
        2 => "READ COMMITTED",
        3 => "REPEATABLE READ",
        4 => "SERIALIZABLE",
        5 => "SNAPSHOT",
        _ => $"unknown({level})",
    };
}
