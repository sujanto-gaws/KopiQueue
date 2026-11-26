using System.Collections.Concurrent;
using System.Data;
using System.Diagnostics;
using System.Security.Cryptography;
using System.Text.RegularExpressions;
using Microsoft.Data.SqlClient;

namespace SqlServerSpikes;

internal enum UniqueStrategy
{
    /// <summary>Design §8.1 as written: INSERT ... WHERE NOT EXISTS (UPDLOCK, HOLDLOCK).</summary>
    HoldLock,

    /// <summary>Alternative: sp_getapplock on the unique key (transaction-owned), then a plain READCOMMITTEDLOCK check.</summary>
    AppLock,
}

internal enum UniqueOutcome
{
    Inserted,
    Skipped,
    Neither,
}

/// <summary>Spike 0.10: unique slot under concurrency, SqlBulkCopy in a caller transaction, poll cost and latency.</summary>
internal static partial class Spike10
{
    private const string Columns = "state, max_attempts, args, kind, queue, unique_key, unique_states";
    private const string Values = "'available', 25, N'{}', N'spike', N'default', @k, @states";

    private static readonly string HoldLockSql = $"""
        INSERT INTO dbo.kopi_job ({Columns})
        OUTPUT inserted.id, CAST(0 AS bit)
        SELECT {Values}
        WHERE NOT EXISTS (SELECT 1 FROM dbo.kopi_job WITH (UPDLOCK, HOLDLOCK) WHERE unique_key = @k AND unique_slot = 0);
        IF @@ROWCOUNT = 0
          SELECT id, CAST(1 AS bit) FROM dbo.kopi_job WITH (READCOMMITTEDLOCK) WHERE unique_key = @k AND unique_slot = 0;
        """;

    private static readonly string AppLockSql = $"""
        DECLARE @r int;
        EXEC @r = sp_getapplock @Resource = @res, @LockMode = 'Exclusive', @LockOwner = 'Transaction', @LockTimeout = 30000;
        IF @r < 0 THROW 50001, 'kopi: could not acquire unique-key lock', 1;
        INSERT INTO dbo.kopi_job ({Columns})
        OUTPUT inserted.id, CAST(0 AS bit)
        SELECT {Values}
        WHERE NOT EXISTS (SELECT 1 FROM dbo.kopi_job WITH (READCOMMITTEDLOCK) WHERE unique_key = @k AND unique_slot = 0);
        IF @@ROWCOUNT = 0
          SELECT id, CAST(1 AS bit) FROM dbo.kopi_job WITH (READCOMMITTEDLOCK) WHERE unique_key = @k AND unique_slot = 0;
        """;

    public static async Task<UniqueOutcome> InsertUniqueAsync(SqlConnection c, SqlTransaction? tx, UniqueStrategy s, byte[] key)
    {
        // The applock is transaction-owned, so the driver would open its own transaction when the caller has none.
        var ownTx = s == UniqueStrategy.AppLock && tx is null ? c.BeginTransaction(IsolationLevel.ReadCommitted) : null;
        try
        {
            await using var cmd = Db.Cmd(c, s == UniqueStrategy.HoldLock ? HoldLockSql : AppLockSql, tx ?? ownTx, timeout: 60);
            cmd.Parameters.Add("@k", SqlDbType.VarBinary, 32).Value = key;
            cmd.Parameters.Add("@states", SqlDbType.TinyInt).Value = Db.DefaultUniqueStates;
            cmd.Parameters.Add("@res", SqlDbType.NVarChar, 255).Value = "kopi_unique:" + Convert.ToHexString(key);
            var outcome = UniqueOutcome.Neither;
            await using (var r = await cmd.ExecuteReaderAsync())
            {
                do
                {
                    while (await r.ReadAsync())
                    {
                        outcome = r.GetBoolean(1) ? UniqueOutcome.Skipped : UniqueOutcome.Inserted;
                    }
                }
                while (await r.NextResultAsync());
            }
            ownTx?.Commit();
            return outcome;
        }
        finally
        {
            ownTx?.Dispose();
        }
    }

    private static byte[] NewKey() => SHA256.HashData(Guid.NewGuid().ToByteArray());

    /// <summary>
    /// <paramref name="tasks"/> concurrent inserters per round. hot: all use one key; otherwise each a distinct key.
    /// holdMs > 0: each insert runs in a caller transaction kept open that long before commit.
    /// </summary>
    public static async Task UniqueConcurrencyAsync(string db, UniqueStrategy s, bool hot, int tasks, int rounds, int holdMs, int prepopulate)
    {
        await Db.TruncateAsync(db);
        if (prepopulate > 0)
        {
            await using var p = await Db.OpenAsync(db);
            for (var i = 0; i < prepopulate; i++)
            {
                await InsertUniqueAsync(p, null, UniqueStrategy.HoldLock, NewKey());
            }
        }

        int inserted = 0, skipped = 0, neither = 0;
        var errors = new ConcurrentDictionary<string, int>();
        var latencies = new ConcurrentBag<double>();
        for (var round = 0; round < rounds; round++)
        {
            var hotKey = NewKey();
            var conns = await Task.WhenAll(Enumerable.Range(0, tasks).Select(_ => Db.OpenAsync(db)));
            var gate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            var work = conns.Select(c => Task.Run(async () =>
            {
                await gate.Task;
                var sw = Stopwatch.StartNew();
                SqlTransaction? tx = holdMs > 0 ? c.BeginTransaction(IsolationLevel.ReadCommitted) : null;
                try
                {
                    var o = await InsertUniqueAsync(c, tx, s, hot ? hotKey : NewKey());
                    if (tx is not null)
                    {
                        await Task.Delay(holdMs);
                        tx.Commit();
                    }
                    _ = o switch
                    {
                        UniqueOutcome.Inserted => Interlocked.Increment(ref inserted),
                        UniqueOutcome.Skipped => Interlocked.Increment(ref skipped),
                        _ => Interlocked.Increment(ref neither),
                    };
                }
                catch (Exception e)
                {
                    errors.AddOrUpdate(e is SqlException se ? $"SqlException {se.Number}" : e.GetType().Name, 1, (_, n) => n + 1);
                    try { tx?.Rollback(); } catch (InvalidOperationException) { /* already rolled back by the server */ }
                }
                finally
                {
                    tx?.Dispose();
                    latencies.Add(sw.Elapsed.TotalMilliseconds);
                }
            })).ToArray();
            gate.SetResult();
            await Task.WhenAll(work);
            foreach (var c in conns)
            {
                await c.DisposeAsync();
            }
        }

        await using var v = await Db.OpenAsync(db);
        var dupGroups = await Db.ScalarAsync<int>(v,
            "SELECT COUNT(*) FROM (SELECT unique_key FROM dbo.kopi_job WHERE unique_slot = 0 GROUP BY unique_key HAVING COUNT(*) > 1) d;");
        var attempts = tasks * rounds;
        var expectInserted = hot ? rounds : attempts;
        var pass = dupGroups == 0 && inserted + errors.Values.Sum() <= attempts && (hot ? inserted == rounds : true);
        Report.Line(
            $"{s} {(hot ? "hot key" : "distinct keys")}, {tasks}x{rounds}{(holdMs > 0 ? $", caller tx held {holdMs} ms" : ", autocommit")}{(prepopulate > 0 ? $", {prepopulate:N0} existing keys" : ", empty table")}",
            $"{(pass ? "PASS" : "FAIL")}: inserted={inserted} (expected {expectInserted}) skipped={skipped} neither={neither} " +
            $"errors={(errors.IsEmpty ? "0" : string.Join(", ", errors.Select(kv => $"{kv.Key} x{kv.Value}")))} " +
            $"duplicate-occupied-keys={dupGroups}; per-insert {Report.Dist([.. latencies])}");
    }

    /// <summary>A caller in SNAPSHOT isolation inserts a key that another transaction committed after the snapshot began.</summary>
    public static async Task SnapshotCallerAsync(string db, UniqueStrategy s, bool xactAbort, bool conflict)
    {
        await Db.TruncateAsync(db);
        var key = NewKey();
        await using var caller = await Db.OpenAsync(db);
        if (xactAbort)
        {
            await Db.ExecAsync(caller, "SET XACT_ABORT ON;");
        }
        var tx = caller.BeginTransaction(IsolationLevel.Snapshot);
        await Db.ScalarAsync<int>(caller, "SELECT COUNT(*) FROM dbo.kopi_job;", tx); // the snapshot starts here
        if (conflict)
        {
            await using var other = await Db.OpenAsync(db);
            await InsertUniqueAsync(other, null, UniqueStrategy.HoldLock, key);
        }

        string result;
        try
        {
            result = (await InsertUniqueAsync(caller, tx, s, key)).ToString();
        }
        catch (Exception e)
        {
            result = Report.Err(e);
        }

        string state;
        try
        {
            state = $"XACT_STATE={await Db.ScalarAsync<short>(caller, "SELECT CAST(XACT_STATE() AS smallint);", tx)}";
        }
        catch (Exception e)
        {
            state = $"tx unusable ({e.GetType().Name})";
        }

        string commit;
        try
        {
            tx.Commit();
            commit = "commit OK";
        }
        catch (Exception e)
        {
            commit = $"commit failed ({e.GetType().Name})";
        }

        Report.Line($"{s} SNAPSHOT caller, XACT_ABORT {(xactAbort ? "ON" : "OFF")}, {(conflict ? "key committed by another tx after snapshot start" : "new key, no conflict")}",
            $"{result}; {state}; {commit}");
    }

    /// <summary>A caller with XACT_ABORT ON (READ COMMITTED) inserts a duplicate: must be skipped, transaction intact.</summary>
    public static async Task XactAbortDuplicateAsync(string db, UniqueStrategy s)
    {
        await Db.TruncateAsync(db);
        var key = NewKey();
        await using var c = await Db.OpenAsync(db);
        await InsertUniqueAsync(c, null, UniqueStrategy.HoldLock, key);
        await Db.ExecAsync(c, "SET XACT_ABORT ON;");
        using var tx = c.BeginTransaction(IsolationLevel.ReadCommitted);
        var o = await InsertUniqueAsync(c, tx, s, key);
        var state = await Db.ScalarAsync<short>(c, "SELECT CAST(XACT_STATE() AS smallint);", tx);
        await Db.SeedAsync(c, 1, "available", "after", tx);
        tx.Commit();
        Report.Line($"{s} XACT_ABORT ON duplicate (READ COMMITTED caller)", $"{o}; XACT_STATE={state}; follow-up insert and commit OK");
    }

    // ---------------------------------------------------------------------------------------------------------

    private static DataTable JobsTable(int n, string queue)
    {
        var t = new DataTable();
        t.Columns.Add("state", typeof(string));
        t.Columns.Add("max_attempts", typeof(short));
        t.Columns.Add("scheduled_at", typeof(DateTime));
        t.Columns.Add("priority", typeof(short));
        t.Columns.Add("args", typeof(string));
        t.Columns.Add("kind", typeof(string));
        t.Columns.Add("queue", typeof(string));
        var now = DateTime.UtcNow;
        for (var i = 0; i < n; i++)
        {
            t.Rows.Add("available", (short)25, now, (short)1, $$"""{"row":{{i}},"email":"user{{i}}@example.com"}""", "bulk_spike", queue);
        }
        return t;
    }

    private static SqlBulkCopy BulkCopy(SqlConnection c, SqlTransaction tx, bool checkConstraints)
    {
        var bc = new SqlBulkCopy(c, checkConstraints ? SqlBulkCopyOptions.CheckConstraints : SqlBulkCopyOptions.Default, tx)
        {
            DestinationTableName = "dbo.kopi_job",
            BatchSize = 5_000,
            BulkCopyTimeout = 300,
        };
        foreach (var col in new[] { "state", "max_attempts", "scheduled_at", "priority", "args", "kind", "queue" })
        {
            bc.ColumnMappings.Add(col, col);
        }
        return bc;
    }

    /// <summary>Fetch from an unrelated queue on another connection while a bulk-copy transaction is open.</summary>
    private static async Task<string> ProbeFetchAsync(string db)
    {
        await using var p = await Db.OpenAsync(db);
        var sw = Stopwatch.StartNew();
        try
        {
            var got = await Db.FetchAsync(p, "default", 1, "probe", timeout: 3);
            return $"not blocked ({got.Count} row in {sw.ElapsedMilliseconds} ms)";
        }
        catch (SqlException e) when (e.Number == -2)
        {
            return $"BLOCKED (timed out after {sw.ElapsedMilliseconds} ms)";
        }
    }

    public static async Task BulkAsync(string db)
    {
        Report.Section($"0.10b SqlBulkCopy in a caller transaction: {db}");

        foreach (var (n, checkConstraints, disableEscalation) in new[]
                 {
                     (4_000, true, false), (50_000, true, false), (50_000, false, false), (50_000, true, true),
                 })
        {
            await Db.TruncateAsync(db);
            await Db.SeedAsync(db, 10, "available", "default");
            await using var c = await Db.OpenAsync(db);
            await Db.ExecAsync(c, $"ALTER TABLE dbo.kopi_job SET (LOCK_ESCALATION = {(disableEscalation ? "DISABLE" : "TABLE")});");
            var table = JobsTable(n, "bulk");

            // Rollback leaves nothing behind.
            var tx = c.BeginTransaction(IsolationLevel.ReadCommitted);
            using (var bc = BulkCopy(c, tx, checkConstraints))
            {
                await bc.WriteToServerAsync(table);
            }
            var inTx = await Db.ScalarAsync<int>(c, "SELECT COUNT(*) FROM dbo.kopi_job WHERE queue = N'bulk';", tx);
            tx.Rollback();
            var afterRollback = await Db.ScalarAsync<int>(c, "SELECT COUNT(*) FROM dbo.kopi_job WHERE queue = N'bulk';");

            // Commit path, with locks and a concurrent fetcher observed while the transaction is open.
            tx = c.BeginTransaction(IsolationLevel.ReadCommitted);
            var sw = Stopwatch.StartNew();
            using (var bc = BulkCopy(c, tx, checkConstraints))
            {
                await bc.WriteToServerAsync(table);
            }
            var copyMs = sw.Elapsed.TotalMilliseconds;
            var (locks, escalated) = await Db.LocksAsync(c, tx);
            var probe = await ProbeFetchAsync(db);
            tx.Commit();
            var committed = await Db.ScalarAsync<int>(c, "SELECT COUNT(*) FROM dbo.kopi_job WHERE queue = N'bulk';");
            var untrusted = await Db.ScalarAsync<int>(c, "SELECT COUNT(*) FROM sys.check_constraints WHERE parent_object_id = OBJECT_ID('dbo.kopi_job') AND is_not_trusted = 1;");
            await Db.ExecAsync(c, "ALTER TABLE dbo.kopi_job SET (LOCK_ESCALATION = TABLE);");
            if (untrusted > 0)
            {
                await Db.ExecAsync(c, "ALTER TABLE dbo.kopi_job WITH CHECK CHECK CONSTRAINT ALL;");
            }

            Report.Line($"{n:N0} rows, CheckConstraints={checkConstraints}, LOCK_ESCALATION={(disableEscalation ? "DISABLE" : "TABLE")}",
                $"in-tx count={inTx:N0}, after rollback={afterRollback}, after commit={committed:N0}; " +
                $"{n / (copyMs / 1000):N0} rows/s; locks: {(escalated ? "ESCALATED " : "")}{locks}; concurrent fetch on another queue: {probe}; " +
                $"untrusted CHECK constraints afterwards: {untrusted}");
        }

        // Comparison: batched INSERT ... SELECT FROM OPENJSON returning ids (InsertManyAsync path), one caller tx.
        {
            await Db.TruncateAsync(db);
            await using var c = await Db.OpenAsync(db);
            const int n = 50_000, batch = 1_000;
            var tx = c.BeginTransaction(IsolationLevel.ReadCommitted);
            var sw = Stopwatch.StartNew();
            var returned = 0;
            for (var start = 0; start < n; start += batch)
            {
                var json = "[" + string.Join(',', Enumerable.Range(start, batch).Select(i =>
                    $$"""{"args":"{\"row\":{{i}}}","kind":"bulk_spike","queue":"bulk"}""")) + "]";
                await using var cmd = Db.Cmd(c, """
                    INSERT INTO dbo.kopi_job (state, max_attempts, args, kind, queue)
                    OUTPUT inserted.id
                    SELECT 'available', 25, j.args, j.kind, j.queue
                    FROM OPENJSON(@rows) WITH (args nvarchar(max) '$.args', kind nvarchar(128) '$.kind', queue nvarchar(128) '$.queue') AS j;
                    """, tx);
                cmd.Parameters.Add("@rows", SqlDbType.NVarChar, -1).Value = json;
                await using var r = await cmd.ExecuteReaderAsync();
                while (await r.ReadAsync())
                {
                    returned++;
                }
            }
            var ms = sw.Elapsed.TotalMilliseconds;
            var (locks, escalated) = await Db.LocksAsync(c, tx);
            await Db.SeedAsync(db, 10, "available", "default");
            var probe = await ProbeFetchAsync(db);
            tx.Commit();
            Report.Line("OPENJSON batched insert, 50,000 rows / 1,000 per batch, ids returned",
                $"{returned:N0} ids; {n / (ms / 1000):N0} rows/s; locks at commit: {(escalated ? "ESCALATED " : "")}{locks}; " +
                $"concurrent fetch on another queue: {probe}");
        }
    }

    // ---------------------------------------------------------------------------------------------------------

    public static async Task PollAsync(string db)
    {
        Report.Section($"0.10c Poll cost and pickup latency: {db}");
        await Db.TruncateAsync(db);
        await Db.SeedAsync(db, 200_000, "completed", "default");
        await Db.SeedAsync(db, 50_000, "available", "busy");
        Report.Line("table", "200,000 completed (history) + 50,000 available in queue 'busy'; polling empty queue 'idle'");

        // Cost of an empty poll.
        await using (var c = await Db.OpenAsync(db))
        {
            var times = new List<double>(2_000);
            for (var i = 0; i < 2_000; i++)
            {
                var sw = Stopwatch.StartNew();
                await Db.FetchAsync(c, "idle", 25, "poll", tag: "/* poll-idle-spike */");
                times.Add(sw.Elapsed.TotalMilliseconds);
            }
            Report.Line("empty poll, client round trip", Report.Dist(times));
            Report.Line("empty poll, server", await QueryStatsAsync(c, "poll-idle-spike"));

            var busy = new List<double>(200);
            for (var i = 0; i < 200; i++)
            {
                var sw = Stopwatch.StartNew();
                await Db.FetchAsync(c, "busy", 25, "poll", tag: "/* poll-busy-spike */");
                busy.Add(sw.Elapsed.TotalMilliseconds);
            }
            Report.Line("fetch 25 from busy queue, client round trip", Report.Dist(busy));
            Report.Line("fetch 25 from busy queue, server", await QueryStatsAsync(c, "poll-busy-spike"));
        }

        // Insert-commit -> fetched latency for a poller with a fixed interval (no notifications, no LocalWaker).
        foreach (var (interval, samples) in new[] { (100, 40), (1_000, 15) })
        {
            var commits = new ConcurrentDictionary<long, long>();
            var latencies = new ConcurrentBag<double>();
            using var stop = new CancellationTokenSource();
            var poller = Task.Run(async () =>
            {
                await using var c = await Db.OpenAsync(db);
                while (!stop.IsCancellationRequested)
                {
                    foreach (var id in await Db.FetchAsync(c, "lat", 10, "poller"))
                    {
                        var now = Stopwatch.GetTimestamp();
                        while (!commits.ContainsKey(id))
                        {
                            await Task.Yield();
                        }
                        latencies.Add(Stopwatch.GetElapsedTime(commits[id], now).TotalMilliseconds);
                    }
                    try { await Task.Delay(interval, stop.Token); } catch (OperationCanceledException) { }
                }
            });
            await using (var ins = await Db.OpenAsync(db))
            {
                for (var i = 0; i < samples; i++)
                {
                    await Task.Delay(Random.Shared.Next(interval / 2, interval * 2));
                    var id = await Db.ScalarAsync<long>(ins, """
                        INSERT INTO dbo.kopi_job (state, max_attempts, args, kind, queue) OUTPUT inserted.id
                        VALUES ('available', 25, N'{}', N'lat', N'lat');
                        """);
                    commits[id] = Stopwatch.GetTimestamp(); // autocommit: committed when the call returns
                }
            }
            await Task.Delay(interval * 3);
            await stop.CancelAsync();
            await poller;
            Report.Line($"pickup latency, poll every {interval} ms", Report.Dist([.. latencies]));
        }
    }

    private static async Task<string> QueryStatsAsync(SqlConnection c, string tag)
    {
        await using var cmd = Db.Cmd(c, """
            SELECT TOP (1) qs.execution_count,
                   qs.total_logical_reads * 1.0 / qs.execution_count,
                   qs.total_worker_time * 1.0 / qs.execution_count,
                   CAST(qp.query_plan AS nvarchar(max))
            FROM sys.dm_exec_query_stats qs
            CROSS APPLY sys.dm_exec_sql_text(qs.sql_handle) st
            CROSS APPLY sys.dm_exec_query_plan(qs.plan_handle) qp
            WHERE st.text LIKE @pattern AND st.text NOT LIKE '%dm_exec_query_stats%'
            ORDER BY qs.execution_count DESC;
            """);
        cmd.Parameters.Add("@pattern", SqlDbType.NVarChar, 200).Value = $"%{tag}%";
        await using var r = await cmd.ExecuteReaderAsync();
        if (!await r.ReadAsync())
        {
            return "no query stats found";
        }
        var plan = r.GetString(3);
        var indexes = IndexRegex().Matches(plan).Select(m => m.Groups[1].Value).Distinct();
        var seeks = Regex.Matches(plan, "PhysicalOp=\"Index Seek\"").Count;
        var scans = Regex.Matches(plan, "PhysicalOp=\"(Clustered )?Index Scan\"").Count;
        return $"executions={r.GetInt64(0):N0}, avg logical reads={Convert.ToDouble(r.GetValue(1), System.Globalization.CultureInfo.InvariantCulture):F1}, " +
               $"avg CPU={Convert.ToDouble(r.GetValue(2), System.Globalization.CultureInfo.InvariantCulture):F0} µs, " +
               $"indexes used: {string.Join(", ", indexes)}; seeks={seeks} scans={scans}";
    }

    [GeneratedRegex("Index=\"\\[([^\\]]+)\\]\"")]
    private static partial Regex IndexRegex();
}
