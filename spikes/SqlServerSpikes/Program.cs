// M0 spikes 0.9 and 0.10 for the SQL Server driver. Usage:
//   dotnet run --project spikes/SqlServerSpikes -c Release -- [all|fetch|isolation|escalation|unique|bulk|poll] [--keep]
// Connection: KOPI_SPIKE_MSSQL (admin connection string); defaults to localhost with Windows auth.
using SqlServerSpikes;

var what = args.FirstOrDefault(a => !a.StartsWith("--", StringComparison.Ordinal)) ?? "all";
var keep = args.Contains("--keep");
bool Run(string name) => what == "all" || what == name;

await using (var m = new Microsoft.Data.SqlClient.SqlConnection(Db.Server))
{
    await m.OpenAsync();
    await using var cmd = Db.Cmd(m, """
        SELECT CAST(SERVERPROPERTY('ProductVersion') AS nvarchar(32)), CAST(SERVERPROPERTY('Edition') AS nvarchar(64)),
               (SELECT cpu_count FROM sys.dm_os_sys_info), (SELECT physical_memory_kb / 1024 FROM sys.dm_os_sys_info)
        """);
    await using var r = await cmd.ExecuteReaderAsync();
    await r.ReadAsync();
    Console.WriteLine($"# SQL Server spike results ({DateTime.UtcNow:yyyy-MM-dd HH:mm} UTC)\n");
    Console.WriteLine($"Server {r.GetString(0)} {r.GetString(1)}, {r.GetInt32(2)} logical CPUs, {r.GetInt64(3):N0} MB RAM. " +
                      $"Client .NET {Environment.Version}, same machine.");
}

await Db.CreateDatabaseAsync(Db.RcDb, rcsi: false);
await Db.CreateDatabaseAsync(Db.RcsiDb, rcsi: true);
string[] dbs = [Db.RcDb, Db.RcsiDb];
try
{
    if (Run("fetch"))
    {
        foreach (var db in dbs)
        {
            await Spike09.ConcurrentFetchAsync(db);
        }
        await Spike09.ConcurrentFetchAsync(Db.RcsiDb, jobs: 20_000, fetchers: 64, completers: 4, limit: 5);
    }

    if (Run("isolation"))
    {
        foreach (var db in dbs)
        {
            await Spike09.IsolationAsync(db);
        }
    }

    if (Run("escalation"))
    {
        await Spike09.EscalationAsync(Db.RcDb);
    }

    if (Run("plan"))
    {
        await Spike09.FetchPlanAsync(Db.RcDb);
        await Spike09.FetchPlanAsync(Db.RcDb, "INDEX(kopi_job_fetch_idx)");
        await Spike09.FetchPlanAsync(Db.RcDb, "FORCESEEK(kopi_job_fetch_idx(queue))");
    }

    if (Run("unique"))
    {
        foreach (var db in dbs)
        {
            Report.Section($"0.10a Unique inserts under concurrency: {db}");
            foreach (var s in new[] { UniqueStrategy.HoldLock, UniqueStrategy.AppLock })
            {
                await Spike10.UniqueConcurrencyAsync(db, s, hot: true, tasks: 100, rounds: 10, holdMs: 0, prepopulate: 0);
                await Spike10.UniqueConcurrencyAsync(db, s, hot: false, tasks: 100, rounds: 10, holdMs: 0, prepopulate: 0);
                await Spike10.UniqueConcurrencyAsync(db, s, hot: true, tasks: 20, rounds: 3, holdMs: 50, prepopulate: 0);
                await Spike10.UniqueConcurrencyAsync(db, s, hot: false, tasks: 50, rounds: 3, holdMs: 50, prepopulate: 0);
                await Spike10.UniqueConcurrencyAsync(db, s, hot: false, tasks: 50, rounds: 3, holdMs: 50, prepopulate: 20_000);
                await Spike10.XactAbortDuplicateAsync(db, s);
                foreach (var xactAbort in new[] { false, true })
                {
                    await Spike10.SnapshotCallerAsync(db, s, xactAbort, conflict: false);
                    await Spike10.SnapshotCallerAsync(db, s, xactAbort, conflict: true);
                }
            }
        }
    }

    if (what == "snapshot")
    {
        foreach (var db in dbs)
        {
            Report.Section($"0.10a SNAPSHOT callers (fallback SELECT with READCOMMITTEDLOCK): {db}");
            foreach (var s in new[] { UniqueStrategy.HoldLock, UniqueStrategy.AppLock })
            {
                foreach (var xactAbort in new[] { false, true })
                {
                    await Spike10.SnapshotCallerAsync(db, s, xactAbort, conflict: true);
                }
            }
        }
    }

    if (Run("bulk"))
    {
        await Spike10.BulkAsync(Db.RcDb);
    }

    if (Run("poll"))
    {
        await Spike10.PollAsync(Db.RcDb);
    }
}
finally
{
    if (!keep)
    {
        foreach (var db in dbs)
        {
            await Db.DropDatabaseAsync(db);
        }
    }
}
