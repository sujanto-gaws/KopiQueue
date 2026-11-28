#:package Microsoft.Data.SqlClient
#:property PublishAot=false
#:property TreatWarningsAsErrors=false
#:property EnforceCodeStyleInBuild=false
#:property AnalysisLevel=none
// Issue #158 probe: how SQL Server's SYSUTCDATETIME() behaves between statements, against the runner's clock.
// Temporary; removed before the pull request is merged.
using System.Diagnostics;
using Microsoft.Data.SqlClient;

var cs = Environment.GetEnvironmentVariable("KOPI_TEST_MSSQL")!;
for (var i = 0; ; i++)
{
    try { await using var c = new SqlConnection(cs); await c.OpenAsync(); break; }
    catch when (i < 60) { await Task.Delay(2000); }
}

await using (var c = new SqlConnection(cs))
{
    await c.OpenAsync();
    await using var info = new SqlCommand("SELECT @@VERSION, cpu_count, scheduler_count FROM sys.dm_os_sys_info", c);
    await using var r = await info.ExecuteReaderAsync();
    await r.ReadAsync();
    Console.WriteLine($"{r.GetString(0).Split('\n')[0].Trim()} | cpus {r.GetInt32(1)}, schedulers {r.GetInt32(2)}");
}

await Phase("idle, pooled connections, 50 ms apart", load: 0, TimeSpan.FromSeconds(30));
await Phase("8 busy sessions, pooled connections, 50 ms apart", load: 8, TimeSpan.FromSeconds(30));
await InBatch();

async Task Phase(string name, int load, TimeSpan length)
{
    using var stop = new CancellationTokenSource(length);
    var busy = Enumerable.Range(0, load).Select(_ => Task.Run(async () =>
    {
        while (!stop.IsCancellationRequested)
        {
            try
            {
                await using var c = new SqlConnection(cs);
                await c.OpenAsync();
                await using var cmd = new SqlCommand("SELECT COUNT_BIG(*) FROM sys.all_objects a CROSS JOIN sys.all_objects b", c) { CommandTimeout = 60 };
                await cmd.ExecuteScalarAsync();
            }
            catch (SqlException) { }
        }
    })).ToArray();

    var samples = new List<(DateTime Server, DateTime Client, double RoundTripMs)>();
    while (!stop.IsCancellationRequested)
    {
        await using var c = new SqlConnection(cs);
        await c.OpenAsync();
        await using var cmd = new SqlCommand("SELECT CAST(SYSUTCDATETIME() AS datetime2(7))", c);
        var before = DateTime.UtcNow;
        var sw = Stopwatch.StartNew();
        var server = (DateTime)(await cmd.ExecuteScalarAsync())!;
        sw.Stop();
        samples.Add((server, before + sw.Elapsed / 2, sw.Elapsed.TotalMilliseconds));
        await Task.Delay(50);
    }

    await Task.WhenAll(busy);

    var steps = samples.Zip(samples.Skip(1), (a, b) => ((b.Server - a.Server).TotalMilliseconds, (b.Client - a.Client).TotalMilliseconds)).ToList();
    var notForward = steps.Where(s => s.Item1 <= 0).ToList();
    var offsets = samples.Select(s => (s.Server - s.Client).TotalMilliseconds).ToList();
    var rtt = samples.Select(s => s.RoundTripMs).Order().ToList();
    var drift = steps.Select(s => s.Item1 - s.Item2).ToList();
    Console.WriteLine();
    Console.WriteLine($"## {name}: {samples.Count} samples");
    Console.WriteLine($"server step not forward: {notForward.Count}" + (notForward.Count > 0 ? $" (worst {notForward.Min(s => s.Item1):F3} ms; client steps then {string.Join(", ", notForward.Take(5).Select(s => $"{s.Item2:F1}"))} ms)" : ""));
    Console.WriteLine($"server step minus client step, ms: min {drift.Min():F2}, max {drift.Max():F2}");
    Console.WriteLine($"server minus client, ms: first {offsets[0]:F2}, last {offsets[^1]:F2}, min {offsets.Min():F2}, max {offsets.Max():F2}");
    Console.WriteLine($"round trip, ms: p50 {rtt[rtt.Count / 2]:F2}, p99 {rtt[(int)(rtt.Count * 0.99)]:F2}, max {rtt[^1]:F2}");
    Console.WriteLine($"distinct server ticks (resolution check): last 5 values {string.Join(" ", samples.TakeLast(5).Select(s => s.Server.ToString("HH:mm:ss.fffffff")))}");
}

async Task InBatch()
{
    await using var c = new SqlConnection(cs);
    await c.OpenAsync();
    var bad = 0;
    var deltas = new List<double>();
    for (var i = 0; i < 100; i++)
    {
        await using var cmd = new SqlCommand("DECLARE @a datetime2(7) = SYSUTCDATETIME(); WAITFOR DELAY '00:00:00.050'; SELECT DATEDIFF_BIG(microsecond, @a, SYSUTCDATETIME())", c);
        var us = (long)(await cmd.ExecuteScalarAsync())!;
        deltas.Add(us / 1000.0);
        if (us <= 0) bad++;
    }

    deltas.Sort();
    Console.WriteLine();
    Console.WriteLine($"## one batch, WAITFOR 50 ms between two SYSUTCDATETIME(): 100 samples");
    Console.WriteLine($"not forward: {bad}; elapsed ms min {deltas[0]:F2}, p50 {deltas[50]:F2}, max {deltas[^1]:F2}");
}
