// M0 spike 0.7: Npgsql LISTEN/NOTIFY behaviour for the KopiQueue notifier. Usage:
//   dotnet run --project spikes/NpgsqlSpikes -c Release -- [all|latency|tx|payload|names|pool|terminate|blackhole] [--keep]
// Connection: KOPI_SPIKE_PG, else KOPI_TEST_PG. Creates and drops the scratch database kopi_spike_notify.
using System.Diagnostics;
using System.Net;
using System.Net.Sockets;
using System.Threading.Channels;
using Npgsql;
using static Helpers;

var what = args.FirstOrDefault(a => !a.StartsWith("--", StringComparison.Ordinal)) ?? "all";
bool Run(string name) => what == "all" || what == name;

var baseCs = Environment.GetEnvironmentVariable("KOPI_SPIKE_PG") ?? Environment.GetEnvironmentVariable("KOPI_TEST_PG")
    ?? throw new InvalidOperationException("Set KOPI_SPIKE_PG or KOPI_TEST_PG.");
var adminCsb = new NpgsqlConnectionStringBuilder(baseCs) { Database = "postgres", Pooling = false };
await using (var admin = new NpgsqlConnection(adminCsb.ConnectionString))
{
    await admin.OpenAsync();
    await ExecAsync(admin, "DROP DATABASE IF EXISTS kopi_spike_notify WITH (FORCE)");
    await ExecAsync(admin, "CREATE DATABASE kopi_spike_notify");
    Console.WriteLine($"# Npgsql LISTEN/NOTIFY spike ({DateTime.UtcNow:yyyy-MM-dd HH:mm} UTC)\n");
    Console.WriteLine($"Server: {await ScalarAsync(admin, "SELECT version()")}. Npgsql {typeof(NpgsqlConnection).Assembly.GetName().Version}, .NET {Environment.Version}, same machine.\n");
}
var csb = new NpgsqlConnectionStringBuilder(baseCs) { Database = "kopi_spike_notify" };
var cs = csb.ConnectionString;
var adminDbCs = new NpgsqlConnectionStringBuilder(cs) { Pooling = false }.ConnectionString;
await using var ds = NpgsqlDataSource.Create(cs);
const string Ch = "public.river_insert";

try
{
    if (Run("latency"))
    {
        Console.WriteLine("## A. Commit-to-delivery latency and burst throughput\n");
        await using var l = await Listener.StartAsync(cs, Ch);
        await using var s = await ds.OpenConnectionAsync();
        var lat = new List<double>();
        for (var i = 0; i < 1000; i++)
        {
            await using var tx = await s.BeginTransactionAsync();
            await NotifyAsync(s, Ch, $"{{\"queue\":\"q\",\"i\":{i}}}", tx);
            var t0 = Stopwatch.GetTimestamp();
            await tx.CommitAsync();
            var (_, at) = await l.NextAsync(TimeSpan.FromSeconds(5)) ?? throw new TimeoutException("lost notification");
            lat.Add(Stopwatch.GetElapsedTime(t0, at).TotalMilliseconds);
        }
        Console.WriteLine($"- 1,000 sequential transactions, commit start → listener event: {Pcts(lat)}");

        const int senders = 8, per = 2_500;
        var start = Stopwatch.GetTimestamp();
        await Task.WhenAll(Enumerable.Range(0, senders).Select(async w =>
        {
            await using var c = await ds.OpenConnectionAsync();
            for (var i = 0; i < per; i++) await NotifyAsync(c, Ch, $"b{w}-{i}");
        }));
        var sent = Stopwatch.GetElapsedTime(start);
        var got = await l.DrainAsync(TimeSpan.FromSeconds(2));
        Console.WriteLine($"- Burst: {senders} senders × {per:N0} autocommit notifies = {senders * per:N0} sent in {sent.TotalSeconds:F2} s " +
                          $"({senders * per / sent.TotalSeconds:N0}/s); received {got.Count:N0}\n");
    }

    if (Run("tx"))
    {
        Console.WriteLine("## B. Transaction semantics and de-duplication\n");
        await using var l = await Listener.StartAsync(cs, Ch);
        await using var s = await ds.OpenConnectionAsync();
        await using (var tx = await s.BeginTransactionAsync())
        {
            for (var i = 0; i < 100; i++) await NotifyAsync(s, Ch, "{\"queue\":\"same\"}", tx);
            foreach (var q in new[] { "a", "b", "c" }) await NotifyAsync(s, Ch, $"{{\"queue\":\"{q}\"}}", tx);
            await NotifyAsync(s, "public.river_control", "{\"queue\":\"same\"}", tx); // other channel, same payload
            await tx.CommitAsync();
        }
        var got = await l.DrainAsync(TimeSpan.FromSeconds(1));
        Console.WriteLine($"- One transaction: 100 × identical payload + 3 distinct payloads on `{Ch}` → received **{got.Count}**: {string.Join(", ", got)}");
        await using (var tx = await s.BeginTransactionAsync())
        {
            await NotifyAsync(s, Ch, "{\"queue\":\"rolled_back\"}", tx);
            await tx.RollbackAsync();
        }
        Console.WriteLine($"- Notify then rollback → received {(await l.DrainAsync(TimeSpan.FromSeconds(1))).Count}");
        await using (var tx = await s.BeginTransactionAsync())
        {
            await NotifyAsync(s, Ch, "{\"queue\":\"sp\"}", tx);
            await ExecAsync(s, "SAVEPOINT x", tx);
            await NotifyAsync(s, Ch, "{\"queue\":\"sp_rolled_back\"}", tx);
            await ExecAsync(s, "ROLLBACK TO SAVEPOINT x", tx);
            await tx.CommitAsync();
        }
        Console.WriteLine($"- Notify, savepoint, notify, rollback to savepoint, commit → received: {string.Join(", ", await l.DrainAsync(TimeSpan.FromSeconds(1)))}");
        for (var i = 0; i < 2; i++) await NotifyAsync(s, Ch, "{\"queue\":\"two_tx\"}");
        Console.WriteLine($"- Same payload in two separate transactions → received {(await l.DrainAsync(TimeSpan.FromSeconds(1))).Count}\n");
    }

    if (Run("payload"))
    {
        Console.WriteLine("## C. Payload size limit\n");
        await using var s = await ds.OpenConnectionAsync();
        foreach (var (label, payload) in new[] { ("7,999 ASCII bytes", new string('x', 7999)), ("8,000 ASCII bytes", new string('x', 8000)),
                     ("2,700 × '☕' (8,100 UTF-8 bytes)", new string('☕', 2700)) })
        {
            try { await NotifyAsync(s, Ch, payload); Console.WriteLine($"- {label}: OK"); }
            catch (PostgresException ex) { Console.WriteLine($"- {label}: **{ex.SqlState}** {ex.MessageText}"); }
        }
        Console.WriteLine();
    }

    if (Run("names"))
    {
        Console.WriteLine("## D. Channel names\n");
        await using var c = await ds.OpenConnectionAsync();
        var notices = new List<string>();
        c.Notice += (_, e) => notices.Add(e.Notice.MessageText);
        foreach (var sql in new[] { "LISTEN public.river_insert", "LISTEN \"public.river_insert\"", $"LISTEN \"{new string('s', 52)}.river_insert\"" })
        {
            notices.Clear();
            try { await ExecAsync(c, sql); Console.WriteLine($"- `{(sql.Length > 60 ? sql[..60] + "…" : sql)}`: OK {string.Join("; ", notices)}"); }
            catch (PostgresException ex) { Console.WriteLine($"- `{sql}`: **{ex.SqlState}** {ex.MessageText}"); }
        }
        Console.WriteLine($"- pg_listening_channels(): {string.Join(", ", await ListAsync(c, "SELECT pg_listening_channels()"))}");
        try { await NotifyAsync(c, new string('s', 52) + ".river_insert", "{}"); Console.WriteLine("- pg_notify with a 65-byte channel name: OK"); }
        catch (PostgresException ex) { Console.WriteLine($"- pg_notify with a 65-byte channel name: **{ex.SqlState}** {ex.MessageText}"); }
        await ExecAsync(c, "UNLISTEN *");
        Console.WriteLine();
    }

    if (Run("pool"))
    {
        Console.WriteLine("## E. LISTEN on a pooled connection\n");
        await using var pooled = NpgsqlDataSource.Create(new NpgsqlConnectionStringBuilder(cs) { MaxPoolSize = 1 }.ConnectionString);
        int pid1;
        await using (var c = await pooled.OpenConnectionAsync())
        {
            pid1 = c.ProcessID;
            await ExecAsync(c, $"LISTEN {Q(Ch)}");
        }
        await using (var c = await pooled.OpenConnectionAsync())
        {
            var channels = await ListAsync(c, "SELECT pg_listening_channels()");
            var count = 0;
            c.Notification += (_, _) => count++;
            await using (var other = await ds.OpenConnectionAsync()) await NotifyAsync(other, Ch, "{}");
            await c.WaitAsync(TimeSpan.FromSeconds(1));
            Console.WriteLine($"- Same physical connection after return to pool: {c.ProcessID == pid1}; listening channels after reopen: [{string.Join(", ", channels)}]; notifications received: {count}");
        }
        Console.WriteLine("  → The pool reset (DISCARD ALL) silently drops LISTEN. The notifier must own a dedicated, non-pooled connection.\n");
    }

    if (Run("terminate"))
    {
        Console.WriteLine("## F. Server-side disconnect (pg_terminate_backend)\n");
        for (var round = 1; round <= 3; round++)
        {
            var l = await Listener.StartAsync(cs, Ch);
            await Task.Delay(200);
            await using (var admin = new NpgsqlConnection(adminDbCs))
            {
                await admin.OpenAsync();
                var t0 = Stopwatch.GetTimestamp();
                await ExecAsync(admin, $"SELECT pg_terminate_backend({l.Pid})");
                await Task.WhenAny(l.Loop, Task.Delay(10_000));
                var detect = l.FailedAt == 0 ? "not detected in 10 s" : $"{Stopwatch.GetElapsedTime(t0, l.FailedAt).TotalMilliseconds:F1} ms";
                var err = l.Error is PostgresException pe ? $"PostgresException {pe.SqlState}" : l.Error?.GetType().Name ?? "none";
                Console.WriteLine($"- Round {round}: detected after {detect}; error: {err}; connection state {l.Conn.FullState}");

                for (var i = 0; i < 5; i++) await NotifyAsync(admin, Ch, $"{{\"while_down\":{i}}}");
                var r0 = Stopwatch.GetTimestamp();
                await using var l2 = await Listener.StartAsync(cs, Ch);
                var reconnect = Stopwatch.GetElapsedTime(r0).TotalMilliseconds;
                var missed = await l2.DrainAsync(TimeSpan.FromSeconds(1));
                Console.WriteLine($"  Reconnect + LISTEN: {reconnect:F1} ms. Of 5 notifications sent while disconnected, received after reconnect: {missed.Count}");
            }
            await l.DisposeAsync();
        }
        Console.WriteLine();
    }

    if (Run("blackhole"))
    {
        Console.WriteLine("## G. Silent connection loss (TCP proxy that stops forwarding)\n");
        var scenarios = new (string Name, string Extra, TimeSpan? Probe, ProbeMode Mode)[]
        {
            ("Plain WaitAsync, no keepalive", "", null, default),
            ("Npgsql Keepalive=2", "Keepalive=2", null, default),
            ("Npgsql Keepalive=10", "Keepalive=10", null, default),
            ("WaitAsync(2 s) + `SELECT 1` probe, Command Timeout=2", "Command Timeout=2;Cancellation Timeout=1000", TimeSpan.FromSeconds(2), ProbeMode.WaitThenSelect),
            ("Delay(2 s) + `SELECT 1` probe (no WaitAsync), Command Timeout=2", "Command Timeout=2;Cancellation Timeout=1000", TimeSpan.FromSeconds(2), ProbeMode.DelayThenSelect),
            ("WaitAsync(2 s) + `SELECT 1` probe + 3 s external watchdog", "", TimeSpan.FromSeconds(2), ProbeMode.WaitThenSelectWithWatchdog),
        };
        foreach (var (name, extra, probe, mode) in scenarios)
        {
            Listener.Mode = mode;
            await using var proxy = new BlackholeProxy(csb.Host!, csb.Port);
            var pcs = new NpgsqlConnectionStringBuilder(cs + (extra.Length > 0 ? ";" + extra : "")) { Host = "127.0.0.1", Port = proxy.Port, Pooling = false }.ConnectionString;
            var l = await Listener.StartAsync(pcs, Ch, probe);
            await using (var s = await ds.OpenConnectionAsync()) await NotifyAsync(s, Ch, "{\"before\":1}");
            var before = await l.DrainAsync(TimeSpan.FromMilliseconds(500));
            proxy.Blackholed = true;
            var t0 = Stopwatch.GetTimestamp();
            await Task.WhenAny(l.Loop, Task.Delay(40_000));
            var detect = l.FailedAt == 0 ? "**not detected in 40 s**" : $"{Stopwatch.GetElapsedTime(t0, l.FailedAt).TotalSeconds:F1} s";
            var err = l.Error switch { null => "", PostgresException pe => $" ({pe.SqlState})", NpgsqlException ne => $" (NpgsqlException: {ne.InnerException?.GetType().Name ?? ne.Message})", var e => $" ({e.GetType().Name})" };
            var t1 = Stopwatch.GetTimestamp();
            Console.WriteLine($"- {name}: notification before blackhole received={before.Count == 1}; failure detected: {detect}{err}");
            proxy.Blackholed = false;
            await l.DisposeAsync();
            Trace($"dispose took {Stopwatch.GetElapsedTime(t1).TotalSeconds:F1} s");
        }
        Console.WriteLine();
    }
}
finally
{
    NpgsqlConnection.ClearAllPools();
    if (!args.Contains("--keep"))
    {
        await using var admin = new NpgsqlConnection(adminCsb.ConnectionString);
        await admin.OpenAsync();
        await ExecAsync(admin, "DROP DATABASE IF EXISTS kopi_spike_notify WITH (FORCE)");
    }
}

static class Helpers
{
    public static void Trace(string m) { if (Environment.GetEnvironmentVariable("SPIKE_TRACE") == "1") Console.WriteLine($"    [trace {DateTime.Now:HH:mm:ss.f}] {m}"); }

    public static string Q(string ident) => "\"" + ident.Replace("\"", "\"\"", StringComparison.Ordinal) + "\"";

    public static async Task ExecAsync(NpgsqlConnection c, string sql, NpgsqlTransaction? tx = null)
    {
        await using var cmd = new NpgsqlCommand(sql, c, tx);
        await cmd.ExecuteNonQueryAsync();
    }

    public static async Task<object?> ScalarAsync(NpgsqlConnection c, string sql)
    {
        await using var cmd = new NpgsqlCommand(sql, c);
        return await cmd.ExecuteScalarAsync();
    }

    public static async Task<List<string>> ListAsync(NpgsqlConnection c, string sql)
    {
        await using var cmd = new NpgsqlCommand(sql, c);
        await using var r = await cmd.ExecuteReaderAsync();
        var list = new List<string>();
        while (await r.ReadAsync()) list.Add(r.GetString(0));
        return list;
    }

    public static async Task NotifyAsync(NpgsqlConnection c, string channel, string payload, NpgsqlTransaction? tx = null)
    {
        await using var cmd = new NpgsqlCommand("SELECT pg_notify($1, $2)", c, tx) { Parameters = { new() { Value = channel }, new() { Value = payload } } };
        await cmd.ExecuteNonQueryAsync();
    }

    public static string Pcts(List<double> v)
    {
        v.Sort();
        double P(double p) => v[Math.Min(v.Count - 1, (int)Math.Ceiling(p * v.Count) - 1)];
        return $"p50 {P(0.50):F2} ms, p95 {P(0.95):F2} ms, p99 {P(0.99):F2} ms, max {v[^1]:F2} ms";
    }
}

/// <summary>A dedicated LISTEN connection with a WaitAsync loop, as the KQ notifier would run it.</summary>
enum ProbeMode { WaitThenSelect, DelayThenSelect, WaitThenSelectWithWatchdog }

sealed class Listener : IAsyncDisposable
{
    public static ProbeMode Mode = ProbeMode.WaitThenSelect;
    private readonly CancellationTokenSource _cts = new();
    private readonly Channel<(string Payload, long At)> _got = Channel.CreateUnbounded<(string, long)>();
    public NpgsqlConnection Conn { get; private set; } = null!;
    public int Pid { get; private set; }
    public Task Loop { get; private set; } = Task.CompletedTask;
    public Exception? Error { get; private set; }
    public long FailedAt { get; private set; }

    public static async Task<Listener> StartAsync(string cs, string channel, TimeSpan? probe = null)
    {
        var l = new Listener { Conn = new NpgsqlConnection(cs) };
        await l.Conn.OpenAsync();
        l.Pid = l.Conn.ProcessID;
        l.Conn.Notification += (_, e) => l._got.Writer.TryWrite((e.Payload, Stopwatch.GetTimestamp()));
        await ExecAsync(l.Conn, $"LISTEN {Q(channel)}");
        l.Loop = Task.Run(async () =>
        {
            try
            {
                while (!l._cts.IsCancellationRequested)
                {
                    if (probe is null)
                    {
                        await l.Conn.WaitAsync(l._cts.Token);
                    }
                    else
                    {
                        if (Mode == ProbeMode.DelayThenSelect)
                        {
                            await Task.Delay(probe.Value, l._cts.Token);
                        }
                        else if (await l.Conn.WaitAsync(probe.Value, l._cts.Token))
                        {
                            continue;
                        }
                        Trace("probing");
                        var task = ScalarAsync(l.Conn, "SELECT 1"); // health probe; Command Timeout should bound it
                        if (Mode == ProbeMode.WaitThenSelectWithWatchdog)
                        {
                            await task.WaitAsync(TimeSpan.FromSeconds(3), l._cts.Token); // TimeoutException → connection is dead
                        }
                        else
                        {
                            await task;
                        }
                        Trace("probe OK");
                    }
                }
            }
            catch (Exception ex) when (!l._cts.IsCancellationRequested)
            {
                l.FailedAt = Stopwatch.GetTimestamp();
                l.Error = ex;
            }
            catch (OperationCanceledException) { }
        });
        return l;
    }

    public async Task<(string Payload, long At)?> NextAsync(TimeSpan timeout)
    {
        using var cts = new CancellationTokenSource(timeout);
        try { return await _got.Reader.ReadAsync(cts.Token); }
        catch (OperationCanceledException) { return null; }
    }

    public async Task<List<string>> DrainAsync(TimeSpan quiet)
    {
        var list = new List<string>();
        while (await NextAsync(quiet) is { } n) list.Add(n.Payload);
        return list;
    }

    public async ValueTask DisposeAsync()
    {
        await _cts.CancelAsync();
        try { await Loop.WaitAsync(TimeSpan.FromSeconds(5)); } catch { }
        try { await Conn.DisposeAsync().AsTask().WaitAsync(TimeSpan.FromSeconds(5)); } catch (TimeoutException) { Trace("Conn.DisposeAsync hung > 5 s; abandoned"); } catch { }
    }
}

/// <summary>Loopback TCP proxy. When <see cref="Blackholed"/> is set it keeps sockets open but stops forwarding,
/// like a hung load balancer or a NAT entry that silently expired.</summary>
sealed class BlackholeProxy : IAsyncDisposable
{
    private readonly TcpListener _listener = new(IPAddress.Loopback, 0);
    private readonly CancellationTokenSource _cts = new();
    private readonly List<TcpClient> _sockets = [];
    public volatile bool Blackholed;
    public int Port => ((IPEndPoint)_listener.LocalEndpoint).Port;

    public BlackholeProxy(string host, int port)
    {
        _listener.Start();
        _ = Task.Run(async () =>
        {
            while (!_cts.IsCancellationRequested)
            {
                var client = await _listener.AcceptTcpClientAsync(_cts.Token);
                var upstream = new TcpClient();
                await upstream.ConnectAsync(host, port, _cts.Token);
                lock (_sockets) _sockets.AddRange([client, upstream]);
                _ = PumpAsync(client.GetStream(), upstream.GetStream());
                _ = PumpAsync(upstream.GetStream(), client.GetStream());
            }
        });
    }

    private async Task PumpAsync(NetworkStream from, NetworkStream to)
    {
        var buf = new byte[64 * 1024];
        try
        {
            while (true)
            {
                var n = await from.ReadAsync(buf, _cts.Token);
                if (n == 0) break;
                while (Blackholed) await Task.Delay(50, _cts.Token);
                await to.WriteAsync(buf.AsMemory(0, n), _cts.Token);
            }
        }
        catch { }
    }

    public ValueTask DisposeAsync()
    {
        _cts.Cancel();
        _listener.Stop();
        lock (_sockets) foreach (var s in _sockets) s.Dispose();
        return ValueTask.CompletedTask;
    }
}
