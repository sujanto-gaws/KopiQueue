using System.Diagnostics;
using System.Text.Json;
using System.Threading.Channels;
using Npgsql;

namespace KopiQueue.Npgsql.Tests.Compat;

/// <summary>
/// Runs <c>kopi-river-compat</c> (<c>tests/compat/go</c>), a real River v0.51.2 client driven through its public API.
/// Its path comes from <c>KOPI_RIVER_COMPAT</c>; tests that need it skip when that is unset, unless
/// <c>KOPI_REQUIRE_RIVER_CLI=1</c> (set in CI) turns the skip into a failure.
/// </summary>
/// <remarks>Clean-room (ADR-008): River is a black box here; the tests compare only what it writes to the database.</remarks>
internal sealed class RiverHarness
{
    public const string PathVariable = "KOPI_RIVER_COMPAT";

    private readonly string _path;
    private readonly string _databaseUrl;

    private RiverHarness(string path, string databaseUrl)
    {
        _path = path;
        _databaseUrl = databaseUrl;
    }

    /// <summary>Returns the harness for <paramref name="connectionString"/>'s database, or skips the test.</summary>
    public static RiverHarness GetOrSkip(string connectionString)
    {
        var path = Environment.GetEnvironmentVariable(PathVariable);
        if (string.IsNullOrEmpty(path) || !File.Exists(path))
        {
            var message = $"River harness not found: set {PathVariable} to a binary built from tests/compat/go.";
            if (Environment.GetEnvironmentVariable(Migrations.RiverCli.RequireVariable) == "1")
            {
                Assert.Fail(message);
            }

            Assert.Skip(message);
        }

        var cs = new NpgsqlConnectionStringBuilder(connectionString);
        var url = $"postgres://{Uri.EscapeDataString(cs.Username ?? "postgres")}:{Uri.EscapeDataString(cs.Password ?? string.Empty)}" +
                  $"@{cs.Host ?? "localhost"}:{cs.Port}/{Uri.EscapeDataString(cs.Database!)}?sslmode=disable";
        return new RiverHarness(path, url);
    }

    /// <summary>
    /// Inserts <paramref name="count"/> jobs with an insert-only River client and returns their IDs: one <c>Insert</c> each,
    /// or with <paramref name="many"/> one <c>InsertMany</c> (one transaction).
    /// </summary>
    public async Task<IReadOnlyList<long>> InsertAsync(
        string schema, string queue, string what, int count = 1, int? maxAttempts = null, bool many = false, CancellationToken cancellationToken = default)
    {
        string[] arguments = ["insert", "-schema", schema, "-queue", queue, "-what", what, "-count", Invariant(count), .. many ? ["-many"] : Array.Empty<string>()];
        if (maxAttempts is { } max)
        {
            arguments = [.. arguments, "-max-attempts", Invariant(max)];
        }

        await using var process = Start(arguments);
        var events = await process.WaitForExitAsync(TimeSpan.FromSeconds(30), cancellationToken);
        return [.. events.Where(e => e.Name == "inserted").Select(e => e.Id!.Value)];
    }

    /// <summary>Starts a River client working <paramref name="queue"/> and returns once it has started.</summary>
    public async Task<RiverProcess> StartWorkerAsync(
        string schema, string queue, int expect, TimeSpan? poll = null, TimeSpan? minRun = null, CancellationToken cancellationToken = default)
    {
        var process = Start([
            "work", "-schema", schema, "-queue", queue, "-expect", Invariant(expect),
            "-poll", Go(poll ?? TimeSpan.FromSeconds(1)), "-min-run", Go(minRun ?? TimeSpan.Zero), "-timeout", "60s",
        ]);
        await process.WaitForEventAsync("ready", TimeSpan.FromSeconds(30), cancellationToken);
        return process;
    }

    private static string Invariant(int value) => value.ToString(System.Globalization.CultureInfo.InvariantCulture);

    private static string Go(TimeSpan value) => Invariant((int)value.TotalMilliseconds) + "ms";

    private RiverProcess Start(string[] arguments)
    {
        var start = new ProcessStartInfo(_path) { RedirectStandardOutput = true, RedirectStandardError = true, UseShellExecute = false };
        foreach (var argument in arguments)
        {
            start.ArgumentList.Add(argument);
        }

        start.Environment["KOPI_COMPAT_PG"] = _databaseUrl;
        return new RiverProcess(Process.Start(start)!);
    }
}

/// <summary>One line of the harness's output.</summary>
internal sealed record RiverEvent(string Name, long? Id, string? What);

/// <summary>A running harness process and the events it has printed.</summary>
internal sealed class RiverProcess : IAsyncDisposable
{
    private readonly Process _process;
    private readonly Channel<RiverEvent> _events = Channel.CreateUnbounded<RiverEvent>();
    private readonly List<RiverEvent> _seen = [];
    private readonly Task _stdout;
    private readonly Task<string> _stderr;

    public RiverProcess(Process process)
    {
        _process = process;
        _stderr = process.StandardError.ReadToEndAsync();
        _stdout = Task.Run(async () =>
        {
            while (await process.StandardOutput.ReadLineAsync() is { } line)
            {
                using var json = JsonDocument.Parse(line);
                var root = json.RootElement;
                await _events.Writer.WriteAsync(new RiverEvent(
                    root.GetProperty("event").GetString()!,
                    root.TryGetProperty("id", out var id) ? id.GetInt64() : null,
                    root.TryGetProperty("what", out var what) ? what.GetString() : null));
            }

            _events.Writer.Complete();
        });
    }

    /// <summary>The jobs River's worker ran so far.</summary>
    public IReadOnlyList<RiverEvent> Worked => [.. _seen.Where(e => e.Name == "worked")];

    public async Task WaitForEventAsync(string name, TimeSpan limit, CancellationToken cancellationToken)
    {
        using var timeout = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        timeout.CancelAfter(limit);
        try
        {
            while (await _events.Reader.WaitToReadAsync(timeout.Token))
            {
                while (_events.Reader.TryRead(out var e))
                {
                    _seen.Add(e);
                    if (e.Name == name)
                    {
                        return;
                    }
                }
            }
        }
        catch (OperationCanceledException) when (!cancellationToken.IsCancellationRequested)
        {
            throw new TimeoutException($"River harness printed no '{name}' within {limit}: {await StderrAsync()}");
        }

        throw new InvalidOperationException($"River harness exited ({_process.ExitCode}) before '{name}': {await StderrAsync()}");
    }

    /// <summary>Waits for the process to exit successfully and returns everything it printed.</summary>
    public async Task<IReadOnlyList<RiverEvent>> WaitForExitAsync(TimeSpan limit, CancellationToken cancellationToken)
    {
        using var timeout = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        timeout.CancelAfter(limit);
        try
        {
            await _process.WaitForExitAsync(timeout.Token);
        }
        catch (OperationCanceledException) when (!cancellationToken.IsCancellationRequested)
        {
            throw new TimeoutException($"River harness did not exit within {limit}: {Describe()}");
        }

        await _stdout;
        while (_events.Reader.TryRead(out var e))
        {
            _seen.Add(e);
        }

        if (_process.ExitCode != 0)
        {
            throw new InvalidOperationException($"River harness exited with {_process.ExitCode}: {await StderrAsync()} {Describe()}");
        }

        return [.. _seen];
    }

    public async ValueTask DisposeAsync()
    {
        if (!_process.HasExited)
        {
            _process.Kill(entireProcessTree: true);
            await _process.WaitForExitAsync();
        }

        _process.Dispose();
    }

    private string Describe() => string.Join(", ", _seen.Select(e => $"{e.Name}{(e.Id is { } id ? $" {id}" : "")}"));

    private async Task<string> StderrAsync() => _process.HasExited ? (await _stderr).Trim() : "(still running)";
}
