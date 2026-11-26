using System.Diagnostics;
using Npgsql;

namespace KopiQueue.Npgsql.Tests.Migrations;

/// <summary>
/// Runs River's own CLI (<c>river</c>, built from <c>github.com/riverqueue/river/cmd/river@v0.51.2</c>) as a black box.
/// Its path comes from <c>KOPI_RIVER_CLI</c>; tests that need it skip when that is unset, unless
/// <c>KOPI_REQUIRE_RIVER_CLI=1</c> (set in CI) turns the skip into a failure.
/// </summary>
/// <remarks>Clean-room (C2): only <c>migrate-up</c>, <c>migrate-down</c> and <c>validate</c> are used, never commands that print River's SQL.</remarks>
internal sealed class RiverCli
{
    public const string PathVariable = "KOPI_RIVER_CLI";
    public const string RequireVariable = "KOPI_REQUIRE_RIVER_CLI";

    /// <summary>The River version the goldens and the main line are pinned to (design §4.1).</summary>
    public const string PinnedVersion = "v0.51.2";

    private readonly string _path;
    private readonly string _databaseUrl;

    private RiverCli(string path, string databaseUrl)
    {
        _path = path;
        _databaseUrl = databaseUrl;
    }

    /// <summary>Returns the CLI for <paramref name="connectionString"/>'s database, or skips the test.</summary>
    public static RiverCli GetOrSkip(string connectionString)
    {
        var path = Environment.GetEnvironmentVariable(PathVariable);
        if (string.IsNullOrEmpty(path) || !File.Exists(path))
        {
            var message = $"River CLI not found: set {PathVariable} to a river binary built from {PinnedVersion}.";
            if (Environment.GetEnvironmentVariable(RequireVariable) == "1")
            {
                Assert.Fail(message);
            }

            Assert.Skip(message);
        }

        var cs = new NpgsqlConnectionStringBuilder(connectionString);
        var url = $"postgres://{Uri.EscapeDataString(cs.Username ?? "postgres")}:{Uri.EscapeDataString(cs.Password ?? string.Empty)}" +
                  $"@{cs.Host ?? "localhost"}:{cs.Port}/{Uri.EscapeDataString(cs.Database!)}?sslmode=disable";
        return new RiverCli(path, url);
    }

    public Task MigrateUpAsync(string schema, int? targetVersion = null, CancellationToken cancellationToken = default) =>
        RunAsync(["migrate-up", "--schema", schema, .. Target(targetVersion)], cancellationToken);

    public Task MigrateDownAsync(string schema, int targetVersion, CancellationToken cancellationToken = default) =>
        RunAsync(["migrate-down", "--schema", schema, .. Target(targetVersion)], cancellationToken);

    /// <summary>Runs <c>river validate</c>: succeeds only when no migrations are outstanding.</summary>
    public Task ValidateAsync(string schema, CancellationToken cancellationToken = default) =>
        RunAsync(["validate", "--schema", schema], cancellationToken);

    private static string[] Target(int? version) =>
        version is { } v ? ["--target-version", v.ToString(System.Globalization.CultureInfo.InvariantCulture)] : [];

    private async Task RunAsync(string[] arguments, CancellationToken cancellationToken)
    {
        var start = new ProcessStartInfo(_path) { RedirectStandardOutput = true, RedirectStandardError = true, UseShellExecute = false };
        foreach (var argument in arguments)
        {
            start.ArgumentList.Add(argument);
        }

        start.ArgumentList.Add("--database-url");
        start.ArgumentList.Add(_databaseUrl);

        using var process = Process.Start(start)!;
        var stdout = process.StandardOutput.ReadToEndAsync(cancellationToken);
        var stderr = process.StandardError.ReadToEndAsync(cancellationToken);
        await process.WaitForExitAsync(cancellationToken);
        if (process.ExitCode != 0)
        {
            throw new InvalidOperationException(
                $"river {arguments[0]} exited with {process.ExitCode}: {(await stderr).Trim()} {(await stdout).Trim()}");
        }
    }
}
