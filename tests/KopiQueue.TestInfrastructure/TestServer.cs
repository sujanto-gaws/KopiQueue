namespace KopiQueue.TestInfrastructure;

/// <summary>
/// The result of locating a database server for a test run: either an admin connection string,
/// or the reason no server is available (tests are then skipped locally, and fail in CI).
/// </summary>
public sealed record TestServer(string? AdminConnectionString, string? UnavailableReason)
{
    public bool IsAvailable => AdminConnectionString is not null;

    public static TestServer Available(string adminConnectionString) => new(adminConnectionString, null);

    public static TestServer Unavailable(string reason) => new(null, reason);
}

/// <summary>Helpers shared by the database fixtures.</summary>
public static class TestEnvironment
{
    /// <summary>True on CI runners (GitHub Actions sets CI=true). Missing databases fail instead of skipping.</summary>
    public static bool IsCI =>
        string.Equals(Environment.GetEnvironmentVariable("CI"), "true", StringComparison.OrdinalIgnoreCase);

    /// <summary>A unique, identifier-safe database name: lowercase letters, digits and underscores only.</summary>
    public static string NewDatabaseName() =>
        $"kopi_test_{DateTime.UtcNow:yyyyMMddHHmmss}_{Guid.NewGuid().ToString("N")[..8]}";

    /// <summary>
    /// Locates a server: the connection string in <paramref name="environmentVariable"/> if set,
    /// otherwise a container started by <paramref name="startContainer"/>.
    /// </summary>
    public static async Task<TestServer> ResolveAsync(
        string environmentVariable,
        Func<Task<string>> startContainer,
        Func<string, CancellationToken, Task> probe)
    {
        var adminConnectionString = Environment.GetEnvironmentVariable(environmentVariable);
        if (string.IsNullOrWhiteSpace(adminConnectionString))
        {
            try
            {
                adminConnectionString = await startContainer();
            }
            catch (Exception ex) when (!IsCI)
            {
                return TestServer.Unavailable(
                    $"{environmentVariable} is not set and a container could not be started " +
                    $"(is Docker running?): {ex.GetType().Name}: {ex.Message}");
            }
        }

        await WaitUntilReachableAsync(adminConnectionString, probe, TimeSpan.FromSeconds(90));
        return TestServer.Available(adminConnectionString);
    }

    // CI service containers can report "started" before the server accepts connections.
    private static async Task WaitUntilReachableAsync(
        string connectionString, Func<string, CancellationToken, Task> probe, TimeSpan timeout)
    {
        using var cts = new CancellationTokenSource(timeout);
        while (true)
        {
            try
            {
                await probe(connectionString, cts.Token);
                return;
            }
            catch (Exception) when (!cts.IsCancellationRequested)
            {
                await Task.Delay(TimeSpan.FromSeconds(1), cts.Token);
            }
        }
    }
}
