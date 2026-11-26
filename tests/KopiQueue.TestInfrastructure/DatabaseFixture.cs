using Xunit;

namespace KopiQueue.TestInfrastructure;

/// <summary>
/// Creates a fresh, uniquely named database on a shared server for one test class, and drops it afterwards.
/// Use through <c>IClassFixture&lt;T&gt;</c> and call <see cref="SkipIfUnavailable"/> at the start of each test.
/// </summary>
public abstract class DatabaseFixture : IAsyncLifetime
{
    private TestServer? _server;

    /// <summary>Name of the database created for this fixture.</summary>
    public string DatabaseName { get; } = TestEnvironment.NewDatabaseName();

    /// <summary>Connection string for <see cref="DatabaseName"/>. Empty when the server is unavailable.</summary>
    public string ConnectionString { get; private set; } = string.Empty;

    /// <summary>Skips the calling test (locally) when no server could be found.</summary>
    public void SkipIfUnavailable()
    {
        if (_server is { IsAvailable: false })
        {
            Assert.Skip(_server.UnavailableReason!);
        }
    }

    protected abstract Task<TestServer> GetServerAsync();

    protected abstract Task CreateDatabaseAsync(string adminConnectionString, CancellationToken ct);

    protected abstract Task DropDatabaseAsync(string adminConnectionString, CancellationToken ct);

    protected abstract string BuildConnectionString(string adminConnectionString, string databaseName);

    public async ValueTask InitializeAsync()
    {
        _server = await GetServerAsync();
        if (!_server.IsAvailable)
        {
            return;
        }

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(60));
        await CreateDatabaseAsync(_server.AdminConnectionString!, cts.Token);
        ConnectionString = BuildConnectionString(_server.AdminConnectionString!, DatabaseName);
    }

    public async ValueTask DisposeAsync()
    {
        GC.SuppressFinalize(this);
        if (_server is { IsAvailable: true })
        {
            using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(60));
            await DropDatabaseAsync(_server.AdminConnectionString!, cts.Token);
        }
    }
}
