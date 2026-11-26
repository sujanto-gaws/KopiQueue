using Npgsql;
using Testcontainers.PostgreSql;

namespace KopiQueue.TestInfrastructure;

/// <summary>
/// A fresh PostgreSQL database per test class.
/// Server: <c>KOPI_TEST_PG</c> (an admin connection string), otherwise a Testcontainers <c>postgres</c> container
/// (image overridable with <c>KOPI_TEST_PG_IMAGE</c>), started once per test run.
/// </summary>
public sealed class PostgresFixture : DatabaseFixture
{
    public const string EnvironmentVariable = "KOPI_TEST_PG";
    public const string ImageEnvironmentVariable = "KOPI_TEST_PG_IMAGE";
    public const string DefaultImage = "postgres:18";

    private static readonly Lazy<Task<TestServer>> SharedServer = new(() =>
        TestEnvironment.ResolveAsync(EnvironmentVariable, StartContainerAsync, ProbeAsync));

    private static PostgreSqlContainer? _container;

    /// <summary>Creates a data source for this fixture's database. The caller disposes it.</summary>
    public NpgsqlDataSource CreateDataSource() => NpgsqlDataSource.Create(ConnectionString);

    protected override Task<TestServer> GetServerAsync() => SharedServer.Value;

    protected override async Task CreateDatabaseAsync(string adminConnectionString, CancellationToken ct)
    {
        await using var conn = new NpgsqlConnection(adminConnectionString);
        await conn.OpenAsync(ct);
        await using var cmd = new NpgsqlCommand($"CREATE DATABASE \"{DatabaseName}\"", conn);
        await cmd.ExecuteNonQueryAsync(ct);
    }

    protected override async Task DropDatabaseAsync(string adminConnectionString, CancellationToken ct)
    {
        NpgsqlConnection.ClearAllPools();
        await using var conn = new NpgsqlConnection(adminConnectionString);
        await conn.OpenAsync(ct);
        await using var cmd = new NpgsqlCommand($"DROP DATABASE IF EXISTS \"{DatabaseName}\" WITH (FORCE)", conn);
        await cmd.ExecuteNonQueryAsync(ct);
    }

    protected override string BuildConnectionString(string adminConnectionString, string databaseName) =>
        new NpgsqlConnectionStringBuilder(adminConnectionString) { Database = databaseName }.ConnectionString;

    private static async Task<string> StartContainerAsync()
    {
        var image = Environment.GetEnvironmentVariable(ImageEnvironmentVariable) ?? DefaultImage;
        _container = new PostgreSqlBuilder(image).Build();
        await _container.StartAsync();
        return _container.GetConnectionString();
    }

    private static async Task ProbeAsync(string connectionString, CancellationToken ct)
    {
        await using var conn = new NpgsqlConnection(connectionString);
        await conn.OpenAsync(ct);
    }
}
