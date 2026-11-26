using Microsoft.Data.SqlClient;
using Testcontainers.MsSql;

namespace KopiQueue.TestInfrastructure;

/// <summary>
/// A fresh SQL Server database per test class.
/// Server: <c>KOPI_TEST_MSSQL</c> (an admin connection string), otherwise a Testcontainers SQL Server container
/// (image overridable with <c>KOPI_TEST_MSSQL_IMAGE</c>), started once per test run.
/// </summary>
public sealed class SqlServerFixture : DatabaseFixture
{
    public const string EnvironmentVariable = "KOPI_TEST_MSSQL";
    public const string ImageEnvironmentVariable = "KOPI_TEST_MSSQL_IMAGE";
    public const string DefaultImage = "mcr.microsoft.com/mssql/server:2022-latest";

    private static readonly Lazy<Task<TestServer>> SharedServer = new(() =>
        TestEnvironment.ResolveAsync(EnvironmentVariable, StartContainerAsync, ProbeAsync));

    private static MsSqlContainer? _container;

    /// <summary>Opens a connection to this fixture's database. The caller disposes it.</summary>
    public async Task<SqlConnection> OpenConnectionAsync(CancellationToken ct)
    {
        var conn = new SqlConnection(ConnectionString);
        await conn.OpenAsync(ct);
        return conn;
    }

    protected override Task<TestServer> GetServerAsync() => SharedServer.Value;

    protected override async Task CreateDatabaseAsync(string adminConnectionString, CancellationToken ct)
    {
        await using var conn = new SqlConnection(adminConnectionString);
        await conn.OpenAsync(ct);
        await using var cmd = new SqlCommand($"CREATE DATABASE [{DatabaseName}]", conn);
        await cmd.ExecuteNonQueryAsync(ct);
    }

    protected override async Task DropDatabaseAsync(string adminConnectionString, CancellationToken ct)
    {
        SqlConnection.ClearAllPools();
        await using var conn = new SqlConnection(adminConnectionString);
        await conn.OpenAsync(ct);
        await using var cmd = new SqlCommand(
            $"""
            IF DB_ID(N'{DatabaseName}') IS NOT NULL
            BEGIN
                ALTER DATABASE [{DatabaseName}] SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
                DROP DATABASE [{DatabaseName}];
            END
            """, conn);
        await cmd.ExecuteNonQueryAsync(ct);
    }

    protected override string BuildConnectionString(string adminConnectionString, string databaseName) =>
        new SqlConnectionStringBuilder(adminConnectionString) { InitialCatalog = databaseName }.ConnectionString;

    private static async Task<string> StartContainerAsync()
    {
        var image = Environment.GetEnvironmentVariable(ImageEnvironmentVariable) ?? DefaultImage;
        _container = new MsSqlBuilder(image).Build();
        await _container.StartAsync();
        return _container.GetConnectionString();
    }

    private static async Task ProbeAsync(string connectionString, CancellationToken ct)
    {
        await using var conn = new SqlConnection(connectionString);
        await conn.OpenAsync(ct);
    }
}
