using KopiQueue;
using KopiQueue.Migrations;
using KopiQueue.Npgsql;
using KopiQueue.Samples.WorkerService;
using KopiQueue.SqlServer;
using Microsoft.Data.SqlClient;
using Npgsql;

var builder = Host.CreateApplicationBuilder(args);

var connectionString = builder.Configuration.GetConnectionString("Jobs")
    ?? throw new InvalidOperationException("Set ConnectionStrings:Jobs, for example with the ConnectionStrings__Jobs environment variable.");
var schema = builder.Configuration["Sample:Schema"];
var database = builder.Configuration["Sample:Database"] ?? "PostgreSql";

// The one line that differs between databases is the driver registration (FR-DB-03). Everything else, including the
// workers and the KopiQueue configuration section, is the same on both.
Action<KopiQueueBuilder> useDatabase;
Func<IServiceProvider, CancellationToken, Task> migrate;
switch (database.ToUpperInvariant())
{
    case "POSTGRESQL":
        var npgsqlOptions = new NpgsqlDriverOptions { Schema = schema };
        builder.Services.AddSingleton(_ => NpgsqlDataSource.Create(connectionString)); // or AddNpgsqlDataSource (Npgsql.DependencyInjection)
        useDatabase = kq => kq.UseNpgsql(npgsqlOptions);
        migrate = (services, ct) => MigratePostgreSqlAsync(services.GetRequiredService<NpgsqlDataSource>(), npgsqlOptions, ct);
        break;
    case "SQLSERVER":
        var sqlServerOptions = new SqlServerDriverOptions { Schema = schema ?? "dbo" };
        useDatabase = kq => kq.UseSqlServer(connectionString, sqlServerOptions);
        migrate = (_, ct) => MigrateSqlServerAsync(connectionString, sqlServerOptions, ct);
        break;
    default:
        throw new InvalidOperationException($"Sample:Database is '{database}': use PostgreSql or SqlServer.");
}

builder.Services.AddKopiQueue(kq =>
    {
        useDatabase(kq);
        kq.UseJsonContext(SampleJsonContext.Default)
            .AddWorker<SendEmailArgs, SendEmailWorker>()
            .AddWorker<FlakyArgs, FlakyWorker>();
    })
    .BindConfiguration("KopiQueue");    // queues, timeouts and modes from appsettings.json
builder.Services.AddSingleton<Outbox>(); // a scoped or singleton dependency of a worker
builder.Services.AddHostedService<DemoInserter>();

using var host = builder.Build();

// Sample convenience: bring the schema up to date before starting. Real deployments run `kopi migrate` (FR-MIG-02).
await migrate(host.Services, CancellationToken.None);

await host.RunAsync();

static async Task MigratePostgreSqlAsync(NpgsqlDataSource dataSource, NpgsqlDriverOptions options, CancellationToken ct)
{
    if (options.Schema is { } schema)
    {
        await using var create = dataSource.CreateCommand($"CREATE SCHEMA IF NOT EXISTS \"{schema.Replace("\"", "\"\"", StringComparison.Ordinal)}\"");
        await create.ExecuteNonQueryAsync(ct);
    }

    await new NpgsqlDriver(dataSource, options).CreateMigrator().MigrateAsync(MigrationDirection.Up, cancellationToken: ct);
}

static async Task MigrateSqlServerAsync(string connectionString, SqlServerDriverOptions options, CancellationToken ct)
{
    await using (var connection = new SqlConnection(connectionString))
    {
        await connection.OpenAsync(ct);
        await using var create = new SqlCommand(
            "IF SCHEMA_ID(@schema) IS NULL BEGIN DECLARE @sql nvarchar(300) = N'CREATE SCHEMA ' + QUOTENAME(@schema); EXEC sys.sp_executesql @sql; END",
            connection);
        create.Parameters.AddWithValue("@schema", options.Schema);
        await create.ExecuteNonQueryAsync(ct);
    }

    await new SqlServerDriver(connectionString, options).CreateMigrator().MigrateAsync(MigrationDirection.Up, cancellationToken: ct);
}
