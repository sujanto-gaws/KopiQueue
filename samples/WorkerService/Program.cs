using KopiQueue.Migrations;
using KopiQueue.Npgsql;
using KopiQueue.Samples.WorkerService;
using Npgsql;

var builder = Host.CreateApplicationBuilder(args);

var connectionString = builder.Configuration.GetConnectionString("Jobs")
    ?? throw new InvalidOperationException("Set ConnectionStrings:Jobs, for example with the ConnectionStrings__Jobs environment variable.");
var driverOptions = new NpgsqlDriverOptions { Schema = builder.Configuration["Sample:Schema"] };

builder.Services.AddSingleton(_ => NpgsqlDataSource.Create(connectionString)); // or AddNpgsqlDataSource (Npgsql.DependencyInjection)
builder.Services.AddKopiQueue(kq => kq
        .UseNpgsql(driverOptions)
        .UseJsonContext(SampleJsonContext.Default)
        .AddWorker<SendEmailArgs, SendEmailWorker>()
        .AddWorker<FlakyArgs, FlakyWorker>())
    .BindConfiguration("KopiQueue");    // queues, timeouts and modes from appsettings.json
builder.Services.AddSingleton<Outbox>(); // a scoped or singleton dependency of a worker
builder.Services.AddHostedService<DemoInserter>();

using var host = builder.Build();

// Sample convenience: bring the schema up to date before starting. Real deployments run `kopi migrate` (FR-MIG-02).
await MigrateAsync(host.Services.GetRequiredService<NpgsqlDataSource>(), driverOptions);

await host.RunAsync();

static async Task MigrateAsync(NpgsqlDataSource dataSource, NpgsqlDriverOptions options)
{
    if (options.Schema is { } schema)
    {
        await using var create = dataSource.CreateCommand($"CREATE SCHEMA IF NOT EXISTS \"{schema.Replace("\"", "\"\"", StringComparison.Ordinal)}\"");
        await create.ExecuteNonQueryAsync();
    }

    await new NpgsqlDriver(dataSource, options).CreateMigrator().MigrateAsync(MigrationDirection.Up);
}
