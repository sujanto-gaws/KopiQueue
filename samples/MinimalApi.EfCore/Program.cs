using KopiQueue;
using KopiQueue.EntityFrameworkCore;
using KopiQueue.Migrations;
using KopiQueue.Npgsql;
using KopiQueue.Samples.MinimalApi;
using KopiQueue.SqlServer;
using Microsoft.Data.SqlClient;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Storage;
using Npgsql;

var builder = WebApplication.CreateBuilder(args);

var connectionString = builder.Configuration.GetConnectionString("Shop")
    ?? throw new InvalidOperationException("Set ConnectionStrings:Shop, for example with the ConnectionStrings__Shop environment variable.");
var schema = builder.Configuration["Sample:Schema"];
var database = builder.Configuration["Sample:Database"] ?? "PostgreSql";

// The registrations that differ between databases: EF Core's provider and KopiQueue's driver (FR-DB-03). The
// endpoints, the worker and the KopiQueue configuration section are the same on both.
Action<IServiceProvider, DbContextOptionsBuilder> useEfCore;
Action<KopiQueueBuilder> useKopiQueue;
Func<IServiceProvider, CancellationToken, Task> migrateKopiQueue;
switch (database.ToUpperInvariant())
{
    case "POSTGRESQL":
        // One data source for both, so EF Core and KopiQueue share Npgsql's connection pool.
        var npgsqlOptions = new NpgsqlDriverOptions { Schema = schema };
        builder.Services.AddSingleton(_ => NpgsqlDataSource.Create(connectionString));
        useEfCore = (services, options) => options.UseNpgsql(services.GetRequiredService<NpgsqlDataSource>());
        useKopiQueue = kq => kq.UseNpgsql(npgsqlOptions);
        migrateKopiQueue = (services, ct) => MigratePostgreSqlAsync(services.GetRequiredService<NpgsqlDataSource>(), npgsqlOptions, ct);
        break;
    case "SQLSERVER":
        var sqlServerOptions = new SqlServerDriverOptions { Schema = schema ?? "dbo" };
        useEfCore = (_, options) => options.UseSqlServer(connectionString);
        useKopiQueue = kq => kq.UseSqlServer(connectionString, sqlServerOptions);
        migrateKopiQueue = (_, ct) => MigrateSqlServerAsync(connectionString, sqlServerOptions, ct);
        break;
    default:
        throw new InvalidOperationException($"Sample:Database is '{database}': use PostgreSql or SqlServer.");
}

// The commit interceptor tells this client when a transaction with jobs commits, so it starts them at once instead of
// at its next poll. On SQL Server, which has no LISTEN/NOTIFY, that is the fast path (FR-DB-04).
builder.Services.AddDbContext<ShopDb>((services, options) =>
{
    useEfCore(services, options);
    options.AddKopiQueueCommitInterceptor();
});

builder.Services.AddKopiQueue(kq =>
    {
        useKopiQueue(kq);
        kq.AddWorker<SendConfirmationArgs, SendConfirmationWorker>();
    })
    .BindConfiguration("KopiQueue");

var app = builder.Build();

// Places an order. The order and its confirmation job commit together or not at all (FR-INS-03): a job never runs for
// an order that was rolled back, and an order never misses its job. ?rollback=true shows the second half.
app.MapPost("/orders", async (NewOrder input, bool? rollback, ShopDb db, IKopiClient kopi, CancellationToken ct) =>
{
    await using var transaction = await db.Database.BeginTransactionAsync(ct);

    var order = new Order { Email = input.Email, Item = input.Item, CreatedAt = DateTimeOffset.UtcNow };
    db.Orders.Add(order);
    await db.SaveChangesAsync(ct);

    var job = await kopi.InsertAsync(new SendConfirmationArgs(order.Id), db, new InsertOptions { Queue = "email" }, ct);

    if (rollback is true)
    {
        await transaction.RollbackAsync(ct);
        return Results.Ok(new { message = "Rolled back: neither the order nor its job exists.", orderId = order.Id, jobId = job.Job.Id });
    }

    await transaction.CommitAsync(ct);
    return Results.Created($"/orders/{order.Id}", new { orderId = order.Id, jobId = job.Job.Id });
});

app.MapGet("/orders/{id:long}", async (long id, ShopDb db, CancellationToken ct) =>
    await db.Orders.AsNoTracking().SingleOrDefaultAsync(o => o.Id == id, ct) is { } order ? Results.Ok(order) : Results.NotFound());

// A job's progress, through KopiQueue's admin API (FR-ADM-01).
app.MapGet("/jobs/{id:long}", async (long id, IKopiClient kopi, CancellationToken ct) =>
    await kopi.JobGetAsync(id, ct) is { } job
        ? Results.Ok(new
        {
            job.Id,
            job.Kind,
            job.Queue,
            State = job.State.ToString(),
            job.Attempt,
            job.CreatedAt,
            job.FinalizedAt,
            Errors = job.Errors.Select(e => new { e.Attempt, e.Error }),
        })
        : Results.NotFound());

// Sample convenience: create the tables before starting. Real applications use EF Core migrations for their own tables
// and `kopi migrate` for KopiQueue's (FR-MIG-02).
await using (var scope = app.Services.CreateAsyncScope())
{
    await CreateShopTablesAsync(scope.ServiceProvider.GetRequiredService<ShopDb>(), CancellationToken.None);
}

await migrateKopiQueue(app.Services, CancellationToken.None);

await app.RunAsync();

// EnsureCreated would do nothing in a database that already has tables, such as KopiQueue's, so the shop's table is
// created when information_schema, which both databases have, does not list it.
static async Task CreateShopTablesAsync(ShopDb db, CancellationToken ct)
{
    var table = db.Model.FindEntityType(typeof(Order))!;
    var schema = table.GetSchema() ?? (db.Database.IsNpgsql() ? "public" : "dbo");
    var name = table.GetTableName()!;
    var exists = await db.Database
        .SqlQuery<int>($"SELECT COUNT(*) AS \"Value\" FROM information_schema.tables WHERE table_schema = {schema} AND table_name = {name}")
        .SingleAsync(ct) > 0;
    if (!exists)
    {
        await db.GetService<IRelationalDatabaseCreator>().CreateTablesAsync(ct);
    }
}

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
