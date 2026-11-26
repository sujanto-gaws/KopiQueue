using KopiQueue.EntityFrameworkCore;
using KopiQueue.Migrations;
using KopiQueue.TestInfrastructure;
using Microsoft.EntityFrameworkCore;
using Npgsql;

namespace KopiQueue.Npgsql.Tests;

/// <summary>Inserting jobs in a <see cref="DbContext"/>'s transaction with the Npgsql EF Core provider (plan task 1.9: FR-INS-03).</summary>
public sealed class EntityFrameworkCoreInsertTests(PostgresFixture db) : IClassFixture<PostgresFixture>
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Fact]
    public async Task Jobs_commit_with_the_contexts_changes()
    {
        db.SkipIfUnavailable();
        await using var fixture = await Fixture.CreateAsync(db);
        await using var context = fixture.Context();

        await using (var tx = await context.Database.BeginTransactionAsync(Ct))
        {
            context.Orders.Add(new Order { Id = 1, Email = "a@example.com" });
            await context.SaveChangesAsync(Ct);
            var result = await fixture.Client.InsertAsync(new ConfirmArgs(1), context, new InsertOptions { Queue = "email" }, Ct);
            Assert.Equal(0L, await fixture.JobCountAsync()); // not visible outside the transaction yet
            Assert.Equal("confirm_order", result.Job.Kind);
            await tx.CommitAsync(Ct);
        }

        Assert.Equal(1L, await fixture.JobCountAsync());
        Assert.Equal(1, await fixture.Context().Orders.CountAsync(Ct));
    }

    [Fact]
    public async Task Jobs_roll_back_with_the_contexts_changes()
    {
        db.SkipIfUnavailable();
        await using var fixture = await Fixture.CreateAsync(db);
        await using var context = fixture.Context();

        await using (var tx = await context.Database.BeginTransactionAsync(Ct))
        {
            context.Orders.Add(new Order { Id = 2, Email = "b@example.com" });
            await context.SaveChangesAsync(Ct);
            await fixture.Client.InsertAsync(new ConfirmArgs(2), context, cancellationToken: Ct);
            await fixture.Client.InsertManyAsync([new(new ConfirmArgs(3)), new(new ConfirmArgs(4))], context, Ct);
            Assert.Equal(5, await fixture.Client.InsertManyFastAsync(Enumerable.Range(10, 5).Select(i => new InsertRequest(new ConfirmArgs(i))), context, Ct));
            await tx.RollbackAsync(Ct);
        }

        Assert.Equal(0L, await fixture.JobCountAsync());
        Assert.Equal(0, await fixture.Context().Orders.CountAsync(Ct));
    }

    [Fact]
    public async Task Bulk_inserts_commit_with_the_context()
    {
        db.SkipIfUnavailable();
        await using var fixture = await Fixture.CreateAsync(db);
        await using var context = fixture.Context();

        await using (var tx = await context.Database.BeginTransactionAsync(Ct))
        {
            var results = await fixture.Client.InsertManyAsync([new(new ConfirmArgs(1)), new(new ConfirmArgs(2))], context, Ct);
            var count = await fixture.Client.InsertManyFastAsync([new(new ConfirmArgs(3))], context, Ct);
            Assert.Equal((2, 1L), (results.Count, count));
            await tx.CommitAsync(Ct);
        }

        Assert.Equal(3L, await fixture.JobCountAsync());
    }

    [Fact]
    public async Task A_transaction_shared_with_UseTransaction_is_used()
    {
        db.SkipIfUnavailable();
        await using var fixture = await Fixture.CreateAsync(db);
        await using var connection = await fixture.DataSource.OpenConnectionAsync(Ct);
        await using var tx = await connection.BeginTransactionAsync(Ct);
        await using var context = Fixture.Context(connection);

        await context.Database.UseTransactionAsync(tx, Ct);
        context.Orders.Add(new Order { Id = 5, Email = "e@example.com" });
        await context.SaveChangesAsync(Ct);
        await fixture.Client.InsertAsync(new ConfirmArgs(5), context, cancellationToken: Ct);
        await tx.CommitAsync(Ct);

        Assert.Equal(1L, await fixture.JobCountAsync());
        Assert.Equal(1, await fixture.Context().Orders.CountAsync(Ct));
    }

    [Fact]
    public async Task A_context_without_a_transaction_is_rejected_and_nothing_is_inserted()
    {
        db.SkipIfUnavailable();
        await using var fixture = await Fixture.CreateAsync(db);
        await using var context = fixture.Context();

        var ex = await Assert.ThrowsAsync<InvalidOperationException>(() => fixture.Client.InsertAsync(new ConfirmArgs(1), context, cancellationToken: Ct));
        await Assert.ThrowsAsync<InvalidOperationException>(() => fixture.Client.InsertManyAsync([new(new ConfirmArgs(1))], context, Ct));
        await Assert.ThrowsAsync<InvalidOperationException>(() => fixture.Client.InsertManyFastAsync([new(new ConfirmArgs(1))], context, Ct));

        Assert.Contains("no current transaction", ex.Message, StringComparison.Ordinal);
        Assert.Equal(0L, await fixture.JobCountAsync());
        await Assert.ThrowsAsync<ArgumentNullException>(() => fixture.Client.InsertAsync(new ConfirmArgs(1), (DbContext)null!, cancellationToken: Ct));
        await Assert.ThrowsAsync<ArgumentNullException>(() => ((IKopiClient)null!).InsertAsync(new ConfirmArgs(1), context, cancellationToken: Ct));
    }

    [JobKind("confirm_order")]
    private sealed record ConfirmArgs(int OrderId) : IJobArgs;

    private sealed class Order
    {
        public int Id { get; set; }

        public required string Email { get; set; }
    }

    /// <summary>
    /// EF Core caches a context type's model, so the schema comes from the connection's <c>search_path</c>, not from
    /// <c>HasDefaultSchema</c>.
    /// </summary>
    private sealed class ShopContext(DbContextOptions<ShopContext> options) : DbContext(options)
    {
        public DbSet<Order> Orders => Set<Order>();

        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.Entity<Order>(e =>
            {
                e.ToTable("orders");
                e.Property(o => o.Id).HasColumnName("id").ValueGeneratedNever();
                e.Property(o => o.Email).HasColumnName("email");
            });
        }
    }

    /// <summary>A migrated schema with an <c>orders</c> table, a KopiQueue client on it, and EF contexts.</summary>
    private sealed class Fixture : IAsyncDisposable
    {
        private readonly string _schema;

        private Fixture(NpgsqlDataSource dataSource, string schema)
        {
            DataSource = dataSource;
            _schema = schema;
            Client = new KopiClient(new NpgsqlDriver(dataSource, new NpgsqlDriverOptions { Schema = schema }));
        }

        public NpgsqlDataSource DataSource { get; }

        public KopiClient Client { get; }

        public static async Task<Fixture> CreateAsync(PostgresFixture db)
        {
            var schema = "e_" + Guid.NewGuid().ToString("N")[..12];
            var dataSource = NpgsqlDataSource.Create(new NpgsqlConnectionStringBuilder(db.ConnectionString) { SearchPath = schema }.ConnectionString);
            await using (var cmd = dataSource.CreateCommand($"CREATE SCHEMA {schema}; CREATE TABLE {schema}.orders (id int PRIMARY KEY, email text NOT NULL)"))
            {
                await cmd.ExecuteNonQueryAsync(Ct);
            }

            await new NpgsqlDriver(dataSource, new NpgsqlDriverOptions { Schema = schema }).CreateMigrator().MigrateAsync(MigrationDirection.Up, cancellationToken: Ct);
            return new Fixture(dataSource, schema);
        }

        public ShopContext Context() =>
            new(new DbContextOptionsBuilder<ShopContext>().UseNpgsql(DataSource).Options);

        public static ShopContext Context(NpgsqlConnection connection) =>
            new(new DbContextOptionsBuilder<ShopContext>().UseNpgsql(connection).Options);

        public async Task<long> JobCountAsync()
        {
            await using var cmd = DataSource.CreateCommand($"SELECT count(*) FROM {_schema}.river_job WHERE kind = 'confirm_order'");
            return (long)(await cmd.ExecuteScalarAsync(Ct))!;
        }

        public ValueTask DisposeAsync() => DataSource.DisposeAsync();
    }
}
