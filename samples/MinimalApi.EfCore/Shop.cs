using Microsoft.EntityFrameworkCore;

namespace KopiQueue.Samples.MinimalApi;

/// <summary>
/// The application's own data. KopiQueue's tables live beside it, in the same database, and with <c>Sample:Schema</c>
/// in the same schema.
/// </summary>
public sealed class ShopDb(DbContextOptions<ShopDb> options, IConfiguration configuration) : DbContext(options)
{
    public DbSet<Order> Orders => Set<Order>();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.HasDefaultSchema(configuration["Sample:Schema"]); // null: the provider's default (public, dbo)
        modelBuilder.Entity<Order>(order =>
        {
            order.ToTable("shop_orders");
            order.Property(o => o.Email).HasMaxLength(320);
            order.Property(o => o.Item).HasMaxLength(200);
        });
    }
}

public sealed class Order
{
    public long Id { get; set; }

    public required string Email { get; set; }

    public required string Item { get; set; }

    public DateTimeOffset CreatedAt { get; set; }

    /// <summary>Set by <see cref="SendConfirmationWorker"/> once the confirmation is sent.</summary>
    public DateTimeOffset? ConfirmationSentAt { get; set; }
}

public sealed record NewOrder(string Email, string Item);
