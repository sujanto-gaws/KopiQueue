using Microsoft.EntityFrameworkCore;

namespace KopiQueue.EntityFrameworkCore;

/// <summary>
/// Adds KopiQueue's commit interceptor to a <see cref="DbContext"/>'s options (design §6.3, FR-DB-04).
/// </summary>
/// <remarks>
/// <para>
/// Jobs inserted in a context's transaction (<see cref="KopiDbContextExtensions"/>) can only be picked up once the
/// transaction commits, which the client cannot see. With the interceptor, EF Core tells it, and the inserting client's
/// own producers fetch at once. Without it, they find the jobs at their next poll
/// (<see cref="KopiClientOptions.FetchPollInterval"/>). On PostgreSQL every client also hears of the commit through
/// LISTEN/NOTIFY, so the interceptor only saves a round trip there; on SQL Server it is the only fast path.
/// </para>
/// <para>
/// It sees commits and rollbacks made through EF Core, such as <c>IDbContextTransaction.CommitAsync</c>. A
/// <c>DbTransaction</c> committed directly, even one given to the context with <c>UseTransaction</c>, is not seen, and
/// its jobs wait for the poll. Producers of other clients and processes always wait for their poll on SQL Server.
/// </para>
/// </remarks>
/// <example>
/// <code>
/// services.AddDbContext&lt;ShopDb&gt;(options => options
///     .UseSqlServer(connectionString)
///     .AddKopiQueueCommitInterceptor());
/// </code>
/// </example>
public static class KopiDbContextOptionsExtensions
{
    /// <summary>Adds the commit interceptor that wakes the inserting client's producers when a transaction with jobs commits.</summary>
    /// <param name="optionsBuilder">The context's options.</param>
    /// <returns><paramref name="optionsBuilder"/>, for chaining.</returns>
    public static DbContextOptionsBuilder AddKopiQueueCommitInterceptor(this DbContextOptionsBuilder optionsBuilder)
    {
        ArgumentNullException.ThrowIfNull(optionsBuilder);
        return optionsBuilder.AddInterceptors(KopiCommitInterceptor.Instance);
    }

    /// <summary>Adds the commit interceptor that wakes the inserting client's producers when a transaction with jobs commits.</summary>
    /// <typeparam name="TContext">The context type.</typeparam>
    /// <param name="optionsBuilder">The context's options.</param>
    /// <returns><paramref name="optionsBuilder"/>, for chaining.</returns>
    public static DbContextOptionsBuilder<TContext> AddKopiQueueCommitInterceptor<TContext>(this DbContextOptionsBuilder<TContext> optionsBuilder)
        where TContext : DbContext
    {
        ArgumentNullException.ThrowIfNull(optionsBuilder);
        return optionsBuilder.AddInterceptors(KopiCommitInterceptor.Instance);
    }
}
