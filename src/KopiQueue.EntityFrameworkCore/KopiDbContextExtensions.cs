using System.Data.Common;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Storage;

namespace KopiQueue.EntityFrameworkCore;

/// <summary>
/// Inserts jobs inside a <see cref="DbContext"/>'s current transaction, so they commit or roll back with the context's
/// own changes (FR-INS-03). Works with any relational EF Core provider whose transaction type the configured KopiQueue
/// driver accepts: Npgsql's for <c>KopiQueue.Npgsql</c>, SQL Server's for <c>KopiQueue.SqlServer</c>. Add
/// <see cref="KopiDbContextOptionsExtensions.AddKopiQueueCommitInterceptor(DbContextOptionsBuilder)"/> so the jobs are picked up
/// as soon as the transaction commits.
/// </summary>
/// <example>
/// <code>
/// await using var tx = await db.Database.BeginTransactionAsync(ct);
/// db.Orders.Add(order);
/// await db.SaveChangesAsync(ct);
/// await kopi.InsertAsync(new SendEmailArgs(order.Email, "order-confirm"), db, cancellationToken: ct);
/// await tx.CommitAsync(ct);
/// </code>
/// </example>
public static class KopiDbContextExtensions
{
    /// <summary>Inserts a job in <paramref name="dbContext"/>'s current transaction.</summary>
    /// <typeparam name="TArgs">The job's args type, with a <see cref="JobKindAttribute"/>.</typeparam>
    /// <param name="client">The KopiQueue client.</param>
    /// <param name="args">The job's arguments.</param>
    /// <param name="dbContext">A context with a transaction started by <c>Database.BeginTransaction</c> or <c>UseTransaction</c>.</param>
    /// <param name="options">Insert options. <c>null</c> uses the defaults.</param>
    /// <param name="cancellationToken">Cancels the insert.</param>
    /// <returns>The inserted job, visible to others once the context's transaction commits.</returns>
    /// <exception cref="InvalidOperationException">The context has no current transaction.</exception>
    /// <exception cref="ArgumentException">An insert option is not valid, or the context's provider is not the configured driver's database.</exception>
    public static Task<InsertResult<TArgs>> InsertAsync<TArgs>(
        this IKopiClient client, TArgs args, DbContext dbContext, InsertOptions? options = null, CancellationToken cancellationToken = default)
        where TArgs : IJobArgs
    {
        ArgumentNullException.ThrowIfNull(client);
        return client.InsertAsync(args, CurrentTransaction(dbContext), options, cancellationToken);
    }

    /// <summary>Inserts several jobs in <paramref name="dbContext"/>'s current transaction, returning them in order (FR-INS-04).</summary>
    /// <param name="client">The KopiQueue client.</param>
    /// <param name="jobs">The jobs.</param>
    /// <param name="dbContext">A context with a current transaction.</param>
    /// <param name="cancellationToken">Cancels the insert.</param>
    /// <returns>One result per job, in input order.</returns>
    /// <exception cref="InvalidOperationException">The context has no current transaction.</exception>
    /// <exception cref="ArgumentException">A job is not valid, or the context's provider is not the configured driver's database.</exception>
    public static Task<IReadOnlyList<InsertResult>> InsertManyAsync(
        this IKopiClient client, IEnumerable<InsertRequest> jobs, DbContext dbContext, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(client);
        return client.InsertManyAsync(jobs, CurrentTransaction(dbContext), cancellationToken);
    }

    /// <summary>Inserts several jobs in <paramref name="dbContext"/>'s current transaction on the fast path, returning the count (FR-INS-04).</summary>
    /// <param name="client">The KopiQueue client.</param>
    /// <param name="jobs">The jobs.</param>
    /// <param name="dbContext">A context with a current transaction.</param>
    /// <param name="cancellationToken">Cancels the insert.</param>
    /// <returns>The number of jobs inserted.</returns>
    /// <exception cref="InvalidOperationException">The context has no current transaction.</exception>
    /// <exception cref="ArgumentException">A job is not valid, or the context's provider is not the configured driver's database.</exception>
    public static Task<long> InsertManyFastAsync(
        this IKopiClient client, IEnumerable<InsertRequest> jobs, DbContext dbContext, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(client);
        return client.InsertManyFastAsync(jobs, CurrentTransaction(dbContext), cancellationToken);
    }

    /// <summary>
    /// The context's current transaction as ADO.NET sees it. Without one the jobs would commit on their own, apart from
    /// the context's changes, so that is an error rather than a silent separate insert.
    /// </summary>
    private static DbTransaction CurrentTransaction(DbContext dbContext)
    {
        ArgumentNullException.ThrowIfNull(dbContext);
        var transaction = dbContext.Database.CurrentTransaction
            ?? throw new InvalidOperationException(
                "The DbContext has no current transaction. Start one with Database.BeginTransactionAsync() so the jobs commit with the context's changes, or insert without the DbContext to commit them on their own.");
        return transaction.GetDbTransaction();
    }
}
