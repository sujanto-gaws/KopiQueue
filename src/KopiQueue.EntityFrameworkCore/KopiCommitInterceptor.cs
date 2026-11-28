using System.Data.Common;
using KopiQueue.Execution;
using Microsoft.EntityFrameworkCore.Diagnostics;

namespace KopiQueue.EntityFrameworkCore;

/// <summary>
/// Reports EF Core commits and rollbacks to KopiQueue, so jobs inserted in a context's transaction wake the inserting
/// client's producers as soon as the transaction commits (design §6.3, FR-DB-04). Stateless: one instance serves every
/// context and client.
/// </summary>
internal sealed class KopiCommitInterceptor : DbTransactionInterceptor
{
    public static KopiCommitInterceptor Instance { get; } = new();

    private KopiCommitInterceptor()
    {
    }

    public override void TransactionCommitted(DbTransaction transaction, TransactionEndEventData eventData) =>
        TransactionWakes.Committed(transaction);

    public override Task TransactionCommittedAsync(
        DbTransaction transaction, TransactionEndEventData eventData, CancellationToken cancellationToken = default)
    {
        TransactionWakes.Committed(transaction);
        return Task.CompletedTask;
    }

    public override void TransactionRolledBack(DbTransaction transaction, TransactionEndEventData eventData) =>
        TransactionWakes.RolledBack(transaction);

    public override Task TransactionRolledBackAsync(
        DbTransaction transaction, TransactionEndEventData eventData, CancellationToken cancellationToken = default)
    {
        TransactionWakes.RolledBack(transaction);
        return Task.CompletedTask;
    }
}
