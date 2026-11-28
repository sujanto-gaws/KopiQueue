using System.Data.Common;
using System.Runtime.CompilerServices;

namespace KopiQueue.Execution;

/// <summary>
/// Wake-ups that wait for a caller's transaction to commit (design §3.3, §6.3, FR-DB-04). A client cannot see when a
/// caller commits, so the jobs it inserts in a caller's transaction would otherwise wait for the next poll on SQL Server.
/// A commit hook that can see the commit, such as the EF Core interceptor in <c>KopiQueue.EntityFrameworkCore</c>,
/// calls <see cref="Committed"/>, which wakes the producers that the transaction's inserts were meant for.
/// </summary>
/// <remarks>
/// Thread-safe. Entries are held weakly by their transaction, so a transaction no hook reports, such as a raw
/// <c>SqlTransaction</c>, leaves nothing behind once it is collected. A wake-up that a rolled back savepoint makes
/// pointless costs one fetch that finds nothing.
/// </remarks>
internal static class TransactionWakes
{
    private static readonly ConditionalWeakTable<DbTransaction, Pending> PendingWakes = new();

    /// <summary>
    /// Wakes <paramref name="waker"/>'s producers of <paramref name="queues"/> when <paramref name="transaction"/> commits.
    /// Nothing is recorded for a client that works no queues.
    /// </summary>
    public static void Add(DbTransaction transaction, LocalWaker waker, IEnumerable<string> queues)
    {
        ArgumentNullException.ThrowIfNull(transaction);
        ArgumentNullException.ThrowIfNull(waker);
        ArgumentNullException.ThrowIfNull(queues);
        if (!waker.HasSubscribers)
        {
            return;
        }

        string[] names = [.. queues];
        if (names.Length > 0)
        {
            PendingWakes.GetValue(transaction, _ => new Pending()).Add(waker, names);
        }
    }

    /// <summary>Wakes what <paramref name="transaction"/>'s inserts recorded, once: call it after the commit.</summary>
    public static void Committed(DbTransaction transaction)
    {
        ArgumentNullException.ThrowIfNull(transaction);
        if (PendingWakes.TryGetValue(transaction, out var pending) && PendingWakes.Remove(transaction))
        {
            pending.Wake();
        }
    }

    /// <summary>Forgets what <paramref name="transaction"/>'s inserts recorded: call it after a rollback.</summary>
    public static void RolledBack(DbTransaction transaction)
    {
        ArgumentNullException.ThrowIfNull(transaction);
        PendingWakes.Remove(transaction);
    }

    /// <summary>Whether <paramref name="transaction"/> has wake-ups waiting (for tests).</summary>
    internal static bool IsPending(DbTransaction transaction) => PendingWakes.TryGetValue(transaction, out _);

    private sealed class Pending
    {
        private readonly Lock _lock = new();
        private readonly Dictionary<LocalWaker, HashSet<string>> _queues = [];

        public void Add(LocalWaker waker, string[] names)
        {
            lock (_lock)
            {
                if (!_queues.TryGetValue(waker, out var set))
                {
                    _queues[waker] = set = new HashSet<string>(StringComparer.Ordinal);
                }

                set.UnionWith(names);
            }
        }

        public void Wake()
        {
            KeyValuePair<LocalWaker, HashSet<string>>[] wakes;
            lock (_lock)
            {
                wakes = [.. _queues];
            }

            foreach (var (waker, queues) in wakes)
            {
                waker.Wake(queues);
            }
        }
    }
}
