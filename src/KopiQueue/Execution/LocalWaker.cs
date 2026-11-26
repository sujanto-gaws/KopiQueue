namespace KopiQueue.Execution;

/// <summary>
/// Wakes the producers of one client when that client has inserted jobs whose commit it knows about (design §3,
/// FR-DB-04). It is the only low-latency path on SQL Server, and it saves a round trip through the database on
/// PostgreSQL. It never crosses processes.
/// </summary>
/// <remarks>Thread-safe. Handlers run on the inserting thread, so they must only signal, never block.</remarks>
internal sealed class LocalWaker
{
    private readonly Lock _lock = new();
    private Dictionary<string, Action[]> _handlers = new(StringComparer.Ordinal);

    /// <summary>Calls <paramref name="wake"/> whenever jobs become available on <paramref name="queue"/>.</summary>
    /// <returns>Dispose to stop the calls.</returns>
    public IDisposable Subscribe(string queue, Action wake)
    {
        ArgumentNullException.ThrowIfNull(queue);
        ArgumentNullException.ThrowIfNull(wake);
        lock (_lock)
        {
            var copy = new Dictionary<string, Action[]>(_handlers, StringComparer.Ordinal);
            copy[queue] = copy.TryGetValue(queue, out var existing) ? [.. existing, wake] : [wake];
            _handlers = copy;
        }

        return new Subscription(this, queue, wake);
    }

    /// <summary>Wakes the subscribers of each queue in <paramref name="queues"/>, once per queue.</summary>
    public void Wake(IEnumerable<string> queues)
    {
        ArgumentNullException.ThrowIfNull(queues);
        var handlers = Volatile.Read(ref _handlers);
        if (handlers.Count == 0)
        {
            return;
        }

        foreach (var queue in queues.Distinct(StringComparer.Ordinal))
        {
            if (handlers.TryGetValue(queue, out var wakes))
            {
                foreach (var wake in wakes)
                {
                    // The insert has already committed: a failing subscriber must not turn it into an error.
#pragma warning disable CA1031
                    try
                    {
                        wake();
                    }
                    catch (Exception)
                    {
                    }
#pragma warning restore CA1031
                }
            }
        }
    }

    private void Unsubscribe(string queue, Action wake)
    {
        lock (_lock)
        {
            if (!_handlers.TryGetValue(queue, out var existing) || Array.IndexOf(existing, wake) < 0)
            {
                return;
            }

            var copy = new Dictionary<string, Action[]>(_handlers, StringComparer.Ordinal);
            var index = Array.IndexOf(existing, wake);
            Action[] remaining = [.. existing[..index], .. existing[(index + 1)..]];
            if (remaining.Length == 0)
            {
                copy.Remove(queue);
            }
            else
            {
                copy[queue] = remaining;
            }

            _handlers = copy;
        }
    }

    private sealed class Subscription(LocalWaker owner, string queue, Action wake) : IDisposable
    {
        private int _disposed;

        public void Dispose()
        {
            if (Interlocked.Exchange(ref _disposed, 1) == 0)
            {
                owner.Unsubscribe(queue, wake);
            }
        }
    }
}
