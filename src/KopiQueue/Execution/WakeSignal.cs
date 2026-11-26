namespace KopiQueue.Execution;

/// <summary>
/// A coalescing wake-up for one waiter: any number of <see cref="Set"/> calls before or during a wait release that wait
/// once. Timeouts run on a <see cref="TimeProvider"/>, so tests can drive them with a fake clock.
/// </summary>
internal sealed class WakeSignal
{
    private readonly Lock _lock = new();
    private bool _pending;
    private TaskCompletionSource? _waiter;

    /// <summary>Wakes the current or next wait. Never blocks.</summary>
    public void Set()
    {
        lock (_lock)
        {
            // A woken waiter stays registered until its wait returns, so a burst wakes it once. It is completed under the
            // lock so that a timeout sees either no set or a completed waiter; its continuations run asynchronously.
            _pending = _waiter is null;
            _waiter?.TrySetResult();
        }
    }

    /// <summary>Forgets a pending wake-up: the caller is about to do what the wake-up asked for.</summary>
    public void Clear()
    {
        lock (_lock)
        {
            _pending = false;
        }
    }

    /// <summary>Waits until <see cref="Set"/> or until <paramref name="timeout"/> passes on <paramref name="time"/>.</summary>
    /// <returns><c>true</c> if woken, <c>false</c> on timeout.</returns>
    /// <exception cref="OperationCanceledException"><paramref name="cancellationToken"/> was cancelled.</exception>
    public async Task<bool> WaitAsync(TimeSpan timeout, TimeProvider time, CancellationToken cancellationToken)
    {
        TaskCompletionSource waiter;
        lock (_lock)
        {
            if (_pending)
            {
                _pending = false;
                return true;
            }

            waiter = _waiter = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        }

        try
        {
            await waiter.Task.WaitAsync(timeout, time, cancellationToken).ConfigureAwait(false);
            lock (_lock)
            {
                _waiter = null;
            }

            return true;
        }
        catch (Exception e) when (e is TimeoutException or OperationCanceledException)
        {
            lock (_lock)
            {
                _waiter = null;
                if (waiter.Task.IsCompleted)
                {
                    // Set raced the timeout or the cancellation: keep the wake-up for the next wait.
                    _pending = true;
                }
            }

            cancellationToken.ThrowIfCancellationRequested();
            return false;
        }
    }
}
