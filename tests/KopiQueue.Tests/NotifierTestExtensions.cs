using KopiQueue.Drivers;
using KopiQueue.Execution;

namespace KopiQueue.Tests;

internal static class NotifierTestExtensions
{
    /// <summary>
    /// Starts <paramref name="notifier"/> and waits until it has connected <b>and</b> run its connect-time resync.
    /// </summary>
    /// <remarks>
    /// The notifier reports <see cref="Notifier.IsConnected"/> just before it calls every subscriber's resync callback.
    /// A component that subscribes in that gap gets a resync, which is correct but makes a test that counts reads or
    /// attempts depend on thread timing. Waiting for the resync closes the gap.
    /// </remarks>
    public static async Task StartAndResyncAsync(this Notifier notifier, CancellationToken cancellationToken)
    {
        var resynced = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        using (notifier.Subscribe(NotificationTopic.Control, static _ => { }, onResync: () => resynced.TrySetResult()))
        {
            await notifier.StartAsync(cancellationToken);
            await resynced.Task.WaitAsync(TimeSpan.FromSeconds(20), cancellationToken);
        }
    }
}
