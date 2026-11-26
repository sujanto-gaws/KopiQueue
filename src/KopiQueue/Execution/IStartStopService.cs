namespace KopiQueue.Execution;

/// <summary>A long-running component of a client that works jobs (design §3.2).</summary>
/// <remarks>
/// The client starts its components in the order notifier, elector, queue maintainer, completer, producers, and stops
/// them in reverse.
/// </remarks>
internal interface IStartStopService
{
    /// <summary>Starts the component. Returns once it is running; it does not wait for any database work.</summary>
    /// <exception cref="InvalidOperationException">The component was already started.</exception>
    Task StartAsync(CancellationToken cancellationToken);

    /// <summary>
    /// Stops the component and returns once it has stopped. <paramref name="cancellationToken"/> tells it to hurry:
    /// it abandons work that would wait longer. Stopping a component that never started does nothing.
    /// </summary>
    Task StopAsync(CancellationToken cancellationToken);
}
