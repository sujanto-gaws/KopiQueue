using System.Diagnostics.CodeAnalysis;

namespace KopiQueue.Drivers;

/// <summary>What a notification is about (design §4.3). Each topic is one channel on PostgreSQL.</summary>
/// <remarks>Experimental (<c>KQ0001</c>): see <see cref="IKopiDriver"/>.</remarks>
[Experimental(DiagnosticIds.ExperimentalDriver)]
public enum NotificationTopic
{
    /// <summary>Jobs became available on a queue (<c>river_insert</c>). Payload: <c>{"queue": "..."}</c>.</summary>
    Insert,

    /// <summary>Cancel a job, or pause or resume a queue (<c>river_control</c>).</summary>
    Control,

    /// <summary>The leader resigned (<c>river_leadership</c>).</summary>
    Leadership,
}

/// <summary>One notification received by a listener.</summary>
/// <param name="Topic">The topic it was sent on.</param>
/// <param name="Payload">The payload as sent, normally JSON. Parse it; never compare it as a string, because senders format JSON differently.</param>
/// <remarks>Experimental (<c>KQ0001</c>): see <see cref="IKopiDriver"/>.</remarks>
[Experimental(DiagnosticIds.ExperimentalDriver)]
public sealed record KopiNotification(NotificationTopic Topic, string Payload);

/// <summary>
/// Opens listener connections for a client's notifier (design §3). The notifier owns reconnecting, backoff and
/// resyncing; a listener only knows how to open one connection that receives every <see cref="NotificationTopic"/>.
/// </summary>
/// <remarks>Experimental (<c>KQ0001</c>): see <see cref="IKopiDriver"/>.</remarks>
[Experimental(DiagnosticIds.ExperimentalDriver)]
public interface IKopiListener
{
    /// <summary>
    /// Opens a dedicated connection and subscribes it to every topic. When this returns, notifications sent after it
    /// are delivered by the connection; anything sent earlier may have been missed.
    /// </summary>
    /// <exception cref="KopiQueueConfigurationException">
    /// The configuration can never work, for example a schema name too long for its channel names. Retrying will not help.
    /// </exception>
    /// <remarks>Other exceptions, such as a refused connection, are transient: the notifier retries with backoff.</remarks>
    Task<IKopiListenerConnection> ConnectAsync(CancellationToken cancellationToken);
}

/// <summary>An open listener connection. Not thread-safe: one caller waits on it at a time.</summary>
/// <remarks>
/// <para>
/// A connection that fails stays failed: <see cref="WaitForNotificationAsync"/> keeps throwing, and the notifier
/// disposes it and connects again. <see cref="IAsyncDisposable.DisposeAsync"/> may hang on a dead network path, so the
/// notifier never awaits it without a timeout.
/// </para>
/// <para>Experimental (<c>KQ0001</c>): see <see cref="IKopiDriver"/>.</para>
/// </remarks>
[Experimental(DiagnosticIds.ExperimentalDriver)]
public interface IKopiListenerConnection : IAsyncDisposable
{
    /// <summary>
    /// Waits for the next notification. A connection the database closed, or one found dead by the driver's
    /// keep-alive, throws (NFR-REL-04a).
    /// </summary>
    Task<KopiNotification> WaitForNotificationAsync(CancellationToken cancellationToken);
}
