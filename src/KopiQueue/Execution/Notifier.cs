using KopiQueue.Drivers;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace KopiQueue.Execution;

/// <summary>Timings of a <see cref="Notifier"/> (spike 0.7, decision D3).</summary>
internal sealed class NotifierOptions
{
    /// <summary>The first reconnect delay; it doubles after each failed attempt.</summary>
    public TimeSpan ReconnectMinDelay { get; init; } = TimeSpan.FromMilliseconds(100);

    /// <summary>The longest reconnect delay.</summary>
    public TimeSpan ReconnectMaxDelay { get; init; } = TimeSpan.FromSeconds(30);

    /// <summary>How long to wait for a connection to close before abandoning it.</summary>
    public TimeSpan DisposeTimeout { get; init; } = TimeSpan.FromSeconds(5);

    /// <summary>Returns a number in [0, 1) for the reconnect jitter. Default: <see cref="Random.Shared"/>.</summary>
    public Func<double>? Random { get; init; }
}

/// <summary>
/// Keeps one listener connection open and fans its notifications out to subscribers (design §3, FR-EXE-03).
/// </summary>
/// <remarks>
/// <para>
/// Notifications are only an optimisation: subscribers also poll, so the notifier never makes the client fail. A lost
/// connection is reconnected with exponential backoff (100 ms to 30 s, with jitter). After every connect, first or
/// later, every subscriber's resync callback runs, because notifications sent while disconnected are lost
/// (NFR-REL-04a, spike 0.7 D2). A broken connection is disposed in the background with a timeout and never delays the
/// reconnect (D3). A configuration error from the listener stops the notifier for good: the client then only polls.
/// </para>
/// <para>
/// Callbacks run on the notifier's loop, one at a time, and must only signal: a slow callback delays every other
/// notification. A callback that throws is logged and does not affect the others.
/// </para>
/// </remarks>
internal sealed partial class Notifier : IStartStopService, IAsyncDisposable
{
    private readonly IKopiListener _listener;
    private readonly NotifierOptions _options;
    private readonly TimeProvider _time;
    private readonly ILogger _logger;
    private readonly Func<double> _random;
    private readonly Lock _lock = new();
    private Subscription[] _subscriptions = [];
    private CancellationTokenSource? _stop;
    private Task? _loop;
    private int _connected;

    public Notifier(IKopiListener listener, NotifierOptions? options = null, TimeProvider? timeProvider = null, ILogger<Notifier>? logger = null)
    {
        ArgumentNullException.ThrowIfNull(listener);
        _listener = listener;
        _options = options ?? new NotifierOptions();
        _time = timeProvider ?? TimeProvider.System;
        _logger = (ILogger?)logger ?? NullLogger.Instance;
        _random = _options.Random ?? Random.Shared.NextDouble;
    }

    /// <summary>Whether a listener connection is open right now (spike 0.7 D8: a health signal).</summary>
    public bool IsConnected => Volatile.Read(ref _connected) == 1;

    /// <summary>Whether the notifier gave up for good after a configuration error, so the client only polls.</summary>
    public bool IsDisabled { get; private set; }

    /// <summary>
    /// Calls <paramref name="onNotification"/> with the payload of every notification on <paramref name="topic"/>, and
    /// <paramref name="onResync"/> after every connect, when notifications may have been missed.
    /// </summary>
    /// <returns>Dispose to stop the calls.</returns>
    public IDisposable Subscribe(NotificationTopic topic, Action<string> onNotification, Action onResync)
    {
        ArgumentNullException.ThrowIfNull(onNotification);
        ArgumentNullException.ThrowIfNull(onResync);
        var subscription = new Subscription(this, topic, onNotification, onResync);
        lock (_lock)
        {
            _subscriptions = [.. _subscriptions, subscription];
        }

        return subscription;
    }

    public Task StartAsync(CancellationToken cancellationToken)
    {
        lock (_lock)
        {
            if (_loop is not null)
            {
                throw new InvalidOperationException("The notifier was already started.");
            }

            _stop = new CancellationTokenSource();
            var stop = _stop.Token;
            _loop = Task.Run(() => RunAsync(stop), CancellationToken.None);
        }

        return Task.CompletedTask;
    }

    public async Task StopAsync(CancellationToken cancellationToken)
    {
        Task? loop;
        CancellationTokenSource? stop;
        lock (_lock)
        {
            (loop, stop) = (_loop, _stop);
        }

        if (loop is null || stop is null)
        {
            return;
        }

        await stop.CancelAsync().ConfigureAwait(false);
        await loop.WaitAsync(cancellationToken).ConfigureAwait(false);
    }

    /// <summary>Stops the notifier, waiting at most for the connection's dispose timeout.</summary>
    public async ValueTask DisposeAsync()
    {
        await StopAsync(CancellationToken.None).ConfigureAwait(false);
        _stop?.Dispose();
    }

    private async Task RunAsync(CancellationToken stop)
    {
        var failures = 0;
        while (!stop.IsCancellationRequested)
        {
            IKopiListenerConnection? connection = null;
            try
            {
                connection = await _listener.ConnectAsync(stop).ConfigureAwait(false);
                failures = 0;
                Volatile.Write(ref _connected, 1);
                LogConnected();
                Resync();
                while (true)
                {
                    var notification = await connection.WaitForNotificationAsync(stop).ConfigureAwait(false);
                    Dispatch(notification);
                }
            }
            catch (Exception) when (stop.IsCancellationRequested)
            {
                // A provider can report a command the stop cancelled as its own error (SqlClient: a SqlException), not a cancellation.
            }
            catch (KopiQueueConfigurationException e)
            {
                LogDisabled(e);
                IsDisabled = true;
                return;
            }
#pragma warning disable CA1031 // Any other failure is a lost connection: reconnect.
            catch (Exception e)
#pragma warning restore CA1031
            {
                LogConnectionLost(e, failures + 1);
            }
            finally
            {
                Volatile.Write(ref _connected, 0);
                if (connection is not null)
                {
                    var dispose = DisposeWithTimeoutAsync(connection);
                    if (stop.IsCancellationRequested)
                    {
                        await dispose.ConfigureAwait(false);
                    }
                }
            }

            if (stop.IsCancellationRequested)
            {
                break;
            }

            var delay = ReconnectDelay(failures++);
            try
            {
                await Task.Delay(delay, _time, stop).ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                break;
            }
        }
    }

    /// <summary>
    /// Exponential backoff: the minimum delay doubled per failure and capped, then shortened by up to 20% at random, so
    /// clients that lost the database together do not all reconnect at the same moment, even at the cap.
    /// </summary>
    internal TimeSpan ReconnectDelay(int failures)
    {
        var capped = Math.Min(_options.ReconnectMinDelay.Ticks * Math.Pow(2, Math.Min(failures, 30)), _options.ReconnectMaxDelay.Ticks);
        return TimeSpan.FromTicks((long)(capped * (1 - (0.2 * _random()))));
    }

    private async Task DisposeWithTimeoutAsync(IKopiListenerConnection connection)
    {
        Task dispose;
        try
        {
            dispose = connection.DisposeAsync().AsTask();
        }
#pragma warning disable CA1031 // Closing a broken connection may fail in any way; it is discarded either way.
        catch (Exception e)
#pragma warning restore CA1031
        {
            LogDisposeFailed(e);
            return;
        }

        try
        {
            await dispose.WaitAsync(_options.DisposeTimeout, _time).ConfigureAwait(false);
        }
        catch (TimeoutException)
        {
            LogDisposeAbandoned(_options.DisposeTimeout.TotalSeconds);
            _ = dispose.ContinueWith(t => _ = t.Exception, CancellationToken.None, TaskContinuationOptions.OnlyOnFaulted, TaskScheduler.Default);
        }
#pragma warning disable CA1031 // See above.
        catch (Exception e)
#pragma warning restore CA1031
        {
            LogDisposeFailed(e);
        }
    }

    private void Resync()
    {
        foreach (var subscription in Volatile.Read(ref _subscriptions))
        {
            Invoke(subscription, static s => s.OnResync());
        }
    }

    private void Dispatch(KopiNotification notification)
    {
        foreach (var subscription in Volatile.Read(ref _subscriptions))
        {
            if (subscription.Topic == notification.Topic)
            {
                Invoke(subscription, s => s.OnNotification(notification.Payload));
            }
        }
    }

    private void Invoke(Subscription subscription, Action<Subscription> callback)
    {
        if (subscription.IsDisposed)
        {
            return;
        }

        try
        {
            callback(subscription);
        }
#pragma warning disable CA1031 // One subscriber's failure must not stop the notifications of the others.
        catch (Exception e)
#pragma warning restore CA1031
        {
            LogCallbackFailed(e, subscription.Topic);
        }
    }

    private void Unsubscribe(Subscription subscription)
    {
        lock (_lock)
        {
            _subscriptions = [.. _subscriptions.Where(s => !ReferenceEquals(s, subscription))];
        }
    }

    [LoggerMessage(EventId = 1100, Level = LogLevel.Debug, Message = "Notifier connected; waking every subscriber to resync")]
    private partial void LogConnected();

    [LoggerMessage(EventId = 1101, Level = LogLevel.Warning, Message = "Notifier connection lost (attempt {Attempt}); reconnecting, and polling meanwhile")]
    private partial void LogConnectionLost(Exception exception, int attempt);

    [LoggerMessage(EventId = 1102, Level = LogLevel.Error, Message = "Notifications are disabled by a configuration error; the client only polls")]
    private partial void LogDisabled(Exception exception);

    [LoggerMessage(EventId = 1103, Level = LogLevel.Warning, Message = "A notifier connection did not close within {TimeoutSeconds} s and was abandoned")]
    private partial void LogDisposeAbandoned(double timeoutSeconds);

    [LoggerMessage(EventId = 1104, Level = LogLevel.Debug, Message = "Closing a notifier connection failed")]
    private partial void LogDisposeFailed(Exception exception);

    [LoggerMessage(EventId = 1105, Level = LogLevel.Error, Message = "A {Topic} notification subscriber failed")]
    private partial void LogCallbackFailed(Exception exception, NotificationTopic topic);

    private sealed class Subscription(Notifier owner, NotificationTopic topic, Action<string> onNotification, Action onResync) : IDisposable
    {
        private int _disposed;

        public NotificationTopic Topic { get; } = topic;

        public bool IsDisposed => Volatile.Read(ref _disposed) == 1;

        public void OnNotification(string payload) => onNotification(payload);

        public void OnResync() => onResync();

        public void Dispose()
        {
            if (Interlocked.Exchange(ref _disposed, 1) == 0)
            {
                owner.Unsubscribe(this);
            }
        }
    }
}
