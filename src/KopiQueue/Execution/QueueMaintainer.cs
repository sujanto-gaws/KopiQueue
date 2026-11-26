using System.Text.Json;
using KopiQueue.Drivers;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace KopiQueue.Execution;

/// <summary>Settings of the <see cref="QueueMaintainer"/>.</summary>
internal sealed class QueueMaintainerOptions
{
    /// <summary>
    /// How often the pause state is read from the database while no notifications arrive (poll-only mode, SQL Server, or
    /// the notifier is down): a pause reaches this client within it (FR-QUE-04, FR-DB-05). Default: 1 s.
    /// </summary>
    public TimeSpan PollInterval { get; init; } = TimeSpan.FromSeconds(1);

    /// <summary>How often the queues' rows are marked worked, so the queue cleaner keeps them. Default: 1 minute.</summary>
    public TimeSpan RefreshInterval { get; init; } = TimeSpan.FromMinutes(1);
}

/// <summary>
/// Keeps the rows of the queues a client works and applies their pause state to its producers (design §3, plan task
/// 3.6: FR-QUE-02..04, FR-DB-05). Runs on every client that works queues, leader or not.
/// </summary>
/// <remarks>
/// <para>
/// When it starts, it creates the queues' rows, or marks them worked, and pauses the producers of paused queues before
/// they first fetch. It refreshes the rows every <see cref="QueueMaintainerOptions.RefreshInterval"/>.
/// </para>
/// <para>
/// Pause and resume arrive as control notifications (<c>{"action": "pause", "queue": "email" | "*"}</c>), which apply
/// at once. While the notifier is not connected, or without one, it reads the pause state every
/// <see cref="QueueMaintainerOptions.PollInterval"/>; after the notifier reconnects, it reads it once to catch up. A read
/// that a notification overtook is not applied: it reads again, so a stale row cannot undo a newer pause or resume.
/// </para>
/// </remarks>
internal sealed partial class QueueMaintainer : IStartStopService, IAsyncDisposable
{
    private readonly IKopiDriver _driver;
    private readonly Dictionary<string, Producer> _producers;
    private readonly string[] _names;
    private readonly QueueMaintainerOptions _options;
    private readonly Notifier? _notifier;
    private readonly TimeProvider _time;
    private readonly ILogger _logger;
    private readonly WakeSignal _read = new();
    private readonly Lock _lock = new();
    private long _controlVersion;
    private bool _refreshed;
    private CancellationTokenSource? _stop;
    private Task? _loop;
    private IDisposable? _subscription;

    public QueueMaintainer(
        IKopiDriver driver,
        IReadOnlyList<Producer> producers,
        QueueMaintainerOptions? options = null,
        Notifier? notifier = null,
        TimeProvider? timeProvider = null,
        ILogger<QueueMaintainer>? logger = null)
    {
        ArgumentNullException.ThrowIfNull(driver);
        ArgumentNullException.ThrowIfNull(producers);
        _driver = driver;
        _producers = producers.ToDictionary(p => p.Queue, StringComparer.Ordinal);
        _names = [.. _producers.Keys.Order(StringComparer.Ordinal)];
        _options = options ?? new QueueMaintainerOptions();
        ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(_options.PollInterval, TimeSpan.Zero, nameof(options));
        ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(_options.RefreshInterval, TimeSpan.Zero, nameof(options));
        _notifier = notifier;
        _time = timeProvider ?? TimeProvider.System;
        _logger = (ILogger?)logger ?? NullLogger.Instance;
    }

    /// <summary>
    /// Subscribes to control notifications, creates or refreshes the queues' rows and applies their pause state, then
    /// keeps doing so in the background. A failed first refresh is logged and tried again at the next poll.
    /// </summary>
    public async Task StartAsync(CancellationToken cancellationToken)
    {
        lock (_lock)
        {
            if (_stop is not null)
            {
                throw new InvalidOperationException("The queue maintainer was already started.");
            }

            _stop = new CancellationTokenSource();
            _subscription = _notifier?.Subscribe(NotificationTopic.Control, OnControlNotification, onResync: _read.Set);
        }

        if (_names.Length > 0)
        {
            try
            {
                await RefreshAsync(cancellationToken).ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                throw;
            }
#pragma warning disable CA1031 // The database may be down while the client starts; the producers cope, and so does this.
            catch (Exception e)
#pragma warning restore CA1031
            {
                LogRefreshFailed(e);
            }

            var stop = _stop.Token;
            _loop = Task.Run(() => RunAsync(stop), CancellationToken.None);
        }
    }

    public async Task StopAsync(CancellationToken cancellationToken)
    {
        Task? loop;
        lock (_lock)
        {
            _stop?.Cancel();
            _subscription?.Dispose();
            _subscription = null;
            loop = _loop;
        }

        if (loop is not null)
        {
            await loop.WaitAsync(cancellationToken).ConfigureAwait(false);
        }
    }

    public async ValueTask DisposeAsync()
    {
        await StopAsync(CancellationToken.None).ConfigureAwait(false);
        _stop?.Dispose();
    }

    /// <summary>
    /// Applies a pause or resume to the producers of <paramref name="queue"/>, or all of them for <c>"*"</c>: everywhere
    /// (as the database now says) or, with <paramref name="locally"/>, this client's own.
    /// </summary>
    /// <returns><c>false</c> if no producer here works <paramref name="queue"/>.</returns>
    public bool Apply(string queue, bool paused, bool locally)
    {
        if (!locally)
        {
            Interlocked.Increment(ref _controlVersion);
        }

        if (queue == "*")
        {
            foreach (var producer in _producers.Values)
            {
                producer.SetPaused(paused, locally);
            }

            return true;
        }

        if (!_producers.TryGetValue(queue, out var one))
        {
            return false;
        }

        one.SetPaused(paused, locally);
        return true;
    }

    /// <summary>Handles a control notification: applies a pause or resume; other actions are not this component's.</summary>
    internal void OnControlNotification(string payload)
    {
        string? action;
        string? queue;
        try
        {
            using var json = JsonDocument.Parse(payload);
            var root = json.RootElement;
            if (root.ValueKind != JsonValueKind.Object)
            {
                return;
            }

            action = root.TryGetProperty("action", out var a) && a.ValueKind == JsonValueKind.String ? a.GetString() : null;
            queue = root.TryGetProperty("queue", out var q) && q.ValueKind == JsonValueKind.String ? q.GetString() : null;
        }
        catch (JsonException)
        {
            LogBadNotification();
            return;
        }

        if (queue is { Length: > 0 } && action is "pause" or "resume")
        {
            Apply(queue, paused: action == "pause", locally: false);
        }
    }

    private async Task RunAsync(CancellationToken stop)
    {
        var nextRefresh = _refreshed ? _time.GetUtcNow() + _options.RefreshInterval : _time.GetUtcNow();
        while (!stop.IsCancellationRequested)
        {
            try
            {
                var asked = await _read.WaitAsync(_options.PollInterval, _time, stop).ConfigureAwait(false);
                _read.Clear();
                if (_time.GetUtcNow() >= nextRefresh)
                {
                    await RefreshAsync(stop).ConfigureAwait(false);
                    nextRefresh = _time.GetUtcNow() + _options.RefreshInterval;
                }
                else if (asked || _notifier is not { IsConnected: true })
                {
                    await PollAsync(stop).ConfigureAwait(false);
                }
            }
            catch (OperationCanceledException) when (stop.IsCancellationRequested)
            {
                break;
            }
#pragma warning disable CA1031 // A failed read (for example the database is down) must not end the loop: try again.
            catch (Exception e)
#pragma warning restore CA1031
            {
                LogRefreshFailed(e);
            }
        }
    }

    private async Task RefreshAsync(CancellationToken cancellationToken)
    {
        var version = Interlocked.Read(ref _controlVersion);
        var rows = await _driver.GetExecutor().QueueCreateOrSetUpdatedAtManyAsync(_names, cancellationToken).ConfigureAwait(false);
        _refreshed = true;
        ApplyRows(rows, version);
    }

    private async Task PollAsync(CancellationToken cancellationToken)
    {
        var version = Interlocked.Read(ref _controlVersion);
        var rows = await _driver.GetExecutor()
            .QueueListAsync(new QueueListQuery { Max = _names.Length, Names = _names }, cancellationToken)
            .ConfigureAwait(false);
        ApplyRows(rows, version);
    }

    private void ApplyRows(IReadOnlyList<QueueRow> rows, long version)
    {
        if (Interlocked.Read(ref _controlVersion) != version)
        {
            _read.Set(); // a pause or resume arrived while reading: the rows may predate it, so read again
            return;
        }

        foreach (var row in rows)
        {
            if (_producers.TryGetValue(row.Name, out var producer))
            {
                producer.SetPaused(row.PausedAt is not null, locally: false);
            }
        }
    }

    [LoggerMessage(EventId = 2500, Level = LogLevel.Warning, Message = "Reading or refreshing the client's queues failed; trying again at the next poll")]
    private partial void LogRefreshFailed(Exception exception);

    [LoggerMessage(EventId = 2501, Level = LogLevel.Debug, Message = "Ignored a control notification that is not valid JSON")]
    private partial void LogBadNotification();
}
