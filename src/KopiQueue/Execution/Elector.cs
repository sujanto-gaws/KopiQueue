using System.Text.Json;
using KopiQueue.Drivers;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace KopiQueue.Execution;

/// <summary>Settings of an <see cref="Elector"/> (design §5.5, §7).</summary>
internal sealed class ElectorOptions
{
    /// <summary>The client's ID, which the lease records.</summary>
    public required string ClientId { get; init; }

    /// <summary>How often a follower tries to win and a leader renews. Default: 5 s.</summary>
    public TimeSpan ElectInterval { get; init; } = TimeSpan.FromSeconds(5);

    /// <summary>How long a lease lasts after it is won or renewed. Default: 15 s; at least twice <see cref="ElectInterval"/>.</summary>
    public TimeSpan LeaderTtl { get; init; } = TimeSpan.FromSeconds(15);

    /// <summary>How much each wait varies, as a fraction of <see cref="ElectInterval"/>, so clients spread out. Default: 0.1 (±10%).</summary>
    public double Jitter { get; init; } = 0.1;
}

/// <summary>
/// Holds the leader lease for one client (design §3, §5.5; FR-MNT-01). A follower tries to win every
/// <see cref="ElectorOptions.ElectInterval"/>, and at once when the leader announces that it resigned or the notifier
/// reconnects. A leader renews every interval. It steps down when another client holds the lease, or when renewals have
/// failed until a third of the lease is left (the safety margin), so that its maintenance has stopped before another
/// client can win. On stop a leader steps down and resigns, which lets another client win at once.
/// </summary>
/// <remarks>
/// Leadership changes are reported to the callback given to the constructor, which is awaited: while it runs, the elector
/// neither renews nor tries to win. It starts and stops the maintenance services (task 3.2).
/// </remarks>
internal sealed partial class Elector : IStartStopService, IAsyncDisposable
{
    private readonly IKopiDriver _driver;
    private readonly ElectorOptions _options;
    private readonly Notifier? _notifier;
    private readonly Func<bool, CancellationToken, Task>? _onLeadershipChanged;
    private readonly TimeProvider _time;
    private readonly ILogger _logger;
    private readonly WakeSignal _wake = new();
    private readonly Lock _lifecycle = new();
    private readonly LeaderElectParams _params;
    private CancellationTokenSource? _stop;
    private Task? _loop;
    private IDisposable? _subscription;
    private Task? _stopping;
    private int _isLeader;

    // When the current lease must be treated as lost if it has not been renewed: TTL − TTL/3 after the start of the last
    // successful election or renewal, measured on this client's clock before the statement was sent.
    private DateTimeOffset _stepDownBy;

    public Elector(
        IKopiDriver driver,
        ElectorOptions options,
        Notifier? notifier = null,
        Func<bool, CancellationToken, Task>? onLeadershipChanged = null,
        TimeProvider? timeProvider = null,
        ILogger<Elector>? logger = null)
    {
        ArgumentNullException.ThrowIfNull(driver);
        ArgumentNullException.ThrowIfNull(options);
        ArgumentException.ThrowIfNullOrEmpty(options.ClientId, nameof(options));
        ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(options.ElectInterval, TimeSpan.Zero, nameof(options));
        if (options.LeaderTtl < options.ElectInterval * 2)
        {
            throw new ArgumentOutOfRangeException(nameof(options), "LeaderTtl must be at least twice ElectInterval.");
        }

        _driver = driver;
        _options = options;
        _notifier = notifier;
        _onLeadershipChanged = onLeadershipChanged;
        _time = timeProvider ?? TimeProvider.System;
        _logger = logger ?? NullLogger<Elector>.Instance;
        _params = new LeaderElectParams { LeaderId = options.ClientId, Ttl = options.LeaderTtl };
    }

    /// <summary>
    /// Whether this client holds the lease, as far as it knows. It turns <c>false</c> only after the leadership callback
    /// has returned, so nothing runs as leader once it is <c>false</c>.
    /// </summary>
    public bool IsLeader => Volatile.Read(ref _isLeader) == 1;

    public Task StartAsync(CancellationToken cancellationToken)
    {
        lock (_lifecycle)
        {
            if (_loop is not null || _stopping is not null)
            {
                throw new InvalidOperationException("The elector was already started.");
            }

            _subscription = _notifier?.Subscribe(NotificationTopic.Leadership, OnLeadershipNotification, onResync: WakeFollower);
            _stop = new CancellationTokenSource();
            var stop = _stop.Token;
            _loop = Task.Run(() => RunAsync(stop), CancellationToken.None);
        }

        return Task.CompletedTask;
    }

    /// <summary>
    /// Stops electing. A leader steps down, which awaits the leadership callback, and then resigns, so another client
    /// can win at once. <paramref name="cancellationToken"/> abandons the resignation; the lease then expires on its own.
    /// </summary>
    public Task StopAsync(CancellationToken cancellationToken)
    {
        lock (_lifecycle)
        {
            if (_loop is null)
            {
                return Task.CompletedTask;
            }

            return _stopping ??= StopCoreAsync(cancellationToken);
        }
    }

    /// <summary>Stops electing without resigning, as a crash would: the lease expires on its own.</summary>
    public async ValueTask DisposeAsync()
    {
        Task? loop;
        lock (_lifecycle)
        {
            loop = _loop;
            _stopping ??= Task.CompletedTask;
        }

        _subscription?.Dispose();
        if (_stop is not null)
        {
            await _stop.CancelAsync().ConfigureAwait(false);
        }

        if (loop is not null)
        {
            await loop.ConfigureAwait(false);
        }

        _stop?.Dispose();
    }

    private async Task StopCoreAsync(CancellationToken cancellationToken)
    {
        _subscription?.Dispose();
        await _stop!.CancelAsync().ConfigureAwait(false);
        await _loop!.ConfigureAwait(false);

        if (IsLeader)
        {
            await StepDownAsync("the client is stopping", cancellationToken).ConfigureAwait(false);
            try
            {
                if (await _driver.GetExecutor().LeaderResignAsync(_options.ClientId, cancellationToken).ConfigureAwait(false))
                {
                    LogResigned(_options.ClientId);
                }
            }
            catch (Exception e) when (e is not OperationCanceledException || cancellationToken.IsCancellationRequested)
            {
                LogResignFailed(e, _options.ClientId, _options.LeaderTtl);
            }
        }
    }

    private async Task RunAsync(CancellationToken stop)
    {
        while (!stop.IsCancellationRequested)
        {
            try
            {
                if (IsLeader)
                {
                    await RenewAsync(stop).ConfigureAwait(false);
                }
                else
                {
                    await TryElectAsync(stop).ConfigureAwait(false);
                }

                var wait = Jittered(_options.ElectInterval);
                if (IsLeader)
                {
                    // Never sleep past the step-down time: a hung renewal must not keep maintenance running.
                    var left = _stepDownBy - _time.GetUtcNow();
                    wait = left < wait ? (left > TimeSpan.Zero ? left : TimeSpan.Zero) : wait;
                }

                await _wake.WaitAsync(wait, _time, stop).ConfigureAwait(false);
            }
            catch (Exception) when (stop.IsCancellationRequested)
            {
                // A provider can report a command the stop cancelled as its own error (SqlClient: a SqlException), not a cancellation.
                return;
            }
        }
    }

    private async Task TryElectAsync(CancellationToken stop)
    {
        var started = _time.GetUtcNow();
        bool won;
        try
        {
            won = await _driver.GetExecutor().LeaderAttemptElectAsync(_params, stop).ConfigureAwait(false);
        }
        catch (Exception e) when (!stop.IsCancellationRequested)
        {
            LogElectFailed(e, _options.ClientId);
            return;
        }

        if (won)
        {
            _stepDownBy = StepDownTime(started);
            Volatile.Write(ref _isLeader, 1);
            LogElected(_options.ClientId);
            await ReportAsync(true, stop).ConfigureAwait(false);
        }
    }

    private async Task RenewAsync(CancellationToken stop)
    {
        var started = _time.GetUtcNow();
        var left = _stepDownBy - started;
        if (left <= TimeSpan.Zero)
        {
            await StepDownAsync("its lease could not be renewed in time", stop).ConfigureAwait(false);
            return;
        }

        bool renewed;
        using (var timeout = new CancellationTokenSource(left, _time))
        using (var deadline = CancellationTokenSource.CreateLinkedTokenSource(stop, timeout.Token))
        {
            try
            {
                renewed = await _driver.GetExecutor().LeaderAttemptReelectAsync(_params, deadline.Token).ConfigureAwait(false);
            }
            catch (Exception e) when (!stop.IsCancellationRequested)
            {
                LogRenewFailed(e, _options.ClientId);
                if (_time.GetUtcNow() >= _stepDownBy)
                {
                    await StepDownAsync("its lease could not be renewed in time", stop).ConfigureAwait(false);
                }

                return;
            }
        }

        if (renewed)
        {
            _stepDownBy = StepDownTime(started);
        }
        else
        {
            await StepDownAsync("another client holds the lease", stop).ConfigureAwait(false);
        }
    }

    private async Task StepDownAsync(string reason, CancellationToken cancellationToken)
    {
        // Stop maintenance first, so that IsLeader is false only once nothing runs as leader any more.
        LogSteppedDown(_options.ClientId, reason);
        await ReportAsync(false, cancellationToken).ConfigureAwait(false);
        Volatile.Write(ref _isLeader, 0);
    }

    private async Task ReportAsync(bool isLeader, CancellationToken cancellationToken)
    {
        if (_onLeadershipChanged is null)
        {
            return;
        }

        try
        {
            await _onLeadershipChanged(isLeader, cancellationToken).ConfigureAwait(false);
        }
        catch (Exception e) when (e is not OperationCanceledException || !cancellationToken.IsCancellationRequested)
        {
            LogCallbackFailed(e, isLeader);
        }
    }

    private DateTimeOffset StepDownTime(DateTimeOffset started) => started + _options.LeaderTtl - (_options.LeaderTtl / 3);

    private TimeSpan Jittered(TimeSpan interval) =>
        _options.Jitter <= 0 ? interval : interval * (1 + (_options.Jitter * ((Random.Shared.NextDouble() * 2) - 1)));

    private void WakeFollower()
    {
        if (!IsLeader)
        {
            _wake.Set();
        }
    }

    /// <summary>Wakes a follower when another client resigns: the lease is free, so trying now wins it at once.</summary>
    private void OnLeadershipNotification(string payload)
    {
        try
        {
            using var json = JsonDocument.Parse(payload);
            var root = json.RootElement;
            if (root.ValueKind == JsonValueKind.Object
                && root.TryGetProperty("action", out var action) && action.ValueEquals("resigned")
                && !(root.TryGetProperty("leader_id", out var leader) && leader.ValueEquals(_options.ClientId)))
            {
                WakeFollower();
            }
        }
        catch (JsonException)
        {
            // Not ours to understand; the next interval tries anyway.
        }
    }

    [LoggerMessage(EventId = 2300, Level = LogLevel.Information, Message = "Client {ClientId} was elected leader")]
    private partial void LogElected(string clientId);

    [LoggerMessage(EventId = 2301, Level = LogLevel.Warning, Message = "Client {ClientId} is no longer leader: {Reason}")]
    private partial void LogSteppedDown(string clientId, string reason);

    [LoggerMessage(EventId = 2302, Level = LogLevel.Information, Message = "Client {ClientId} resigned as leader")]
    private partial void LogResigned(string clientId);

    [LoggerMessage(EventId = 2303, Level = LogLevel.Warning, Message = "Client {ClientId} could not try to become leader")]
    private partial void LogElectFailed(Exception exception, string clientId);

    [LoggerMessage(EventId = 2304, Level = LogLevel.Warning, Message = "Client {ClientId} could not renew its leader lease")]
    private partial void LogRenewFailed(Exception exception, string clientId);

    [LoggerMessage(EventId = 2305, Level = LogLevel.Warning, Message = "Client {ClientId} could not resign as leader; its lease expires within {LeaderTtl}")]
    private partial void LogResignFailed(Exception exception, string clientId, TimeSpan leaderTtl);

    [LoggerMessage(EventId = 2306, Level = LogLevel.Error, Message = "The leadership callback failed (leader: {IsLeader})")]
    private partial void LogCallbackFailed(Exception exception, bool isLeader);
}
