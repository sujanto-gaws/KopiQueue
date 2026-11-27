using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace KopiQueue.Execution;

/// <summary>
/// A maintenance service that runs only on the leader (design §3, §5.7): the scheduler, cleaners and the like. It works
/// in bounded batches, each a statement of its own, so it never holds locks for long (FR-MNT-07).
/// </summary>
internal interface IMaintenanceService
{
    /// <summary>The service's name, for logs.</summary>
    string Name { get; }

    /// <summary>How long to wait after a batch that was not full, before jitter.</summary>
    TimeSpan Interval { get; }

    /// <summary>The most rows one batch handles. A batch that handles this many is followed by another at once.</summary>
    int BatchSize { get; }

    /// <summary>Runs one batch and returns how many rows it handled, at most <see cref="BatchSize"/>.</summary>
    /// <param name="cancellationToken">Fires when the client stops leading or stops.</param>
    Task<int> RunBatchAsync(CancellationToken cancellationToken);
}

/// <summary>Settings of a <see cref="MaintenanceHost"/>.</summary>
internal sealed class MaintenanceHostOptions
{
    /// <summary>How much each wait varies, as a fraction of the service's interval. Default: 0.1 (±10%, design §5.7).</summary>
    public double Jitter { get; init; } = 0.1;
}

/// <summary>
/// Runs the maintenance services while this client leads (plan task 3.2, design §5.7, FR-MNT-07). The elector reports
/// leadership through <see cref="OnLeadershipChangedAsync"/>: gaining it starts one loop per service, and losing it
/// cancels the loops and waits for them, so that maintenance has stopped before the lease can pass to another client.
/// </summary>
/// <remarks>
/// Each loop runs one batch at once, then another at once while batches come back full, and otherwise waits the service's
/// interval with jitter. A failed batch is logged and tried again after the interval; it never ends the loop.
/// </remarks>
internal sealed partial class MaintenanceHost : IAsyncDisposable
{
    private readonly IReadOnlyList<IMaintenanceService> _services;
    private readonly MaintenanceHostOptions _options;
    private readonly TimeProvider _time;
    private readonly ILogger _logger;
    private readonly SemaphoreSlim _transition = new(1, 1);
    private CancellationTokenSource? _stop;
    private Task[] _loops = [];

    public MaintenanceHost(
        IReadOnlyList<IMaintenanceService> services,
        MaintenanceHostOptions? options = null,
        TimeProvider? timeProvider = null,
        ILogger<MaintenanceHost>? logger = null)
    {
        ArgumentNullException.ThrowIfNull(services);
        foreach (var service in services)
        {
            ArgumentNullException.ThrowIfNull(service, nameof(services));
            ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(service.Interval, TimeSpan.Zero, nameof(services));
            ArgumentOutOfRangeException.ThrowIfLessThan(service.BatchSize, 1, nameof(services));
        }

        _services = services;
        _options = options ?? new MaintenanceHostOptions();
        _time = timeProvider ?? TimeProvider.System;
        _logger = logger ?? NullLogger<MaintenanceHost>.Instance;
    }

    /// <summary>Whether the services are running, that is, whether this client leads.</summary>
    public bool IsRunning => Volatile.Read(ref _stop) is not null;

    /// <summary>The elector's callback: starts the services on gaining leadership and stops them on losing it.</summary>
    /// <param name="isLeader">Whether this client now leads.</param>
    /// <param name="cancellationToken">On losing leadership: stops waiting for the loops, which are cancelled either way.</param>
    public Task OnLeadershipChangedAsync(bool isLeader, CancellationToken cancellationToken) =>
        isLeader ? StartAsync() : StopAsync(cancellationToken);

    public async ValueTask DisposeAsync()
    {
        await StopAsync(CancellationToken.None).ConfigureAwait(false);
        _transition.Dispose();
    }

    private async Task StartAsync()
    {
        await _transition.WaitAsync().ConfigureAwait(false);
        try
        {
            if (_stop is not null || _services.Count == 0)
            {
                return;
            }

            var stop = new CancellationTokenSource();
            _loops = [.. _services.Select(service => Task.Run(() => RunAsync(service, stop.Token), CancellationToken.None))];
            Volatile.Write(ref _stop, stop);
            LogStarted(_services.Count);
        }
        finally
        {
            _transition.Release();
        }
    }

    private async Task StopAsync(CancellationToken cancellationToken)
    {
        await _transition.WaitAsync(CancellationToken.None).ConfigureAwait(false);
        try
        {
            var stop = _stop;
            if (stop is null)
            {
                return;
            }

            await stop.CancelAsync().ConfigureAwait(false);
            try
            {
                await Task.WhenAll(_loops).WaitAsync(cancellationToken).ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                // Stop waiting; the cancelled loops end on their own.
            }

            Volatile.Write(ref _stop, null);
            _loops = [];
            stop.Dispose();
            LogStopped(_services.Count);
        }
        finally
        {
            _transition.Release();
        }
    }

    private async Task RunAsync(IMaintenanceService service, CancellationToken stop)
    {
        while (!stop.IsCancellationRequested)
        {
            TimeSpan wait;
            try
            {
                var handled = await service.RunBatchAsync(stop).ConfigureAwait(false);
                if (handled > 0)
                {
                    LogBatch(service.Name, handled);
                }

                wait = handled >= service.BatchSize ? TimeSpan.Zero : Jittered(service.Interval);
            }
            catch (OperationCanceledException) when (stop.IsCancellationRequested)
            {
                return;
            }
            catch (Exception e)
            {
                LogBatchFailed(e, service.Name, service.Interval);
                wait = Jittered(service.Interval);
            }

            try
            {
                if (wait > TimeSpan.Zero)
                {
                    await Task.Delay(wait, _time, stop).ConfigureAwait(false);
                }
                else
                {
                    await Task.Yield();
                }
            }
            catch (OperationCanceledException) when (stop.IsCancellationRequested)
            {
                return;
            }
        }
    }

    private TimeSpan Jittered(TimeSpan interval) =>
        _options.Jitter <= 0 ? interval : interval * (1 + (_options.Jitter * ((Random.Shared.NextDouble() * 2) - 1)));

    [LoggerMessage(EventId = 2400, Level = LogLevel.Information, Message = "Started {Count} maintenance services as leader")]
    private partial void LogStarted(int count);

    [LoggerMessage(EventId = 2401, Level = LogLevel.Information, Message = "Stopped {Count} maintenance services")]
    private partial void LogStopped(int count);

    [LoggerMessage(EventId = 2402, Level = LogLevel.Debug, Message = "Maintenance service {Service} handled {Rows} rows")]
    private partial void LogBatch(string service, int rows);

    [LoggerMessage(EventId = 2403, Level = LogLevel.Warning, Message = "Maintenance service {Service} failed; it tries again in about {Interval}")]
    private partial void LogBatchFailed(Exception exception, string service, TimeSpan interval);
}
