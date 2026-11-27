using System.Globalization;
using KopiQueue.Drivers;
using KopiQueue.Execution;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace KopiQueue;

/// <summary>Working jobs: starting and stopping the client (plan task 2.7, FR-HST-03; design §3.2, §5.9).</summary>
public sealed partial class KopiClient
{
    private const int MaxQueueWorkers = 10_000;

    /// <summary>The longest client ID the leader lease stores (<c>river_leader.leader_id</c>).</summary>
    private const int MaxClientIdLength = 127;

    private readonly Lock _lifecycle = new();
    private Running? _running;
    private Task? _stopping;
    private bool _started;

    /// <summary>Whether the client is working jobs: started, and not yet stopped.</summary>
    public bool IsWorking
    {
        get
        {
            lock (_lifecycle)
            {
                return _running is not null && _stopping is null;
            }
        }
    }

    /// <summary>
    /// Starts working the configured <see cref="KopiClientOptions.Queues"/>: one producer per queue, the executor, the
    /// completer and, unless <see cref="KopiClientOptions.NotificationMode"/> says otherwise, the notifier. An insert-only
    /// client (no queues and no workers) has nothing to start, and stays without connections or background work.
    /// </summary>
    /// <remarks>
    /// The configuration is checked first (FR-DEF-06), and nothing starts if it is wrong. A client starts once; after
    /// <see cref="StopAsync"/>, create a new one.
    /// </remarks>
    /// <param name="cancellationToken">Cancels starting.</param>
    /// <exception cref="KopiQueueConfigurationException">The queues, workers or options are not valid together.</exception>
    /// <exception cref="InvalidOperationException">The client was already started.</exception>
    public async Task StartAsync(CancellationToken cancellationToken = default)
    {
        lock (_lifecycle)
        {
            if (_started)
            {
                throw new InvalidOperationException("The client was already started; a stopped client cannot be started again.");
            }

            _started = true;
        }

        var queues = _options.Queues ?? new Dictionary<string, QueueOptions>();
        if (queues.Count == 0 && (_options.Workers is null || _options.Workers.Kinds.Count == 0))
        {
            return; // insert-only (FR-INS-08)
        }

        var running = Build(queues);
        try
        {
            if (running.Notifier is { } notifier)
            {
                await notifier.StartAsync(cancellationToken).ConfigureAwait(false);
            }

            await running.Elector.StartAsync(cancellationToken).ConfigureAwait(false);
            await running.Completer.StartAsync(cancellationToken).ConfigureAwait(false);
            foreach (var producer in running.Producers)
            {
                await producer.StartAsync(cancellationToken).ConfigureAwait(false);
            }
        }
        catch
        {
            await running.DisposeAsync().ConfigureAwait(false);
            throw;
        }

        lock (_lifecycle)
        {
            _running = running;
        }

        if (running.Logger.IsEnabled(LogLevel.Information))
        {
            var names = string.Join(", ", running.Producers.Select(p => p.Queue));
            LogStarted(running.Logger, ClientId, names, running.Notifier is not null);
        }
    }

    /// <summary>
    /// Stops working jobs (FR-HST-03, design §5.9): stops fetching, then waits for the running jobs (soft stop). When
    /// <paramref name="cancellationToken"/> fires, or <see cref="KopiClientOptions.SoftStopTimeout"/> passes, it cancels
    /// the running jobs' tokens (hard stop) and waits at most <see cref="KopiClientOptions.HardStopGrace"/> for them.
    /// Then it writes the pending outcomes, again within the grace period, resigns leadership if it leads (FR-MNT-01),
    /// and closes the notifier. Jobs still running after that stay <c>running</c> in the database and are rescued later
    /// (at-least-once).
    /// </summary>
    /// <remarks>Calling it again, or on a client that never started, is harmless; inserting still works afterwards.</remarks>
    /// <param name="cancellationToken">Ends the soft stop; for a hosted client, the host's shutdown token.</param>
    public Task StopAsync(CancellationToken cancellationToken = default)
    {
        lock (_lifecycle)
        {
            if (_running is null)
            {
                return Task.CompletedTask;
            }

            return _stopping ??= StopCoreAsync(_running, cancellationToken);
        }
    }

    private static string DefaultClientId()
    {
        // The leader lease takes at most 127 characters; host names can be longer.
        var machine = Environment.MachineName;
        machine = machine.Length > 100 ? machine[..100] : machine;
        return string.Create(CultureInfo.InvariantCulture, $"{machine}-{Environment.ProcessId}-{Random.Shared.Next(0x1000000):x6}");
    }

    private async Task StopCoreAsync(Running running, CancellationToken cancellationToken)
    {
        if (running.Logger.IsEnabled(LogLevel.Information))
        {
            var count = running.Producers.Sum(p => p.RunningCount);
            LogStopping(running.Logger, ClientId, count);
        }
        using var soft = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        if (_options.SoftStopTimeout is { } timeout)
        {
            soft.CancelAfter(timeout);
        }

        // 1. Stop fetching and unsubscribe; a fetch in progress still starts its jobs.
        await Task.WhenAll(running.Producers.Select(p => p.StopFetchingAsync())).ConfigureAwait(false);

        // 2-3. Soft stop, then hard stop.
        try
        {
            await Task.WhenAll(running.Producers.Select(p => p.WaitForRunningAsync(soft.Token))).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (soft.IsCancellationRequested)
        {
            await Task.WhenAll(running.Producers.Select(p => p.HardStopAsync())).ConfigureAwait(false);
        }

        // 4. Write what is pending, interrupted jobs included, within the grace period.
        using (var flush = new CancellationTokenSource(_options.HardStopGrace, _timeProvider))
        {
            await running.Completer.StopAsync(flush.Token).ConfigureAwait(false);
        }

        // 5. Resign leadership, so another client takes over at once, within the grace period.
        using (var resign = new CancellationTokenSource(_options.HardStopGrace, _timeProvider))
        {
            await running.Elector.StopAsync(resign.Token).ConfigureAwait(false);
        }

        // 6. Close the notifier.
        await running.DisposeAsync().ConfigureAwait(false);
        LogStopped(running.Logger, ClientId);
    }

    /// <summary>
    /// Everything wrong with <paramref name="options"/> on its own, without the driver (FR-DEF-06): queues and workers
    /// configured together, queue names and concurrency, the workers, and the times. Empty when it is valid. An insert-only
    /// configuration (no queues and no workers) only has its times checked.
    /// </summary>
    internal static List<string> ConfigurationErrors(KopiClientOptions options)
    {
        var errors = new List<string>();
        var queues = options.Queues ?? new Dictionary<string, QueueOptions>();
        var workers = options.Workers;
        var hasWorkers = workers is not null && workers.Kinds.Count > 0;
        if (queues.Count > 0 || hasWorkers)
        {
            if (queues.Count == 0)
            {
                errors.Add("Workers are registered but no queues are configured; set KopiClientOptions.Queues.");
            }

            if (workers is null)
            {
                errors.Add("Queues are configured but no workers are registered; set KopiClientOptions.Workers.");
            }
            else
            {
                try
                {
                    workers.ValidateForWork();
                }
                catch (KopiQueueConfigurationException e)
                {
                    errors.Add(e.Message);
                }
            }

            foreach (var (name, queue) in queues)
            {
                if (JobValidation.QueueError(name) is { } error)
                {
                    errors.Add($"Queue '{name}': {error}");
                }

                if (queue is null || queue.MaxWorkers is < 1 or > MaxQueueWorkers)
                {
                    errors.Add(
                        $"Queue '{name}': MaxWorkers must be between 1 and {MaxQueueWorkers}, not {queue?.MaxWorkers.ToString(CultureInfo.InvariantCulture) ?? "missing"}.");
                }
            }
        }

        Check(options.JobTimeout > TimeSpan.Zero || options.JobTimeout == Timeout.InfiniteTimeSpan, nameof(KopiClientOptions.JobTimeout), "must be positive, or infinite");
        Check(options.FetchPollInterval > TimeSpan.Zero, nameof(KopiClientOptions.FetchPollInterval), "must be positive");
        Check(options.FetchCooldown >= TimeSpan.Zero, nameof(KopiClientOptions.FetchCooldown), "must not be negative");
        Check(options.FetchBatchMax >= 1, nameof(KopiClientOptions.FetchBatchMax), "must be at least 1");
        Check(options.SoftStopTimeout is null || options.SoftStopTimeout >= TimeSpan.Zero, nameof(KopiClientOptions.SoftStopTimeout), "must not be negative");
        Check(options.HardStopGrace >= TimeSpan.Zero, nameof(KopiClientOptions.HardStopGrace), "must not be negative");
        Check(options.ClientId is null || !string.IsNullOrWhiteSpace(options.ClientId), nameof(KopiClientOptions.ClientId), "must not be empty");
        Check(options.ClientId is null || options.ClientId.Length <= MaxClientIdLength, nameof(KopiClientOptions.ClientId), $"must be at most {MaxClientIdLength} characters");
        Check(options.ElectInterval > TimeSpan.Zero, nameof(KopiClientOptions.ElectInterval), "must be positive");
        Check(options.LeaderTtl >= options.ElectInterval * 2, nameof(KopiClientOptions.LeaderTtl), "must be at least twice ElectInterval");
        Check(options.QueueRetention is null || options.QueueRetention > TimeSpan.Zero, nameof(KopiClientOptions.QueueRetention), "must be positive, or null to keep queues");
        Check(Enum.IsDefined(options.NotificationMode), nameof(KopiClientOptions.NotificationMode), "is not a known mode");
        Check(Enum.IsDefined(options.CompleterMode), nameof(KopiClientOptions.CompleterMode), "is not a known mode");
        return errors;

        void Check(bool valid, string option, string rule)
        {
            if (!valid)
            {
                errors.Add($"KopiClientOptions.{option} {rule}.");
            }
        }
    }

    /// <summary>Checks the configuration and builds the components, starting none of them.</summary>
    private Running Build(IReadOnlyDictionary<string, QueueOptions> queues)
    {
        if (ConfigurationErrors(_options) is [_, ..] errors)
        {
            throw new KopiQueueConfigurationException(string.Join(Environment.NewLine, errors));
        }

        var workers = _options.Workers!;
        var listener = _options.NotificationMode == NotificationMode.PollOnly ? null : _driver.CreateListener();
        if (_options.NotificationMode == NotificationMode.ListenNotify && listener is null)
        {
            throw new KopiQueueConfigurationException("NotificationMode is ListenNotify, but the driver cannot listen; use Auto or PollOnly.");
        }

        var loggers = _options.LoggerFactory ?? NullLoggerFactory.Instance;
        var completer = JobCompleter.Create(
            _options.CompleterMode switch
            {
                CompleterMode.Async => CompleterKind.Async,
                CompleterMode.Inline => CompleterKind.Inline,
                _ => CompleterKind.Batch,
            },
            _driver,
            timeProvider: _timeProvider,
            logger: loggers.CreateLogger<JobCompleter>());
        var executor = new JobExecutor(
            workers,
            _serializer,
            completer,
            _options.Services,
            new JobExecutorOptions { JobTimeout = _options.JobTimeout, RetryPolicy = _options.RetryPolicy },
            _timeProvider,
            loggers.CreateLogger<JobExecutor>());
        var notifier = listener is null ? null : new Notifier(listener, timeProvider: _timeProvider, logger: loggers.CreateLogger<Notifier>());
        var maintenance = new MaintenanceHost(MaintenanceServices(), timeProvider: _timeProvider, logger: loggers.CreateLogger<MaintenanceHost>());
        var elector = new Elector(
            _driver,
            new ElectorOptions { ClientId = ClientId, ElectInterval = _options.ElectInterval, LeaderTtl = _options.LeaderTtl },
            notifier,
            maintenance.OnLeadershipChangedAsync,
            _timeProvider,
            loggers.CreateLogger<Elector>());
        var producers = queues
            .OrderBy(q => q.Key, StringComparer.Ordinal)
            .Select(q => new Producer(
                _driver,
                new ProducerOptions
                {
                    Queue = q.Key,
                    MaxWorkers = q.Value.MaxWorkers,
                    ClientId = ClientId,
                    FetchBatchMax = _options.FetchBatchMax,
                    FetchCooldown = _options.FetchCooldown,
                    FetchPollInterval = _options.FetchPollInterval,
                    HardStopGrace = _options.HardStopGrace,
                },
                executor.ExecuteAsync,
                notifier,
                LocalWaker,
                _timeProvider,
                loggers.CreateLogger<Producer>()))
            .ToArray();
        return new Running(producers, completer, elector, maintenance, notifier, loggers.CreateLogger<KopiClient>());
    }

    /// <summary>The leader-only maintenance services (design §5.7), which run while this client leads.</summary>
    /// <remarks>The scheduler, rescuer and job cleaner join with plan tasks 3.3–3.5.</remarks>
    private IMaintenanceService[] MaintenanceServices() =>
        _options.QueueRetention is { } queueRetention ? [new QueueCleaner(_driver, queueRetention)] : [];

    [LoggerMessage(EventId = 1000, Level = LogLevel.Information, Message = "Client {ClientId} started working queues {Queues} (notifications: {Listening})")]
    private static partial void LogStarted(ILogger logger, string clientId, string queues, bool listening);

    [LoggerMessage(EventId = 1001, Level = LogLevel.Information, Message = "Client {ClientId} is stopping; {Running} jobs are running")]
    private static partial void LogStopping(ILogger logger, string clientId, int running);

    [LoggerMessage(EventId = 1002, Level = LogLevel.Information, Message = "Client {ClientId} stopped")]
    private static partial void LogStopped(ILogger logger, string clientId);

    /// <summary>The components of a working client.</summary>
    private sealed class Running(
        Producer[] producers, JobCompleter completer, Elector elector, MaintenanceHost maintenance, Notifier? notifier, ILogger logger) : IAsyncDisposable
    {
        public Producer[] Producers { get; } = producers;

        public JobCompleter Completer { get; } = completer;

        public Elector Elector { get; } = elector;

        public MaintenanceHost Maintenance { get; } = maintenance;

        public Notifier? Notifier { get; } = notifier;

        public ILogger Logger { get; } = logger;

        public async ValueTask DisposeAsync()
        {
            foreach (var producer in Producers)
            {
                await producer.DisposeAsync().ConfigureAwait(false);
            }

            await Completer.DisposeAsync().ConfigureAwait(false);
            await Elector.DisposeAsync().ConfigureAwait(false);
            await Maintenance.DisposeAsync().ConfigureAwait(false);
            if (Notifier is not null)
            {
                await Notifier.DisposeAsync().ConfigureAwait(false);
            }
        }
    }
}
