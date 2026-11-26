using System.Collections.Concurrent;
using System.Threading.Channels;
using KopiQueue.Drivers;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace KopiQueue.Execution;

/// <summary>How completions are written (design §5.3).</summary>
internal enum CompleterKind
{
    /// <summary>Collected and written in batches: the fewest writes. The default.</summary>
    Batch,

    /// <summary>Each written on its own, many at once.</summary>
    Async,

    /// <summary>Each written before <see cref="IJobCompleter.CompleteAsync"/> returns; mainly for tests.</summary>
    Inline,
}

/// <summary>A completion after it was written: the row as stored, or <c>null</c> if the job no longer exists.</summary>
/// <param name="Completion">What the executor reported.</param>
/// <param name="Row">
/// The row after the write. Its state differs from the completion's when the job was no longer running (rescued,
/// cancelled or deleted meanwhile) or was cancelled while it ran.
/// </param>
internal sealed record CompletedJob(JobCompletion Completion, JobRow? Row);

/// <summary>Settings shared by the completers.</summary>
internal sealed class JobCompleterOptions
{
    /// <summary>The most completions one batch writes. Default: 2,000, and never more than the driver's row limit.</summary>
    public int MaxBatchSize { get; init; } = 2_000;

    /// <summary>How long a batch waits for more completions after its first. Default: 50 ms.</summary>
    public TimeSpan FlushInterval { get; init; } = TimeSpan.FromMilliseconds(50);

    /// <summary>How many completions the batch completer holds before callers wait. Default: twice the batch size.</summary>
    public int? Capacity { get; init; }

    /// <summary>How many writes the async completer runs at once. Default: 100.</summary>
    public int MaxConcurrentWrites { get; init; } = 100;

    /// <summary>The first wait before a failed write is tried again; it doubles per failure. Default: 100 ms.</summary>
    public TimeSpan RetryMinDelay { get; init; } = TimeSpan.FromMilliseconds(100);

    /// <summary>The longest wait between tries of a failed write. Default: 5 s.</summary>
    public TimeSpan RetryMaxDelay { get; init; } = TimeSpan.FromSeconds(5);
}

/// <summary>
/// Writes completions with <see cref="IKopiExecutor.JobSetStateIfRunningManyAsync"/> and reports them once written
/// (FR-EXE-06, NFR-REL-03): <c>onCompleted</c> sees a completion only after the database has committed it.
/// </summary>
/// <remarks>
/// <para>
/// A failed write is retried with backoff until it succeeds or the completer is stopped hard, so a database outage
/// holds completions back without losing them or crashing the host (NFR-REL-04). Jobs whose completions are abandoned
/// at a hard stop stay <c>running</c> and are rescued later (at-least-once).
/// </para>
/// <para>
/// Stop order (design §5.9 step 4): stop the producers first, so nothing new arrives; then <see cref="StopAsync"/>
/// writes what is pending, and abandons it if its token fires first.
/// </para>
/// </remarks>
internal abstract partial class JobCompleter : IJobCompleter, IStartStopService, IAsyncDisposable
{
    private readonly IKopiDriver _driver;
    private readonly Action<IReadOnlyList<CompletedJob>>? _onCompleted;
    private readonly CancellationTokenSource _abort = new();
    private readonly ILogger _logger;

    protected JobCompleter(
        IKopiDriver driver,
        JobCompleterOptions? options,
        Action<IReadOnlyList<CompletedJob>>? onCompleted,
        TimeProvider? timeProvider,
        ILogger? logger)
    {
        ArgumentNullException.ThrowIfNull(driver);
        _driver = driver;
        Options = options ?? new JobCompleterOptions();
        ArgumentOutOfRangeException.ThrowIfLessThan(Options.MaxBatchSize, 1, nameof(options));
        ArgumentOutOfRangeException.ThrowIfLessThan(Options.MaxConcurrentWrites, 1, nameof(options));
        _onCompleted = onCompleted;
        Time = timeProvider ?? TimeProvider.System;
        _logger = logger ?? NullLogger.Instance;
        MaxBatchSize = Math.Min(Options.MaxBatchSize, driver.Capabilities.MaxRowsPerStatement ?? int.MaxValue);
    }

    protected JobCompleterOptions Options { get; }

    protected TimeProvider Time { get; }

    protected int MaxBatchSize { get; }

    /// <summary>Cancelled by a hard stop: pending writes are abandoned.</summary>
    protected CancellationToken Abort => _abort.Token;

    /// <summary>Creates a completer of <paramref name="kind"/>.</summary>
    public static JobCompleter Create(
        CompleterKind kind,
        IKopiDriver driver,
        JobCompleterOptions? options = null,
        Action<IReadOnlyList<CompletedJob>>? onCompleted = null,
        TimeProvider? timeProvider = null,
        ILogger<JobCompleter>? logger = null) => kind switch
        {
            CompleterKind.Batch => new BatchJobCompleter(driver, options, onCompleted, timeProvider, logger),
            CompleterKind.Async => new AsyncJobCompleter(driver, options, onCompleted, timeProvider, logger),
            CompleterKind.Inline => new InlineJobCompleter(driver, options, onCompleted, timeProvider, logger),
            _ => throw new ArgumentOutOfRangeException(nameof(kind), kind, null),
        };

    public abstract ValueTask CompleteAsync(JobCompletion completion, CancellationToken cancellationToken);

    public virtual Task StartAsync(CancellationToken cancellationToken) => Task.CompletedTask;

    /// <summary>Writes what is pending; if <paramref name="cancellationToken"/> fires first, abandons it.</summary>
    public async Task StopAsync(CancellationToken cancellationToken)
    {
        try
        {
            await DrainAsync().WaitAsync(cancellationToken).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            await _abort.CancelAsync().ConfigureAwait(false);
            await DrainAsync().ConfigureAwait(false);
        }
    }

    public async ValueTask DisposeAsync()
    {
        await StopAsync(new CancellationToken(canceled: true)).ConfigureAwait(false);
        _abort.Dispose();
        DisposeCore();
    }

    /// <summary>Stops taking completions and returns when everything taken was written or abandoned.</summary>
    protected abstract Task DrainAsync();

    protected virtual void DisposeCore()
    {
    }

    /// <summary>Logs completions abandoned at a hard stop.</summary>
    protected void Abandoned(int count)
    {
        if (count > 0)
        {
            LogAbandoned(count);
        }
    }

    /// <summary>Writes <paramref name="batch"/>, retrying with backoff until it succeeds or a hard stop abandons it.</summary>
    /// <returns><c>true</c> if written.</returns>
    protected async Task<bool> WriteAsync(IReadOnlyList<JobCompletion> batch)
    {
        var parameters = batch.Select(ToParams).ToArray();
        var delay = Options.RetryMinDelay;
        for (var failures = 0; ; failures++)
        {
            try
            {
                var rows = await _driver.GetExecutor().JobSetStateIfRunningManyAsync(parameters, Abort).ConfigureAwait(false);
                Report(batch, rows);
                return true;
            }
            catch (OperationCanceledException) when (Abort.IsCancellationRequested)
            {
                LogAbandoned(batch.Count);
                return false;
            }
#pragma warning disable CA1031 // A failed write (the database is down, say) is retried, never dropped or thrown at the executor.
            catch (Exception e)
#pragma warning restore CA1031
            {
                LogWriteFailed(e, batch.Count, failures + 1, delay);
            }

            try
            {
                await Task.Delay(delay, Time, Abort).ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                LogAbandoned(batch.Count);
                return false;
            }

            delay = TimeSpan.FromTicks(Math.Min(delay.Ticks * 2, Options.RetryMaxDelay.Ticks));
        }
    }

    private static JobSetStateParams ToParams(JobCompletion c) => new()
    {
        Id = c.Job.Id,
        State = c.State,
        Attempt = c.Attempt,
        Error = c.Error,
        FinalizedAt = c.FinalizedAt,
        MetadataUpdates = c.MetadataUpdates,
        ScheduledAt = c.ScheduledAt,
    };

    private void Report(IReadOnlyList<JobCompletion> batch, IReadOnlyList<JobRow> rows)
    {
        if (_onCompleted is null)
        {
            return;
        }

        var byId = rows.ToDictionary(r => r.Id);
        var completed = batch.Select(c => new CompletedJob(c, byId.GetValueOrDefault(c.Job.Id))).ToArray();
        try
        {
            _onCompleted(completed);
        }
#pragma warning disable CA1031 // A failing subscriber must not undo a write or stop the completer.
        catch (Exception e)
#pragma warning restore CA1031
        {
            LogSubscriberFailed(e);
        }
    }

    [LoggerMessage(EventId = 2200, Level = LogLevel.Warning, Message = "Writing {Count} job completions failed (try {Try}); trying again in {Delay}")]
    private partial void LogWriteFailed(Exception exception, int count, int @try, TimeSpan delay);

    [LoggerMessage(EventId = 2201, Level = LogLevel.Warning, Message = "Abandoned {Count} job completions at a hard stop; the jobs stay running and will be rescued")]
    private partial void LogAbandoned(int count);

    [LoggerMessage(EventId = 2202, Level = LogLevel.Error, Message = "A job completion subscriber failed")]
    private partial void LogSubscriberFailed(Exception exception);
}

/// <summary>Writes each completion before returning (design §5.3, <c>Inline</c>).</summary>
internal sealed class InlineJobCompleter(
    IKopiDriver driver, JobCompleterOptions? options, Action<IReadOnlyList<CompletedJob>>? onCompleted, TimeProvider? timeProvider, ILogger? logger)
    : JobCompleter(driver, options, onCompleted, timeProvider, logger)
{
    public override async ValueTask CompleteAsync(JobCompletion completion, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(completion);
        await WriteAsync([completion]).ConfigureAwait(false);
    }

    /// <summary>Writes happen on the callers' tasks; the producers' stop waits for those.</summary>
    protected override Task DrainAsync() => Task.CompletedTask;
}

/// <summary>Writes each completion on its own, at most <see cref="JobCompleterOptions.MaxConcurrentWrites"/> at once (design §5.3, <c>Async</c>).</summary>
internal sealed class AsyncJobCompleter : JobCompleter
{
    private readonly SemaphoreSlim _slots;
    private readonly ConcurrentDictionary<Task, byte> _writes = new();

    public AsyncJobCompleter(
        IKopiDriver driver, JobCompleterOptions? options, Action<IReadOnlyList<CompletedJob>>? onCompleted, TimeProvider? timeProvider, ILogger? logger)
        : base(driver, options, onCompleted, timeProvider, logger) =>
        _slots = new SemaphoreSlim(Options.MaxConcurrentWrites, Options.MaxConcurrentWrites);

    /// <summary>Returns once the write has started; waits first while the most writes are running (back-pressure).</summary>
    public override async ValueTask CompleteAsync(JobCompletion completion, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(completion);
        await _slots.WaitAsync(cancellationToken).ConfigureAwait(false);
        var write = Task.Run(async () =>
        {
            try
            {
                await WriteAsync([completion]).ConfigureAwait(false);
            }
            finally
            {
                _slots.Release();
            }
        }, CancellationToken.None);
        _writes.TryAdd(write, 0);
        _ = write.ContinueWith(t => _writes.TryRemove(t, out _), CancellationToken.None, TaskContinuationOptions.ExecuteSynchronously, TaskScheduler.Default);
    }

    protected override Task DrainAsync() => Task.WhenAll(_writes.Keys);

    protected override void DisposeCore() => _slots.Dispose();
}

/// <summary>
/// Collects completions and writes them in batches (design §5.3, <c>Batch</c>, the default): a batch is written when
/// it reaches <see cref="JobCompleterOptions.MaxBatchSize"/> or <see cref="JobCompleterOptions.FlushInterval"/> after
/// its first completion, whichever comes first. One batch is written at a time; while it is, the next one collects.
/// </summary>
internal sealed class BatchJobCompleter : JobCompleter
{
    private readonly Channel<JobCompletion> _channel;
    private readonly Lock _lock = new();
    private Task? _loop;

    public BatchJobCompleter(
        IKopiDriver driver, JobCompleterOptions? options, Action<IReadOnlyList<CompletedJob>>? onCompleted, TimeProvider? timeProvider, ILogger? logger)
        : base(driver, options, onCompleted, timeProvider, logger)
    {
        var capacity = Options.Capacity ?? (2 * MaxBatchSize);
        ArgumentOutOfRangeException.ThrowIfLessThan(capacity, 1, nameof(options));
        _channel = Channel.CreateBounded<JobCompletion>(new BoundedChannelOptions(capacity)
        {
            SingleReader = true,
            FullMode = BoundedChannelFullMode.Wait,
        });
    }

    /// <summary>How many completions wait to be written.</summary>
    public int Pending => _channel.Reader.Count;

    public override Task StartAsync(CancellationToken cancellationToken)
    {
        lock (_lock)
        {
            if (_loop is not null)
            {
                throw new InvalidOperationException("The completer was already started.");
            }

            _loop = Task.Run(RunAsync, CancellationToken.None);
        }

        return Task.CompletedTask;
    }

    /// <summary>Returns once the completion is queued; waits first while the queue is full (back-pressure, design §5.3).</summary>
    /// <exception cref="InvalidOperationException">The completer was stopped.</exception>
    public override async ValueTask CompleteAsync(JobCompletion completion, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(completion);
        try
        {
            await _channel.Writer.WriteAsync(completion, cancellationToken).ConfigureAwait(false);
        }
        catch (ChannelClosedException e)
        {
            throw new InvalidOperationException("The completer was stopped; the job stays running and will be rescued.", e);
        }
    }

    protected override Task DrainAsync()
    {
        _channel.Writer.TryComplete();
        lock (_lock)
        {
            return _loop ?? Task.CompletedTask;
        }
    }

    private async Task RunAsync()
    {
        var reader = _channel.Reader;
        var batch = new List<JobCompletion>(MaxBatchSize);
        while (await WaitToReadAsync(Timeout.InfiniteTimeSpan).ConfigureAwait(false))
        {
            var deadline = Time.GetUtcNow() + Options.FlushInterval;
            while (batch.Count < MaxBatchSize)
            {
                while (batch.Count < MaxBatchSize && reader.TryRead(out var completion))
                {
                    batch.Add(completion);
                }

                var left = deadline - Time.GetUtcNow();
                if (batch.Count >= MaxBatchSize || left <= TimeSpan.Zero || !await WaitToReadAsync(left).ConfigureAwait(false))
                {
                    break;
                }
            }

            if (batch.Count > 0)
            {
                await WriteAsync([.. batch]).ConfigureAwait(false);
                batch.Clear();
            }
        }

        Abandoned(reader.Count);
    }

    /// <summary>Waits for a completion to read; <c>false</c> on timeout, when the channel is closed, or at a hard stop.</summary>
    private async Task<bool> WaitToReadAsync(TimeSpan timeout)
    {
        using var timer = timeout == Timeout.InfiniteTimeSpan ? new CancellationTokenSource() : new CancellationTokenSource(timeout, Time);
        using var linked = CancellationTokenSource.CreateLinkedTokenSource(timer.Token, Abort);
        try
        {
            return await _channel.Reader.WaitToReadAsync(linked.Token).ConfigureAwait(false);
        }
        catch (OperationCanceledException)
        {
            // A hard stop abandons what is still queued; a timeout flushes what was collected.
            return !Abort.IsCancellationRequested && _channel.Reader.Count > 0;
        }
    }
}
