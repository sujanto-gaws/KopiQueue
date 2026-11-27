using System.Globalization;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json.Nodes;
using KopiQueue.Drivers;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace KopiQueue.Execution;

/// <summary>Settings of a <see cref="PeriodicEnqueuer"/>.</summary>
internal sealed class PeriodicEnqueuerOptions
{
    /// <summary>
    /// How recent the latest tick must be for a new leader to insert it (FR-PER-04): about as long as leadership can be
    /// vacant, the lease plus an election interval. Older ticks are not caught up.
    /// </summary>
    public required TimeSpan CatchUpWindow { get; init; }

    /// <summary>How long to wait before trying a failed insert again. Default: 5 s.</summary>
    public TimeSpan RetryInterval { get; init; } = TimeSpan.FromSeconds(5);

    /// <summary>The longest wait between checks when no tick is near. Default: 1 hour.</summary>
    public TimeSpan MaxWait { get; init; } = TimeSpan.FromHours(1);
}

/// <summary>
/// Inserts periodic jobs on the leader (plan task 4.3, FR-PER-01..04, design §5.7). Unlike the other maintenance
/// services it has no fixed interval: it waits until the next tick, or until the periodic jobs change.
/// </summary>
/// <remarks>
/// <para>
/// The maintenance host runs it with a new cancellation token each time this client becomes leader, which is how it
/// tells a new leadership: it then forgets its ticks, schedules every job from now, inserts the jobs with
/// <see cref="PeriodicJob.RunOnStart"/>, and inserts the latest tick of each job if it falls within
/// <see cref="PeriodicEnqueuerOptions.CatchUpWindow"/>.
/// </para>
/// <para>
/// A tick's job is unique for that tick in every state, unless its options set their own uniqueness, so a tick that a
/// previous leader inserted, or that a failed batch already inserted, is skipped as a duplicate. Ticks are only
/// advanced after a batch is inserted; a batch that fails is tried again after the retry interval. When the enqueuer
/// falls behind by more than one tick, it inserts the latest one and continues from now.
/// </para>
/// </remarks>
internal sealed partial class PeriodicEnqueuer : IMaintenanceService
{
    /// <summary>Unique states with every state: a tick's job holds its tick even once finalised.</summary>
    private const byte EveryState = 0b1111_1111;

    private readonly IKopiDriver _driver;
    private readonly PeriodicJobBundle _jobs;
    private readonly Func<InsertRequest, DateTimeOffset, JobInsertParams> _prepare;
    private readonly Action<IReadOnlyList<JobInsertParams>>? _inserted;
    private readonly PeriodicEnqueuerOptions _options;
    private readonly TimeProvider _time;
    private readonly ILogger _logger;
    private readonly Dictionary<PeriodicJobHandle, State> _states = new(ReferenceEqualityComparer.Instance);
    private CancellationToken _leadership;
    private bool _leading;

    /// <param name="driver">Inserts the jobs.</param>
    /// <param name="jobs">The client's periodic jobs.</param>
    /// <param name="prepare">The client's insert validation: a request and the current time to the driver's parameters.</param>
    /// <param name="options">The settings.</param>
    /// <param name="inserted">Called after each insert, to wake this client's producers.</param>
    /// <param name="timeProvider">The clock.</param>
    /// <param name="logger">Logs skipped ticks.</param>
    public PeriodicEnqueuer(
        IKopiDriver driver,
        PeriodicJobBundle jobs,
        Func<InsertRequest, DateTimeOffset, JobInsertParams> prepare,
        PeriodicEnqueuerOptions options,
        Action<IReadOnlyList<JobInsertParams>>? inserted = null,
        TimeProvider? timeProvider = null,
        ILogger<PeriodicEnqueuer>? logger = null)
    {
        ArgumentNullException.ThrowIfNull(driver);
        ArgumentNullException.ThrowIfNull(jobs);
        ArgumentNullException.ThrowIfNull(prepare);
        ArgumentNullException.ThrowIfNull(options);
        _driver = driver;
        _jobs = jobs;
        _prepare = prepare;
        _options = options;
        _inserted = inserted;
        _time = timeProvider ?? TimeProvider.System;
        _logger = logger ?? NullLogger<PeriodicEnqueuer>.Instance;
    }

    public string Name => "periodic_enqueuer";

    /// <summary>After a failed batch, how long until it is tried again.</summary>
    public TimeSpan Interval => _options.RetryInterval;

    /// <summary>Never full: one batch inserts everything that is due.</summary>
    public int BatchSize => int.MaxValue;

    public async Task<int> RunBatchAsync(CancellationToken cancellationToken)
    {
        var now = _time.GetUtcNow();
        var newLeadership = !_leading || !cancellationToken.Equals(_leadership);
        if (newLeadership)
        {
            _leadership = cancellationToken;
            _leading = true;
            _states.Clear();
        }

        _jobs.Changed.Clear();
        Sync(_jobs.Snapshot(), now, newLeadership);

        var inserts = new List<JobInsertParams>();
        foreach (var (handle, state) in _states)
        {
            if (state.StartPending)
            {
                Build(handle, tick: null, now, inserts);
            }

            if (state.CatchUp is { } missed)
            {
                Build(handle, missed, now, inserts);
            }

            if (state.Next is { } next && next <= now)
            {
                Build(handle, next, now, inserts);
            }
        }

        if (inserts.Count > 0)
        {
            await _driver.GetExecutor().JobInsertManyAsync(inserts, cancellationToken).ConfigureAwait(false);
            _inserted?.Invoke(inserts);
        }

        foreach (var (handle, state) in _states)
        {
            state.StartPending = false;
            state.CatchUp = null;
            if (state.Next is { } next && next <= now)
            {
                var following = handle.Job.Schedule.NextAfter(next);
                state.Next = following is { } f && f <= now ? handle.Job.Schedule.NextAfter(now) : following;
            }
        }

        return inserts.Count;
    }

    /// <summary>Waits until the next tick, the periodic jobs change, or, with work left from a failed batch, the retry interval.</summary>
    public async Task WaitAsync(TimeSpan wait, TimeProvider time, CancellationToken cancellationToken)
    {
        var now = time.GetUtcNow();
        var pending = _states.Values.Any(s => s.StartPending || s.CatchUp is not null || s.Next <= now);
        if (!pending)
        {
            var next = _states.Values.Select(s => s.Next).Where(n => n is not null).Min();
            wait = next is { } n ? n - now : _options.MaxWait;
            if (wait > _options.MaxWait)
            {
                wait = _options.MaxWait;
            }
        }

        if (wait > TimeSpan.Zero)
        {
            await _jobs.Changed.WaitAsync(wait, time, cancellationToken).ConfigureAwait(false);
        }
    }

    /// <summary>
    /// The unique key of a tick's job: the kind, queue, args and tick. River's keys never contain <c>&amp;periodic_tick=</c>,
    /// so it cannot collide with a key of River's.
    /// </summary>
    internal static byte[] TickKey(JobInsertParams job, DateTimeOffset tick) => SHA256.HashData(Encoding.UTF8.GetBytes(
        "&kind=" + job.Kind + "&queue=" + job.Queue + "&args=" + job.ArgsJson
        + "&periodic_tick=" + tick.UtcDateTime.ToString("yyyy-MM-dd'T'HH:mm:ss.fffffff'Z'", CultureInfo.InvariantCulture)));

    private void Sync(IReadOnlyList<PeriodicJobHandle> handles, DateTimeOffset now, bool newLeadership)
    {
        var current = new HashSet<PeriodicJobHandle>(handles, ReferenceEqualityComparer.Instance);
        foreach (var removed in _states.Keys.Where(h => !current.Contains(h)).ToList())
        {
            _states.Remove(removed);
        }

        foreach (var handle in handles)
        {
            if (_states.ContainsKey(handle))
            {
                continue;
            }

            var schedule = handle.Job.Schedule;
            var state = new State { Next = schedule.NextAfter(now), StartPending = handle.Job.RunOnStart };
            if (newLeadership && schedule.LatestAtOrBefore(now) is { } latest && now - latest <= _options.CatchUpWindow)
            {
                state.CatchUp = latest;
            }

            _states[handle] = state;
        }
    }

    /// <summary>Builds the job for one tick, or for a start insert without a tick; a failure skips it and is logged.</summary>
    private void Build(PeriodicJobHandle handle, DateTimeOffset? tick, DateTimeOffset now, List<JobInsertParams> inserts)
    {
        try
        {
            if (handle.Job.Constructor() is not { } request)
            {
                return;
            }

            var job = _prepare(request, now);
            var metadata = JsonNode.Parse(job.MetadataJson) as JsonObject ?? [];
            metadata["periodic"] = true;
            var tickUnique = tick is not null && job.UniqueKey is null;
            inserts.Add(new JobInsertParams
            {
                Kind = job.Kind,
                ArgsJson = job.ArgsJson,
                Queue = job.Queue,
                State = job.State,
                Priority = job.Priority,
                MaxAttempts = job.MaxAttempts,
                ScheduledAt = job.ScheduledAt,
                MetadataJson = metadata.ToJsonString(),
                Tags = job.Tags,
                UniqueKey = tickUnique ? TickKey(job, tick!.Value) : job.UniqueKey,
                UniqueStates = tickUnique ? EveryState : job.UniqueStates,
            });
        }
        catch (Exception e) when (e is not OperationCanceledException)
        {
            LogSkipped(e, handle.Job.Id ?? handle.Job.Schedule.ToString() ?? "periodic job", tick);
        }
    }

    [LoggerMessage(EventId = 2700, Level = LogLevel.Error, Message = "Periodic job {Job} skipped its tick {Tick}: its constructor or options failed")]
    private partial void LogSkipped(Exception exception, string job, DateTimeOffset? tick);

    private sealed class State
    {
        public DateTimeOffset? Next { get; set; }

        public bool StartPending { get; set; }

        public DateTimeOffset? CatchUp { get; set; }
    }
}
