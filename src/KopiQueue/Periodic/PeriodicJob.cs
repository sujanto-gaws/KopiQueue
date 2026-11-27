using KopiQueue.Execution;

namespace KopiQueue;

/// <summary>
/// A job the leader inserts on a schedule (FR-PER-01, FR-PER-02): at each tick it calls the constructor and inserts the
/// job it returns. Register it on every client that can lead, with <see cref="KopiClientOptions.PeriodicJobs"/> or
/// <see cref="KopiClient.PeriodicJobs"/>, so the schedule survives a change of leader.
/// </summary>
/// <remarks>
/// <para>
/// Each tick is inserted at most once however leadership changes (FR-PER-04): unless the constructor's options set
/// <see cref="InsertOptions.Unique"/>, the job is unique for its tick, in every state. A new leader inserts the latest tick
/// if it is recent enough that the previous leader may have missed it; if the previous leader did insert it, that insert
/// is skipped as a duplicate. Inserted jobs carry <c>"periodic": true</c> in their metadata, as River's do.
/// </para>
/// <para>
/// The constructor runs on the leader at each tick. It can return <c>null</c> to skip the tick. If it throws, or returns
/// options that are not valid, the tick is skipped and the error is logged.
/// </para>
/// </remarks>
/// <example>
/// <code>
/// // UC-3: once per cluster, every night at 02:00 UTC.
/// var rebuild = PeriodicJob.Create(PeriodicSchedule.Cron("0 2 * * *"), () => new RebuildSearchIndexArgs());
/// </code>
/// </example>
public sealed class PeriodicJob
{
    /// <summary>Creates a periodic job whose constructor returns the job to insert at each tick.</summary>
    /// <param name="schedule">When it runs.</param>
    /// <param name="constructor">The args and options to insert at a tick, or <c>null</c> to skip it.</param>
    public PeriodicJob(PeriodicSchedule schedule, Func<InsertRequest?> constructor)
    {
        ArgumentNullException.ThrowIfNull(schedule);
        ArgumentNullException.ThrowIfNull(constructor);
        Schedule = schedule;
        Constructor = constructor;
    }

    /// <summary>When the job runs.</summary>
    public PeriodicSchedule Schedule { get; }

    /// <summary>Returns the job to insert at a tick, or <c>null</c> to skip it.</summary>
    public Func<InsertRequest?> Constructor { get; }

    /// <summary>
    /// Also inserts the job as soon as a client becomes leader, and when the job is added to the leader at runtime. That
    /// insert is not a tick, so it is not unique unless the constructor's options say so. Default: <c>false</c>.
    /// </summary>
    public bool RunOnStart { get; init; }

    /// <summary>
    /// A name for <see cref="PeriodicJobBundle.RemoveById"/>, unique among one client's periodic jobs. Default: none.
    /// </summary>
    public string? Id { get; init; }

    /// <summary>Creates a periodic job that inserts the args <paramref name="args"/> returns, with <paramref name="options"/>.</summary>
    /// <typeparam name="TArgs">The args type.</typeparam>
    /// <param name="schedule">When it runs.</param>
    /// <param name="args">The args to insert at a tick.</param>
    /// <param name="options">The insert options. Default: none.</param>
    public static PeriodicJob Create<TArgs>(PeriodicSchedule schedule, Func<TArgs> args, InsertOptions? options = null)
        where TArgs : IJobArgs
    {
        ArgumentNullException.ThrowIfNull(args);
        return new PeriodicJob(schedule, () => new InsertRequest(args(), options));
    }
}

/// <summary>A periodic job as added to a <see cref="PeriodicJobBundle"/>, for <see cref="PeriodicJobBundle.Remove"/>.</summary>
public sealed class PeriodicJobHandle
{
    internal PeriodicJobHandle(PeriodicJob job) => Job = job;

    /// <summary>The periodic job.</summary>
    public PeriodicJob Job { get; }
}

/// <summary>
/// A client's periodic jobs (FR-PER-03), which can change while it runs. Changes take effect at once on the leader;
/// apply them on every client that can lead. Removing a job leaves the jobs it inserted alone. Thread-safe.
/// </summary>
public sealed class PeriodicJobBundle
{
    private readonly Lock _lock = new();
    private List<PeriodicJobHandle> _handles = [];

    internal PeriodicJobBundle()
    {
    }

    /// <summary>How many periodic jobs there are.</summary>
    public int Count
    {
        get
        {
            lock (_lock)
            {
                return _handles.Count;
            }
        }
    }

    /// <summary>Wakes the periodic enqueuer when the jobs change.</summary>
    internal WakeSignal Changed { get; } = new();

    /// <summary>Adds <paramref name="job"/>.</summary>
    /// <returns>The handle for <see cref="Remove"/>.</returns>
    /// <exception cref="ArgumentException">Another job has the same <see cref="PeriodicJob.Id"/>.</exception>
    public PeriodicJobHandle Add(PeriodicJob job) => AddMany([job])[0];

    /// <summary>Adds <paramref name="jobs"/>, all or none.</summary>
    /// <returns>The handles, in order.</returns>
    /// <exception cref="ArgumentException">Two jobs, or a job and one already added, have the same <see cref="PeriodicJob.Id"/>.</exception>
    public IReadOnlyList<PeriodicJobHandle> AddMany(IEnumerable<PeriodicJob> jobs)
    {
        ArgumentNullException.ThrowIfNull(jobs);
        var added = jobs.Select(j => new PeriodicJobHandle(j ?? throw new ArgumentException("A periodic job is null.", nameof(jobs)))).ToList();
        lock (_lock)
        {
            var ids = new HashSet<string>(_handles.Where(h => h.Job.Id is not null).Select(h => h.Job.Id!), StringComparer.Ordinal);
            foreach (var handle in added)
            {
                if (handle.Job.Id is { } id && !ids.Add(id))
                {
                    throw new ArgumentException($"A periodic job with ID '{id}' is already added.", nameof(jobs));
                }
            }

            _handles = [.. _handles, .. added];
        }

        Changed.Set();
        return added;
    }

    /// <summary>Removes the job <paramref name="handle"/> was returned for.</summary>
    /// <returns>Whether it was there.</returns>
    public bool Remove(PeriodicJobHandle handle)
    {
        ArgumentNullException.ThrowIfNull(handle);
        return RemoveWhere(h => ReferenceEquals(h, handle));
    }

    /// <summary>Removes the job with <see cref="PeriodicJob.Id"/> <paramref name="id"/>.</summary>
    /// <returns>Whether it was there.</returns>
    public bool RemoveById(string id)
    {
        ArgumentNullException.ThrowIfNull(id);
        return RemoveWhere(h => string.Equals(h.Job.Id, id, StringComparison.Ordinal));
    }

    /// <summary>Removes every job.</summary>
    public void Clear() => RemoveWhere(_ => true);

    /// <summary>The jobs now, for the enqueuer.</summary>
    internal IReadOnlyList<PeriodicJobHandle> Snapshot()
    {
        lock (_lock)
        {
            return _handles;
        }
    }

    private bool RemoveWhere(Func<PeriodicJobHandle, bool> match)
    {
        bool removed;
        lock (_lock)
        {
            var kept = _handles.Where(h => !match(h)).ToList();
            removed = kept.Count != _handles.Count;
            _handles = kept;
        }

        if (removed)
        {
            Changed.Set();
        }

        return removed;
    }
}
