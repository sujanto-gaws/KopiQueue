using System.Collections.Concurrent;
using System.Diagnostics;

namespace KopiQueue.Benchmarks;

[JobKind(Kind)]
internal sealed record NoOpArgs(int N) : IJobArgs
{
    public const string Kind = "bench_noop";
}

/// <summary>Does nothing: what's measured is KopiQueue and the database.</summary>
internal sealed class NoOpWorker : Worker<NoOpArgs>
{
    private int _worked;

    /// <summary>Record when each job started (pickup latency), keyed by job ID.</summary>
    public bool RecordStarts { get; init; }

    public ConcurrentDictionary<long, long> Started { get; } = new();

    public int Worked => Volatile.Read(ref _worked);

    public override Task WorkAsync(Job<NoOpArgs> job, CancellationToken cancellationToken)
    {
        if (RecordStarts)
        {
            Started.TryAdd(job.Id, Stopwatch.GetTimestamp());
        }

        Interlocked.Increment(ref _worked);
        return Task.CompletedTask;
    }
}
