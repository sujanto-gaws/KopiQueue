using KopiQueue.Drivers;

namespace KopiQueue.Execution;

/// <summary>
/// Deletes finalised jobs past their retention (plan task 3.5, FR-MNT-04, design §5.7): completed, cancelled and
/// discarded jobs whose <see cref="JobRow.FinalizedAt"/> is older, by the database's clock, than their state's retention.
/// Every 30 s, in batches of up to 1,000, oldest first, on the leader only. A state without a retention is kept; jobs in
/// any other state are never deleted. Deleting a job frees any unique slot it held (design §5.6).
/// </summary>
internal sealed class JobCleaner(IKopiDriver driver, JobDeleteFinalizedParams retentions, TimeSpan? interval = null) : IMaintenanceService
{
    /// <summary>The most jobs one batch deletes (design §5.7).</summary>
    public const int DefaultBatchSize = 1_000;

    public string Name => "job_cleaner";

    public TimeSpan Interval { get; } = interval ?? TimeSpan.FromSeconds(30);

    public int BatchSize => retentions.Max;

    /// <summary>Whether any state has a retention; without one the service is not registered.</summary>
    public static bool AnyRetention(KopiClientOptions options) =>
        options.CompletedJobRetention is not null || options.CancelledJobRetention is not null || options.DiscardedJobRetention is not null;

    public async Task<int> RunBatchAsync(CancellationToken cancellationToken)
    {
        var deleted = await driver.GetExecutor().JobDeleteFinalizedAsync(retentions, cancellationToken).ConfigureAwait(false);
        return deleted.Count;
    }
}
