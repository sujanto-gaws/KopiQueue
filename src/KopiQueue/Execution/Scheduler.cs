using KopiQueue.Drivers;

namespace KopiQueue.Execution;

/// <summary>
/// Makes due <see cref="JobState.Scheduled"/> and <see cref="JobState.Retryable"/> jobs <see cref="JobState.Available"/>
/// (plan task 3.3, FR-SCH-01, FR-SCH-02, FR-MNT-02, design §5.7): every <see cref="KopiClientOptions.SchedulerInterval"/>,
/// in batches of up to 10,000, on the leader only. Due is judged by the database's clock, so client clock skew does not
/// matter. Each queue that got jobs is notified on commit; poll-only clients find them at their next poll.
/// </summary>
/// <remarks>
/// A retryable unique job whose slot a duplicate took while it waited is discarded instead, as River documents, with an
/// error saying why (<see cref="JobScheduleParams.DefaultUniqueConflictError"/>). A batch returns every job it changed,
/// promoted or discarded, so a full batch runs again at once.
/// </remarks>
internal sealed class Scheduler(IKopiDriver driver, TimeSpan interval, int batchSize = Scheduler.DefaultBatchSize) : IMaintenanceService
{
    /// <summary>The most jobs one batch changes (design §5.7).</summary>
    public const int DefaultBatchSize = 10_000;

    public string Name => "scheduler";

    public TimeSpan Interval { get; } = interval;

    public int BatchSize { get; } = batchSize;

    public async Task<int> RunBatchAsync(CancellationToken cancellationToken)
    {
        var changed = await driver.GetExecutor()
            .JobScheduleAsync(new JobScheduleParams { Max = BatchSize }, cancellationToken)
            .ConfigureAwait(false);
        return changed.Count;
    }
}
