using KopiQueue.Drivers;

namespace KopiQueue.Execution;

/// <summary>
/// Deletes queues nobody has worked within the retention period (plan task 3.5, FR-MNT-05, design §5.7): every minute,
/// in batches of up to 1,000. Runs only on the leader. Clients keep the rows of the queues they work fresh (task 3.6), so
/// a queue goes only after no client has worked it for the whole period; its jobs are not touched.
/// </summary>
internal sealed class QueueCleaner(IKopiDriver driver, TimeSpan retention, TimeSpan? interval = null, int batchSize = 1_000) : IMaintenanceService
{
    public string Name => "queue_cleaner";

    public TimeSpan Interval { get; } = interval ?? TimeSpan.FromMinutes(1);

    public int BatchSize { get; } = batchSize;

    public async Task<int> RunBatchAsync(CancellationToken cancellationToken)
    {
        var deleted = await driver.GetExecutor()
            .QueueDeleteExpiredAsync(new QueueDeleteExpiredParams { Retention = retention, Max = BatchSize }, cancellationToken)
            .ConfigureAwait(false);
        return deleted.Count;
    }
}
