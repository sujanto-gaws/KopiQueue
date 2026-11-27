using KopiQueue.Drivers;
using Xunit;

namespace KopiQueue.TestInfrastructure.Conformance;

/// <summary>Job cleaner cases (plan task 3.5: FR-MNT-04, FR-MNT-07; design §5.7).</summary>
/// <remarks>
/// The cleaner's statement looks at every job of the schema, so each case runs in a schema of its own. Jobs are
/// finalised through <see cref="IKopiExecutor.JobSetStateIfRunningManyAsync"/> with a <c>finalized_at</c> hours in the
/// past, far enough from the cutoffs that the database's and the test's clocks need not agree.
/// </remarks>
public abstract partial class DriverConformanceTests<TFixture>
{
    private static readonly JobDeleteFinalizedParams DefaultRetentions = Retentions();

    /// <summary>FR-MNT-04's defaults (24 h, 24 h, 7 d), with the given batch size and discarded retention.</summary>
    private static JobDeleteFinalizedParams Retentions(int max = 100, TimeSpan? discarded = null) => new()
    {
        CompletedRetention = TimeSpan.FromHours(24),
        CancelledRetention = TimeSpan.FromHours(24),
        DiscardedRetention = discarded ?? TimeSpan.FromDays(7),
        Max = max,
    };

    [Fact]
    public async Task Each_finalised_state_is_deleted_only_past_its_own_retention()
    {
        var executor = (await IsolatedSchemaAsync()).Driver.GetExecutor();
        var queue = UniqueQueue();
        var completedOld = await FinalizedAsync(executor, queue, JobState.Completed, TimeSpan.FromHours(25));
        var completedNew = await FinalizedAsync(executor, queue, JobState.Completed, TimeSpan.FromHours(23));
        var cancelledOld = await FinalizedAsync(executor, queue, JobState.Cancelled, TimeSpan.FromHours(25));
        var cancelledNew = await FinalizedAsync(executor, queue, JobState.Cancelled, TimeSpan.FromHours(23));
        var discardedOld = await FinalizedAsync(executor, queue, JobState.Discarded, TimeSpan.FromDays(8));
        var discardedNew = await FinalizedAsync(executor, queue, JobState.Discarded, TimeSpan.FromDays(6)); // older than a day

        var deleted = await executor.JobDeleteFinalizedAsync(DefaultRetentions, Ct);

        Assert.Equal(new[] { completedOld, cancelledOld, discardedOld }.Order(), deleted);
        foreach (var id in new[] { completedOld, cancelledOld, discardedOld })
        {
            Assert.Null(await executor.JobGetByIdAsync(id, Ct));
        }

        foreach (var id in new[] { completedNew, cancelledNew, discardedNew })
        {
            Assert.NotNull(await executor.JobGetByIdAsync(id, Ct));
        }

        Assert.Empty(await executor.JobDeleteFinalizedAsync(DefaultRetentions, Ct));
    }

    [Fact]
    public async Task A_state_without_a_retention_is_kept()
    {
        var executor = (await IsolatedSchemaAsync()).Driver.GetExecutor();
        var queue = UniqueQueue();
        var completed = await FinalizedAsync(executor, queue, JobState.Completed, TimeSpan.FromDays(400));
        var cancelled = await FinalizedAsync(executor, queue, JobState.Cancelled, TimeSpan.FromDays(400));
        var discarded = await FinalizedAsync(executor, queue, JobState.Discarded, TimeSpan.FromDays(400));

        var onlyCancelled = await executor.JobDeleteFinalizedAsync(
            new JobDeleteFinalizedParams { CancelledRetention = TimeSpan.FromHours(1), Max = 100 },
            Ct);
        var none = await executor.JobDeleteFinalizedAsync(new JobDeleteFinalizedParams { Max = 100 }, Ct);

        Assert.Equal([cancelled], onlyCancelled);
        Assert.Empty(none);
        Assert.NotNull(await executor.JobGetByIdAsync(completed, Ct));
        Assert.NotNull(await executor.JobGetByIdAsync(discarded, Ct));
    }

    [Fact]
    public async Task Jobs_that_are_not_finalised_are_never_deleted_however_old()
    {
        var executor = (await IsolatedSchemaAsync()).Driver.GetExecutor();
        var kind = UniqueKind();
        var queue = UniqueQueue();
        var ancient = Microseconds(DateTimeOffset.UtcNow.AddYears(-2));
        var waiting = (await executor.JobInsertManyAsync(
            [
                At(kind, queue, JobState.Available, ancient),
                At(kind, queue, JobState.Scheduled, ancient),
                At(kind, queue, JobState.Retryable, ancient),
            ],
            Ct)).Select(r => r.Job.Id).ToList();
        var running = await StartOneAsync(executor, kind, UniqueQueue());
        waiting.Add(running.Id);

        // The shortest retentions: only a finalised job could ever be old enough.
        var tiny = TimeSpan.FromTicks(10);
        Assert.Empty(await executor.JobDeleteFinalizedAsync(
            new JobDeleteFinalizedParams { CompletedRetention = tiny, CancelledRetention = tiny, DiscardedRetention = tiny, Max = 100 },
            Ct));

        foreach (var id in waiting)
        {
            Assert.NotNull(await executor.JobGetByIdAsync(id, Ct));
        }
    }

    [Fact]
    public async Task The_cleaner_deletes_at_most_max_jobs_a_call_oldest_first()
    {
        var executor = (await IsolatedSchemaAsync()).Driver.GetExecutor();
        var queue = UniqueQueue();

        // Created newest first, so the order comes from finalized_at and not from the ids.
        var ids = new List<long>();
        foreach (var (state, days) in new[] { (JobState.Completed, 2), (JobState.Discarded, 10), (JobState.Cancelled, 3), (JobState.Completed, 5), (JobState.Discarded, 9) })
        {
            ids.Add(await FinalizedAsync(executor, queue, state, TimeSpan.FromDays(days)));
        }

        var batch = Retentions(max: 2);

        Assert.Equal([ids[1], ids[4]], await executor.JobDeleteFinalizedAsync(batch, Ct));   // 10 and 9 days
        Assert.Equal([ids[2], ids[3]], await executor.JobDeleteFinalizedAsync(batch, Ct));   // 5 and 3 days
        Assert.Equal([ids[0]], await executor.JobDeleteFinalizedAsync(batch, Ct));
        Assert.Empty(await executor.JobDeleteFinalizedAsync(batch, Ct));
    }

    [Fact]
    public async Task The_cleaner_skips_a_job_another_transaction_holds_without_waiting_and_deletes_it_later()
    {
        var schema = await IsolatedSchemaAsync();
        var executor = schema.Driver.GetExecutor();
        var queue = UniqueQueue();
        var held = await FinalizedAsync(executor, queue, JobState.Completed, TimeSpan.FromDays(2));
        var free = await FinalizedAsync(executor, queue, JobState.Completed, TimeSpan.FromDays(2));

        await using (var holder = await schema.BeginTransactionAsync(Ct))
        {
            // Merging metadata updates the row whatever its state, so the holder's transaction now locks it.
            await schema.Driver.GetExecutor(holder.Transaction).JobSetStateIfRunningManyAsync(
                [new JobSetStateParams { Id = held, State = JobState.Completed, MetadataUpdates = "{}" }],
                Ct);

            var whileHeld = await executor.JobDeleteFinalizedAsync(DefaultRetentions, Ct).WaitAsync(TimeSpan.FromSeconds(10), Ct);

            Assert.Equal([free], whileHeld);
        }

        Assert.Equal([held], await executor.JobDeleteFinalizedAsync(DefaultRetentions, Ct));
    }

    [Fact]
    public async Task The_cleaner_rejects_a_batch_below_one_and_a_retention_that_is_not_positive()
    {
        var executor = (await MigratedAsync()).Driver.GetExecutor();

        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(() => executor.JobDeleteFinalizedAsync(Retentions(max: 0), Ct));
        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(() =>
            executor.JobDeleteFinalizedAsync(Retentions(discarded: TimeSpan.Zero), Ct));
    }

    /// <summary>Runs a job of its own and finalises it as <paramref name="state"/>, <paramref name="age"/> ago.</summary>
    private static async Task<long> FinalizedAsync(IKopiExecutor executor, string queue, JobState state, TimeSpan age)
    {
        var job = await StartOneAsync(executor, UniqueKind(), queue);
        var finalized = Microseconds(DateTimeOffset.UtcNow - age);
        var row = Assert.Single(await executor.JobSetStateIfRunningManyAsync(
            [new JobSetStateParams { Id = job.Id, State = state, FinalizedAt = finalized }],
            Ct));
        Assert.Equal((state, finalized), (row.State, row.FinalizedAt));
        return job.Id;
    }
}
