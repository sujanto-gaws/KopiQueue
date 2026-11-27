using KopiQueue.Drivers;
using KopiQueue.Migrations;
using Xunit;

namespace KopiQueue.TestInfrastructure.Conformance;

/// <summary>Scheduler cases (plan task 3.3: FR-SCH-01, FR-MNT-02, FR-MNT-07; design §5.7).</summary>
/// <remarks>
/// The scheduler's statement looks at every job of the schema, so each case runs in a schema of its own, where no other
/// case's jobs can be due.
/// </remarks>
public abstract partial class DriverConformanceTests<TFixture>
{
    /// <summary>Unique states without retryable: scheduled, running, pending and available (FR-UNQ-04's minimum).</summary>
    private const byte StatesWithoutRetryable = 128 | 64 | 16 | 1;

    /// <summary>The default unique states (design §5.6): <c>11110101</c>.</summary>
    private const byte DefaultUniqueStates = 0b1111_0101;

    /// <summary>The discarded bit of a unique-states mask.</summary>
    private const byte DiscardedBit = 8;

    [Fact]
    public async Task Schedule_makes_due_scheduled_and_retryable_jobs_available_and_changes_nothing_else_about_them()
    {
        var executor = (await IsolatedSchemaAsync()).Driver.GetExecutor();
        var kind = UniqueKind();
        var (a, b) = (UniqueQueue(), UniqueQueue());
        var past = Microseconds(DateTimeOffset.UtcNow.AddMinutes(-10));

        // A retryable job that really ran: attempt 1, attempted_at, attempted_by and one error.
        var ran = (await executor.JobInsertManyAsync([Job(kind, b)], Ct)).Single().Job;
        Assert.Single(await executor.JobGetAvailableAsync(new JobGetAvailableParams { Queue = b, Max = 1, ClientId = "worker-1" }, Ct));
        var error = new AttemptError(past, 1, "boom", "at Worker.WorkAsync()");
        await executor.JobSetStateIfRunningManyAsync([new JobSetStateParams { Id = ran.Id, State = JobState.Retryable, Error = error, ScheduledAt = past }], Ct);

        var due = (await executor.JobInsertManyAsync(
            [
                At(kind, a, JobState.Scheduled, past, priority: 3, metadata: """{"tenant": "t1"}"""),
                At(kind, a, JobState.Retryable, past.AddMinutes(1)),
                At(kind, b, JobState.Scheduled, past.AddMinutes(2)),
            ],
            Ct)).Select(r => r.Job).ToList();
        var before = new List<JobRow>();
        foreach (var id in due.Select(j => j.Id).Append(ran.Id))
        {
            before.Add((await executor.JobGetByIdAsync(id, Ct))!);
        }

        var changed = await executor.JobScheduleAsync(new JobScheduleParams { Max = 100 }, Ct);

        Assert.Equal(
            before.OrderBy(j => j.Id).Select(j => new JobScheduleResult(j.Id, j.Queue, JobState.Available)),
            changed);
        foreach (var old in before)
        {
            var now = (await executor.JobGetByIdAsync(old.Id, Ct))!;
            Assert.Equal(JobState.Available, now.State);
            Assert.Equal(
                (old.Queue, old.Priority, old.ScheduledAt, old.Attempt, old.AttemptedAt, old.MetadataJson, old.FinalizedAt),
                (now.Queue, now.Priority, now.ScheduledAt, now.Attempt, now.AttemptedAt, now.MetadataJson, now.FinalizedAt));
            Assert.Equal(old.AttemptedBy, now.AttemptedBy);
            Assert.Equal(old.Errors, now.Errors);
        }

        var promoted = (await executor.JobGetByIdAsync(ran.Id, Ct))!;
        Assert.Equal(1, promoted.Attempt);
        Assert.Equal(["worker-1"], promoted.AttemptedBy);
        Assert.Equal([error], promoted.Errors);
        Assert.Empty(await executor.JobScheduleAsync(new JobScheduleParams { Max = 100 }, Ct));
    }

    [Fact]
    public async Task Schedule_leaves_jobs_not_yet_due_and_jobs_in_other_states_alone()
    {
        var executor = (await IsolatedSchemaAsync()).Driver.GetExecutor();
        var kind = UniqueKind();
        var queue = UniqueQueue();
        var past = Microseconds(DateTimeOffset.UtcNow.AddMinutes(-10));
        var future = Microseconds(DateTimeOffset.UtcNow.AddMinutes(10));

        var jobs = (await executor.JobInsertManyAsync(
            [
                At(kind, queue, JobState.Scheduled, future),
                At(kind, queue, JobState.Retryable, future),
                At(kind, queue, JobState.Available, past), // fetched: running
                At(kind, queue, JobState.Available, past), // fetched, then completed
                At(kind, queue, JobState.Available, past),
            ],
            Ct)).Select(r => r.Job.Id).ToArray();
        var running = await executor.JobGetAvailableAsync(new JobGetAvailableParams { Queue = queue, Max = 2, ClientId = "c" }, Ct);
        Assert.Equal(2, running.Count);
        await executor.JobSetStateIfRunningManyAsync(
            [new JobSetStateParams { Id = running[1].Id, State = JobState.Completed, FinalizedAt = Microseconds(DateTimeOffset.UtcNow) }],
            Ct);
        var before = new Dictionary<long, JobState>();
        foreach (var id in jobs)
        {
            before[id] = (await executor.JobGetByIdAsync(id, Ct))!.State;
        }

        Assert.Equal(
            [JobState.Scheduled, JobState.Retryable, JobState.Running, JobState.Completed, JobState.Available],
            jobs.Select(id => before[id]));
        Assert.Empty(await executor.JobScheduleAsync(new JobScheduleParams { Max = 100 }, Ct));

        foreach (var id in jobs)
        {
            Assert.Equal(before[id], (await executor.JobGetByIdAsync(id, Ct))!.State);
        }
    }

    [Fact]
    public async Task Schedule_changes_at_most_max_jobs_a_call_most_overdue_first()
    {
        var executor = (await IsolatedSchemaAsync()).Driver.GetExecutor();
        var kind = UniqueKind();
        var queue = UniqueQueue();
        var past = Microseconds(DateTimeOffset.UtcNow.AddHours(-1));

        // Inserted newest first, so the order comes from scheduled_at and not from the ids.
        var ids = (await executor.JobInsertManyAsync(
            [.. Enumerable.Range(0, 5).Select(i => At(kind, queue, i % 2 == 0 ? JobState.Scheduled : JobState.Retryable, past.AddMinutes(-i)))],
            Ct)).Select(r => r.Job.Id).ToArray();
        var parameters = new JobScheduleParams { Max = 2 };

        var first = await executor.JobScheduleAsync(parameters, Ct);
        var second = await executor.JobScheduleAsync(parameters, Ct);
        var third = await executor.JobScheduleAsync(parameters, Ct);

        Assert.Equal([ids[3], ids[4]], first.Select(r => r.Id).Order());
        Assert.Equal([ids[1], ids[2]], second.Select(r => r.Id).Order());
        Assert.Equal([ids[0]], third.Select(r => r.Id));
        Assert.Empty(await executor.JobScheduleAsync(parameters, Ct));
    }

    [Fact]
    public async Task Schedule_skips_a_job_another_transaction_holds_without_waiting_and_promotes_it_later()
    {
        var schema = await IsolatedSchemaAsync();
        var executor = schema.Driver.GetExecutor();
        var kind = UniqueKind();
        var queue = UniqueQueue();
        var past = Microseconds(DateTimeOffset.UtcNow.AddMinutes(-10));
        var ids = (await executor.JobInsertManyAsync(
            [At(kind, queue, JobState.Scheduled, past), At(kind, queue, JobState.Retryable, past)],
            Ct)).Select(r => r.Job.Id).ToArray();

        await using (var holder = await schema.BeginTransactionAsync(Ct))
        {
            // Merging metadata updates the row whatever its state, so the holder's transaction now locks it.
            await schema.Driver.GetExecutor(holder.Transaction).JobSetStateIfRunningManyAsync(
                [new JobSetStateParams { Id = ids[0], State = JobState.Completed, MetadataUpdates = "{}" }],
                Ct);

            var whileHeld = await executor.JobScheduleAsync(new JobScheduleParams { Max = 100 }, Ct).WaitAsync(TimeSpan.FromSeconds(10), Ct);

            Assert.Equal([ids[1]], whileHeld.Select(r => r.Id));
            Assert.Equal(JobState.Scheduled, (await executor.JobGetByIdAsync(ids[0], Ct))!.State);
        }

        var afterwards = await executor.JobScheduleAsync(new JobScheduleParams { Max = 100 }, Ct);

        Assert.Equal([new JobScheduleResult(ids[0], queue, JobState.Available)], afterwards);
    }

    [Fact]
    public async Task Schedule_discards_only_the_retryable_job_whose_unique_slot_a_duplicate_took()
    {
        var schema = await IsolatedSchemaAsync();
        Assert.SkipUnless(schema.CanSetUnique, "The driver's fixture cannot set unique keys.");
        var executor = schema.Driver.GetExecutor();
        var kind = UniqueKind();
        var queue = UniqueQueue();
        var past = Microseconds(DateTimeOffset.UtcNow.AddMinutes(-10));
        var jobs = (await executor.JobInsertManyAsync(
            [
                At(kind, queue, JobState.Available, past),                 // 0: holds key 1
                At(kind, queue, JobState.Retryable, past),                 // 1: key 1, its states leave out retryable: conflicts
                At(kind, queue, JobState.Retryable, past.AddMinutes(1)),   // 2: key 2, the older of two due duplicates: promoted
                At(kind, queue, JobState.Retryable, past.AddMinutes(2)),   // 3: key 2, the newer one: conflicts with job 2
                At(kind, queue, JobState.Retryable, past),                 // 4: key 3, its states include retryable: holds its slot
                At(kind, queue, JobState.Scheduled, past),                 // 5: no key
                At(kind, queue, JobState.Available, past),                 // 6: holds key 4
                At(kind, queue, JobState.Retryable, past),                 // 7: key 4, conflicts, but would conflict as discarded too
            ],
            Ct)).Select(r => r.Job).ToArray();
        await schema.SetUniqueAsync(jobs[0].Id, Key(1), DefaultUniqueStates, Ct);
        await schema.SetUniqueAsync(jobs[1].Id, Key(1), StatesWithoutRetryable, Ct);
        await schema.SetUniqueAsync(jobs[2].Id, Key(2), StatesWithoutRetryable, Ct);
        await schema.SetUniqueAsync(jobs[3].Id, Key(2), StatesWithoutRetryable, Ct);
        await schema.SetUniqueAsync(jobs[4].Id, Key(3), DefaultUniqueStates, Ct);
        await schema.SetUniqueAsync(jobs[6].Id, Key(4), DefaultUniqueStates, Ct);
        await schema.SetUniqueAsync(jobs[7].Id, Key(4), StatesWithoutRetryable | DiscardedBit, Ct);

        var changed = await executor.JobScheduleAsync(new JobScheduleParams { Max = 100 }, Ct);

        Assert.Equal(
            [
                new JobScheduleResult(jobs[1].Id, queue, JobState.Discarded),
                new JobScheduleResult(jobs[2].Id, queue, JobState.Available),
                new JobScheduleResult(jobs[3].Id, queue, JobState.Discarded),
                new JobScheduleResult(jobs[4].Id, queue, JobState.Available),
                new JobScheduleResult(jobs[5].Id, queue, JobState.Available),
            ],
            changed);
        foreach (var discarded in new[] { jobs[1], jobs[3] })
        {
            var row = (await executor.JobGetByIdAsync(discarded.Id, Ct))!;
            Assert.Equal(JobState.Discarded, row.State);
            Assert.NotNull(row.FinalizedAt);
            Assert.InRange(row.FinalizedAt.Value, DateTimeOffset.UtcNow.AddMinutes(-5), DateTimeOffset.UtcNow.AddMinutes(5));
            var error = Assert.Single(row.Errors);
            Assert.Equal((discarded.Attempt, JobScheduleParams.DefaultUniqueConflictError, (string?)null), (error.Attempt, error.Error, error.Trace));
            Assert.InRange(error.At, row.FinalizedAt.Value.AddSeconds(-1), row.FinalizedAt.Value.AddSeconds(1));
        }

        Assert.Equal(JobState.Available, (await executor.JobGetByIdAsync(jobs[0].Id, Ct))!.State);
        Assert.Equal(JobState.Available, (await executor.JobGetByIdAsync(jobs[6].Id, Ct))!.State);
        var waiting = (await executor.JobGetByIdAsync(jobs[7].Id, Ct))!;
        Assert.Equal((JobState.Retryable, (DateTimeOffset?)null), (waiting.State, waiting.FinalizedAt));
        Assert.Empty(waiting.Errors);
        Assert.Empty(await executor.JobScheduleAsync(new JobScheduleParams { Max = 100 }, Ct));

        static byte[] Key(byte n) => [.. Enumerable.Repeat(n, 32)];
    }

    [Fact]
    public async Task Schedule_rejects_a_batch_size_below_one()
    {
        var schema = await MigratedAsync();

        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(() => schema.Driver.GetExecutor().JobScheduleAsync(new JobScheduleParams { Max = 0 }, Ct));
    }

    /// <summary>A new schema of this case's own, migrated to the latest version.</summary>
    private async Task<ConformanceSchema> IsolatedSchemaAsync()
    {
        Fixture.SkipIfUnavailable();
        var schema = await Fixture.CreateSchemaAsync(Ct);
        await schema.Driver.CreateMigrator().MigrateAsync(MigrationDirection.Up, cancellationToken: Ct);
        return schema;
    }

    private static JobInsertParams At(string kind, string queue, JobState state, DateTimeOffset scheduledAt, int priority = 1, string metadata = "{}") => new()
    {
        Kind = kind,
        ArgsJson = "{}",
        Queue = queue,
        State = state,
        Priority = priority,
        MaxAttempts = 25,
        ScheduledAt = scheduledAt,
        MetadataJson = metadata,
    };
}
