using System.Security.Cryptography;
using System.Text.Json;
using KopiQueue.Drivers;
using Xunit;

namespace KopiQueue.TestInfrastructure.Conformance;

/// <summary>
/// Unique inserts (plan task 4.2: FR-UNQ-02, FR-UNQ-03, FR-UNQ-05; design §5.6): the database enforces the key while a
/// job is in one of its unique states, a duplicate returns the job that holds the key, and batches dedupe too.
/// </summary>
public abstract partial class DriverConformanceTests<TFixture>
{
    /// <summary>Unique states with every state, cancelled and discarded included.</summary>
    private const byte AllUniqueStates = 0b1111_1111;

    [Fact]
    public async Task A_duplicate_returns_the_job_that_holds_the_key_unchanged()
    {
        var schema = await MigratedAsync();
        await using var tx = await schema.BeginTransactionAsync(Ct);
        var executor = schema.Driver.GetExecutor(tx.Transaction);
        var key = NewUniqueKey();

        var first = Assert.Single(await executor.JobInsertManyAsync([Unique(Job("first"), key)], Ct));
        var duplicate = new JobInsertParams
        {
            Kind = "second", ArgsJson = "{\"n\":2}", Queue = "other", State = JobState.Available, Priority = 3, MaxAttempts = 2,
            Tags = ["dup"], MetadataJson = "{\"m\":1}", UniqueKey = key, UniqueStates = DefaultUniqueStates,
        };
        var second = Assert.Single(await executor.JobInsertManyAsync([duplicate], Ct));

        Assert.False(first.UniqueSkippedAsDuplicate);
        Assert.True(second.UniqueSkippedAsDuplicate);
        Assert.Equal(first.Job.Id, second.Job.Id);

        // The existing job is returned as it is stored: nothing of the duplicate is written, not even its kind.
        var stored = second.Job;
        Assert.Equal(("first", "default", 1, 25, JobState.Available), (stored.Kind, stored.Queue, stored.Priority, stored.MaxAttempts, stored.State));
        Assert.Equal(1, JsonElement.Parse(stored.ArgsJson).GetProperty("n").GetInt32());
        Assert.Empty(stored.Tags);
        Assert.Equal(first.Job.CreatedAt, stored.CreatedAt);
        Assert.Equal(first.Job.ScheduledAt, stored.ScheduledAt);
        Assert.Equal(1, await CountAsync(executor, first.Job.Id, second.Job.Id));
    }

    [Fact]
    public async Task Scheduled_and_running_jobs_hold_their_keys()
    {
        var schema = await MigratedAsync();
        await using var tx = await schema.BeginTransactionAsync(Ct);
        var executor = schema.Driver.GetExecutor(tx.Transaction);
        var (scheduledKey, runningKey) = (NewUniqueKey(), NewUniqueKey());
        var queue = UniqueQueue();

        var scheduled = Assert.Single(await executor.JobInsertManyAsync(
            [Unique(new JobInsertParams
            {
                Kind = "k", ArgsJson = "{}", Queue = queue, State = JobState.Scheduled, Priority = 1, MaxAttempts = 1,
                ScheduledAt = DateTimeOffset.UtcNow.AddDays(1),
            }, scheduledKey)],
            Ct)).Job;
        var inserted = Assert.Single(await executor.JobInsertManyAsync([Unique(Job("k", queue), runningKey)], Ct)).Job;
        var running = Assert.Single(await executor.JobGetAvailableAsync(new JobGetAvailableParams { Queue = queue, Max = 10, ClientId = "c1" }, Ct));
        Assert.Equal(inserted.Id, running.Id);

        var results = await executor.JobInsertManyAsync([Unique(Job("k", queue), scheduledKey), Unique(Job("k", queue), runningKey)], Ct);

        Assert.All(results, r => Assert.True(r.UniqueSkippedAsDuplicate));
        Assert.Equal((scheduled.Id, JobState.Scheduled), (results[0].Job.Id, results[0].Job.State));
        Assert.Equal((running.Id, JobState.Running, 1), (results[1].Job.Id, results[1].Job.State, results[1].Job.Attempt));
    }

    [Fact]
    public async Task A_job_outside_its_unique_states_frees_its_key_and_one_inside_keeps_it()
    {
        var schema = await MigratedAsync();
        await using var tx = await schema.BeginTransactionAsync(Ct);
        var executor = schema.Driver.GetExecutor(tx.Transaction);
        var (freed, kept) = (NewUniqueKey(), NewUniqueKey());

        var defaultStates = Assert.Single(await executor.JobInsertManyAsync([Unique(Job("k"), freed)], Ct)).Job;
        var allStates = Assert.Single(await executor.JobInsertManyAsync([Unique(Job("k"), kept, AllUniqueStates)], Ct)).Job;
        Assert.Equal(JobState.Cancelled, (await executor.JobCancelAsync(defaultStates.Id, Ct))!.State);
        Assert.Equal(JobState.Cancelled, (await executor.JobCancelAsync(allStates.Id, Ct))!.State);

        var results = await executor.JobInsertManyAsync([Unique(Job("k"), freed), Unique(Job("k"), kept)], Ct);

        // The default states leave out cancelled, so the cancelled job no longer holds its key: a new job takes it.
        Assert.False(results[0].UniqueSkippedAsDuplicate);
        Assert.NotEqual(defaultStates.Id, results[0].Job.Id);
        Assert.Equal(JobState.Available, results[0].Job.State);

        // States that include cancelled keep the key: the cancelled job is the existing one.
        Assert.True(results[1].UniqueSkippedAsDuplicate);
        Assert.Equal((allStates.Id, JobState.Cancelled), (results[1].Job.Id, results[1].Job.State));
    }

    [Fact]
    public async Task A_batch_inserts_each_key_once_and_returns_results_in_input_order()
    {
        var schema = await MigratedAsync();
        await using var tx = await schema.BeginTransactionAsync(Ct);
        var executor = schema.Driver.GetExecutor(tx.Transaction);
        var (existingKey, newKey, otherKey) = (NewUniqueKey(), NewUniqueKey(), NewUniqueKey());
        var existing = Assert.Single(await executor.JobInsertManyAsync([Unique(Numbered(0), existingKey)], Ct)).Job;

        var results = await executor.JobInsertManyAsync(
            [
                Numbered(1),
                Unique(Numbered(2), existingKey),
                Unique(Numbered(3), newKey),
                Numbered(4),
                Unique(Numbered(5), newKey),
                Unique(Numbered(6), otherKey),
                Unique(Numbered(7), existingKey),
                Numbered(8),
            ],
            Ct);

        Assert.Equal([1, 0, 3, 4, 3, 6, 0, 8], results.Select(r => JsonElement.Parse(r.Job.ArgsJson).GetProperty("n").GetInt32()));
        Assert.Equal([false, true, false, false, true, false, true, false], results.Select(r => r.UniqueSkippedAsDuplicate));
        Assert.Equal(existing.Id, results[1].Job.Id);
        Assert.Equal(existing.Id, results[6].Job.Id);
        Assert.Equal(results[2].Job.Id, results[4].Job.Id);
        long[] inserted = [results[0].Job.Id, results[2].Job.Id, results[3].Job.Id, results[5].Job.Id, results[7].Job.Id];
        Assert.Equal(inserted.Order(), inserted); // inserted jobs keep their input order
        Assert.Equal(6, await CountAsync(executor, [existing.Id, .. inserted]));
    }

    [Fact]
    public async Task A_second_transaction_inserting_the_same_key_waits_and_gets_the_committed_job()
    {
        var schema = await MigratedAsync();
        var kind = UniqueKind();
        var key = NewUniqueKey();
        await using var first = await schema.BeginTransactionAsync(Ct);
        await using var second = await schema.BeginTransactionAsync(Ct);

        var held = Assert.Single(await schema.Driver.GetExecutor(first.Transaction).JobInsertManyAsync([Unique(Job(kind), key)], Ct));
        var waiting = schema.Driver.GetExecutor(second.Transaction).JobInsertManyAsync([Unique(Job(kind), key)], Ct);
        await Task.Delay(300, Ct);
        Assert.False(waiting.IsCompleted, "The second insert did not wait for the transaction that holds the key.");

        await first.Transaction.CommitAsync(Ct);
        var result = Assert.Single(await waiting);
        await second.Transaction.CommitAsync(Ct);

        Assert.True(result.UniqueSkippedAsDuplicate);
        Assert.Equal(held.Job.Id, result.Job.Id);
        Assert.Equal(1, await schema.CountJobsAsync(kind, Ct));
    }

    [Fact]
    public async Task A_second_transaction_inserts_the_key_when_the_first_rolls_back()
    {
        var schema = await MigratedAsync();
        var kind = UniqueKind();
        var key = NewUniqueKey();
        await using var first = await schema.BeginTransactionAsync(Ct);
        await using var second = await schema.BeginTransactionAsync(Ct);

        var rolledBack = Assert.Single(await schema.Driver.GetExecutor(first.Transaction).JobInsertManyAsync([Unique(Job(kind), key)], Ct));
        var waiting = schema.Driver.GetExecutor(second.Transaction).JobInsertManyAsync([Unique(Job(kind), key)], Ct);
        await Task.Delay(300, Ct);
        await first.Transaction.RollbackAsync(Ct);
        var result = Assert.Single(await waiting);
        await second.Transaction.CommitAsync(Ct);

        Assert.False(result.UniqueSkippedAsDuplicate);
        Assert.NotEqual(rolledBack.Job.Id, result.Job.Id);
        Assert.Equal(1, await schema.CountJobsAsync(kind, Ct));
    }

    [Fact]
    public async Task Unique_jobs_need_a_whole_key_and_states_and_the_fast_path_rejects_them()
    {
        var schema = await MigratedAsync();
        var executor = schema.Driver.GetExecutor();
        var unique = Unique(Job("k"), NewUniqueKey());

        await Assert.ThrowsAsync<NotSupportedException>(() => executor.JobInsertFastAsync([Job("k"), unique], Ct));
        await Assert.ThrowsAsync<ArgumentException>(() => executor.JobInsertManyAsync([Unique(Job("k"), [1, 2, 3])], Ct));
        await Assert.ThrowsAsync<ArgumentException>(() => executor.JobInsertManyAsync(
            [new JobInsertParams { Kind = "k", ArgsJson = "{}", Queue = "q", State = JobState.Available, Priority = 1, MaxAttempts = 1, UniqueKey = NewUniqueKey() }],
            Ct));
        await Assert.ThrowsAsync<ArgumentException>(() => executor.JobInsertManyAsync(
            [new JobInsertParams { Kind = "k", ArgsJson = "{}", Queue = "q", State = JobState.Available, Priority = 1, MaxAttempts = 1, UniqueStates = DefaultUniqueStates }],
            Ct));
    }

    [Fact]
    public async Task The_client_skips_duplicates_by_args_singly_and_in_batches()
    {
        var schema = await MigratedAsync();
        var client = new KopiClient(schema.Driver);
        await using var tx = await schema.BeginTransactionAsync(Ct);
        var byArgs = new InsertOptions { Queue = UniqueQueue(), Unique = new() { ByArgs = true, ByQueue = true } };

        var first = await client.InsertAsync(new ConformanceArgs(1), tx.Transaction, byArgs, Ct);
        var again = await client.InsertAsync(new ConformanceArgs(1), tx.Transaction, byArgs, Ct);
        var other = await client.InsertAsync(new ConformanceArgs(2), tx.Transaction, byArgs, Ct);
        var many = await client.InsertManyAsync(
            [new(new ConformanceArgs(1), byArgs), new(new ConformanceArgs(3), byArgs), new(new ConformanceArgs(3), byArgs), new(new ConformanceArgs(3))],
            tx.Transaction,
            Ct);

        Assert.False(first.UniqueSkippedAsDuplicate);
        Assert.True(again.UniqueSkippedAsDuplicate);
        Assert.Equal(first.Job.Id, again.Job.Id);
        Assert.False(other.UniqueSkippedAsDuplicate);
        Assert.Equal([true, false, true, false], many.Select(r => r.UniqueSkippedAsDuplicate));
        Assert.Equal(first.Job.Id, many[0].Job.Id);
        Assert.Equal(many[1].Job.Id, many[2].Job.Id);
        Assert.NotEqual(many[1].Job.Id, many[3].Job.Id); // the job without unique options is not deduplicated
        await Assert.ThrowsAsync<NotSupportedException>(() => client.InsertManyFastAsync([new(new ConformanceArgs(4), byArgs)], tx.Transaction, Ct));
    }

    [Fact]
    public async Task Concurrent_batches_sharing_unique_keys_in_different_orders_neither_deadlock_nor_store_a_key_twice()
    {
        // Plan task 4.6. Each batch holds the same keys in its own order; a driver that took key slots in input order
        // would let two batches each hold a key the other waits for.
        var schema = await MigratedAsync();
        var kind = UniqueKind();
        for (var round = 0; round < 3; round++)
        {
            var keys = Enumerable.Range(0, 30).Select(_ => NewUniqueKey()).ToArray();
            var batches = await Task.WhenAll(Enumerable.Range(0, 8).Select(batch => Task.Run(
                () => schema.Driver.GetExecutor().JobInsertManyAsync(
                    [.. keys.Select((key, i) => (key, i)).OrderBy(x => HashCode.Combine(x.i, batch, round)).Select(x => Unique(Job(kind), x.key))],
                    Ct),
                Ct)));

            Assert.Equal(30, batches.Sum(b => b.Count(r => !r.UniqueSkippedAsDuplicate)));
            Assert.Equal(30, batches.SelectMany(b => b).Select(r => r.Job.Id).Distinct().Count());
        }

        Assert.Equal(90, await schema.CountJobsAsync(kind, Ct));
    }

    /// <summary>A random 32-byte unique key, so that committed cases never meet each other's keys.</summary>
    protected static byte[] NewUniqueKey() => RandomNumberGenerator.GetBytes(32);

    private static JobInsertParams Unique(JobInsertParams job, byte[] key, byte states = DefaultUniqueStates) => new()
    {
        Kind = job.Kind,
        ArgsJson = job.ArgsJson,
        Queue = job.Queue,
        State = job.State,
        Priority = job.Priority,
        MaxAttempts = job.MaxAttempts,
        ScheduledAt = job.ScheduledAt,
        MetadataJson = job.MetadataJson,
        Tags = job.Tags,
        UniqueKey = key,
        UniqueStates = states,
    };

    private static JobInsertParams Numbered(int n) => new()
    {
        Kind = "k",
        ArgsJson = $"{{\"n\":{n}}}",
        Queue = "default",
        State = JobState.Available,
        Priority = 1,
        MaxAttempts = 25,
    };

    private static async Task<int> CountAsync(IKopiExecutor executor, params long[] ids)
    {
        var count = 0;
        foreach (var id in ids.Distinct())
        {
            count += await executor.JobGetByIdAsync(id, Ct) is null ? 0 : 1;
        }

        return count;
    }
}
