using System.Data;
using System.Data.Common;
using System.Text.Json;
using KopiQueue.Drivers;
using Xunit;

namespace KopiQueue.TestInfrastructure.Conformance;

/// <summary>Insert cases (plan tasks 1.4-1.7: FR-INS-01, -02, -04, -05, -06).</summary>
public abstract partial class DriverConformanceTests<TFixture>
{
    [Fact]
    public async Task Insert_returns_the_stored_row_with_the_database_defaults()
    {
        var schema = await MigratedAsync();
        Assert.True(schema.Driver.Capabilities.MaxRowsPerStatement is null or > 0);
        await using var tx = await schema.BeginTransactionAsync(Ct);
        var before = DateTimeOffset.UtcNow.AddMinutes(-5);

        var result = Assert.Single(await schema.Driver.GetExecutor(tx.Transaction).JobInsertManyAsync([Job("k")], Ct));

        var job = result.Job;
        Assert.False(result.UniqueSkippedAsDuplicate);
        Assert.True(job.Id > 0);
        Assert.Equal(("k", "default", JobState.Available, 1, 0, 25), (job.Kind, job.Queue, job.State, job.Priority, job.Attempt, job.MaxAttempts));
        Assert.Equal(1, JsonElement.Parse(job.ArgsJson).GetProperty("n").GetInt32());
        Assert.Empty(JsonElement.Parse(job.MetadataJson).EnumerateObject());
        Assert.Empty(job.Tags);
        Assert.Empty(job.AttemptedBy);
        Assert.Empty(job.Errors);
        Assert.Null(job.AttemptedAt);
        Assert.Null(job.FinalizedAt);
        Assert.InRange(job.CreatedAt, before, DateTimeOffset.UtcNow.AddMinutes(5));
        Assert.Equal(job.CreatedAt, job.ScheduledAt);
        Assert.Equal(TimeSpan.Zero, job.CreatedAt.Offset);
    }

    [Fact]
    public async Task Insert_many_keeps_the_input_order_and_every_field()
    {
        var schema = await MigratedAsync();
        await using var tx = await schema.BeginTransactionAsync(Ct);
        var later = Microseconds(DateTimeOffset.UtcNow.AddDays(3));
        JobInsertParams[] jobs =
        [
            Job("a"),
            new()
            {
                Kind = "b", ArgsJson = "[1, \"two\"]", Queue = "reports", State = JobState.Scheduled, Priority = 4, MaxAttempts = 3,
                ScheduledAt = later, MetadataJson = "{\"k\": \"v\"}", Tags = ["nightly", "batch-1"],
            },
            Job("c"),
        ];

        var rows = (await schema.Driver.GetExecutor(tx.Transaction).JobInsertManyAsync(jobs, Ct)).Select(r => r.Job).ToList();

        Assert.Equal(["a", "b", "c"], rows.Select(r => r.Kind));
        Assert.True(rows[0].Id < rows[1].Id && rows[1].Id < rows[2].Id);
        var b = rows[1];
        Assert.Equal(("reports", JobState.Scheduled, 4, 3, later), (b.Queue, b.State, b.Priority, b.MaxAttempts, b.ScheduledAt));
        Assert.Equal("two", JsonElement.Parse(b.ArgsJson)[1].GetString());
        Assert.Equal("v", JsonElement.Parse(b.MetadataJson).GetProperty("k").GetString());
        Assert.Equal(["nightly", "batch-1"], b.Tags);
    }

    [Fact]
    public async Task Every_insertable_state_round_trips()
    {
        var schema = await MigratedAsync();
        await using var tx = await schema.BeginTransactionAsync(Ct);
        JobState[] states = [JobState.Available, JobState.Pending, JobState.Retryable, JobState.Running, JobState.Scheduled];

        var rows = await schema.Driver.GetExecutor(tx.Transaction).JobInsertManyAsync([.. states.Select(s => Job("k", state: s))], Ct);

        Assert.Equal(states, rows.Select(r => r.Job.State));
    }

    [Fact]
    public async Task A_batch_larger_than_one_statement_stays_ordered()
    {
        var schema = await MigratedAsync();
        await using var tx = await schema.BeginTransactionAsync(Ct);
        var count = ((schema.Driver.Capabilities.MaxRowsPerStatement ?? 1_000) * 2) + 1;

        var rows = await schema.Driver.GetExecutor(tx.Transaction).JobInsertManyAsync([.. Enumerable.Range(0, count).Select(i => Job($"k{i}"))], Ct);
        var copied = await schema.Driver.GetExecutor(tx.Transaction).JobInsertFastAsync([.. Enumerable.Range(0, count).Select(i => Job($"k{i}"))], Ct);

        Assert.Equal(Enumerable.Range(0, count).Select(i => $"k{i}"), rows.Select(r => r.Job.Kind));
        Assert.Equal(rows.Select(r => r.Job.Id).Order(), rows.Select(r => r.Job.Id));
        Assert.Equal(count, copied);
    }

    [Fact]
    public async Task The_fast_path_stores_the_same_rows_as_the_batch_insert()
    {
        var schema = await MigratedAsync();
        var kind = UniqueKind();
        var later = Microseconds(DateTimeOffset.UtcNow.AddHours(2));
        JobInsertParams[] jobs =
        [
            Job(kind),
            new() { Kind = kind, ArgsJson = "{\"x\": [1, 2]}", Queue = "q", State = JobState.Scheduled, Priority = 3, MaxAttempts = 2, ScheduledAt = later, MetadataJson = "{\"m\": 1}", Tags = ["tag-a"] },
        ];
        await using var tx = await schema.BeginTransactionAsync(Ct);
        var executor = schema.Driver.GetExecutor(tx.Transaction);

        var batch = (await executor.JobInsertManyAsync(jobs, Ct)).Select(r => r.Job).ToList();
        Assert.Equal(2, await executor.JobInsertFastAsync(jobs, Ct));
        var marker = (await executor.JobInsertManyAsync([Job(kind)], Ct))[0].Job.Id;
        var fast = new List<JobRow>();
        for (var id = batch[^1].Id + 1; id < marker; id++)
        {
            if (await executor.JobGetByIdAsync(id, Ct) is { } row)
            {
                fast.Add(row);
            }
        }

        static object Shape(JobRow j) => (j.Kind, j.Queue, j.State, j.Priority, j.MaxAttempts, Normalise(j.ArgsJson), Normalise(j.MetadataJson), string.Join('|', j.Tags), j.Attempt);
        Assert.Equal(batch.Select(Shape), fast.Select(Shape));
        Assert.Equal(later, fast[1].ScheduledAt);
        Assert.Equal(fast[0].CreatedAt, fast[0].ScheduledAt);
    }

    [Fact]
    public async Task Empty_batches_return_nothing()
    {
        var schema = await MigratedAsync();

        Assert.Empty(await schema.Driver.GetExecutor().JobInsertManyAsync([], Ct));
        Assert.Equal(0, await schema.Driver.GetExecutor().JobInsertFastAsync([], Ct));
    }

    [Fact]
    public async Task Jobs_in_a_caller_transaction_are_visible_only_to_it_until_commit()
    {
        var schema = await MigratedAsync();
        var kind = UniqueKind();
        long id;

        await using (var tx = await schema.BeginTransactionAsync(Ct))
        {
            var executor = schema.Driver.GetExecutor(tx.Transaction);
            id = (await executor.JobInsertManyAsync([Job(kind)], Ct))[0].Job.Id;
            await executor.JobInsertFastAsync([Job(kind), Job(kind)], Ct);

            Assert.NotNull(await executor.JobGetByIdAsync(id, Ct));
            Assert.Equal(0, await schema.CountJobsAsync(kind, Ct));
            await tx.Transaction.CommitAsync(Ct);
        }

        Assert.Equal(3, await schema.CountJobsAsync(kind, Ct));
        Assert.Equal(kind, (await schema.Driver.GetExecutor().JobGetByIdAsync(id, Ct))?.Kind);
    }

    [Fact]
    public async Task Jobs_in_a_rolled_back_transaction_are_gone()
    {
        var schema = await MigratedAsync();
        var kind = UniqueKind();
        long id;

        await using (var tx = await schema.BeginTransactionAsync(Ct))
        {
            var executor = schema.Driver.GetExecutor(tx.Transaction);
            id = (await executor.JobInsertManyAsync([Job(kind)], Ct))[0].Job.Id;
            await executor.JobInsertFastAsync([Job(kind)], Ct);
            await tx.Transaction.RollbackAsync(Ct);
        }

        Assert.Equal(0, await schema.CountJobsAsync(kind, Ct));
        Assert.Null(await schema.Driver.GetExecutor().JobGetByIdAsync(id, Ct));
    }

    [Fact]
    public async Task A_pooled_insert_commits_on_its_own()
    {
        var schema = await MigratedAsync();
        var kind = UniqueKind();

        await schema.Driver.GetExecutor().JobInsertManyAsync([Job(kind), Job(kind)], Ct);
        await schema.Driver.GetExecutor().JobInsertFastAsync([Job(kind)], Ct);

        Assert.Equal(3, await schema.CountJobsAsync(kind, Ct));
    }

    [Fact]
    public async Task A_batch_that_fails_in_the_database_inserts_nothing()
    {
        var schema = await MigratedAsync();
        var kind = UniqueKind();

        // Priority 9 passes the driver but fails the table's CHECK: the valid first job must not be committed alone.
        JobInsertParams[] jobs =
        [
            Job(kind),
            new() { Kind = kind, ArgsJson = "{}", Queue = "q", State = JobState.Available, Priority = 9, MaxAttempts = 1 },
        ];

        await Assert.ThrowsAnyAsync<DbException>(() => schema.Driver.GetExecutor().JobInsertManyAsync(jobs, Ct));
        await Assert.ThrowsAnyAsync<DbException>(() => schema.Driver.GetExecutor().JobInsertFastAsync(jobs, Ct));

        Assert.Equal(0, await schema.CountJobsAsync(kind, Ct));
    }

    [Fact]
    public async Task Get_by_id_returns_null_for_a_missing_job()
    {
        var schema = await MigratedAsync();

        Assert.Null(await schema.Driver.GetExecutor().JobGetByIdAsync(long.MaxValue, Ct));
    }

    [Fact]
    public async Task A_foreign_or_completed_transaction_is_rejected()
    {
        var schema = await MigratedAsync();
        await using var tx = await schema.BeginTransactionAsync(Ct);
        await tx.Transaction.CommitAsync(Ct);

        Assert.Throws<ArgumentException>(() => schema.Driver.GetExecutor(new ForeignTransaction()));
        Assert.Throws<ArgumentNullException>(() => schema.Driver.GetExecutor(null!));
        await Assert.ThrowsAsync<InvalidOperationException>(() => schema.Driver.GetExecutor(tx.Transaction).JobInsertManyAsync([Job("late")], Ct));
        await Assert.ThrowsAsync<InvalidOperationException>(() => schema.Driver.GetExecutor(tx.Transaction).JobInsertFastAsync([Job("late")], Ct));
        await Assert.ThrowsAsync<InvalidOperationException>(() => schema.Driver.GetExecutor(tx.Transaction).JobGetByIdAsync(1, Ct));
    }

    [Fact]
    public async Task The_client_inserts_through_the_driver_in_a_transaction()
    {
        var schema = await MigratedAsync();
        var client = new KopiClient(schema.Driver);
        await using var tx = await schema.BeginTransactionAsync(Ct);

        var single = await client.InsertAsync(new ConformanceArgs(1), tx.Transaction, new InsertOptions { Queue = "email", Tags = ["welcome"] }, Ct);
        var scheduled = await client.InsertAsync(new ConformanceArgs(2), tx.Transaction, new InsertOptions { ScheduledAt = DateTimeOffset.UtcNow.AddHours(1) }, Ct);
        var many = await client.InsertManyAsync([new(new ConformanceArgs(3)), new(new ConformanceArgs(4), new InsertOptions { Priority = 2 })], tx.Transaction, Ct);
        var copied = await client.InsertManyFastAsync([new(new ConformanceArgs(5))], tx.Transaction, Ct);

        Assert.Equal(("conformance", "email", JobState.Available), (single.Job.Kind, single.Job.Queue, single.Job.State));
        Assert.Equal(["welcome"], single.Job.Tags);
        Assert.Equal(1, JsonElement.Parse(single.Job.ArgsJson).GetProperty("n").GetInt32());
        Assert.Equal(JobState.Scheduled, scheduled.Job.State);
        Assert.Equal([1, 2], many.Select(r => r.Job.Priority));
        Assert.Equal(1, copied);
    }

    private static string Normalise(string json) => JsonSerializer.Serialize(JsonElement.Parse(json));

    private sealed class ForeignTransaction : DbTransaction
    {
        public override IsolationLevel IsolationLevel => IsolationLevel.ReadCommitted;

        protected override DbConnection? DbConnection => null;

        public override void Commit()
        {
        }

        public override void Rollback()
        {
        }
    }
}

/// <summary>Args used by the conformance suite's client cases.</summary>
/// <param name="N">A number.</param>
[JobKind("conformance")]
public sealed record ConformanceArgs(int N) : IJobArgs;
