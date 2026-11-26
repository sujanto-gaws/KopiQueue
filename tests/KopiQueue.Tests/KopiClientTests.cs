using System.Data;
using System.Data.Common;
using KopiQueue.Drivers;
using KopiQueue.Serialization;
using Microsoft.Extensions.Time.Testing;

namespace KopiQueue.Tests;

/// <summary><see cref="KopiClient.InsertAsync{TArgs}(TArgs, InsertOptions?, CancellationToken)"/> against a fake driver (plan task 1.6: FR-INS-01, FR-INS-02).</summary>
public sealed class KopiClientTests
{
    private static readonly DateTimeOffset Now = new(2026, 11, 26, 12, 0, 0, TimeSpan.Zero);

    private static readonly SystemTextJsonArgsSerializer Json = new(WebJsonContext.Default);

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Fact]
    public async Task Insert_serialises_the_args_with_the_kind_and_defaults()
    {
        var driver = new RecordingDriver();
        var client = new KopiClient(driver, new KopiClientOptions { TimeProvider = new FakeTimeProvider(Now), ArgsSerializer = Json });
        var args = new SendEmailArgs("a@example.com", "welcome", [1, 2]);

        var result = await client.InsertAsync(args, cancellationToken: Ct);

        var insert = Assert.Single(driver.Inserts);
        Assert.Equal(("send_email", "default", JobState.Available, 1, 25), (insert.Kind, insert.Queue, insert.State, insert.Priority, insert.MaxAttempts));
        Assert.Equal("""{"to":"a@example.com","templateId":"welcome","ids":[1,2]}""", insert.ArgsJson);
        Assert.Null(driver.Transaction);
        Assert.Same(args, result.Args);
        Assert.Equal(1L, result.Job.Id);
        Assert.False(result.UniqueSkippedAsDuplicate);
    }

    [Fact]
    public async Task Insert_in_a_transaction_uses_the_transaction_executor()
    {
        var driver = new RecordingDriver();
        var client = new KopiClient(driver, new KopiClientOptions { TimeProvider = new FakeTimeProvider(Now), ArgsSerializer = Json });
        var tx = new FakeTransaction();

        await client.InsertAsync(new SendEmailArgs("b@example.com", "t", []), tx, new InsertOptions { Queue = "email" }, Ct);

        Assert.Same(tx, driver.Transaction);
        Assert.Equal("email", Assert.Single(driver.Inserts).Queue);
    }

    [Fact]
    public async Task A_future_time_by_the_clients_clock_schedules_the_job()
    {
        var driver = new RecordingDriver();
        var time = new FakeTimeProvider(Now);
        var client = new KopiClient(driver, new KopiClientOptions { TimeProvider = time, ArgsSerializer = Json });
        var at = Now.AddMinutes(10);

        await client.InsertAsync(new SendEmailArgs("c", "t", []), new InsertOptions { ScheduledAt = at }, Ct);
        time.Advance(TimeSpan.FromMinutes(11));
        await client.InsertAsync(new SendEmailArgs("c", "t", []), new InsertOptions { ScheduledAt = at }, Ct);

        Assert.Equal([JobState.Scheduled, JobState.Available], driver.Inserts.Select(i => i.State));
        Assert.All(driver.Inserts, i => Assert.Equal(at, i.ScheduledAt));
    }

    [Fact]
    public async Task Invalid_input_never_reaches_the_driver()
    {
        var driver = new RecordingDriver();
        var client = new KopiClient(driver, new KopiClientOptions { ArgsSerializer = Json });

        await Assert.ThrowsAsync<ArgumentException>(() => client.InsertAsync(new SendEmailArgs("d", "t", []), new InsertOptions { Priority = 0 }, Ct));
        await Assert.ThrowsAsync<KopiQueueConfigurationException>(() => client.InsertAsync(new NoKindArgs(), cancellationToken: Ct));
        await Assert.ThrowsAsync<ArgumentNullException>(() => client.InsertAsync<SendEmailArgs>(null!, cancellationToken: Ct));
        await Assert.ThrowsAsync<ArgumentNullException>(() => client.InsertAsync(new SendEmailArgs("d", "t", []), (DbTransaction)null!, cancellationToken: Ct));

        Assert.Empty(driver.Inserts);
        Assert.Throws<ArgumentNullException>(() => new KopiClient(null!));
    }

    [Fact]
    public async Task The_driver_result_is_returned_unchanged()
    {
        var driver = new RecordingDriver { SkipAsDuplicate = true };
        var client = new KopiClient(driver, new KopiClientOptions { ArgsSerializer = Json });

        var result = await client.InsertAsync(new SendEmailArgs("e", "t", []), cancellationToken: Ct);

        Assert.True(result.UniqueSkippedAsDuplicate);
        Assert.Equal("send_email", result.Job.Kind);
    }

    [Fact]
    public async Task A_driver_returning_the_wrong_row_count_is_an_error()
    {
        var client = new KopiClient(new RecordingDriver { ReturnNothing = true }, new KopiClientOptions { ArgsSerializer = Json });

        await Assert.ThrowsAsync<InvalidOperationException>(() => client.InsertAsync(new SendEmailArgs("f", "t", []), cancellationToken: Ct));
    }

    [Fact]
    public async Task Insert_many_reads_each_kind_from_the_runtime_type_and_keeps_the_order()
    {
        var driver = new RecordingDriver();
        var client = new KopiClient(driver, new KopiClientOptions { TimeProvider = new FakeTimeProvider(Now), ArgsSerializer = Json });
        IJobArgs[] args = [new SendEmailArgs("a", "t", []), new ResizeArgs(7), new SendEmailArgs("b", "t", [3])];

        var results = await client.InsertManyAsync(
            [new(args[0]), new(args[1], new InsertOptions { Queue = "images", Priority = 3 }), new(args[2])], cancellationToken: Ct);

        Assert.Equal(["send_email", "resize_image", "send_email"], driver.Inserts.Select(i => i.Kind));
        Assert.Equal(["""{"to":"a","templateId":"t","ids":[]}""", """{"width":7}""", """{"to":"b","templateId":"t","ids":[3]}"""], driver.Inserts.Select(i => i.ArgsJson));
        Assert.Equal(("images", 3), (driver.Inserts[1].Queue, driver.Inserts[1].Priority));
        Assert.Equal([1L, 2L, 3L], results.Select(r => r.Job.Id));
        Assert.Equal(1, driver.Calls);
        Assert.Null(driver.Transaction);
    }

    [Fact]
    public async Task Insert_many_validates_every_job_before_touching_the_database()
    {
        var driver = new RecordingDriver();
        var client = new KopiClient(driver, new KopiClientOptions { ArgsSerializer = Json });

        var ex = await Assert.ThrowsAsync<ArgumentException>(() => client.InsertManyAsync(
            [new(new ResizeArgs(1)), new(new ResizeArgs(2), new InsertOptions { Tags = ["no"] })], cancellationToken: Ct));
        await Assert.ThrowsAsync<KopiQueueConfigurationException>(() => client.InsertManyFastAsync([new(new ResizeArgs(1)), new(new NoKindArgs())], cancellationToken: Ct));
        var nullJob = await Assert.ThrowsAsync<ArgumentException>(() => client.InsertManyAsync([new(new ResizeArgs(1)), null!], cancellationToken: Ct));

        Assert.StartsWith("Job 1: Invalid insert option Tags", ex.Message, StringComparison.Ordinal);
        Assert.Equal("jobs", ex.ParamName);
        Assert.Equal("Job 1 is null. (Parameter 'jobs')", nullJob.Message);
        Assert.Equal(0, driver.Calls);
        await Assert.ThrowsAsync<ArgumentNullException>(() => client.InsertManyAsync(null!, cancellationToken: Ct));
        Assert.Throws<ArgumentNullException>(() => new InsertRequest(null!));
    }

    [Fact]
    public async Task An_empty_batch_does_not_touch_the_database()
    {
        var driver = new RecordingDriver();
        var client = new KopiClient(driver, new KopiClientOptions { ArgsSerializer = Json });

        Assert.Empty(await client.InsertManyAsync([], cancellationToken: Ct));
        Assert.Equal(0, await client.InsertManyFastAsync(Enumerable.Empty<InsertRequest>(), cancellationToken: Ct));
        Assert.Equal(0, driver.Calls);
    }

    [Fact]
    public async Task Insert_many_fast_returns_the_drivers_count_and_uses_the_transaction()
    {
        var driver = new RecordingDriver();
        var client = new KopiClient(driver, new KopiClientOptions { TimeProvider = new FakeTimeProvider(Now), ArgsSerializer = Json });
        var tx = new FakeTransaction();

        var count = await client.InsertManyFastAsync(Enumerable.Range(1, 3).Select(i => new InsertRequest(new ResizeArgs(i))), tx, Ct);

        Assert.Equal(3, count);
        Assert.Same(tx, driver.Transaction);
        Assert.Equal(1, driver.FastCalls);
        Assert.Equal(["""{"width":1}""", """{"width":2}""", """{"width":3}"""], driver.Inserts.Select(i => i.ArgsJson));
    }

    [Fact]
    public async Task Insert_many_rejects_a_driver_returning_the_wrong_row_count()
    {
        var client = new KopiClient(new RecordingDriver { ReturnNothing = true }, new KopiClientOptions { ArgsSerializer = Json });

        await Assert.ThrowsAsync<InvalidOperationException>(() => client.InsertManyAsync([new(new ResizeArgs(1))], cancellationToken: Ct));
    }

    [Fact]
    public async Task The_configured_serialiser_is_used_for_every_insert()
    {
        var driver = new RecordingDriver();
        var serializer = new CountingSerializer();
        var client = new KopiClient(driver, new KopiClientOptions { ArgsSerializer = serializer });

        await client.InsertAsync(new ResizeArgs(1), cancellationToken: Ct);
        await client.InsertManyAsync([new(new LargeResizeArgs(2, "x")), new(new ResizeArgs(3))], cancellationToken: Ct);
        await client.InsertManyFastAsync([new(new ResizeArgs(4))], cancellationToken: Ct);

        Assert.Equal([typeof(ResizeArgs), typeof(LargeResizeArgs), typeof(ResizeArgs), typeof(ResizeArgs)], serializer.Types);
        Assert.All(driver.Inserts, i => Assert.Equal("{\"custom\":true}", i.ArgsJson));
    }

    [Fact]
    public async Task Without_reflection_the_default_serialiser_fails_before_the_database()
    {
        var driver = new RecordingDriver();
        var client = new KopiClient(driver);

        var ex = await Assert.ThrowsAsync<NotSupportedException>(() => client.InsertAsync(new ResizeArgs(1), cancellationToken: Ct));
        await Assert.ThrowsAsync<NotSupportedException>(() => client.InsertManyAsync([new(new ResizeArgs(1))], cancellationToken: Ct));

        Assert.Contains("JsonSerializerContext", ex.Message, StringComparison.Ordinal);
        Assert.Equal(0, driver.Calls);
    }

    [Fact]
    public async Task A_type_missing_from_the_context_fails_before_the_database()
    {
        var driver = new RecordingDriver();
        var client = new KopiClient(driver, new KopiClientOptions { ArgsSerializer = Json });

        await Assert.ThrowsAsync<NotSupportedException>(() => client.InsertManyAsync([new(new ResizeArgs(1)), new(new UnlistedArgs(2))], cancellationToken: Ct));

        Assert.Equal(0, driver.Calls);
    }

    private sealed class CountingSerializer : IJobArgsSerializer
    {
        public List<Type> Types { get; } = [];

        public string Serialize(IJobArgs args, Type argsType)
        {
            Types.Add(argsType);
            return "{\"custom\":true}";
        }

        public IJobArgs Deserialize(string json, Type argsType) => throw new NotSupportedException();
    }

    private sealed record NoKindArgs : IJobArgs;

    private sealed class RecordingDriver : IKopiDriver, IKopiExecutor
    {
        public List<JobInsertParams> Inserts { get; } = [];

        public DbTransaction? Transaction { get; private set; }

        public bool SkipAsDuplicate { get; init; }

        public bool ReturnNothing { get; init; }

        public int Calls { get; private set; }

        public int FastCalls { get; private set; }

        public DriverCapabilities Capabilities { get; } = new() { ListenNotify = false, BulkCopy = false };

        public IKopiExecutor GetExecutor() => this;

        public IKopiExecutor GetExecutor(DbTransaction transaction)
        {
            Transaction = transaction;
            return this;
        }

        public KopiQueue.Migrations.KopiMigrator CreateMigrator(Microsoft.Extensions.Logging.ILoggerFactory? loggerFactory = null) =>
            throw new NotSupportedException();

        public Task<long> JobInsertFastAsync(IReadOnlyList<JobInsertParams> jobs, CancellationToken cancellationToken)
        {
            Calls++;
            FastCalls++;
            Inserts.AddRange(jobs);
            return Task.FromResult((long)jobs.Count);
        }

        public Task<IReadOnlyList<JobInsertResult>> JobInsertManyAsync(IReadOnlyList<JobInsertParams> jobs, CancellationToken cancellationToken)
        {
            Calls++;
            var first = Inserts.Count;
            Inserts.AddRange(jobs);
            if (ReturnNothing)
            {
                return Task.FromResult<IReadOnlyList<JobInsertResult>>([]);
            }

            return Task.FromResult<IReadOnlyList<JobInsertResult>>(
                [.. jobs.Select((j, i) => new JobInsertResult(
                    new JobRow
                    {
                        Id = first + i + 1, Kind = j.Kind, Queue = j.Queue, State = j.State, Priority = j.Priority,
                        ArgsJson = j.ArgsJson, MetadataJson = j.MetadataJson, Tags = j.Tags, Attempt = 0, MaxAttempts = j.MaxAttempts,
                        CreatedAt = Now, ScheduledAt = j.ScheduledAt ?? Now,
                    },
                    SkipAsDuplicate))]);
        }

        public Task<JobRow?> JobGetByIdAsync(long id, CancellationToken cancellationToken) => throw new NotSupportedException();
    }

    private sealed class FakeTransaction : DbTransaction
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
