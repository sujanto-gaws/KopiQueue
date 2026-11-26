using System.Data;
using System.Data.Common;
using KopiQueue.Drivers;
using Microsoft.Extensions.Time.Testing;

namespace KopiQueue.Tests;

/// <summary><see cref="KopiClient.InsertAsync{TArgs}(TArgs, InsertOptions?, CancellationToken)"/> against a fake driver (plan task 1.6: FR-INS-01, FR-INS-02).</summary>
public sealed class KopiClientTests
{
    private static readonly DateTimeOffset Now = new(2026, 11, 26, 12, 0, 0, TimeSpan.Zero);

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Fact]
    public async Task Insert_serialises_the_args_with_the_kind_and_defaults()
    {
        var driver = new RecordingDriver();
        var client = new KopiClient(driver, new FakeTimeProvider(Now));
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
        var client = new KopiClient(driver, new FakeTimeProvider(Now));
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
        var client = new KopiClient(driver, time);
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
        var client = new KopiClient(driver);

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
        var client = new KopiClient(driver);

        var result = await client.InsertAsync(new SendEmailArgs("e", "t", []), cancellationToken: Ct);

        Assert.True(result.UniqueSkippedAsDuplicate);
        Assert.Equal("send_email", result.Job.Kind);
    }

    [Fact]
    public async Task A_driver_returning_the_wrong_row_count_is_an_error()
    {
        var client = new KopiClient(new RecordingDriver { ReturnNothing = true });

        await Assert.ThrowsAsync<InvalidOperationException>(() => client.InsertAsync(new SendEmailArgs("f", "t", []), cancellationToken: Ct));
    }

    [JobKind("send_email")]
    private sealed record SendEmailArgs(string To, string TemplateId, int[] Ids) : IJobArgs;

    private sealed record NoKindArgs : IJobArgs;

    private sealed class RecordingDriver : IKopiDriver, IKopiExecutor
    {
        public List<JobInsertParams> Inserts { get; } = [];

        public DbTransaction? Transaction { get; private set; }

        public bool SkipAsDuplicate { get; init; }

        public bool ReturnNothing { get; init; }

        public DriverCapabilities Capabilities { get; } = new() { ListenNotify = false, BulkCopy = false };

        public IKopiExecutor GetExecutor() => this;

        public IKopiExecutor GetExecutor(DbTransaction transaction)
        {
            Transaction = transaction;
            return this;
        }

        public KopiQueue.Migrations.KopiMigrator CreateMigrator(Microsoft.Extensions.Logging.ILoggerFactory? loggerFactory = null) =>
            throw new NotSupportedException();

        public Task<IReadOnlyList<JobInsertResult>> JobInsertManyAsync(IReadOnlyList<JobInsertParams> jobs, CancellationToken cancellationToken)
        {
            Inserts.AddRange(jobs);
            if (ReturnNothing)
            {
                return Task.FromResult<IReadOnlyList<JobInsertResult>>([]);
            }

            return Task.FromResult<IReadOnlyList<JobInsertResult>>(
                [.. jobs.Select(j => new JobInsertResult(
                    new JobRow
                    {
                        Id = Inserts.Count, Kind = j.Kind, Queue = j.Queue, State = j.State, Priority = j.Priority,
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
