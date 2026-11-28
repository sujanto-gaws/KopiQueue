using System.Diagnostics;
using KopiQueue.Drivers;
using KopiQueue.Migrations;
using KopiQueue.SqlServer.Tests.Migrations;
using KopiQueue.TestInfrastructure;
using Microsoft.Data.SqlClient;

namespace KopiQueue.SqlServer.Tests;

/// <summary>
/// The leader, maintenance, queue and admin statements on SQL Server (plan task 5.6: FR-MNT-*, FR-ADM-*, design §8.1):
/// what the conformance suite does not reach on this driver, such as batches split across statements, the row locks a
/// maintenance statement holds, durations longer than <c>DATEADD</c>'s <c>int</c> of milliseconds, and races that
/// SQL Server's locking, unlike PostgreSQL's <c>ON CONFLICT</c>, has to settle.
/// </summary>
public sealed class SqlServerMaintenanceTests(SqlServerFixture db) : IClassFixture<SqlServerFixture>
{
    private static readonly DateTimeOffset Past = DateTimeOffset.UtcNow.AddHours(-1);

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Fact]
    public async Task One_schedule_call_promotes_more_due_jobs_than_one_statement_touches()
    {
        var (driver, _) = await MigratedAsync();
        await driver.Executor().JobInsertManyAsync([.. Enumerable.Range(0, 2_500).Select(i => Job(JobState.Scheduled, Past.AddSeconds(i)))], Ct);

        var promoted = await driver.Executor().JobScheduleAsync(new JobScheduleParams { Max = 10_000 }, Ct);

        Assert.Equal(2_500, promoted.Count);
        Assert.All(promoted, r => Assert.Equal(JobState.Available, r.State));
        Assert.Equal(promoted.Select(r => r.Id).Order(), promoted.Select(r => r.Id));
    }

    [Fact]
    public async Task A_job_the_schedule_leaves_alone_does_not_end_the_call_early_or_come_back()
    {
        // The most overdue job is retryable and unique, its slot taken, and its states include discarded: it is left as it
        // is. The first statement's 1,000 due jobs include it, so the call must go on, after it, rather than stop or re-read it.
        var (driver, _) = await MigratedAsync();
        var key = new byte[32];
        key[0] = 7;
        var waiting = (await driver.Executor().JobInsertManyAsync(
            [Job(JobState.Retryable, Past.AddDays(-1), key, states: 0b0000_1001)], Ct))[0].Job;
        await driver.Executor().JobInsertManyAsync([Job(JobState.Available, Past, key, states: 0b1111_0101)], Ct);
        await driver.Executor().JobInsertManyAsync([.. Enumerable.Range(0, 1_500).Select(i => Job(JobState.Scheduled, Past.AddSeconds(i)))], Ct);

        var promoted = await driver.Executor().JobScheduleAsync(new JobScheduleParams { Max = 10_000 }, Ct);

        Assert.Equal(1_500, promoted.Count);
        Assert.DoesNotContain(promoted, r => r.Id == waiting.Id);
        Assert.Equal(JobState.Retryable, (await driver.Executor().JobGetByIdAsync(waiting.Id, Ct))!.State);
    }

    [Fact]
    public async Task Maintenance_in_an_open_transaction_holds_row_locks_and_blocks_no_fetcher()
    {
        // Design §8.1 rules 3 and 4: seeks only, no more than 1,000 rows per statement, so no lock escalates and a fetcher
        // of another queue never waits.
        var (driver, schema) = await MigratedAsync();
        await driver.Executor().JobInsertManyAsync([.. Enumerable.Range(0, 1_000).Select(i => Job(JobState.Scheduled, Past.AddSeconds(i)))], Ct);
        var finished = await driver.Executor().JobInsertManyAsync([.. Enumerable.Range(0, 1_000).Select(_ => Job(JobState.Available, Past, queue: "done"))], Ct);
        await db.ExecuteAsync(
            $"UPDATE [{schema}].kopi_job SET state = 'completed', finalized_at = DATEADD(day, -2, SYSUTCDATETIME()) WHERE queue = 'done'");
        await driver.Executor().JobInsertManyAsync([Job(JobState.Available, Past, queue: "other")], Ct);
        await using var connection = await db.OpenConnectionAsync(Ct);
        await using var tx = (SqlTransaction)await connection.BeginTransactionAsync(Ct);
        var executor = ((IKopiDriver)driver).GetExecutor(tx);

        var promoted = await executor.JobScheduleAsync(new JobScheduleParams { Max = 1_000 }, Ct);
        var deleted = await executor.JobDeleteFinalizedAsync(new JobDeleteFinalizedParams { CompletedRetention = TimeSpan.FromDays(1), Max = 1_000 }, Ct);
        var locks = await LockModesAsync(connection, tx);
        var watch = Stopwatch.StartNew();
        var fetched = await driver.Executor().JobGetAvailableAsync(new JobGetAvailableParams { Queue = "other", Max = 10, ClientId = "c" }, Ct);
        var fetchTime = watch.Elapsed;
        await tx.RollbackAsync(Ct);

        Assert.Equal((1_000, 1_000), (promoted.Count, deleted.Count));
        Assert.Equal(finished.Select(r => r.Job.Id), deleted);
        Assert.DoesNotContain(locks, l => l.Type is "PAGE" or "OBJECT" && l.Mode is "X");
        Assert.Single(fetched);
        Assert.True(fetchTime < TimeSpan.FromSeconds(5), $"The fetch waited {fetchTime}.");
    }

    [Fact]
    public async Task A_cleaner_call_deletes_across_statements_with_retentions_of_weeks()
    {
        var (driver, schema) = await MigratedAsync();
        var jobs = await driver.Executor().JobInsertManyAsync([.. Enumerable.Range(0, 1_600).Select(_ => Job(JobState.Available, Past))], Ct);
        var ids = jobs.Select(r => r.Job.Id).ToArray();
        await db.ExecuteAsync($"""
            UPDATE [{schema}].kopi_job
            SET state = CASE WHEN id % 2 = 0 THEN 'completed' ELSE 'discarded' END,
                finalized_at = DATEADD(day, CASE WHEN id <= {ids[1_499]} THEN -41 ELSE -39 END, SYSUTCDATETIME())
            """);

        var deleted = await driver.Executor().JobDeleteFinalizedAsync(
            new JobDeleteFinalizedParams { CompletedRetention = TimeSpan.FromDays(40), DiscardedRetention = TimeSpan.FromDays(40), Max = 5_000 }, Ct);

        Assert.Equal(ids[..1_500], deleted);
    }

    [Fact]
    public async Task A_delete_many_call_deletes_more_jobs_than_one_statement_touches()
    {
        var (driver, _) = await MigratedAsync();
        var ids = (await driver.Executor().JobInsertManyAsync([.. Enumerable.Range(0, 2_500).Select(_ => Job(JobState.Available, Past, kind: "bulk"))], Ct))
            .Select(r => r.Job.Id);

        var deleted = await driver.Executor().JobDeleteManyAsync(
            new JobDeleteManyQuery { Filter = new JobDeleteManyParams { Kinds = ["bulk"] }, Max = 10_000 }, Ct);

        Assert.Equal(ids, deleted.Select(j => j.Id));
    }

    [Fact]
    public async Task Clients_electing_at_once_get_exactly_one_leader()
    {
        var (driver, _) = await MigratedAsync();
        var ttl = TimeSpan.FromSeconds(30);

        for (var round = 0; round < 5; round++)
        {
            var results = await Task.WhenAll(Enumerable.Range(0, 8).Select(i => Task.Run(() =>
                driver.Executor().LeaderAttemptElectAsync(new LeaderElectParams { LeaderId = $"client-{round}-{i}", Ttl = ttl }, Ct))));

            var winner = Assert.Single(results.Select((won, i) => (won, i)), r => r.won).i;
            Assert.True(await driver.Executor().LeaderAttemptReelectAsync(new LeaderElectParams { LeaderId = $"client-{round}-{winner}", Ttl = ttl }, Ct));
            Assert.True(await driver.Executor().LeaderResignAsync($"client-{round}-{winner}", Ct));
        }
    }

    [Fact]
    public async Task Clients_creating_the_same_queues_at_once_neither_fail_nor_duplicate_them()
    {
        var (driver, _) = await MigratedAsync();
        var names = Enumerable.Range(0, 20).Select(i => $"q{i:D2}").ToArray();

        var results = await Task.WhenAll(Enumerable.Range(0, 8).Select(i => Task.Run(() =>
            driver.Executor().QueueCreateOrSetUpdatedAtManyAsync([.. names.OrderBy(n => (n.GetHashCode(StringComparison.Ordinal) * (i + 1)) % 97)], Ct))));

        Assert.All(results, rows => Assert.Equal(names, rows.Select(q => q.Name)));
        Assert.Equal(names, (await driver.Executor().QueueListAsync(new QueueListQuery { Max = 100 }, Ct)).Select(q => q.Name));
    }

    [Fact]
    public async Task Listing_stuck_jobs_returns_the_database_time_when_none_are_stuck()
    {
        var (driver, _) = await MigratedAsync();

        var result = await driver.Executor().JobListStuckAsync(new JobListStuckParams { StuckAfter = TimeSpan.FromDays(400), Max = 10 }, Ct);

        Assert.Empty(result.Jobs);
        Assert.InRange(result.Now, DateTimeOffset.UtcNow.AddMinutes(-5), DateTimeOffset.UtcNow.AddMinutes(5));
    }

    private async Task<(SqlServerDriver Driver, string Schema)> MigratedAsync()
    {
        db.SkipIfUnavailable();
        var schema = await db.CreateSchemaAsync();
        var driver = new SqlServerDriver(db.ConnectionString, new SqlServerDriverOptions { Schema = schema });
        await driver.CreateMigrator().MigrateAsync(MigrationDirection.Up, cancellationToken: Ct);
        return (driver, schema);
    }

    private static async Task<List<(string Type, string Mode)>> LockModesAsync(SqlConnection connection, SqlTransaction tx)
    {
        await using var cmd = new SqlCommand(
            "SELECT DISTINCT resource_type, request_mode FROM sys.dm_tran_locks WHERE request_session_id = @@SPID AND resource_database_id = DB_ID()",
            connection,
            tx);
        await using var reader = await cmd.ExecuteReaderAsync(Ct);
        var locks = new List<(string, string)>();
        while (await reader.ReadAsync(Ct))
        {
            locks.Add((reader.GetString(0).Trim(), reader.GetString(1).Trim()));
        }

        return locks;
    }

    private static JobInsertParams Job(
        JobState state, DateTimeOffset scheduledAt, byte[]? key = null, byte? states = null, string queue = "default", string kind = "k") => new()
    {
        Kind = kind,
        ArgsJson = "{}",
        Queue = queue,
        State = state,
        Priority = 1,
        MaxAttempts = 25,
        ScheduledAt = scheduledAt,
        UniqueKey = key,
        UniqueStates = states,
    };
}
