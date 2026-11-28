using System.Data;
using System.Security.Cryptography;
using KopiQueue.Drivers;
using KopiQueue.Migrations;
using KopiQueue.SqlServer.Tests.Migrations;
using KopiQueue.TestInfrastructure;
using Microsoft.Data.SqlClient;

namespace KopiQueue.SqlServer.Tests;

/// <summary>
/// Unique inserts on SQL Server (plan task 5.4: FR-UNQ-02, FR-UNQ-03, FR-UNQ-05, ADR-011): the transaction-owned
/// <c>sp_getapplock</c> on each key, <c>READCOMMITTEDLOCK</c> reads of the holder, and <c>unique_slot</c>.
/// </summary>
public sealed class SqlServerUniqueTests(SqlServerFixture db) : IClassFixture<SqlServerFixture>
{
    private const byte DefaultStates = 0b1111_0101;

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Fact]
    public async Task A_job_that_leaves_its_unique_states_frees_its_key()
    {
        var (driver, schema) = await MigratedAsync();
        var (freed, kept) = (Key(), Key());
        var executor = driver.Executor();
        var defaultStates = (await executor.JobInsertManyAsync([Unique(freed)], Ct))[0].Job;
        var allStates = (await executor.JobInsertManyAsync([Unique(kept, states: 0b1111_1111)], Ct))[0].Job;
        await db.ExecuteAsync(
            $"UPDATE [{schema}].kopi_job SET state = 'cancelled', finalized_at = SYSUTCDATETIME() WHERE id IN ({defaultStates.Id}, {allStates.Id})");

        var results = await executor.JobInsertManyAsync([Unique(freed), Unique(kept)], Ct);

        Assert.False(results[0].UniqueSkippedAsDuplicate);
        Assert.NotEqual(defaultStates.Id, results[0].Job.Id);
        Assert.True(results[1].UniqueSkippedAsDuplicate);
        Assert.Equal((allStates.Id, JobState.Cancelled), (results[1].Job.Id, results[1].Job.State));
    }

    [Fact]
    public async Task A_snapshot_caller_gets_the_job_committed_after_its_snapshot_began()
    {
        // Spike 0.10: without READCOMMITTEDLOCK a SNAPSHOT caller sees no holder, inserts, and fails on the unique index.
        var (driver, _) = await MigratedAsync();
        await db.ExecuteAsync("ALTER DATABASE CURRENT SET ALLOW_SNAPSHOT_ISOLATION ON");
        var key = Key();
        await using var connection = await db.OpenConnectionAsync(Ct);
        await using var snapshot = (SqlTransaction)await connection.BeginTransactionAsync(IsolationLevel.Snapshot, Ct);
        var executor = ((IKopiDriver)driver).GetExecutor(snapshot);
        Assert.Null(await executor.JobGetByIdAsync(1, Ct)); // the snapshot begins at its first read

        var committed = (await driver.Executor().JobInsertManyAsync([Unique(key)], Ct))[0].Job;
        var result = Assert.Single(await executor.JobInsertManyAsync([Unique(key)], Ct));
        await snapshot.CommitAsync(Ct);

        Assert.True(result.UniqueSkippedAsDuplicate);
        Assert.Equal(committed.Id, result.Job.Id);
    }

    [Fact]
    public async Task Different_keys_do_not_wait_for_each_other_in_caller_transactions()
    {
        // ADR-011: HOLDLOCK key-range locks made unrelated keys block each other; the applock serialises one key only.
        var (driver, _) = await MigratedAsync();
        await using var first = await db.OpenConnectionAsync(Ct);
        await using var firstTx = (SqlTransaction)await first.BeginTransactionAsync(Ct);
        await ((IKopiDriver)driver).GetExecutor(firstTx).JobInsertManyAsync([Unique(Key())], Ct);
        await using var second = await db.OpenConnectionAsync(Ct);
        await using var secondTx = (SqlTransaction)await second.BeginTransactionAsync(Ct);

        var other = ((IKopiDriver)driver).GetExecutor(secondTx).JobInsertManyAsync([Unique(Key()), Unique(Key())], Ct);

        Assert.Same(other, await Task.WhenAny(other, Task.Delay(TimeSpan.FromSeconds(10), Ct)));
        Assert.All(await other, r => Assert.False(r.UniqueSkippedAsDuplicate));
    }

    [Fact]
    public async Task The_same_key_in_another_schema_or_prefix_is_another_key()
    {
        var (driver, _) = await MigratedAsync();
        var (otherDriver, _) = await MigratedAsync();
        var otherSchemaName = otherDriver.Schema;
        var otherPrefix = new SqlServerDriver(db.ConnectionString, new SqlServerDriverOptions { Schema = driver.Schema, TablePrefix = "other_" });
        await otherPrefix.CreateMigrator().MigrateAsync(MigrationDirection.Up, cancellationToken: Ct);
        var key = Key();
        await using var connection = await db.OpenConnectionAsync(Ct);
        await using var tx = (SqlTransaction)await connection.BeginTransactionAsync(Ct);
        await ((IKopiDriver)driver).GetExecutor(tx).JobInsertManyAsync([Unique(key)], Ct);

        var elsewhere = Task.WhenAll(
            otherDriver.Executor().JobInsertManyAsync([Unique(key)], Ct),
            otherPrefix.Executor().JobInsertManyAsync([Unique(key)], Ct));

        Assert.Same(elsewhere, await Task.WhenAny(elsewhere, Task.Delay(TimeSpan.FromSeconds(10), Ct)));
        Assert.All((await elsewhere).SelectMany(r => r), r => Assert.False(r.UniqueSkippedAsDuplicate));
        Assert.NotEqual(driver.Schema, otherSchemaName);
    }

    [Fact]
    public async Task Unique_jobs_across_the_statements_of_a_large_batch_keep_order_and_dedupe()
    {
        var (driver, _) = await MigratedAsync();
        var held = Key();
        var existing = (await driver.Executor().JobInsertManyAsync([Unique(held)], Ct))[0].Job;
        var repeated = Key();
        var jobs = Enumerable.Range(0, 2100).Select(i => i switch
        {
            10 or 1500 => Unique(repeated, n: i),
            1200 or 2050 => Unique(held, n: i),
            _ when i % 7 == 0 => Unique(Key(), n: i),
            _ => Plain(i),
        }).ToList();

        var results = await driver.Executor().JobInsertManyAsync(jobs, Ct);

        var skipped = results.Select((r, i) => (r, i)).Where(x => x.r.UniqueSkippedAsDuplicate).Select(x => x.i).ToList();
        Assert.Equal([1200, 1500, 2050], skipped);
        Assert.Equal(results[10].Job.Id, results[1500].Job.Id);
        Assert.Equal(existing.Id, results[1200].Job.Id);
        var inserted = results.Where(r => !r.UniqueSkippedAsDuplicate).Select(r => r.Job.Id).ToList();
        Assert.Equal(inserted.Order(), inserted);
        Assert.Equal(2097, inserted.Count);
        Assert.Equal(2098, results.Select(r => r.Job.Id).Distinct().Count()); // the 2,097 inserted, and the existing holder
    }

    [Fact]
    public async Task The_unique_key_and_states_are_stored_and_occupy_the_slot()
    {
        var (driver, schema) = await MigratedAsync();
        var key = Key();

        var job = (await driver.Executor().JobInsertManyAsync([Unique(key)], Ct))[0].Job;

        await using var connection = await db.OpenConnectionAsync(Ct);
        await using var cmd = new SqlCommand($"SELECT unique_key, unique_states, unique_slot FROM [{schema}].kopi_job WHERE id = {job.Id}", connection);
        await using var reader = await cmd.ExecuteReaderAsync(Ct);
        Assert.True(await reader.ReadAsync(Ct));
        Assert.Equal(key, (byte[])reader[0]);
        Assert.Equal(DefaultStates, reader.GetByte(1));
        Assert.Equal(0L, reader.GetInt64(2));
    }

    private async Task<(SqlServerDriver Driver, string Schema)> MigratedAsync()
    {
        db.SkipIfUnavailable();
        var schema = await db.CreateSchemaAsync();
        var driver = new SqlServerDriver(db.ConnectionString, new SqlServerDriverOptions { Schema = schema });
        await driver.CreateMigrator().MigrateAsync(MigrationDirection.Up, cancellationToken: Ct);
        return (driver, schema);
    }

    private static byte[] Key() => RandomNumberGenerator.GetBytes(32);

    private static JobInsertParams Plain(int n) => new()
    {
        Kind = "k", ArgsJson = $$"""{"n":{{n}}}""", Queue = "default", State = JobState.Available, Priority = 1, MaxAttempts = 25,
    };

    private static JobInsertParams Unique(byte[] key, byte states = DefaultStates, int n = 0) => new()
    {
        Kind = "k", ArgsJson = $$"""{"n":{{n}}}""", Queue = "default", State = JobState.Available, Priority = 1, MaxAttempts = 25,
        UniqueKey = key, UniqueStates = states,
    };
}
