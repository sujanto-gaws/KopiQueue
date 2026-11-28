using System.Data;
using KopiQueue.Drivers;
using KopiQueue.Migrations;
using KopiQueue.SqlServer.Tests.Migrations;
using KopiQueue.TestInfrastructure;
using Microsoft.Data.SqlClient;

namespace KopiQueue.SqlServer.Tests;

/// <summary>
/// SNAPSHOT isolation wherever application code can leave it (plan task 5.8: NFR-REL-06; spike 0.9, design §8.1 rule 1).
/// SNAPSHOT survives SqlClient's pool reset, so a pooled session can carry it into the driver's next statement, and a
/// caller's transaction can be a SNAPSHOT transaction. <c>READPAST</c> fails under SNAPSHOT (error 650), and concurrent
/// writes under SNAPSHOT fail with update conflicts (error 3960): every statement must work all the same.
/// </summary>
[Collection(nameof(PooledSessionTests))]
public sealed class SqlServerSnapshotSessionTests(SqlServerFixture db) : IClassFixture<SqlServerFixture>
{
    private const int SnapshotLevel = 5; // sys.dm_exec_sessions.transaction_isolation_level

    private static readonly DateTimeOffset Past = DateTimeOffset.UtcNow.AddHours(-1);

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Fact]
    public async Task Every_operation_works_on_a_pooled_session_left_in_SNAPSHOT()
    {
        // One pooled session, left in SNAPSHOT again before each call: the driver's own transactions set READ COMMITTED on
        // the session they run on, so without that, later calls would no longer start from SNAPSHOT.
        var (driver, schema) = await MigratedAsync(maxPoolSize: 1);
        var old = await OldCompletedJobAsync(driver, schema);
        var executor = SnapshotFirst.Wrap(driver.Executor(), driver.Connections.ConnectionString);

        await ExerciseEveryOperationAsync(executor, old);

        Assert.Equal(
            typeof(IKopiExecutor).GetMethods().Select(m => m.Name).Distinct().Order(StringComparer.Ordinal),
            SnapshotFirst.Called(executor).Order(StringComparer.Ordinal));
    }

    [Fact]
    public async Task Every_operation_works_in_a_caller_SNAPSHOT_transaction()
    {
        var (driver, schema) = await MigratedAsync(maxPoolSize: 10);
        var old = await OldCompletedJobAsync(driver, schema);
        await using var connection = await db.OpenConnectionAsync(Ct);
        await using var tx = (SqlTransaction)await connection.BeginTransactionAsync(IsolationLevel.Snapshot, Ct);

        await ExerciseEveryOperationAsync(((IKopiDriver)driver).GetExecutor(tx), old);
        await tx.CommitAsync(Ct);
    }

    [Fact]
    public async Task Concurrent_writers_on_pooled_SNAPSHOT_sessions_neither_conflict_nor_double_a_job()
    {
        const int clients = 8;
        var (driver, _) = await MigratedAsync(maxPoolSize: clients);
        await LeaveSnapshotAsync(driver, clients);
        var executor = driver.Executor();

        for (var round = 0; round < 5; round++)
        {
            var won = await Task.WhenAll(Enumerable.Range(0, clients).Select(i => Task.Run(() =>
                executor.LeaderAttemptElectAsync(new LeaderElectParams { LeaderId = $"c{round}-{i}", Ttl = TimeSpan.FromSeconds(30) }, Ct))));
            var leader = Assert.Single(won.Select((w, i) => (w, i)), r => r.w).i;
            await Task.WhenAll(Enumerable.Range(0, clients).Select(i => Task.Run(() =>
                executor.LeaderAttemptReelectAsync(new LeaderElectParams { LeaderId = $"c{round}-{i}", Ttl = TimeSpan.FromSeconds(30) }, Ct))));
            Assert.True(await executor.LeaderResignAsync($"c{round}-{leader}", Ct));
        }

        string[] queues = [.. Enumerable.Range(0, 10).Select(i => $"q{i}")];
        await Task.WhenAll(Enumerable.Range(0, clients).Select(_ => Task.Run(() => executor.QueueCreateOrSetUpdatedAtManyAsync(queues, Ct))));
        await Task.WhenAll(Enumerable.Range(0, clients).Select(_ => Task.Run(() => executor.QueuePauseAsync("q1", Ct))));

        var keys = Enumerable.Range(0, 20).Select(i => Key((byte)i)).ToArray();
        var unique = await Task.WhenAll(Enumerable.Range(0, clients).Select(c => Task.Run(() =>
            executor.JobInsertManyAsync([.. keys.Select((k, i) => Job(JobState.Available, key: keys[(i + c) % keys.Length]))], Ct))));
        Assert.Equal(keys.Length, unique.SelectMany(r => r).Where(r => !r.UniqueSkippedAsDuplicate).Select(r => r.Job.Id).Distinct().Count());

        await executor.JobInsertManyAsync([.. Enumerable.Range(0, 400).Select(_ => Job(JobState.Available))], Ct);
        var fetched = await Task.WhenAll(Enumerable.Range(0, clients).Select(c => Task.Run(async () =>
        {
            var mine = new List<long>();
            while (true)
            {
                var batch = await executor.JobGetAvailableAsync(new JobGetAvailableParams { Queue = "default", Max = 25, ClientId = $"c{c}" }, Ct);
                if (batch.Count == 0)
                {
                    return mine;
                }

                mine.AddRange(batch.Select(j => j.Id));
                await executor.JobSetStateIfRunningManyAsync(
                    [.. batch.Select(j => new JobSetStateParams { Id = j.Id, State = JobState.Completed, FinalizedAt = DateTimeOffset.UtcNow })], Ct);
            }
        })));
        var all = fetched.SelectMany(f => f).ToList();
        Assert.Equal(all.Count, all.Distinct().Count());
        Assert.Equal(420, all.Count);
    }

    /// <summary>Calls every executor operation once, checking the results that show the statement did its work.</summary>
    /// <param name="old">A job completed two days ago, for the cleaner.</param>
    private static async Task ExerciseEveryOperationAsync(IKopiExecutor executor, long old)
    {
        Assert.True(await executor.LeaderAttemptElectAsync(new LeaderElectParams { LeaderId = "me", Ttl = TimeSpan.FromSeconds(30) }, Ct));
        Assert.True(await executor.LeaderAttemptReelectAsync(new LeaderElectParams { LeaderId = "me", Ttl = TimeSpan.FromSeconds(30) }, Ct));

        Assert.Equal(["a", "b"], (await executor.QueueCreateOrSetUpdatedAtManyAsync(["b", "a"], Ct)).Select(q => q.Name));
        Assert.True(await executor.QueuePauseAsync("a", Ct));
        Assert.True(await executor.QueueResumeAsync("a", Ct));
        Assert.NotNull(await executor.QueueUpdateAsync(new QueueUpdateParams { Name = "b", MetadataJson = """{"k":1}""" }, Ct));
        Assert.NotNull(await executor.QueueGetAsync("b", Ct));
        Assert.Equal(2, (await executor.QueueListAsync(new QueueListQuery { Max = 10 }, Ct)).Count);
        Assert.Empty(await executor.QueueDeleteExpiredAsync(new QueueDeleteExpiredParams { Retention = TimeSpan.FromDays(1), Max = 10 }, Ct));

        var inserted = await executor.JobInsertManyAsync(
            [Job(JobState.Available), Job(JobState.Available), Job(JobState.Available), Job(JobState.Scheduled), Job(JobState.Available, key: Key(1))], Ct);
        Assert.True((await executor.JobInsertManyAsync([Job(JobState.Available, key: Key(1))], Ct))[0].UniqueSkippedAsDuplicate);
        Assert.Equal(2, await executor.JobInsertFastAsync([Job(JobState.Available, queue: "other"), Job(JobState.Available, queue: "other")], Ct));
        Assert.NotNull(await executor.JobGetByIdAsync(inserted[0].Job.Id, Ct));

        Assert.Single(await executor.JobScheduleAsync(new JobScheduleParams { Max = 100 }, Ct));
        var running = await executor.JobGetAvailableAsync(new JobGetAvailableParams { Queue = "default", Max = 3, ClientId = "me" }, Ct);
        Assert.Equal(3, running.Count);

        Assert.Equal(JobState.Running, (await executor.JobCancelAsync(running[0].Id, Ct))!.State);
        Assert.Equal([running[0].Id], await executor.JobListCancelAttemptedAsync([.. running.Select(j => j.Id).Order()], Ct));
        Assert.Equal(2, (await executor.JobSetStateIfRunningManyAsync(
            [
                new JobSetStateParams { Id = running[0].Id, State = JobState.Cancelled, FinalizedAt = DateTimeOffset.UtcNow },
                new JobSetStateParams { Id = running[1].Id, State = JobState.Discarded, FinalizedAt = DateTimeOffset.UtcNow },
            ], Ct)).Count);

        await Task.Delay(20, Ct);
        var stuck = await executor.JobListStuckAsync(new JobListStuckParams { StuckAfter = TimeSpan.FromMilliseconds(1), Max = 10 }, Ct);
        var rescue = Assert.Single(stuck.Jobs);
        Assert.Single(await executor.JobRescueManyAsync(
            [
                new JobRescueParams
                {
                    Id = rescue.Id, AttemptedAt = rescue.AttemptedAt!.Value, State = JobState.Retryable, ScheduledAt = stuck.Now,
                    Error = new AttemptError(stuck.Now, rescue.Attempt, "stuck", null),
                },
            ], Ct));

        Assert.Equal(JobState.Available, (await executor.JobRetryAsync(running[1].Id, Ct))!.State);
        Assert.True((await executor.JobDeleteAsync(running[1].Id, Ct))!.Deleted);
        Assert.NotEmpty(await executor.JobListAsync(new JobListQuery { Filter = new JobListParams { Kinds = ["k"] }, Max = 100 }, Ct));
        Assert.Equal(2, (await executor.JobDeleteManyAsync(new JobDeleteManyQuery { Filter = new JobDeleteManyParams { Queues = ["other"] }, Max = 10 }, Ct)).Count);
        Assert.Equal([old], await executor.JobDeleteFinalizedAsync(new JobDeleteFinalizedParams { CompletedRetention = TimeSpan.FromDays(1), Max = 10 }, Ct));

        Assert.True(await executor.LeaderResignAsync("me", Ct));
    }

    /// <summary>A job completed two days ago, made on a session of the test's own pool.</summary>
    private async Task<long> OldCompletedJobAsync(SqlServerDriver driver, string schema)
    {
        var id = (await driver.Executor().JobInsertManyAsync([Job(JobState.Available, queue: "old")], Ct))[0].Job.Id;
        await db.ExecuteAsync(
            $"UPDATE [{schema}].kopi_job SET state = 'completed', finalized_at = DATEADD(day, -2, SYSUTCDATETIME()) WHERE id = {id}");
        return id;
    }

    private async Task<(SqlServerDriver Driver, string Schema)> MigratedAsync(int maxPoolSize)
    {
        db.SkipIfUnavailable();
        await db.ExecuteAsync("ALTER DATABASE CURRENT SET ALLOW_SNAPSHOT_ISOLATION ON");
        var schema = await db.CreateSchemaAsync();
        var cs = new SqlConnectionStringBuilder(db.ConnectionString) { MaxPoolSize = maxPoolSize, ApplicationName = "KopiQueue-" + schema }.ConnectionString;
        var driver = new SqlServerDriver(cs, new SqlServerDriverOptions { Schema = schema });
        await driver.CreateMigrator().MigrateAsync(MigrationDirection.Up, cancellationToken: Ct);
        return (driver, schema);
    }

    /// <summary>Opens <paramref name="sessions"/> of the driver's pooled sessions at once and leaves each in SNAPSHOT.</summary>
    private static async Task LeaveSnapshotAsync(SqlServerDriver driver, int sessions)
    {
        var connections = new List<SqlConnection>();
        try
        {
            for (var i = 0; i < sessions; i++)
            {
                var connection = new SqlConnection(driver.Connections.ConnectionString);
                connections.Add(connection);
                await connection.OpenAsync(Ct);
                await using var cmd = new SqlCommand("SET TRANSACTION ISOLATION LEVEL SNAPSHOT", connection);
                await cmd.ExecuteNonQueryAsync(Ct);
            }
        }
        finally
        {
            foreach (var connection in connections)
            {
                await connection.DisposeAsync();
            }
        }

        Assert.All(await PooledIsolationLevelsAsync(driver, sessions), level => Assert.Equal(SnapshotLevel, level));
    }

    /// <summary>The isolation level of each of <paramref name="sessions"/> pooled sessions, opened at once.</summary>
    private static async Task<List<int>> PooledIsolationLevelsAsync(SqlServerDriver driver, int sessions)
    {
        var connections = new List<SqlConnection>();
        var levels = new List<int>();
        try
        {
            for (var i = 0; i < sessions; i++)
            {
                var connection = new SqlConnection(driver.Connections.ConnectionString);
                connections.Add(connection);
                await connection.OpenAsync(Ct);
                await using var cmd = new SqlCommand("SELECT transaction_isolation_level FROM sys.dm_exec_sessions WHERE session_id = @@SPID", connection);
                levels.Add(Convert.ToInt32(await cmd.ExecuteScalarAsync(Ct), System.Globalization.CultureInfo.InvariantCulture));
            }
        }
        finally
        {
            foreach (var connection in connections)
            {
                await connection.DisposeAsync();
            }
        }

        return levels;
    }

    /// <summary>An executor that leaves the one pooled session in SNAPSHOT, and checks it is, before each call.</summary>
#pragma warning disable CA1852 // DispatchProxy derives from it, so it cannot be sealed.
    private class SnapshotFirst : System.Reflection.DispatchProxy
#pragma warning restore CA1852
    {
        private IKopiExecutor _inner = null!;
        private string _connectionString = null!;
        private readonly HashSet<string> _called = [];

        public static IKopiExecutor Wrap(IKopiExecutor inner, string connectionString)
        {
            var proxy = Create<IKopiExecutor, SnapshotFirst>();
            var self = (SnapshotFirst)(object)proxy;
            (self._inner, self._connectionString) = (inner, connectionString);
            return proxy;
        }

        /// <summary>The operations called through <paramref name="proxy"/>.</summary>
        public static HashSet<string> Called(IKopiExecutor proxy) => ((SnapshotFirst)(object)proxy)._called;

        protected override object? Invoke(System.Reflection.MethodInfo? targetMethod, object?[]? args)
        {
            // Synchronous, so no await is needed inside DispatchProxy's synchronous Invoke.
            using (var connection = new SqlConnection(_connectionString))
            {
                connection.Open();
                using var cmd = new SqlCommand(
                    "SET TRANSACTION ISOLATION LEVEL SNAPSHOT; SELECT transaction_isolation_level FROM sys.dm_exec_sessions WHERE session_id = @@SPID",
                    connection);
                Assert.Equal(SnapshotLevel, Convert.ToInt32(cmd.ExecuteScalar(), System.Globalization.CultureInfo.InvariantCulture));
            }

            _called.Add(targetMethod!.Name);
            try
            {
                return targetMethod!.Invoke(_inner, args);
            }
            catch (System.Reflection.TargetInvocationException e) when (e.InnerException is not null)
            {
                throw e.InnerException;
            }
        }
    }

    private static byte[] Key(byte b)
    {
        var key = new byte[32];
        key[0] = b;
        return key;
    }

    private static JobInsertParams Job(JobState state, byte[]? key = null, string queue = "default") => new()
    {
        Kind = "k",
        ArgsJson = "{}",
        Queue = queue,
        State = state,
        Priority = 1,
        MaxAttempts = 25,
        ScheduledAt = Past,
        UniqueKey = key,
        UniqueStates = key is null ? null : (byte)0b1111_0101,
    };
}

/// <summary>
/// Tests that rely on what pooled sessions carry run alone: another class's fixture clears every pool in the process
/// when it drops its database, which would replace the sessions a test left in SNAPSHOT with fresh ones.
/// </summary>
[CollectionDefinition(nameof(PooledSessionTests), DisableParallelization = true)]
public sealed class PooledSessionTests;
