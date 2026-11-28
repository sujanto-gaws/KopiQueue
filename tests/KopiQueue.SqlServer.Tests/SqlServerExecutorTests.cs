using System.Data;
using KopiQueue.Drivers;
using KopiQueue.Migrations;
using KopiQueue.SqlServer.Tests.Migrations;
using KopiQueue.TestInfrastructure;
using Microsoft.Data.SqlClient;

namespace KopiQueue.SqlServer.Tests;

/// <summary>
/// The executor's foundations against a real SQL Server (plan task 5.2: FR-DB-02, FR-INS-02): connections and caller
/// transactions, the session check, and the job and queue row readers with their UTC times and JSON arrays.
/// </summary>
public sealed class SqlServerExecutorTests(SqlServerFixture db) : IClassFixture<SqlServerFixture>
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Fact]
    public async Task A_job_reads_back_with_every_field_and_UTC_times()
    {
        var (driver, schema) = await MigratedAsync();
        var id = await InsertJobAsync(
            schema,
            """
            N'running', 2, 5, '2026-01-02T03:04:05.123456', '2026-01-01T00:00:00.000001', NULL, '2026-01-01T12:00:00', 3,
            N'{"n": 1}', N'["client-a","client-b"]',
            N'[{"at":"2026-01-02T03:00:00.5Z","attempt":1,"error":"boom","trace":"at X"},{"at":"2026-01-02T03:01:00Z","attempt":2,"error":"again","trace":""}]',
            N'email.send', N'{"k": "v"}', N'mail', N'["nightly","batch-1"]'
            """);

        var job = await driver.Executor().JobGetByIdAsync(id, Ct);

        Assert.NotNull(job);
        Assert.Equal((id, "email.send", "mail", JobState.Running, 3), (job.Id, job.Kind, job.Queue, job.State, job.Priority));
        Assert.Equal((2, 5), (job.Attempt, job.MaxAttempts));
        Assert.Equal("""{"n": 1}""", job.ArgsJson);
        Assert.Equal("""{"k": "v"}""", job.MetadataJson);
        Assert.Equal(["nightly", "batch-1"], job.Tags);
        Assert.Equal(["client-a", "client-b"], job.AttemptedBy);
        Assert.Equal(
            [
                new AttemptError(Utc(2026, 1, 2, 3, 0, 0).AddMilliseconds(500), 1, "boom", "at X"),
                new AttemptError(Utc(2026, 1, 2, 3, 1, 0), 2, "again", null),
            ],
            job.Errors);
        Assert.Equal(Utc(2026, 1, 2, 3, 4, 5).AddTicks(1_234_560), job.AttemptedAt);
        Assert.Equal(Utc(2026, 1, 1, 0, 0, 0).AddTicks(10), job.CreatedAt);
        Assert.Equal(Utc(2026, 1, 1, 12, 0, 0), job.ScheduledAt);
        Assert.Null(job.FinalizedAt);
        Assert.All([job.AttemptedAt!.Value, job.CreatedAt, job.ScheduledAt], t => Assert.Equal(TimeSpan.Zero, t.Offset));
    }

    [Fact]
    public async Task A_job_with_the_table_defaults_reads_back_with_empty_lists_and_the_database_clock_in_UTC()
    {
        var (driver, schema) = await MigratedAsync();
        var id = (long)(await db.ScalarAsync(
            $"INSERT INTO [{schema}].kopi_job (args, kind) OUTPUT inserted.id VALUES (N'{{}}', N'k')"))!;

        var job = await driver.Executor().JobGetByIdAsync(id, Ct);

        Assert.NotNull(job);
        Assert.Equal((JobState.Available, "default", 1, 0, 25), (job.State, job.Queue, job.Priority, job.Attempt, job.MaxAttempts));
        Assert.Empty(job.Tags);
        Assert.Empty(job.AttemptedBy);
        Assert.Empty(job.Errors);
        Assert.Null(job.AttemptedAt);
        // SYSUTCDATETIME read as UTC is close to now; read as local time it would be off by the machine's offset.
        Assert.InRange(job.CreatedAt, DateTimeOffset.UtcNow.AddMinutes(-2), DateTimeOffset.UtcNow.AddMinutes(2));
        Assert.Equal(job.CreatedAt, job.ScheduledAt);
    }

    [Fact]
    public async Task Every_state_reads_back()
    {
        var (driver, schema) = await MigratedAsync();
        var executor = driver.Executor();

        foreach (var state in Enum.GetValues<JobState>())
        {
            var finalized = state is JobState.Cancelled or JobState.Completed or JobState.Discarded ? "SYSUTCDATETIME()" : "NULL";
            var id = (long)(await db.ScalarAsync(
                $"INSERT INTO [{schema}].kopi_job (state, finalized_at, args, kind) OUTPUT inserted.id VALUES (@state, {finalized}, N'{{}}', N'k')",
                new SqlParameter("@state", StoredJob.ToDatabase(state))))!;

            var job = await executor.JobGetByIdAsync(id, Ct);

            Assert.Equal(state, job!.State);
            Assert.Equal(finalized != "NULL", job.FinalizedAt.HasValue);
        }
    }

    [Fact]
    public async Task A_queue_reads_back_with_UTC_times()
    {
        var (driver, schema) = await MigratedAsync();
        await db.ExecuteAsync(
            $$"""
            INSERT INTO [{{schema}}].kopi_queue (name, created_at, updated_at, paused_at, metadata)
            VALUES (N'mail', '2026-03-01T08:00:00', '2026-03-02T08:00:00.25', '2026-03-03T08:00:00.000002', N'{"owner":"ops"}');
            INSERT INTO [{{schema}}].kopi_queue (name) VALUES (N'Mail');
            """);
        var executor = driver.Executor();

        var queue = await executor.QueueGetAsync("mail", Ct);
        var other = await executor.QueueGetAsync("Mail", Ct);

        Assert.NotNull(queue);
        Assert.Equal(("mail", """{"owner":"ops"}"""), (queue.Name, queue.MetadataJson));
        Assert.Equal(Utc(2026, 3, 1, 8, 0, 0), queue.CreatedAt);
        Assert.Equal(Utc(2026, 3, 2, 8, 0, 0).AddMilliseconds(250), queue.UpdatedAt);
        Assert.Equal(Utc(2026, 3, 3, 8, 0, 0).AddTicks(20), queue.PausedAt);
        Assert.Equal(("Mail", null, "{}"), (other!.Name, other.PausedAt, other.MetadataJson));
    }

    [Fact]
    public async Task What_does_not_exist_reads_as_null()
    {
        var (driver, _) = await MigratedAsync();
        var executor = driver.Executor();

        Assert.Null(await executor.JobGetByIdAsync(long.MaxValue, Ct));
        Assert.Null(await executor.QueueGetAsync("missing", Ct));
        Assert.Null(await executor.QueueGetAsync("MAIL", Ct));
    }

    [Fact]
    public async Task A_caller_transaction_sees_its_own_uncommitted_rows_and_its_rollback()
    {
        var (driver, schema) = await MigratedAsync();
        await using var connection = await db.OpenConnectionAsync(Ct);
        long id;
        await using (var tx = (SqlTransaction)await connection.BeginTransactionAsync(Ct))
        {
            await using (var insert = new SqlCommand(
                $"INSERT INTO [{schema}].kopi_job (args, kind) OUTPUT inserted.id VALUES (N'{{}}', N'in_tx')", connection, tx))
            {
                id = (long)(await insert.ExecuteScalarAsync(Ct))!;
            }

            var inTransaction = await ((IKopiDriver)driver).GetExecutor(tx).JobGetByIdAsync(id, Ct);

            Assert.Equal("in_tx", inTransaction?.Kind);
            await tx.RollbackAsync(Ct);
        }

        Assert.Null(await driver.Executor().JobGetByIdAsync(id, Ct));
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task A_completed_caller_transaction_is_refused(bool commit)
    {
        var (driver, _) = await MigratedAsync();
        await using var connection = await db.OpenConnectionAsync(Ct);
        await using var tx = (SqlTransaction)await connection.BeginTransactionAsync(Ct);
        var executor = ((IKopiDriver)driver).GetExecutor(tx);
        Assert.Null(await executor.JobGetByIdAsync(1, Ct));

        await (commit ? tx.CommitAsync(Ct) : tx.RollbackAsync(Ct));

        var ex = await Assert.ThrowsAsync<InvalidOperationException>(() => executor.JobGetByIdAsync(1, Ct));
        Assert.Contains("has completed", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task A_caller_session_with_wrong_settings_fails_before_any_statement()
    {
        var (driver, _) = await MigratedAsync();
        // Not pooled, so the changed session is not handed to other tests.
        await using var connection = new SqlConnection(new SqlConnectionStringBuilder(db.ConnectionString) { Pooling = false }.ConnectionString);
        await connection.OpenAsync(Ct);
        await using (var set = new SqlCommand("SET ANSI_WARNINGS OFF", connection))
        {
            await set.ExecuteNonQueryAsync(Ct);
        }

        await using var tx = (SqlTransaction)await connection.BeginTransactionAsync(Ct);

        var ex = await Assert.ThrowsAsync<KopiQueueConfigurationException>(
            () => ((IKopiDriver)driver).GetExecutor(tx).JobGetByIdAsync(1, Ct));
        Assert.Contains("ANSI_WARNINGS is OFF", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task Pooled_sessions_are_checked_until_one_passes()
    {
        var (driver, _) = await MigratedAsync();
        var level = (byte)(await db.ScalarAsync("SELECT compatibility_level FROM sys.databases WHERE database_id = DB_ID()"))!;
        var executor = driver.Executor();
        await db.ExecuteAsync("ALTER DATABASE CURRENT SET COMPATIBILITY_LEVEL = 140");
        try
        {
            var ex = await Assert.ThrowsAsync<KopiQueueConfigurationException>(() => executor.JobGetByIdAsync(1, Ct));
            Assert.Contains("compatibility level is 140", ex.Message, StringComparison.Ordinal);
        }
        finally
        {
            await db.ExecuteAsync($"ALTER DATABASE CURRENT SET COMPATIBILITY_LEVEL = {level}");
        }

        Assert.Null(await executor.JobGetByIdAsync(1, Ct));

        // Once a pooled session has passed, the driver stops checking: reads still work at level 140 anyway.
        await db.ExecuteAsync("ALTER DATABASE CURRENT SET COMPATIBILITY_LEVEL = 140");
        try
        {
            Assert.Null(await driver.Executor().JobGetByIdAsync(1, Ct));
        }
        finally
        {
            await db.ExecuteAsync($"ALTER DATABASE CURRENT SET COMPATIBILITY_LEVEL = {level}");
        }
    }

    [Fact]
    public async Task A_pooled_session_gets_its_settings_reset_when_it_is_reused()
    {
        // The premise of SqlServerSessionGate: a session that passed the check once passes it again after any reuse.
        db.SkipIfUnavailable();
        var connections = new SqlServerConnectionFactory(new SqlConnectionStringBuilder(db.ConnectionString)
        {
            ApplicationName = "kopi-reset-" + Guid.NewGuid().ToString("N")[..8],
            MaxPoolSize = 1,
        }.ConnectionString);
        short spid;
        await using (var first = await connections.OpenAsync(Ct))
        {
            await using var set = new SqlCommand("SET ANSI_WARNINGS OFF; SET NUMERIC_ROUNDABORT ON; SELECT @@SPID", first);
            spid = (short)(await set.ExecuteScalarAsync(Ct))!;
        }

        await using var reused = await connections.OpenAsync(Ct);
        await using var read = new SqlCommand("SELECT @@SPID", reused);

        Assert.Equal(spid, (short)(await read.ExecuteScalarAsync(Ct))!);
        await SqlServerSessionCheck.EnsureAsync(reused, transaction: null, Ct);
    }

    [Fact]
    public async Task A_malformed_array_column_is_reported_by_name()
    {
        var (driver, schema) = await MigratedAsync();
        var id = (long)(await db.ScalarAsync(
            $"INSERT INTO [{schema}].kopi_job (args, kind, attempted_by) OUTPUT inserted.id VALUES (N'{{}}', N'k', N'{{\"not\": \"an array\"}}')"))!;

        var ex = await Assert.ThrowsAsync<InvalidOperationException>(() => driver.Executor().JobGetByIdAsync(id, Ct));

        Assert.Contains("attempted_by", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task Times_round_trip_through_a_parameter_in_UTC()
    {
        db.SkipIfUnavailable();
        var instant = new DateTimeOffset(2026, 6, 1, 9, 30, 0, TimeSpan.FromHours(7)).AddTicks(1_234_560);
        await using var connection = await db.OpenConnectionAsync(Ct);
        await using var cmd = new SqlCommand("SELECT CAST(@t AS datetime2(6)), DATEPART(hour, @t)", connection);
        cmd.Parameters.Add(SqlServerValues.Time("@t", instant));
        await using var reader = await cmd.ExecuteReaderAsync(Ct);
        Assert.True(await reader.ReadAsync(Ct));

        Assert.Equal(instant, SqlServerValues.FromDatabase(reader.GetDateTime(0)));
        Assert.Equal(2, reader.GetInt32(1)); // 09:30 at +07:00 is stored as 02:30 UTC
    }

    private async Task<(SqlServerDriver Driver, string Schema)> MigratedAsync()
    {
        db.SkipIfUnavailable();
        var schema = await db.CreateSchemaAsync();
        var driver = new SqlServerDriver(db.ConnectionString, new SqlServerDriverOptions { Schema = schema });
        await driver.CreateMigrator().MigrateAsync(MigrationDirection.Up, cancellationToken: Ct);
        return (driver, schema);
    }

    private async Task<long> InsertJobAsync(string schema, string values) => (long)(await db.ScalarAsync(
        $"""
        INSERT INTO [{schema}].kopi_job
            (state, attempt, max_attempts, attempted_at, created_at, finalized_at, scheduled_at, priority,
             args, attempted_by, errors, kind, metadata, queue, tags)
        OUTPUT inserted.id
        VALUES ({values})
        """))!;

    private static DateTimeOffset Utc(int year, int month, int day, int hour, int minute, int second) =>
        new(year, month, day, hour, minute, second, TimeSpan.Zero);
}

/// <summary>Shorthands for the explicit <see cref="IKopiDriver"/> members.</summary>
internal static class SqlServerDriverTestExtensions
{
    public static IKopiExecutor Executor(this SqlServerDriver driver) => ((IKopiDriver)driver).GetExecutor();
}
