using KopiQueue.Drivers;
using KopiQueue.Migrations;
using KopiQueue.TestInfrastructure;
using Npgsql;

namespace KopiQueue.Npgsql.Tests;

/// <summary>The PostgreSQL driver and executor against a real PostgreSQL (plan task 1.4: design §8, FR-DB-03).</summary>
public sealed class NpgsqlDriverTests(PostgresFixture db) : IClassFixture<PostgresFixture>
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Fact]
    public void Capabilities_describe_PostgreSQL()
    {
        using var ds = NpgsqlDataSource.Create("Host=unused");
        IKopiDriver driver = new NpgsqlDriver(ds);

        Assert.True(driver.Capabilities.ListenNotify);
        Assert.True(driver.Capabilities.BulkCopy);
        Assert.Null(driver.Capabilities.MaxRowsPerStatement);
    }

    [Fact]
    public async Task Insert_applies_database_defaults_and_returns_the_stored_row()
    {
        db.SkipIfUnavailable();
        await using var ds = db.CreateDataSource();
        var driver = await MigratedDriverAsync(ds);
        var before = DateTimeOffset.UtcNow.AddMinutes(-1);

        var result = Assert.Single(await driver.GetExecutor().JobInsertManyAsync([Job("email")], Ct));

        var job = result.Job;
        Assert.False(result.UniqueSkippedAsDuplicate);
        Assert.True(job.Id > 0);
        Assert.Equal(("email", "default", JobState.Available, 2, 0, 25), (job.Kind, job.Queue, job.State, job.Priority, job.Attempt, job.MaxAttempts));
        Assert.Equal("{\"to\": \"a@example.com\"}", job.ArgsJson);
        Assert.Equal("{}", job.MetadataJson);
        Assert.Empty(job.Tags);
        Assert.Empty(job.AttemptedBy);
        Assert.Empty(job.Errors);
        Assert.Null(job.AttemptedAt);
        Assert.Null(job.FinalizedAt);
        Assert.True(job.CreatedAt > before);
        Assert.Equal(job.CreatedAt, job.ScheduledAt);
        Assert.Equal(TimeSpan.Zero, job.CreatedAt.Offset);
    }

    [Fact]
    public async Task Insert_many_keeps_the_input_order_and_every_field()
    {
        db.SkipIfUnavailable();
        await using var ds = db.CreateDataSource();
        var driver = await MigratedDriverAsync(ds);
        var later = new DateTimeOffset(2030, 1, 2, 3, 4, 5, TimeSpan.FromHours(7));
        JobInsertParams[] jobs =
        [
            Job("a"),
            new()
            {
                Kind = "b", ArgsJson = "[1, 2]", Queue = "reports", State = JobState.Scheduled, Priority = 4, MaxAttempts = 3,
                ScheduledAt = later, MetadataJson = "{\"k\": \"v\"}", Tags = ["nightly", "batch-1"],
            },
            Job("c"),
        ];

        var results = await driver.GetExecutor().JobInsertManyAsync(jobs, Ct);

        Assert.Equal(["a", "b", "c"], results.Select(r => r.Job.Kind));
        Assert.True(results[0].Job.Id < results[1].Job.Id && results[1].Job.Id < results[2].Job.Id);
        var b = results[1].Job;
        Assert.Equal(("reports", JobState.Scheduled, 4, 3), (b.Queue, b.State, b.Priority, b.MaxAttempts));
        Assert.Equal(later, b.ScheduledAt);
        Assert.Equal("[1, 2]", b.ArgsJson);
        Assert.Equal("{\"k\": \"v\"}", b.MetadataJson);
        Assert.Equal(["nightly", "batch-1"], b.Tags);
    }

    [Fact]
    public async Task Insert_many_handles_a_large_batch_in_one_statement()
    {
        db.SkipIfUnavailable();
        await using var ds = db.CreateDataSource();
        var driver = await MigratedDriverAsync(ds);

        var results = await driver.GetExecutor().JobInsertManyAsync([.. Enumerable.Range(0, 5_000).Select(i => Job($"k{i}"))], Ct);

        Assert.Equal(Enumerable.Range(0, 5_000).Select(i => $"k{i}"), results.Select(r => r.Job.Kind));
        Assert.Equal(5_000, results.Select(r => r.Job.Id).Distinct().Count());
    }

    [Fact]
    public async Task Insert_of_nothing_does_not_touch_the_database()
    {
        await using var ds = NpgsqlDataSource.Create("Host=unused.invalid");
        IKopiDriver driver = new NpgsqlDriver(ds);

        Assert.Empty(await driver.GetExecutor().JobInsertManyAsync([], Ct));
    }

    [Fact]
    public async Task Inserts_in_a_caller_transaction_commit_and_roll_back_with_it()
    {
        db.SkipIfUnavailable();
        await using var ds = db.CreateDataSource();
        var driver = await MigratedDriverAsync(ds);
        await using var connection = await ds.OpenConnectionAsync(Ct);

        long rolledBack;
        await using (var tx = await connection.BeginTransactionAsync(Ct))
        {
            var executor = driver.GetExecutor(tx);
            rolledBack = (await executor.JobInsertManyAsync([Job("gone")], Ct))[0].Job.Id;
            Assert.NotNull(await executor.JobGetByIdAsync(rolledBack, Ct));     // visible inside the transaction
            Assert.Null(await driver.GetExecutor().JobGetByIdAsync(rolledBack, Ct)); // not outside it
            await tx.RollbackAsync(Ct);
        }

        long committed;
        await using (var tx = await connection.BeginTransactionAsync(Ct))
        {
            committed = (await driver.GetExecutor(tx).JobInsertManyAsync([Job("kept")], Ct))[0].Job.Id;
            await tx.CommitAsync(Ct);
        }

        Assert.Null(await driver.GetExecutor().JobGetByIdAsync(rolledBack, Ct));
        Assert.Equal("kept", (await driver.GetExecutor().JobGetByIdAsync(committed, Ct))?.Kind);
    }

    [Fact]
    public async Task A_completed_or_foreign_transaction_is_rejected()
    {
        db.SkipIfUnavailable();
        await using var ds = db.CreateDataSource();
        IKopiDriver driver = new NpgsqlDriver(ds);
        await using var connection = await ds.OpenConnectionAsync(Ct);
        var tx = await connection.BeginTransactionAsync(Ct);
        await tx.CommitAsync(Ct);

        await Assert.ThrowsAsync<InvalidOperationException>(() => driver.GetExecutor(tx).JobInsertManyAsync([Job("late")], Ct));
        await Assert.ThrowsAsync<InvalidOperationException>(() => driver.GetExecutor(tx).JobGetByIdAsync(1, Ct));
        Assert.Throws<ArgumentException>(() => driver.GetExecutor(new ForeignTransaction()));
        Assert.Throws<ArgumentNullException>(() => driver.GetExecutor(null!));
    }

    [Fact]
    public async Task Get_by_id_returns_null_for_a_missing_job()
    {
        db.SkipIfUnavailable();
        await using var ds = db.CreateDataSource();
        var driver = await MigratedDriverAsync(ds);

        Assert.Null(await driver.GetExecutor().JobGetByIdAsync(long.MaxValue, Ct));
    }

    [Fact]
    public async Task Get_by_id_reads_attempts_and_errors()
    {
        db.SkipIfUnavailable();
        await using var ds = db.CreateDataSource();
        var (driver, schema) = await MigratedDriverWithSchemaAsync(ds);
        var id = (await driver.GetExecutor().JobInsertManyAsync([Job("failing")], Ct))[0].Job.Id;
        await using (var cmd = ds.CreateCommand($$"""
            UPDATE {{schema}}.river_job SET state = 'retryable', attempt = 1, attempted_at = '2026-01-01T00:00:00Z',
                attempted_by = ARRAY['host-1'],
                errors = ARRAY['{"at": "2026-01-01T00:00:01.5Z", "attempt": 1, "error": "boom", "trace": "at X"}']::jsonb[]
            WHERE id = {{id}}
            """))
        {
            await cmd.ExecuteNonQueryAsync(Ct);
        }

        var job = await driver.GetExecutor().JobGetByIdAsync(id, Ct);

        Assert.NotNull(job);
        Assert.Equal((JobState.Retryable, 1), (job.State, job.Attempt));
        Assert.Equal(new DateTimeOffset(2026, 1, 1, 0, 0, 0, TimeSpan.Zero), job.AttemptedAt);
        Assert.Equal(["host-1"], job.AttemptedBy);
        Assert.Equal([new AttemptError(new DateTimeOffset(2026, 1, 1, 0, 0, 1, 500, TimeSpan.Zero), 1, "boom", "at X")], job.Errors);
    }

    [Fact]
    public async Task Every_job_state_round_trips()
    {
        db.SkipIfUnavailable();
        await using var ds = db.CreateDataSource();
        var driver = await MigratedDriverAsync(ds);
        var states = Enum.GetValues<JobState>();
        var finals = new[] { JobState.Cancelled, JobState.Completed, JobState.Discarded };
        var executor = driver.GetExecutor();

        var results = await executor.JobInsertManyAsync(
            [.. states.Where(s => !finals.Contains(s)).Select(s => new JobInsertParams
            {
                Kind = "k", ArgsJson = "{}", Queue = "default", State = s, Priority = 1, MaxAttempts = 1,
            })],
            Ct);

        Assert.Equal(states.Where(s => !finals.Contains(s)), results.Select(r => r.Job.State));
    }

    [Fact]
    public async Task Bad_unique_fields_unique_fast_inserts_and_comma_tags_are_rejected_before_reaching_the_database()
    {
        await using var ds = NpgsqlDataSource.Create("Host=unused.invalid");
        var executor = ((IKopiDriver)new NpgsqlDriver(ds)).GetExecutor();
        var unique = new JobInsertParams
        {
            Kind = "k", ArgsJson = "{}", Queue = "q", State = JobState.Available, Priority = 1, MaxAttempts = 1, UniqueKey = new byte[32], UniqueStates = 0b1111_0101,
        };

        await Assert.ThrowsAsync<ArgumentException>(() => executor.JobInsertManyAsync(
            [new JobInsertParams { Kind = "k", ArgsJson = "{}", Queue = "q", State = JobState.Available, Priority = 1, MaxAttempts = 1, UniqueKey = [1] }], Ct));
        await Assert.ThrowsAsync<NotSupportedException>(() => executor.JobInsertFastAsync([unique], Ct));
        await Assert.ThrowsAsync<ArgumentException>(() => executor.JobInsertManyAsync(
            [new JobInsertParams { Kind = "k", ArgsJson = "{}", Queue = "q", State = JobState.Available, Priority = 1, MaxAttempts = 1, Tags = ["a,b"] }], Ct));
    }

    [Fact]
    public async Task A_custom_prefix_and_schema_are_used_by_every_statement()
    {
        db.SkipIfUnavailable();
        await using var ds = db.CreateDataSource();
        var schema = await CreateSchemaAsync(ds);
        var kopi = new NpgsqlDriver(ds, new NpgsqlDriverOptions { Schema = schema, TablePrefix = "kopi_" });
        await kopi.CreateMigrator().MigrateAsync(MigrationDirection.Up, cancellationToken: Ct);
        IKopiDriver driver = kopi;

        var id = (await driver.GetExecutor().JobInsertManyAsync([Job("prefixed")], Ct))[0].Job.Id;

        Assert.Equal("prefixed", (await driver.GetExecutor().JobGetByIdAsync(id, Ct))?.Kind);
        await using var cmd = ds.CreateCommand($"SELECT count(*) FROM {schema}.kopi_job");
        Assert.Equal(1L, await cmd.ExecuteScalarAsync(Ct));
        Assert.Equal((schema, "kopi_"), (kopi.Schema, kopi.TablePrefix));
    }

    [Fact]
    public async Task The_database_rejects_values_outside_its_constraints()
    {
        db.SkipIfUnavailable();
        await using var ds = db.CreateDataSource();
        var driver = await MigratedDriverAsync(ds);

        var ex = await Assert.ThrowsAsync<PostgresException>(() => driver.GetExecutor().JobInsertManyAsync(
            [new JobInsertParams { Kind = "k", ArgsJson = "{}", Queue = "q", State = JobState.Available, Priority = 9, MaxAttempts = 1 }], Ct));

        Assert.Equal(("23514", "priority_in_range"), (ex.SqlState, ex.ConstraintName));
    }

    [Fact]
    public async Task Resolved_insert_options_are_stored_as_given()
    {
        db.SkipIfUnavailable();
        await using var ds = db.CreateDataSource();
        var driver = await MigratedDriverAsync(ds);
        var now = DateTimeOffset.UtcNow;
        var later = now.AddHours(1);
        later = later.AddTicks(-(later.Ticks % 10)); // PostgreSQL stores microseconds

        var results = await driver.GetExecutor().JobInsertManyAsync(
            [
                JobValidation.ToInsertParams("plain", "{}", null, now),
                JobValidation.ToInsertParams(
                    "full",
                    "{\"n\": 1}",
                    new InsertOptions
                    {
                        Queue = "email", Priority = 3, MaxAttempts = 2, ScheduledAt = later, Tags = ["welcome"],
                        Metadata = new System.Text.Json.Nodes.JsonObject { ["source"] = "signup" },
                    },
                    now),
                JobValidation.ToInsertParams("longest", "{}", new InsertOptions { Queue = new string('q', JobValidation.MaxNameLength) }, now),
            ],
            Ct);

        var plain = results[0].Job;
        Assert.Equal(("default", JobState.Available, 1, 25), (plain.Queue, plain.State, plain.Priority, plain.MaxAttempts));
        var full = results[1].Job;
        Assert.Equal(("email", JobState.Scheduled, 3, 2, later), (full.Queue, full.State, full.Priority, full.MaxAttempts, full.ScheduledAt));
        Assert.Equal(["welcome"], full.Tags);
        Assert.Equal("{\"source\": \"signup\"}", full.MetadataJson);
        Assert.Equal(127, results[2].Job.Queue.Length);
    }

    [Fact]
    public async Task The_fast_path_stores_the_same_rows_as_the_batch_insert()
    {
        db.SkipIfUnavailable();
        await using var ds = db.CreateDataSource();
        var driver = await MigratedDriverAsync(ds);
        var later = new DateTimeOffset(2030, 1, 2, 3, 4, 5, 678, TimeSpan.FromHours(7));
        JobInsertParams[] jobs =
        [
            Job("a"),
            new()
            {
                Kind = "b", ArgsJson = "{\"z\": [1, 2], \"a\": null}", Queue = "reports", State = JobState.Scheduled, Priority = 4,
                MaxAttempts = 3, ScheduledAt = later, MetadataJson = "{\"k\": \"v\"}", Tags = ["nightly", "batch-1"],
            },
            new() { Kind = "c", ArgsJson = "{}", Queue = "q", State = JobState.Pending, Priority = 1, MaxAttempts = 1 },
        ];

        var batch = (await driver.GetExecutor().JobInsertManyAsync(jobs, Ct)).Select(r => r.Job).ToList();
        Assert.Equal(3, await driver.GetExecutor().JobInsertFastAsync(jobs, Ct));
        var fast = new List<JobRow>();
        for (var id = batch[^1].Id + 1; id <= batch[^1].Id + 3; id++)
        {
            fast.Add((await driver.GetExecutor().JobGetByIdAsync(id, Ct))!);
        }

        static object Shape(JobRow j) => (j.Kind, j.Queue, j.State, j.Priority, j.MaxAttempts, j.ArgsJson, j.MetadataJson, string.Join('|', j.Tags), j.Attempt, j.AttemptedAt, j.FinalizedAt);
        Assert.Equal(batch.Select(Shape), fast.Select(Shape));
        Assert.Equal(later, fast[1].ScheduledAt);
        Assert.Equal(fast[0].CreatedAt, fast[0].ScheduledAt); // the transaction's now(), as the batch insert's default gives
        Assert.Equal(fast[0].CreatedAt, fast[2].CreatedAt);
    }

    [Fact]
    public async Task The_fast_path_runs_in_a_caller_transaction_and_with_a_custom_prefix()
    {
        db.SkipIfUnavailable();
        await using var ds = db.CreateDataSource();
        var schema = await CreateSchemaAsync(ds);
        var kopi = new NpgsqlDriver(ds, new NpgsqlDriverOptions { Schema = schema, TablePrefix = "kopi_" });
        await kopi.CreateMigrator().MigrateAsync(MigrationDirection.Up, cancellationToken: Ct);
        IKopiDriver driver = kopi;
        await using var connection = await ds.OpenConnectionAsync(Ct);

        await using (var tx = await connection.BeginTransactionAsync(Ct))
        {
            Assert.Equal(2, await driver.GetExecutor(tx).JobInsertFastAsync([Job("x"), Job("y")], Ct));
            await tx.CommitAsync(Ct);
        }

        await using var cmd = ds.CreateCommand($"SELECT string_agg(kind, ',' ORDER BY id) FROM {schema}.kopi_job");
        Assert.Equal("x,y", await cmd.ExecuteScalarAsync(Ct));
    }

    [Fact]
    public async Task The_fast_path_rejects_unsupported_jobs_before_reaching_the_database()
    {
        await using var ds = NpgsqlDataSource.Create("Host=unused.invalid");
        var executor = ((IKopiDriver)new NpgsqlDriver(ds)).GetExecutor();

        Assert.Equal(0, await executor.JobInsertFastAsync([], Ct));
        await Assert.ThrowsAsync<NotSupportedException>(() => executor.JobInsertFastAsync(
            [new JobInsertParams { Kind = "k", ArgsJson = "{}", Queue = "q", State = JobState.Available, Priority = 1, MaxAttempts = 1, UniqueKey = [1] }], Ct));
        await Assert.ThrowsAsync<ArgumentException>(() => executor.JobInsertFastAsync([null!], Ct));
    }

    private static JobInsertParams Job(string kind) => new()
    {
        Kind = kind,
        ArgsJson = "{\"to\":\"a@example.com\"}",
        Queue = "default",
        State = JobState.Available,
        Priority = 2,
        MaxAttempts = 25,
    };

    [Theory]
    [InlineData(0, 3, new[] { "me" })]
    [InlineData(2, 3, new[] { "h1", "h2", "me" })]
    [InlineData(3, 3, new[] { "h2", "h3", "me" })]
    [InlineData(5, 3, new[] { "h4", "h5", "me" })]
    [InlineData(5, 1, new[] { "me" })]
    public async Task Fetch_appends_the_client_and_keeps_the_newest_attempted_by_entries(int previous, int keep, string[] expected)
    {
        db.SkipIfUnavailable();
        await using var ds = db.CreateDataSource();
        var (driver, schema) = await MigratedDriverWithSchemaAsync(ds);
        var id = (await driver.GetExecutor().JobInsertManyAsync([Job("retried")], Ct))[0].Job.Id;
        var hosts = string.Join(", ", Enumerable.Range(1, previous).Select(i => $"'h{i}'"));
        await using (var cmd = ds.CreateCommand(
            $"UPDATE {schema}.river_job SET attempt = {previous}, attempted_by = {(previous == 0 ? "NULL" : $"ARRAY[{hosts}]")} WHERE id = {id}"))
        {
            await cmd.ExecuteNonQueryAsync(Ct);
        }

        var job = Assert.Single(await driver.GetExecutor().JobGetAvailableAsync(
            new JobGetAvailableParams { Queue = "default", Max = 1, ClientId = "me", MaxAttemptedBy = keep }, Ct));

        Assert.Equal(previous + 1, job.Attempt);
        Assert.Equal(expected, job.AttemptedBy);
    }

    [Fact]
    public async Task Fetch_skips_a_job_another_transaction_has_locked()
    {
        db.SkipIfUnavailable();
        await using var ds = db.CreateDataSource();
        var (driver, schema) = await MigratedDriverWithSchemaAsync(ds);
        var ids = (await driver.GetExecutor().JobInsertManyAsync([Job("a"), Job("b")], Ct)).Select(r => r.Job.Id).ToArray();
        await using var other = await ds.OpenConnectionAsync(Ct);
        await using var lockTx = await other.BeginTransactionAsync(Ct);
        await using (var cmd = new NpgsqlCommand($"SELECT id FROM {schema}.river_job WHERE id = {ids[0]} FOR UPDATE", other, lockTx))
        {
            await cmd.ExecuteScalarAsync(Ct);
        }

        var fetched = await driver.GetExecutor().JobGetAvailableAsync(new JobGetAvailableParams { Queue = "default", Max = 10, ClientId = "c" }, Ct);
        await lockTx.RollbackAsync(Ct);
        var after = await driver.GetExecutor().JobGetAvailableAsync(new JobGetAvailableParams { Queue = "default", Max = 10, ClientId = "c" }, Ct);

        Assert.Equal([ids[1]], fetched.Select(j => j.Id));
        Assert.Equal([ids[0]], after.Select(j => j.Id));
    }

    [Fact]
    public void Errors_are_written_as_River_writes_them()
    {
        var at = new DateTimeOffset(2026, 1, 1, 9, 0, 1, 500, TimeSpan.FromHours(9));

        Assert.Equal(
            """{"at":"2026-01-01T00:00:01.5Z","attempt":3,"error":"boom","trace":""}""",
            NpgsqlExecutor.FormatError(new AttemptError(at, 3, "boom", null)));
        Assert.Equal(
            """{"at":"2026-01-01T00:00:01Z","attempt":1,"error":"x","trace":"at Y"}""",
            NpgsqlExecutor.FormatError(new AttemptError(at.AddMilliseconds(-500), 1, "x", "at Y")));
        Assert.Null(NpgsqlExecutor.ParseError("""{"at":"2026-01-01T00:00:01Z","attempt":1,"error":"x","trace":""}""").Trace);
    }

    private static async Task<IKopiDriver> MigratedDriverAsync(NpgsqlDataSource ds) => (await MigratedDriverWithSchemaAsync(ds)).Driver;

    private static async Task<(IKopiDriver Driver, string Schema)> MigratedDriverWithSchemaAsync(NpgsqlDataSource ds)
    {
        var schema = await CreateSchemaAsync(ds);
        var driver = new NpgsqlDriver(ds, new NpgsqlDriverOptions { Schema = schema });
        await driver.CreateMigrator().MigrateAsync(MigrationDirection.Up, cancellationToken: Ct);
        return (driver, schema);
    }

    private static async Task<string> CreateSchemaAsync(NpgsqlDataSource ds)
    {
        var name = "d_" + Guid.NewGuid().ToString("N")[..12];
        await using var cmd = ds.CreateCommand($"CREATE SCHEMA {name}");
        await cmd.ExecuteNonQueryAsync(Ct);
        return name;
    }

    private sealed class ForeignTransaction : System.Data.Common.DbTransaction
    {
        public override System.Data.IsolationLevel IsolationLevel => System.Data.IsolationLevel.ReadCommitted;

        protected override System.Data.Common.DbConnection? DbConnection => null;

        public override void Commit()
        {
        }

        public override void Rollback()
        {
        }
    }
}
