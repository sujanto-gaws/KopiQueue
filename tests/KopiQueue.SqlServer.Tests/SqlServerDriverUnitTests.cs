using System.Data;
using System.Data.Common;
using System.Text.Json;
using System.Text.RegularExpressions;
using KopiQueue.Drivers;
using KopiQueue.Migrations;
using KopiQueue.SqlServer.Migrations;
using Microsoft.Data.SqlClient;

namespace KopiQueue.SqlServer.Tests;

/// <summary>Parts of the SQL Server driver that need no database (plan task 5.1).</summary>
public sealed partial class SqlServerDriverUnitTests
{
    [Fact]
    public void The_main_line_is_embedded_with_one_transactional_version()
    {
        var migration = Assert.Single(SqlServerMigrationDriver.MainLine);

        Assert.Equal(("main", 1, "initial schema"), (migration.Line, migration.Version, migration.Name));
        Assert.True(migration.Up.InTransaction);
        Assert.True(migration.Down.InTransaction);
    }

    [Fact]
    public void Every_name_the_main_line_creates_carries_the_prefix()
    {
        // Constraint names are scoped to the schema, so an unprefixed one would stop two prefixes sharing a schema.
        foreach (var migration in SqlServerMigrationDriver.MainLine)
        {
            var names = ObjectName().Matches(migration.Up.Sql).Select(m => m.Groups["name"].Value).ToList();

            Assert.NotEmpty(names);
            Assert.All(names, name => Assert.StartsWith("{{prefix}}", name, StringComparison.Ordinal));
        }
    }

    [Fact]
    public void The_main_line_renders_for_a_schema_and_prefix()
    {
        foreach (var migration in SqlServerMigrationDriver.MainLine)
        {
            foreach (var script in new[] { migration.Up, migration.Down })
            {
                // Comments may mention the default prefix; statements may not.
                var sql = string.Join('\n', MigrationTemplate.Render(script.Sql, "[queue].", "jobs_")
                    .Split('\n').Where(line => !line.TrimStart().StartsWith("--", StringComparison.Ordinal)));

                Assert.DoesNotContain("{{", sql, StringComparison.Ordinal);
                Assert.DoesNotContain("kopi_", sql, StringComparison.Ordinal);
                Assert.Contains("[queue].jobs_job", sql, StringComparison.Ordinal);
            }
        }
    }

    [Fact]
    public void Scripts_split_into_batches_at_GO_lines()
    {
        Assert.Equal(["SELECT 1;\n"], SqlServerMigrationDriver.SplitBatches("SELECT 1;"));
        Assert.Equal(
            ["SELECT 1;\n", "CREATE VIEW v AS SELECT 2 AS x;\n", "SELECT 3;\n"],
            SqlServerMigrationDriver.SplitBatches("SELECT 1;\r\nGO\r\nCREATE VIEW v AS SELECT 2 AS x;\n  go  \nSELECT 3;\nGO\n\nGO"));
        Assert.Empty(SqlServerMigrationDriver.SplitBatches("\nGO\n  \n"));

        // Only a line of its own is a separator.
        Assert.Single(SqlServerMigrationDriver.SplitBatches("SELECT 1 AS GO;\nSELECT 'GO';\nGOTO done;\ndone:"));
    }

    [Fact]
    public void The_migration_lock_is_keyed_by_schema_and_prefix()
    {
        var connections = new SqlServerConnectionFactory("Server=unused");

        Assert.Equal("kopi_migrate:dbo.kopi_", new SqlServerMigrationDriver(connections).LockResource);
        Assert.Equal("kopi_migrate:queue.jobs_", new SqlServerMigrationDriver(connections, "queue", "jobs_").LockResource);
        Assert.True(SqlServerMigrationDriver.LockResourceFor(new string('s', 63), new string('p', 32)).Length <= 255);
    }

    [Fact]
    public void The_migration_table_is_quoted_and_schema_qualified()
    {
        var driver = new SqlServerMigrationDriver(new SqlServerConnectionFactory("Server=unused"), "Queue_1", "kopi_");

        Assert.Equal("[Queue_1].", driver.SchemaQualifier);
        Assert.Equal("[Queue_1].[kopi_migration]", driver.MigrationTable);
        Assert.Equal("[a]]b]", SqlServerIdentifiers.Quote("a]b"));
    }

    [Theory]
    [InlineData("Server=db;Database=app", "KopiQueue")]
    [InlineData("Server=db;Database=app;Application Name=Billing", "Billing")]
    [InlineData("Server=db;Database=app;App=Billing", "Billing")]
    public void Connections_get_the_KopiQueue_application_name_by_default(string connectionString, string expected)
    {
        var factory = new SqlServerConnectionFactory(connectionString);

        Assert.Equal(expected, new SqlConnectionStringBuilder(factory.ConnectionString).ApplicationName);
    }

    [Fact]
    public void The_driver_rejects_unsafe_or_missing_options()
    {
        Assert.Throws<ArgumentException>(() => new SqlServerDriver("Server=unused", new SqlServerDriverOptions { Schema = "a]; DROP TABLE x; --" }));
        Assert.Throws<ArgumentException>(() => new SqlServerDriver("Server=unused", new SqlServerDriverOptions { Schema = null! }));
        Assert.Throws<ArgumentException>(() => new SqlServerDriver("Server=unused", new SqlServerDriverOptions { TablePrefix = "Bad" }));
        Assert.Throws<ArgumentException>(() => new SqlServerDriver("Server=unused", new SqlServerDriverOptions { TablePrefix = "" }));
        Assert.ThrowsAny<ArgumentException>(() => new SqlServerDriver(""));
        Assert.ThrowsAny<ArgumentException>(() => new SqlServerDriver(null!));
        Assert.ThrowsAny<ArgumentException>(() => new SqlServerDriver("Not a=connection;string=at all;Bogus Keyword=1"));
    }

    [Fact]
    public void The_driver_defaults_to_dbo_and_the_kopi_prefix()
    {
        var driver = new SqlServerDriver("Server=unused");
        var migrator = driver.CreateMigrator();

        Assert.Equal(("dbo", "kopi_"), (driver.Schema, driver.TablePrefix));
        Assert.Equal(("main", 1), (migrator.Line, migrator.LatestVersion));
    }

    [Fact]
    public void The_driver_is_poll_only_and_caps_rows_per_statement()
    {
        IKopiDriver driver = new SqlServerDriver("Server=unused");

        Assert.Equal((false, false, 1000), (driver.Capabilities.ListenNotify, driver.Capabilities.BulkCopy, driver.Capabilities.MaxRowsPerStatement));
        Assert.Null(driver.CreateListener());
    }

    [Fact]
    public void A_transaction_of_another_database_client_is_rejected()
    {
        IKopiDriver driver = new SqlServerDriver("Server=unused");

        var ex = Assert.Throws<ArgumentException>(() => driver.GetExecutor(new ForeignTransaction()));
        Assert.Contains(nameof(ForeignTransaction), ex.Message, StringComparison.Ordinal);
        Assert.Throws<ArgumentNullException>(() => driver.GetExecutor(null!));
    }

    [Fact]
    public async Task Operations_of_later_tasks_say_which_task_adds_them()
    {
        // Nothing is opened, so no server is needed.
        var executor = ((IKopiDriver)new SqlServerDriver("Server=unused")).GetExecutor();

        var ex = await Assert.ThrowsAsync<NotSupportedException>(
            () => executor.JobScheduleAsync(new JobScheduleParams { Max = 1 }, TestContext.Current.CancellationToken));

        Assert.Contains("JobScheduleAsync", ex.Message, StringComparison.Ordinal);
        Assert.Contains("plan task 5.6", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task Arguments_are_checked_before_anything_is_opened()
    {
        var executor = ((IKopiDriver)new SqlServerDriver("Server=unused")).GetExecutor();
        var ct = TestContext.Current.CancellationToken;
        JobInsertParams[] unique =
        [
            new() { Kind = "k", ArgsJson = "{}", Queue = "q", State = JobState.Available, Priority = 1, MaxAttempts = 1, UniqueKey = new byte[32], UniqueStates = 1 },
        ];

        var ex = await Assert.ThrowsAsync<NotSupportedException>(() => executor.JobInsertManyAsync(unique, ct));
        Assert.Contains("plan task 5.4", ex.Message, StringComparison.Ordinal);
        await Assert.ThrowsAsync<NotSupportedException>(() => executor.JobInsertFastAsync(unique, ct));
        await Assert.ThrowsAsync<ArgumentException>(() => executor.JobInsertManyAsync([null!], ct));
        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(
            () => executor.JobGetAvailableAsync(new JobGetAvailableParams { Queue = "q", Max = 1001, ClientId = "c" }, ct));
        await Assert.ThrowsAsync<ArgumentException>(() => executor.JobSetStateIfRunningManyAsync([new JobSetStateParams { Id = 1, State = JobState.Running }], ct));
        Assert.Empty(await executor.JobInsertManyAsync([], ct));
        Assert.Equal(0, await executor.JobInsertFastAsync([], ct));
        Assert.Empty(await executor.JobSetStateIfRunningManyAsync([], ct));
    }

    [Fact]
    public void Insert_batches_carry_json_as_strings_and_times_in_UTC()
    {
        JobInsertParams[] jobs =
        [
            new() { Kind = "skip", ArgsJson = "{}", Queue = "q", State = JobState.Available, Priority = 1, MaxAttempts = 1 },
            new()
            {
                Kind = "k", ArgsJson = """{"n": 1}""", Queue = "q", State = JobState.Scheduled, Priority = 2, MaxAttempts = 3,
                ScheduledAt = new DateTimeOffset(2026, 6, 1, 9, 30, 0, TimeSpan.FromHours(7)).AddTicks(1234567), MetadataJson = """{"m": true}""", Tags = ["a", "b"],
            },
            new() { Kind = "now", ArgsJson = "[]", Queue = "q", State = JobState.Available, Priority = 1, MaxAttempts = 1 },
        ];

        var json = SqlServerExecutor.InsertBatch(jobs, 1, 2);

        var rows = JsonElement.Parse(json).EnumerateArray().ToList();
        Assert.Equal(2, rows.Count);
        Assert.Equal(
            (1, """{"n": 1}""", "k", 3, """{"m": true}""", 2, "q", "2026-06-01T02:30:00.1234567", "scheduled", """["a","b"]"""),
            (rows[0].GetProperty("o").GetInt32(), rows[0].GetProperty("a").GetString(), rows[0].GetProperty("k").GetString(),
             rows[0].GetProperty("m").GetInt32(), rows[0].GetProperty("d").GetString(), rows[0].GetProperty("p").GetInt32(),
             rows[0].GetProperty("q").GetString(), rows[0].GetProperty("s").GetString(), rows[0].GetProperty("st").GetString(),
             rows[0].GetProperty("t").GetString()));
        Assert.Equal((2, "[]", "[]"), (rows[1].GetProperty("o").GetInt32(), rows[1].GetProperty("a").GetString(), rows[1].GetProperty("t").GetString()));
        Assert.False(rows[1].TryGetProperty("s", out _)); // the database's now
    }

    [Fact]
    public void Outcome_batches_leave_out_what_does_not_change()
    {
        var at = new DateTimeOffset(2026, 6, 1, 9, 30, 0, TimeSpan.FromHours(7));

        var json = SqlServerExecutor.OutcomeBatch(
            [
                new JobSetStateParams { Id = 7, State = JobState.Completed, FinalizedAt = at },
                new JobSetStateParams { Id = 8, State = JobState.Retryable, Attempt = 2, Error = new AttemptError(at, 2, "x", null), ScheduledAt = at, MetadataUpdates = "{}" },
            ],
            0,
            2);

        var rows = JsonElement.Parse(json).EnumerateArray().ToList();
        Assert.Equal(["i", "f", "st"], rows[0].EnumerateObject().Select(p => p.Name));
        Assert.Equal((7, "2026-06-01T02:30:00.0000000", "completed"), (rows[0].GetProperty("i").GetInt32(), rows[0].GetProperty("f").GetString(), rows[0].GetProperty("st").GetString()));
        Assert.Equal(["i", "at", "e", "d", "s", "st"], rows[1].EnumerateObject().Select(p => p.Name));
        Assert.Equal(
            (2, """{"at":"2026-06-01T02:30:00Z","attempt":2,"error":"x","trace":""}""", "{}", "2026-06-01T02:30:00.0000000"),
            (rows[1].GetProperty("at").GetInt32(), rows[1].GetProperty("e").GetString(), rows[1].GetProperty("d").GetString(), rows[1].GetProperty("s").GetString()));
    }

    [Fact]
    public void Statements_render_for_the_schema_and_prefix()
    {
        var statements = new SqlServerStatements("[queue].", "jobs_");

        Assert.Contains("FROM [queue].jobs_job", statements.JobGetById, StringComparison.Ordinal);
        Assert.Contains("INSERT INTO [queue].jobs_job", statements.JobInsertMany, StringComparison.Ordinal);
        Assert.Contains("FORCESEEK (jobs_job_fetch_idx (queue))", statements.JobGetAvailable, StringComparison.Ordinal);
        Assert.Contains("FROM [queue].jobs_job AS j", statements.JobSetStateIfRunningMany, StringComparison.Ordinal);
        Assert.DoesNotContain("{{", statements.JobSetStateIfRunningMany, StringComparison.Ordinal);
        Assert.Contains("FROM [queue].jobs_queue", statements.QueueGet, StringComparison.Ordinal);
    }

    [Fact]
    public void Times_convert_to_UTC_both_ways()
    {
        var stored = new DateTime(2026, 6, 1, 2, 30, 0, DateTimeKind.Unspecified);

        var read = SqlServerValues.FromDatabase(stored);

        Assert.Equal((TimeSpan.Zero, stored.Ticks), (read.Offset, read.UtcTicks));
        var written = SqlServerValues.ToDatabase(new DateTimeOffset(2026, 6, 1, 9, 30, 0, TimeSpan.FromHours(7)));
        Assert.Equal((DateTimeKind.Utc, stored.Ticks), (written.Kind, written.Ticks));
        Assert.Equal(DBNull.Value, SqlServerValues.Time("@t", null).Value);
        Assert.Equal(SqlDbType.DateTime2, SqlServerValues.Time("@t", null).SqlDbType);
    }

    // CONSTRAINT <name> and CREATE [UNIQUE] INDEX <name>: every name the script gives an object of its own.
    [GeneratedRegex(@"(?:CONSTRAINT|INDEX)\s+(?<name>[^\s(]+)", RegexOptions.CultureInvariant)]
    private static partial Regex ObjectName();
}

/// <summary>A transaction of some other database client.</summary>
internal sealed class ForeignTransaction : DbTransaction
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
