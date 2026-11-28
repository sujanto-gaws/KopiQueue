using System.Data.Common;
using System.Diagnostics;
using System.Text.Json;
using KopiQueue.Drivers;
using KopiQueue.Migrations;
using KopiQueue.SqlServer.Tests.Migrations;
using KopiQueue.TestInfrastructure;
using Microsoft.Data.SqlClient;

namespace KopiQueue.SqlServer.Tests;

/// <summary>
/// The insert, fetch and complete statements against a real SQL Server (plan task 5.3: FR-EXE-01, FR-EXE-06, FR-INS-04):
/// what the conformance suite does not reach on this driver, such as the T-SQL that stands in for PostgreSQL's arrays and
/// <c>jsonb ||</c>, and batches split across statements.
/// </summary>
public sealed class SqlServerJobStatementTests(SqlServerFixture db) : IClassFixture<SqlServerFixture>
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Fact]
    public async Task Inserted_json_is_stored_exactly_as_given()
    {
        var (driver, schema) = await MigratedAsync();
        const string Args = """{"b": 1,   "a": [true, null, "é\"\\"]}""";

        var job = (await driver.Executor().JobInsertManyAsync(
            [Job("k", args: Args, metadata: """{"x":1}""", tags: ["a b", "ü", "quote\"d"])], Ct))[0].Job;

        Assert.Equal(Args, job.ArgsJson);
        Assert.Equal("""{"x":1}""", job.MetadataJson);
        Assert.Equal(["a b", "ü", "quote\"d"], job.Tags);
        Assert.Equal(Args, await db.ScalarAsync($"SELECT args FROM [{schema}].kopi_job WHERE id = {job.Id}"));
    }

    [Fact]
    public async Task A_pooled_batch_over_one_statement_that_fails_late_inserts_nothing()
    {
        var (driver, _) = await MigratedAsync();
        var kind = "late_failure";
        var jobs = Enumerable.Range(0, 1500).Select(i => Job(kind, priority: i == 1200 ? 9 : 1)).ToList();

        await Assert.ThrowsAnyAsync<DbException>(() => driver.Executor().JobInsertManyAsync(jobs, Ct));
        await Assert.ThrowsAnyAsync<DbException>(() => driver.Executor().JobInsertFastAsync(jobs, Ct));

        Assert.Equal(0, await CountAsync(driver, kind));
    }

    [Fact]
    public async Task A_batch_over_one_statement_in_a_caller_transaction_commits_with_it()
    {
        var (driver, _) = await MigratedAsync();
        await using var connection = await db.OpenConnectionAsync(Ct);
        await using var tx = (SqlTransaction)await connection.BeginTransactionAsync(Ct);

        var rows = await ((IKopiDriver)driver).GetExecutor(tx).JobInsertManyAsync([.. Enumerable.Range(0, 2500).Select(i => Job($"k{i}"))], Ct);
        await tx.CommitAsync(Ct);

        Assert.Equal(Enumerable.Range(0, 2500).Select(i => $"k{i}"), rows.Select(r => r.Job.Kind));
        Assert.Equal(rows.Count, rows.Select(r => r.Job.Id).Distinct().Count());
        Assert.Equal("k2499", (await driver.Executor().JobGetByIdAsync(rows[^1].Job.Id, Ct))?.Kind);
    }

    [Theory]
    [InlineData(100, new[] { "c1", "c2", "c3", "new \"one\"" })]
    [InlineData(3, new[] { "c2", "c3", "new \"one\"" })]
    [InlineData(1, new[] { "new \"one\"" })]
    public async Task Fetch_appends_the_client_and_keeps_the_newest_entries(int maxAttemptedBy, string[] expected)
    {
        var (driver, schema) = await MigratedAsync();
        var queue = "q" + maxAttemptedBy;
        await db.ExecuteAsync(
            $"""INSERT INTO [{schema}].kopi_job (args, kind, queue, attempted_by) VALUES (N'{"{}"}', N'k', N'{queue}', N'["c1","c2","c3"]')""");

        var job = Assert.Single(await driver.Executor().JobGetAvailableAsync(
            new JobGetAvailableParams { Queue = queue, Max = 10, ClientId = "new \"one\"", MaxAttemptedBy = maxAttemptedBy }, Ct));

        Assert.Equal(expected, job.AttemptedBy);
        Assert.Equal(1, job.Attempt);
    }

    [Fact]
    public async Task Fetch_skips_a_job_another_transaction_holds_without_waiting()
    {
        var (driver, _) = await MigratedAsync();
        var ids = (await driver.Executor().JobInsertManyAsync([Job("k", "held"), Job("k", "held")], Ct)).Select(r => r.Job.Id).ToList();
        await using var connection = await db.OpenConnectionAsync(Ct);
        await using var tx = (SqlTransaction)await connection.BeginTransactionAsync(Ct);
        var holder = Assert.Single(await ((IKopiDriver)driver).GetExecutor(tx).JobGetAvailableAsync(
            new JobGetAvailableParams { Queue = "held", Max = 1, ClientId = "holder" }, Ct));

        var watch = Stopwatch.StartNew();
        var other = await driver.Executor().JobGetAvailableAsync(new JobGetAvailableParams { Queue = "held", Max = 10, ClientId = "other" }, Ct);

        Assert.True(watch.Elapsed < TimeSpan.FromSeconds(5), $"The fetch waited {watch.Elapsed}.");
        Assert.Equal(ids[0], holder.Id);
        Assert.Equal([ids[1]], other.Select(j => j.Id));
    }

    [Fact]
    public async Task Fetch_skips_an_uncommitted_insert_without_waiting()
    {
        var (driver, _) = await MigratedAsync();
        await using var connection = await db.OpenConnectionAsync(Ct);
        await using var tx = (SqlTransaction)await connection.BeginTransactionAsync(Ct);
        await ((IKopiDriver)driver).GetExecutor(tx).JobInsertManyAsync([Job("k", "pending_insert")], Ct);

        var watch = Stopwatch.StartNew();
        var fetched = await driver.Executor().JobGetAvailableAsync(new JobGetAvailableParams { Queue = "pending_insert", Max = 10, ClientId = "c" }, Ct);

        Assert.Empty(fetched);
        Assert.True(watch.Elapsed < TimeSpan.FromSeconds(5), $"The fetch waited {watch.Elapsed}.");
    }

    [Theory]
    [InlineData("""{"a": 1, "b": "x"}""", """{"b": "y", "c": null}""", """{"a": 1, "b": "y", "c": null}""")]
    [InlineData("""{"keep": {"nested": [1, {"d": 2}]}}""", """{"n": 1.5e3, "t": true, "f": false}""", """{"keep": {"nested": [1, {"d": 2}]}, "n": 1.5e3, "t": true, "f": false}""")]
    [InlineData("""{"Case": 1}""", """{"case": 2}""", """{"Case": 1, "case": 2}""")]
    [InlineData("""{"a": 1}""", "{}", """{"a": 1}""")]
    [InlineData("{}", "{}", "{}")]
    [InlineData("{}", """{"s": "quote \" slash / tab \t ü 😀"}""", """{"s": "quote \" slash / tab \t ü 😀"}""")]
    [InlineData("""{"replace": {"old": true}}""", """{"replace": ["new"]}""", """{"replace": ["new"]}""")]
    public async Task Outcomes_merge_metadata_at_the_top_level_as_jsonb_does(string before, string updates, string expected)
    {
        var (driver, schema) = await MigratedAsync();
        var queue = "m" + Guid.NewGuid().ToString("N")[..8];
        await driver.Executor().JobInsertManyAsync([Job("k", queue, metadata: before)], Ct);
        var running = Assert.Single(await driver.Executor().JobGetAvailableAsync(new JobGetAvailableParams { Queue = queue, Max = 1, ClientId = "c" }, Ct));

        var row = Assert.Single(await driver.Executor().JobSetStateIfRunningManyAsync(
            [new JobSetStateParams { Id = running.Id, State = JobState.Completed, FinalizedAt = DateTimeOffset.UtcNow, MetadataUpdates = updates }], Ct));

        Assert.True(JsonElement.DeepEquals(JsonElement.Parse(expected), JsonElement.Parse(row.MetadataJson)), row.MetadataJson);
        Assert.Equal(1, await db.ScalarAsync($"SELECT ISJSON(metadata) FROM [{schema}].kopi_job WHERE id = {row.Id}"));
    }

    [Fact]
    public async Task Errors_are_stored_as_a_json_array_of_objects()
    {
        var (driver, schema) = await MigratedAsync();
        await driver.Executor().JobInsertManyAsync([Job("k", "errors")], Ct);
        var job = Assert.Single(await driver.Executor().JobGetAvailableAsync(new JobGetAvailableParams { Queue = "errors", Max = 1, ClientId = "c" }, Ct));
        var at = new DateTimeOffset(2026, 11, 28, 1, 2, 3, TimeSpan.Zero);

        await driver.Executor().JobSetStateIfRunningManyAsync(
            [new JobSetStateParams { Id = job.Id, State = JobState.Retryable, Error = new AttemptError(at, 1, "first", "trace"), ScheduledAt = at }], Ct);
        await db.ExecuteAsync($"UPDATE [{schema}].kopi_job SET state = 'running' WHERE id = {job.Id}");
        var row = Assert.Single(await driver.Executor().JobSetStateIfRunningManyAsync(
            [new JobSetStateParams { Id = job.Id, State = JobState.Discarded, Error = new AttemptError(at, 2, "second", null), FinalizedAt = at }], Ct));

        Assert.Equal(["first", "second"], row.Errors.Select(e => e.Error));
        Assert.Equal(
            ["first", "second", "trace", ""],
            await db.StringsAsync(
                $"""
                SELECT JSON_VALUE(errors, '$[0].error') FROM [{schema}].kopi_job WHERE id = {job.Id}
                UNION ALL SELECT JSON_VALUE(errors, '$[1].error') FROM [{schema}].kopi_job WHERE id = {job.Id}
                UNION ALL SELECT JSON_VALUE(errors, '$[0].trace') FROM [{schema}].kopi_job WHERE id = {job.Id}
                UNION ALL SELECT JSON_VALUE(errors, '$[1].trace') FROM [{schema}].kopi_job WHERE id = {job.Id}
                """));
    }

    [Fact]
    public async Task Outcomes_over_one_statement_are_applied_together()
    {
        var (driver, _) = await MigratedAsync();
        await driver.Executor().JobInsertManyAsync([.. Enumerable.Range(0, 1200).Select(_ => Job("k", "many"))], Ct);
        var running = new List<JobRow>();
        while (running.Count < 1200)
        {
            running.AddRange(await driver.Executor().JobGetAvailableAsync(new JobGetAvailableParams { Queue = "many", Max = 1000, ClientId = "c" }, Ct));
        }

        var at = DateTimeOffset.UtcNow;
        var rows = await driver.Executor().JobSetStateIfRunningManyAsync(
            [.. running.Select(j => new JobSetStateParams { Id = j.Id, State = JobState.Completed, FinalizedAt = at })], Ct);

        Assert.Equal(running.Select(j => j.Id).Order(), rows.Select(r => r.Id));
        Assert.All(rows, r => Assert.Equal(JobState.Completed, r.State));
    }

    private async Task<(SqlServerDriver Driver, string Schema)> MigratedAsync()
    {
        db.SkipIfUnavailable();
        var schema = await db.CreateSchemaAsync();
        var driver = new SqlServerDriver(db.ConnectionString, new SqlServerDriverOptions { Schema = schema });
        await driver.CreateMigrator().MigrateAsync(MigrationDirection.Up, cancellationToken: Ct);
        return (driver, schema);
    }

    private static async Task<long> CountAsync(SqlServerDriver driver, string kind)
    {
        await using var connection = await driver.Connections.OpenAsync(Ct);
        await using var cmd = new SqlCommand($"SELECT COUNT_BIG(*) FROM [{driver.Schema}].kopi_job WHERE kind = @kind", connection);
        cmd.Parameters.AddWithValue("@kind", kind);
        return (long)(await cmd.ExecuteScalarAsync(Ct))!;
    }

    private static JobInsertParams Job(
        string kind, string queue = "default", int priority = 1, string args = "{}", string metadata = "{}", string[]? tags = null) => new()
    {
        Kind = kind,
        ArgsJson = args,
        Queue = queue,
        State = JobState.Available,
        Priority = priority,
        MaxAttempts = 25,
        MetadataJson = metadata,
        Tags = tags ?? [],
    };
}
