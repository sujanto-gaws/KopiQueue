using KopiQueue.Migrations;
using KopiQueue.SqlServer.Migrations;
using KopiQueue.TestInfrastructure;
using Microsoft.Data.SqlClient;

namespace KopiQueue.SqlServer.Tests.Migrations;

/// <summary>KopiQueue's SQL Server main line (design §4.4, plan task 5.1): its shape, and what its constraints allow.</summary>
public sealed class SqlServerMainLineTests(SqlServerFixture db) : IClassFixture<SqlServerFixture>
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Fact]
    public async Task Up_creates_the_tables_and_down_removes_every_object()
    {
        db.SkipIfUnavailable();
        var schema = await db.CreateSchemaAsync();
        var driver = db.MigrationDriver(schema);
        var runner = new MigrationRunner(driver);

        var up = await runner.RunAsync(Up(), Ct);
        Assert.Equal((0, 1), (up.VersionBefore, up.VersionAfter));
        Assert.Equal(["kopi_job", "kopi_leader", "kopi_migration", "kopi_queue"], await db.TablesAsync(schema));
        Assert.Equal([1L], await db.RecordedAsync(driver));

        await runner.RunAsync(new(SqlServerMigrationDriver.MainLine, "main", MigrationDirection.Down, TargetVersion: 0), Ct);
        Assert.Equal(0, await db.ObjectCountAsync(schema));

        await runner.RunAsync(Up(), Ct);
        Assert.Equal(4, (await db.TablesAsync(schema)).Count);
    }

    [Fact]
    public async Task The_job_table_has_the_designed_columns()
    {
        db.SkipIfUnavailable();
        var schema = await MigratedSchemaAsync();

        var columns = await db.StringsAsync(
            """
            SELECT CONCAT(c.name, ' ', t.name,
                          CASE WHEN t.name IN ('nvarchar', 'varchar', 'varbinary')
                               THEN CONCAT('(', IIF(c.max_length = -1, 'max', CAST(IIF(t.name = 'nvarchar', c.max_length / 2, c.max_length) AS varchar(8))), ')')
                               WHEN t.name = 'datetime2' THEN CONCAT('(', c.scale, ')') ELSE '' END,
                          IIF(c.is_nullable = 1, ' NULL', ' NOT NULL'),
                          IIF(c.is_identity = 1, ' IDENTITY', ''),
                          IIF(c.is_computed = 1, ' COMPUTED', ''),
                          IIF(c.collation_name IS NOT NULL AND c.collation_name <> CAST(DATABASEPROPERTYEX(DB_NAME(), 'Collation') AS sysname),
                              CONCAT(' COLLATE ', c.collation_name), ''))
            FROM sys.columns c JOIN sys.types t ON t.user_type_id = c.user_type_id
            WHERE c.object_id = OBJECT_ID(@table)
            ORDER BY c.column_id
            """, new SqlParameter("@table", $"[{schema}].kopi_job"));

        Assert.Equal(
        [
            "id bigint NOT NULL IDENTITY",
            "state varchar(16) NOT NULL COLLATE Latin1_General_100_BIN2",
            "attempt smallint NOT NULL",
            "max_attempts smallint NOT NULL",
            "attempted_at datetime2(6) NULL",
            "created_at datetime2(6) NOT NULL",
            "finalized_at datetime2(6) NULL",
            "scheduled_at datetime2(6) NOT NULL",
            "priority smallint NOT NULL",
            "args nvarchar(max) NOT NULL",
            "attempted_by nvarchar(max) NULL",
            "errors nvarchar(max) NULL",
            "kind nvarchar(128) NOT NULL COLLATE Latin1_General_100_BIN2",
            "metadata nvarchar(max) NOT NULL",
            "queue nvarchar(128) NOT NULL COLLATE Latin1_General_100_BIN2",
            "tags nvarchar(max) NOT NULL",
            "unique_key varbinary(32) NULL",
            "unique_states tinyint NULL",
            "unique_slot bigint NOT NULL COMPUTED",
        ], columns);
    }

    [Fact]
    public async Task The_job_indexes_have_the_designed_keys_and_filters()
    {
        db.SkipIfUnavailable();
        var schema = await MigratedSchemaAsync();

        var indexes = await db.StringsAsync(
            """
            SELECT CONCAT(i.name, IIF(i.is_unique = 1, ' UNIQUE', ''), ' (',
                          (SELECT STRING_AGG(c.name, ', ') WITHIN GROUP (ORDER BY ic.key_ordinal)
                           FROM sys.index_columns ic JOIN sys.columns c ON c.object_id = ic.object_id AND c.column_id = ic.column_id
                           WHERE ic.object_id = i.object_id AND ic.index_id = i.index_id AND ic.key_ordinal > 0), ')',
                          (SELECT ' INCLUDE (' + STRING_AGG(c.name, ', ') + ')'
                           FROM sys.index_columns ic JOIN sys.columns c ON c.object_id = ic.object_id AND c.column_id = ic.column_id
                           WHERE ic.object_id = i.object_id AND ic.index_id = i.index_id AND ic.is_included_column = 1),
                          IIF(i.has_filter = 1, ' FILTERED', ''))
            FROM sys.indexes i
            WHERE i.object_id = OBJECT_ID(@table)
            ORDER BY i.name
            """, new SqlParameter("@table", $"[{schema}].kopi_job"));

        Assert.Equal(
        [
            "kopi_job_fetch_idx (queue, priority, scheduled_at, id) FILTERED",
            "kopi_job_finalized_idx (state, finalized_at) FILTERED",
            "kopi_job_kind_idx (kind)",
            "kopi_job_pk UNIQUE (id)",
            "kopi_job_running_idx (attempted_at) FILTERED",
            "kopi_job_schedule_idx (scheduled_at) INCLUDE (queue) FILTERED",
            "kopi_job_unique_idx UNIQUE (unique_key, unique_slot) FILTERED",
        ], indexes);
    }

    [Fact]
    public async Task A_job_inserted_with_only_args_and_kind_gets_the_defaults()
    {
        db.SkipIfUnavailable();
        var schema = await MigratedSchemaAsync();
        var before = DateTime.UtcNow.AddMinutes(-1);

        await using var conn = await db.OpenConnectionAsync(Ct);
        await using var cmd = new SqlCommand(
            $$"""
            INSERT INTO [{{schema}}].kopi_job (args, kind)
            OUTPUT inserted.state, inserted.attempt, inserted.max_attempts, inserted.priority, inserted.queue,
                   inserted.metadata, inserted.tags, inserted.created_at, inserted.scheduled_at, inserted.unique_slot, inserted.id
            VALUES (N'{}', N'email')
            """, conn);
        await using var reader = await cmd.ExecuteReaderAsync(Ct);
        Assert.True(await reader.ReadAsync(Ct));

        Assert.Equal("available", reader.GetString(0));
        Assert.Equal((short)0, reader.GetInt16(1));
        Assert.Equal((short)25, reader.GetInt16(2));
        Assert.Equal((short)1, reader.GetInt16(3));
        Assert.Equal("default", reader.GetString(4));
        Assert.Equal("{}", reader.GetString(5));
        Assert.Equal("[]", reader.GetString(6));
        Assert.InRange(reader.GetDateTime(7), before, DateTime.UtcNow.AddMinutes(1));
        // Each SYSUTCDATETIME() default reads the clock on its own, so the two can differ slightly. The driver's insert
        // reads it once for both (job_insert_many.sql).
        Assert.InRange(reader.GetDateTime(8) - reader.GetDateTime(7), TimeSpan.FromSeconds(-1), TimeSpan.FromSeconds(1));
        Assert.Equal(reader.GetInt64(10), reader.GetInt64(9)); // no unique key: the slot is the job's own ID
    }

    [Theory]
    [InlineData("state", "N'{}', N'k', 'bogus', NULL, 1, N'default'")]
    [InlineData("finalized_at with a live state", "N'{}', N'k', 'available', SYSUTCDATETIME(), 1, N'default'")]
    [InlineData("a finalised state without finalized_at", "N'{}', N'k', 'completed', NULL, 1, N'default'")]
    [InlineData("priority", "N'{}', N'k', 'available', NULL, 5, N'default'")]
    [InlineData("args JSON", "N'not json', N'k', 'available', NULL, 1, N'default'")]
    [InlineData("empty kind", "N'{}', N'', 'available', NULL, 1, N'default'")]
    [InlineData("long kind", "N'{}', REPLICATE(N'k', 128), 'available', NULL, 1, N'default'")]
    [InlineData("empty queue", "N'{}', N'k', 'available', NULL, 1, N''")]
    public async Task The_job_constraints_reject_bad_rows(string what, string values)
    {
        db.SkipIfUnavailable();
        var schema = await MigratedSchemaAsync();

        var ex = await Assert.ThrowsAsync<SqlException>(() => db.ExecuteAsync(
            $"INSERT INTO [{schema}].kopi_job (args, kind, state, finalized_at, priority, queue) VALUES ({values})"));

        Assert.True(ex.Number == 547, $"{what}: expected a CHECK violation (547), got {ex.Number}: {ex.Message}");
    }

    [Fact]
    public async Task The_job_constraints_accept_the_longest_names_and_every_state()
    {
        db.SkipIfUnavailable();
        var schema = await MigratedSchemaAsync();

        var inserted = await db.ExecuteAsync(
            $$"""
            INSERT INTO [{{schema}}].kopi_job (args, kind, queue, state, finalized_at, priority) VALUES
              (N'{}', REPLICATE(N'k', 127), REPLICATE(N'q', 127), 'available', NULL, 4),
              (N'[]', N'k', N'q', 'cancelled', SYSUTCDATETIME(), 1), (N'{"a": 1}', N'k', N'q', 'completed', SYSUTCDATETIME(), 1),
              (N'{}', N'k', N'q', 'discarded', SYSUTCDATETIME(), 1), (N'{}', N'k', N'q', 'pending', NULL, 1),
              (N'{}', N'k', N'q', 'retryable', NULL, 1), (N'{}', N'k', N'q', 'running', NULL, 1),
              (N'{}', N'k', N'q', 'scheduled', NULL, 1)
            """);

        Assert.Equal(8, inserted);
    }

    [Fact]
    public async Task The_unique_slot_follows_the_shared_bitmask_for_every_state_and_bit()
    {
        db.SkipIfUnavailable();
        var schema = await MigratedSchemaAsync();
        await using var conn = await db.OpenConnectionAsync(Ct);

        foreach (var state in Enum.GetValues<JobState>())
        {
            for (var bit = 0; bit < 8; bit++)
            {
                var mask = (byte)(1 << bit);
                await using var cmd = new SqlCommand(
                    $$"""
                    INSERT INTO [{{schema}}].kopi_job (args, kind, state, finalized_at, unique_key, unique_states)
                    OUTPUT inserted.unique_slot
                    VALUES (N'{}', N'k', @state, IIF(@state IN ('cancelled', 'completed', 'discarded'), SYSUTCDATETIME(), NULL),
                            CAST(NEWID() AS varbinary(16)), @mask)
                    """, conn);
                cmd.Parameters.AddWithValue("@state", state.ToString().ToLowerInvariant());
                cmd.Parameters.AddWithValue("@mask", mask);

                var holds = (long)(await cmd.ExecuteScalarAsync(Ct))! == 0;

                Assert.True(holds == (mask == UniqueStates.Bit(state)), $"state {state}, mask {mask}: holds = {holds}");
            }
        }
    }

    [Fact]
    public async Task One_job_holds_a_unique_key_while_in_its_unique_states()
    {
        db.SkipIfUnavailable();
        var schema = await MigratedSchemaAsync();
        var table = $"[{schema}].kopi_job";
        var mask = new SqlParameter("@mask", UniqueStates.Default);
        string Insert(string state, string finalizedAt = "NULL", string key = "0x01", string states = "@mask") =>
            $"INSERT INTO {table} (args, kind, state, finalized_at, unique_key, unique_states) VALUES (N'{{}}', N'k', '{state}', {finalizedAt}, {key}, {states})";

        await db.ExecuteAsync(Insert("available"), mask);
        var duplicate = await Assert.ThrowsAsync<SqlException>(() => db.ExecuteAsync(Insert("scheduled"), Clone(mask)));
        Assert.Equal(2601, duplicate.Number);

        // Outside the default unique states, without a bitmask or without a key, jobs never hold it.
        await db.ExecuteAsync(Insert("discarded", "SYSUTCDATETIME()"), Clone(mask));
        await db.ExecuteAsync(Insert("cancelled", "SYSUTCDATETIME()"), Clone(mask));
        await db.ExecuteAsync(Insert("available", states: "NULL"));
        await db.ExecuteAsync(Insert("available", key: "NULL"), Clone(mask));
        await db.ExecuteAsync(Insert("available", key: "NULL"), Clone(mask));

        // Cancelling the holder frees the key.
        await db.ExecuteAsync($"UPDATE {table} SET state = 'cancelled', finalized_at = SYSUTCDATETIME() WHERE state = 'available' AND unique_slot = 0");
        await db.ExecuteAsync(Insert("available"), Clone(mask));
        Assert.Equal(1, await db.ScalarAsync($"SELECT COUNT(*) FROM {table} WHERE unique_key = 0x01 AND unique_slot = 0"));
    }

    [Fact]
    public async Task Names_compare_case_sensitively_whatever_the_database_collation()
    {
        db.SkipIfUnavailable();
        var schema = await MigratedSchemaAsync();

        await db.ExecuteAsync($"INSERT INTO [{schema}].kopi_queue (name) VALUES (N'Reports'), (N'reports')");
        await db.ExecuteAsync($"INSERT INTO [{schema}].kopi_job (args, kind) VALUES (N'{{}}', N'Email')");

        Assert.Equal(2, await db.ScalarAsync($"SELECT COUNT(*) FROM [{schema}].kopi_queue"));
        Assert.Equal(0, await db.ScalarAsync($"SELECT COUNT(*) FROM [{schema}].kopi_job WHERE kind = N'email'"));
        Assert.Equal(1, await db.ScalarAsync($"SELECT COUNT(*) FROM [{schema}].kopi_job WHERE kind = @kind", new SqlParameter("@kind", "Email")));
    }

    [Fact]
    public async Task The_leader_table_holds_only_the_default_row()
    {
        db.SkipIfUnavailable();
        var schema = await MigratedSchemaAsync();
        var table = $"[{schema}].kopi_leader";

        var other = await Assert.ThrowsAsync<SqlException>(() => db.ExecuteAsync(
            $"INSERT INTO {table} (name, leader_id, elected_at, expires_at) VALUES ('other', N'c1', SYSUTCDATETIME(), SYSUTCDATETIME())"));
        await db.ExecuteAsync($"INSERT INTO {table} (leader_id, elected_at, expires_at) VALUES (N'c1', SYSUTCDATETIME(), SYSUTCDATETIME())");
        var second = await Assert.ThrowsAsync<SqlException>(() => db.ExecuteAsync(
            $"INSERT INTO {table} (leader_id, elected_at, expires_at) VALUES (N'c2', SYSUTCDATETIME(), SYSUTCDATETIME())"));

        Assert.Equal(547, other.Number);
        Assert.Equal(2627, second.Number); // primary key violation
        Assert.Equal("default", await db.ScalarAsync($"SELECT name FROM {table}"));
    }

    [Fact]
    public async Task A_queue_gets_its_times_and_metadata_by_default()
    {
        db.SkipIfUnavailable();
        var schema = await MigratedSchemaAsync();

        await db.ExecuteAsync($"INSERT INTO [{schema}].kopi_queue (name) VALUES (N'default')");

        Assert.Equal(
            ["{}:1:1"],
            await db.StringsAsync($"SELECT CONCAT(metadata, ':', IIF(updated_at >= created_at, 1, 0), ':', IIF(paused_at IS NULL, 1, 0)) FROM [{schema}].kopi_queue"));
        await Assert.ThrowsAsync<SqlException>(() => db.ExecuteAsync($"INSERT INTO [{schema}].kopi_queue (name, metadata) VALUES (N'q', N'nope')"));
    }

    [Fact]
    public async Task Two_prefixes_share_a_schema()
    {
        db.SkipIfUnavailable();
        var schema = await db.CreateSchemaAsync();

        await new MigrationRunner(db.MigrationDriver(schema)).RunAsync(Up(), Ct);
        await new MigrationRunner(db.MigrationDriver(schema, "other_")).RunAsync(Up(), Ct);

        Assert.Equal(
            ["kopi_job", "kopi_leader", "kopi_migration", "kopi_queue", "other_job", "other_leader", "other_migration", "other_queue"],
            await db.TablesAsync(schema));

        await new MigrationRunner(db.MigrationDriver(schema, "other_"))
            .RunAsync(new(SqlServerMigrationDriver.MainLine, "main", MigrationDirection.Down, TargetVersion: 0), Ct);
        Assert.Equal(["kopi_job", "kopi_leader", "kopi_migration", "kopi_queue"], await db.TablesAsync(schema));
    }

    private static MigrationRequest Up() => new(SqlServerMigrationDriver.MainLine, "main", MigrationDirection.Up);

    private static SqlParameter Clone(SqlParameter parameter) => new(parameter.ParameterName, parameter.Value);

    private async Task<string> MigratedSchemaAsync()
    {
        var schema = await db.CreateSchemaAsync();
        await new MigrationRunner(db.MigrationDriver(schema)).RunAsync(Up(), Ct);
        return schema;
    }
}
