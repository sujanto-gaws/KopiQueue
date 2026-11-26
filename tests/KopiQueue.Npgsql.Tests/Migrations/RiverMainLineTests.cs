using KopiQueue.Migrations;
using KopiQueue.Npgsql.Migrations;
using KopiQueue.TestInfrastructure;
using Npgsql;

namespace KopiQueue.Npgsql.Tests.Migrations;

/// <summary>
/// The River-compatible main line (plan task 1.2: design §4.1, FR-MIG-05, NFR-COMP-05).
/// Every version, up and down, must leave the same catalog as River v0.51.2's CLI does. The expected catalogs are
/// goldens under <c>Migrations/River/v0.51.2</c>, which <see cref="River_cli_produces_the_goldens"/> checks against
/// the real CLI (and regenerates when <c>KOPI_RIVER_GOLDEN_DIR</c> is set).
/// </summary>
public sealed class RiverMainLineTests(PostgresFixture db) : IClassFixture<PostgresFixture>
{
    public const int Latest = 8;
    public const string GoldenDirVariable = "KOPI_RIVER_GOLDEN_DIR";

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    public static TheoryData<int> Versions => [.. Enumerable.Range(0, Latest + 1)];

    [Fact]
    public void The_embedded_main_line_has_versions_1_to_8()
    {
        Assert.Equal(Enumerable.Range(1, Latest), NpgsqlMigrationDriver.MainLine.Select(m => m.Version));
        Assert.All(NpgsqlMigrationDriver.MainLine, m => Assert.True(m.Up.InTransaction && m.Down.InTransaction));
    }

    [Theory]
    [MemberData(nameof(Versions))]
    public async Task Up_to_each_version_matches_River(int version)
    {
        db.SkipIfUnavailable();
        await using var ds = db.CreateDataSource();
        var schema = await CreateSchemaAsync(ds);

        if (version > 0)
        {
            await UpAsync(ds, schema, version);
        }

        await AssertCatalogAsync(ds, schema, await Goldens.UpAsync(ds, version));
    }

    [Fact]
    public async Task Down_one_version_at_a_time_matches_River()
    {
        db.SkipIfUnavailable();
        await using var ds = db.CreateDataSource();
        var schema = await CreateSchemaAsync(ds);
        await UpAsync(ds, schema);

        for (var version = Latest - 1; version >= 0; version--)
        {
            var result = await new MigrationRunner(new NpgsqlMigrationDriver(ds, schema))
                .RunAsync(new(NpgsqlMigrationDriver.MainLine, "main", MigrationDirection.Down), Ct);

            Assert.Equal(version, result.VersionAfter);
            await AssertCatalogAsync(ds, schema, await Goldens.DownAsync(ds, version), $"after reverting to version {version}");
        }
    }

    [Fact]
    public async Task Down_to_zero_then_up_again_matches_a_fresh_schema()
    {
        db.SkipIfUnavailable();
        await using var ds = db.CreateDataSource();
        var schema = await CreateSchemaAsync(ds);
        await UpAsync(ds, schema);

        await DownAsync(ds, schema, 0);
        await AssertCatalogAsync(ds, schema, await Goldens.UpAsync(ds, 0));
        await UpAsync(ds, schema);

        await AssertCatalogAsync(ds, schema, await Goldens.UpAsync(ds, Latest));
    }

    [Fact]
    public async Task Another_prefix_gives_the_same_shape_and_coexists_with_river_()
    {
        db.SkipIfUnavailable();
        await using var ds = db.CreateDataSource();
        var schema = await CreateSchemaAsync(ds);
        var kopi = new NpgsqlMigrationDriver(ds, schema, "kopi_");

        await new MigrationRunner(kopi).RunAsync(new(NpgsqlMigrationDriver.MainLine, "main", MigrationDirection.Up), Ct);
        var kopiOnly = await CatalogSnapshot.TakeAsync(ds, schema, Ct);
        await UpAsync(ds, schema);

        // Renaming changes the sort order within sections, so compare sorted lines.
        Assert.Equal(
            CatalogSnapshot.Lines(await Goldens.UpAsync(ds, Latest)).Order(StringComparer.Ordinal),
            CatalogSnapshot.Lines(kopiOnly.Replace("kopi_", "river_", StringComparison.Ordinal)).Order(StringComparer.Ordinal));
        await AssertTableCountAsync(ds, schema, 10);
    }

    [Fact]
    public async Task Without_a_schema_the_search_path_is_used()
    {
        db.SkipIfUnavailable();
        string schema;
        await using (var setup = db.CreateDataSource())
        {
            schema = await CreateSchemaAsync(setup);
        }

        await using var ds = NpgsqlDataSource.Create(new NpgsqlConnectionStringBuilder(db.ConnectionString) { SearchPath = schema }.ConnectionString);
        await new MigrationRunner(new NpgsqlMigrationDriver(ds)).RunAsync(new(NpgsqlMigrationDriver.MainLine, "main", MigrationDirection.Up), Ct);

        await AssertCatalogAsync(ds, schema, await Goldens.UpAsync(ds, Latest));
    }

    [Fact]
    public async Task Version_3_fills_null_tags_and_version_4_fills_null_args_and_metadata()
    {
        db.SkipIfUnavailable();
        await using var ds = db.CreateDataSource();
        var schema = await CreateSchemaAsync(ds);
        await UpAsync(ds, schema, 2);
        await ExecAsync(ds, $"INSERT INTO {schema}.river_job (args, kind, max_attempts, tags) VALUES (NULL, 'k', 3, NULL)");

        await UpAsync(ds, schema, 3);
        Assert.Equal("{}|", await ScalarAsync<string>(ds, $"SELECT tags::text || '|' || coalesce(args::text, '') FROM {schema}.river_job"));

        // A revert of version 4 leaves metadata nullable; going up again fills the gaps.
        await UpAsync(ds, schema, 4);
        await DownAsync(ds, schema, 3);
        await ExecAsync(ds, $"INSERT INTO {schema}.river_job (args, kind, max_attempts, metadata) VALUES ('{{}}', 'm', 3, NULL)");
        await UpAsync(ds, schema, 4);

        Assert.Equal(0L, await ScalarAsync<long>(ds, $"SELECT count(*) FROM {schema}.river_job WHERE args IS NULL OR metadata IS NULL"));
        Assert.Equal(2L, await ScalarAsync<long>(ds, $"SELECT count(*) FROM {schema}.river_job WHERE args = '{{}}' AND metadata = '{{}}'"));
    }

    [Fact]
    public async Task Version_4_fails_and_rolls_back_while_a_leader_row_has_another_name()
    {
        db.SkipIfUnavailable();
        await using var ds = db.CreateDataSource();
        var schema = await CreateSchemaAsync(ds);
        await UpAsync(ds, schema, 3);
        await ExecAsync(ds, $"INSERT INTO {schema}.river_leader (name, leader_id, elected_at, expires_at) VALUES ('other', 'l', now(), now())");

        var ex = await Assert.ThrowsAsync<KopiQueueMigrationException>(() => UpAsync(ds, schema));

        Assert.Equal(PostgresErrorCodes.CheckViolation, Assert.IsType<PostgresException>(ex.InnerException).SqlState);
        await AssertCatalogAsync(ds, schema, await Goldens.UpAsync(ds, 3));
    }

    [Fact]
    public async Task Version_5_moves_recorded_versions_and_their_timestamps_to_the_new_migration_table()
    {
        db.SkipIfUnavailable();
        await using var ds = db.CreateDataSource();
        var schema = await CreateSchemaAsync(ds);
        await UpAsync(ds, schema, 4);
        await ExecAsync(ds, $"UPDATE {schema}.river_migration SET created_at = timestamptz '2020-01-01 00:00:00+00' + version * interval '1 day'");

        await UpAsync(ds, schema, 5);

        Assert.Equal(
            "main 1 2020-01-02,main 2 2020-01-03,main 3 2020-01-04,main 4 2020-01-05",
            await ScalarAsync<string>(ds, $"""
                SELECT string_agg(line || ' ' || version || ' ' || to_char(created_at AT TIME ZONE 'UTC', 'YYYY-MM-DD'), ',' ORDER BY version)
                FROM {schema}.river_migration WHERE version < 5
                """));

        await DownAsync(ds, schema, 4);
        Assert.Equal(
            "1 2020-01-02,2 2020-01-03,3 2020-01-04,4 2020-01-05",
            await ScalarAsync<string>(ds, $"""
                SELECT string_agg(version || ' ' || to_char(created_at AT TIME ZONE 'UTC', 'YYYY-MM-DD'), ',' ORDER BY version)
                FROM {schema}.river_migration
                """));
    }

    [Fact]
    public async Task Version_5_cannot_be_reverted_while_other_lines_are_recorded()
    {
        db.SkipIfUnavailable();
        await using var ds = db.CreateDataSource();
        var schema = await CreateSchemaAsync(ds);
        await UpAsync(ds, schema, 5);
        await ExecAsync(ds, $"INSERT INTO {schema}.river_migration (line, version) VALUES ('extra', 1)");

        var ex = await Assert.ThrowsAsync<KopiQueueMigrationException>(() => DownAsync(ds, schema, 4));

        Assert.Equal(PostgresErrorCodes.RaiseException, Assert.IsType<PostgresException>(ex.InnerException).SqlState);
        await AssertCatalogAsync(ds, schema, await Goldens.UpAsync(ds, 5));
    }

    [Fact]
    public async Task Other_lines_need_main_at_version_5()
    {
        db.SkipIfUnavailable();
        await using var ds = db.CreateDataSource();
        var schema = await CreateSchemaAsync(ds);
        await UpAsync(ds, schema, 4);
        Migration[] extra = [new("extra", 1, "noop", new MigrationScript("SELECT 1", true), new MigrationScript("SELECT 1", true))];
        var runner = new MigrationRunner(new NpgsqlMigrationDriver(ds, schema));

        var ex = await Assert.ThrowsAsync<KopiQueueMigrationException>(() => runner.RunAsync(new(extra, "extra", MigrationDirection.Up), Ct));
        Assert.Contains("version 5 or later", ex.InnerException!.Message, StringComparison.Ordinal);

        await UpAsync(ds, schema, 5);
        var ok = await runner.RunAsync(new(extra, "extra", MigrationDirection.Up), Ct);
        Assert.Equal(1, ok.VersionAfter);
    }

    [Fact]
    public async Task Versions_2_and_3_notify_river_insert_for_available_jobs()
    {
        db.SkipIfUnavailable();
        await using var ds = db.CreateDataSource();
        var schema = await CreateSchemaAsync(ds);
        await UpAsync(ds, schema, 2);
        await using var listener = await ds.OpenConnectionAsync(Ct);
        var payloads = new List<string>();
        listener.Notification += (_, e) => payloads.Add(e.Channel + " " + e.Payload);
        await using (var listen = new NpgsqlCommand("LISTEN river_insert", listener))
        {
            await listen.ExecuteNonQueryAsync(Ct);
        }

        await ExecAsync(ds, $$"""
            INSERT INTO {{schema}}.river_job (args, kind, max_attempts, queue, state, scheduled_at) VALUES
                ('{}', 'k', 3, 'now', 'available', now()),
                ('{}', 'k', 3, 'later', 'scheduled', now() + interval '1 hour'),
                ('{}', 'k', 3, 'future', 'available', now() + interval '1 hour')
            """);
        await DrainAsync(listener);

        Assert.Equal(["river_insert {\"queue\" : \"now\"}", "river_insert {\"queue\" : \"future\"}"], payloads);

        await UpAsync(ds, schema, 4);
        await ExecAsync(ds, $"INSERT INTO {schema}.river_job (args, kind, max_attempts, queue) VALUES ('{{}}', 'k', 3, 'after')");
        await DrainAsync(listener);
        Assert.Equal(2, payloads.Count);
    }

    [Fact]
    public async Task The_state_bitmask_function_maps_bits_scheduled_to_available_from_the_left()
    {
        db.SkipIfUnavailable();
        await using var ds = db.CreateDataSource();
        var schema = await CreateSchemaAsync(ds);
        await UpAsync(ds, schema, 6);

        var table = await TruthTableAsync(ds, schema);

        string[] rightToLeft = ["available", "cancelled", "completed", "discarded", "pending", "retryable", "running", "scheduled"];
        var expected = Enumerable.Range(0, 256)
            .SelectMany(mask => rightToLeft.Select((state, bit) => $"{mask}|{state}|{(((mask >> bit) & 1) == 1 ? "t" : "f")}"))
            .Order(StringComparer.Ordinal)
            .Concat(["null-mask|NULL", "null-state|f"]);
        Assert.Equal(expected, table);
    }

    [Fact]
    public async Task The_unique_index_applies_only_in_the_states_of_the_bitmask()
    {
        db.SkipIfUnavailable();
        await using var ds = db.CreateDataSource();
        var schema = await CreateSchemaAsync(ds);
        await UpAsync(ds, schema);
        var insert = $"INSERT INTO {schema}.river_job (args, kind, unique_key, unique_states, state, finalized_at) VALUES ";

        // 11110101: everything except cancelled and discarded.
        await ExecAsync(ds, insert + "('{}', 'k', '\\x01', B'11110101', 'cancelled', now())");
        await ExecAsync(ds, insert + "('{}', 'k', '\\x01', B'11110101', 'available', NULL)");
        var duplicate = await Assert.ThrowsAsync<PostgresException>(() => ExecAsync(ds, insert + "('{}', 'other', '\\x01', B'11110101', 'scheduled', NULL)"));

        Assert.Equal(PostgresErrorCodes.UniqueViolation, duplicate.SqlState);
        Assert.Equal("river_job_unique_idx", duplicate.ConstraintName);
    }

    // ---- Against River's own CLI (skipped unless KOPI_RIVER_CLI is set) ----

    [Fact]
    public async Task River_cli_produces_the_goldens()
    {
        db.SkipIfUnavailable();
        var river = RiverCli.GetOrSkip(db.ConnectionString);
        await using var ds = db.CreateDataSource();
        var schema = await CreateSchemaAsync(ds);
        var writeTo = Environment.GetEnvironmentVariable(GoldenDirVariable);
        var up = new string[Latest + 1];

        for (var version = 1; version <= Latest; version++)
        {
            await river.MigrateUpAsync(schema, version, Ct);
            up[version] = await CatalogSnapshot.TakeAsync(ds, schema, Ct);
            if (writeTo is null)
            {
                Assert.Equal(CatalogSnapshot.Lines(await Goldens.UpAsync(ds, version)), CatalogSnapshot.Lines(up[version]));
            }
            else
            {
                await File.WriteAllTextAsync(Path.Combine(writeTo, $"up-{version}.txt"), up[version], Ct);
            }
        }

        for (var version = Latest - 1; version >= 0; version--)
        {
            await river.MigrateDownAsync(schema, version, Ct);
            var down = await CatalogSnapshot.TakeAsync(ds, schema, Ct);
            var downFile = writeTo is null ? null : Path.Combine(writeTo, $"down-{version}.txt");
            if (downFile is not null)
            {
                // Only states that differ from the up state get their own file.
                var differs = version > 0 && down != up[version];
                if (differs)
                {
                    await File.WriteAllTextAsync(downFile, down, Ct);
                }
                else
                {
                    File.Delete(downFile);
                }
            }
            else
            {
                Assert.Equal(CatalogSnapshot.Lines(await Goldens.DownAsync(ds, version)), CatalogSnapshot.Lines(down));
            }
        }
    }

    [Fact]
    public async Task KopiQueue_continues_a_schema_River_migrated_to_version_4()
    {
        db.SkipIfUnavailable();
        var river = RiverCli.GetOrSkip(db.ConnectionString);
        await using var ds = db.CreateDataSource();
        var schema = await CreateSchemaAsync(ds);
        await river.MigrateUpAsync(schema, 4, Ct);

        var result = await new MigrationRunner(new NpgsqlMigrationDriver(ds, schema))
            .RunAsync(new(NpgsqlMigrationDriver.MainLine, "main", MigrationDirection.Up), Ct);

        Assert.Equal((4, Latest), (result.VersionBefore, result.VersionAfter));
        await AssertCatalogAsync(ds, schema, await Goldens.UpAsync(ds, Latest));
        await river.ValidateAsync(schema, Ct);
    }

    [Fact]
    public async Task River_continues_and_reverts_a_schema_KopiQueue_migrated()
    {
        db.SkipIfUnavailable();
        var river = RiverCli.GetOrSkip(db.ConnectionString);
        await using var ds = db.CreateDataSource();
        var schema = await CreateSchemaAsync(ds);

        await UpAsync(ds, schema, 3);
        await river.MigrateUpAsync(schema, null, Ct);
        await AssertCatalogAsync(ds, schema, await Goldens.UpAsync(ds, Latest));
        await river.ValidateAsync(schema, Ct);

        await DownAsync(ds, schema, 0);
        await UpAsync(ds, schema);
        await river.ValidateAsync(schema, Ct);
        await river.MigrateDownAsync(schema, 0, Ct);
        await AssertCatalogAsync(ds, schema, await Goldens.UpAsync(ds, 0));
    }

    [Fact]
    public async Task The_state_bitmask_function_returns_what_Rivers_returns()
    {
        db.SkipIfUnavailable();
        var river = RiverCli.GetOrSkip(db.ConnectionString);
        await using var ds = db.CreateDataSource();
        var riverSchema = await CreateSchemaAsync(ds);
        var kopiSchema = await CreateSchemaAsync(ds);
        await river.MigrateUpAsync(riverSchema, 6, Ct);
        await UpAsync(ds, kopiSchema, 6);

        Assert.Equal(await TruthTableAsync(ds, riverSchema), await TruthTableAsync(ds, kopiSchema));
    }

    private static async Task<List<string>> TruthTableAsync(NpgsqlDataSource ds, string schema)
    {
        var rows = new List<string>();
        await using (var cmd = ds.CreateCommand($"""
            SELECT line FROM (
                SELECT m || '|' || s::text || '|' || CASE WHEN {schema}.river_job_state_in_bitmask(m::bit(8), s) THEN 't' ELSE 'f' END AS line
                FROM generate_series(0, 255) m CROSS JOIN unnest(enum_range(NULL::{schema}.river_job_state)) s
            ) x ORDER BY line COLLATE "C"
            """))
        await using (var reader = await cmd.ExecuteReaderAsync(Ct))
        {
            while (await reader.ReadAsync(Ct))
            {
                rows.Add(reader.GetString(0));
            }
        }

        rows.Add("null-mask|" + (await ScalarAsync<bool?>(ds, $"SELECT {schema}.river_job_state_in_bitmask(NULL, 'available')") switch
        {
            null => "NULL",
            var b => b.Value ? "t" : "f",
        }));
        rows.Add("null-state|" + (await ScalarAsync<bool?>(ds, $"SELECT {schema}.river_job_state_in_bitmask(B'11111111', NULL)") switch
        {
            null => "NULL",
            var b => b.Value ? "t" : "f",
        }));
        return rows;
    }

    private static async Task UpAsync(NpgsqlDataSource ds, string schema, int? target = null) =>
        await new MigrationRunner(new NpgsqlMigrationDriver(ds, schema))
            .RunAsync(new(NpgsqlMigrationDriver.MainLine, "main", MigrationDirection.Up, target), Ct);

    private static async Task DownAsync(NpgsqlDataSource ds, string schema, int target) =>
        await new MigrationRunner(new NpgsqlMigrationDriver(ds, schema))
            .RunAsync(new(NpgsqlMigrationDriver.MainLine, "main", MigrationDirection.Down, target), Ct);

    private static async Task AssertCatalogAsync(NpgsqlDataSource ds, string schema, string expected, string? because = null)
    {
        var actual = await CatalogSnapshot.TakeAsync(ds, schema, Ct);
        if (because is null)
        {
            Assert.Equal(CatalogSnapshot.Lines(expected), CatalogSnapshot.Lines(actual));
        }
        else
        {
            Assert.True(CatalogSnapshot.Lines(expected).SequenceEqual(CatalogSnapshot.Lines(actual)),
                $"Catalog differs from River's {because}:\n{actual}");
        }
    }

    private static async Task AssertTableCountAsync(NpgsqlDataSource ds, string schema, long expected) =>
        Assert.Equal(expected, await ScalarAsync<long>(ds, $"SELECT count(*) FROM pg_tables WHERE schemaname = '{schema}'"));

    private static async Task<string> CreateSchemaAsync(NpgsqlDataSource ds)
    {
        var name = "r_" + Guid.NewGuid().ToString("N")[..12];
        await ExecAsync(ds, $"CREATE SCHEMA {name}");
        return name;
    }

    private static async Task ExecAsync(NpgsqlDataSource ds, string sql)
    {
        await using var cmd = ds.CreateCommand(sql);
        await cmd.ExecuteNonQueryAsync(Ct);
    }

    private static async Task<T> ScalarAsync<T>(NpgsqlDataSource ds, string sql)
    {
        await using var cmd = ds.CreateCommand(sql);
        var value = await cmd.ExecuteScalarAsync(Ct);
        return value is DBNull ? default! : (T)value!;
    }

    /// <summary>Processes notifications until none arrives for 500 ms.</summary>
    private static async Task DrainAsync(NpgsqlConnection listener)
    {
        while (await listener.WaitAsync(500, Ct))
        {
        }
    }

    /// <summary>Expected catalogs recorded from River's CLI (see <see cref="River_cli_produces_the_goldens"/>).</summary>
    private static class Goldens
    {
        private static readonly string Directory = Path.Combine(AppContext.BaseDirectory, "Migrations", "River", RiverCli.PinnedVersion);

        /// <summary>The catalog River leaves after migrating up to <paramref name="version"/> (0: an empty schema).</summary>
        public static async Task<string> UpAsync(NpgsqlDataSource ds, int version) =>
            version == 0 ? await EmptyAsync(ds) : await File.ReadAllTextAsync(Path.Combine(Directory, $"up-{version}.txt"), Ct);

        /// <summary>The catalog River leaves after reverting from a later version down to <paramref name="version"/>.</summary>
        public static async Task<string> DownAsync(NpgsqlDataSource ds, int version)
        {
            var file = Path.Combine(Directory, $"down-{version}.txt");
            return File.Exists(file) ? await File.ReadAllTextAsync(file, Ct) : await UpAsync(ds, version);
        }

        private static async Task<string> EmptyAsync(NpgsqlDataSource ds)
        {
            var schema = await CreateSchemaAsync(ds);
            return await CatalogSnapshot.TakeAsync(ds, schema, Ct);
        }
    }
}
