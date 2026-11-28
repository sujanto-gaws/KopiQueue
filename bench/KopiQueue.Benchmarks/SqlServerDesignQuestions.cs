using System.Data;
using System.Diagnostics;
using KopiQueue.Drivers;
using Microsoft.Data.SqlClient;
using Microsoft.Data.SqlClient.Server;

namespace KopiQueue.Benchmarks;

/// <summary>The SQL Server design questions plan task 5.9 settles by measurement: DQ-6 and DQ-7 (design §14).</summary>
internal sealed class SqlServerDesignQuestions(BenchDatabase db, BenchSettings settings)
{
    /// <summary>
    /// DQ-6: the driver's OPENJSON insert against the same <c>INSERT ... SELECT</c> fed by a table-valued parameter.
    /// Both include building the parameter on the client. The calls alternate, so neither side gets a warmer cache.
    /// </summary>
    public async Task BatchParametersAsync()
    {
        await db.ExecAsync("""
            CREATE TYPE dbo.bench_job_rows AS TABLE (
                ordinal      int           NOT NULL PRIMARY KEY,
                args         nvarchar(max) NOT NULL,
                kind         nvarchar(128) NOT NULL,
                max_attempts smallint      NOT NULL,
                metadata     nvarchar(max) NOT NULL,
                priority     smallint      NOT NULL,
                queue        nvarchar(128) NOT NULL,
                scheduled_at datetime2(7)  NULL,
                state        varchar(16)   NOT NULL,
                tags         nvarchar(max) NOT NULL);
            """);

        var executor = db.Driver.GetExecutor();
        foreach (var size in new[] { 100, 1_000 })
        {
            Report.Heading($"DQ-6: OPENJSON or a table-valued parameter, {Report.N(size)} jobs a statement");
            var jobs = Enumerable.Range(0, size).Select(i => new JobInsertParams
            {
                Kind = NoOpArgs.Kind,
                ArgsJson = $$"""{"n":{{i}},"to":"user{{i}}@example.com","template":"welcome"}""",
                Queue = "dq6",
                State = JobState.Available,
                Priority = 1,
                MaxAttempts = 25,
            }).ToArray();

            await executor.JobInsertFastAsync(jobs, CancellationToken.None); // warm up both
            await InsertWithTableValuedParameterAsync(jobs);

            var calls = settings.Scale(size == 100 ? 300 : 40);
            var openJson = new List<double>(calls);
            var tvp = new List<double>(calls);
            var before = await db.SnapshotAsync();
            for (var i = 0; i < calls; i++)
            {
                var start = Stopwatch.GetTimestamp();
                await executor.JobInsertFastAsync(jobs, CancellationToken.None);
                openJson.Add(Stopwatch.GetElapsedTime(start).TotalMilliseconds);

                start = Stopwatch.GetTimestamp();
                await InsertWithTableValuedParameterAsync(jobs);
                tvp.Add(Stopwatch.GetElapsedTime(start).TotalMilliseconds);
            }

            var statements = (await db.SnapshotAsync()).Since(before);
            Report.Table(
                ["Parameter", "Jobs/s", "Call", "Server CPU µs / statement", "Logical reads / statement"],
                [
                    Row("OPENJSON (the driver)", openJson, statements.FirstOrDefault(s => s.Statement.EndsWith("AS i ORDER BY i.ordinal", StringComparison.Ordinal)).Cost),
                    Row("Table-valued parameter", tvp, statements.FirstOrDefault(s => s.Statement.EndsWith("AS r ORDER BY r.ordinal", StringComparison.Ordinal)).Cost),
                ]);

            IReadOnlyList<string> Row(string name, List<double> times, StatementCost? cost) =>
            [
                name,
                Report.N(size * times.Count / (times.Sum() / 1_000)),
                Report.Distribution(times),
                cost is null ? "?" : Report.N((double)cost.CpuMicroseconds / cost.Executions),
                cost is null ? "?" : Report.N((double)cost.Reads / cost.Executions),
            ];
        }
    }

    /// <summary>
    /// DQ-7: <c>state</c> as <c>varchar(16)</c> or <c>tinyint</c>. Two tables that differ only in that column, each with
    /// the job table's indexes that involve state, the same rows, and the driver's fetch and complete statements reduced
    /// to those columns. The arms alternate.
    /// </summary>
    public async Task StateColumnTypeAsync()
    {
        Report.Heading("DQ-7: state as varchar(16) or tinyint");
        var arms = new[]
        {
            new StateArm("varchar(16) COLLATE Latin1_General_100_BIN2", "dq7_varchar", "'available'", "'running'", "'completed'", "'scheduled', 'retryable'"),
            new StateArm("tinyint", "dq7_tinyint", "1", "64", "4", "128, 32"),
        };

        var history = settings.Scale(200_000);
        var available = settings.Scale(50_000);
        foreach (var arm in arms)
        {
            await db.ExecAsync($$"""
                CREATE TABLE dbo.{{arm.Table}} (
                    id           bigint IDENTITY(1,1) NOT NULL PRIMARY KEY CLUSTERED,
                    state        {{arm.Type}} NOT NULL,
                    attempt      smallint      NOT NULL,
                    attempted_at datetime2(6)  NULL,
                    finalized_at datetime2(6)  NULL,
                    scheduled_at datetime2(6)  NOT NULL,
                    priority     smallint      NOT NULL,
                    args         nvarchar(max) NOT NULL,
                    kind         nvarchar(128) COLLATE Latin1_General_100_BIN2 NOT NULL,
                    queue        nvarchar(128) COLLATE Latin1_General_100_BIN2 NOT NULL);
                CREATE INDEX {{arm.Table}}_fetch_idx ON dbo.{{arm.Table}} (queue, priority, scheduled_at, id) WHERE state = {{arm.Available}};
                CREATE INDEX {{arm.Table}}_schedule_idx ON dbo.{{arm.Table}} (scheduled_at) INCLUDE (queue) WHERE state IN ({{arm.Waiting}});
                CREATE INDEX {{arm.Table}}_running_idx ON dbo.{{arm.Table}} (attempted_at) WHERE state = {{arm.Running}};
                CREATE INDEX {{arm.Table}}_finalized_idx ON dbo.{{arm.Table}} (state, finalized_at) WHERE finalized_at IS NOT NULL;
                """);
            await db.ExecAsync($$"""
                WITH n AS (
                    SELECT TOP (@count) ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS i
                    FROM sys.all_objects a CROSS JOIN sys.all_objects b CROSS JOIN sys.all_objects c
                )
                INSERT INTO dbo.{{arm.Table}} (state, attempt, attempted_at, finalized_at, scheduled_at, priority, args, kind, queue)
                SELECT CASE WHEN i <= @history THEN {{arm.Completed}} ELSE {{arm.Available}} END,
                       CASE WHEN i <= @history THEN 1 ELSE 0 END,
                       CASE WHEN i <= @history THEN SYSUTCDATETIME() END,
                       CASE WHEN i <= @history THEN SYSUTCDATETIME() END,
                       DATEADD(minute, -1, SYSUTCDATETIME()), 1, N'{"n":' + CAST(i AS nvarchar(20)) + N'}', N'bench', N'busy'
                FROM n;
                """, ("@count", history + available), ("@history", history));
        }

        var fetches = settings.Scale(200);
        var before = await db.SnapshotAsync();
        for (var i = 0; i < fetches; i++)
        {
            foreach (var arm in arms)
            {
                await FetchAndCompleteAsync(arm);
            }
        }

        var statements = (await db.SnapshotAsync()).Since(before);
        var sizes = await IndexPagesAsync();
        Report.Table(
            ["state", "Row bytes, average", "Pages: table, fetch, running, finalized", "Fetch 25: reads, CPU µs", "Complete 25: reads, CPU µs"],
            arms.Select(arm =>
            {
                var fetch = statements.First(s => s.Statement.Contains($"dbo.{arm.Table} WITH (UPDLOCK", StringComparison.Ordinal)).Cost;
                var complete = statements.First(s => s.Statement.StartsWith($"UPDATE dbo.{arm.Table} SET state", StringComparison.Ordinal)).Cost;
                var pages = sizes.Where(s => s.Table == arm.Table).ToDictionary(s => s.Index, s => s.Pages);
                return (IReadOnlyList<string>)
                [
                    arm.Type.Split(' ')[0],
                    Report.N(sizes.First(s => s.Table == arm.Table && s.Index == "").RowBytes, 1),
                    $"{Report.N(pages[""])}, {Report.N(pages[$"{arm.Table}_fetch_idx"])}, {Report.N(pages[$"{arm.Table}_running_idx"])}, {Report.N(pages[$"{arm.Table}_finalized_idx"])}",
                    $"{Report.N((double)fetch.Reads / fetch.Executions, 1)}, {Report.N((double)fetch.CpuMicroseconds / fetch.Executions)}",
                    $"{Report.N((double)complete.Reads / complete.Executions, 1)}, {Report.N((double)complete.CpuMicroseconds / complete.Executions)}",
                ];
            }));
    }

    private async Task FetchAndCompleteAsync(StateArm arm)
    {
        await using var connection = await db.OpenAsync();
        var ids = new List<long>(25);
        await using (var fetch = new SqlCommand($"""
            SET TRANSACTION ISOLATION LEVEL READ COMMITTED;
            WITH locked AS (
                SELECT TOP (25) * FROM dbo.{arm.Table} WITH (UPDLOCK, READPAST, ROWLOCK, FORCESEEK ({arm.Table}_fetch_idx (queue)))
                WHERE state = {arm.Available} AND queue = N'busy' AND scheduled_at <= CAST(SYSUTCDATETIME() AS datetime2(6))
                ORDER BY priority, scheduled_at, id
            )
            UPDATE locked SET state = {arm.Running}, attempt = attempt + 1, attempted_at = SYSUTCDATETIME()
            OUTPUT inserted.id;
            """, connection))
        await using (var reader = await fetch.ExecuteReaderAsync())
        {
            while (await reader.ReadAsync())
            {
                ids.Add(reader.GetInt64(0));
            }
        }

        await using var complete = new SqlCommand(
            $"UPDATE dbo.{arm.Table} SET state = {arm.Completed}, finalized_at = SYSUTCDATETIME() " +
            $"WHERE id IN (SELECT CAST(value AS bigint) FROM OPENJSON(@ids)) AND state = {arm.Running};",
            connection);
        complete.Parameters.Add("@ids", SqlDbType.NVarChar, -1).Value = $"[{string.Join(',', ids)}]";
        await complete.ExecuteNonQueryAsync();
    }

    private async Task<List<(string Table, string Index, long Pages, double RowBytes)>> IndexPagesAsync()
    {
        await using var connection = await db.OpenAsync();
        await using var command = new SqlCommand("""
            SELECT OBJECT_NAME(ps.object_id), ISNULL(i.name, N''), SUM(ps.used_page_count),
                   SUM(ps.used_page_count) * 8192.0 / NULLIF(SUM(ps.row_count), 0)
            FROM sys.dm_db_partition_stats AS ps
            JOIN sys.indexes AS i ON i.object_id = ps.object_id AND i.index_id = ps.index_id
            WHERE OBJECT_NAME(ps.object_id) LIKE N'dq7[_]%'
            GROUP BY ps.object_id, i.name, i.index_id;
            """, connection);
        var sizes = new List<(string, string, long, double)>();
        await using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            var name = reader.GetString(1);
            sizes.Add((reader.GetString(0), name.StartsWith("PK_", StringComparison.Ordinal) ? "" : name, reader.GetInt64(2),
                reader.IsDBNull(3) ? 0 : Convert.ToDouble(reader.GetValue(3), System.Globalization.CultureInfo.InvariantCulture)));
        }

        return sizes;
    }

    private async Task InsertWithTableValuedParameterAsync(IReadOnlyList<JobInsertParams> jobs)
    {
        await using var connection = await db.OpenAsync();
        await using var command = new SqlCommand($"""
            SET TRANSACTION ISOLATION LEVEL READ COMMITTED;
            DECLARE @now datetime2(6) = SYSUTCDATETIME();
            INSERT INTO {BenchDatabase.Job} (args, created_at, kind, max_attempts, metadata, priority, queue, scheduled_at, state, tags)
            SELECT r.args, @now, r.kind, r.max_attempts, r.metadata, r.priority, r.queue, ISNULL(r.scheduled_at, @now), r.state, r.tags
            FROM @rows AS r
            ORDER BY r.ordinal;
            SELECT CAST(@@ROWCOUNT AS bigint);
            """, connection);
        command.Parameters.Add(new SqlParameter("@rows", SqlDbType.Structured) { TypeName = "dbo.bench_job_rows", Value = Records(jobs) });
        await command.ExecuteScalarAsync();
    }

    private static readonly SqlMetaData[] RowShape =
    [
        new("ordinal", SqlDbType.Int),
        new("args", SqlDbType.NVarChar, SqlMetaData.Max),
        new("kind", SqlDbType.NVarChar, 128),
        new("max_attempts", SqlDbType.SmallInt),
        new("metadata", SqlDbType.NVarChar, SqlMetaData.Max),
        new("priority", SqlDbType.SmallInt),
        new("queue", SqlDbType.NVarChar, 128),
        new("scheduled_at", SqlDbType.DateTime2, 0, 7),
        new("state", SqlDbType.VarChar, 16),
        new("tags", SqlDbType.NVarChar, SqlMetaData.Max),
    ];

    /// <summary>Streams the rows: SqlClient sends them without building a DataTable.</summary>
    private static IEnumerable<SqlDataRecord> Records(IReadOnlyList<JobInsertParams> jobs)
    {
        var record = new SqlDataRecord(RowShape);
        for (var i = 0; i < jobs.Count; i++)
        {
            var job = jobs[i];
            record.SetInt32(0, i);
            record.SetString(1, job.ArgsJson);
            record.SetString(2, job.Kind);
            record.SetInt16(3, (short)job.MaxAttempts);
            record.SetString(4, job.MetadataJson);
            record.SetInt16(5, (short)job.Priority);
            record.SetString(6, job.Queue);
            if (job.ScheduledAt is { } scheduledAt)
            {
                record.SetDateTime(7, scheduledAt.UtcDateTime);
            }
            else
            {
                record.SetDBNull(7);
            }

            record.SetString(8, "available");
            record.SetString(9, "[]");
            yield return record;
        }
    }

    private sealed record StateArm(string Type, string Table, string Available, string Running, string Completed, string Waiting);
}
