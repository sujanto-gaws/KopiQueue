using System.Data;
using Microsoft.Data.SqlClient;

namespace SqlServerSpikes;

/// <summary>Connection helpers, the spike schema (design §4.4) and the candidate statements (design §8.1).</summary>
internal static class Db
{
    public static readonly string Server =
        Environment.GetEnvironmentVariable("KOPI_SPIKE_MSSQL")
        ?? "Server=localhost;Integrated Security=true;TrustServerCertificate=true;Max Pool Size=400";

    public const string RcDb = "kopi_spike_rc";      // READ_COMMITTED_SNAPSHOT OFF (SQL Server default)
    public const string RcsiDb = "kopi_spike_rcsi";  // READ_COMMITTED_SNAPSHOT ON  (Azure SQL Database default)

    // Default unique states: available, completed, pending, retryable, running, scheduled (FR-UNQ-04).
    public const byte DefaultUniqueStates = 1 | 4 | 16 | 32 | 64 | 128;

    public static string For(string db) =>
        new SqlConnectionStringBuilder(Server) { InitialCatalog = db }.ConnectionString;

    public static async Task<SqlConnection> OpenAsync(string db)
    {
        var c = new SqlConnection(For(db));
        await c.OpenAsync();
        return c;
    }

    public static SqlCommand Cmd(SqlConnection c, string sql, SqlTransaction? tx = null, int timeout = 300) =>
        new(sql, c, tx) { CommandTimeout = timeout };

    public static async Task<int> ExecAsync(SqlConnection c, string sql, SqlTransaction? tx = null, int timeout = 300)
    {
        await using var cmd = Cmd(c, sql, tx, timeout);
        return await cmd.ExecuteNonQueryAsync();
    }

    public static async Task<T> ScalarAsync<T>(SqlConnection c, string sql, SqlTransaction? tx = null)
    {
        await using var cmd = Cmd(c, sql, tx);
        return (T)(await cmd.ExecuteScalarAsync())!;
    }

    public static async Task CreateDatabaseAsync(string db, bool rcsi)
    {
        await using (var master = new SqlConnection(Server))
        {
            await master.OpenAsync();
            await ExecAsync(master, DropSql(db) + $"CREATE DATABASE [{db}];");
            await ExecAsync(master, $"ALTER DATABASE [{db}] SET ALLOW_SNAPSHOT_ISOLATION ON;");
            if (rcsi)
            {
                await ExecAsync(master, $"ALTER DATABASE [{db}] SET READ_COMMITTED_SNAPSHOT ON WITH ROLLBACK IMMEDIATE;");
            }
        }

        await using var c = await OpenAsync(db);
        await ExecAsync(c, Schema);
    }

    public static async Task DropDatabaseAsync(string db)
    {
        SqlConnection.ClearAllPools();
        await using var master = new SqlConnection(Server);
        await master.OpenAsync();
        await ExecAsync(master, DropSql(db));
    }

    private static string DropSql(string db) =>
        $"IF DB_ID(N'{db}') IS NOT NULL BEGIN ALTER DATABASE [{db}] SET SINGLE_USER WITH ROLLBACK IMMEDIATE; DROP DATABASE [{db}]; END;";

    public static async Task TruncateAsync(string db)
    {
        await using var c = await OpenAsync(db);
        await ExecAsync(c, "TRUNCATE TABLE dbo.kopi_job;");
    }

    /// <summary>Set-based seed of <paramref name="n"/> jobs in one state and queue.</summary>
    public static async Task SeedAsync(SqlConnection c, int n, string state, string queue, SqlTransaction? tx = null)
    {
        await using var cmd = Cmd(c, SeedSql, tx);
        cmd.Parameters.Add("@n", SqlDbType.Int).Value = n;
        cmd.Parameters.Add("@state", SqlDbType.VarChar, 16).Value = state;
        cmd.Parameters.Add("@queue", SqlDbType.NVarChar, 128).Value = queue;
        await cmd.ExecuteNonQueryAsync();
    }

    public static async Task SeedAsync(string db, int n, string state, string queue)
    {
        await using var c = await OpenAsync(db);
        await SeedAsync(c, n, state, queue);
    }

    public static async Task<List<long>> FetchAsync(
        SqlConnection c, string queue, int limit, string clientId,
        SqlTransaction? tx = null, int timeout = 30, string prefix = "", string tag = "")
    {
        await using var cmd = Cmd(c, prefix + FetchSql.Replace("/*tag*/", tag, StringComparison.Ordinal), tx, timeout);
        cmd.Parameters.Add("@limit", SqlDbType.Int).Value = limit;
        cmd.Parameters.Add("@queue", SqlDbType.NVarChar, 128).Value = queue;
        cmd.Parameters.Add("@client_id", SqlDbType.NVarChar, 128).Value = clientId;
        var ids = new List<long>(limit);
        await using var r = await cmd.ExecuteReaderAsync();
        while (await r.ReadAsync())
        {
            ids.Add(r.GetInt64(0));
        }
        return ids;
    }

    public static async Task<int> CompleteAsync(SqlConnection c, IReadOnlyCollection<long> ids, SqlTransaction? tx = null)
    {
        await using var cmd = Cmd(c, CompleteSql, tx);
        cmd.Parameters.Add("@batch", SqlDbType.NVarChar, -1).Value =
            "[" + string.Join(',', ids.Select(id => $$"""{"id":{{id}},"state":"completed"}""")) + "]";
        return await cmd.ExecuteNonQueryAsync();
    }

    /// <summary>Lock summary for the current session in the current database, e.g. "KEY/X=4900 OBJECT/IX=1".</summary>
    public static async Task<(string Summary, bool Escalated)> LocksAsync(SqlConnection c, SqlTransaction? tx)
    {
        await using var cmd = Cmd(c, """
            SELECT resource_type, request_mode, COUNT(*)
            FROM sys.dm_tran_locks
            WHERE request_session_id = @@SPID AND resource_database_id = DB_ID()
            GROUP BY resource_type, request_mode ORDER BY 1, 2;
            """, tx);
        var parts = new List<string>();
        var escalated = false;
        await using var r = await cmd.ExecuteReaderAsync();
        while (await r.ReadAsync())
        {
            var type = r.GetString(0).Trim();
            var mode = r.GetString(1);
            parts.Add($"{type}/{mode}={r.GetInt32(2)}");
            escalated |= type == "OBJECT" && mode is "X" or "S" or "U" or "SIX";
        }
        return (string.Join(' ', parts), escalated);
    }

    public const string SeedSql = """
        INSERT INTO dbo.kopi_job (state, attempt, max_attempts, attempted_at, finalized_at, args, kind, queue)
        SELECT TOP (@n) @state,
               CASE WHEN @state IN ('available', 'scheduled') THEN 0 ELSE 1 END,
               25,
               CASE WHEN @state IN ('available', 'scheduled') THEN NULL ELSE SYSUTCDATETIME() END,
               CASE WHEN @state IN ('completed', 'cancelled', 'discarded') THEN SYSUTCDATETIME() END,
               N'{"spike":true}', N'spike', @queue
        FROM sys.all_objects a CROSS JOIN sys.all_objects b;
        """;

    // Design §8.1 fetch: SKIP LOCKED -> UPDLOCK, READPAST, ROWLOCK on an updatable CTE.
    // FORCESEEK added after 0.9d: without it the optimizer flips to a clustered scan (and escalates) at larger limits.
    public const string FetchSql = """
        /*tag*/
        WITH locked AS (
          SELECT TOP (@limit) id, state, attempt, attempted_at, attempted_by
          FROM dbo.kopi_job WITH (UPDLOCK, READPAST, ROWLOCK, FORCESEEK(kopi_job_fetch_idx(queue)))
          WHERE state = 'available' AND queue = @queue AND scheduled_at <= SYSUTCDATETIME()
          ORDER BY priority, scheduled_at, id
        )
        UPDATE locked
        SET state = 'running',
            attempt = attempt + 1,
            attempted_at = SYSUTCDATETIME(),
            attempted_by = JSON_MODIFY(ISNULL(attempted_by, N'[]'), 'append $', @client_id)
        OUTPUT inserted.id;
        """;

    // Design §8.1 batch complete via OPENJSON.
    public const string CompleteSql = """
        UPDATE j
        SET state = u.state, finalized_at = SYSUTCDATETIME()
        FROM dbo.kopi_job AS j WITH (ROWLOCK)
        JOIN OPENJSON(@batch) WITH (id bigint '$.id', state varchar(16) '$.state') AS u ON j.id = u.id
        WHERE j.state = 'running';
        """;

    // Design §4.4, kopi_job only (the other tables are not exercised by these spikes).
    public const string Schema = """
        CREATE TABLE dbo.kopi_job (
          id            bigint IDENTITY(1,1) NOT NULL CONSTRAINT kopi_job_pk PRIMARY KEY CLUSTERED,
          state         varchar(16)    NOT NULL CONSTRAINT kopi_job_state_df DEFAULT 'available',
          attempt       smallint       NOT NULL DEFAULT 0,
          max_attempts  smallint       NOT NULL,
          attempted_at  datetime2(6)   NULL,
          created_at    datetime2(6)   NOT NULL DEFAULT SYSUTCDATETIME(),
          finalized_at  datetime2(6)   NULL,
          scheduled_at  datetime2(6)   NOT NULL DEFAULT SYSUTCDATETIME(),
          priority      smallint       NOT NULL DEFAULT 1,
          args          nvarchar(max)  NOT NULL,
          attempted_by  nvarchar(max)  NULL,
          errors        nvarchar(max)  NULL,
          kind          nvarchar(128)  NOT NULL,
          metadata      nvarchar(max)  NOT NULL DEFAULT N'{}',
          queue         nvarchar(128)  NOT NULL DEFAULT N'default',
          tags          nvarchar(max)  NOT NULL DEFAULT N'[]',
          unique_key    varbinary(32)  NULL,
          unique_states tinyint        NULL,
          unique_slot AS CAST(CASE
              WHEN unique_key IS NOT NULL AND unique_states IS NOT NULL
               AND (unique_states & CASE state
                     WHEN 'available' THEN 1   WHEN 'cancelled' THEN 2  WHEN 'completed' THEN 4
                     WHEN 'discarded' THEN 8   WHEN 'pending'   THEN 16 WHEN 'retryable' THEN 32
                     WHEN 'running'   THEN 64  WHEN 'scheduled' THEN 128 END) <> 0
              THEN 0 ELSE id END AS bigint) PERSISTED,
          CONSTRAINT kopi_job_state_valid CHECK (state IN
            ('available','cancelled','completed','discarded','pending','retryable','running','scheduled')),
          CONSTRAINT kopi_job_finalized_or_finalized_at_null CHECK (
            (finalized_at IS NULL     AND state NOT IN ('cancelled','completed','discarded')) OR
            (finalized_at IS NOT NULL AND state     IN ('cancelled','completed','discarded'))),
          CONSTRAINT kopi_job_max_attempts_is_positive CHECK (max_attempts > 0),
          CONSTRAINT kopi_job_priority_in_range        CHECK (priority BETWEEN 1 AND 4),
          CONSTRAINT kopi_job_queue_length CHECK (LEN(queue) BETWEEN 1 AND 128),
          CONSTRAINT kopi_job_kind_length  CHECK (LEN(kind)  BETWEEN 1 AND 128),
          CONSTRAINT kopi_job_args_json     CHECK (ISJSON(args) = 1),
          CONSTRAINT kopi_job_metadata_json CHECK (ISJSON(metadata) = 1)
        );
        CREATE INDEX kopi_job_fetch_idx     ON dbo.kopi_job (queue, priority, scheduled_at, id) WHERE state = 'available';
        CREATE INDEX kopi_job_schedule_idx  ON dbo.kopi_job (scheduled_at) INCLUDE (queue) WHERE state IN ('scheduled','retryable');
        CREATE INDEX kopi_job_running_idx   ON dbo.kopi_job (attempted_at) WHERE state = 'running';
        CREATE INDEX kopi_job_finalized_idx ON dbo.kopi_job (state, finalized_at) WHERE finalized_at IS NOT NULL;
        CREATE INDEX kopi_job_kind_idx      ON dbo.kopi_job (kind);
        CREATE UNIQUE INDEX kopi_job_unique_idx ON dbo.kopi_job (unique_key, unique_slot);
        """;
}

internal static class Report
{
    public static void Section(string title) => Console.WriteLine($"\n## {title}\n");

    public static void Line(string label, string value) => Console.WriteLine($"- **{label}:** {value}");

    public static double Percentile(IReadOnlyList<double> sorted, double p) =>
        sorted.Count == 0 ? double.NaN : sorted[(int)Math.Min(sorted.Count - 1, Math.Ceiling(p * sorted.Count) - 1)];

    public static string Dist(List<double> values, string unit = "ms")
    {
        values.Sort();
        return $"n={values.Count} p50={Percentile(values, 0.50):F1}{unit} p95={Percentile(values, 0.95):F1}{unit} " +
               $"p99={Percentile(values, 0.99):F1}{unit} max={values[^1]:F1}{unit}";
    }

    public static string Err(Exception e) => e is SqlException s
        ? $"SqlException {s.Number}: {Trim(s.Message)}"
        : $"{e.GetType().Name}: {Trim(e.Message)}";

    private static string Trim(string m) => m.Length > 140 ? m[..140] + "…" : m.ReplaceLineEndings(" ");
}
