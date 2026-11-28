using System.Data;
using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;
using System.Text;
using KopiQueue.Drivers;
using KopiQueue.Migrations;
using Microsoft.Data.SqlClient;
using static KopiQueue.Drivers.StoredJob;

namespace KopiQueue.SqlServer;

/// <summary>The driver's SQL, loaded from embedded resources and rendered once for a schema and prefix.</summary>
internal sealed class SqlServerStatements
{
    private const string ResourceRoot = "kopi-sql/";

    public SqlServerStatements(string schemaQualifier, string tablePrefix)
    {
        string Load(string name)
        {
            using var stream = typeof(SqlServerStatements).Assembly.GetManifestResourceStream(ResourceRoot + name)
                ?? throw new InvalidOperationException($"Embedded SQL '{name}' is missing.");
            using var reader = new StreamReader(stream, Encoding.UTF8);
            return MigrationTemplate.Render(reader.ReadToEnd(), schemaQualifier, tablePrefix);
        }

        JobGetById = Load("job_get_by_id.sql");
        QueueGet = Load("queue_get.sql");
    }

    public string JobGetById { get; }

    public string QueueGet { get; }
}

/// <summary>
/// Runs the driver's statements either on a pooled connection per operation, or on the connection of a caller's
/// transaction (FR-INS-02). Times are UTC both ways, and the JSON-array columns are read as lists (design §4.4).
/// </summary>
/// <remarks>
/// Every session is checked before its first statement (<see cref="SqlServerSessionCheck"/>): pooled sessions once per
/// driver (<see cref="SqlServerSessionGate"/>), a caller's session once per executor, since the caller's code may have
/// changed its settings. The operations that later M5 tasks add throw <see cref="NotSupportedException"/> until then.
/// </remarks>
[SuppressMessage("Security", "CA2100:Review SQL queries for security vulnerabilities",
    Justification = "Statements are the driver's own embedded SQL, rendered with validated identifiers; values are parameters.")]
internal sealed class SqlServerExecutor(
    SqlServerStatements statements,
    SqlServerConnectionFactory connections,
    SqlServerSessionGate pooledSessions,
    SqlTransaction? transaction) : IKopiExecutor
{
    private bool _callerSessionChecked;

    public Task<IReadOnlyList<JobInsertResult>> JobInsertManyAsync(IReadOnlyList<JobInsertParams> jobs, CancellationToken cancellationToken) =>
        NotYet<IReadOnlyList<JobInsertResult>>("5.3");

    public Task<long> JobInsertFastAsync(IReadOnlyList<JobInsertParams> jobs, CancellationToken cancellationToken) =>
        NotYet<long>("5.5");

    public async Task<JobRow?> JobGetByIdAsync(long id, CancellationToken cancellationToken)
    {
        var rows = await RowsAsync(statements.JobGetById, [new SqlParameter("@id", SqlDbType.BigInt) { Value = id }], ReadJob, cancellationToken)
            .ConfigureAwait(false);
        return rows.Count == 0 ? null : rows[0];
    }

    public Task<IReadOnlyList<JobRow>> JobGetAvailableAsync(JobGetAvailableParams parameters, CancellationToken cancellationToken) =>
        NotYet<IReadOnlyList<JobRow>>("5.3");

    public Task<IReadOnlyList<JobRow>> JobSetStateIfRunningManyAsync(IReadOnlyList<JobSetStateParams> jobs, CancellationToken cancellationToken) =>
        NotYet<IReadOnlyList<JobRow>>("5.3");

    public Task<IReadOnlyList<JobScheduleResult>> JobScheduleAsync(JobScheduleParams parameters, CancellationToken cancellationToken) =>
        NotYet<IReadOnlyList<JobScheduleResult>>("5.6");

    public Task<JobListStuckResult> JobListStuckAsync(JobListStuckParams parameters, CancellationToken cancellationToken) =>
        NotYet<JobListStuckResult>("5.6");

    public Task<IReadOnlyList<JobRow>> JobRescueManyAsync(IReadOnlyList<JobRescueParams> jobs, CancellationToken cancellationToken) =>
        NotYet<IReadOnlyList<JobRow>>("5.6");

    public Task<IReadOnlyList<long>> JobDeleteFinalizedAsync(JobDeleteFinalizedParams parameters, CancellationToken cancellationToken) =>
        NotYet<IReadOnlyList<long>>("5.6");

    public Task<JobRow?> JobCancelAsync(long id, CancellationToken cancellationToken) =>
        NotYet<JobRow?>("5.6");

    public Task<IReadOnlyList<long>> JobListCancelAttemptedAsync(IReadOnlyList<long> ids, CancellationToken cancellationToken) =>
        NotYet<IReadOnlyList<long>>("5.7");

    public Task<IReadOnlyList<JobRow>> JobListAsync(JobListQuery query, CancellationToken cancellationToken) =>
        NotYet<IReadOnlyList<JobRow>>("5.6");

    public Task<JobDeleteResult?> JobDeleteAsync(long id, CancellationToken cancellationToken) =>
        NotYet<JobDeleteResult?>("5.6");

    public Task<IReadOnlyList<JobRow>> JobDeleteManyAsync(JobDeleteManyQuery query, CancellationToken cancellationToken) =>
        NotYet<IReadOnlyList<JobRow>>("5.6");

    public Task<JobRow?> JobRetryAsync(long id, CancellationToken cancellationToken) =>
        NotYet<JobRow?>("5.6");

    public Task<bool> LeaderAttemptElectAsync(LeaderElectParams parameters, CancellationToken cancellationToken) =>
        NotYet<bool>("5.6");

    public Task<bool> LeaderAttemptReelectAsync(LeaderElectParams parameters, CancellationToken cancellationToken) =>
        NotYet<bool>("5.6");

    public Task<bool> LeaderResignAsync(string leaderId, CancellationToken cancellationToken) =>
        NotYet<bool>("5.6");

    public Task<IReadOnlyList<string>> QueueDeleteExpiredAsync(QueueDeleteExpiredParams parameters, CancellationToken cancellationToken) =>
        NotYet<IReadOnlyList<string>>("5.6");

    public Task<IReadOnlyList<QueueRow>> QueueCreateOrSetUpdatedAtManyAsync(IReadOnlyList<string> names, CancellationToken cancellationToken) =>
        NotYet<IReadOnlyList<QueueRow>>("5.6");

    public async Task<QueueRow?> QueueGetAsync(string name, CancellationToken cancellationToken)
    {
        ArgumentException.ThrowIfNullOrEmpty(name);
        var rows = await RowsAsync(statements.QueueGet, [new SqlParameter("@name", SqlDbType.NVarChar, 128) { Value = name }], ReadQueue, cancellationToken)
            .ConfigureAwait(false);
        return rows.Count == 0 ? null : rows[0];
    }

    public Task<IReadOnlyList<QueueRow>> QueueListAsync(QueueListQuery query, CancellationToken cancellationToken) =>
        NotYet<IReadOnlyList<QueueRow>>("5.6");

    public Task<bool> QueuePauseAsync(string name, CancellationToken cancellationToken) =>
        NotYet<bool>("5.6");

    public Task<bool> QueueResumeAsync(string name, CancellationToken cancellationToken) =>
        NotYet<bool>("5.6");

    public Task<QueueRow?> QueueUpdateAsync(QueueUpdateParams parameters, CancellationToken cancellationToken) =>
        NotYet<QueueRow?>("5.6");

    /// <summary>Maps the column list shared by the driver's job statements (<c>job_get_by_id.sql</c>).</summary>
    internal static JobRow ReadJob(SqlDataReader reader) => new()
    {
        Id = reader.GetInt64(0),
        Kind = reader.GetString(1),
        Queue = reader.GetString(2),
        State = FromDatabase(reader.GetString(3)),
        Priority = reader.GetInt16(4),
        ArgsJson = reader.GetString(5),
        MetadataJson = reader.GetString(6),
        Tags = SqlServerValues.ReadStrings(reader, 7),
        Attempt = reader.GetInt16(8),
        MaxAttempts = reader.GetInt16(9),
        AttemptedAt = reader.IsDBNull(10) ? null : SqlServerValues.FromDatabase(reader.GetDateTime(10)),
        AttemptedBy = SqlServerValues.ReadStrings(reader, 11),
        Errors = SqlServerValues.ReadErrors(reader, 12),
        CreatedAt = SqlServerValues.FromDatabase(reader.GetDateTime(13)),
        ScheduledAt = SqlServerValues.FromDatabase(reader.GetDateTime(14)),
        FinalizedAt = reader.IsDBNull(15) ? null : SqlServerValues.FromDatabase(reader.GetDateTime(15)),
    };

    /// <summary>Maps the queue column list (<c>queue_get.sql</c>).</summary>
    internal static QueueRow ReadQueue(SqlDataReader reader) => new()
    {
        Name = reader.GetString(0),
        CreatedAt = SqlServerValues.FromDatabase(reader.GetDateTime(1)),
        UpdatedAt = SqlServerValues.FromDatabase(reader.GetDateTime(2)),
        PausedAt = reader.IsDBNull(3) ? null : SqlServerValues.FromDatabase(reader.GetDateTime(3)),
        MetadataJson = reader.GetString(4),
    };

    /// <summary>Runs <paramref name="sql"/> and reads every row it returns with <paramref name="read"/>.</summary>
    private async Task<List<T>> RowsAsync<T>(string sql, SqlParameter[] parameters, Func<SqlDataReader, T> read, CancellationToken cancellationToken)
    {
        var (connection, owned) = await OpenAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            var command = new SqlCommand(sql, connection, transaction);
            await using (command.ConfigureAwait(false))
            {
                command.Parameters.AddRange(parameters);
                var rows = new List<T>();
                var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
                await using (reader.ConfigureAwait(false))
                {
                    while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
                    {
                        rows.Add(read(reader));
                    }

                    // Drain later result sets so that their errors surface here.
                    while (await reader.NextResultAsync(cancellationToken).ConfigureAwait(false))
                    {
                    }
                }

                return rows;
            }
        }
        finally
        {
            if (owned)
            {
                await connection.DisposeAsync().ConfigureAwait(false);
            }
        }
    }

    /// <summary>
    /// The connection to run on: the caller's, or a newly opened pooled one that the caller of this method disposes.
    /// Either way its session has passed the session check.
    /// </summary>
    /// <exception cref="InvalidOperationException">The caller's transaction has completed.</exception>
    /// <exception cref="KopiQueueConfigurationException">The session's settings would break writes.</exception>
    private async Task<(SqlConnection Connection, bool Owned)> OpenAsync(CancellationToken cancellationToken)
    {
        if (transaction is null)
        {
            var pooled = await connections.OpenAsync(cancellationToken).ConfigureAwait(false);
            try
            {
                await pooledSessions.EnsureAsync(pooled, cancellationToken).ConfigureAwait(false);
                return (pooled, true);
            }
            catch
            {
                await pooled.DisposeAsync().ConfigureAwait(false);
                throw;
            }
        }

        // SqlClient clears the connection of a transaction that committed or rolled back, including one SQL Server rolled
        // back itself, for example after an error with XACT_ABORT on.
        var connection = transaction.Connection ?? throw new InvalidOperationException(
            "The caller's transaction has completed (committed or rolled back), so KopiQueue cannot run in it.");
        if (!_callerSessionChecked)
        {
            await SqlServerSessionCheck.EnsureAsync(connection, transaction, cancellationToken).ConfigureAwait(false);
            _callerSessionChecked = true;
        }

        return (connection, false);
    }

    private static Task<T> NotYet<T>(string task, [CallerMemberName] string operation = "") =>
        Task.FromException<T>(new NotSupportedException(
            $"The SQL Server driver does not support {operation} yet: it arrives with plan task {task} (milestone M5)."));
}
