using System.Data;
using System.Diagnostics.CodeAnalysis;
using System.Text;
using System.Text.RegularExpressions;
using KopiQueue.Migrations;
using Microsoft.Data.SqlClient;

namespace KopiQueue.SqlServer.Migrations;

/// <summary>
/// SQL Server side of the migration engine (FR-MIG-06). The lock is a session-owned application lock
/// (<c>sp_getapplock</c>) keyed by schema and prefix, so separate schemas and prefixes migrate independently. Each step
/// runs in its own READ COMMITTED transaction together with its version row: SQL Server DDL is transactional.
/// </summary>
/// <remarks>
/// <para>
/// A script may hold several batches separated by lines that contain only <c>GO</c>, as in sqlcmd and SSMS, for
/// statements that must start a batch (<c>CREATE VIEW</c>, <c>CREATE TRIGGER</c>) or that use a column added earlier
/// in the same script. The separator is recognised on any line, even inside a comment or string.
/// </para>
/// <para>Steps run without a command timeout, because DDL on a large table can take long. Cancel the run instead.</para>
/// </remarks>
[SuppressMessage("Security", "CA2100:Review SQL queries for security vulnerabilities",
    Justification = "Only validated, quoted identifiers and the library's own migration SQL are composed into commands.")]
internal sealed partial class SqlServerMigrationDriver : IMigrationDriver
{
    /// <summary>Default schema.</summary>
    public const string DefaultSchema = "dbo";

    /// <summary>Default table prefix. SQL Server's schema is KopiQueue's own, so it does not use River's prefix.</summary>
    public const string DefaultTablePrefix = "kopi_";

    /// <summary>The migration line that creates and evolves the job schema.</summary>
    public const string MainLineName = "main";

    private const int InvalidObjectName = 208;

    private static readonly Lazy<IReadOnlyDictionary<string, IReadOnlyList<Migration>>> EmbeddedLines =
        new(() => MigrationLoader.FromAssembly(typeof(SqlServerMigrationDriver).Assembly));

    private readonly SqlServerConnectionFactory _connections;

    /// <param name="connections">Where to migrate.</param>
    /// <param name="schema">Schema to migrate. It must exist.</param>
    /// <param name="tablePrefix">Table prefix.</param>
    /// <exception cref="ArgumentException">The schema name or table prefix is not allowed.</exception>
    public SqlServerMigrationDriver(SqlServerConnectionFactory connections, string schema = DefaultSchema, string tablePrefix = DefaultTablePrefix)
    {
        ArgumentNullException.ThrowIfNull(connections);
        _connections = connections;
        Schema = SchemaIdentifiers.ValidateSchema(schema ?? throw new ArgumentException("A schema is required on SQL Server.", nameof(schema)))!;
        TablePrefix = SchemaIdentifiers.ValidatePrefix(tablePrefix);
        SchemaQualifier = SqlServerIdentifiers.Quote(Schema) + ".";
        MigrationTable = SchemaQualifier + SqlServerIdentifiers.Quote(TablePrefix + "migration");
        LockResource = LockResourceFor(Schema, TablePrefix);
    }

    /// <summary>The schema.</summary>
    public string Schema { get; }

    /// <inheritdoc />
    public string SchemaQualifier { get; }

    /// <inheritdoc />
    public string TablePrefix { get; }

    /// <summary>Quoted, schema-qualified name of the migration table.</summary>
    internal string MigrationTable { get; }

    /// <summary>The <c>sp_getapplock</c> resource of the migration lock.</summary>
    internal string LockResource { get; }

    /// <summary>KopiQueue's SQL Server main line (design §4.4), embedded in this assembly.</summary>
    internal static IReadOnlyList<Migration> MainLine => EmbeddedLines.Value[MainLineName];

    /// <inheritdoc />
    /// <exception cref="KopiQueueConfigurationException">The session's settings would break writes to KopiQueue's tables.</exception>
    public async Task<IMigrationSession> OpenSessionAsync(CancellationToken cancellationToken)
    {
        var connection = await _connections.OpenAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            await SqlServerSessionCheck.EnsureAsync(connection, transaction: null, cancellationToken).ConfigureAwait(false);
            return new Session(connection, MigrationTable, LockResource);
        }
        catch
        {
            await connection.DisposeAsync().ConfigureAwait(false);
            throw;
        }
    }

    /// <summary>The application lock resource for a schema and prefix: <c>kopi_migrate:&lt;schema&gt;.&lt;prefix&gt;</c>, at most 109 characters.</summary>
    internal static string LockResourceFor(string schema, string tablePrefix) => $"kopi_migrate:{schema}.{tablePrefix}";

    /// <summary>Splits a script into batches at lines that contain only <c>GO</c> (any case). Batches of only white space are dropped.</summary>
    internal static IReadOnlyList<string> SplitBatches(string sql)
    {
        ArgumentNullException.ThrowIfNull(sql);
        var batches = new List<string>();
        var current = new StringBuilder();
        using var reader = new StringReader(sql);
        while (reader.ReadLine() is { } line)
        {
            if (BatchSeparator().IsMatch(line))
            {
                Flush();
            }
            else
            {
                current.Append(line).Append('\n');
            }
        }

        Flush();
        return batches;

        void Flush()
        {
            var batch = current.ToString();
            if (!string.IsNullOrWhiteSpace(batch))
            {
                batches.Add(batch);
            }

            current.Clear();
        }
    }

    [GeneratedRegex(@"^\s*GO\s*$", RegexOptions.IgnoreCase | RegexOptions.CultureInvariant)]
    private static partial Regex BatchSeparator();

    private sealed class Session(SqlConnection connection, string migrationTable, string lockResource) : IMigrationSession
    {
        private bool _locked;

        public async Task<bool> TryAcquireLockAsync(CancellationToken cancellationToken)
        {
            // Session-owned, so the lock lasts across every step's transaction. Timeout 0: the runner polls.
            var command = new SqlCommand(
                """
                DECLARE @result int;
                EXEC @result = sys.sp_getapplock @Resource = @resource, @LockMode = 'Exclusive', @LockOwner = 'Session', @LockTimeout = 0;
                SELECT @result;
                """, connection);
            await using (command.ConfigureAwait(false))
            {
                command.Parameters.Add(ResourceParameter());
                var result = (int)(await command.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false))!;
                _locked = result switch
                {
                    0 or 1 => true,   // granted (1: after waiting, which timeout 0 rules out)
                    -1 => false,      // held by another session
                    _ => throw new InvalidOperationException($"sp_getapplock failed with result {result} for the migration lock '{lockResource}'."),
                };
                return _locked;
            }
        }

        public async Task ReleaseLockAsync(CancellationToken cancellationToken)
        {
            if (!_locked)
            {
                return;
            }

            var command = new SqlCommand(
                "EXEC sys.sp_releaseapplock @Resource = @resource, @LockOwner = 'Session';", connection);
            await using (command.ConfigureAwait(false))
            {
                command.Parameters.Add(ResourceParameter());
                await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
                _locked = false;
            }
        }

        public async Task<IReadOnlyList<AppliedVersion>> GetAppliedVersionsAsync(string line, CancellationToken cancellationToken)
        {
            // Before the main line's first version there is no migration table: then no versions are applied.
            var command = new SqlCommand(
                $"""
                IF OBJECT_ID(@table, N'U') IS NOT NULL
                  SELECT version, created_at FROM {migrationTable} WHERE line = @line ORDER BY version;
                """, connection);
            await using (command.ConfigureAwait(false))
            {
                command.Parameters.Add(new SqlParameter("@table", SqlDbType.NVarChar, 512) { Value = migrationTable });
                command.Parameters.Add(LineParameter(line));
                var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
                await using (reader.ConfigureAwait(false))
                {
                    var versions = new List<AppliedVersion>();
                    while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
                    {
                        var appliedAt = DateTime.SpecifyKind(reader.GetDateTime(1), DateTimeKind.Utc);
                        versions.Add(new AppliedVersion(checked((int)reader.GetInt64(0)), new DateTimeOffset(appliedAt)));
                    }

                    return versions;
                }
            }
        }

        public async Task ExecuteAsync(MigrationStep step, CancellationToken cancellationToken)
        {
            // READ COMMITTED explicitly: an isolation level left on a pooled session (SNAPSHOT) would break some DDL.
            var transaction = step.InTransaction
                ? (SqlTransaction)await connection.BeginTransactionAsync(IsolationLevel.ReadCommitted, cancellationToken).ConfigureAwait(false)
                : null;
            try
            {
                if (step.Direction == MigrationDirection.Down)
                {
                    await DeleteVersionAsync(step, transaction, cancellationToken).ConfigureAwait(false);
                }

                foreach (var batch in SplitBatches(step.Sql))
                {
                    var command = new SqlCommand(batch, connection, transaction) { CommandTimeout = 0 };
                    await using (command.ConfigureAwait(false))
                    {
                        await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
                    }
                }

                if (step.Direction == MigrationDirection.Up)
                {
                    await InsertVersionAsync(step, transaction, cancellationToken).ConfigureAwait(false);
                }

                if (transaction is not null)
                {
                    await transaction.CommitAsync(cancellationToken).ConfigureAwait(false);
                }
            }
            finally
            {
                if (transaction is not null)
                {
                    await transaction.DisposeAsync().ConfigureAwait(false); // rolls back unless committed
                }
            }
        }

        public async ValueTask DisposeAsync()
        {
            try
            {
                // A pooled connection keeps its session, and with it a session-owned lock: release it before returning it.
                await ReleaseLockAsync(CancellationToken.None).ConfigureAwait(false);
            }
            catch (SqlException)
            {
                // A broken connection is discarded by the pool, and its session's locks with it.
            }

            await connection.DisposeAsync().ConfigureAwait(false);
        }

        private async Task InsertVersionAsync(MigrationStep step, SqlTransaction? transaction, CancellationToken cancellationToken)
        {
            var command = new SqlCommand($"INSERT INTO {migrationTable} (line, version) VALUES (@line, @version);", connection, transaction);
            await using (command.ConfigureAwait(false))
            {
                command.Parameters.Add(LineParameter(step.Line));
                command.Parameters.Add(new SqlParameter("@version", SqlDbType.BigInt) { Value = (long)step.Version });
                try
                {
                    await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
                }
                catch (SqlException ex) when (ex.Number == InvalidObjectName)
                {
                    throw new InvalidOperationException(
                        $"The migration table {migrationTable} does not exist after migration {step.Line} {step.Version}. " +
                        $"It is created by the first migration of the {MainLineName} line, so apply that line first.", ex);
                }
            }
        }

        private async Task DeleteVersionAsync(MigrationStep step, SqlTransaction? transaction, CancellationToken cancellationToken)
        {
            var command = new SqlCommand($"DELETE FROM {migrationTable} WHERE line = @line AND version = @version;", connection, transaction);
            await using (command.ConfigureAwait(false))
            {
                command.Parameters.Add(LineParameter(step.Line));
                command.Parameters.Add(new SqlParameter("@version", SqlDbType.BigInt) { Value = (long)step.Version });
                if (await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false) != 1)
                {
                    throw new InvalidOperationException(
                        $"Migration {step.Line} {step.Version} is not recorded in {migrationTable}, so it cannot be reverted.");
                }
            }
        }

        private SqlParameter ResourceParameter() => new("@resource", SqlDbType.NVarChar, 255) { Value = lockResource };

        private static SqlParameter LineParameter(string line) => new("@line", SqlDbType.NVarChar, 128) { Value = line };
    }
}
