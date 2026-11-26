using System.Buffers.Binary;
using System.Diagnostics.CodeAnalysis;
using System.Security.Cryptography;
using System.Text;
using KopiQueue.Migrations;
using Npgsql;

namespace KopiQueue.Npgsql.Migrations;

/// <summary>
/// PostgreSQL side of the migration engine. The lock is a session-level advisory lock keyed by schema and prefix,
/// and every step runs in its own transaction: PostgreSQL DDL is transactional (FR-MIG-06).
/// </summary>
/// <remarks>
/// The migration table has two shapes, both River-compatible: versions 1-4 of the main line use
/// <c>(id, created_at, version)</c>, which can only record the main line; version 5 replaces it with
/// <c>(line, version, created_at)</c>. The session detects the shape whenever it reads or writes a version.
/// </remarks>
[SuppressMessage("Security", "CA2100:Review SQL queries for security vulnerabilities",
    Justification = "Only validated, quoted identifiers and the library's own migration SQL are composed into commands.")]
internal sealed class NpgsqlMigrationDriver : IMigrationDriver
{
    /// <summary>Default table prefix: River-compatible (ADR-002).</summary>
    public const string DefaultTablePrefix = "river_";

    /// <summary>The migration line that creates and evolves the job schema.</summary>
    public const string MainLineName = "main";

    private static readonly Lazy<IReadOnlyDictionary<string, IReadOnlyList<Migration>>> EmbeddedLines =
        new(() => MigrationLoader.FromAssembly(typeof(NpgsqlMigrationDriver).Assembly));

    private readonly NpgsqlDataSource _dataSource;
    private readonly string _migrationTable;
    private readonly long _lockKey;

    /// <param name="dataSource">Where to migrate.</param>
    /// <param name="schema">Schema to migrate, or <c>null</c> for the connection's <c>search_path</c>.</param>
    /// <param name="tablePrefix">Table prefix; <c>river_</c> keeps the schema River-compatible.</param>
    public NpgsqlMigrationDriver(NpgsqlDataSource dataSource, string? schema = null, string tablePrefix = DefaultTablePrefix)
    {
        ArgumentNullException.ThrowIfNull(dataSource);
        _dataSource = dataSource;
        schema = SchemaIdentifiers.ValidateSchema(schema);
        TablePrefix = SchemaIdentifiers.ValidatePrefix(tablePrefix);
        SchemaQualifier = schema is null ? string.Empty : QuoteIdentifier(schema) + ".";
        _migrationTable = SchemaQualifier + QuoteIdentifier(TablePrefix + "migration");
        _lockKey = LockKey(schema, TablePrefix);
    }

    /// <inheritdoc />
    public string SchemaQualifier { get; }

    /// <inheritdoc />
    public string TablePrefix { get; }

    /// <summary>Quoted, schema-qualified name of the migration table.</summary>
    internal string MigrationTable => _migrationTable;

    /// <summary>The advisory lock key used by this driver.</summary>
    internal long AdvisoryLockKey => _lockKey;

    /// <summary>
    /// The River-compatible main line, versions 1-8 (River v0.51.2, design §4.1), embedded in this assembly.
    /// </summary>
    internal static IReadOnlyList<Migration> MainLine => EmbeddedLines.Value[MainLineName];

    /// <inheritdoc />
    public async Task<IMigrationSession> OpenSessionAsync(CancellationToken cancellationToken)
    {
        var connection = await _dataSource.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        return new Session(connection, _migrationTable, _lockKey);
    }

    internal static string QuoteIdentifier(string identifier) =>
        "\"" + identifier.Replace("\"", "\"\"", StringComparison.Ordinal) + "\"";

    /// <summary>A stable 64-bit key for <c>pg_advisory_lock</c>, so two schemas (or prefixes) migrate independently.</summary>
    internal static long LockKey(string? schema, string tablePrefix)
    {
        Span<byte> hash = stackalloc byte[SHA256.HashSizeInBytes];
        SHA256.HashData(Encoding.UTF8.GetBytes($"kopi_migrate:{schema}.{tablePrefix}"), hash);
        return BinaryPrimitives.ReadInt64BigEndian(hash);
    }

    private sealed class Session(NpgsqlConnection connection, string migrationTable, long lockKey) : IMigrationSession
    {
        private bool _locked;

        public async Task<bool> TryAcquireLockAsync(CancellationToken cancellationToken)
        {
            var command = new NpgsqlCommand("SELECT pg_try_advisory_lock($1)", connection) { Parameters = { new() { Value = lockKey } } };
            await using (command.ConfigureAwait(false))
            {
                _locked = await command.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false) is true;
                return _locked;
            }
        }

        public async Task ReleaseLockAsync(CancellationToken cancellationToken)
        {
            if (!_locked)
            {
                return;
            }

            var command = new NpgsqlCommand("SELECT pg_advisory_unlock($1)", connection) { Parameters = { new() { Value = lockKey } } };
            await using (command.ConfigureAwait(false))
            {
                await command.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false);
                _locked = false;
            }
        }

        public async Task<IReadOnlyList<AppliedVersion>> GetAppliedVersionsAsync(string line, CancellationToken cancellationToken)
        {
            var shape = await GetTableShapeAsync(null, cancellationToken).ConfigureAwait(false);
            if (shape == TableShape.Missing || (shape == TableShape.Original && line != MainLineName))
            {
                return [];
            }

            var command = shape == TableShape.Original
                ? new NpgsqlCommand($"SELECT version, created_at FROM {migrationTable} ORDER BY version", connection)
                : new NpgsqlCommand($"SELECT version, created_at FROM {migrationTable} WHERE line = $1 ORDER BY version", connection)
                {
                    Parameters = { new() { Value = line } },
                };
            await using (command.ConfigureAwait(false))
            {
                var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
                await using (reader.ConfigureAwait(false))
                {
                    var versions = new List<AppliedVersion>();
                    while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
                    {
                        versions.Add(new AppliedVersion(checked((int)reader.GetInt64(0)), reader.GetFieldValue<DateTimeOffset>(1)));
                    }

                    return versions;
                }
            }
        }

        public async Task ExecuteAsync(MigrationStep step, CancellationToken cancellationToken)
        {
            var transaction = step.InTransaction
                ? await connection.BeginTransactionAsync(cancellationToken).ConfigureAwait(false)
                : null;
            try
            {
                if (step.Direction == MigrationDirection.Down)
                {
                    await DeleteVersionAsync(step, transaction, cancellationToken).ConfigureAwait(false);
                }

                var command = new NpgsqlCommand(step.Sql, connection, transaction);
                await using (command.ConfigureAwait(false))
                {
                    await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
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
                // Npgsql resets a pooled connection only when it is reused, so never return one holding the lock.
                await ReleaseLockAsync(CancellationToken.None).ConfigureAwait(false);
            }
            catch (NpgsqlException)
            {
                // A broken connection has already lost its session lock on the server.
            }

            await connection.DisposeAsync().ConfigureAwait(false);
        }

        private async Task InsertVersionAsync(MigrationStep step, NpgsqlTransaction? transaction, CancellationToken cancellationToken)
        {
            var shape = await GetTableShapeAsync(transaction, cancellationToken).ConfigureAwait(false);
            if (shape == TableShape.Missing)
            {
                throw new InvalidOperationException(
                    $"The migration table {migrationTable} does not exist after migration {step.Line} {step.Version}. " +
                    "It is created by the first migration of the main line, so apply that line first.");
            }

            if (shape == TableShape.Original && step.Line != MainLineName)
            {
                throw OriginalShapeHoldsOnlyMain(step);
            }

            var command = shape == TableShape.Original
                ? new NpgsqlCommand($"INSERT INTO {migrationTable} (version) VALUES ($1)", connection, transaction)
                {
                    Parameters = { new() { Value = (long)step.Version } },
                }
                : new NpgsqlCommand($"INSERT INTO {migrationTable} (line, version) VALUES ($1, $2)", connection, transaction)
                {
                    Parameters = { new() { Value = step.Line }, new() { Value = (long)step.Version } },
                };
            await using (command.ConfigureAwait(false))
            {
                await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
            }
        }

        private async Task DeleteVersionAsync(MigrationStep step, NpgsqlTransaction? transaction, CancellationToken cancellationToken)
        {
            var shape = await GetTableShapeAsync(transaction, cancellationToken).ConfigureAwait(false);
            if (shape == TableShape.Original && step.Line != MainLineName)
            {
                throw OriginalShapeHoldsOnlyMain(step);
            }

            var command = shape == TableShape.Original
                ? new NpgsqlCommand($"DELETE FROM {migrationTable} WHERE version = $1", connection, transaction)
                {
                    Parameters = { new() { Value = (long)step.Version } },
                }
                : new NpgsqlCommand($"DELETE FROM {migrationTable} WHERE line = $1 AND version = $2", connection, transaction)
                {
                    Parameters = { new() { Value = step.Line }, new() { Value = (long)step.Version } },
                };
            await using (command.ConfigureAwait(false))
            {
                if (await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false) != 1)
                {
                    throw new InvalidOperationException(
                        $"Migration {step.Line} {step.Version} is not recorded in {migrationTable}, so it cannot be reverted.");
                }
            }
        }

        private async Task<TableShape> GetTableShapeAsync(NpgsqlTransaction? transaction, CancellationToken cancellationToken)
        {
            // No row when the table does not exist; otherwise whether it has a "line" column.
            var command = new NpgsqlCommand(
                """
                SELECT EXISTS (SELECT 1 FROM pg_catalog.pg_attribute
                               WHERE attrelid = t.oid AND attname = 'line' AND attnum > 0 AND NOT attisdropped)
                FROM (SELECT pg_catalog.to_regclass($1) AS oid) t
                WHERE t.oid IS NOT NULL
                """, connection, transaction)
            {
                Parameters = { new() { Value = migrationTable } },
            };
            await using (command.ConfigureAwait(false))
            {
                return await command.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false) switch
                {
                    true => TableShape.Lines,
                    false => TableShape.Original,
                    _ => TableShape.Missing,
                };
            }
        }

        private InvalidOperationException OriginalShapeHoldsOnlyMain(MigrationStep step) => new(
            $"Migration line '{step.Line}' cannot be recorded yet: {migrationTable} has its original shape, which holds only " +
            $"the '{MainLineName}' line. Migrate '{MainLineName}' to version 5 or later first.");
    }

    private enum TableShape
    {
        Missing,
        Original,
        Lines,
    }
}
