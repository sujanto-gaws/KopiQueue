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
[SuppressMessage("Security", "CA2100:Review SQL queries for security vulnerabilities",
    Justification = "Only validated, quoted identifiers and the library's own migration SQL are composed into commands.")]
internal sealed class NpgsqlMigrationDriver : IMigrationDriver
{
    /// <summary>Default table prefix: River-compatible (ADR-002).</summary>
    public const string DefaultTablePrefix = "river_";

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

        public async Task<IReadOnlyList<int>> GetAppliedVersionsAsync(string line, CancellationToken cancellationToken)
        {
            if (!await MigrationTableExistsAsync(null, cancellationToken).ConfigureAwait(false))
            {
                return [];
            }

            var command = new NpgsqlCommand($"SELECT version FROM {migrationTable} WHERE line = $1 ORDER BY version", connection)
            {
                Parameters = { new() { Value = line } },
            };
            await using (command.ConfigureAwait(false))
            {
                var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
                await using (reader.ConfigureAwait(false))
                {
                    var versions = new List<int>();
                    while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
                    {
                        versions.Add(checked((int)reader.GetInt64(0)));
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
            if (!await MigrationTableExistsAsync(transaction, cancellationToken).ConfigureAwait(false))
            {
                throw new InvalidOperationException(
                    $"The migration table {migrationTable} does not exist after migration {step.Line} {step.Version}. " +
                    "It is created by the first migration of the main line, so apply that line first.");
            }

            var command = new NpgsqlCommand($"INSERT INTO {migrationTable} (line, version) VALUES ($1, $2)", connection, transaction)
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
            var command = new NpgsqlCommand($"DELETE FROM {migrationTable} WHERE line = $1 AND version = $2", connection, transaction)
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

        private async Task<bool> MigrationTableExistsAsync(NpgsqlTransaction? transaction, CancellationToken cancellationToken)
        {
            var command = new NpgsqlCommand("SELECT to_regclass($1) IS NOT NULL", connection, transaction)
            {
                Parameters = { new() { Value = migrationTable } },
            };
            await using (command.ConfigureAwait(false))
            {
                return await command.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false) is true;
            }
        }
    }
}
