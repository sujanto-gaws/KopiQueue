using System.Data.Common;
using KopiQueue.Drivers;
using KopiQueue.Migrations;
using KopiQueue.SqlServer.Migrations;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;

namespace KopiQueue.SqlServer;

/// <summary>Where the KopiQueue schema lives in a SQL Server database.</summary>
public sealed class SqlServerDriverOptions
{
    /// <summary>The schema that holds the tables, for example <c>queue</c> (FR-MIG-05). It must already exist. Default: <c>dbo</c>.</summary>
    /// <remarks>1-63 letters, digits or underscores, not starting with a digit.</remarks>
    public string Schema { get; init; } = SqlServerMigrationDriver.DefaultSchema;

    /// <summary>
    /// Prefix of every table, constraint and index name. Default: <c>kopi_</c>. Tables with different prefixes can share
    /// a schema.
    /// </summary>
    /// <remarks>1-32 lower-case letters, digits or underscores, not starting with a digit.</remarks>
    public string TablePrefix { get; init; } = SqlServerMigrationDriver.DefaultTablePrefix;
}

/// <summary>
/// The SQL Server and Azure SQL driver for KopiQueue, on Microsoft.Data.SqlClient (FR-DB-02). The schema is
/// KopiQueue's own (design §4.4); it needs SQL Server 2019 or later, at database compatibility level 150 or later.
/// </summary>
/// <example>
/// <code>
/// var driver = new SqlServerDriver(connectionString, new SqlServerDriverOptions { Schema = "queue" });
/// await driver.CreateMigrator().MigrateAsync(MigrationDirection.Up, cancellationToken: ct);
/// </code>
/// </example>
/// <remarks>
/// <para>
/// Unless the connection string names an application, the driver's connections get <c>Application Name=KopiQueue</c>,
/// which gives them a connection pool of their own.
/// </para>
/// <para>
/// SQL Server cannot notify other processes, so clients on this driver find new work by polling (FR-DB-04). Every session
/// the driver uses is first checked for the settings KopiQueue's tables need: see <see cref="CreateMigrator"/>.
/// </para>
/// <para>Thread-safe. The driver holds no connections of its own: it opens pooled ones per operation.</para>
/// </remarks>
public sealed class SqlServerDriver : IKopiDriver
{
    /// <summary>
    /// SQL Server's capabilities: no LISTEN/NOTIFY, so the client polls (FR-DB-04); no bulk-copy path, because
    /// <c>SqlBulkCopy</c> takes page locks that stall every producer (ADR-014); and at most 1,000 rows per statement, the
    /// batched insert size of spike 0.10, safely below the 2,000-row lock-escalation cap of spike 0.9 (design §8.1).
    /// </summary>
    private static readonly DriverCapabilities SqlServerCapabilities = new()
    {
        ListenNotify = false,
        BulkCopy = false,
        MaxRowsPerStatement = 1000,
    };

    private readonly SqlServerMigrationDriver _migrationDriver;
    private readonly SqlServerStatements _statements;
    private readonly SqlServerSessionGate _pooledSessions = new();

    /// <summary>Creates a driver for the database <paramref name="connectionString"/> names.</summary>
    /// <param name="connectionString">A SqlClient connection string. It is not opened here.</param>
    /// <param name="options">Schema and table prefix. <c>null</c> uses the defaults.</param>
    /// <exception cref="ArgumentException">
    /// The connection string is empty or not valid, or the schema name or table prefix is not allowed.
    /// </exception>
    public SqlServerDriver(string connectionString, SqlServerDriverOptions? options = null)
    {
        options ??= new SqlServerDriverOptions();
        if (options.Schema is null)
        {
            throw new ArgumentException("SqlServerDriverOptions.Schema is required: use dbo for the default schema.", nameof(options));
        }

        Connections = new SqlServerConnectionFactory(connectionString);
        _migrationDriver = new SqlServerMigrationDriver(Connections, options.Schema, options.TablePrefix);
        _statements = new SqlServerStatements(_migrationDriver.SchemaQualifier, _migrationDriver.TablePrefix);
        Schema = options.Schema;
        TablePrefix = options.TablePrefix;
    }

    /// <summary>The schema.</summary>
    public string Schema { get; }

    /// <summary>The table prefix.</summary>
    public string TablePrefix { get; }

    internal SqlServerConnectionFactory Connections { get; }

    /// <summary>Creates a migrator for KopiQueue's SQL Server <c>main</c> line.</summary>
    /// <param name="loggerFactory">Logs each migration (event IDs 4000-4999). <c>null</c> disables logging.</param>
    /// <remarks>
    /// Concurrent runs serialise on a session-owned application lock for the schema and prefix (<c>sp_getapplock</c>).
    /// Every call first checks the session's settings, and throws <see cref="KopiQueueConfigurationException"/> when
    /// they would break writes to KopiQueue's tables or the database's compatibility level is below 150.
    /// </remarks>
    public KopiMigrator CreateMigrator(ILoggerFactory? loggerFactory = null) =>
        new(_migrationDriver, SqlServerMigrationDriver.MainLineName, SqlServerMigrationDriver.MainLine, loggerFactory);

    DriverCapabilities IKopiDriver.Capabilities => SqlServerCapabilities;

    /// <summary>SQL Server has no LISTEN/NOTIFY, so there is no listener and clients poll (FR-DB-04).</summary>
    IKopiListener? IKopiDriver.CreateListener() => null;

    IKopiExecutor IKopiDriver.GetExecutor() => new SqlServerExecutor(_statements, Connections, _pooledSessions, transaction: null);

    IKopiExecutor IKopiDriver.GetExecutor(DbTransaction transaction)
    {
        ArgumentNullException.ThrowIfNull(transaction);
        if (transaction is not SqlTransaction sqlTransaction)
        {
            throw new ArgumentException(
                $"The SQL Server driver needs a SqlTransaction from Microsoft.Data.SqlClient, not a {transaction.GetType().FullName}.",
                nameof(transaction));
        }

        return new SqlServerExecutor(_statements, Connections, _pooledSessions, sqlTransaction);
    }
}
