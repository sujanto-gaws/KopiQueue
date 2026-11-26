using System.Data.Common;
using KopiQueue.Drivers;
using KopiQueue.Migrations;
using KopiQueue.Npgsql.Migrations;
using Microsoft.Extensions.Logging;
using Npgsql;

namespace KopiQueue.Npgsql;

/// <summary>Where the KopiQueue schema lives in a PostgreSQL database.</summary>
public sealed class NpgsqlDriverOptions
{
    /// <summary>
    /// The schema that holds the tables, for example <c>queue</c> (FR-MIG-05). It must already exist.
    /// Default: <c>null</c>, which uses the connection's <c>search_path</c>.
    /// </summary>
    /// <remarks>1-63 letters, digits or underscores, not starting with a digit.</remarks>
    public string? Schema { get; init; }

    /// <summary>
    /// Prefix of every table, type, function and index name. Default: <c>river_</c>, which keeps the schema
    /// compatible with River. Another prefix, for example <c>kopi_</c>, gives the same schema under other names.
    /// </summary>
    /// <remarks>1-32 lower-case letters, digits or underscores, not starting with a digit.</remarks>
    public string TablePrefix { get; init; } = NpgsqlMigrationDriver.DefaultTablePrefix;

    /// <summary>
    /// The connection string of the notifier's LISTEN connection, when it must bypass the data source: for example when
    /// the data source goes through PgBouncer in transaction mode or a proxy, which drop LISTEN (spike 0.7 D7).
    /// Default: <c>null</c>, which derives the connection from the data source, credentials included.
    /// </summary>
    /// <remarks>KopiQueue always turns pooling off on this connection and sets <see cref="NotifierKeepAlive"/>.</remarks>
    public string? NotifierConnectionString { get; init; }

    /// <summary>
    /// Npgsql's TCP keep-alive on the notifier connection, which is how a silently dead connection is found: in about
    /// twice this time (NFR-REL-04a, spike 0.7). Default: 5 s. Rounded up to whole seconds.
    /// </summary>
    public TimeSpan NotifierKeepAlive { get; init; } = DefaultNotifierKeepAlive;

    internal static TimeSpan DefaultNotifierKeepAlive { get; } = TimeSpan.FromSeconds(5);
}

/// <summary>The PostgreSQL driver for KopiQueue, on Npgsql (FR-DB-01). The schema is River-compatible.</summary>
/// <example>
/// <code>
/// var driver = new NpgsqlDriver(dataSource, new NpgsqlDriverOptions { Schema = "queue" });
/// await driver.CreateMigrator().MigrateAsync(MigrationDirection.Up, cancellationToken: ct);
/// </code>
/// </example>
/// <remarks>Thread-safe. The driver holds no connections of its own: it borrows them from the data source per operation.</remarks>
public sealed class NpgsqlDriver : IKopiDriver
{
    private static readonly DriverCapabilities PostgresCapabilities = new()
    {
        ListenNotify = true,
        BulkCopy = true,
        MaxRowsPerStatement = null,
    };

    private readonly NpgsqlDataSource _dataSource;
    private readonly NpgsqlMigrationDriver _migrationDriver;
    private readonly NpgsqlStatements _statements;
    private readonly NpgsqlListener _listener;

    /// <summary>Creates a driver for <paramref name="dataSource"/>.</summary>
    /// <param name="dataSource">The database. KopiQueue borrows pooled connections from it.</param>
    /// <param name="options">Schema and table prefix. <c>null</c> uses the defaults.</param>
    /// <exception cref="ArgumentException">The schema name or table prefix is not allowed.</exception>
    /// <exception cref="ArgumentOutOfRangeException"><see cref="NpgsqlDriverOptions.NotifierKeepAlive"/> is shorter than 1 s or longer than a day.</exception>
    public NpgsqlDriver(NpgsqlDataSource dataSource, NpgsqlDriverOptions? options = null)
    {
        ArgumentNullException.ThrowIfNull(dataSource);
        options ??= new NpgsqlDriverOptions();
        _dataSource = dataSource;
        _migrationDriver = new NpgsqlMigrationDriver(dataSource, options.Schema, options.TablePrefix);
        _statements = new NpgsqlStatements(options.Schema, _migrationDriver.SchemaQualifier, _migrationDriver.TablePrefix);
        Schema = options.Schema;
        TablePrefix = options.TablePrefix;
        if (options.NotifierKeepAlive < TimeSpan.FromSeconds(1) || options.NotifierKeepAlive > TimeSpan.FromDays(1))
        {
            throw new ArgumentOutOfRangeException(nameof(options), options.NotifierKeepAlive, "NotifierKeepAlive must be from 1 s to 1 day.");
        }

        _listener = new NpgsqlListener(dataSource, options.Schema, options.NotifierConnectionString, options.NotifierKeepAlive);
    }

    /// <summary>The schema, or <c>null</c> for the connection's <c>search_path</c>.</summary>
    public string? Schema { get; }

    /// <summary>The table prefix.</summary>
    public string TablePrefix { get; }

    /// <summary>
    /// Creates a migrator for the River-compatible <c>main</c> line: versions 1-8, matching River v0.51.2's schema.
    /// </summary>
    /// <param name="loggerFactory">Logs each migration (event IDs 4000-4999). <c>null</c> disables logging.</param>
    public KopiMigrator CreateMigrator(ILoggerFactory? loggerFactory = null) =>
        new(_migrationDriver, NpgsqlMigrationDriver.MainLineName, NpgsqlMigrationDriver.MainLine, loggerFactory);

    DriverCapabilities IKopiDriver.Capabilities => PostgresCapabilities;

    IKopiListener? IKopiDriver.CreateListener() => _listener;

    IKopiExecutor IKopiDriver.GetExecutor() => new NpgsqlExecutor(_statements, _dataSource, transaction: null);

    IKopiExecutor IKopiDriver.GetExecutor(DbTransaction transaction)
    {
        ArgumentNullException.ThrowIfNull(transaction);
        if (transaction is not NpgsqlTransaction npgsqlTransaction)
        {
            throw new ArgumentException(
                $"The PostgreSQL driver needs an NpgsqlTransaction, not a {transaction.GetType().Name}.", nameof(transaction));
        }

        return new NpgsqlExecutor(_statements, _dataSource, npgsqlTransaction);
    }
}
