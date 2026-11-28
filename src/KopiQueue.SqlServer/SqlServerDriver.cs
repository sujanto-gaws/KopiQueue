using KopiQueue.Migrations;
using KopiQueue.SqlServer.Migrations;
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
/// <para>Thread-safe. The driver holds no connections of its own: it opens pooled ones per operation.</para>
/// </remarks>
public sealed class SqlServerDriver
{
    private readonly SqlServerMigrationDriver _migrationDriver;

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
}
