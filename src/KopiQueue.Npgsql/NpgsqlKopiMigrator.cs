using KopiQueue.Migrations;
using KopiQueue.Npgsql.Migrations;
using Microsoft.Extensions.Logging;
using Npgsql;

namespace KopiQueue.Npgsql;

/// <summary>Where the KopiQueue schema lives in a PostgreSQL database.</summary>
public sealed class NpgsqlMigratorOptions
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
}

/// <summary>Creates <see cref="KopiMigrator"/> instances for PostgreSQL.</summary>
/// <example>
/// <code>
/// var migrator = NpgsqlKopiMigrator.Create(dataSource);
/// var status = await migrator.ValidateAsync(ct);
/// if (!status.IsCurrent)
/// {
///     await migrator.MigrateAsync(MigrationDirection.Up, cancellationToken: ct);
/// }
/// </code>
/// </example>
public static class NpgsqlKopiMigrator
{
    /// <summary>
    /// Creates a migrator for the River-compatible <c>main</c> line: versions 1-8, matching River v0.51.2's schema.
    /// </summary>
    /// <param name="dataSource">The database to migrate. Migrations use one connection from it per call.</param>
    /// <param name="options">Schema and table prefix. <c>null</c> uses the defaults.</param>
    /// <param name="loggerFactory">Logs each migration (event IDs 4000-4999). <c>null</c> disables logging.</param>
    /// <exception cref="ArgumentException">The schema name or table prefix is not allowed.</exception>
    public static KopiMigrator Create(NpgsqlDataSource dataSource, NpgsqlMigratorOptions? options = null, ILoggerFactory? loggerFactory = null)
    {
        ArgumentNullException.ThrowIfNull(dataSource);
        options ??= new NpgsqlMigratorOptions();
        var driver = new NpgsqlMigrationDriver(dataSource, options.Schema, options.TablePrefix);
        return new KopiMigrator(driver, NpgsqlMigrationDriver.MainLineName, NpgsqlMigrationDriver.MainLine, loggerFactory);
    }
}
