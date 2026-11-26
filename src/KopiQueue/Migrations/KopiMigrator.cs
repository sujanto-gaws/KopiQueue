using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace KopiQueue.Migrations;

/// <summary>
/// Migrates the KopiQueue schema of one migration line (FR-MIG-02). Get one from a driver package, for example
/// <c>new NpgsqlDriver(dataSource).CreateMigrator()</c> in <c>KopiQueue.Npgsql</c>, or <c>IKopiDriver.CreateMigrator</c>.
/// </summary>
/// <remarks>
/// <para>
/// Safe to run from several processes at once: <see cref="MigrateAsync"/> holds a database lock for the whole run,
/// and a process that gets the lock after another has finished finds nothing left to do (FR-MIG-06).
/// Each migration runs in its own transaction together with the row that records it.
/// </para>
/// <para>Instances are thread-safe and hold no connection between calls.</para>
/// </remarks>
public sealed class KopiMigrator
{
    private readonly IMigrationDriver _driver;
    private readonly IReadOnlyList<Migration> _migrations;
    private readonly ILogger _logger;
    private readonly TimeProvider _timeProvider;

    internal KopiMigrator(
        IMigrationDriver driver, string line, IReadOnlyList<Migration> migrations, ILoggerFactory? loggerFactory = null,
        TimeProvider? timeProvider = null)
    {
        ArgumentNullException.ThrowIfNull(driver);
        ArgumentNullException.ThrowIfNull(migrations);
        _driver = driver;
        Line = line;
        _migrations = migrations;
        _logger = (loggerFactory ?? NullLoggerFactory.Instance).CreateLogger("KopiQueue.Migrations");
        _timeProvider = timeProvider ?? TimeProvider.System;
    }

    /// <summary>The migration line this migrator manages, for example <c>main</c>.</summary>
    public string Line { get; }

    /// <summary>The latest version this library knows for <see cref="Line"/>.</summary>
    public int LatestVersion => _migrations.Count;

    /// <summary>
    /// Migrates up (by default to the latest version) or down (by default one version), as limited by
    /// <see cref="MigrateOptions.TargetVersion"/>.
    /// </summary>
    /// <param name="direction">Up or down.</param>
    /// <param name="options">Target version, dry run and lock timeout. <c>null</c> uses the defaults.</param>
    /// <param name="cancellationToken">Cancels the run. A migration that already started is rolled back if it runs in a transaction.</param>
    /// <returns>The versions before and after, and each migration executed.</returns>
    /// <exception cref="ArgumentOutOfRangeException">The up target is not between 1 and <see cref="LatestVersion"/>, or the down target is negative.</exception>
    /// <exception cref="KopiQueueMigrationException">
    /// The lock was not acquired in time, the recorded versions do not allow the run (for example a down target above
    /// the current version, or a database newer than this library when migrating down), or a migration failed.
    /// Migrations before a failed one stay applied.
    /// </exception>
    public Task<MigrationResult> MigrateAsync(
        MigrationDirection direction, MigrateOptions? options = null, CancellationToken cancellationToken = default)
    {
        options ??= new MigrateOptions();
        if (!Enum.IsDefined(direction))
        {
            throw new ArgumentOutOfRangeException(nameof(direction), direction, "Unknown migration direction.");
        }

        ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(options.LockTimeout, TimeSpan.Zero, nameof(options));
        return new MigrationRunner(_driver, _logger, _timeProvider).RunAsync(
            new MigrationRequest(_migrations, Line, direction, options.TargetVersion, options.LockTimeout, options.DryRun),
            cancellationToken);
    }

    /// <summary>
    /// Lists every version this library knows, with when it was applied, plus any applied version this library does not know.
    /// Does not take the migration lock.
    /// </summary>
    /// <returns>Versions in ascending order.</returns>
    public async Task<IReadOnlyList<MigrationVersion>> GetVersionsAsync(CancellationToken cancellationToken = default)
    {
        var applied = await GetAppliedAsync(cancellationToken).ConfigureAwait(false);
        var appliedAt = applied.ToDictionary(a => a.Version, a => a.AppliedAt);
        var versions = _migrations
            .Select(m => new MigrationVersion(m.Version, m.Name, appliedAt.TryGetValue(m.Version, out var at) ? at : null))
            .Concat(applied.Where(a => a.Version > LatestVersion).Select(a => new MigrationVersion(a.Version, null, a.AppliedAt)));
        return [.. versions.OrderBy(v => v.Version)];
    }

    /// <summary>Checks whether the schema is at the latest version this library knows (FR-MIG-02). Does not take the migration lock.</summary>
    /// <exception cref="KopiQueueMigrationException">The recorded versions have gaps.</exception>
    public async Task<MigrationValidationResult> ValidateAsync(CancellationToken cancellationToken = default)
    {
        var applied = await GetAppliedAsync(cancellationToken).ConfigureAwait(false);
        var current = MigrationPlanner.CurrentVersion([.. applied.Select(a => a.Version)]);
        return new MigrationValidationResult(Line, current, LatestVersion);
    }

    private async Task<IReadOnlyList<AppliedVersion>> GetAppliedAsync(CancellationToken cancellationToken)
    {
        var session = await _driver.OpenSessionAsync(cancellationToken).ConfigureAwait(false);
        await using (session.ConfigureAwait(false))
        {
            return await session.GetAppliedVersionsAsync(Line, cancellationToken).ConfigureAwait(false);
        }
    }
}
