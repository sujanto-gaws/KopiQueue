using System.Globalization;

namespace KopiQueue.Migrations;

/// <summary>Direction of a migration run.</summary>
public enum MigrationDirection
{
    /// <summary>Apply migrations, raising the schema version.</summary>
    Up,

    /// <summary>Revert migrations, lowering the schema version.</summary>
    Down,
}

/// <summary>Options for <see cref="KopiMigrator.MigrateAsync"/>.</summary>
public sealed class MigrateOptions
{
    /// <summary>Default for <see cref="LockTimeout"/>: 30 seconds.</summary>
    public static readonly TimeSpan DefaultLockTimeout = TimeSpan.FromSeconds(30);

    /// <summary>
    /// The version to end at. Up: defaults to the latest version this library knows. Down: defaults to one version
    /// below the current one; <c>0</c> reverts every migration.
    /// </summary>
    public int? TargetVersion { get; init; }

    /// <summary>Plan the run and return the SQL it would execute, without changing the database. Default: <c>false</c>.</summary>
    public bool DryRun { get; init; }

    /// <summary>How long to wait while another process holds the migration lock. Default: <see cref="DefaultLockTimeout"/>.</summary>
    public TimeSpan LockTimeout { get; init; } = DefaultLockTimeout;
}

/// <summary>Outcome of <see cref="KopiMigrator.MigrateAsync"/>.</summary>
public sealed class MigrationResult
{
    internal MigrationResult(
        string line, MigrationDirection direction, int versionBefore, int versionAfter, int latestVersion, bool dryRun,
        IReadOnlyList<MigrationStepResult> steps)
    {
        Line = line;
        Direction = direction;
        VersionBefore = versionBefore;
        VersionAfter = versionAfter;
        LatestVersion = latestVersion;
        DryRun = dryRun;
        Steps = steps;
    }

    /// <summary>The migration line, for example <c>main</c>.</summary>
    public string Line { get; }

    /// <summary>Up or down.</summary>
    public MigrationDirection Direction { get; }

    /// <summary>Schema version before the run (0: none applied).</summary>
    public int VersionBefore { get; }

    /// <summary>Schema version after the run. For a dry run, the version the run would reach.</summary>
    public int VersionAfter { get; }

    /// <summary>The latest version this library knows.</summary>
    public int LatestVersion { get; }

    /// <summary>Whether this was a dry run, so nothing was executed.</summary>
    public bool DryRun { get; }

    /// <summary>The migrations executed (or, for a dry run, planned), in order. Empty when there was nothing to do.</summary>
    public IReadOnlyList<MigrationStepResult> Steps { get; }
}

/// <summary>One migration executed (or planned) by a run.</summary>
public sealed class MigrationStepResult
{
    internal MigrationStepResult(MigrationStep step, TimeSpan duration)
    {
        Step = step;
        Duration = duration;
    }

    /// <summary>The migration's version.</summary>
    public int Version => Step.Version;

    /// <summary>The migration's name, for example "initial schema".</summary>
    public string Name => Step.Name;

    /// <summary>Up or down.</summary>
    public MigrationDirection Direction => Step.Direction;

    /// <summary>The SQL executed, with the schema and table prefix already substituted.</summary>
    public string Sql => Step.Sql;

    /// <summary>How long the migration took. Zero for a dry run.</summary>
    public TimeSpan Duration { get; }

    internal MigrationStep Step { get; }
}

/// <summary>One version of a migration line, as reported by <see cref="KopiMigrator.GetVersionsAsync"/>.</summary>
public sealed class MigrationVersion
{
    internal MigrationVersion(int version, string? name, DateTimeOffset? appliedAt)
    {
        Version = version;
        Name = name;
        AppliedAt = appliedAt;
    }

    /// <summary>The version number.</summary>
    public int Version { get; }

    /// <summary>The migration's name, or <c>null</c> for a version applied by a newer library that this one does not know.</summary>
    public string? Name { get; }

    /// <summary>When the version was applied, or <c>null</c> if it is not applied.</summary>
    public DateTimeOffset? AppliedAt { get; }

    /// <summary>Whether the version is applied.</summary>
    public bool IsApplied => AppliedAt is not null;

    /// <summary>Whether this library knows the version.</summary>
    public bool IsKnown => Name is not null;
}

/// <summary>Whether the schema is current, as reported by <see cref="KopiMigrator.ValidateAsync"/>.</summary>
public sealed class MigrationValidationResult
{
    internal MigrationValidationResult(string line, int currentVersion, int latestVersion)
    {
        Line = line;
        CurrentVersion = currentVersion;
        LatestVersion = latestVersion;
        PendingVersions = [.. Enumerable.Range(currentVersion + 1, Math.Max(latestVersion - currentVersion, 0))];
    }

    /// <summary>The migration line, for example <c>main</c>.</summary>
    public string Line { get; }

    /// <summary>The schema version in the database (0: none applied).</summary>
    public int CurrentVersion { get; }

    /// <summary>The latest version this library knows.</summary>
    public int LatestVersion { get; }

    /// <summary>Whether the database is exactly at the latest version this library knows.</summary>
    public bool IsCurrent => CurrentVersion == LatestVersion;

    /// <summary>
    /// Whether the database has versions this library does not know, applied by a newer KopiQueue or River.
    /// Migrating up is then a no-op, but this library may not work with the newer schema.
    /// </summary>
    public bool IsDatabaseNewer => CurrentVersion > LatestVersion;

    /// <summary>Versions that migrating up would apply, in order. Empty when the schema is current or newer.</summary>
    public IReadOnlyList<int> PendingVersions { get; }

    /// <summary>A one-line description, suitable for a health check or a log message.</summary>
    public string Message => IsCurrent
        ? string.Create(CultureInfo.InvariantCulture, $"Migration line '{Line}' is current at version {CurrentVersion}.")
        : IsDatabaseNewer
            ? string.Create(CultureInfo.InvariantCulture,
                $"Migration line '{Line}' is at version {CurrentVersion}, newer than the latest version {LatestVersion} this library knows.")
            : string.Create(CultureInfo.InvariantCulture,
                $"Migration line '{Line}' is at version {CurrentVersion}; versions {PendingVersions[0]} to {LatestVersion} are pending.");
}
