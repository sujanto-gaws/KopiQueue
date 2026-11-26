namespace KopiQueue.Migrations;

/// <summary>Decides which migrations a run executes, and in which order. Pure: no I/O.</summary>
internal static class MigrationPlanner
{
    /// <summary>
    /// Returns the migrations to execute, in execution order.
    /// </summary>
    /// <param name="available">The line's migrations, versions 1..N without gaps (as produced by <see cref="MigrationLoader"/>).</param>
    /// <param name="applied">Versions recorded in the database for this line.</param>
    /// <param name="direction">Up or down.</param>
    /// <param name="targetVersion">
    /// Up: the version to migrate to (default: the latest). Down: the version to end at (default: one below the current one;
    /// <c>0</c> reverts the whole line).
    /// </param>
    /// <exception cref="ArgumentOutOfRangeException">The target is outside the versions this library knows.</exception>
    /// <exception cref="KopiQueueMigrationException">The database state does not allow the requested run.</exception>
    public static IReadOnlyList<Migration> Plan(
        IReadOnlyList<Migration> available, IReadOnlyCollection<int> applied, MigrationDirection direction, int? targetVersion)
    {
        ArgumentNullException.ThrowIfNull(available);
        ArgumentNullException.ThrowIfNull(applied);
        var latest = available.Count;
        var current = CurrentVersion(applied);

        if (direction == MigrationDirection.Up)
        {
            var target = targetVersion ?? latest;
            if (target < 1 || target > latest)
            {
                throw new ArgumentOutOfRangeException(
                    nameof(targetVersion), target, $"Up target must be between 1 and {latest}, the latest version this library knows.");
            }

            if (current > latest && targetVersion is null)
            {
                return []; // The database is newer than this library: nothing this library can add.
            }

            if (target < current)
            {
                throw new KopiQueueMigrationException(
                    $"The database is at version {current}, above the up target {target}. Migrate down to reach a lower version.");
            }

            return [.. available.Where(m => m.Version > current && m.Version <= target)];
        }

        var downTarget = targetVersion ?? Math.Max(current - 1, 0);
        if (downTarget < 0)
        {
            throw new ArgumentOutOfRangeException(nameof(targetVersion), downTarget, "Down target must be 0 or more.");
        }

        if (downTarget > current)
        {
            throw new KopiQueueMigrationException(
                $"The database is at version {current}, below the down target {downTarget}. Migrate up to reach a higher version.");
        }

        if (current > latest)
        {
            throw new KopiQueueMigrationException(
                $"The database is at version {current}, but this library only knows versions up to {latest}, " +
                "so it cannot revert the newer ones. Use the newer library or tool that applied them.");
        }

        return [.. available.Where(m => m.Version > downTarget && m.Version <= current).OrderByDescending(m => m.Version)];
    }

    /// <summary>The highest applied version (0 when none), after checking that versions 1..N are all present.</summary>
    /// <exception cref="KopiQueueMigrationException">The applied versions have gaps.</exception>
    public static int CurrentVersion(IReadOnlyCollection<int> applied)
    {
        ArgumentNullException.ThrowIfNull(applied);
        if (applied.Count == 0)
        {
            return 0;
        }

        var sorted = applied.Order().ToArray();
        for (var i = 0; i < sorted.Length; i++)
        {
            if (sorted[i] != i + 1)
            {
                throw new KopiQueueMigrationException(
                    $"The recorded migration versions are inconsistent ({string.Join(", ", sorted)}): " +
                    "versions must run from 1 without gaps. Check the migration table.");
            }
        }

        return sorted[^1];
    }
}
