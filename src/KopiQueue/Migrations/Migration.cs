namespace KopiQueue.Migrations;

/// <summary>Direction of a migration run.</summary>
internal enum MigrationDirection
{
    Up,
    Down,
}

/// <summary>One versioned migration of a line (for example <c>main</c>), with its up and down SQL templates.</summary>
/// <param name="Line">Migration line. Versions are numbered per line, from 1, without gaps.</param>
/// <param name="Version">Version number within the line.</param>
/// <param name="Name">Human-readable name, taken from the file name (<c>001_create_job</c> gives "create job").</param>
/// <param name="Up">Script that applies the migration.</param>
/// <param name="Down">Script that reverts it.</param>
internal sealed record Migration(string Line, int Version, string Name, MigrationScript Up, MigrationScript Down)
{
    /// <summary>The script for <paramref name="direction"/>.</summary>
    public MigrationScript For(MigrationDirection direction) => direction == MigrationDirection.Up ? Up : Down;
}

/// <summary>A SQL template, and whether it must run inside a transaction.</summary>
/// <param name="Sql">SQL with <c>{{schema}}</c> and <c>{{prefix}}</c> placeholders (see <see cref="MigrationTemplate"/>).</param>
/// <param name="InTransaction">
/// <c>false</c> when the script starts with the <c>-- kopi:no-transaction</c> directive, for statements such as
/// <c>CREATE INDEX CONCURRENTLY</c> that cannot run in a transaction.
/// </param>
internal sealed record MigrationScript(string Sql, bool InTransaction);

/// <summary>One step of a run: rendered SQL for one migration in one direction, ready for the driver.</summary>
internal sealed record MigrationStep(string Line, int Version, string Name, MigrationDirection Direction, string Sql, bool InTransaction);
