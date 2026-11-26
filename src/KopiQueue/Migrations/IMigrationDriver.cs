namespace KopiQueue.Migrations;

/// <summary>
/// The database-specific half of the migration engine. Each driver package implements it
/// (PostgreSQL: advisory lock; SQL Server: <c>sp_getapplock</c>, FR-MIG-06).
/// </summary>
internal interface IMigrationDriver
{
    /// <summary>Value substituted for <c>{{schema}}</c>: the quoted schema and a dot, or empty for the default schema.</summary>
    string SchemaQualifier { get; }

    /// <summary>Value substituted for <c>{{prefix}}</c>.</summary>
    string TablePrefix { get; }

    /// <summary>Opens a session on a dedicated connection. The lock and every step of a run use this one session.</summary>
    Task<IMigrationSession> OpenSessionAsync(CancellationToken cancellationToken);
}

/// <summary>A connection that holds the migration lock and executes steps.</summary>
internal interface IMigrationSession : IAsyncDisposable
{
    /// <summary>
    /// Tries once to take the migration lock for this schema and prefix, without waiting.
    /// The lock belongs to the session, not to a transaction, so it is held across all steps.
    /// </summary>
    Task<bool> TryAcquireLockAsync(CancellationToken cancellationToken);

    /// <summary>Releases the lock taken by <see cref="TryAcquireLockAsync"/>. Does nothing if it is not held.</summary>
    Task ReleaseLockAsync(CancellationToken cancellationToken);

    /// <summary>Versions recorded for <paramref name="line"/>. Empty when the migration table does not exist yet.</summary>
    Task<IReadOnlyList<int>> GetAppliedVersionsAsync(string line, CancellationToken cancellationToken);

    /// <summary>
    /// Executes one step and records it, atomically when <see cref="MigrationStep.InTransaction"/> is set:
    /// up runs the SQL and then inserts the version row (the line's first migration may create the migration table);
    /// down deletes the version row and then runs the SQL (the first migration's down may drop the table).
    /// </summary>
    Task ExecuteAsync(MigrationStep step, CancellationToken cancellationToken);
}
