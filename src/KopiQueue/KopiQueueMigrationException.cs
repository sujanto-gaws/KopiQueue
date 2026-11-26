namespace KopiQueue;

/// <summary>
/// A schema migration could not run: the migration lock was not acquired in time, the recorded versions do not allow
/// the requested change, or a migration failed. When a migration failed, <see cref="Exception.InnerException"/> holds
/// the database error.
/// </summary>
public class KopiQueueMigrationException : Exception
{
    /// <summary>Creates an exception with a default message.</summary>
    public KopiQueueMigrationException()
    {
    }

    /// <summary>Creates an exception with <paramref name="message"/>.</summary>
    public KopiQueueMigrationException(string? message)
        : base(message)
    {
    }

    /// <summary>Creates an exception with <paramref name="message"/> caused by <paramref name="innerException"/>.</summary>
    public KopiQueueMigrationException(string? message, Exception? innerException)
        : base(message, innerException)
    {
    }
}
