namespace KopiQueue;

/// <summary>
/// A queue as stored in the database (FR-QUE-02): created when a client first works it, and kept while clients work it
/// (the queue cleaner deletes it after <see cref="KopiClientOptions.QueueRetention"/> without).
/// </summary>
public sealed class QueueRow
{
    /// <summary>The queue's name.</summary>
    public required string Name { get; init; }

    /// <summary>When the queue's row was created.</summary>
    public required DateTimeOffset CreatedAt { get; init; }

    /// <summary>When a client last worked, paused, resumed or updated the queue.</summary>
    public required DateTimeOffset UpdatedAt { get; init; }

    /// <summary>When the queue was paused, or <c>null</c> if it is not paused.</summary>
    public DateTimeOffset? PausedAt { get; init; }

    /// <summary>The queue's metadata, as a JSON object.</summary>
    public required string MetadataJson { get; init; }
}

/// <summary>Which queues <see cref="IKopiClient.QueueListAsync"/> returns.</summary>
public sealed class QueueListParams
{
    /// <summary>The most queues to return, from 1 to 10,000. Default: 100.</summary>
    public int Limit { get; init; } = 100;

    /// <summary>
    /// Returns only queues whose name sorts after this one: pass the last name of a page to get the next. <c>null</c>
    /// starts from the first queue.
    /// </summary>
    public string? After { get; init; }
}

/// <summary>How <see cref="IKopiClient.QueuePauseAsync(string, QueuePauseOptions?, CancellationToken)"/> and <see cref="IKopiClient.QueueResumeAsync(string, QueuePauseOptions?, CancellationToken)"/> apply.</summary>
public sealed class QueuePauseOptions
{
    /// <summary>
    /// <c>true</c> pauses or resumes the queue on this client only, without touching the database; other clients carry
    /// on as they were. The queue is fetched here only while it is resumed both locally and everywhere. Default:
    /// <c>false</c>, everywhere.
    /// </summary>
    public bool LocalOnly { get; init; }
}

/// <summary>A job or queue named in a call does not exist, or, for a local pause or resume, this client does not work the queue.</summary>
public class KopiQueueNotFoundException : Exception
{
    /// <summary>Creates an exception with a default message.</summary>
    public KopiQueueNotFoundException()
    {
    }

    /// <summary>Creates an exception with <paramref name="message"/>.</summary>
    public KopiQueueNotFoundException(string? message)
        : base(message)
    {
    }

    /// <summary>Creates an exception with <paramref name="message"/> caused by <paramref name="innerException"/>.</summary>
    public KopiQueueNotFoundException(string? message, Exception? innerException)
        : base(message, innerException)
    {
    }
}
