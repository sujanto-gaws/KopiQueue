using System.Text.Json.Nodes;

namespace KopiQueue;

/// <summary>
/// Options for inserting a job (FR-INS-05). Every option is optional: an unset option takes the client's default.
/// </summary>
/// <example>
/// <code>
/// new InsertOptions { Queue = "email", Priority = 2, ScheduledAt = DateTimeOffset.UtcNow.AddMinutes(5), Tags = ["welcome"] }
/// </code>
/// </example>
/// <remarks>Options are validated when the job is inserted (FR-INS-09); an invalid option throws <see cref="ArgumentException"/>.</remarks>
public sealed class InsertOptions
{
    /// <summary>
    /// The queue to run on. Default: <see cref="InsertDefaults.Queue"/> (<c>default</c>).
    /// </summary>
    /// <remarks>1-127 characters: lower-case ASCII letters, digits, <c>_</c> and <c>-</c>, starting and ending with a letter or digit.</remarks>
    public string? Queue { get; init; }

    /// <summary>Priority from 1 (highest) to 4 (lowest). Default: <see cref="InsertDefaults.Priority"/> (1).</summary>
    public int? Priority { get; init; }

    /// <summary>
    /// When the job becomes available. A time in the future inserts the job as <see cref="JobState.Scheduled"/>.
    /// Default: now, by the database's clock.
    /// </summary>
    public DateTimeOffset? ScheduledAt { get; init; }

    /// <summary>How many attempts the job gets, at least 1. Default: <see cref="InsertDefaults.MaxAttempts"/> (25).</summary>
    public int? MaxAttempts { get; init; }

    /// <summary>
    /// Tags for finding jobs later. Each tag is 1-255 characters: ASCII letters, digits, <c>_</c> and <c>-</c>, starting and
    /// ending with a letter, digit or <c>_</c>, and at least 3 characters long (as River requires). Default: none.
    /// </summary>
    public IReadOnlyList<string>? Tags { get; init; }

    /// <summary>Metadata stored with the job, as a JSON object. Default: empty.</summary>
    public JsonObject? Metadata { get; init; }

    /// <summary>
    /// Makes the job unique (FR-UNQ-01): if a job with the same unique key holds its slot, the insert returns that job
    /// with <see cref="InsertResult.UniqueSkippedAsDuplicate"/> set, and inserts nothing. Default: <c>null</c>, not unique.
    /// </summary>
    /// <remarks>
    /// Works with <see cref="IKopiClient.InsertAsync{TArgs}(TArgs, InsertOptions?, CancellationToken)"/> and
    /// <see cref="IKopiClient.InsertManyAsync"/>, but not with <see cref="IKopiClient.InsertManyFastAsync"/>.
    /// </remarks>
    public UniqueOptions? Unique { get; init; }
}

/// <summary>The defaults for options that an insert leaves unset.</summary>
public static class InsertDefaults
{
    /// <summary>The default queue: <c>default</c>.</summary>
    public const string Queue = "default";

    /// <summary>The default priority: 1, the highest.</summary>
    public const int Priority = 1;

    /// <summary>The default number of attempts: 25.</summary>
    public const int MaxAttempts = 25;
}
