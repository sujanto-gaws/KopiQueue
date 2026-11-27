namespace KopiQueue;

/// <summary>
/// Makes a job unique (design §5.6, FR-UNQ-01): while a job with the same unique key is in one of the unique states, an
/// insert returns that job instead, with <see cref="InsertResult.UniqueSkippedAsDuplicate"/> set (FR-UNQ-03).
/// </summary>
/// <remarks>
/// <para>
/// The key is built from the job's kind (unless <see cref="ExcludeKind"/> is set) and each dimension that is set:
/// <see cref="ByArgs"/>, <see cref="ByPeriod"/> and <see cref="ByQueue"/>. It is the key River computes for the same
/// options, so KopiQueue and River producers that share a database dedupe against each other (FR-UNQ-06).
/// </para>
/// <para>
/// As in River, options with nothing set enforce nothing: the job is not unique. To make a kind unique on its own, set
/// <see cref="ByState"/>.
/// </para>
/// <para>
/// The database enforces uniqueness (FR-UNQ-02), so concurrent inserts of one key, from any number of processes, store
/// one job. Under the default <see cref="ByState"/>, a completed job keeps its slot until the job cleaner deletes it
/// (<see cref="KopiClientOptions.CompletedJobRetention"/>).
/// </para>
/// </remarks>
/// <example>
/// <code>
/// // At most one sync per customer waiting or running; once it completes and is cleaned up, another can be inserted.
/// await client.InsertAsync(new SyncCustomerArgs(customerId), new InsertOptions { Unique = new() { ByArgs = true } });
/// </code>
/// </example>
public sealed class UniqueOptions
{
    /// <summary>
    /// Whether the args are part of the key: all the top-level members of their JSON, or only those marked
    /// <see cref="UniqueKeyAttribute"/> when the args type has any. The args must serialise to a JSON object.
    /// </summary>
    public bool ByArgs { get; init; }

    /// <summary>
    /// The length of the time window the job is unique within, or <c>null</c> for none. The window starts at the
    /// insert time, by the client's clock, rounded down to a multiple of the period since 1970-01-01 UTC, to the second.
    /// Positive.
    /// </summary>
    public TimeSpan? ByPeriod { get; init; }

    /// <summary>Whether the queue is part of the key, so the same job can wait in each queue once.</summary>
    public bool ByQueue { get; init; }

    /// <summary>
    /// The states in which a job holds its key, so that no other job with the key can be inserted (FR-UNQ-04).
    /// <c>null</c> for the default: every state but <see cref="JobState.Cancelled"/> and <see cref="JobState.Discarded"/>.
    /// A custom set must include <see cref="JobState.Available"/>, <see cref="JobState.Pending"/>,
    /// <see cref="JobState.Running"/> and <see cref="JobState.Scheduled"/>. The states are not part of the key.
    /// </summary>
    /// <remarks>
    /// Leaving out <see cref="JobState.Retryable"/> lets a duplicate be inserted while the job waits for a retry. When
    /// the retry comes due, the scheduler then discards the waiting job, because the duplicate holds the key.
    /// </remarks>
    public IReadOnlyList<JobState>? ByState { get; init; }

    /// <summary>
    /// Whether the kind is left out of the key, so jobs of different kinds can be duplicates of each other. Needs
    /// another dimension.
    /// </summary>
    public bool ExcludeKind { get; init; }

    /// <summary>Whether nothing is set, so the options enforce nothing, as in River.</summary>
    internal bool IsEmpty => !ByArgs && ByPeriod is null && !ByQueue && ByState is null && !ExcludeKind;
}

/// <summary>
/// Marks an args member as part of the unique key when <see cref="UniqueOptions.ByArgs"/> is set (FR-UNQ-01). When an
/// args type marks any member, only the marked members are part of the key; otherwise all are. The River equivalent
/// is the <c>river:"unique"</c> struct tag.
/// </summary>
/// <remarks>
/// The serialiser maps each marked member to its JSON name (<see cref="Serialization.IJobArgsSerializer.GetUniqueMemberNames"/>).
/// A marked member the serialiser leaves out of the JSON is not part of the key.
/// </remarks>
/// <example>
/// <code>
/// [JobKind("sync_customer")]
/// public sealed record SyncCustomerArgs([property: UniqueKey] long CustomerId, string RequestedBy) : IJobArgs;
/// </code>
/// </example>
[AttributeUsage(AttributeTargets.Property | AttributeTargets.Field, Inherited = true, AllowMultiple = false)]
public sealed class UniqueKeyAttribute : Attribute;

/// <summary>How the values of a unique job's args are written into its unique key (DQ-8).</summary>
public enum UniqueArgsEncoding
{
    /// <summary>
    /// Each value is written again as Go's <c>encoding/json</c> writes it, so the key does not depend on how the args were
    /// serialised: the same values give the same key as River's for a Go producer (the default, DQ-8).
    /// </summary>
    GoCompatible,

    /// <summary>Each value's JSON is used as the serialiser wrote it.</summary>
    Verbatim,
}
