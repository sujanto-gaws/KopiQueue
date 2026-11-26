using KopiQueue.Drivers;

namespace KopiQueue.Execution;

/// <summary>
/// How one attempt ended and what it changes on the job (design §5.2): what the executor hands the completer. It maps
/// onto River's <c>JobSetStateIfRunningParams</c>, so both write the same rows.
/// </summary>
/// <param name="Job">The job as fetched, with this attempt's number.</param>
/// <param name="State">
/// The new state: <see cref="JobState.Completed"/>, <see cref="JobState.Retryable"/>, <see cref="JobState.Discarded"/>,
/// <see cref="JobState.Cancelled"/>, <see cref="JobState.Scheduled"/>, or <see cref="JobState.Available"/> for a job
/// that is due again within the scheduler interval or was interrupted by a client stop.
/// </param>
/// <param name="FinalizedAt">When the job was finalised, for a final state; otherwise <c>null</c>.</param>
/// <param name="ScheduledAt">When the job is due again; <c>null</c> keeps the stored value.</param>
/// <param name="Error">The error to append to the job's <c>errors</c>, or <c>null</c>.</param>
/// <param name="Duration">How long the worker ran.</param>
/// <param name="Attempt">The attempt number to store instead, for an attempt that does not count; <c>null</c> keeps it.</param>
/// <param name="MetadataUpdates">A JSON object merged into the job's <c>metadata</c> at the top level, or <c>null</c>.</param>
internal sealed record JobCompletion(
    JobRow Job,
    JobState State,
    DateTimeOffset? FinalizedAt,
    DateTimeOffset? ScheduledAt,
    AttemptError? Error,
    TimeSpan Duration,
    int? Attempt = null,
    string? MetadataUpdates = null);

/// <summary>
/// Records completions in the database (design §5.3; plan task 2.6). The executor waits for
/// <see cref="CompleteAsync"/> before it frees the job's slot, so a completer that accepts quickly and writes later
/// keeps slots from waiting on the database, and one that fills up slows the producers down.
/// </summary>
internal interface IJobCompleter
{
    /// <summary>Accepts <paramref name="completion"/>. Returns once the completer has taken it, not necessarily written it.</summary>
    ValueTask CompleteAsync(JobCompletion completion, CancellationToken cancellationToken);
}
