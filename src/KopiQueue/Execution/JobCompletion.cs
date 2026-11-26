using KopiQueue.Drivers;

namespace KopiQueue.Execution;

/// <summary>
/// How one attempt ended and the state it moves the job to (design §5.2): what the executor hands the completer.
/// </summary>
/// <param name="Job">The job as fetched, with this attempt's number.</param>
/// <param name="State">
/// The new state: <see cref="JobState.Completed"/>, <see cref="JobState.Retryable"/> or <see cref="JobState.Discarded"/>.
/// </param>
/// <param name="FinalizedAt">When the job was finalised, for a final state; otherwise <c>null</c>.</param>
/// <param name="ScheduledAt">When a retryable job is due again; otherwise <c>null</c>, which keeps the stored value.</param>
/// <param name="Error">The error to append to the job's <c>errors</c>, or <c>null</c>.</param>
/// <param name="Duration">How long the worker ran.</param>
internal sealed record JobCompletion(
    JobRow Job,
    JobState State,
    DateTimeOffset? FinalizedAt,
    DateTimeOffset? ScheduledAt,
    AttemptError? Error,
    TimeSpan Duration);

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
