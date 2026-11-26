namespace KopiQueue;

/// <summary>The state of a job (design §4.2). The names and meanings match River's <c>river_job_state</c>.</summary>
public enum JobState
{
    /// <summary>Ready to run as soon as a worker is free.</summary>
    Available,

    /// <summary>Cancelled; it will not run again. A final state.</summary>
    Cancelled,

    /// <summary>Ran successfully. A final state.</summary>
    Completed,

    /// <summary>Failed on its last allowed attempt, or was discarded by its worker. A final state.</summary>
    Discarded,

    /// <summary>Waiting for something outside the queue before it can be scheduled.</summary>
    Pending,

    /// <summary>Failed, and waiting to be retried at <see cref="JobRow.ScheduledAt"/>.</summary>
    Retryable,

    /// <summary>Being worked on.</summary>
    Running,

    /// <summary>Waiting to become available at <see cref="JobRow.ScheduledAt"/>.</summary>
    Scheduled,
}

/// <summary>One failed attempt of a job, as recorded in <see cref="JobRow.Errors"/>.</summary>
/// <param name="At">When the attempt failed.</param>
/// <param name="Attempt">The attempt number, from 1.</param>
/// <param name="Error">The error message.</param>
/// <param name="Trace">The stack trace, truncated, if one was recorded.</param>
public sealed record AttemptError(DateTimeOffset At, int Attempt, string Error, string? Trace);

/// <summary>A job as stored in the database.</summary>
public sealed class JobRow
{
    /// <summary>The job's ID, assigned by the database on insert.</summary>
    public required long Id { get; init; }

    /// <summary>The job's kind, which selects its worker.</summary>
    public required string Kind { get; init; }

    /// <summary>The queue the job runs on.</summary>
    public required string Queue { get; init; }

    /// <summary>The job's current state.</summary>
    public required JobState State { get; init; }

    /// <summary>Priority from 1 (highest) to 4 (lowest).</summary>
    public required int Priority { get; init; }

    /// <summary>The job's arguments, as JSON.</summary>
    public required string ArgsJson { get; init; }

    /// <summary>The job's metadata, as a JSON object.</summary>
    public required string MetadataJson { get; init; }

    /// <summary>The job's tags.</summary>
    public required IReadOnlyList<string> Tags { get; init; }

    /// <summary>How many times the job has been attempted.</summary>
    public required int Attempt { get; init; }

    /// <summary>How many attempts the job gets before it is discarded.</summary>
    public required int MaxAttempts { get; init; }

    /// <summary>When the current or last attempt started, or <c>null</c> if the job has not been attempted.</summary>
    public DateTimeOffset? AttemptedAt { get; init; }

    /// <summary>The IDs of the clients that attempted the job, oldest first.</summary>
    public IReadOnlyList<string> AttemptedBy { get; init; } = [];

    /// <summary>One entry per failed attempt, oldest first.</summary>
    public IReadOnlyList<AttemptError> Errors { get; init; } = [];

    /// <summary>When the job was inserted.</summary>
    public required DateTimeOffset CreatedAt { get; init; }

    /// <summary>When the job is (or was) due to run.</summary>
    public required DateTimeOffset ScheduledAt { get; init; }

    /// <summary>When the job reached a final state, or <c>null</c> if it has not.</summary>
    public DateTimeOffset? FinalizedAt { get; init; }
}
