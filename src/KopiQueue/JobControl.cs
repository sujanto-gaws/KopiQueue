namespace KopiQueue;

/// <summary>
/// Ends a job's attempt with an outcome other than success or error, from inside <c>WorkAsync</c> (FR-ERR-03,
/// FR-ERR-04; ADR-004). Throw what these methods return:
/// <code>
/// throw JobControl.Cancel("customer deleted");
/// throw JobControl.Snooze(TimeSpan.FromMinutes(5));
/// </code>
/// </summary>
/// <remarks>
/// The exceptions work from helper methods and across <c>await</c>s, and are recognised when wrapped, for example in
/// an <see cref="AggregateException"/> or as an <see cref="Exception.InnerException"/>.
/// </remarks>
public static class JobControl
{
    /// <summary>
    /// Cancels the job for good: it moves to <see cref="JobState.Cancelled"/> with no further attempts, whatever its
    /// attempts left, and <paramref name="reason"/> is recorded in its errors.
    /// </summary>
    /// <param name="reason">Why the job was cancelled.</param>
    /// <returns>The exception to throw.</returns>
    public static JobCancelException Cancel(string reason) => new(reason);

    /// <summary>
    /// Reschedules the job to run again after <paramref name="delay"/> without using up an attempt. No error is
    /// recorded; the job's <c>snoozes</c> metadata counts how often it was snoozed.
    /// </summary>
    /// <param name="delay">How long to wait; zero runs it again as soon as possible.</param>
    /// <returns>The exception to throw.</returns>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="delay"/> is negative.</exception>
    public static JobSnoozeException Snooze(TimeSpan delay) => new(delay);
}

/// <summary>Thrown from a worker to cancel its job for good; create it with <see cref="JobControl.Cancel"/>.</summary>
public sealed class JobCancelException : Exception
{
    /// <summary>Creates the exception.</summary>
    /// <param name="reason">Why the job was cancelled.</param>
    public JobCancelException(string reason)
        : base(reason)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(reason);
    }

    /// <summary>Creates the exception for a cancellation caused by <paramref name="innerException"/>.</summary>
    /// <param name="reason">Why the job was cancelled.</param>
    /// <param name="innerException">What led to the cancellation.</param>
    public JobCancelException(string reason, Exception? innerException)
        : base(reason, innerException)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(reason);
    }

    /// <summary>Why the job was cancelled.</summary>
    public string Reason => Message;
}

/// <summary>Thrown from a worker to run its job again later; create it with <see cref="JobControl.Snooze"/>.</summary>
public sealed class JobSnoozeException : Exception
{
    /// <summary>Creates the exception.</summary>
    /// <param name="delay">How long to wait before the job runs again.</param>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="delay"/> is negative.</exception>
    public JobSnoozeException(TimeSpan delay)
        : base($"The job was snoozed for {delay}.")
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(delay, TimeSpan.Zero);
        Delay = delay;
    }

    /// <summary>How long to wait before the job runs again.</summary>
    public TimeSpan Delay { get; }
}
