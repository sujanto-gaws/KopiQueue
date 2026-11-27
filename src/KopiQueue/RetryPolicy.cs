namespace KopiQueue;

/// <summary>
/// Decides when a failed job runs again (FR-ERR-01, FR-ERR-02; design §5.4). A worker's <c>NextRetry</c> override wins
/// over the client's policy.
/// </summary>
public interface IRetryPolicy
{
    /// <summary>
    /// When <paramref name="job"/> should run again after the attempt that just failed. The job's
    /// <see cref="JobRow.Errors"/> do not yet include that attempt's error.
    /// </summary>
    /// <param name="job">The job, as fetched for the failed attempt.</param>
    /// <param name="now">The current time.</param>
    /// <returns>The time to run again. A time before <paramref name="now"/> makes the client use <see cref="DefaultRetryPolicy"/>.</returns>
    DateTimeOffset NextRetry(JobRow job, DateTimeOffset now);
}

/// <summary>
/// The default retry policy, as River's: <c>n^4</c> seconds with ±10% jitter, where <c>n</c> counts the job's errors
/// including this one, at most <see cref="MaxDelay"/>. The first error waits about 1 s, the second 16 s, the third
/// 81 s, the fifth 10 minutes and the tenth 2.8 hours; with the default 25 attempts a job is retried for about three
/// weeks. Counting errors rather than attempts keeps snoozes and interruptions, which use no attempt, out of it.
/// </summary>
public sealed class DefaultRetryPolicy : IRetryPolicy
{
    private readonly Func<double> _random;

    /// <summary>Creates the policy.</summary>
    public DefaultRetryPolicy()
        : this(Random.Shared.NextDouble)
    {
    }

    /// <summary>Creates the policy with a jitter source, for tests.</summary>
    internal DefaultRetryPolicy(Func<double> random) => _random = random;

    /// <summary>A shared instance.</summary>
    public static DefaultRetryPolicy Instance { get; } = new();

    /// <summary>The longest delay. Default: 24 hours.</summary>
    /// <exception cref="ArgumentOutOfRangeException">The value is not positive.</exception>
    public TimeSpan MaxDelay
    {
        get;
        init
        {
            ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(value, TimeSpan.Zero);
            field = value;
        }
    } = TimeSpan.FromHours(24);

    /// <inheritdoc />
    public DateTimeOffset NextRetry(JobRow job, DateTimeOffset now)
    {
        ArgumentNullException.ThrowIfNull(job);
        return now + Delay(job.Errors.Count + 1, _random());
    }

    /// <summary>The delay after the <paramref name="errorCount"/>th error, for a jitter draw in [0, 1).</summary>
    internal TimeSpan Delay(int errorCount, double random)
    {
        var seconds = Math.Pow(Math.Max(errorCount, 1), 4) * (0.9 + (0.2 * random));
        return seconds >= MaxDelay.TotalSeconds ? MaxDelay : TimeSpan.FromSeconds(seconds);
    }
}

/// <summary>
/// When a job runs again after an attempt that did not succeed (design §5.4): the worker's <c>NextRetry</c> delay, else
/// the client's policy. A policy that throws, or a time before now, falls back to <see cref="DefaultRetryPolicy"/>, as in
/// River. Shared by the executor and the rescuer, so a failed and a stuck attempt are retried by the same rules.
/// </summary>
internal static class RetrySchedule
{
    /// <summary>The time to run <paramref name="job"/> again.</summary>
    /// <param name="job">The job, with the attempt that did not succeed; its errors do not include that attempt's yet.</param>
    /// <param name="workerDelay">The worker's delay, or <c>null</c> to ask <paramref name="policy"/>.</param>
    /// <param name="policy">The client's retry policy.</param>
    /// <param name="now">The current time.</param>
    /// <param name="onPolicyFailed">Called when the policy throws.</param>
    /// <param name="onInThePast">Called when the time was before <paramref name="now"/> and the default policy is used.</param>
    public static DateTimeOffset Next(
        JobRow job,
        TimeSpan? workerDelay,
        IRetryPolicy policy,
        DateTimeOffset now,
        Action<Exception>? onPolicyFailed = null,
        Action? onInThePast = null)
    {
        DateTimeOffset at;
        try
        {
            at = workerDelay is { } delay ? now + delay : policy.NextRetry(job, now);
        }
#pragma warning disable CA1031 // A retry policy that throws must not lose the outcome.
        catch (Exception e)
#pragma warning restore CA1031
        {
            onPolicyFailed?.Invoke(e);
            at = DateTimeOffset.MinValue;
        }

        if (at < now)
        {
            onInThePast?.Invoke();
            at = DefaultRetryPolicy.Instance.NextRetry(job, now);
        }

        return at;
    }
}
