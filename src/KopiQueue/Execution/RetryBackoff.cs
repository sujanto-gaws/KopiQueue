namespace KopiQueue.Execution;

/// <summary>
/// The default retry delay (FR-ERR-02, design §5.4): <c>n^4</c> seconds, ±10% jitter, at most 24 hours, where
/// <c>n</c> is the number of errors including this one. As in River it counts errors, not attempts, so snoozes and
/// interruptions, which use no attempt, do not lengthen it. The first error waits about 1 s, the second 16 s, the third
/// 81 s, the fifth 10 min and the tenth 2.8 h.
/// </summary>
internal static class RetryBackoff
{
    /// <summary>The longest delay.</summary>
    public static readonly TimeSpan Max = TimeSpan.FromHours(24);

    /// <summary>The delay after the <paramref name="errorCount"/>th error.</summary>
    /// <param name="errorCount">How many errors the job has had, including this one, from 1.</param>
    /// <param name="random">A number in [0, 1), which picks the jitter.</param>
    public static TimeSpan Delay(int errorCount, double random)
    {
        var seconds = Math.Pow(Math.Max(errorCount, 1), 4);
        var jittered = seconds * (0.9 + (0.2 * random));
        return jittered >= Max.TotalSeconds ? Max : TimeSpan.FromSeconds(jittered);
    }
}
