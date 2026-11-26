namespace KopiQueue.Execution;

/// <summary>
/// The default retry delay (FR-ERR-02, design §5.4): <c>attempt^4</c> seconds, ±10% jitter, at most 24 hours.
/// Attempt 1 waits about 1 s, 2 about 16 s, 3 about 81 s, 5 about 10 min and 10 about 2.8 h.
/// </summary>
internal static class RetryBackoff
{
    /// <summary>The longest delay.</summary>
    public static readonly TimeSpan Max = TimeSpan.FromHours(24);

    /// <summary>The delay after failed attempt <paramref name="attempt"/>.</summary>
    /// <param name="attempt">The attempt that failed, from 1.</param>
    /// <param name="random">A number in [0, 1), which picks the jitter.</param>
    public static TimeSpan Delay(int attempt, double random)
    {
        var seconds = Math.Pow(Math.Max(attempt, 1), 4);
        var jittered = seconds * (0.9 + (0.2 * random));
        return jittered >= Max.TotalSeconds ? Max : TimeSpan.FromSeconds(jittered);
    }
}
