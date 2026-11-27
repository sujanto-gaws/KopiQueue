namespace KopiQueue;

/// <summary>
/// The unique-states bitmask (design §5.6, FR-UNQ-04): the states in which a job's unique key occupies its slot. Shared
/// by both drivers: PostgreSQL stores it as <c>bit(8)</c> text, SQL Server as a <c>tinyint</c>, with the same positions.
/// </summary>
/// <remarks>
/// As an integer, available = 1, cancelled = 2, completed = 4, discarded = 8, pending = 16, retryable = 32, running = 64
/// and scheduled = 128, so the <c>bit(8)</c> text reads, left to right, scheduled, running, retryable, pending, discarded,
/// completed, cancelled, available (spike 0.8, matched by River v0.51.2's golden vectors).
/// </remarks>
internal static class UniqueStates
{
    /// <summary>The default: every state but cancelled and discarded (<c>11110101</c>).</summary>
    public const byte Default = 0b1111_0101;

    /// <summary>The states every custom set must include: available, pending, running and scheduled (<c>11010001</c>).</summary>
    public const byte Required = 0b1101_0001;

    private static readonly JobState[] All =
    [
        JobState.Available, JobState.Cancelled, JobState.Completed, JobState.Discarded,
        JobState.Pending, JobState.Retryable, JobState.Running, JobState.Scheduled,
    ];

    /// <summary>The bit of <paramref name="state"/>.</summary>
    public static byte Bit(JobState state) => state switch
    {
        JobState.Available => 1,
        JobState.Cancelled => 2,
        JobState.Completed => 4,
        JobState.Discarded => 8,
        JobState.Pending => 16,
        JobState.Retryable => 32,
        JobState.Running => 64,
        JobState.Scheduled => 128,
        _ => throw new ArgumentOutOfRangeException(nameof(state), state, "Not a job state."),
    };

    /// <summary>The bitmask of <paramref name="states"/>, or <see cref="Default"/> for <c>null</c>.</summary>
    /// <exception cref="ArgumentException">The set leaves out available, pending, running or scheduled (FR-UNQ-04).</exception>
    public static byte ToBitmask(IEnumerable<JobState>? states)
    {
        if (states is null)
        {
            return Default;
        }

        byte mask = 0;
        foreach (var state in states)
        {
            mask |= Bit(state);
        }

        if ((mask & Required) != Required)
        {
            var missing = string.Join(", ", FromBitmask((byte)(Required & ~mask)).Select(s => s.ToString().ToLowerInvariant()));
            throw new ArgumentException(
                $"Unique states must include available, pending, running and scheduled; missing: {missing}.",
                nameof(states));
        }

        return mask;
    }

    /// <summary>The states in <paramref name="mask"/>, in the enum's order.</summary>
    public static IReadOnlyList<JobState> FromBitmask(byte mask) => [.. All.Where(s => (mask & Bit(s)) != 0)];

    /// <summary>The mask as PostgreSQL <c>bit(8)</c> text, scheduled first: <c>11110101</c> for <see cref="Default"/>.</summary>
    public static string ToBitString(byte mask) => Convert.ToString(mask, 2).PadLeft(8, '0');
}
