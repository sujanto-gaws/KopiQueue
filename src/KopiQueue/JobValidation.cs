using System.Globalization;
using KopiQueue.Drivers;

namespace KopiQueue;

/// <summary>Input validation for kinds, queues, tags and insert options (FR-INS-05, FR-INS-09).</summary>
/// <remarks>Each <c>...Error</c> method returns <c>null</c> for a valid value, or a message saying what is wrong.</remarks>
internal static class JobValidation
{
    /// <summary>Kinds and queues: the columns' CHECK allows 1-127 characters.</summary>
    public const int MaxNameLength = 127;

    public const int MaxTagLength = 255;

    public const int MinPriority = 1;

    public const int MaxPriority = 4;

    public static string? KindError(string? kind) =>
        kind is null || kind.Length is 0 or > MaxNameLength
            ? $"the kind must be 1-{MaxNameLength} characters, but is {Describe(kind)}."
            : null;

    public static string? QueueError(string? queue)
    {
        if (queue is null || queue.Length is 0 or > MaxNameLength)
        {
            return $"the queue name must be 1-{MaxNameLength} characters, but is {Describe(queue)}.";
        }

        foreach (var c in queue)
        {
            if (!IsLowerAlphanumeric(c) && c is not '_' and not '-')
            {
                return $"the queue name '{queue}' may contain only lower-case ASCII letters, digits, '_' and '-'.";
            }
        }

        return IsLowerAlphanumeric(queue[0]) && IsLowerAlphanumeric(queue[^1])
            ? null
            : $"the queue name '{queue}' must start and end with a lower-case letter or digit.";
    }

    /// <summary>River's rule, <c>\A[\w][\w\-]+[\w]\z</c> with Go's ASCII <c>\w</c>, up to 255 characters (spike 0.8 D7).</summary>
    public static string? TagError(string? tag)
    {
        if (tag is null || tag.Length is < 3 or > MaxTagLength)
        {
            return $"each tag must be 3-{MaxTagLength} characters, but one is {Describe(tag)}.";
        }

        for (var i = 0; i < tag.Length; i++)
        {
            var c = tag[i];
            var edge = i == 0 || i == tag.Length - 1;
            if (!IsWord(c) && (edge || c != '-'))
            {
                return $"the tag '{tag}' must contain only ASCII letters, digits, '_' and '-', and must not start or end with '-'.";
            }
        }

        return null;
    }

    /// <summary>
    /// Validates <paramref name="options"/> and resolves it, with the defaults, into the driver's insert parameters.
    /// </summary>
    /// <param name="kind">The job's kind, already validated by <see cref="JobKindRegistry"/>.</param>
    /// <param name="argsJson">The serialised args.</param>
    /// <param name="options">The caller's options, or <c>null</c> for all defaults.</param>
    /// <param name="now">
    /// The client's current time: to tell a future <see cref="InsertOptions.ScheduledAt"/>, and the time whose period a
    /// unique job falls in (<see cref="UniqueOptions.ByPeriod"/>).
    /// </param>
    /// <exception cref="ArgumentException">An option is not valid. The message names it.</exception>
    /// <param name="subject">Prefixed to error messages, for example <c>"Job 3: "</c> in a batch.</param>
    /// <param name="paramName">The parameter named by an <see cref="ArgumentException"/>.</param>
    /// <param name="unique">How to build a unique key; <c>null</c> for all members and the Go-compatible encoding.</param>
    public static JobInsertParams ToInsertParams(
        string kind,
        string argsJson,
        InsertOptions? options,
        DateTimeOffset now,
        string subject = "",
        string paramName = "options",
        UniqueContext? unique = null)
    {
        ArgumentNullException.ThrowIfNull(kind);
        ArgumentNullException.ThrowIfNull(argsJson);

        var queue = options?.Queue ?? InsertDefaults.Queue;
        var priority = options?.Priority ?? InsertDefaults.Priority;
        var maxAttempts = options?.MaxAttempts ?? InsertDefaults.MaxAttempts;
        var tags = options?.Tags ?? [];
        var scheduledAt = options?.ScheduledAt;

        Check(QueueError(queue), nameof(InsertOptions.Queue), subject, paramName);
        Check(
            priority is >= MinPriority and <= MaxPriority ? null : $"the priority must be {MinPriority}-{MaxPriority}, but is {Format(priority)}.",
            nameof(InsertOptions.Priority), subject, paramName);
        Check(
            maxAttempts is >= 1 and <= short.MaxValue ? null : $"max attempts must be 1-{Format(short.MaxValue)}, but is {Format(maxAttempts)}.",
            nameof(InsertOptions.MaxAttempts), subject, paramName);
        foreach (var tag in tags)
        {
            Check(TagError(tag), nameof(InsertOptions.Tags), subject, paramName);
        }

        byte[]? uniqueKey = null;
        byte? uniqueStates = null;
        if (options?.Unique is { IsEmpty: false } uniqueOptions)
        {
            try
            {
                uniqueStates = UniqueStates.ToBitmask(uniqueOptions.ByState);
                var members = uniqueOptions.ByArgs ? unique?.Members() : null;
                uniqueKey = UniqueKey.Compute(
                    kind, argsJson, queue, now, uniqueOptions, members is { Count: > 0 } ? members : null,
                    unique?.Encoding ?? UniqueArgsEncoding.GoCompatible);
            }
            catch (ArgumentException e)
            {
                Check(char.ToLowerInvariant(e.Message[0]) + e.Message[1..], nameof(InsertOptions.Unique), subject, paramName);
            }
        }

        return new JobInsertParams
        {
            Kind = kind,
            ArgsJson = argsJson,
            Queue = queue,
            State = scheduledAt > now ? JobState.Scheduled : JobState.Available,
            Priority = priority,
            MaxAttempts = maxAttempts,
            ScheduledAt = scheduledAt,
            MetadataJson = options?.Metadata?.ToJsonString() ?? "{}",
            Tags = [.. tags],
            UniqueKey = uniqueKey,
            UniqueStates = uniqueStates,
        };
    }

    /// <summary>What building a unique key needs beyond the options.</summary>
    /// <param name="Members">The JSON names of the args' members marked unique; empty when none are. Called only for <see cref="UniqueOptions.ByArgs"/>.</param>
    /// <param name="Encoding">How arg values are written into the key.</param>
    public sealed record UniqueContext(Func<IReadOnlySet<string>> Members, UniqueArgsEncoding Encoding);

    private static void Check(string? error, string option, string subject, string paramName)
    {
        if (error is not null)
        {
            throw new ArgumentException($"{subject}Invalid insert option {option}: {error}", paramName);
        }
    }

    private static bool IsLowerAlphanumeric(char c) => c is (>= 'a' and <= 'z') or (>= '0' and <= '9');

    private static bool IsWord(char c) => c is (>= 'a' and <= 'z') or (>= 'A' and <= 'Z') or (>= '0' and <= '9') or '_';

    private static string Describe(string? value) =>
        value is null ? "null" : $"{Format(value.Length)} characters long";

    private static string Format(int value) => value.ToString(CultureInfo.InvariantCulture);
}
