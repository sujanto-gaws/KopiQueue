using System.Globalization;
using System.Text;
using System.Text.Json;

namespace KopiQueue.Drivers;

/// <summary>
/// How a job's state and errors are written in the database, the same on every driver: the state as River's lower-case
/// name, and each error as River's JSON object (design §4.1, §4.4).
/// </summary>
internal static class StoredJob
{
    /// <summary>The stored name of <paramref name="state"/>.</summary>
    public static string ToDatabase(JobState state) => state switch
    {
        JobState.Available => "available",
        JobState.Cancelled => "cancelled",
        JobState.Completed => "completed",
        JobState.Discarded => "discarded",
        JobState.Pending => "pending",
        JobState.Retryable => "retryable",
        JobState.Running => "running",
        JobState.Scheduled => "scheduled",
        _ => throw new ArgumentOutOfRangeException(nameof(state), state, "Unknown job state."),
    };

    /// <summary>The state a stored name stands for.</summary>
    /// <exception cref="InvalidOperationException">The name is not a state.</exception>
    public static JobState FromDatabase(string state) => state switch
    {
        "available" => JobState.Available,
        "cancelled" => JobState.Cancelled,
        "completed" => JobState.Completed,
        "discarded" => JobState.Discarded,
        "pending" => JobState.Pending,
        "retryable" => JobState.Retryable,
        "running" => JobState.Running,
        "scheduled" => JobState.Scheduled,
        _ => throw new InvalidOperationException($"Unknown job state '{state}' in the database."),
    };

    /// <summary>
    /// Formats an error as River stores it: <c>{"at": ..., "attempt": n, "error": "...", "trace": "..."}</c>, with the
    /// time in UTC as Go's RFC 3339 with up to nanosecond fractions, and an empty trace when there is none.
    /// </summary>
    public static string FormatError(AttemptError error)
    {
        ArgumentNullException.ThrowIfNull(error);
        using var buffer = new MemoryStream();
        using (var writer = new Utf8JsonWriter(buffer))
        {
            writer.WriteStartObject();
            writer.WriteString("at", error.At.UtcDateTime.ToString("yyyy-MM-dd'T'HH:mm:ss.FFFFFFF'Z'", CultureInfo.InvariantCulture));
            writer.WriteNumber("attempt", error.Attempt);
            writer.WriteString("error", error.Error);
            writer.WriteString("trace", error.Trace ?? string.Empty);
            writer.WriteEndObject();
        }

        return Encoding.UTF8.GetString(buffer.ToArray());
    }

    /// <summary>Parses one stored error: <c>{"at": ..., "attempt": n, "error": "...", "trace": "..."}</c>.</summary>
    public static AttemptError ParseError(string json)
    {
        using var document = JsonDocument.Parse(json);
        return ParseError(document.RootElement);
    }

    /// <summary>Parses one stored error, already parsed as JSON. Missing or mistyped fields get their defaults.</summary>
    public static AttemptError ParseError(JsonElement root)
    {
        var isObject = root.ValueKind == JsonValueKind.Object;
        var at = isObject && root.TryGetProperty("at", out var a) && a.ValueKind == JsonValueKind.String
            ? DateTimeOffset.Parse(a.GetString()!, CultureInfo.InvariantCulture, DateTimeStyles.AssumeUniversal)
            : default;
        var attempt = isObject && root.TryGetProperty("attempt", out var n) && n.ValueKind == JsonValueKind.Number && n.TryGetInt32(out var value) ? value : 0;
        var error = isObject && root.TryGetProperty("error", out var e) && e.ValueKind == JsonValueKind.String ? e.GetString()! : string.Empty;
        // River stores an empty trace when there is none.
        var trace = isObject && root.TryGetProperty("trace", out var t) && t.ValueKind == JsonValueKind.String && t.GetString() is { Length: > 0 } text
            ? text
            : null;
        return new AttemptError(at, attempt, error, trace);
    }
}
