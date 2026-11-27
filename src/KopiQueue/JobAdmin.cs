using System.Buffers.Text;
using System.Globalization;
using System.Text;
using System.Text.Json.Nodes;
using KopiQueue.Drivers;

namespace KopiQueue;

/// <summary>
/// Which jobs an admin call matches (FR-ADM-02, FR-ADM-03): the jobs that pass every filter set. A <c>null</c> filter
/// matches any job; an empty list matches none.
/// </summary>
public abstract class JobFilter
{
    private protected JobFilter()
    {
    }

    /// <summary>Only jobs with these IDs. Default: any.</summary>
    public IReadOnlyList<long>? Ids { get; init; }

    /// <summary>Only jobs of these kinds. Default: any.</summary>
    public IReadOnlyList<string>? Kinds { get; init; }

    /// <summary>Only jobs in these queues. Default: any.</summary>
    public IReadOnlyList<string>? Queues { get; init; }

    /// <summary>Only jobs in these states. Default: any.</summary>
    public IReadOnlyList<JobState>? States { get; init; }

    /// <summary>Only jobs with these priorities, each from 1 to 4. Default: any.</summary>
    public IReadOnlyList<int>? Priorities { get; init; }

    /// <summary>
    /// Only jobs whose metadata contains this JSON object: every key it has, with the same value, objects and arrays
    /// matched by containment, as PostgreSQL's <c>@&gt;</c> does. Default: any.
    /// </summary>
    /// <example><c>MetadataContains = new JsonObject { ["tenant"] = "acme" }</c></example>
    public JsonObject? MetadataContains { get; init; }

    /// <summary>Whether a filter is an empty list, so no job can match.</summary>
    internal bool MatchesNothing =>
        Ids is { Count: 0 } || Kinds is { Count: 0 } || Queues is { Count: 0 } || States is { Count: 0 } || Priorities is { Count: 0 };

    /// <summary>Checks the filters, before any SQL runs.</summary>
    /// <exception cref="ArgumentException">A filter holds a value no job can have.</exception>
    internal void Validate(string paramName)
    {
        if (Kinds?.Any(string.IsNullOrEmpty) == true)
        {
            throw new ArgumentException("Kinds has an empty kind.", paramName);
        }

        if (Queues?.Any(string.IsNullOrEmpty) == true)
        {
            throw new ArgumentException("Queues has an empty queue.", paramName);
        }

        foreach (var state in States ?? [])
        {
            if (!Enum.IsDefined(state))
            {
                throw new ArgumentException($"States has an unknown state ({(int)state}).", paramName);
            }
        }

        foreach (var priority in Priorities ?? [])
        {
            if (priority is < 1 or > 4)
            {
                throw new ArgumentException($"Priorities has {priority}: priorities are from 1 to 4.", paramName);
            }
        }
    }
}

/// <summary>What <see cref="JobListParams"/> sorts jobs by. Jobs that sort equal are in ID order.</summary>
public enum JobListOrderBy
{
    /// <summary>The job's ID, which is the order jobs were inserted in.</summary>
    Id,

    /// <summary><see cref="JobRow.CreatedAt"/>.</summary>
    CreatedAt,

    /// <summary><see cref="JobRow.ScheduledAt"/>.</summary>
    ScheduledAt,

    /// <summary><see cref="JobRow.AttemptedAt"/>. Jobs never attempted sort last, or first when descending.</summary>
    AttemptedAt,

    /// <summary><see cref="JobRow.FinalizedAt"/>. Jobs not finalised sort last, or first when descending.</summary>
    FinalizedAt,
}

/// <summary>Which jobs <see cref="IKopiClient.JobListAsync"/> returns, in what order, and from where (FR-ADM-02).</summary>
/// <example>
/// <code>
/// var page = await client.JobListAsync(new JobListParams { States = [JobState.Discarded], OrderBy = JobListOrderBy.FinalizedAt, Descending = true });
/// while (page.NextCursor is { } next)
/// {
///     page = await client.JobListAsync(new JobListParams { States = [JobState.Discarded], OrderBy = JobListOrderBy.FinalizedAt, Descending = true, After = next });
/// }
/// </code>
/// </example>
public sealed class JobListParams : JobFilter
{
    /// <summary>The most jobs to return, from 1 to 10,000. Default: 100.</summary>
    public int Limit { get; init; } = 100;

    /// <summary>What to sort by. Default: <see cref="JobListOrderBy.Id"/>.</summary>
    public JobListOrderBy OrderBy { get; init; }

    /// <summary>Sorts from the highest value down. Default: <c>false</c>, ascending.</summary>
    public bool Descending { get; init; }

    /// <summary>
    /// Where the page starts: the <see cref="JobListResult.NextCursor"/> of the previous page, for the same
    /// <see cref="OrderBy"/> and <see cref="Descending"/>. <c>null</c> starts from the first job.
    /// </summary>
    /// <remarks>
    /// The cursor holds the position of the previous page's last job, not the filters, so pass the same filters again.
    /// Pages are keyset pages: a job inserted or changed between two calls does not shift later pages, but a job whose
    /// sort value changes in between (when sorted by a time that changes, such as <see cref="JobListOrderBy.ScheduledAt"/>)
    /// can be returned twice or not at all. Sorted by <see cref="JobListOrderBy.Id"/>, no job is ever returned twice.
    /// </remarks>
    public string? After { get; init; }
}

/// <summary>A page of jobs from <see cref="IKopiClient.JobListAsync"/>.</summary>
public sealed class JobListResult
{
    /// <summary>The jobs, in the order asked for.</summary>
    public required IReadOnlyList<JobRow> Jobs { get; init; }

    /// <summary>
    /// Pass as <see cref="JobListParams.After"/> to get the next page, or <c>null</c> when no job came after this page
    /// at the time of the call. Opaque: its format may change between versions.
    /// </summary>
    public string? NextCursor { get; init; }
}

/// <summary>Which jobs <see cref="IKopiClient.JobDeleteManyAsync(JobDeleteManyParams, CancellationToken)"/> deletes (FR-ADM-03).</summary>
public sealed class JobDeleteManyParams : JobFilter
{
    /// <summary>The most jobs to delete in one call, from 1 to 10,000. Default: 1,000.</summary>
    public int Limit { get; init; } = 1_000;
}

/// <summary>A job cannot be deleted because it is running (<see cref="IKopiClient.JobDeleteAsync(long, CancellationToken)"/>).</summary>
public class KopiQueueJobRunningException : InvalidOperationException
{
    /// <summary>Creates an exception with a default message.</summary>
    public KopiQueueJobRunningException()
    {
    }

    /// <summary>Creates an exception with <paramref name="message"/>.</summary>
    public KopiQueueJobRunningException(string? message)
        : base(message)
    {
    }

    /// <summary>Creates an exception with <paramref name="message"/> caused by <paramref name="innerException"/>.</summary>
    public KopiQueueJobRunningException(string? message, Exception? innerException)
        : base(message, innerException)
    {
    }
}

/// <summary>
/// The cursor of <see cref="JobListResult.NextCursor"/>: <c>"1."</c> and, in base64url, the sort, the direction, the last
/// job's sort value in UTC ticks (<c>-</c> for none) and its ID, separated by colons.
/// </summary>
#pragma warning disable KQ0001 // The executor's position type is experimental; the cursor maps to it.
internal static class JobListCursor
{
    private const string Version = "1.";

    public static string Encode(JobListOrderBy orderBy, bool descending, JobRow last)
    {
        var value = orderBy switch
        {
            JobListOrderBy.Id => null,
            JobListOrderBy.CreatedAt => last.CreatedAt,
            JobListOrderBy.ScheduledAt => last.ScheduledAt,
            JobListOrderBy.AttemptedAt => last.AttemptedAt,
            JobListOrderBy.FinalizedAt => last.FinalizedAt,
            _ => throw new ArgumentOutOfRangeException(nameof(orderBy), orderBy, null),
        };
        var ticks = value is { } v ? v.UtcTicks.ToString(CultureInfo.InvariantCulture) : "-";
        var text = string.Create(CultureInfo.InvariantCulture, $"{(int)orderBy}:{(descending ? 'd' : 'a')}:{ticks}:{last.Id}");
        return Version + Base64Url.EncodeToString(Encoding.UTF8.GetBytes(text));
    }

    /// <exception cref="ArgumentException">The cursor is not one, or was made for another sort or direction.</exception>
    public static JobListPosition Decode(string cursor, JobListOrderBy orderBy, bool descending, string paramName)
    {
        string[] parts;
        try
        {
            parts = cursor.StartsWith(Version, StringComparison.Ordinal)
                ? Encoding.UTF8.GetString(Base64Url.DecodeFromChars(cursor.AsSpan(Version.Length))).Split(':')
                : [];
        }
        catch (FormatException)
        {
            parts = [];
        }

        if (parts.Length != 4
            || !int.TryParse(parts[0], NumberStyles.None, CultureInfo.InvariantCulture, out var order)
            || parts[1] is not ("a" or "d")
            || !long.TryParse(parts[3], NumberStyles.AllowLeadingSign, CultureInfo.InvariantCulture, out var id))
        {
            throw new ArgumentException("After is not a cursor from JobListAsync.", paramName);
        }

        if (order != (int)orderBy || (parts[1] == "d") != descending)
        {
            throw new ArgumentException("After is a cursor for another OrderBy or Descending: pass the same ones as for the page it came from.", paramName);
        }

        DateTimeOffset? value = null;
        if (parts[2] != "-")
        {
            if (!long.TryParse(parts[2], NumberStyles.None, CultureInfo.InvariantCulture, out var ticks)
                || ticks > DateTimeOffset.MaxValue.UtcTicks)
            {
                throw new ArgumentException("After is not a cursor from JobListAsync.", paramName);
            }

            value = new DateTimeOffset(ticks, TimeSpan.Zero);
        }

        return new JobListPosition(id, value);
    }
}
#pragma warning restore KQ0001
