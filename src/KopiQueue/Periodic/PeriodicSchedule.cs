using System.Globalization;
using Cronos;

namespace KopiQueue;

/// <summary>
/// When a periodic job runs (FR-PER-01): every fixed interval, or on a cron schedule in a time zone. Ticks are points in
/// time that every client computes alike, so a new leader continues the same series of ticks.
/// </summary>
/// <example>
/// <code>
/// PeriodicSchedule.Every(TimeSpan.FromMinutes(15));                  // :00, :15, :30 and :45 of every hour
/// PeriodicSchedule.Cron("0 2 * * *");                                // 02:00 UTC every day (UC-3)
/// PeriodicSchedule.Cron("30 9 * * 1-5", TimeZoneInfo.FindSystemTimeZoneById("Asia/Singapore"));
/// PeriodicSchedule.Cron("CRON_TZ=Europe/Berlin @daily");             // River's portable form
/// </code>
/// </example>
public abstract class PeriodicSchedule
{
    private protected PeriodicSchedule()
    {
    }

    /// <summary>A schedule of every <paramref name="interval"/>, at whole multiples of it since 1970-01-01 UTC.</summary>
    /// <param name="interval">At least one second.</param>
    /// <remarks>
    /// The ticks do not depend on when a client started, so every client and every leader agrees on them. An interval
    /// that divides a day starts its ticks at midnight UTC.
    /// </remarks>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="interval"/> is shorter than a second.</exception>
    public static PeriodicSchedule Every(TimeSpan interval)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(interval, TimeSpan.FromSeconds(1));
        return new IntervalSchedule(interval);
    }

    /// <summary>
    /// A cron schedule: five fields (minute, hour, day of month, month, day of week), or six with seconds first, or a
    /// macro such as <c>@hourly</c>, <c>@daily</c> or <c>@weekly</c>. Parsed by Cronos.
    /// </summary>
    /// <param name="expression">
    /// The expression. It can start with <c>CRON_TZ=&lt;time zone id&gt;</c> and a space, as River's portable form does,
    /// unless <paramref name="timeZone"/> is given.
    /// </param>
    /// <param name="timeZone">The time zone the fields are read in. Default: UTC.</param>
    /// <remarks>
    /// Daylight saving time follows Cronos: a time skipped by the change runs once at the change, and an hour that
    /// repeats runs once, unless the expression fires more than once an hour.
    /// </remarks>
    /// <exception cref="FormatException">The expression is not valid.</exception>
    /// <exception cref="ArgumentException">The expression names a time zone and <paramref name="timeZone"/> is given too.</exception>
    /// <exception cref="TimeZoneNotFoundException">The time zone in <c>CRON_TZ=</c> is not known.</exception>
    public static PeriodicSchedule Cron(string expression, TimeZoneInfo? timeZone = null)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(expression);
        var text = expression.Trim();
        if (text.StartsWith("CRON_TZ=", StringComparison.Ordinal) || text.StartsWith("TZ=", StringComparison.Ordinal))
        {
            if (timeZone is not null)
            {
                throw new ArgumentException("The expression names its time zone, so timeZone must be null.", nameof(timeZone));
            }

            var space = text.IndexOf(' ', StringComparison.Ordinal);
            if (space < 0)
            {
                throw new FormatException($"The cron expression '{expression}' has a time zone but no schedule.");
            }

            var id = text[(text.IndexOf('=', StringComparison.Ordinal) + 1)..space];
            timeZone = TimeZoneInfo.FindSystemTimeZoneById(id);
            text = text[(space + 1)..].TrimStart();
        }

        var fields = text.StartsWith('@') ? 5 : text.Split(' ', StringSplitOptions.RemoveEmptyEntries).Length;
        if (fields is not (5 or 6))
        {
            throw new FormatException($"The cron expression '{expression}' has {fields.ToString(CultureInfo.InvariantCulture)} fields; it needs 5, or 6 with seconds.");
        }

        CronExpression parsed;
        try
        {
            parsed = CronExpression.Parse(text, fields == 6 ? CronFormat.IncludeSeconds : CronFormat.Standard);
        }
        catch (CronFormatException e)
        {
            throw new FormatException($"The cron expression '{expression}' is not valid: {e.Message}", e);
        }

        return new CronSchedule(parsed, timeZone ?? TimeZoneInfo.Utc, expression);
    }

    /// <summary>The first tick after <paramref name="after"/>, or <c>null</c> if there is none.</summary>
    public abstract DateTimeOffset? NextAfter(DateTimeOffset after);

    /// <summary>The last tick at or before <paramref name="atOrBefore"/>, or <c>null</c> if there is none.</summary>
    public abstract DateTimeOffset? LatestAtOrBefore(DateTimeOffset atOrBefore);

    private sealed class IntervalSchedule(TimeSpan interval) : PeriodicSchedule
    {
        public override DateTimeOffset? NextAfter(DateTimeOffset after) => LatestAtOrBefore(after) + interval;

        public override DateTimeOffset? LatestAtOrBefore(DateTimeOffset atOrBefore)
        {
            var ticks = atOrBefore.UtcTicks - DateTimeOffset.UnixEpoch.UtcTicks;
            var start = ticks - (((ticks % interval.Ticks) + interval.Ticks) % interval.Ticks);
            return DateTimeOffset.UnixEpoch.AddTicks(start);
        }

        public override string ToString() => $"every {interval}";
    }

    private sealed class CronSchedule(CronExpression expression, TimeZoneInfo zone, string text) : PeriodicSchedule
    {
        public override DateTimeOffset? NextAfter(DateTimeOffset after) =>
            expression.GetNextOccurrence(after.ToUniversalTime(), zone, inclusive: false)?.ToUniversalTime();

        public override DateTimeOffset? LatestAtOrBefore(DateTimeOffset atOrBefore) =>
            expression.GetPreviousOccurrence(atOrBefore.ToUniversalTime(), zone, inclusive: true)?.ToUniversalTime();

        public override string ToString() => $"cron '{text}' in {zone.Id}";
    }
}
