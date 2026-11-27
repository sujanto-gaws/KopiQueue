using System.Globalization;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;

namespace KopiQueue;

/// <summary>
/// Builds a job's unique key (design §5.6, FR-UNQ-01, FR-UNQ-06), shared by both drivers. Reproduces River v0.51.2's keys
/// (the 22 golden vectors of spike 0.8, recorded black-box).
/// </summary>
/// <remarks>
/// <para>
/// The key is the SHA-256 of the enabled parts, in this order: <c>&amp;kind=</c> and the kind (unless
/// <see cref="UniqueOptions.ExcludeKind"/>), <c>&amp;args=</c> and the canonical args (<see cref="UniqueOptions.ByArgs"/>),
/// <c>&amp;period=</c> and the start of the period as RFC 3339 UTC to the second (<see cref="UniqueOptions.ByPeriod"/>),
/// and <c>&amp;queue=</c> and the queue (<see cref="UniqueOptions.ByQueue"/>). The unique states and the period's length
/// are not part of it.
/// </para>
/// <para>
/// The canonical args are the top-level members of the args' JSON object, only those named in the unique members when
/// there are any, sorted by their names' UTF-8 bytes: <c>{"a":1,"b":{"z":2,"y":3}}</c>. Nested objects keep their order.
/// </para>
/// </remarks>
internal static class UniqueKey
{
    /// <summary>The key of a job: 32 bytes, for the <c>unique_key</c> column.</summary>
    /// <param name="kind">The job's kind.</param>
    /// <param name="argsJson">The job's serialised args, a JSON object when <see cref="UniqueOptions.ByArgs"/> is set.</param>
    /// <param name="queue">The job's queue.</param>
    /// <param name="now">The time whose period the job falls in, for <see cref="UniqueOptions.ByPeriod"/>.</param>
    /// <param name="options">The dimensions.</param>
    /// <param name="uniqueMembers">The JSON names of the args' members marked unique, or <c>null</c> to use them all.</param>
    /// <param name="encoding">How the args' values are written. Default: <see cref="UniqueArgsEncoding.GoCompatible"/>.</param>
    /// <exception cref="ArgumentException">
    /// The args are not a JSON object while <see cref="UniqueOptions.ByArgs"/> is set, the period is not positive, or
    /// <see cref="UniqueOptions.ExcludeKind"/> leaves nothing to hash.
    /// </exception>
    public static byte[] Compute(
        string kind,
        string argsJson,
        string queue,
        DateTimeOffset now,
        UniqueOptions options,
        IReadOnlySet<string>? uniqueMembers = null,
        UniqueArgsEncoding encoding = UniqueArgsEncoding.GoCompatible) =>
        SHA256.HashData(Encoding.UTF8.GetBytes(Preimage(kind, argsJson, queue, now, options, uniqueMembers, encoding)));

    /// <summary>The text whose SHA-256 is the key: see <see cref="Compute"/>.</summary>
    public static string Preimage(
        string kind,
        string argsJson,
        string queue,
        DateTimeOffset now,
        UniqueOptions options,
        IReadOnlySet<string>? uniqueMembers = null,
        UniqueArgsEncoding encoding = UniqueArgsEncoding.GoCompatible)
    {
        ArgumentException.ThrowIfNullOrEmpty(kind);
        ArgumentNullException.ThrowIfNull(argsJson);
        ArgumentException.ThrowIfNullOrEmpty(queue);
        ArgumentNullException.ThrowIfNull(options);

        var text = new StringBuilder();
        if (!options.ExcludeKind)
        {
            text.Append("&kind=").Append(kind);
        }

        if (options.ByArgs)
        {
            text.Append("&args=");
            AppendCanonicalArgs(text, argsJson, uniqueMembers, encoding);
        }

        if (options.ByPeriod is { } period)
        {
            if (period <= TimeSpan.Zero)
            {
                throw new ArgumentException($"The unique period must be positive, not {period}.");
            }

            text.Append("&period=").Append(PeriodStart(now, period).ToString("yyyy-MM-dd'T'HH:mm:ss'Z'", CultureInfo.InvariantCulture));
        }

        if (options.ByQueue)
        {
            text.Append("&queue=").Append(queue);
        }

        if (text.Length == 0)
        {
            throw new ArgumentException(
                "A unique job that excludes its kind needs another dimension (ByArgs, ByPeriod or ByQueue); otherwise every such job of every kind would share one slot.");
        }

        return text.ToString();
    }

    /// <summary>
    /// The start of the period <paramref name="now"/> falls in: <paramref name="now"/> to the whole second, rounded down to
    /// a multiple of <paramref name="period"/> since the Unix epoch.
    /// </summary>
    public static DateTimeOffset PeriodStart(DateTimeOffset now, TimeSpan period)
    {
        var ticks = now.ToUnixTimeSeconds() * TimeSpan.TicksPerSecond;
        var start = ticks - (((ticks % period.Ticks) + period.Ticks) % period.Ticks);
        return DateTimeOffset.UnixEpoch.AddTicks(start);
    }

    private static void AppendCanonicalArgs(StringBuilder text, string argsJson, IReadOnlySet<string>? uniqueMembers, UniqueArgsEncoding encoding)
    {
        JsonDocument document;
        try
        {
            document = JsonDocument.Parse(argsJson);
        }
        catch (JsonException e)
        {
            throw new ArgumentException("The args are not valid JSON, so they cannot be part of a unique key.", e);
        }

        using (document)
        {
            if (document.RootElement.ValueKind != JsonValueKind.Object)
            {
                throw new ArgumentException(
                    $"Unique by args needs args that serialise to a JSON object, not {document.RootElement.ValueKind}.");
            }

            var members = document.RootElement.EnumerateObject()
                .Where(m => uniqueMembers is null || uniqueMembers.Contains(m.Name))
                .Select(m => (Name: m.Name, Utf8: Encoding.UTF8.GetBytes(m.Name), m.Value))
                .OrderBy(m => m.Utf8, Utf8Order.Instance) // stable: duplicate names keep their order
                .ToList();

            text.Append('{');
            for (var i = 0; i < members.Count; i++)
            {
                if (i > 0)
                {
                    text.Append(',');
                }

                GoJson.AppendString(text, members[i].Name);
                text.Append(':');
                if (encoding == UniqueArgsEncoding.Verbatim)
                {
                    text.Append(members[i].Value.GetRawText());
                }
                else
                {
                    GoJson.AppendValue(text, members[i].Value);
                }
            }

            text.Append('}');
        }
    }

    private sealed class Utf8Order : IComparer<byte[]>
    {
        public static readonly Utf8Order Instance = new();

        public int Compare(byte[]? x, byte[]? y) => x.AsSpan().SequenceCompareTo(y);
    }
}

/// <summary>
/// Writes JSON as Go's <c>encoding/json</c> does by default (DQ-8), so unique keys match those of Go producers: compact,
/// members in the order given, strings escaped HTML-safely, and floating-point numbers in Go's shortest form.
/// </summary>
internal static class GoJson
{
    private static readonly System.Buffers.SearchValues<char> Digits = System.Buffers.SearchValues.Create("0123456789");

    /// <summary>Appends <paramref name="value"/>, keeping the order of nested members.</summary>
    public static void AppendValue(StringBuilder text, JsonElement value)
    {
        switch (value.ValueKind)
        {
            case JsonValueKind.Object:
                text.Append('{');
                var first = true;
                foreach (var member in value.EnumerateObject())
                {
                    if (!first)
                    {
                        text.Append(',');
                    }

                    first = false;
                    AppendString(text, member.Name);
                    text.Append(':');
                    AppendValue(text, member.Value);
                }

                text.Append('}');
                break;
            case JsonValueKind.Array:
                text.Append('[');
                var index = 0;
                foreach (var item in value.EnumerateArray())
                {
                    if (index++ > 0)
                    {
                        text.Append(',');
                    }

                    AppendValue(text, item);
                }

                text.Append(']');
                break;
            case JsonValueKind.String:
                string decoded;
                try
                {
                    decoded = value.GetString()!;
                }
                catch (InvalidOperationException)
                {
                    text.Append(value.GetRawText()); // not valid UTF-16 (a lone surrogate): kept as it was written
                    break;
                }

                AppendString(text, decoded);
                break;
            case JsonValueKind.Number:
                text.Append(Number(value.GetRawText()));
                break;
            case JsonValueKind.True:
                text.Append("true");
                break;
            case JsonValueKind.False:
                text.Append("false");
                break;
            default:
                text.Append("null");
                break;
        }
    }

    /// <summary>
    /// Appends <paramref name="value"/> as a JSON string the way Go does: <c>"</c>, <c>\</c> and control characters
    /// escaped (<c>\n</c>, <c>\r</c>, <c>\t</c>, <c>\b</c>, <c>\f</c>, else <c>\u00XX</c>), <c>&lt;</c>, <c>&gt;</c>,
    /// <c>&amp;</c>, U+2028 and U+2029 as <c>\u003c</c>, <c>\u003e</c>, <c>\u0026</c>, <c>\u2028</c> and <c>\u2029</c>,
    /// a lone surrogate as <c>\ufffd</c>, and everything else as it is.
    /// </summary>
    public static void AppendString(StringBuilder text, string value)
    {
        text.Append('"');
        for (var i = 0; i < value.Length; i++)
        {
            var c = value[i];
            switch (c)
            {
                case '"':
                    text.Append("\\\"");
                    break;
                case '\\':
                    text.Append("\\\\");
                    break;
                case '\n':
                    text.Append("\\n");
                    break;
                case '\r':
                    text.Append("\\r");
                    break;
                case '\t':
                    text.Append("\\t");
                    break;
                case '\b':
                    text.Append("\\b");
                    break;
                case '\f':
                    text.Append("\\f");
                    break;
                case '<' or '>' or '&' or '\u2028' or '\u2029':
                    text.Append("\\u").Append(((int)c).ToString("x4", CultureInfo.InvariantCulture));
                    break;
                case < ' ':
                    text.Append("\\u").Append(((int)c).ToString("x4", CultureInfo.InvariantCulture));
                    break;
                default:
                    if (char.IsHighSurrogate(c) && i + 1 < value.Length && char.IsLowSurrogate(value[i + 1]))
                    {
                        text.Append(c).Append(value[++i]);
                    }
                    else if (char.IsSurrogate(c))
                    {
                        text.Append("\\ufffd");
                    }
                    else
                    {
                        text.Append(c);
                    }

                    break;
            }
        }

        text.Append('"');
    }

    /// <summary>
    /// A JSON number as Go writes it. An integer literal is kept as it is, so integers beyond a double's precision stay
    /// exact (Go's integer types). Anything else is read as a double and written in Go's <c>float64</c> form: the
    /// shortest digits that round-trip, in plain notation from 1e-6 up to 1e21 and otherwise as <c>1e-7</c> or <c>1e+21</c>.
    /// </summary>
    public static string Number(string literal)
    {
        var digitsOnly = literal.AsSpan(literal.StartsWith('-') ? 1 : 0);
        if (digitsOnly.Length > 0 && !digitsOnly.ContainsAnyExcept(Digits))
        {
            return literal;
        }

        var value = double.Parse(literal, NumberStyles.Float, CultureInfo.InvariantCulture);
        return Float(value);
    }

    /// <summary>A double in Go's <c>encoding/json</c> <c>float64</c> form.</summary>
    public static string Float(double value)
    {
        if (!double.IsFinite(value))
        {
            throw new ArgumentOutOfRangeException(nameof(value), value, "JSON has no representation for NaN or infinity.");
        }

        // The shortest round-trip digits and the decimal point's position: value = 0.<digits> × 10^point.
        var r = value.ToString("R", CultureInfo.InvariantCulture);
        var negative = r.StartsWith('-');
        if (negative)
        {
            r = r[1..];
        }

        var e = r.IndexOfAny(['E', 'e']);
        var mantissa = e < 0 ? r : r[..e];
        var exponent = e < 0 ? 0 : int.Parse(r[(e + 1)..], NumberStyles.AllowLeadingSign, CultureInfo.InvariantCulture);
        var dot = mantissa.IndexOf('.', StringComparison.Ordinal);
        var digits = mantissa.Replace(".", string.Empty, StringComparison.Ordinal);
        var point = (dot < 0 ? mantissa.Length : dot) + exponent;
        var lead = 0;
        while (lead < digits.Length - 1 && digits[lead] == '0')
        {
            lead++;
        }

        digits = digits[lead..].TrimEnd('0');
        point -= lead;
        var sign = negative ? "-" : string.Empty;
        if (digits.Length == 0)
        {
            return sign + "0";
        }

        var magnitude = Math.Abs(value);
        if (magnitude < 1e-6 || magnitude >= 1e21)
        {
            var exp = point - 1;
            var mant = digits.Length == 1 ? digits : digits[0] + "." + digits[1..];

            // Go's 'e' format pads the exponent to two digits; encoding/json then drops the padding of a negative one.
            var expText = exp < 0
                ? "-" + (-exp).ToString(CultureInfo.InvariantCulture)
                : "+" + exp.ToString("00", CultureInfo.InvariantCulture);
            return sign + mant + "e" + expText;
        }

        if (point <= 0)
        {
            return sign + "0." + new string('0', -point) + digits;
        }

        return point >= digits.Length
            ? sign + digits + new string('0', point - digits.Length)
            : sign + digits[..point] + "." + digits[point..];
    }
}
