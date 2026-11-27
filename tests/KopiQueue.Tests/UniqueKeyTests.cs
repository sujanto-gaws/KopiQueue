using System.Globalization;
using System.Text;
using System.Text.Encodings.Web;
using System.Text.Json;

namespace KopiQueue.Tests;

/// <summary>
/// The unique key and unique states (plan task 4.1: FR-UNQ-01, FR-UNQ-04, FR-UNQ-06; design §5.6; DQ-8), against the 22
/// keys River v0.51.2 computed in spike 0.8 (<c>spikes/RiverSchema/golden</c>, recorded black-box).
/// </summary>
public sealed class UniqueKeyTests
{
    /// <summary>The fields the spike's harness tagged unique on kind <c>golden_tagged</c>.</summary>
    private static readonly IReadOnlySet<string> TaggedFields = new HashSet<string>(["region", "customer", "omit"], StringComparer.Ordinal);

    public static TheoryData<string> Vectors() => [.. Golden.All.Select(v => v.Name)];

    [Theory]
    [MemberData(nameof(Vectors))]
    public void Each_golden_vector_gives_Rivers_key_and_states_with_either_encoding(string name)
    {
        var vector = Golden.All.Single(v => v.Name == name);
        var members = vector.Kind == "golden_tagged" ? TaggedFields : null;

        foreach (var encoding in new[] { UniqueArgsEncoding.GoCompatible, UniqueArgsEncoding.Verbatim })
        {
            var key = UniqueKey.Compute(vector.Kind, vector.ArgsJson, vector.Queue, vector.CreatedAt, vector.Options, members, encoding);
            Assert.Equal(vector.KeyHex, Convert.ToHexStringLower(key));
        }

        Assert.Equal(vector.StatesBits, UniqueStates.ToBitString(UniqueStates.ToBitmask(vector.Options.ByState)));
    }

    [Fact]
    public void The_vectors_are_all_there()
    {
        Assert.Equal(22, Golden.All.Count);
        Assert.Equal(22, Golden.All.Select(v => v.Name).Distinct().Count());
    }

    [Fact]
    public void Args_serialised_by_System_Text_Json_get_the_same_key_as_Gos_serialisation_of_the_same_values()
    {
        // The same logical args as the vectors "html chars", "unicode" and "numbers", written with System.Text.Json's
        // default escaping: <, >, &, ' and every non-ASCII character as \uXXXX, and doubles as 1E-07 and 1E+21.
        string[] names = ["html chars", "unicode", "numbers"];
        string[] netJson =
        [
            Serialize(w => w.WriteString("s", "<a href='x'>&amp;</a>")),
            Serialize(w =>
            {
                w.WriteString("e", "tab\tnl\nquote\"bs\\");
                w.WriteString("s", "kopi \u2615 caf\u00e9 \u2028 \U0001F600");
            }),
            Serialize(w =>
            {
                w.WriteNumber("big", 9007199254740993L);
                w.WriteNumber("exp", 1e21);
                w.WriteNumber("f", 1.5);
                w.WriteNumber("i", 42);
                w.WriteNumber("neg", -7);
                w.WriteNumber("small", 1e-7);
                w.WriteNumber("zero", 0);
            }),
        ];

        for (var i = 0; i < names.Length; i++)
        {
            var vector = Golden.All.Single(v => v.Name == names[i]);
            var args = $$"""{"v":{{JsonSerializer.Serialize(names[i], JsonContext.Default.String)}},"p":{{netJson[i]}}}""";
            Assert.NotEqual(vector.ArgsJson, args); // the text differs...

            var go = UniqueKey.Compute(vector.Kind, args, vector.Queue, vector.CreatedAt, vector.Options);
            var verbatim = UniqueKey.Compute(vector.Kind, args, vector.Queue, vector.CreatedAt, vector.Options, encoding: UniqueArgsEncoding.Verbatim);

            Assert.Equal(vector.KeyHex, Convert.ToHexStringLower(go)); // ...but the Go-compatible key is River's
            Assert.NotEqual(vector.KeyHex, Convert.ToHexStringLower(verbatim));
        }
    }

    [Fact]
    public void The_canonical_args_sort_top_level_names_by_their_UTF8_bytes_and_keep_nested_order()
    {
        // U+FF61 is EF BD A1 in UTF-8 and U+1F600 is F0 9F 98 80, but in UTF-16 the emoji's high surrogate (D83D) sorts first.
        const string args = """{"b":1,"\uff61":2,"\ud83d\ude00":3,"A":{"z":1,"a":2},"a":[3,1]}""";

        var preimage = UniqueKey.Preimage("k", args, "q", DateTimeOffset.UnixEpoch, new UniqueOptions { ByArgs = true });

        Assert.Equal("&kind=k&args={\"A\":{\"z\":1,\"a\":2},\"a\":[3,1],\"b\":1,\"\uff61\":2,\"\U0001F600\":3}", preimage);
    }

    [Fact]
    public void Only_the_members_marked_unique_are_used_and_the_parts_come_in_a_fixed_order()
    {
        var options = new UniqueOptions { ByArgs = true, ByQueue = true, ByPeriod = TimeSpan.FromHours(1) };
        var at = new DateTimeOffset(2030, 5, 6, 7, 59, 59, 999, TimeSpan.FromHours(2));

        var preimage = UniqueKey.Preimage("email", """{"to":"a@b","body":"hi","id":7}""", "mail", at, options, new HashSet<string>(["id", "to"]));

        Assert.Equal("&kind=email&args={\"id\":7,\"to\":\"a@b\"}&period=2030-05-06T05:00:00Z&queue=mail", preimage);
    }

    [Theory]
    [InlineData("2026-11-26T01:59:59.9999999Z", 3600, "2026-11-26T01:00:00Z")]
    [InlineData("2026-11-26T01:14:59Z", 900, "2026-11-26T01:00:00Z")]
    [InlineData("2026-11-26T01:15:00Z", 900, "2026-11-26T01:15:00Z")]
    [InlineData("2026-11-26T23:59:59Z", 86400, "2026-11-26T00:00:00Z")]
    [InlineData("2026-11-26T01:00:07.5Z", 5, "2026-11-26T01:00:05Z")]
    [InlineData("1969-12-31T23:59:30Z", 60, "1969-12-31T23:59:00Z")]
    public void A_period_starts_at_a_multiple_of_its_length_since_the_epoch_to_the_second(string now, int seconds, string start)
    {
        var at = DateTimeOffset.Parse(now, CultureInfo.InvariantCulture);

        Assert.Equal(DateTimeOffset.Parse(start, CultureInfo.InvariantCulture), UniqueKey.PeriodStart(at, TimeSpan.FromSeconds(seconds)));
    }

    [Fact]
    public void Invalid_unique_options_are_rejected()
    {
        var at = DateTimeOffset.UnixEpoch;

        Assert.Throws<ArgumentException>(() => UniqueKey.Compute("k", "[1]", "q", at, new UniqueOptions { ByArgs = true }));
        Assert.Throws<ArgumentException>(() => UniqueKey.Compute("k", "not json", "q", at, new UniqueOptions { ByArgs = true }));
        Assert.Throws<ArgumentException>(() => UniqueKey.Compute("k", "{}", "q", at, new UniqueOptions { ByPeriod = TimeSpan.Zero }));
        Assert.Throws<ArgumentException>(() => UniqueKey.Compute("k", "{}", "q", at, new UniqueOptions { ExcludeKind = true }));

        // Args that are not an object are fine when they are not part of the key.
        Assert.Equal(32, UniqueKey.Compute("k", "[1]", "q", at, new UniqueOptions { ByQueue = true }).Length);
    }

    [Theory]
    [InlineData("1", "1")]
    [InlineData("-0", "-0")]
    [InlineData("9007199254740993", "9007199254740993")]
    [InlineData("123456789012345678901234567890", "123456789012345678901234567890")]
    [InlineData("1.5", "1.5")]
    [InlineData("1.50", "1.5")]
    [InlineData("1E-07", "1e-7")]
    [InlineData("1e-10", "1e-10")]
    [InlineData("0.000001", "0.000001")]
    [InlineData("0.0000012345", "0.0000012345")]
    [InlineData("1E+20", "100000000000000000000")]
    [InlineData("1E+21", "1e+21")]
    [InlineData("1.2345E+21", "1.2345e+21")]
    [InlineData("1.7976931348623157E+308", "1.7976931348623157e+308")]
    [InlineData("5E-324", "5e-324")]
    [InlineData("-12345678.9", "-12345678.9")]
    [InlineData("1e3", "1000")]
    [InlineData("0.1000000000000000055511151231257827", "0.1")]
    [InlineData("-0.0", "-0")]
    public void Numbers_are_written_in_Gos_float64_form_and_integers_are_kept(string literal, string go) =>
        Assert.Equal(go, GoJson.Number(literal));

    [Fact]
    public void Strings_are_escaped_as_Go_escapes_them()
    {
        var text = new StringBuilder();

        GoJson.AppendString(text, "q\"b\\n\nr\rt\tb\bf\fc\u0001d\u007f<>&\u2028\u2029'\u00e9\U0001F600\ud800x");

        Assert.Equal(
            "\"q\\\"b\\\\n\\nr\\rt\\tb\\bf\\fc\\u0001d\u007f\\u003c\\u003e\\u0026\\u2028\\u2029'\u00e9\U0001F600\\ufffdx\"",
            text.ToString());
    }

    [Fact]
    public void The_default_unique_states_and_the_required_minimum_follow_FR_UNQ_04()
    {
        Assert.Equal("11110101", UniqueStates.ToBitString(UniqueStates.ToBitmask(null)));
        Assert.Equal(
            [JobState.Available, JobState.Completed, JobState.Pending, JobState.Retryable, JobState.Running, JobState.Scheduled],
            UniqueStates.FromBitmask(UniqueStates.Default));
        Assert.Equal("11010001", UniqueStates.ToBitString(UniqueStates.ToBitmask([JobState.Scheduled, JobState.Running, JobState.Pending, JobState.Available])));
        Assert.Equal(0b1111_1111, UniqueStates.ToBitmask(Enum.GetValues<JobState>()));

        var missing = Assert.Throws<ArgumentException>(() => UniqueStates.ToBitmask([JobState.Available, JobState.Running, JobState.Completed]));
        Assert.Contains("missing: pending, scheduled", missing.Message, StringComparison.Ordinal);
        Assert.Throws<ArgumentException>(() => UniqueStates.ToBitmask([]));
    }

    [Theory]
    [InlineData(JobState.Available, "00000001")]
    [InlineData(JobState.Cancelled, "00000010")]
    [InlineData(JobState.Completed, "00000100")]
    [InlineData(JobState.Discarded, "00001000")]
    [InlineData(JobState.Pending, "00010000")]
    [InlineData(JobState.Retryable, "00100000")]
    [InlineData(JobState.Running, "01000000")]
    [InlineData(JobState.Scheduled, "10000000")]
    public void Each_state_has_its_bit_scheduled_first_in_the_bit_text(JobState state, string bits) =>
        Assert.Equal(bits, UniqueStates.ToBitString(UniqueStates.Bit(state)));

    private static string Serialize(Action<Utf8JsonWriter> members)
    {
        using var buffer = new MemoryStream();
        using (var writer = new Utf8JsonWriter(buffer, new JsonWriterOptions { Encoder = JavaScriptEncoder.Default }))
        {
            writer.WriteStartObject();
            members(writer);
            writer.WriteEndObject();
        }

        return Encoding.UTF8.GetString(buffer.ToArray());
    }

    /// <summary>One line of <c>river-v0.51.2-unique-keys.jsonl</c>.</summary>
    private sealed record Golden(string Name, string Kind, string ArgsJson, string Queue, DateTimeOffset CreatedAt, UniqueOptions Options, string KeyHex, string StatesBits)
    {
        public static readonly List<Golden> All = Load();

        private static List<Golden> Load()
        {
            var path = Path.Combine(AppContext.BaseDirectory, "Unique", "river-v0.51.2-unique-keys.jsonl");
            var vectors = new List<Golden>();
            foreach (var line in File.ReadLines(path, Encoding.UTF8).Where(l => l.Length > 0))
            {
                using var json = JsonDocument.Parse(line);
                var v = json.RootElement;
                var o = v.GetProperty("unique_opts");
                var periodNs = o.GetProperty("ByPeriod").GetInt64();
                var states = o.GetProperty("ByState");
                vectors.Add(new Golden(
                    v.GetProperty("name").GetString()!,
                    v.GetProperty("kind").GetString()!,
                    v.GetProperty("args_json").GetString()!,
                    v.GetProperty("queue").GetString()!,
                    DateTimeOffset.Parse(v.GetProperty("created_at_utc").GetString()!, CultureInfo.InvariantCulture),
                    new UniqueOptions
                    {
                        ByArgs = o.GetProperty("ByArgs").GetBoolean(),
                        ByPeriod = periodNs == 0 ? null : TimeSpan.FromTicks(periodNs / 100),
                        ByQueue = o.GetProperty("ByQueue").GetBoolean(),
                        ByState = states.ValueKind == JsonValueKind.Null
                            ? null
                            : [.. states.EnumerateArray().Select(s => Enum.Parse<JobState>(s.GetString()!, ignoreCase: true))],
                        ExcludeKind = o.GetProperty("ExcludeKind").GetBoolean(),
                    },
                    v.GetProperty("unique_key_hex").GetString()!,
                    v.GetProperty("unique_states").GetString()!));
            }

            return vectors;
        }
    }
}

[System.Text.Json.Serialization.JsonSerializable(typeof(string))]
internal sealed partial class JsonContext : System.Text.Json.Serialization.JsonSerializerContext;
