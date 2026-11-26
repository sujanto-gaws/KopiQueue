using System.Text.Json;
using System.Text.Json.Serialization;
using KopiQueue.Serialization;

namespace KopiQueue.Tests.Serialization;

/// <summary>
/// The <c>System.Text.Json</c> args serialiser (plan task 1.10: design §9, NFR-COMP-06). This test project runs with
/// reflection-based JSON disabled (<c>JsonSerializerIsReflectionEnabledByDefault=false</c>), as trimmed and Native AOT
/// apps do, so everything that works here works without reflection.
/// </summary>
public sealed class SystemTextJsonArgsSerializerTests
{
    [Fact]
    public void This_test_project_runs_without_reflection_based_json()
    {
        Assert.False(JsonSerializer.IsReflectionEnabledByDefault);
    }

    [Fact]
    public void A_context_serialises_and_reads_back_with_its_own_options()
    {
        var web = new SystemTextJsonArgsSerializer(WebJsonContext.Default);
        var plain = new SystemTextJsonArgsSerializer(PlainJsonContext.Default);
        var args = new SendEmailArgs("a@example.com", "welcome", [1, 2]);

        var webJson = web.Serialize(args, typeof(SendEmailArgs));
        var plainJson = plain.Serialize(args, typeof(SendEmailArgs));

        Assert.Equal("""{"to":"a@example.com","templateId":"welcome","ids":[1,2]}""", webJson);
        Assert.Equal("""{"To":"a@example.com","TemplateId":"welcome","Ids":[1,2]}""", plainJson);
        var read = Assert.IsType<SendEmailArgs>(web.Deserialize(webJson, typeof(SendEmailArgs)));
        Assert.Equal((args.To, args.TemplateId), (read.To, read.TemplateId));
        Assert.Equal(args.Ids, read.Ids);
    }

    [Fact]
    public void Web_reading_is_case_insensitive_as_the_default_is()
    {
        var web = new SystemTextJsonArgsSerializer(WebJsonContext.Default);

        var read = Assert.IsType<SendEmailArgs>(web.Deserialize("""{"TO":"x","templateid":"t","Ids":[]}""", typeof(SendEmailArgs)));

        Assert.Equal(("x", "t"), (read.To, read.TemplateId));
    }

    [Fact]
    public void A_type_missing_from_the_context_gets_a_clear_error()
    {
        var serializer = new SystemTextJsonArgsSerializer(WebJsonContext.Default);

        var write = Assert.Throws<NotSupportedException>(() => serializer.Serialize(new UnlistedArgs(1), typeof(UnlistedArgs)));
        var read = Assert.Throws<NotSupportedException>(() => serializer.Deserialize("{}", typeof(UnlistedArgs)));

        Assert.Contains("[JsonSerializable(typeof(UnlistedArgs))]", write.Message, StringComparison.Ordinal);
        Assert.Contains("[JsonSerializable(typeof(UnlistedArgs))]", read.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void The_default_asks_for_a_context_when_reflection_is_disabled()
    {
        var ex = Assert.Throws<NotSupportedException>(() => SystemTextJsonArgsSerializer.Default.Serialize(new UnlistedArgs(1), typeof(UnlistedArgs)));

        Assert.Contains("reflection-based JSON is disabled", ex.Message, StringComparison.Ordinal);
        Assert.Contains("KopiClientOptions.ArgsSerializer", ex.Message, StringComparison.Ordinal);
        Assert.Throws<NotSupportedException>(() => SystemTextJsonArgsSerializer.Default.Deserialize("{}", typeof(UnlistedArgs)));
    }

    [Fact]
    public void Options_from_a_resolver_are_accepted_and_made_read_only()
    {
        var options = new JsonSerializerOptions(JsonSerializerDefaults.Web) { TypeInfoResolver = WebJsonContext.Default };

        var serializer = new SystemTextJsonArgsSerializer(options);

        Assert.True(options.IsReadOnly);
        Assert.Equal("""{"width":3}""", serializer.Serialize(new ResizeArgs(3), typeof(ResizeArgs)));
    }

    [Fact]
    public void Options_without_a_resolver_are_rejected()
    {
        var ex = Assert.Throws<ArgumentException>(() => new SystemTextJsonArgsSerializer(new JsonSerializerOptions()));

        Assert.Contains("TypeInfoResolver", ex.Message, StringComparison.Ordinal);
        Assert.Throws<ArgumentNullException>(() => new SystemTextJsonArgsSerializer((JsonSerializerOptions)null!));
        Assert.Throws<ArgumentNullException>(() => new SystemTextJsonArgsSerializer((JsonSerializerContext)null!));
    }

    [Fact]
    public void Arguments_are_checked()
    {
        var serializer = new SystemTextJsonArgsSerializer(WebJsonContext.Default);

        Assert.Throws<ArgumentException>(() => serializer.Serialize(new ResizeArgs(1), typeof(SendEmailArgs)));
        Assert.Throws<ArgumentException>(() => serializer.Deserialize("{}", typeof(string)));
        Assert.Throws<JsonException>(() => serializer.Deserialize("null", typeof(ResizeArgs)));
        Assert.Throws<JsonException>(() => serializer.Deserialize("not json", typeof(ResizeArgs)));
        Assert.Throws<ArgumentNullException>(() => serializer.Serialize(null!, typeof(ResizeArgs)));
        Assert.Throws<ArgumentNullException>(() => serializer.Deserialize(null!, typeof(ResizeArgs)));
    }

    [Fact]
    public void A_base_type_serialises_only_its_own_members()
    {
        var serializer = new SystemTextJsonArgsSerializer(WebJsonContext.Default);

        Assert.Equal("""{"width":5}""", serializer.Serialize(new LargeResizeArgs(5, "hi"), typeof(ResizeArgs)));
        Assert.Equal("""{"label":"hi","width":5}""", serializer.Serialize(new LargeResizeArgs(5, "hi"), typeof(LargeResizeArgs)));
    }
}
