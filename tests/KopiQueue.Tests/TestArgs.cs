using System.Text.Json;
using System.Text.Json.Serialization;

namespace KopiQueue.Tests;

// Args types shared by the unit tests, and the source-generated contexts that serialise them: this test project
// runs without reflection-based JSON.

[JobKind("send_email")]
public sealed record SendEmailArgs(string To, string TemplateId, int[] Ids) : IJobArgs;

[JobKind("resize_image")]
public record ResizeArgs(int Width) : IJobArgs;

[JobKind("resize_large")]
public sealed record LargeResizeArgs(int Width, string Label) : ResizeArgs(Width);

[JobKind("unlisted")]
public sealed record UnlistedArgs(int N) : IJobArgs;

[JsonSourceGenerationOptions(JsonSerializerDefaults.Web)]
[JsonSerializable(typeof(SendEmailArgs))]
[JsonSerializable(typeof(ResizeArgs))]
[JsonSerializable(typeof(LargeResizeArgs))]
internal sealed partial class WebJsonContext : JsonSerializerContext;

[JsonSerializable(typeof(SendEmailArgs))]
internal sealed partial class PlainJsonContext : JsonSerializerContext;
