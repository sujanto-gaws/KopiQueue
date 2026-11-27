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

[JobKind("sync_customer")]
public sealed record SyncCustomerArgs(
    [property: UniqueKey] long CustomerId,
    [property: UniqueKey, JsonPropertyName("tenant")] string TenantName,
    string RequestedBy) : IJobArgs;

[JobKind("rebuild_index")]
public sealed record RebuildIndexArgs(string Index) : IJobArgs;

[JsonSourceGenerationOptions(JsonSerializerDefaults.Web)]
[JsonSerializable(typeof(RebuildIndexArgs))]
[JsonSerializable(typeof(SendEmailArgs))]
[JsonSerializable(typeof(ResizeArgs))]
[JsonSerializable(typeof(LargeResizeArgs))]
[JsonSerializable(typeof(SyncCustomerArgs))]
internal sealed partial class WebJsonContext : JsonSerializerContext;

[JsonSerializable(typeof(SendEmailArgs))]
[JsonSerializable(typeof(SyncCustomerArgs))]
internal sealed partial class PlainJsonContext : JsonSerializerContext;
