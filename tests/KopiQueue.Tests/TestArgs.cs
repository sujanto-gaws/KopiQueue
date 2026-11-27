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

/// <summary>Supplies defaults that depend on its value (FR-DEF-05).</summary>
[JobKind("tenant_report")]
public sealed record TenantReportArgs(string Tenant) : IJobArgsWithInsertOptions
{
    public InsertOptions GetInsertOptions() => new()
    {
        Queue = "reports-" + Tenant,
        Priority = 3,
        MaxAttempts = 5,
        Tags = ["report"],
        Metadata = new System.Text.Json.Nodes.JsonObject { ["tenant"] = Tenant },
        Unique = new() { ByArgs = true },
    };
}

[JobKind("no_defaults")]
public sealed record NoDefaultsArgs(int N) : IJobArgsWithInsertOptions
{
    public InsertOptions? GetInsertOptions() => null;
}

[JobKind("bad_defaults")]
public sealed record BadDefaultsArgs(int N) : IJobArgsWithInsertOptions
{
    public InsertOptions GetInsertOptions() => new() { Priority = 9 };
}

[JsonSourceGenerationOptions(JsonSerializerDefaults.Web)]
[JsonSerializable(typeof(TenantReportArgs))]
[JsonSerializable(typeof(NoDefaultsArgs))]
[JsonSerializable(typeof(BadDefaultsArgs))]
[JsonSerializable(typeof(RebuildIndexArgs))]
[JsonSerializable(typeof(SendEmailArgs))]
[JsonSerializable(typeof(ResizeArgs))]
[JsonSerializable(typeof(LargeResizeArgs))]
[JsonSerializable(typeof(SyncCustomerArgs))]
internal sealed partial class WebJsonContext : JsonSerializerContext;

[JsonSerializable(typeof(SendEmailArgs))]
[JsonSerializable(typeof(SyncCustomerArgs))]
internal sealed partial class PlainJsonContext : JsonSerializerContext;
