using System.Diagnostics.CodeAnalysis;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace KopiQueue.Serialization;

/// <summary>Turns job args into the JSON stored with a job, and back (design §9, §6.4).</summary>
/// <remarks>Implementations must be thread-safe: one instance serves every insert and every worker.</remarks>
public interface IJobArgsSerializer
{
    /// <summary>Serialises <paramref name="args"/> as <paramref name="argsType"/>.</summary>
    /// <param name="args">The args. Must be an instance of <paramref name="argsType"/>.</param>
    /// <param name="argsType">The type whose shape to write: the insert's type argument, or the args' runtime type in a bulk insert.</param>
    /// <returns>A JSON value, normally an object.</returns>
    string Serialize(IJobArgs args, Type argsType);

    /// <summary>Reads args of <paramref name="argsType"/> from <paramref name="json"/>.</summary>
    /// <param name="json">The JSON stored with the job.</param>
    /// <param name="argsType">The args type registered for the job's kind.</param>
    /// <returns>The args.</returns>
    IJobArgs Deserialize(string json, Type argsType);

    /// <summary>
    /// The JSON names of the members of <paramref name="argsType"/> marked <see cref="UniqueKeyAttribute"/>, as this
    /// serialiser writes them, for unique jobs by args (FR-UNQ-01). Empty when no member is marked.
    /// </summary>
    /// <param name="argsType">The args type, as passed to <see cref="Serialize"/>.</param>
    /// <returns>The names; KopiQueue caches them per type.</returns>
    /// <remarks>
    /// The default implementation cannot know the JSON names, so it returns an empty set for a type that marks no
    /// member and throws <see cref="NotSupportedException"/> for one that does. Override it to support
    /// <see cref="UniqueKeyAttribute"/>.
    /// </remarks>
    /// <exception cref="NotSupportedException">The serialiser cannot map marked members to JSON names.</exception>
    IReadOnlySet<string> GetUniqueMemberNames(Type argsType) => UniqueMemberNames.Unmapped(GetType(), argsType);
}

/// <summary>The default of <see cref="IJobArgsSerializer.GetUniqueMemberNames"/>.</summary>
internal static class UniqueMemberNames
{
    public static readonly IReadOnlySet<string> None = new HashSet<string>(StringComparer.Ordinal);

    [UnconditionalSuppressMessage("Trimming", "IL2070:UnrecognizedReflectionPattern",
        Justification = "Finds marked members that are serialised; a member the trimmer removed is not serialised either.")]
    public static IReadOnlySet<string> Unmapped(Type serializerType, Type argsType)
    {
        ArgumentNullException.ThrowIfNull(argsType);
        const System.Reflection.BindingFlags members =
            System.Reflection.BindingFlags.Public | System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance;
        var marked = argsType.GetProperties(members).Cast<System.Reflection.MemberInfo>()
            .Concat(argsType.GetFields(members))
            .Any(m => m.IsDefined(typeof(UniqueKeyAttribute), inherit: true));
        return marked
            ? throw new NotSupportedException(
                $"{argsType.FullName} marks members with [UniqueKey], but the args serialiser {serializerType.FullName} does not map them to "
                + "JSON names. Implement IJobArgsSerializer.GetUniqueMemberNames.")
            : None;
    }
}

/// <summary>
/// The default args serialiser, on <c>System.Text.Json</c> (design §9). For trimmed and Native AOT apps, create it from
/// a <see cref="JsonSerializerContext"/> that lists every args type (NFR-COMP-06).
/// </summary>
/// <example>
/// <code>
/// [JsonSourceGenerationOptions(JsonSerializerDefaults.Web)]
/// [JsonSerializable(typeof(SendEmailArgs))]
/// internal sealed partial class AppJsonContext : JsonSerializerContext;
///
/// var client = new KopiClient(driver, new KopiClientOptions { ArgsSerializer = new SystemTextJsonArgsSerializer(AppJsonContext.Default) });
/// </code>
/// </example>
/// <remarks>
/// <see cref="Default"/> uses <see cref="JsonSerializerDefaults.Web"/>: camel-case property names and case-insensitive
/// reading. To keep the same JSON with a context, declare it with
/// <c>[JsonSourceGenerationOptions(JsonSerializerDefaults.Web)]</c>.
/// </remarks>
public sealed class SystemTextJsonArgsSerializer : IJobArgsSerializer
{
    private readonly JsonSerializerOptions? _options;

    /// <summary>Creates a serialiser that uses <paramref name="options"/>, including their type-info resolver.</summary>
    /// <param name="options">The options. They are made read-only.</param>
    public SystemTextJsonArgsSerializer(JsonSerializerOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);
        if (options.TypeInfoResolver is null)
        {
            throw new ArgumentException(
                "The options have no TypeInfoResolver. Set one, for example a JsonSerializerContext, or use SystemTextJsonArgsSerializer.Default.",
                nameof(options));
        }

        options.MakeReadOnly();
        _options = options;
    }

    /// <summary>
    /// Creates a serialiser that takes all type information, and its options, from <paramref name="context"/>. Needs no
    /// reflection, so it works in trimmed and Native AOT apps.
    /// </summary>
    /// <param name="context">A source-generated context with a <c>[JsonSerializable]</c> entry for every args type.</param>
    public SystemTextJsonArgsSerializer(JsonSerializerContext context)
        : this((context ?? throw new ArgumentNullException(nameof(context))).Options)
    {
    }

    private SystemTextJsonArgsSerializer(JsonSerializerOptions? reflectionOptions, bool reflection)
    {
        _ = reflection;
        _options = reflectionOptions;
    }

    /// <summary>
    /// The serialiser used when none is configured: <see cref="JsonSerializerDefaults.Web"/> with reflection. Where the app
    /// disables reflection-based JSON (trimmed and Native AOT apps), using it throws <see cref="NotSupportedException"/>
    /// that asks for a <see cref="JsonSerializerContext"/>.
    /// </summary>
    public static SystemTextJsonArgsSerializer Default { get; } = new(CreateReflectionOptions(), reflection: true);

    /// <inheritdoc />
    /// <exception cref="NotSupportedException">No JSON metadata is available for <paramref name="argsType"/>.</exception>
    public string Serialize(IJobArgs args, Type argsType)
    {
        ArgumentNullException.ThrowIfNull(args);
        ArgumentNullException.ThrowIfNull(argsType);
        if (!argsType.IsInstanceOfType(args))
        {
            throw new ArgumentException($"The args are a {args.GetType().FullName}, not a {argsType.FullName}.", nameof(args));
        }

        return JsonSerializer.Serialize(args, TypeInfo(argsType));
    }

    /// <inheritdoc />
    /// <exception cref="NotSupportedException">No JSON metadata is available for <paramref name="argsType"/>.</exception>
    /// <exception cref="JsonException"><paramref name="json"/> is not valid for <paramref name="argsType"/>, or is <c>null</c>.</exception>
    public IJobArgs Deserialize(string json, Type argsType)
    {
        ArgumentNullException.ThrowIfNull(json);
        ArgumentNullException.ThrowIfNull(argsType);
        if (!typeof(IJobArgs).IsAssignableFrom(argsType))
        {
            throw new ArgumentException($"{argsType.FullName} does not implement IJobArgs.", nameof(argsType));
        }

        return JsonSerializer.Deserialize(json, TypeInfo(argsType)) as IJobArgs
            ?? throw new JsonException($"The stored args are JSON null, which is not a valid {argsType.FullName}.");
    }

    /// <inheritdoc />
    /// <remarks>
    /// The names are the JSON names <c>System.Text.Json</c> writes, after the naming policy and
    /// <see cref="JsonPropertyNameAttribute"/>. Works with a <see cref="JsonSerializerContext"/> too.
    /// </remarks>
    /// <exception cref="NotSupportedException">No JSON metadata is available for <paramref name="argsType"/>.</exception>
    public IReadOnlySet<string> GetUniqueMemberNames(Type argsType)
    {
        ArgumentNullException.ThrowIfNull(argsType);
        var info = TypeInfo(argsType);
        if (info.Kind != System.Text.Json.Serialization.Metadata.JsonTypeInfoKind.Object)
        {
            return UniqueMemberNames.None;
        }

        var names = new HashSet<string>(StringComparer.Ordinal);
        foreach (var property in info.Properties)
        {
            if (property.AttributeProvider?.IsDefined(typeof(UniqueKeyAttribute), inherit: true) == true)
            {
                names.Add(property.Name);
            }
        }

        return names.Count == 0 ? UniqueMemberNames.None : names;
    }

    private System.Text.Json.Serialization.Metadata.JsonTypeInfo TypeInfo(Type argsType)
    {
        var options = _options ?? throw new NotSupportedException(
            $"Cannot serialise {argsType.FullName}: reflection-based JSON is disabled (for example in a trimmed or Native AOT app). "
            + "Configure KopiClientOptions.ArgsSerializer with a SystemTextJsonArgsSerializer created from your JsonSerializerContext.");
        try
        {
            return options.GetTypeInfo(argsType);
        }
        catch (NotSupportedException ex)
        {
            throw new NotSupportedException(
                $"No JSON metadata for {argsType.FullName}. Add [JsonSerializable(typeof({argsType.Name}))] to your JsonSerializerContext.", ex);
        }
    }

    [UnconditionalSuppressMessage("Trimming", "IL2026:RequiresUnreferencedCode",
        Justification = "Used only when JsonSerializer.IsReflectionEnabledByDefault is true, which trimmed and AOT apps turn off.")]
    [UnconditionalSuppressMessage("AOT", "IL3050:RequiresDynamicCode",
        Justification = "Used only when JsonSerializer.IsReflectionEnabledByDefault is true, which AOT apps turn off.")]
    private static JsonSerializerOptions? CreateReflectionOptions() =>
        JsonSerializer.IsReflectionEnabledByDefault ? JsonSerializerOptions.Web : null;
}
