namespace KopiQueue;

/// <summary>
/// Marks a type as the arguments of a job (FR-DEF-01). The type is usually a <c>record</c>, must be JSON-serialisable,
/// and declares its kind with <see cref="JobKindAttribute"/>.
/// </summary>
/// <example>
/// <code>
/// [JobKind("send_email")]
/// public sealed record SendEmailArgs(string To, string TemplateId) : IJobArgs;
/// </code>
/// </example>
/// <remarks>
/// The kind is an attribute rather than a member so that it never ends up in the job's JSON, and so that
/// <c>IJobArgs</c> can be used as a type argument (ADR-003).
/// </remarks>
#pragma warning disable CA1040 // A marker interface is the design (ADR-003): the kind is an attribute, not a member.
public interface IJobArgs;
#pragma warning restore CA1040

/// <summary>
/// An args type that supplies its own default insert options (FR-DEF-05), as River's <c>JobArgsWithInsertOpts</c> does:
/// every insert of it, and every periodic insert, starts from them.
/// </summary>
/// <remarks>
/// <para>
/// Each option is resolved on its own: the insert's own option if set, otherwise this one, otherwise the client's
/// default (<see cref="InsertDefaults"/>). A list or object option (<see cref="InsertOptions.Tags"/>,
/// <see cref="InsertOptions.Metadata"/>, <see cref="InsertOptions.Unique"/>) is replaced whole, not merged.
/// </para>
/// <para>
/// The options are validated with the insert's, so an invalid one fails each insert with <see cref="ArgumentException"/>.
/// They are read from the args at each insert, so they can depend on the args' values. A method rather than a property,
/// so that it is never serialised into the job's args.
/// </para>
/// </remarks>
/// <example>
/// <code>
/// [JobKind("sync_customer")]
/// public sealed record SyncCustomerArgs(long CustomerId) : IJobArgsWithInsertOptions
/// {
///     public InsertOptions GetInsertOptions() =>
///         new() { Queue = "sync", MaxAttempts = 5, Unique = new() { ByArgs = true, ByPeriod = TimeSpan.FromHours(1) } };
/// }
/// </code>
/// </example>
public interface IJobArgsWithInsertOptions : IJobArgs
{
    /// <summary>The default insert options for these args, or <c>null</c> for none.</summary>
    InsertOptions? GetInsertOptions();
}

/// <summary>
/// Declares the stable kind of an <see cref="IJobArgs"/> type (FR-DEF-01). The kind is stored with each job and selects
/// its worker, so it must not change once jobs of the type exist.
/// </summary>
/// <remarks>
/// A kind is 1-127 characters, the limit of the <c>kind</c> column. Pick a name that survives refactoring, such as
/// <c>send_email</c>, rather than the type's name. Kinds are checked when the type is first used or registered
/// (FR-DEF-06), not when the attribute is declared.
/// </remarks>
/// <param name="kind">The kind.</param>
[AttributeUsage(AttributeTargets.Class | AttributeTargets.Struct, Inherited = false, AllowMultiple = false)]
public sealed class JobKindAttribute(string kind) : Attribute
{
    /// <summary>The kind.</summary>
    public string Kind { get; } = kind;
}

/// <summary>
/// KopiQueue is configured in a way that cannot work, for example two job args types declare the same kind (FR-DEF-06).
/// </summary>
public class KopiQueueConfigurationException : Exception
{
    /// <summary>Creates an exception with a default message.</summary>
    public KopiQueueConfigurationException()
    {
    }

    /// <summary>Creates an exception with <paramref name="message"/>.</summary>
    public KopiQueueConfigurationException(string? message)
        : base(message)
    {
    }

    /// <summary>Creates an exception with <paramref name="message"/> caused by <paramref name="innerException"/>.</summary>
    public KopiQueueConfigurationException(string? message, Exception? innerException)
        : base(message, innerException)
    {
    }
}
