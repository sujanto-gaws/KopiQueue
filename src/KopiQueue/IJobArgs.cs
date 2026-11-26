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
