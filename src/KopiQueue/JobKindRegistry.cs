using System.Collections.Concurrent;
using System.Diagnostics.CodeAnalysis;
using System.Reflection;

namespace KopiQueue;

/// <summary>
/// Maps job args types to their kinds and back (FR-DEF-01, FR-DEF-06). The client builds one from its registered workers
/// at startup; reading a single type's kind needs no registry (<see cref="GetKind{TArgs}"/>).
/// </summary>
/// <remarks>Thread-safe.</remarks>
public sealed class JobKindRegistry
{
    private static readonly ConcurrentDictionary<Type, string> KindCache = new();

    private readonly ConcurrentDictionary<string, Type> _types = new(StringComparer.Ordinal);

    /// <summary>The registered kinds.</summary>
    public IReadOnlyCollection<string> Kinds => [.. _types.Keys];

    /// <summary>Returns the kind declared by <typeparamref name="TArgs"/>.</summary>
    /// <exception cref="KopiQueueConfigurationException">The type has no <see cref="JobKindAttribute"/>, or its kind is not valid.</exception>
    public static string GetKind<TArgs>()
        where TArgs : IJobArgs =>
        KindCache.GetOrAdd(typeof(TArgs), ReadKind);

    /// <summary>Returns the kind declared by <paramref name="argsType"/>.</summary>
    /// <exception cref="ArgumentException"><paramref name="argsType"/> does not implement <see cref="IJobArgs"/>.</exception>
    /// <exception cref="KopiQueueConfigurationException">The type has no <see cref="JobKindAttribute"/>, or its kind is not valid.</exception>
    public static string GetKind(Type argsType)
    {
        ArgumentNullException.ThrowIfNull(argsType);
        if (!typeof(IJobArgs).IsAssignableFrom(argsType))
        {
            throw new ArgumentException($"{argsType.FullName} does not implement IJobArgs.", nameof(argsType));
        }

        return KindCache.GetOrAdd(argsType, ReadKind);
    }

    /// <summary>Registers <typeparamref name="TArgs"/> under its kind.</summary>
    /// <returns>The kind.</returns>
    /// <exception cref="KopiQueueConfigurationException">
    /// The type's kind is missing or not valid, or another type is registered with the same kind.
    /// </exception>
    public string Register<TArgs>()
        where TArgs : IJobArgs =>
        Register(typeof(TArgs));

    /// <summary>Registers <paramref name="argsType"/> under its kind. Registering the same type again does nothing.</summary>
    /// <returns>The kind.</returns>
    /// <exception cref="ArgumentException"><paramref name="argsType"/> does not implement <see cref="IJobArgs"/>.</exception>
    /// <exception cref="KopiQueueConfigurationException">
    /// The type's kind is missing or not valid, or another type is registered with the same kind.
    /// </exception>
    public string Register(Type argsType)
    {
        var kind = GetKind(argsType);
        var registered = _types.GetOrAdd(kind, argsType);
        if (registered != argsType)
        {
            throw new KopiQueueConfigurationException(
                $"Job kind '{kind}' is declared by both {registered.FullName} and {argsType.FullName}. Each kind needs exactly one args type.");
        }

        return kind;
    }

    /// <summary>Finds the args type registered for <paramref name="kind"/>.</summary>
    /// <returns><c>true</c> if a type is registered for the kind.</returns>
    public bool TryGetType(string kind, [NotNullWhen(true)] out Type? argsType)
    {
        ArgumentNullException.ThrowIfNull(kind);
        return _types.TryGetValue(kind, out argsType);
    }

    private static string ReadKind(Type argsType)
    {
        var attribute = argsType.GetCustomAttribute<JobKindAttribute>(inherit: false)
            ?? throw new KopiQueueConfigurationException(
                $"{argsType.FullName} implements IJobArgs but has no [JobKind] attribute. Add one, for example [JobKind(\"my_job\")].");
        var error = JobValidation.KindError(attribute.Kind);
        return error is null
            ? attribute.Kind
            : throw new KopiQueueConfigurationException($"{argsType.FullName} has an invalid [JobKind]: {error}");
    }
}
