using System.Collections.Concurrent;
using System.Diagnostics.CodeAnalysis;
using System.Reflection;
using KopiQueue.Serialization;
using Microsoft.Extensions.DependencyInjection;

namespace KopiQueue;

/// <summary>
/// The workers a client runs, one per job kind (FR-DEF-02, FR-DEF-06). Register every worker before the client starts
/// working jobs.
/// </summary>
/// <example>
/// <code>
/// var workers = new WorkerRegistry()
///     .Add&lt;SendEmailArgs, SendEmailWorker&gt;()   // created in each job's DI scope
///     .Add(new ResizeImageWorker());               // one shared instance
/// </code>
/// </example>
/// <remarks>
/// <para>
/// Registration fails fast with <see cref="KopiQueueConfigurationException"/>: an args type without a valid
/// <see cref="JobKindAttribute"/>, a second worker for a kind, and two args types with the same kind are all errors.
/// A client that works jobs also refuses to start with no workers.
/// </para>
/// <para>Thread-safe, but registering while the client works jobs is not supported.</para>
/// </remarks>
public sealed class WorkerRegistry
{
    private readonly JobKindRegistry _kinds = new();
    private readonly ConcurrentDictionary<string, WorkerRegistration> _workers = new(StringComparer.Ordinal);

    /// <summary>The kinds that have a worker.</summary>
    public IReadOnlyCollection<string> Kinds => [.. _workers.Keys];

    /// <summary>The args types of the registered kinds.</summary>
    internal JobKindRegistry KindRegistry => _kinds;

    /// <summary>
    /// Registers <typeparamref name="TWorker"/> for <typeparamref name="TArgs"/> jobs. The worker is created in each job's
    /// dependency-injection scope: resolved from it if <typeparamref name="TWorker"/> is registered there, otherwise
    /// constructed with its constructor's parameters resolved from it (and then disposed with the job).
    /// </summary>
    /// <typeparam name="TArgs">The job args type.</typeparam>
    /// <typeparam name="TWorker">The worker type: a concrete class.</typeparam>
    /// <returns>This registry.</returns>
    /// <exception cref="KopiQueueConfigurationException">
    /// <typeparamref name="TArgs"/> has no valid kind, its kind already has a worker or another args type, or
    /// <typeparamref name="TWorker"/> is abstract.
    /// </exception>
    public WorkerRegistry Add<TArgs, [DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors)] TWorker>()
        where TArgs : IJobArgs
        where TWorker : class, IWorker<TArgs>
    {
        if (typeof(TWorker).IsAbstract)
        {
            throw new KopiQueueConfigurationException($"Worker {typeof(TWorker).FullName} is abstract. Register a concrete worker type.");
        }

        var factory = ActivatorUtilities.CreateFactory<TWorker>(Type.EmptyTypes);
        return Add(new WorkerRegistration<TArgs>(Kind<TArgs>(), typeof(TWorker), services =>
        {
            var resolved = services.GetService<TWorker>();
            return resolved is null ? (factory(services, null), true) : (resolved, false);
        }));
    }

    /// <summary>
    /// Registers <paramref name="worker"/> for <typeparamref name="TArgs"/> jobs. Every job uses this instance, so it must
    /// be thread-safe; it is never disposed by KopiQueue.
    /// </summary>
    /// <typeparam name="TArgs">The job args type.</typeparam>
    /// <param name="worker">The worker.</param>
    /// <returns>This registry.</returns>
    /// <exception cref="KopiQueueConfigurationException">
    /// <typeparamref name="TArgs"/> has no valid kind, or its kind already has a worker or another args type.
    /// </exception>
    public WorkerRegistry Add<TArgs>(IWorker<TArgs> worker)
        where TArgs : IJobArgs
    {
        ArgumentNullException.ThrowIfNull(worker);
        return Add(new WorkerRegistration<TArgs>(Kind<TArgs>(), worker.GetType(), _ => (worker, false)));
    }

    /// <summary>
    /// Registers <typeparamref name="TWorker"/> for the args type of the one <see cref="IWorker{TArgs}"/> it implements,
    /// like <see cref="Add{TArgs, TWorker}"/>. Trimmed and Native AOT apps call <see cref="Add{TArgs, TWorker}"/> instead.
    /// </summary>
    /// <typeparam name="TWorker">The worker type: a concrete class that implements <see cref="IWorker{TArgs}"/> once.</typeparam>
    /// <returns>This registry.</returns>
    /// <exception cref="KopiQueueConfigurationException">
    /// <typeparamref name="TWorker"/> implements <see cref="IWorker{TArgs}"/> for no args type or for more than one, or
    /// registering it fails as in <see cref="Add{TArgs, TWorker}"/>.
    /// </exception>
    [RequiresUnreferencedCode("Finds the args type through reflection. Use Add<TArgs, TWorker>() in trimmed apps.")]
    [RequiresDynamicCode("Makes a generic method for the args type at runtime. Use Add<TArgs, TWorker>() in Native AOT apps.")]
    public WorkerRegistry Add<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors | DynamicallyAccessedMemberTypes.Interfaces)] TWorker>()
        where TWorker : class
    {
        var argsTypes = typeof(TWorker).GetInterfaces()
            .Where(i => i.IsGenericType && i.GetGenericTypeDefinition() == typeof(IWorker<>))
            .Select(i => i.GetGenericArguments()[0])
            .ToArray();
        if (argsTypes.Length != 1)
        {
            throw new KopiQueueConfigurationException(argsTypes.Length == 0
                ? $"{typeof(TWorker).FullName} does not implement IWorker<TArgs>. Derive it from Worker<TArgs>."
                : $"{typeof(TWorker).FullName} implements IWorker<TArgs> for {string.Join(", ", argsTypes.Select(t => t.FullName))}. "
                  + "Register each with Add<TArgs, TWorker>().");
        }

        var add = typeof(WorkerRegistry).GetMethods()
            .Single(m => m.Name == nameof(Add) && m.GetGenericArguments().Length == 2)
            .MakeGenericMethod(argsTypes[0], typeof(TWorker));
        try
        {
            return (WorkerRegistry)add.Invoke(this, null)!;
        }
        catch (TargetInvocationException e) when (e.InnerException is not null)
        {
            System.Runtime.ExceptionServices.ExceptionDispatchInfo.Throw(e.InnerException);
            throw;
        }
    }

    /// <summary>Finds the worker for <paramref name="kind"/>.</summary>
    internal bool TryGet(string kind, [NotNullWhen(true)] out WorkerRegistration? registration) =>
        _workers.TryGetValue(kind, out registration);

    /// <summary>Checks what a client that works jobs needs (FR-DEF-06); called when it starts.</summary>
    /// <exception cref="KopiQueueConfigurationException">No worker is registered.</exception>
    internal void ValidateForWork()
    {
        if (_workers.IsEmpty)
        {
            throw new KopiQueueConfigurationException(
                "No workers are registered. A client that works jobs needs a worker for each kind it fetches; "
                + "a client that only inserts jobs needs no queues and no workers.");
        }
    }

    private string Kind<TArgs>()
        where TArgs : IJobArgs =>
        _kinds.Register<TArgs>();

    private WorkerRegistry Add(WorkerRegistration registration)
    {
        var existing = _workers.GetOrAdd(registration.Kind, registration);
        if (!ReferenceEquals(existing, registration))
        {
            throw new KopiQueueConfigurationException(
                $"Job kind '{registration.Kind}' already has worker {existing.WorkerType.FullName}, so {registration.WorkerType.FullName} "
                + "cannot be registered for it. Each kind has exactly one worker.");
        }

        return this;
    }
}

/// <summary>A registered worker: how to make a <see cref="IWorkUnit"/> for one of its jobs (used by the executor).</summary>
internal abstract class WorkerRegistration(string kind, Type argsType, Type workerType)
{
    /// <summary>The kind the worker works.</summary>
    public string Kind { get; } = kind;

    /// <summary>The job args type.</summary>
    public Type ArgsType { get; } = argsType;

    /// <summary>The worker's type, for messages.</summary>
    public Type WorkerType { get; } = workerType;

    /// <summary>
    /// Deserialises <paramref name="row"/>'s args, then gets the worker from <paramref name="services"/> (the job's
    /// scope). Throws what deserialisation or the worker's construction throws.
    /// </summary>
    public abstract IWorkUnit CreateWorkUnit(JobRow row, IJobArgsSerializer serializer, IServiceProvider services);
}

/// <summary>One job bound to its worker, independent of the args type (design §5.2).</summary>
internal interface IWorkUnit : IAsyncDisposable
{
    /// <summary>The worker's timeout for the job, or <c>null</c> for the client's.</summary>
    TimeSpan? Timeout();

    /// <summary>The worker's retry delay after a failed attempt, or <c>null</c> for the client's retry policy.</summary>
    TimeSpan? NextRetry();

    /// <summary>Runs the worker.</summary>
    Task WorkAsync(CancellationToken cancellationToken);
}

/// <summary>A worker of <typeparamref name="TArgs"/> jobs.</summary>
/// <param name="kind">The kind.</param>
/// <param name="workerType">The worker's type.</param>
/// <param name="getWorker">Gets the worker from the job's scope, and whether the unit owns (and disposes) it.</param>
internal sealed class WorkerRegistration<TArgs>(string kind, Type workerType, Func<IServiceProvider, (IWorker<TArgs> Worker, bool Owned)> getWorker)
    : WorkerRegistration(kind, typeof(TArgs), workerType)
    where TArgs : IJobArgs
{
    public override IWorkUnit CreateWorkUnit(JobRow row, IJobArgsSerializer serializer, IServiceProvider services)
    {
        ArgumentNullException.ThrowIfNull(row);
        ArgumentNullException.ThrowIfNull(serializer);
        ArgumentNullException.ThrowIfNull(services);
        if (!string.Equals(row.Kind, Kind, StringComparison.Ordinal))
        {
            throw new ArgumentException($"Job {row.Id} has kind '{row.Kind}', not '{Kind}'.", nameof(row));
        }

        var args = serializer.Deserialize(row.ArgsJson, typeof(TArgs)) is TArgs typed
            ? typed
            : throw new InvalidOperationException($"The args serialiser did not return a {typeof(TArgs).FullName} for job {row.Id}.");
        var job = new Job<TArgs>(row, args);
        var (worker, owned) = getWorker(services);
        return new WorkUnit(job, worker, owned);
    }

    private sealed class WorkUnit(Job<TArgs> job, IWorker<TArgs> worker, bool owned) : IWorkUnit
    {
        public TimeSpan? Timeout() => worker.Timeout(job);

        public TimeSpan? NextRetry() => worker.NextRetry(job);

        public Task WorkAsync(CancellationToken cancellationToken) => worker.WorkAsync(job, cancellationToken);

        public ValueTask DisposeAsync()
        {
            if (!owned)
            {
                return ValueTask.CompletedTask;
            }

            if (worker is IAsyncDisposable asyncDisposable)
            {
                return asyncDisposable.DisposeAsync();
            }

            (worker as IDisposable)?.Dispose();
            return ValueTask.CompletedTask;
        }
    }
}
