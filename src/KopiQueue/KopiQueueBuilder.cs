using System.Diagnostics.CodeAnalysis;
using System.Text.Json.Serialization;
using KopiQueue.Drivers;
using KopiQueue.Serialization;
using Microsoft.Extensions.DependencyInjection;

namespace KopiQueue;

/// <summary>
/// Settings of a hosted client that can come from configuration (FR-HST-02, design §7), for example the <c>KopiQueue</c>
/// section of <c>appsettings.json</c>. Each one maps to the <see cref="KopiClientOptions"/> property of the same name.
/// </summary>
/// <example>
/// <code>
/// "KopiQueue": {
///   "Queues": { "default": { "MaxWorkers": 50 }, "email": { "MaxWorkers": 10 } },
///   "JobTimeout": "00:05:00",
///   "NotificationMode": "PollOnly"
/// }
/// </code>
/// </example>
public sealed class KopiQueueOptions
{
    /// <summary>The queues to work, by name. Empty (the default) makes an insert-only client.</summary>
    public Dictionary<string, QueueSettings> Queues { get; } = new(StringComparer.Ordinal);

    /// <inheritdoc cref="KopiClientOptions.ClientId"/>
    public string? ClientId { get; set; }

    /// <inheritdoc cref="KopiClientOptions.JobTimeout"/>
    public TimeSpan JobTimeout { get; set; } = TimeSpan.FromMinutes(1);

    /// <inheritdoc cref="KopiClientOptions.FetchPollInterval"/>
    public TimeSpan FetchPollInterval { get; set; } = TimeSpan.FromSeconds(1);

    /// <inheritdoc cref="KopiClientOptions.FetchCooldown"/>
    public TimeSpan FetchCooldown { get; set; } = TimeSpan.FromMilliseconds(100);

    /// <inheritdoc cref="KopiClientOptions.FetchBatchMax"/>
    public int FetchBatchMax { get; set; } = 1_000;

    /// <inheritdoc cref="KopiClientOptions.NotificationMode"/>
    public NotificationMode NotificationMode { get; set; } = NotificationMode.Auto;

    /// <inheritdoc cref="KopiClientOptions.CompleterMode"/>
    public CompleterMode CompleterMode { get; set; } = CompleterMode.Batch;

    /// <inheritdoc cref="KopiClientOptions.SoftStopTimeout"/>
    public TimeSpan? SoftStopTimeout { get; set; }

    /// <inheritdoc cref="KopiClientOptions.HardStopGrace"/>
    public TimeSpan HardStopGrace { get; set; } = TimeSpan.FromSeconds(5);

    /// <inheritdoc cref="KopiClientOptions.ElectInterval"/>
    public TimeSpan ElectInterval { get; set; } = TimeSpan.FromSeconds(5);

    /// <inheritdoc cref="KopiClientOptions.LeaderTtl"/>
    public TimeSpan LeaderTtl { get; set; } = TimeSpan.FromSeconds(15);

    /// <inheritdoc cref="KopiClientOptions.QueueRetention"/>
    public TimeSpan? QueueRetention { get; set; } = TimeSpan.FromHours(24);

    /// <inheritdoc cref="KopiClientOptions.SchedulerInterval"/>
    public TimeSpan SchedulerInterval { get; set; } = TimeSpan.FromSeconds(5);
}

/// <summary>Settings of one queue in <see cref="KopiQueueOptions.Queues"/>.</summary>
public sealed class QueueSettings
{
    /// <inheritdoc cref="QueueOptions.MaxWorkers"/>
    public int MaxWorkers { get; set; }
}

/// <summary>
/// Configures a client registered with <c>services.AddKopiQueue(...)</c> (package <c>KopiQueue.Hosting</c>, design §6.2):
/// the database driver, the queues and the workers. Driver packages add their own methods, such as <c>UseNpgsql()</c>.
/// </summary>
/// <remarks>Settings made here are applied before configuration bound with <c>BindConfiguration</c>, which wins.</remarks>
public sealed class KopiQueueBuilder
{
    /// <summary>Creates a builder that registers into <paramref name="services"/>. Called by <c>AddKopiQueue</c>.</summary>
    /// <param name="services">The application's services.</param>
    public KopiQueueBuilder(IServiceCollection services)
    {
        ArgumentNullException.ThrowIfNull(services);
        Services = services;
    }

    /// <summary>The application's services.</summary>
    public IServiceCollection Services { get; }

    /// <summary>The workers, one per job kind. The <c>AddWorker</c> methods add to it.</summary>
    public WorkerRegistry Workers { get; } = new();

    /// <summary>The args serialiser set with <see cref="UseArgsSerializer"/> or <see cref="UseJsonContext"/>, if any.</summary>
    public IJobArgsSerializer? ArgsSerializer { get; private set; }

    /// <summary>The retry policy set with <see cref="UseRetryPolicy"/>, if any.</summary>
    public IRetryPolicy? RetryPolicy { get; private set; }

    /// <summary>Creates the driver; set by <see cref="UseDriver"/>.</summary>
    internal Func<IServiceProvider, IKopiDriver>? DriverFactory { get; private set; }

    /// <summary>Works <paramref name="name"/> with up to <paramref name="maxWorkers"/> jobs at once.</summary>
    /// <param name="name">The queue's name.</param>
    /// <param name="maxWorkers">The most jobs of the queue to run at once: 1 to 10,000.</param>
    /// <returns>This builder.</returns>
    public KopiQueueBuilder AddQueue(string name, int maxWorkers)
    {
        ArgumentNullException.ThrowIfNull(name);
        return Configure(o => o.Queues[name] = new QueueSettings { MaxWorkers = maxWorkers });
    }

    /// <summary>Changes the settings in code.</summary>
    /// <param name="configure">Changes the settings.</param>
    /// <returns>This builder.</returns>
    public KopiQueueBuilder Configure(Action<KopiQueueOptions> configure)
    {
        ArgumentNullException.ThrowIfNull(configure);
        Services.Configure(configure);
        return this;
    }

    /// <summary>Adds the worker for <typeparamref name="TArgs"/>, created in each job's scope (see <see cref="WorkerRegistry.Add{TArgs, TWorker}"/>).</summary>
    /// <typeparam name="TArgs">The job's args.</typeparam>
    /// <typeparam name="TWorker">The worker.</typeparam>
    /// <returns>This builder.</returns>
    public KopiQueueBuilder AddWorker<TArgs, [DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors)] TWorker>()
        where TArgs : IJobArgs
        where TWorker : class, IWorker<TArgs>
    {
        Workers.Add<TArgs, TWorker>();
        return this;
    }

    /// <summary>Adds a worker instance shared by every job of its kind.</summary>
    /// <typeparam name="TArgs">The job's args.</typeparam>
    /// <param name="worker">The worker.</param>
    /// <returns>This builder.</returns>
    public KopiQueueBuilder AddWorker<TArgs>(IWorker<TArgs> worker)
        where TArgs : IJobArgs
    {
        Workers.Add(worker);
        return this;
    }

    /// <summary>Adds a worker, finding its args type through reflection (see <see cref="WorkerRegistry.Add{TWorker}()"/>).</summary>
    /// <typeparam name="TWorker">The worker.</typeparam>
    /// <returns>This builder.</returns>
    [RequiresUnreferencedCode("Finds the args type through reflection. Use AddWorker<TArgs, TWorker>() in trimmed apps.")]
    [RequiresDynamicCode("Makes a generic method for the args type at runtime. Use AddWorker<TArgs, TWorker>() in Native AOT apps.")]
    public KopiQueueBuilder AddWorker<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors | DynamicallyAccessedMemberTypes.Interfaces)] TWorker>()
        where TWorker : class
    {
        Workers.Add<TWorker>();
        return this;
    }

    /// <summary>Serialises job args with <paramref name="serializer"/> (<see cref="KopiClientOptions.ArgsSerializer"/>).</summary>
    /// <param name="serializer">The serialiser.</param>
    /// <returns>This builder.</returns>
    public KopiQueueBuilder UseArgsSerializer(IJobArgsSerializer serializer)
    {
        ArgumentNullException.ThrowIfNull(serializer);
        ArgsSerializer = serializer;
        return this;
    }

    /// <summary>Serialises job args with the source-generated <paramref name="context"/>, for trimmed and Native AOT apps (NFR-COMP-06).</summary>
    /// <param name="context">The application's <see cref="JsonSerializerContext"/>, which lists every args type.</param>
    /// <returns>This builder.</returns>
    public KopiQueueBuilder UseJsonContext(JsonSerializerContext context) => UseArgsSerializer(new SystemTextJsonArgsSerializer(context));

    /// <summary>Decides when failed jobs run again (<see cref="KopiClientOptions.RetryPolicy"/>).</summary>
    /// <param name="policy">The policy.</param>
    /// <returns>This builder.</returns>
    public KopiQueueBuilder UseRetryPolicy(IRetryPolicy policy)
    {
        ArgumentNullException.ThrowIfNull(policy);
        RetryPolicy = policy;
        return this;
    }

    /// <summary>Uses the driver <paramref name="factory"/> creates. Driver packages call this from their <c>Use…</c> methods.</summary>
    /// <param name="factory">Creates the driver from the application's services, once.</param>
    /// <returns>This builder.</returns>
    [Experimental(DiagnosticIds.ExperimentalDriver)]
    public KopiQueueBuilder UseDriver(Func<IServiceProvider, IKopiDriver> factory)
    {
        ArgumentNullException.ThrowIfNull(factory);
        DriverFactory = factory;
        return this;
    }
}
