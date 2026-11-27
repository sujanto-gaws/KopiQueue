using KopiQueue.Drivers;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace KopiQueue.Hosting;

/// <summary>Starts the client with the host and stops it with the host (FR-HST-01, FR-HST-03).</summary>
internal sealed class KopiQueueHostedService(KopiClient client) : IHostedService
{
    public Task StartAsync(CancellationToken cancellationToken) => client.StartAsync(cancellationToken);

    /// <summary>The host's token fires at its <c>ShutdownTimeout</c>, which turns the soft stop into a hard stop.</summary>
    public Task StopAsync(CancellationToken cancellationToken) => client.StopAsync(cancellationToken);
}

/// <summary>Checks the settings when the host starts (FR-HST-02, FR-DEF-06).</summary>
internal sealed class KopiQueueOptionsValidator(KopiQueueBuilder builder) : IValidateOptions<KopiQueueOptions>
{
    public ValidateOptionsResult Validate(string? name, KopiQueueOptions options)
    {
        var errors = KopiClient.ConfigurationErrors(KopiQueueClientFactory.ToClientOptions(options, builder, services: null));
        if (builder.DriverFactory is null)
        {
            errors.Add("No database driver is configured; call UseNpgsql() inside AddKopiQueue.");
        }

        return errors.Count == 0 ? ValidateOptionsResult.Success : ValidateOptionsResult.Fail(errors);
    }
}

/// <summary>Creates the client from the bound settings and the builder.</summary>
internal static class KopiQueueClientFactory
{
    public static KopiClient Create(IServiceProvider services, KopiQueueBuilder builder)
    {
        var options = services.GetRequiredService<IOptions<KopiQueueOptions>>().Value;
        var factory = builder.DriverFactory
            ?? throw new KopiQueueConfigurationException("No database driver is configured; call UseNpgsql() inside AddKopiQueue.");
        IKopiDriver driver = factory(services) ?? throw new KopiQueueConfigurationException("The driver factory returned null.");
        return new KopiClient(driver, ToClientOptions(options, builder, services));
    }

    public static KopiClientOptions ToClientOptions(KopiQueueOptions options, KopiQueueBuilder builder, IServiceProvider? services) => new()
    {
        Queues = options.Queues.ToDictionary(q => q.Key, q => new QueueOptions { MaxWorkers = q.Value?.MaxWorkers ?? 0 }, StringComparer.Ordinal),
        Workers = builder.Workers,
        ArgsSerializer = builder.ArgsSerializer,
        RetryPolicy = builder.RetryPolicy,
        Services = services,
        LoggerFactory = services?.GetService<ILoggerFactory>(),
        TimeProvider = services?.GetService<TimeProvider>(),
        ClientId = options.ClientId,
        JobTimeout = options.JobTimeout,
        FetchPollInterval = options.FetchPollInterval,
        FetchCooldown = options.FetchCooldown,
        FetchBatchMax = options.FetchBatchMax,
        NotificationMode = options.NotificationMode,
        CompleterMode = options.CompleterMode,
        SoftStopTimeout = options.SoftStopTimeout,
        HardStopGrace = options.HardStopGrace,
        ElectInterval = options.ElectInterval,
        LeaderTtl = options.LeaderTtl,
        QueueRetention = options.QueueRetention,
        SchedulerInterval = options.SchedulerInterval,
    };
}
