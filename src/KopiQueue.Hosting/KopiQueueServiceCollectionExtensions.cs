using KopiQueue;
using KopiQueue.Hosting;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;

namespace Microsoft.Extensions.DependencyInjection;

/// <summary>Registers KopiQueue with the Generic Host (FR-HST-01, FR-HST-02; design §6.2).</summary>
public static class KopiQueueServiceCollectionExtensions
{
    /// <summary>
    /// Registers a <see cref="KopiClient"/> (also as <see cref="IKopiClient"/>) and a hosted service that starts it with
    /// the host and stops it gracefully when the host stops (FR-HST-03). Without queues and workers the client is
    /// insert-only and the hosted service does nothing.
    /// </summary>
    /// <remarks>
    /// The settings are validated when the host starts, so an invalid configuration fails startup with an
    /// <see cref="OptionsValidationException"/> that lists every problem. The host's <c>ShutdownTimeout</c> (30 s by
    /// default) bounds the soft stop unless <see cref="KopiQueueOptions.SoftStopTimeout"/> is shorter.
    /// </remarks>
    /// <example>
    /// <code>
    /// builder.Services.AddNpgsqlDataSource(builder.Configuration.GetConnectionString("Db")!);
    /// builder.Services.AddKopiQueue(kq =>
    /// {
    ///     kq.UseNpgsql();
    ///     kq.AddQueue("default", maxWorkers: 50);
    ///     kq.AddWorker&lt;SendEmailArgs, SendEmailWorker&gt;();
    /// })
    /// .BindConfiguration("KopiQueue");
    /// </code>
    /// </example>
    /// <param name="services">The application's services.</param>
    /// <param name="configure">Sets the driver, queues and workers.</param>
    /// <returns>The builder, for example to call <see cref="BindConfiguration"/>.</returns>
    /// <exception cref="InvalidOperationException">KopiQueue was already added; one client per service collection for now.</exception>
    public static KopiQueueBuilder AddKopiQueue(this IServiceCollection services, Action<KopiQueueBuilder> configure)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(configure);
        if (services.Any(d => d.ServiceType == typeof(KopiQueueBuilder)))
        {
            throw new InvalidOperationException("AddKopiQueue was already called; configure everything in one call.");
        }

        var builder = new KopiQueueBuilder(services);
        services.AddSingleton(builder);
        services.AddOptions<KopiQueueOptions>().ValidateOnStart();
        services.TryAddEnumerable(ServiceDescriptor.Singleton<IValidateOptions<KopiQueueOptions>, KopiQueueOptionsValidator>());
        services.AddSingleton(sp => KopiQueueClientFactory.Create(sp, builder));
        services.AddSingleton<IKopiClient>(sp => sp.GetRequiredService<KopiClient>());
        services.AddSingleton<IHostedService, KopiQueueHostedService>();
        configure(builder);
        return builder;
    }

    /// <summary>
    /// Binds <see cref="KopiQueueOptions"/> to the configuration section <paramref name="configSectionPath"/>, such as
    /// <c>"KopiQueue"</c> in <c>appsettings.json</c>. Values found there override those set in code.
    /// </summary>
    /// <param name="builder">The builder.</param>
    /// <param name="configSectionPath">The section's path, with <c>:</c> between levels.</param>
    /// <returns>The builder.</returns>
    public static KopiQueueBuilder BindConfiguration(this KopiQueueBuilder builder, string configSectionPath)
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentException.ThrowIfNullOrWhiteSpace(configSectionPath);
        builder.Services.AddOptions<KopiQueueOptions>().BindConfiguration(configSectionPath);
        return builder;
    }
}
