using KopiQueue;
using KopiQueue.Npgsql;
using Npgsql;

namespace Microsoft.Extensions.DependencyInjection;

/// <summary>Uses PostgreSQL for a client registered with <c>AddKopiQueue</c> (FR-HST-04).</summary>
public static class NpgsqlKopiQueueBuilderExtensions
{
#pragma warning disable RS0026 // The two UseNpgsql overloads differ by a required data source, so a call cannot be ambiguous.
    /// <summary>
    /// Uses the <see cref="NpgsqlDataSource"/> registered in the application's services, for example with
    /// <c>services.AddNpgsqlDataSource(connectionString)</c>.
    /// </summary>
    /// <param name="builder">The builder.</param>
    /// <param name="options">Schema, table prefix and notifier settings. <c>null</c> uses the defaults.</param>
    /// <returns>The builder.</returns>
    public static KopiQueueBuilder UseNpgsql(this KopiQueueBuilder builder, NpgsqlDriverOptions? options = null)
    {
        ArgumentNullException.ThrowIfNull(builder);
        return builder.UseDriver(sp => new NpgsqlDriver(sp.GetRequiredService<NpgsqlDataSource>(), options));
    }

    /// <summary>Uses <paramref name="dataSource"/>, which the application owns and disposes.</summary>
    /// <param name="builder">The builder.</param>
    /// <param name="dataSource">The data source.</param>
    /// <param name="options">Schema, table prefix and notifier settings. <c>null</c> uses the defaults.</param>
    /// <returns>The builder.</returns>
    public static KopiQueueBuilder UseNpgsql(this KopiQueueBuilder builder, NpgsqlDataSource dataSource, NpgsqlDriverOptions? options = null)
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentNullException.ThrowIfNull(dataSource);
        return builder.UseDriver(_ => new NpgsqlDriver(dataSource, options));
    }
#pragma warning restore RS0026

    /// <summary>
    /// Uses the <see cref="NpgsqlDataSource"/> registered under <paramref name="serviceKey"/>, for example with
    /// <c>services.AddNpgsqlDataSource(connectionString, serviceKey: "jobs")</c>.
    /// </summary>
    /// <param name="builder">The builder.</param>
    /// <param name="serviceKey">The data source's service key.</param>
    /// <param name="options">Schema, table prefix and notifier settings. <c>null</c> uses the defaults.</param>
    /// <returns>The builder.</returns>
    public static KopiQueueBuilder UseKeyedNpgsql(this KopiQueueBuilder builder, object serviceKey, NpgsqlDriverOptions? options = null)
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentNullException.ThrowIfNull(serviceKey);
        return builder.UseDriver(sp => new NpgsqlDriver(sp.GetRequiredKeyedService<NpgsqlDataSource>(serviceKey), options));
    }
}
