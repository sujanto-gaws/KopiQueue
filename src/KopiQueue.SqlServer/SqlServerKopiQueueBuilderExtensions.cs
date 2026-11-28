using KopiQueue;
using KopiQueue.SqlServer;

namespace Microsoft.Extensions.DependencyInjection;

/// <summary>Uses SQL Server for a client registered with <c>AddKopiQueue</c> (FR-HST-04, FR-DB-03).</summary>
/// <remarks>
/// SqlClient has no data source to register in the application's services, as Npgsql has, so the driver takes a
/// connection string. Its connections get <c>Application Name=KopiQueue</c> unless the string names an application,
/// which gives them a pool of their own (see <see cref="SqlServerDriver"/>).
/// </remarks>
public static class SqlServerKopiQueueBuilderExtensions
{
#pragma warning disable RS0026 // The overloads differ by the type of a required argument, so a call cannot be ambiguous.
    /// <summary>Uses the SQL Server database <paramref name="connectionString"/> names.</summary>
    /// <param name="builder">The builder.</param>
    /// <param name="connectionString">A SqlClient connection string. It is checked when the client is created.</param>
    /// <param name="options">Schema and table prefix. <c>null</c> uses the defaults.</param>
    /// <returns>The builder.</returns>
    /// <example>
    /// <code>
    /// services.AddKopiQueue(kq => kq.UseSqlServer(configuration.GetConnectionString("Jobs")!));
    /// </code>
    /// </example>
    public static KopiQueueBuilder UseSqlServer(this KopiQueueBuilder builder, string connectionString, SqlServerDriverOptions? options = null)
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentException.ThrowIfNullOrWhiteSpace(connectionString);
        return builder.UseDriver(_ => new SqlServerDriver(connectionString, options));
    }

    /// <summary>
    /// Uses the SQL Server database whose connection string <paramref name="connectionString"/> returns from the
    /// application's services when the client is created, for example from <c>IConfiguration</c>.
    /// </summary>
    /// <param name="builder">The builder.</param>
    /// <param name="connectionString">Returns a SqlClient connection string.</param>
    /// <param name="options">Schema and table prefix. <c>null</c> uses the defaults.</param>
    /// <returns>The builder.</returns>
    /// <example>
    /// <code>
    /// services.AddKopiQueue(kq => kq.UseSqlServer(sp => sp.GetRequiredService&lt;IConfiguration&gt;().GetConnectionString("Jobs")!));
    /// </code>
    /// </example>
    public static KopiQueueBuilder UseSqlServer(
        this KopiQueueBuilder builder, Func<IServiceProvider, string> connectionString, SqlServerDriverOptions? options = null)
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentNullException.ThrowIfNull(connectionString);
        return builder.UseDriver(sp => new SqlServerDriver(connectionString(sp), options));
    }
#pragma warning restore RS0026
}
