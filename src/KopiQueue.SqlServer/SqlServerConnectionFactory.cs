using Microsoft.Data.SqlClient;

namespace KopiQueue.SqlServer;

/// <summary>
/// Opens the driver's connections. Unless the connection string names an application, it gets
/// <c>Application Name=KopiQueue</c>: KopiQueue's sessions then have a connection pool of their own, so session state
/// that application code leaves on a pooled connection (such as SNAPSHOT isolation) does not reach them, and they can
/// be told apart in DMVs (design §8.1, spike 0.9 D3).
/// </summary>
internal sealed class SqlServerConnectionFactory
{
    /// <summary>The application name KopiQueue's connections get by default.</summary>
    public const string DefaultApplicationName = "KopiQueue";

    private const string ApplicationNameKeyword = "Application Name";

    /// <exception cref="ArgumentException">The connection string is empty or not valid.</exception>
    public SqlServerConnectionFactory(string connectionString)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(connectionString);
        var builder = new SqlConnectionStringBuilder(connectionString);
        if (!builder.ShouldSerialize(ApplicationNameKeyword))
        {
            builder.ApplicationName = DefaultApplicationName;
        }

        ConnectionString = builder.ConnectionString;
    }

    /// <summary>The connection string the driver uses, with the application name applied. It may hold a password: never log it.</summary>
    public string ConnectionString { get; }

    /// <summary>Opens a pooled connection. The caller disposes it.</summary>
    public async Task<SqlConnection> OpenAsync(CancellationToken cancellationToken)
    {
        var connection = new SqlConnection(ConnectionString);
        try
        {
            await connection.OpenAsync(cancellationToken).ConfigureAwait(false);
            return connection;
        }
        catch
        {
            await connection.DisposeAsync().ConfigureAwait(false);
            throw;
        }
    }
}

/// <summary>Quoting of identifiers that are composed into SQL Server statements.</summary>
internal static class SqlServerIdentifiers
{
    /// <summary>Quotes <paramref name="identifier"/> as <c>QUOTENAME</c> does: in brackets, with <c>]</c> doubled.</summary>
    public static string Quote(string identifier)
    {
        ArgumentNullException.ThrowIfNull(identifier);
        return "[" + identifier.Replace("]", "]]", StringComparison.Ordinal) + "]";
    }
}
