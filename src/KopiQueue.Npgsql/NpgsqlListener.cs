using System.Text;
using KopiQueue.Drivers;
using Npgsql;

namespace KopiQueue.Npgsql;

/// <summary>
/// Opens the notifier's LISTEN connections (design §3, §4.3; spike 0.7). Each is a dedicated, non-pooled connection,
/// because a pool reset silently drops LISTEN (D1), with Npgsql's <c>Keepalive</c> so that a silently dead connection
/// is found (NFR-REL-04a).
/// </summary>
internal sealed class NpgsqlListener(NpgsqlDataSource dataSource, string? schema, string? notifierConnectionString, TimeSpan keepAlive)
    : IKopiListener
{
    /// <summary>PostgreSQL's identifier limit: <c>LISTEN</c> truncates longer channel names while <c>pg_notify</c> rejects them.</summary>
    public const int MaxChannelBytes = 63;

    /// <summary>River's channel names, after the schema and a dot.</summary>
    internal static readonly IReadOnlyList<(NotificationTopic Topic, string Name)> Topics =
    [
        (NotificationTopic.Insert, "river_insert"),
        (NotificationTopic.Control, "river_control"),
        (NotificationTopic.Leadership, "river_leadership"),
    ];

    public async Task<IKopiListenerConnection> ConnectAsync(CancellationToken cancellationToken)
    {
        if (schema is not null)
        {
            _ = Channels(schema);
        }

        var connection = await OpenAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            var channels = Channels(schema ?? await CurrentSchemaAsync(connection, cancellationToken).ConfigureAwait(false));
            var listener = new NpgsqlListenerConnection(connection, channels);
            await listener.ListenAsync(cancellationToken).ConfigureAwait(false);
            return listener;
        }
        catch
        {
            await connection.DisposeAsync().ConfigureAwait(false);
            throw;
        }
    }

    /// <summary>The quoted-later channel names for <paramref name="schemaName"/>, checked against the 63-byte limit (D4).</summary>
    /// <exception cref="KopiQueueConfigurationException">A channel name is too long.</exception>
    internal static Dictionary<string, NotificationTopic> Channels(string schemaName)
    {
        var channels = new Dictionary<string, NotificationTopic>(StringComparer.Ordinal);
        foreach (var (topic, name) in Topics)
        {
            var channel = $"{schemaName}.{name}";
            var bytes = Encoding.UTF8.GetByteCount(channel);
            if (bytes > MaxChannelBytes)
            {
                throw new KopiQueueConfigurationException(
                    $"The notification channel \"{channel}\" is {bytes} bytes, and PostgreSQL allows {MaxChannelBytes}, so the schema name "
                    + $"can be at most {MaxChannelBytes - Encoding.UTF8.GetByteCount(".river_leadership")} bytes with notifications on. "
                    + "Use a shorter schema name, or run in poll-only mode.");
            }

            channels[channel] = topic;
        }

        return channels;
    }

    /// <summary>The connection string the notifier uses: never pooled, with keep-alive, never enlisted.</summary>
    internal string ConnectionString(string baseConnectionString) =>
        new NpgsqlConnectionStringBuilder(baseConnectionString)
        {
            Pooling = false,
            KeepAlive = (int)Math.Ceiling(keepAlive.TotalSeconds),
            Enlist = false,
        }.ConnectionString;

    private static async Task<string> CurrentSchemaAsync(NpgsqlConnection connection, CancellationToken cancellationToken)
    {
        var command = new NpgsqlCommand("SELECT current_schema()", connection);
        await using (command.ConfigureAwait(false))
        {
            return await command.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false) as string
                ?? throw new KopiQueueConfigurationException(
                    "The notifier connection has no current schema (its search_path names no existing schema). Configure the schema.");
        }
    }

    private async Task<NpgsqlConnection> OpenAsync(CancellationToken cancellationToken)
    {
        NpgsqlConnection connection;
        if (notifierConnectionString is not null)
        {
            connection = new NpgsqlConnection(ConnectionString(notifierConnectionString));
        }
        else
        {
            // The data source's ConnectionString leaves out the password; CloneWith carries the data source's credentials over.
            var template = dataSource.CreateConnection();
            await using (template.ConfigureAwait(false))
            {
                connection = await template.CloneWithAsync(ConnectionString(dataSource.ConnectionString), cancellationToken).ConfigureAwait(false);
            }
        }

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

/// <summary>One open LISTEN connection. Not thread-safe: the notifier's loop is its only caller.</summary>
internal sealed class NpgsqlListenerConnection : IKopiListenerConnection
{
    private readonly NpgsqlConnection _connection;
    private readonly Dictionary<string, NotificationTopic> _channels;
    private readonly Queue<KopiNotification> _pending = new();

    public NpgsqlListenerConnection(NpgsqlConnection connection, Dictionary<string, NotificationTopic> channels)
    {
        _connection = connection;
        _channels = channels;
        _connection.Notification += OnNotification;
    }

    /// <summary>The server process ID of the connection, for tests.</summary>
    internal int ProcessId => _connection.ProcessID;

    /// <summary><c>LISTEN</c> on every channel. Names are always quoted: the dot is a syntax error otherwise (D4).</summary>
    public async Task ListenAsync(CancellationToken cancellationToken)
    {
        var batch = new NpgsqlBatch(_connection);
        await using (batch.ConfigureAwait(false))
        {
            foreach (var channel in _channels.Keys)
            {
                batch.BatchCommands.Add(new NpgsqlBatchCommand($"LISTEN {Quote(channel)}"));
            }

            await batch.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
        }
    }

    public async Task<KopiNotification> WaitForNotificationAsync(CancellationToken cancellationToken)
    {
        KopiNotification? notification;
        while (!_pending.TryDequeue(out notification))
        {
            await _connection.WaitAsync(cancellationToken).ConfigureAwait(false);
        }

        return notification;
    }

    public ValueTask DisposeAsync()
    {
        _connection.Notification -= OnNotification;
        return _connection.DisposeAsync();
    }

    internal static string Quote(string identifier) => "\"" + identifier.Replace("\"", "\"\"", StringComparison.Ordinal) + "\"";

    private void OnNotification(object sender, NpgsqlNotificationEventArgs e)
    {
        if (_channels.TryGetValue(e.Channel, out var topic))
        {
            _pending.Enqueue(new KopiNotification(topic, e.Payload));
        }
    }
}
