using KopiQueue.Drivers;
using KopiQueue.TestInfrastructure;
using KopiQueue.TestInfrastructure.Conformance;
using KopiQueue.TestInfrastructure.Integration;
using Npgsql;

namespace KopiQueue.Npgsql.Tests;

/// <summary>PostgreSQL for the shared client integration suites (plan task 5.8): a fresh database per test class.</summary>
public sealed class NpgsqlIntegrationFixture : IntegrationFixture
{
    private readonly PostgresFixture _database = new();
    private NpgsqlDataSource? _dataSource;

    /// <inheritdoc />
    public override bool HasNotifications => true;

    /// <summary>The fixture database's data source, for PostgreSQL-specific checks.</summary>
    public NpgsqlDataSource DataSource => _dataSource ?? throw new InvalidOperationException("The fixture is not initialised.");

    /// <summary>The fixture database's connection string.</summary>
    public string ConnectionString => _database.ConnectionString;

    /// <inheritdoc />
    public override void SkipIfUnavailable() => _database.SkipIfUnavailable();

    /// <inheritdoc />
    public override async ValueTask InitializeAsync()
    {
        await _database.InitializeAsync();
        if (!string.IsNullOrEmpty(_database.ConnectionString))
        {
            _dataSource = _database.CreateDataSource();
        }
    }

    /// <inheritdoc />
    public override async ValueTask DisposeAsync()
    {
        await base.DisposeAsync();
        if (_dataSource is not null)
        {
            await _dataSource.DisposeAsync();
        }

        await _database.DisposeAsync();
    }

    /// <inheritdoc />
    protected override async Task<IntegrationSchema> CreateSchemaCoreAsync(CancellationToken cancellationToken)
    {
        var name = "x_" + Guid.NewGuid().ToString("N")[..12];
        await using (var cmd = DataSource.CreateCommand($"CREATE SCHEMA {name}"))
        {
            await cmd.ExecuteNonQueryAsync(cancellationToken);
        }

        return new Schema(DataSource, name);
    }

    private sealed class Schema(NpgsqlDataSource dataSource, string name) : IntegrationSchema
    {
        public override IKopiDriver Driver { get; } = new NpgsqlDriver(dataSource, new NpgsqlDriverOptions { Schema = name });

        public override string Name => name;

        public override async Task<CallerTransaction> BeginTransactionAsync(CancellationToken cancellationToken)
        {
            var connection = await dataSource.OpenConnectionAsync(cancellationToken);
            return new CallerTransaction(connection, await connection.BeginTransactionAsync(cancellationToken));
        }

        public override async Task<DateTimeOffset> NowAsync(CancellationToken cancellationToken)
        {
            await using var cmd = dataSource.CreateCommand("SELECT now()");
            return new DateTimeOffset((DateTime)(await cmd.ExecuteScalarAsync(cancellationToken))!, TimeSpan.Zero);
        }

        public override Task SetLeaderAsync(string leaderId, TimeSpan expiresIn, CancellationToken cancellationToken) => ExecuteAsync(
            $"INSERT INTO {name}.river_leader (leader_id, elected_at, expires_at) VALUES ($1, now(), now() + $2)", cancellationToken, leaderId, expiresIn);

        public override Task DeleteLeaderAsync(CancellationToken cancellationToken) =>
            ExecuteAsync($"DELETE FROM {name}.river_leader", cancellationToken);

        public override async Task<string?> LeaderAsync(CancellationToken cancellationToken)
        {
            await using var cmd = dataSource.CreateCommand($"SELECT leader_id FROM {name}.river_leader");
            return (string?)await cmd.ExecuteScalarAsync(cancellationToken);
        }

        public override Task InsertQueueAsync(string queue, TimeSpan age, bool paused, CancellationToken cancellationToken) => ExecuteAsync(
            $"""
            INSERT INTO {name}.river_queue (name, metadata, created_at, updated_at, paused_at)
            VALUES ($1, $4::jsonb, now() - $2, now() - $2, CASE WHEN $3 THEN now() - $2 END)
            """,
            cancellationToken,
            queue,
            age,
            paused,
            "{}");

        public override async Task<IAsyncDisposable> LockQueueRowAsync(string queue, CancellationToken cancellationToken)
        {
            var holder = await BeginTransactionAsync(cancellationToken);
            await using var cmd = new NpgsqlCommand(
                $"SELECT 1 FROM {name}.river_queue WHERE name = $1 FOR UPDATE",
                (NpgsqlConnection)holder.Transaction.Connection!,
                (NpgsqlTransaction)holder.Transaction);
            cmd.Parameters.Add(new NpgsqlParameter { Value = queue });
            await cmd.ExecuteNonQueryAsync(cancellationToken);
            return holder;
        }

        public override Task SetUniqueAsync(long id, byte[] key, byte states, CancellationToken cancellationToken) => ExecuteAsync(
            $"UPDATE {name}.river_job SET unique_key = $2, unique_states = $3::integer::bit(8) WHERE id = $1", cancellationToken, id, key, (int)states);

        public override async Task<(byte[]? Key, byte? States)> UniqueAsync(long id, CancellationToken cancellationToken)
        {
            await using var cmd = dataSource.CreateCommand($"SELECT unique_key, unique_states::integer FROM {name}.river_job WHERE id = $1");
            cmd.Parameters.Add(new NpgsqlParameter { Value = id });
            await using var reader = await cmd.ExecuteReaderAsync(cancellationToken);
            Assert.True(await reader.ReadAsync(cancellationToken), $"Job {id} is missing.");
            return (reader.IsDBNull(0) ? null : (byte[])reader[0], reader.IsDBNull(1) ? null : (byte)reader.GetInt32(1));
        }

        public override bool IsUniqueViolation(Exception exception) =>
            exception is PostgresException { SqlState: PostgresErrorCodes.UniqueViolation };

        public override Task MarkCancelAttemptedAsync(long id, CancellationToken cancellationToken) => ExecuteAsync(
            $"UPDATE {name}.river_job SET metadata = metadata || jsonb_build_object('cancel_attempted_at', now()) WHERE id = $1", cancellationToken, id);

        public override ValueTask DisposeAsync() => ValueTask.CompletedTask;

        private async Task ExecuteAsync(string sql, CancellationToken cancellationToken, params object[] values)
        {
            await using var cmd = dataSource.CreateCommand(sql);
            foreach (var value in values)
            {
                cmd.Parameters.Add(new NpgsqlParameter { Value = value });
            }

            await cmd.ExecuteNonQueryAsync(cancellationToken);
        }
    }
}
