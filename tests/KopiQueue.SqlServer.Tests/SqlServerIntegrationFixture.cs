using KopiQueue.Drivers;
using KopiQueue.SqlServer.Tests.Migrations;
using KopiQueue.TestInfrastructure;
using KopiQueue.TestInfrastructure.Conformance;
using KopiQueue.TestInfrastructure.Integration;
using Microsoft.Data.SqlClient;

namespace KopiQueue.SqlServer.Tests;

/// <summary>
/// SQL Server for the shared client integration suites (plan task 5.8): a fresh database per test class, with
/// READ_COMMITTED_SNAPSHOT on, so the suites' reads don't wait on running clients, as on PostgreSQL. The conformance
/// suite covers the driver with it off.
/// </summary>
public sealed class SqlServerIntegrationFixture : IntegrationFixture
{
    private readonly SqlServerFixture _database = new();

    /// <inheritdoc />
    public override bool HasNotifications => false;

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
            await _database.ExecuteAsync("ALTER DATABASE CURRENT SET READ_COMMITTED_SNAPSHOT ON WITH ROLLBACK IMMEDIATE");
        }
    }

    /// <inheritdoc />
    public override async ValueTask DisposeAsync()
    {
        await base.DisposeAsync();
        await _database.DisposeAsync();
    }

    /// <inheritdoc />
    protected override async Task<IntegrationSchema> CreateSchemaCoreAsync(CancellationToken cancellationToken)
    {
        var name = "x_" + Guid.NewGuid().ToString("N")[..12];
        await _database.ExecuteAsync($"CREATE SCHEMA [{name}]");
        return new Schema(_database.ConnectionString, name);
    }

    private sealed class Schema(string connectionString, string name) : IntegrationSchema
    {
        public override IKopiDriver Driver { get; } = new SqlServerDriver(connectionString, new SqlServerDriverOptions { Schema = name });

        public override string Name => name;

        public override async Task<CallerTransaction> BeginTransactionAsync(CancellationToken cancellationToken)
        {
            var connection = new SqlConnection(connectionString);
            await connection.OpenAsync(cancellationToken);
            return new CallerTransaction(connection, await connection.BeginTransactionAsync(cancellationToken));
        }

        public override async Task<DateTimeOffset> NowAsync(CancellationToken cancellationToken) =>
            new(DateTime.SpecifyKind((DateTime)(await ScalarAsync("SELECT SYSUTCDATETIME()", cancellationToken))!, DateTimeKind.Utc));

        public override Task SetLeaderAsync(string leaderId, TimeSpan expiresIn, CancellationToken cancellationToken) => ExecuteAsync(
            $"""
            DECLARE @now datetime2(6) = SYSUTCDATETIME();
            INSERT INTO [{name}].kopi_leader (leader_id, elected_at, expires_at)
            VALUES (@p0, @now, DATEADD(microsecond, @p2, DATEADD(second, @p1, @now)))
            """,
            cancellationToken,
            leaderId,
            (int)expiresIn.TotalSeconds,
            (int)(expiresIn.Ticks % TimeSpan.TicksPerSecond / 10));

        public override Task DeleteLeaderAsync(CancellationToken cancellationToken) =>
            ExecuteAsync($"DELETE FROM [{name}].kopi_leader", cancellationToken);

        public override async Task<string?> LeaderAsync(CancellationToken cancellationToken) =>
            (string?)await ScalarAsync($"SELECT leader_id FROM [{name}].kopi_leader", cancellationToken);

        public override Task InsertQueueAsync(string queue, TimeSpan age, bool paused, CancellationToken cancellationToken) => ExecuteAsync(
            $"""
            DECLARE @at datetime2(6) = DATEADD(second, -@p1, SYSUTCDATETIME());
            INSERT INTO [{name}].kopi_queue (name, created_at, updated_at, paused_at) VALUES (@p0, @at, @at, IIF(@p2 = 1, @at, NULL))
            """,
            cancellationToken,
            queue,
            (int)age.TotalSeconds,
            paused);

        public override async Task<IAsyncDisposable> LockQueueRowAsync(string queue, CancellationToken cancellationToken)
        {
            var holder = await BeginTransactionAsync(cancellationToken);
            await using var cmd = new SqlCommand(
                $"SELECT 1 FROM [{name}].kopi_queue WITH (UPDLOCK, ROWLOCK) WHERE name = @p0",
                (SqlConnection)holder.Transaction.Connection!,
                (SqlTransaction)holder.Transaction);
            cmd.Parameters.AddWithValue("@p0", queue);
            await cmd.ExecuteNonQueryAsync(cancellationToken);
            return holder;
        }

        public override Task SetUniqueAsync(long id, byte[] key, byte states, CancellationToken cancellationToken) => ExecuteAsync(
            $"UPDATE [{name}].kopi_job SET unique_key = @p1, unique_states = @p2 WHERE id = @p0", cancellationToken, id, key, states);

        public override async Task<(byte[]? Key, byte? States)> UniqueAsync(long id, CancellationToken cancellationToken)
        {
            await using var connection = new SqlConnection(connectionString);
            await connection.OpenAsync(cancellationToken);
            await using var cmd = new SqlCommand($"SELECT unique_key, unique_states FROM [{name}].kopi_job WHERE id = @p0", connection);
            cmd.Parameters.AddWithValue("@p0", id);
            await using var reader = await cmd.ExecuteReaderAsync(cancellationToken);
            Assert.True(await reader.ReadAsync(cancellationToken), $"Job {id} is missing.");
            return (reader.IsDBNull(0) ? null : (byte[])reader[0], reader.IsDBNull(1) ? null : reader.GetByte(1));
        }

        public override bool IsUniqueViolation(Exception exception) => exception is SqlException { Number: 2601 or 2627 };

        public override Task MarkCancelAttemptedAsync(long id, CancellationToken cancellationToken) => ExecuteAsync(
            $"""
            UPDATE [{name}].kopi_job
            SET metadata = JSON_MODIFY(metadata, '$.cancel_attempted_at', CONVERT(nvarchar(30), SYSUTCDATETIME(), 126) + N'Z')
            WHERE id = @p0
            """,
            cancellationToken,
            id);

        public override ValueTask DisposeAsync() => ValueTask.CompletedTask;

        private async Task ExecuteAsync(string sql, CancellationToken cancellationToken, params object[] values)
        {
            await using var connection = new SqlConnection(connectionString);
            await connection.OpenAsync(cancellationToken);
            await using var cmd = new SqlCommand(sql, connection);
            for (var i = 0; i < values.Length; i++)
            {
                cmd.Parameters.AddWithValue("@p" + i, values[i]);
            }

            await cmd.ExecuteNonQueryAsync(cancellationToken);
        }

        private async Task<object?> ScalarAsync(string sql, CancellationToken cancellationToken)
        {
            await using var connection = new SqlConnection(connectionString);
            await connection.OpenAsync(cancellationToken);
            await using var cmd = new SqlCommand(sql, connection);
            var value = await cmd.ExecuteScalarAsync(cancellationToken);
            return value is DBNull ? null : value;
        }
    }
}
