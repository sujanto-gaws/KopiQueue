using KopiQueue.TestInfrastructure;
using KopiQueue.TestInfrastructure.Conformance;
using Npgsql;

namespace KopiQueue.Npgsql.Tests;

/// <summary>The driver conformance suite on PostgreSQL (plan task 1.11: FR-DB-06, ADR-012).</summary>
public sealed class NpgsqlConformanceTests(NpgsqlConformanceFixture fixture)
    : DriverConformanceTests<NpgsqlConformanceFixture>(fixture), IClassFixture<NpgsqlConformanceFixture>;

/// <summary>PostgreSQL schemas for the conformance suite, in a fresh database per test class.</summary>
public sealed class NpgsqlConformanceFixture : DriverConformanceFixture
{
    private readonly PostgresFixture _database = new();

    /// <inheritdoc />
    public override void SkipIfUnavailable() => _database.SkipIfUnavailable();

    /// <inheritdoc />
    public override ValueTask InitializeAsync() => _database.InitializeAsync();

    /// <inheritdoc />
    public override async ValueTask DisposeAsync()
    {
        await base.DisposeAsync();
        await _database.DisposeAsync();
    }

    /// <inheritdoc />
    protected override async Task<ConformanceSchema> CreateSchemaCoreAsync(CancellationToken cancellationToken)
    {
        var name = "c_" + Guid.NewGuid().ToString("N")[..12];
        var dataSource = _database.CreateDataSource();
        await using (var cmd = dataSource.CreateCommand($"CREATE SCHEMA {name}"))
        {
            await cmd.ExecuteNonQueryAsync(cancellationToken);
        }

        return new ConformanceSchema(
            new NpgsqlDriver(dataSource, new NpgsqlDriverOptions { Schema = name }),
            async ct =>
            {
                var connection = await dataSource.OpenConnectionAsync(ct);
                return new CallerTransaction(connection, await connection.BeginTransactionAsync(ct));
            },
            async (kind, ct) =>
            {
                await using var cmd = dataSource.CreateCommand($"SELECT count(*) FROM {name}.river_job WHERE kind = $1");
                cmd.Parameters.Add(new NpgsqlParameter { Value = kind });
                return (long)(await cmd.ExecuteScalarAsync(ct))!;
            },
            dataSource.DisposeAsync,
            async (id, key, states, ct) =>
            {
                // unique_states is bit(8); an integer cast keeps its low 8 bits, so available = 1 and scheduled = 128.
                await using var cmd = dataSource.CreateCommand(
                    $"UPDATE {name}.river_job SET unique_key = $2, unique_states = $3::integer::bit(8) WHERE id = $1");
                cmd.Parameters.Add(new NpgsqlParameter { Value = id });
                cmd.Parameters.Add(new NpgsqlParameter { Value = key });
                cmd.Parameters.Add(new NpgsqlParameter { Value = (int)states });
                await cmd.ExecuteNonQueryAsync(ct);
            });
    }
}
