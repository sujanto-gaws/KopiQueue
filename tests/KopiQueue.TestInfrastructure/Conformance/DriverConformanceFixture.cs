using System.Data.Common;
using KopiQueue.Drivers;
using KopiQueue.Migrations;
using Xunit;

namespace KopiQueue.TestInfrastructure.Conformance;

/// <summary>
/// What a driver supplies to run <see cref="DriverConformanceTests{TFixture}"/> (FR-DB-06, ADR-012): schemas to test in,
/// caller transactions, and one read-only query the executor does not offer. Everything else the suite does goes
/// through <see cref="IKopiDriver"/> and <see cref="KopiClient"/>, so the same cases run unchanged on every driver.
/// </summary>
/// <remarks>Use through <c>IClassFixture&lt;T&gt;</c>. One migrated schema is shared by a test class (<see cref="GetMigratedSchemaAsync"/>).</remarks>
public abstract class DriverConformanceFixture : IAsyncLifetime
{
    private readonly SemaphoreSlim _gate = new(1, 1);
    private readonly List<ConformanceSchema> _schemas = [];
    private ConformanceSchema? _migrated;

    /// <summary>Skips the calling test when the database server is unavailable (locally; CI requires it).</summary>
    public abstract void SkipIfUnavailable();

    /// <summary>The shared schema, migrated to the latest version once per test class.</summary>
    public async Task<ConformanceSchema> GetMigratedSchemaAsync(CancellationToken cancellationToken)
    {
        await _gate.WaitAsync(cancellationToken);
        try
        {
            if (_migrated is null)
            {
                var schema = await CreateSchemaAsync(cancellationToken);
                await schema.Driver.CreateMigrator().MigrateAsync(MigrationDirection.Up, cancellationToken: cancellationToken);
                _migrated = schema;
            }

            return _migrated;
        }
        finally
        {
            _gate.Release();
        }
    }

    /// <summary>A new, empty schema for a test that migrates it itself. It is dropped with the fixture.</summary>
    public async Task<ConformanceSchema> CreateSchemaAsync(CancellationToken cancellationToken)
    {
        var schema = await CreateSchemaCoreAsync(cancellationToken);
        lock (_schemas)
        {
            _schemas.Add(schema);
        }

        return schema;
    }

    /// <summary>Creates an empty schema and a driver on it.</summary>
    protected abstract Task<ConformanceSchema> CreateSchemaCoreAsync(CancellationToken cancellationToken);

    /// <inheritdoc />
    public abstract ValueTask InitializeAsync();

    /// <inheritdoc />
    public virtual async ValueTask DisposeAsync()
    {
        GC.SuppressFinalize(this);
        ConformanceSchema[] schemas;
        lock (_schemas)
        {
            schemas = [.. _schemas];
        }

        foreach (var schema in schemas)
        {
            await schema.DisposeAsync();
        }

        _gate.Dispose();
    }
}

/// <summary>A schema under test: a driver on it plus the driver-specific helpers the suite needs.</summary>
/// <param name="driver">The driver, configured for this schema.</param>
/// <param name="beginTransactionAsync">Opens a new connection and begins a caller transaction on it.</param>
/// <param name="countJobsAsync">Counts committed jobs of a kind, outside any transaction.</param>
/// <param name="dispose">Releases the driver's resources, such as its data source.</param>
/// <param name="setUniqueAsync">
/// Gives a committed job a unique key and unique states (a bitmask, design §5.6), which the executor's insert cannot do
/// until unique inserts land (plan task 4.2). <c>null</c> when the driver has no such helper; cases that need it then skip.
/// </param>
public sealed class ConformanceSchema(
    IKopiDriver driver,
    Func<CancellationToken, Task<CallerTransaction>> beginTransactionAsync,
    Func<string, CancellationToken, Task<long>> countJobsAsync,
    Func<ValueTask> dispose,
    Func<long, byte[], byte, CancellationToken, Task>? setUniqueAsync = null) : IAsyncDisposable
{
    /// <summary>The driver under test.</summary>
    public IKopiDriver Driver { get; } = driver;

    /// <summary>Begins a caller transaction on a connection of its own.</summary>
    public Task<CallerTransaction> BeginTransactionAsync(CancellationToken cancellationToken) => beginTransactionAsync(cancellationToken);

    /// <summary>Counts committed jobs of <paramref name="kind"/>.</summary>
    public Task<long> CountJobsAsync(string kind, CancellationToken cancellationToken) => countJobsAsync(kind, cancellationToken);

    /// <summary>Whether <see cref="SetUniqueAsync"/> is available.</summary>
    public bool CanSetUnique => setUniqueAsync is not null;

    /// <summary>Sets the unique key and the unique states of committed job <paramref name="id"/>.</summary>
    public Task SetUniqueAsync(long id, byte[] key, byte states, CancellationToken cancellationToken) =>
        (setUniqueAsync ?? throw new NotSupportedException("The driver's fixture cannot set unique keys."))(id, key, states, cancellationToken);

    /// <inheritdoc />
    public ValueTask DisposeAsync() => dispose();
}

/// <summary>A caller's transaction and the connection it runs on; disposing it rolls back if not committed and closes both.</summary>
/// <param name="connection">The connection.</param>
/// <param name="transaction">The transaction.</param>
public sealed class CallerTransaction(DbConnection connection, DbTransaction transaction) : IAsyncDisposable
{
    /// <summary>The transaction, as the driver receives it.</summary>
    public DbTransaction Transaction { get; } = transaction;

    /// <inheritdoc />
    public async ValueTask DisposeAsync()
    {
        await Transaction.DisposeAsync();
        await connection.DisposeAsync();
    }
}
