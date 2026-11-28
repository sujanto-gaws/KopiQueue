using System.Data;
using KopiQueue.SqlServer.Tests.Migrations;
using KopiQueue.TestInfrastructure;
using KopiQueue.TestInfrastructure.Conformance;
using Microsoft.Data.SqlClient;

namespace KopiQueue.SqlServer.Tests;

/// <summary>
/// The driver conformance suite on SQL Server with READ_COMMITTED_SNAPSHOT off, SQL Server's default: reads take shared
/// locks and wait for writers (plan task 5.8: FR-DB-06, NFR-REL-06).
/// </summary>
public sealed class SqlServerConformanceTests(SqlServerConformanceFixture fixture)
    : DriverConformanceTests<SqlServerConformanceFixture>(fixture), IClassFixture<SqlServerConformanceFixture>;

/// <summary>
/// The driver conformance suite on SQL Server with READ_COMMITTED_SNAPSHOT on, Azure SQL Database's default: reads see
/// the last committed version and never wait (NFR-REL-06).
/// </summary>
public sealed class SqlServerRcsiConformanceTests(SqlServerRcsiConformanceFixture fixture)
    : DriverConformanceTests<SqlServerRcsiConformanceFixture>(fixture), IClassFixture<SqlServerRcsiConformanceFixture>;

/// <summary>
/// The driver conformance suite on SQL Server with SNAPSHOT isolation wherever application code can leave it: every
/// caller's transaction is a SNAPSHOT transaction, and the driver's pooled sessions carry SNAPSHOT, which survives
/// SqlClient's pool reset (spike 0.9, NFR-REL-06). <c>READPAST</c> fails with error 650 under SNAPSHOT, so each locking
/// statement must set READ COMMITTED for its own batch.
/// </summary>
public sealed class SqlServerSnapshotConformanceTests(SqlServerSnapshotConformanceFixture fixture)
    : DriverConformanceTests<SqlServerSnapshotConformanceFixture>(fixture), IClassFixture<SqlServerSnapshotConformanceFixture>;

/// <summary>SQL Server schemas for the conformance suite with READ_COMMITTED_SNAPSHOT off.</summary>
public sealed class SqlServerConformanceFixture() : SqlServerConformanceFixtureBase(SqlServerIsolation.LockingReadCommitted);

/// <summary>SQL Server schemas for the conformance suite with READ_COMMITTED_SNAPSHOT on.</summary>
public sealed class SqlServerRcsiConformanceFixture() : SqlServerConformanceFixtureBase(SqlServerIsolation.ReadCommittedSnapshot);

/// <summary>SQL Server schemas for the conformance suite with SNAPSHOT callers and pooled sessions.</summary>
public sealed class SqlServerSnapshotConformanceFixture() : SqlServerConformanceFixtureBase(SqlServerIsolation.Snapshot);

/// <summary>How a conformance database reads and which isolation callers and pooled sessions use.</summary>
public enum SqlServerIsolation
{
    /// <summary>READ_COMMITTED_SNAPSHOT off: locking READ COMMITTED.</summary>
    LockingReadCommitted,

    /// <summary>READ_COMMITTED_SNAPSHOT on.</summary>
    ReadCommittedSnapshot,

    /// <summary>ALLOW_SNAPSHOT_ISOLATION on, SNAPSHOT caller transactions and SNAPSHOT left on the driver's pooled sessions.</summary>
    Snapshot,
}

/// <summary>SQL Server schemas for the conformance suite, in a fresh database per test class.</summary>
public abstract class SqlServerConformanceFixtureBase(SqlServerIsolation isolation) : DriverConformanceFixture
{
    /// <summary>How many of the driver's pooled sessions the SNAPSHOT fixture leaves in SNAPSHOT, per schema.</summary>
    private const int SnapshotSessions = 4;

    private readonly SqlServerFixture _database = new();

    /// <inheritdoc />
    public override void SkipIfUnavailable() => _database.SkipIfUnavailable();

    /// <inheritdoc />
    public override async ValueTask InitializeAsync()
    {
        await _database.InitializeAsync();
        if (string.IsNullOrEmpty(_database.ConnectionString))
        {
            return;
        }

        switch (isolation)
        {
            case SqlServerIsolation.ReadCommittedSnapshot:
                await _database.ExecuteAsync("ALTER DATABASE CURRENT SET READ_COMMITTED_SNAPSHOT ON WITH ROLLBACK IMMEDIATE");
                break;
            case SqlServerIsolation.Snapshot:
                await _database.ExecuteAsync("ALTER DATABASE CURRENT SET ALLOW_SNAPSHOT_ISOLATION ON");
                break;
        }
    }

    /// <inheritdoc />
    public override async ValueTask DisposeAsync()
    {
        GC.SuppressFinalize(this);
        await base.DisposeAsync();
        await _database.DisposeAsync();
    }

    /// <inheritdoc />
    protected override async Task<ConformanceSchema> CreateSchemaCoreAsync(CancellationToken cancellationToken)
    {
        var name = "c_" + Guid.NewGuid().ToString("N")[..12];
        var cs = _database.ConnectionString;
        await using (var conn = new SqlConnection(cs))
        {
            await conn.OpenAsync(cancellationToken);
            await using var cmd = new SqlCommand($"CREATE SCHEMA [{name}]", conn);
            await cmd.ExecuteNonQueryAsync(cancellationToken);
        }

        var driver = new SqlServerDriver(cs, new SqlServerDriverOptions { Schema = name });
        if (isolation == SqlServerIsolation.Snapshot)
        {
            await LeaveSnapshotOnPooledSessionsAsync(driver.Connections.ConnectionString, cancellationToken);
        }

        var callerIsolation = isolation == SqlServerIsolation.Snapshot ? IsolationLevel.Snapshot : IsolationLevel.ReadCommitted;
        return new ConformanceSchema(
            driver,
            async ct =>
            {
                var connection = new SqlConnection(cs);
                await connection.OpenAsync(ct);
                return new CallerTransaction(connection, await connection.BeginTransactionAsync(callerIsolation, ct));
            },
            async (kind, ct) =>
            {
                // READPAST: under locking READ COMMITTED, a plain count would wait for a caller's uncommitted inserts, which
                // PostgreSQL's MVCC never does; the suite counts committed jobs. The SET is needed as in the driver: a SNAPSHOT
                // caller's session returns to the pool still in SNAPSHOT, where READPAST fails (error 650).
                await using var conn = new SqlConnection(cs);
                await conn.OpenAsync(ct);
                await using var cmd = new SqlCommand(
                    $"SET TRANSACTION ISOLATION LEVEL READ COMMITTED; SELECT COUNT_BIG(*) FROM [{name}].kopi_job WITH (READPAST) WHERE kind = @kind", conn);
                cmd.Parameters.AddWithValue("@kind", kind);
                return (long)(await cmd.ExecuteScalarAsync(ct))!;
            },
            () => ValueTask.CompletedTask,
            async (id, key, states, ct) =>
            {
                await using var conn = new SqlConnection(cs);
                await conn.OpenAsync(ct);
                await using var cmd = new SqlCommand($"UPDATE [{name}].kopi_job SET unique_key = @key, unique_states = @states WHERE id = @id", conn);
                cmd.Parameters.AddWithValue("@id", id);
                cmd.Parameters.AddWithValue("@key", key);
                cmd.Parameters.AddWithValue("@states", states);
                await cmd.ExecuteNonQueryAsync(ct);
            });
    }

    /// <summary>
    /// Opens several of the driver's pooled sessions at once, sets SNAPSHOT on each and returns them to the pool, as
    /// application code sharing the pool could. SqlClient's pool reset keeps the isolation level (spike 0.9).
    /// </summary>
    private static async Task LeaveSnapshotOnPooledSessionsAsync(string driverConnectionString, CancellationToken cancellationToken)
    {
        var connections = new List<SqlConnection>();
        try
        {
            for (var i = 0; i < SnapshotSessions; i++)
            {
                var connection = new SqlConnection(driverConnectionString);
                connections.Add(connection);
                await connection.OpenAsync(cancellationToken);
                await using var cmd = new SqlCommand("SET TRANSACTION ISOLATION LEVEL SNAPSHOT", connection);
                await cmd.ExecuteNonQueryAsync(cancellationToken);
            }
        }
        finally
        {
            foreach (var connection in connections)
            {
                await connection.DisposeAsync();
            }
        }
    }
}
