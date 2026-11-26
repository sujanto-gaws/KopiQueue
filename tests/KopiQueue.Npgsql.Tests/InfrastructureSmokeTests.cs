using System.Globalization;
using KopiQueue.TestInfrastructure;
using Npgsql;

namespace KopiQueue.Npgsql.Tests;

/// <summary>Proves the PostgreSQL test fixture works (M0 task 0.4). Replaced by the conformance suite from M1.</summary>
public sealed class InfrastructureSmokeTests(PostgresFixture db) : IClassFixture<PostgresFixture>
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Fact]
    public async Task Server_version_is_supported()
    {
        db.SkipIfUnavailable();
        await using var ds = db.CreateDataSource();
        await using var cmd = ds.CreateCommand("SHOW server_version_num");

        var versionNum = int.Parse((string)(await cmd.ExecuteScalarAsync(Ct))!, CultureInfo.InvariantCulture);

        Assert.True(versionNum >= 14_00_00, $"PostgreSQL 14+ required (NFR-COMP-02), got {versionNum}");
    }

    [Fact]
    public async Task Fixture_database_is_fresh_and_isolated()
    {
        db.SkipIfUnavailable();
        await using var ds = db.CreateDataSource();
        await using var cmd = ds.CreateCommand(
            "SELECT current_database(), (SELECT count(*) FROM pg_tables WHERE schemaname = 'public')");
        await using var reader = await cmd.ExecuteReaderAsync(Ct);
        Assert.True(await reader.ReadAsync(Ct));

        Assert.Equal(db.DatabaseName, reader.GetString(0));
        Assert.Equal(0L, reader.GetInt64(1));
    }

    [Fact]
    public async Task Listen_notify_round_trip_is_delivered_only_after_commit()
    {
        db.SkipIfUnavailable();
        await using var ds = db.CreateDataSource();
        await using var listener = await ds.OpenConnectionAsync(Ct);
        var received = new TaskCompletionSource<string>(TaskCreationOptions.RunContinuationsAsynchronously);
        listener.Notification += (_, e) => received.TrySetResult(e.Payload);
        await using (var listen = new NpgsqlCommand("LISTEN kopi_smoke", listener))
        {
            await listen.ExecuteNonQueryAsync(Ct);
        }

        await using var sender = await ds.OpenConnectionAsync(Ct);
        await using var tx = await sender.BeginTransactionAsync(Ct);
        await using (var notify = new NpgsqlCommand("SELECT pg_notify('kopi_smoke', '{\"queue\":\"default\"}')", sender, tx))
        {
            await notify.ExecuteNonQueryAsync(Ct);
        }

        // Not delivered before commit (the property FR-INS-07 relies on).
        using (var early = CancellationTokenSource.CreateLinkedTokenSource(Ct))
        {
            early.CancelAfter(TimeSpan.FromMilliseconds(300));
            await Assert.ThrowsAnyAsync<OperationCanceledException>(() => listener.WaitAsync(early.Token));
        }
        Assert.False(received.Task.IsCompleted);

        await tx.CommitAsync(Ct);
        using var timeout = CancellationTokenSource.CreateLinkedTokenSource(Ct);
        timeout.CancelAfter(TimeSpan.FromSeconds(5));
        await listener.WaitAsync(timeout.Token);

        Assert.Equal("{\"queue\":\"default\"}", await received.Task);
    }
}
