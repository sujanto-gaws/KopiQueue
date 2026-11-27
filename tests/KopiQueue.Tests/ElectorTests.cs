using System.Collections.Concurrent;
using System.Data.Common;
using System.Threading.Channels;
using KopiQueue.Drivers;
using KopiQueue.Execution;
using Microsoft.Extensions.Time.Testing;

namespace KopiQueue.Tests;

/// <summary>
/// The elector's lease rules against an in-memory lease (plan task 3.1: FR-MNT-01; design §5.5): winning, renewing,
/// stepping down before the lease can pass to another client, and resigning.
/// </summary>
/// <remarks>
/// Time is a <see cref="FakeTimeProvider"/> shared with the fake lease, with no jitter: ElectInterval 5 s, LeaderTtl 15 s,
/// so a leader that cannot renew steps down 10 s after its last renewal. The loop runs on its own thread, so tests advance
/// the clock in small steps until the expected state appears, with a real-time limit.
/// </remarks>
public sealed class ElectorTests
{
    private static readonly TimeSpan Step = TimeSpan.FromMilliseconds(100);
    private static readonly TimeSpan Interval = TimeSpan.FromSeconds(5);
    private static readonly TimeSpan Ttl = TimeSpan.FromSeconds(15);

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Fact]
    public async Task Wins_a_free_lease_at_once_and_reports_it_once()
    {
        var (lease, time) = Lease();
        var reports = new ConcurrentQueue<bool>();
        await using var elector = Elector(lease, time, reports);

        await elector.StartAsync(Ct);
        await AdvanceUntil(time, () => elector.IsLeader);
        await Settle(time, Interval * 3);

        Assert.Equal("me", lease.Holder);
        Assert.Equal([true], reports);
    }

    [Fact]
    public async Task Waits_for_another_clients_lease_to_expire_before_winning()
    {
        var (lease, time) = Lease();
        lease.Hold("other", Ttl);
        await using var elector = Elector(lease, time);

        await elector.StartAsync(Ct);
        var waited = await AdvanceUntil(time, () => elector.IsLeader);

        // The other lease ran out at 15 s; the next attempt, every 5 s, wins it.
        Assert.InRange(waited, Ttl, Ttl + Interval + Step);
        Assert.Equal("me", lease.Holder);
    }

    [Fact]
    public async Task Renews_its_lease_every_interval_and_keeps_it()
    {
        var (lease, time) = Lease();
        await using var elector = Elector(lease, time);

        await elector.StartAsync(Ct);
        await AdvanceUntil(time, () => elector.IsLeader);
        await Settle(time, (Interval * 4) + Step);

        Assert.InRange(lease.Renewals, 4, 5);
        Assert.True(lease.ExpiresAt > time.GetUtcNow() + Ttl - Interval - Step, "The lease is kept a full TTL ahead.");
        Assert.True(elector.IsLeader);
    }

    [Fact]
    public async Task Steps_down_at_once_when_another_client_holds_the_lease()
    {
        var (lease, time) = Lease();
        var reports = new ConcurrentQueue<bool>();
        await using var elector = Elector(lease, time, reports);
        await elector.StartAsync(Ct);
        await AdvanceUntil(time, () => elector.IsLeader);

        lease.Hold("other", Ttl);
        var noticed = await AdvanceUntil(time, () => !elector.IsLeader);

        Assert.InRange(noticed, TimeSpan.Zero, Interval + Step);
        Assert.Equal([true, false], reports);
    }

    [Fact]
    public async Task Steps_down_before_its_lease_expires_when_renewals_fail()
    {
        var (lease, time) = Lease();
        var reports = new ConcurrentQueue<(bool Leader, DateTimeOffset At)>();
        await using var elector = new Elector(
            lease, Options(), onLeadershipChanged: (leader, _) => Report(reports, (leader, time.GetUtcNow())), timeProvider: time);
        await elector.StartAsync(Ct);
        await AdvanceUntil(time, () => elector.IsLeader);
        var lastRenewal = time.GetUtcNow();

        lease.DatabaseDown = true;
        await AdvanceUntil(time, () => !elector.IsLeader);

        // Stepped down at TTL − TTL/3 after the last renewal: 5 s before the lease could pass to another client.
        var steppedDown = reports.Last();
        Assert.False(steppedDown.Leader);
        Assert.InRange(steppedDown.At - lastRenewal, (Ttl * 2 / 3) - Step, (Ttl * 2 / 3) + Step);
        Assert.True(steppedDown.At < lease.ExpiresAt, "Maintenance stopped while the lease was still this client's.");
    }

    [Fact]
    public async Task Steps_down_in_time_when_a_renewal_hangs()
    {
        var (lease, time) = Lease();
        await using var elector = Elector(lease, time);
        await elector.StartAsync(Ct);
        await AdvanceUntil(time, () => elector.IsLeader);
        var lastRenewal = time.GetUtcNow();

        lease.RenewalsHang = true;
        var held = await AdvanceUntil(time, () => !elector.IsLeader);

        Assert.InRange(held, (Ttl * 2 / 3) - Step, (Ttl * 2 / 3) + Step);
        Assert.True(lastRenewal + held < lease.ExpiresAt);
    }

    [Fact]
    public async Task Wins_again_after_stepping_down_once_the_database_is_back()
    {
        var (lease, time) = Lease();
        var reports = new ConcurrentQueue<bool>();
        await using var elector = Elector(lease, time, reports);
        await elector.StartAsync(Ct);
        await AdvanceUntil(time, () => elector.IsLeader);
        lease.DatabaseDown = true;
        await AdvanceUntil(time, () => !elector.IsLeader);

        lease.DatabaseDown = false;
        await AdvanceUntil(time, () => elector.IsLeader);

        Assert.Equal([true, false, true], reports);
    }

    [Fact]
    public async Task Stopping_a_leader_steps_down_and_then_resigns()
    {
        var (lease, time) = Lease();
        var events = new ConcurrentQueue<string>();
        lease.OnResign = () => events.Enqueue("resigned");
        await using var elector = new Elector(
            lease, Options(), onLeadershipChanged: (leader, _) => Report(events, leader ? "elected" : "stepped down"), timeProvider: time);
        await elector.StartAsync(Ct);
        await AdvanceUntil(time, () => elector.IsLeader);

        await elector.StopAsync(Ct);

        Assert.Equal(["elected", "stepped down", "resigned"], events);
        Assert.Null(lease.Holder);
        Assert.False(elector.IsLeader);
    }

    [Fact]
    public async Task Stopping_a_follower_resigns_nothing()
    {
        var (lease, time) = Lease();
        lease.Hold("other", Ttl);
        await using var elector = Elector(lease, time);
        await elector.StartAsync(Ct);
        await Settle(time, Interval);

        await elector.StopAsync(Ct);

        Assert.Equal(0, lease.Resignations);
        Assert.Equal("other", lease.Holder);
    }

    [Fact]
    public async Task Disposing_without_stopping_leaves_the_lease_to_expire()
    {
        var (lease, time) = Lease();
        var elector = Elector(lease, time);
        await elector.StartAsync(Ct);
        await AdvanceUntil(time, () => elector.IsLeader);

        await elector.DisposeAsync();

        Assert.Equal(0, lease.Resignations);
        Assert.Equal("me", lease.Holder);
    }

    [Fact]
    public async Task A_resignation_announced_by_another_client_wakes_a_follower_at_once()
    {
        var (lease, time) = Lease();
        lease.Hold("other", TimeSpan.FromHours(1));
        var connection = new FakeConnection();
        await using var notifier = new Notifier(new FakeListener(connection), timeProvider: time);
        await notifier.StartAsync(Ct);
        await AdvanceUntil(time, () => notifier.IsConnected);
        await using var elector = new Elector(lease, Options(), notifier, timeProvider: time);
        await elector.StartAsync(Ct);
        await AdvanceUntil(time, () => lease.Attempts == 1);

        lease.Release();
        connection.Send(NotificationTopic.Leadership, """{"leader_id" : "other", "action" : "resigned"}""");
        var waited = await AdvanceUntil(time, () => elector.IsLeader);

        Assert.True(waited < Interval, $"Won {waited} after the resignation, not at the next interval.");
    }

    [Fact]
    public async Task Its_own_resignation_and_unknown_messages_do_not_wake_a_follower()
    {
        var (lease, time) = Lease();
        lease.Hold("other", TimeSpan.FromHours(1));
        var connection = new FakeConnection();
        await using var notifier = new Notifier(new FakeListener(connection), timeProvider: time);
        await notifier.StartAsync(Ct);
        await AdvanceUntil(time, () => notifier.IsConnected);
        await using var elector = new Elector(lease, Options(), notifier, timeProvider: time);
        await elector.StartAsync(Ct);
        await AdvanceUntil(time, () => lease.Attempts == 1);

        connection.Send(NotificationTopic.Leadership, """{"leader_id" : "me", "action" : "resigned"}""");
        connection.Send(NotificationTopic.Leadership, """{"leader_id" : "other", "action" : "elected"}""");
        connection.Send(NotificationTopic.Leadership, "not json");
        await Settle(time, Interval - Step - Step);

        Assert.Equal(1, lease.Attempts);
    }

    [Fact]
    public async Task A_failing_callback_does_not_stop_it_leading()
    {
        var (lease, time) = Lease();
        await using var elector = new Elector(
            lease, Options(), onLeadershipChanged: (_, _) => throw new InvalidOperationException("boom"), timeProvider: time);

        await elector.StartAsync(Ct);
        await AdvanceUntil(time, () => elector.IsLeader);
        await Settle(time, Interval * 2);

        Assert.True(elector.IsLeader);
        Assert.True(lease.Renewals >= 1);
    }

    [Fact]
    public async Task Maintenance_runs_only_while_leading_and_stops_before_the_lease_can_pass_on()
    {
        var (lease, time) = Lease();
        var service = new MaintenanceHostTests.FakeService(time, "cleaner");
        await using var maintenance = new MaintenanceHost([service], new MaintenanceHostOptions { Jitter = 0 }, time);
        await using var elector = new Elector(lease, Options(), onLeadershipChanged: maintenance.OnLeadershipChangedAsync, timeProvider: time);

        await elector.StartAsync(Ct);
        await AdvanceUntil(time, () => service.Batches >= 1);
        Assert.True(maintenance.IsRunning);

        lease.DatabaseDown = true;
        await AdvanceUntil(time, () => !elector.IsLeader);

        Assert.False(maintenance.IsRunning);
        Assert.True(time.GetUtcNow() < lease.ExpiresAt, "Maintenance stopped while no other client could lead.");
        var batches = service.Batches;
        await Settle(time, Interval * 2);
        Assert.Equal(batches, service.Batches);
    }

    [Fact]
    public void A_lease_shorter_than_two_intervals_is_refused()
    {
        var (lease, time) = Lease();

        Assert.Throws<ArgumentOutOfRangeException>(() => new Elector(
            lease, new ElectorOptions { ClientId = "me", ElectInterval = Interval, LeaderTtl = Interval * 1.5 }, timeProvider: time));
    }

    private static (FakeLease Lease, FakeTimeProvider Time) Lease()
    {
        var time = new FakeTimeProvider();
        return (new FakeLease(time), time);
    }

    private static ElectorOptions Options() => new() { ClientId = "me", ElectInterval = Interval, LeaderTtl = Ttl, Jitter = 0 };

    private static Elector Elector(FakeLease lease, FakeTimeProvider time, ConcurrentQueue<bool>? reports = null) =>
        new(lease, Options(), onLeadershipChanged: reports is null ? null : (leader, _) => Report(reports, leader), timeProvider: time);

    private static Task Report<T>(ConcurrentQueue<T> reports, T value)
    {
        reports.Enqueue(value);
        return Task.CompletedTask;
    }

    /// <summary>Advances fake time in steps until <paramref name="condition"/> holds, and returns how much passed.</summary>
    private static async Task<TimeSpan> AdvanceUntil(FakeTimeProvider time, Func<bool> condition)
    {
        var start = time.GetUtcNow();
        var limit = DateTime.UtcNow.AddSeconds(20);
        while (!condition())
        {
            Assert.True(DateTime.UtcNow < limit, $"The condition was not met within 20 s; fake time advanced {time.GetUtcNow() - start}.");

            // Let the loop reach its next timer before moving the clock, so a step is never skipped over.
            await Task.Delay(5, Ct);
            if (!condition())
            {
                time.Advance(Step);
            }
        }

        return time.GetUtcNow() - start;
    }

    /// <summary>Advances fake time by <paramref name="duration"/> in steps, letting the loop run in between.</summary>
    private static async Task Settle(FakeTimeProvider time, TimeSpan duration)
    {
        for (var passed = TimeSpan.Zero; passed < duration; passed += Step)
        {
            await Task.Delay(5, Ct);
            time.Advance(Step);
        }

        await Task.Delay(20, Ct);
    }

    /// <summary>One lease, on the fake clock, with the driver contract's rules (<see cref="IKopiExecutor.LeaderAttemptElectAsync"/>).</summary>
    private sealed class FakeLease(TimeProvider time) : IKopiDriver, IKopiExecutor
    {
        private readonly Lock _lock = new();
        private int _attempts;
        private int _renewals;
        private int _resignations;

        public string? Holder { get; private set; }

        public DateTimeOffset ExpiresAt { get; private set; }

        public int Attempts => Volatile.Read(ref _attempts);

        public int Renewals => Volatile.Read(ref _renewals);

        public int Resignations => Volatile.Read(ref _resignations);

        /// <summary>Every election and renewal fails, as when the database is unreachable.</summary>
        public volatile bool DatabaseDown;

        public volatile bool RenewalsHang;

        public Action? OnResign { get; set; }

        public DriverCapabilities Capabilities { get; } = new() { ListenNotify = true, BulkCopy = false };

        public void Hold(string holder, TimeSpan ttl)
        {
            lock (_lock)
            {
                (Holder, ExpiresAt) = (holder, time.GetUtcNow() + ttl);
            }
        }

        public void Release()
        {
            lock (_lock)
            {
                Holder = null;
            }
        }

        public Task<bool> LeaderAttemptElectAsync(LeaderElectParams parameters, CancellationToken cancellationToken)
        {
            Interlocked.Increment(ref _attempts);
            if (DatabaseDown)
            {
                return Task.FromException<bool>(new DbExceptionStub("The database is unreachable."));
            }

            lock (_lock)
            {
                var now = time.GetUtcNow();
                if (Holder is null || ExpiresAt < now || Holder == parameters.LeaderId)
                {
                    (Holder, ExpiresAt) = (parameters.LeaderId, now + parameters.Ttl);
                    return Task.FromResult(true);
                }

                return Task.FromResult(false);
            }
        }

        public async Task<bool> LeaderAttemptReelectAsync(LeaderElectParams parameters, CancellationToken cancellationToken)
        {
            if (RenewalsHang)
            {
                await Task.Delay(Timeout.Infinite, cancellationToken);
            }

            if (DatabaseDown)
            {
                throw new DbExceptionStub("The database is unreachable.");
            }

            Interlocked.Increment(ref _renewals);
            lock (_lock)
            {
                if (Holder != parameters.LeaderId)
                {
                    return false;
                }

                ExpiresAt = time.GetUtcNow() + parameters.Ttl;
                return true;
            }
        }

        public Task<bool> LeaderResignAsync(string leaderId, CancellationToken cancellationToken)
        {
            lock (_lock)
            {
                if (Holder != leaderId)
                {
                    return Task.FromResult(false);
                }

                Holder = null;
            }

            Interlocked.Increment(ref _resignations);
            OnResign?.Invoke();
            return Task.FromResult(true);
        }

        public IKopiExecutor GetExecutor() => this;

        public IKopiExecutor GetExecutor(DbTransaction transaction) => throw new NotSupportedException();

        public KopiQueue.Migrations.KopiMigrator CreateMigrator(Microsoft.Extensions.Logging.ILoggerFactory? loggerFactory = null) =>
            throw new NotSupportedException();

        public IKopiListener? CreateListener() => null;

        public Task<IReadOnlyList<JobInsertResult>> JobInsertManyAsync(IReadOnlyList<JobInsertParams> jobs, CancellationToken cancellationToken) =>
            throw new NotSupportedException();

        public Task<long> JobInsertFastAsync(IReadOnlyList<JobInsertParams> jobs, CancellationToken cancellationToken) =>
            throw new NotSupportedException();

        public Task<JobRow?> JobGetByIdAsync(long id, CancellationToken cancellationToken) => throw new NotSupportedException();

        public Task<IReadOnlyList<JobRow>> JobGetAvailableAsync(JobGetAvailableParams parameters, CancellationToken cancellationToken) =>
            throw new NotSupportedException();

        public Task<IReadOnlyList<QueueRow>> QueueCreateOrSetUpdatedAtManyAsync(IReadOnlyList<string> names, CancellationToken cancellationToken) =>
            Task.FromResult<IReadOnlyList<QueueRow>>([]);

        public Task<QueueRow?> QueueGetAsync(string name, CancellationToken cancellationToken) => Task.FromResult<QueueRow?>(null);

        public Task<IReadOnlyList<QueueRow>> QueueListAsync(QueueListQuery query, CancellationToken cancellationToken) =>
            Task.FromResult<IReadOnlyList<QueueRow>>([]);

        public Task<bool> QueuePauseAsync(string name, CancellationToken cancellationToken) => Task.FromResult(false);

        public Task<bool> QueueResumeAsync(string name, CancellationToken cancellationToken) => Task.FromResult(false);

        public Task<QueueRow?> QueueUpdateAsync(QueueUpdateParams parameters, CancellationToken cancellationToken) => Task.FromResult<QueueRow?>(null);

        public Task<IReadOnlyList<string>> QueueDeleteExpiredAsync(QueueDeleteExpiredParams parameters, CancellationToken cancellationToken) =>
            Task.FromResult<IReadOnlyList<string>>([]);

        public Task<IReadOnlyList<JobRow>> JobSetStateIfRunningManyAsync(IReadOnlyList<JobSetStateParams> jobs, CancellationToken cancellationToken) =>
            throw new NotSupportedException();
    }

    private sealed class DbExceptionStub(string message) : DbException(message);

    private sealed class FakeListener(FakeConnection connection) : IKopiListener
    {
        private int _connects;

        public async Task<IKopiListenerConnection> ConnectAsync(CancellationToken cancellationToken)
        {
            if (Interlocked.Increment(ref _connects) > 1)
            {
                await Task.Delay(Timeout.Infinite, cancellationToken);
            }

            return connection;
        }
    }

    private sealed class FakeConnection : IKopiListenerConnection
    {
        private readonly Channel<KopiNotification> _items = Channel.CreateUnbounded<KopiNotification>();

        public void Send(NotificationTopic topic, string payload) => _items.Writer.TryWrite(new KopiNotification(topic, payload));

        public async Task<KopiNotification> WaitForNotificationAsync(CancellationToken cancellationToken) =>
            await _items.Reader.ReadAsync(cancellationToken);

        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }
}
