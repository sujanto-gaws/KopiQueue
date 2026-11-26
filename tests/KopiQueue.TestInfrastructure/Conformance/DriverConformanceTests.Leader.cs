using System.Text.Json;
using KopiQueue.Drivers;
using Xunit;

namespace KopiQueue.TestInfrastructure.Conformance;

/// <summary>Leader lease cases (plan task 3.1: FR-MNT-01; design §5.5).</summary>
/// <remarks>
/// The schema has one lease, shared by every case of a class, so each case uses client IDs of its own and gives the
/// lease up when it ends. The cases commit, since the lease's expiry follows the database's clock.
/// </remarks>
public abstract partial class DriverConformanceTests<TFixture>
{
    [Fact]
    public async Task One_client_at_a_time_holds_the_lease_until_it_resigns()
    {
        var schema = await MigratedAsync();
        var executor = schema.Driver.GetExecutor();
        var (a, b) = (Lease("a"), Lease("b"));
        try
        {
            Assert.True(await executor.LeaderAttemptElectAsync(a, Ct));
            Assert.False(await executor.LeaderAttemptElectAsync(b, Ct));
            Assert.True(await executor.LeaderAttemptElectAsync(a, Ct)); // a holder winning again keeps the lease
            Assert.True(await executor.LeaderAttemptReelectAsync(a, Ct));
            Assert.False(await executor.LeaderAttemptReelectAsync(b, Ct));
            Assert.False(await executor.LeaderResignAsync(b.LeaderId, Ct));

            Assert.True(await executor.LeaderResignAsync(a.LeaderId, Ct));
            Assert.False(await executor.LeaderAttemptReelectAsync(a, Ct));
            Assert.False(await executor.LeaderResignAsync(a.LeaderId, Ct));
            Assert.True(await executor.LeaderAttemptElectAsync(b, Ct));
        }
        finally
        {
            await ReleaseAsync(executor, a, b);
        }
    }

    [Fact]
    public async Task An_expired_lease_passes_to_the_next_client_that_tries()
    {
        var schema = await MigratedAsync();
        var executor = schema.Driver.GetExecutor();
        var a = Lease("a", TimeSpan.FromSeconds(1));
        var b = Lease("b");
        try
        {
            Assert.True(await executor.LeaderAttemptElectAsync(a, Ct));
            Assert.False(await executor.LeaderAttemptElectAsync(b, Ct));

            await Task.Delay(TimeSpan.FromSeconds(1.5), Ct);

            Assert.True(await executor.LeaderAttemptElectAsync(b, Ct));
            Assert.False(await executor.LeaderAttemptReelectAsync(a, Ct)); // a must step down
        }
        finally
        {
            await ReleaseAsync(executor, a, b);
        }
    }

    [Fact]
    public async Task Renewing_keeps_a_lease_past_its_first_expiry()
    {
        var schema = await MigratedAsync();
        var executor = schema.Driver.GetExecutor();
        var a = Lease("a", TimeSpan.FromSeconds(2));
        var b = Lease("b");
        try
        {
            Assert.True(await executor.LeaderAttemptElectAsync(a, Ct));
            await Task.Delay(TimeSpan.FromSeconds(1.2), Ct);
            Assert.True(await executor.LeaderAttemptReelectAsync(a, Ct));
            await Task.Delay(TimeSpan.FromSeconds(1.2), Ct);

            Assert.False(await executor.LeaderAttemptElectAsync(b, Ct));
        }
        finally
        {
            await ReleaseAsync(executor, a, b);
        }
    }

    [Fact]
    public async Task Resigning_announces_it_on_the_leadership_topic_as_River_does()
    {
        var schema = await MigratedAsync();
        var listener = schema.Driver.CreateListener();
        if (listener is null)
        {
            return;
        }

        var executor = schema.Driver.GetExecutor();
        var (a, b) = (Lease("a"), Lease("b"));
        await using var connection = await listener.ConnectAsync(Ct);
        try
        {
            Assert.True(await executor.LeaderAttemptElectAsync(a, Ct));
            Assert.False(await executor.LeaderResignAsync(b.LeaderId, Ct)); // announces nothing
            Assert.True(await executor.LeaderResignAsync(a.LeaderId, Ct));

            KopiNotification notification;
            do
            {
                notification = await connection.WaitForNotificationAsync(Ct).WaitAsync(TimeSpan.FromSeconds(10), Ct);
            }
            while (notification.Topic != NotificationTopic.Leadership);

            using var payload = JsonDocument.Parse(notification.Payload);
            Assert.Equal(a.LeaderId, payload.RootElement.GetProperty("leader_id").GetString());
            Assert.Equal("resigned", payload.RootElement.GetProperty("action").GetString());
        }
        finally
        {
            await ReleaseAsync(executor, a, b);
        }
    }

    private static LeaderElectParams Lease(string name, TimeSpan? ttl = null) => new()
    {
        LeaderId = $"conf-{name}-{Guid.NewGuid():N}",
        Ttl = ttl ?? TimeSpan.FromSeconds(30),
    };

    private static async Task ReleaseAsync(IKopiExecutor executor, params LeaderElectParams[] leases)
    {
        foreach (var lease in leases)
        {
            await executor.LeaderResignAsync(lease.LeaderId, CancellationToken.None);
        }
    }
}
