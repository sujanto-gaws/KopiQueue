using System.Text.Json;
using KopiQueue.Drivers;
using Xunit;

namespace KopiQueue.TestInfrastructure.Conformance;

/// <summary>Listener cases (plan task 2.2: FR-EXE-03, FR-DB-04, FR-INS-07).</summary>
public abstract partial class DriverConformanceTests<TFixture>
{
    [Fact]
    public async Task A_listener_exists_exactly_when_the_driver_can_notify()
    {
        var schema = await MigratedAsync();

        Assert.Equal(schema.Driver.Capabilities.ListenNotify, schema.Driver.CreateListener() is not null);
    }

    [Fact]
    public async Task A_committed_insert_reaches_a_listener_connection_as_an_insert_notification()
    {
        var schema = await MigratedAsync();
        var listener = schema.Driver.CreateListener();
        if (listener is null)
        {
            return;
        }

        var queue = "q" + Guid.NewGuid().ToString("N")[..12];
        await using var connection = await listener.ConnectAsync(Ct);
        await schema.Driver.GetExecutor().JobInsertManyAsync([Job(UniqueKind(), queue)], Ct);

        KopiNotification notification;
        do
        {
            notification = await connection.WaitForNotificationAsync(Ct).WaitAsync(TimeSpan.FromSeconds(10), Ct);
        }
        while (notification.Topic != NotificationTopic.Insert || QueueOf(notification) != queue);

        Assert.Equal(NotificationTopic.Insert, notification.Topic);
    }

    private static string? QueueOf(KopiNotification notification)
    {
        using var json = JsonDocument.Parse(notification.Payload);
        return json.RootElement.TryGetProperty("queue", out var queue) ? queue.GetString() : null;
    }
}
