using System.Collections.Concurrent;
using System.Threading.Channels;
using KopiQueue.Drivers;
using KopiQueue.Execution;
using Microsoft.Extensions.Time.Testing;

namespace KopiQueue.Tests;

/// <summary>
/// The notifier's reconnect, resync and fan-out rules against a scripted listener (plan task 2.2: FR-EXE-03,
/// NFR-REL-04a; spike 0.7 D2, D3).
/// </summary>
/// <remarks>
/// Time is a <see cref="FakeTimeProvider"/>. Since the loop runs on its own thread, tests advance the clock in small
/// steps until the expected state appears, with a real-time limit.
/// </remarks>
public sealed class NotifierTests
{
    private static readonly TimeSpan Step = TimeSpan.FromMilliseconds(10);

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Fact]
    public void Reconnect_delays_double_from_100_ms_up_to_30_s_with_jitter()
    {
        var exact = new Notifier(new FakeListener(), new NotifierOptions { Random = () => 0 });
        var shortest = new Notifier(new FakeListener(), new NotifierOptions { Random = () => 0.999999 });

        Assert.Equal([100, 200, 400, 800, 1600], Enumerable.Range(0, 5).Select(f => exact.ReconnectDelay(f).TotalMilliseconds));
        Assert.Equal(TimeSpan.FromSeconds(30), exact.ReconnectDelay(9));
        Assert.Equal(TimeSpan.FromSeconds(30), exact.ReconnectDelay(1000));
        Assert.InRange(shortest.ReconnectDelay(0).TotalMilliseconds, 80, 80.001);
        Assert.InRange(shortest.ReconnectDelay(9).TotalSeconds, 24, 24.001);
    }

    [Fact]
    public async Task Notifications_reach_the_subscribers_of_their_topic()
    {
        var listener = new FakeListener();
        var connection = listener.Script(new FakeConnection());
        await using var notifier = new Notifier(listener);
        var inserts = Recorder();
        var controls = Recorder();
        using var a = notifier.Subscribe(NotificationTopic.Insert, p => inserts.Writer.TryWrite(p), () => inserts.Writer.TryWrite("resync"));
        using var b = notifier.Subscribe(NotificationTopic.Control, p => controls.Writer.TryWrite(p), () => { });

        await notifier.StartAsync(Ct);
        Assert.Equal("resync", await NextAsync(inserts));
        connection.Send(NotificationTopic.Insert, """{"queue": "email"}""");
        connection.Send(NotificationTopic.Leadership, "{}");
        connection.Send(NotificationTopic.Control, """{"action": "pause"}""");

        Assert.Equal("""{"queue": "email"}""", await NextAsync(inserts));
        Assert.Equal("""{"action": "pause"}""", await NextAsync(controls));
        Assert.False(inserts.Reader.TryRead(out _));
        Assert.True(notifier.IsConnected);
    }

    [Fact]
    public async Task A_lost_connection_is_disposed_reconnected_after_the_backoff_and_resynced()
    {
        var time = new FakeTimeProvider();
        var listener = new FakeListener();
        var first = listener.Script(new FakeConnection());
        var second = listener.Script(new FakeConnection());
        await using var notifier = new Notifier(listener, new NotifierOptions { Random = () => 0 }, time);
        var events = Recorder();
        using var subscription = notifier.Subscribe(NotificationTopic.Insert, p => events.Writer.TryWrite(p), () => events.Writer.TryWrite("resync"));

        await notifier.StartAsync(Ct);
        Assert.Equal("resync", await NextAsync(events));
        first.Fail(new InvalidOperationException("connection reset"));
        await first.Disposed.WaitAsync(Ct);
        await Until(() => !notifier.IsConnected);
        Assert.Equal(1, listener.Connects);

        var elapsed = await AdvanceUntil(time, () => listener.Connects == 2);
        Assert.InRange(elapsed, TimeSpan.FromMilliseconds(100), TimeSpan.FromMilliseconds(100) + Step);
        Assert.Equal("resync", await NextAsync(events));
        second.Send(NotificationTopic.Insert, "after");
        Assert.Equal("after", await NextAsync(events));
        Assert.True(notifier.IsConnected);
    }

    [Fact]
    public async Task Failed_connects_back_off_exponentially_and_a_success_resets_the_backoff()
    {
        var time = new FakeTimeProvider();
        var listener = new FakeListener();
        listener.Script(new TimeoutException("1"));
        listener.Script(new TimeoutException("2"));
        listener.Script(new TimeoutException("3"));
        var connected = listener.Script(new FakeConnection());
        listener.Script(new FakeConnection());
        await using var notifier = new Notifier(listener, new NotifierOptions { Random = () => 0 }, time);

        await notifier.StartAsync(Ct);
        await Until(() => listener.Connects == 1);
        var elapsed = await AdvanceUntil(time, () => notifier.IsConnected);
        Assert.Equal(4, listener.Connects);
        Assert.InRange(elapsed, TimeSpan.FromMilliseconds(700), TimeSpan.FromMilliseconds(700) + (3 * Step));

        connected.Fail(new InvalidOperationException("lost"));
        await Until(() => !notifier.IsConnected);
        elapsed = await AdvanceUntil(time, () => notifier.IsConnected);
        Assert.InRange(elapsed, TimeSpan.FromMilliseconds(100), TimeSpan.FromMilliseconds(100) + Step);
    }

    [Fact]
    public async Task A_connection_whose_dispose_hangs_does_not_delay_the_reconnect()
    {
        var time = new FakeTimeProvider();
        var listener = new FakeListener();
        var stuck = listener.Script(new FakeConnection { HangOnDispose = true });
        listener.Script(new FakeConnection());
        await using var notifier = new Notifier(listener, new NotifierOptions { Random = () => 0 }, time);

        await notifier.StartAsync(Ct);
        await Until(() => notifier.IsConnected);
        stuck.Fail(new TimeoutException("keep-alive"));
        await stuck.Disposed.WaitAsync(Ct);

        var elapsed = await AdvanceUntil(time, () => listener.Connects == 2 && notifier.IsConnected);
        Assert.True(elapsed < TimeSpan.FromSeconds(1), $"Reconnected only after {elapsed}.");
    }

    [Fact]
    public async Task A_failing_subscriber_does_not_stop_the_others_or_the_loop()
    {
        var listener = new FakeListener();
        var connection = listener.Script(new FakeConnection());
        await using var notifier = new Notifier(listener);
        var events = Recorder();
        using var bad = notifier.Subscribe(NotificationTopic.Insert, _ => throw new InvalidOperationException("bad"), () => throw new InvalidOperationException("bad"));
        using var good = notifier.Subscribe(NotificationTopic.Insert, p => events.Writer.TryWrite(p), () => events.Writer.TryWrite("resync"));

        await notifier.StartAsync(Ct);
        connection.Send(NotificationTopic.Insert, "one");
        connection.Send(NotificationTopic.Insert, "two");

        Assert.Equal(["resync", "one", "two"], [await NextAsync(events), await NextAsync(events), await NextAsync(events)]);
        Assert.True(notifier.IsConnected);
    }

    [Fact]
    public async Task A_disposed_subscription_gets_no_more_calls()
    {
        var listener = new FakeListener();
        var connection = listener.Script(new FakeConnection());
        await using var notifier = new Notifier(listener);
        var events = Recorder();
        var dropped = 0;
        var subscription = notifier.Subscribe(NotificationTopic.Insert, _ => dropped++, () => { });
        using var marker = notifier.Subscribe(NotificationTopic.Insert, p => events.Writer.TryWrite(p), () => { });
        subscription.Dispose();
        subscription.Dispose();

        await notifier.StartAsync(Ct);
        connection.Send(NotificationTopic.Insert, "x");

        Assert.Equal("x", await NextAsync(events));
        Assert.Equal(0, dropped);
    }

    [Fact]
    public async Task A_configuration_error_disables_the_notifier_for_good()
    {
        var time = new FakeTimeProvider();
        var listener = new FakeListener();
        listener.Script(new KopiQueueConfigurationException("schema too long"));
        await using var notifier = new Notifier(listener, timeProvider: time);

        await notifier.StartAsync(Ct);
        await Until(() => notifier.IsDisabled);
        time.Advance(TimeSpan.FromMinutes(10));
        await Task.Delay(50, Ct);

        Assert.Equal(1, listener.Connects);
        Assert.False(notifier.IsConnected);
    }

    [Fact]
    public async Task Stop_closes_the_connection_and_ends_the_loop()
    {
        var listener = new FakeListener();
        var connection = listener.Script(new FakeConnection());
        var notifier = new Notifier(listener);
        await notifier.StartAsync(Ct);
        await Until(() => notifier.IsConnected);

        await notifier.StopAsync(Ct);

        Assert.True(connection.Disposed.IsCompleted);
        Assert.False(notifier.IsConnected);
        Assert.Equal(1, listener.Connects);
        await notifier.DisposeAsync();
    }

    [Fact]
    public async Task Stop_abandons_a_connection_that_does_not_close_within_the_timeout()
    {
        var time = new FakeTimeProvider();
        var listener = new FakeListener();
        listener.Script(new FakeConnection { HangOnDispose = true });
        await using var notifier = new Notifier(listener, timeProvider: time);
        await notifier.StartAsync(Ct);
        await Until(() => notifier.IsConnected);

        var stop = notifier.StopAsync(Ct);
        var elapsed = await AdvanceUntil(time, () => stop.IsCompleted);
        await stop;

        Assert.InRange(elapsed, TimeSpan.FromSeconds(5), TimeSpan.FromSeconds(5) + Step);
    }

    [Fact]
    public async Task Stop_while_waiting_to_reconnect_ends_at_once()
    {
        var listener = new FakeListener();
        listener.Script(new TimeoutException("down"));
        await using var notifier = new Notifier(listener, timeProvider: new FakeTimeProvider());
        await notifier.StartAsync(Ct);
        await Until(() => listener.Connects == 1);

        await notifier.StopAsync(Ct).WaitAsync(TimeSpan.FromSeconds(5), Ct);

        Assert.Equal(1, listener.Connects);
    }

    [Fact]
    public async Task Start_twice_is_an_error_and_stop_without_start_does_nothing()
    {
        await using var idle = new Notifier(new FakeListener());
        await idle.StopAsync(Ct);

        await using var notifier = new Notifier(new FakeListener());
        await notifier.StartAsync(Ct);
        await Assert.ThrowsAsync<InvalidOperationException>(() => notifier.StartAsync(Ct));
    }

    [Fact]
    public void LocalWaker_wakes_each_subscribed_queue_once_per_call()
    {
        var waker = new LocalWaker();
        var woken = new List<string>();
        using var email = waker.Subscribe("email", () => woken.Add("email 1"));
        var second = waker.Subscribe("email", () => woken.Add("email 2"));
        using var bad = waker.Subscribe("email", () => throw new InvalidOperationException("bad"));
        using var other = waker.Subscribe("other", () => woken.Add("other"));

        waker.Wake(["email", "email", "missing"]);
        second.Dispose();
        second.Dispose();
        waker.Wake(["email", "other"]);

        Assert.Equal(["email 1", "email 2", "email 1", "other"], woken);
    }

    private static Channel<string> Recorder() => Channel.CreateUnbounded<string>();

    private static async Task<string> NextAsync(Channel<string> events) =>
        await events.Reader.ReadAsync(Ct).AsTask().WaitAsync(TimeSpan.FromSeconds(5), Ct);

    private static async Task Until(Func<bool> condition)
    {
        var limit = DateTime.UtcNow.AddSeconds(5);
        while (!condition())
        {
            Assert.True(DateTime.UtcNow < limit, "The condition was not met within 5 s.");
            await Task.Delay(5, Ct);
        }
    }

    /// <summary>Advances fake time in steps until <paramref name="condition"/> holds; returns the fake time it took.</summary>
    private static async Task<TimeSpan> AdvanceUntil(FakeTimeProvider time, Func<bool> condition)
    {
        var start = time.GetUtcNow();
        var limit = DateTime.UtcNow.AddSeconds(10);
        while (!condition())
        {
            Assert.True(DateTime.UtcNow < limit, $"The condition was not met within 10 s; fake time advanced {time.GetUtcNow() - start}.");

            // Let the loop reach its next timer before moving the clock, so a step is never skipped over.
            await Task.Delay(5, Ct);
            if (!condition())
            {
                time.Advance(Step);
            }
        }

        return time.GetUtcNow() - start;
    }

    private sealed class FakeListener : IKopiListener
    {
        private readonly ConcurrentQueue<object> _script = new();
        private int _connects;

        public int Connects => Volatile.Read(ref _connects);

        public T Script<T>(T outcome)
            where T : class
        {
            _script.Enqueue(outcome);
            return outcome;
        }

        public async Task<IKopiListenerConnection> ConnectAsync(CancellationToken cancellationToken)
        {
            Interlocked.Increment(ref _connects);
            if (!_script.TryDequeue(out var outcome))
            {
                await Task.Delay(Timeout.Infinite, cancellationToken);
            }

            return outcome as IKopiListenerConnection ?? throw (Exception)outcome!;
        }
    }

    private sealed class FakeConnection : IKopiListenerConnection
    {
        private readonly Channel<object> _items = Channel.CreateUnbounded<object>();
        private readonly TaskCompletionSource _disposed = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public bool HangOnDispose { get; init; }

        public Task Disposed => _disposed.Task;

        public void Send(NotificationTopic topic, string payload) => _items.Writer.TryWrite(new KopiNotification(topic, payload));

        public void Fail(Exception exception) => _items.Writer.TryWrite(exception);

        public async Task<KopiNotification> WaitForNotificationAsync(CancellationToken cancellationToken)
        {
            var item = await _items.Reader.ReadAsync(cancellationToken);
            return item as KopiNotification ?? throw (Exception)item;
        }

        public ValueTask DisposeAsync()
        {
            _disposed.TrySetResult();
            return HangOnDispose ? new ValueTask(Task.Delay(Timeout.Infinite)) : ValueTask.CompletedTask;
        }
    }
}
