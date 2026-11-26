using KopiQueue.Execution;
using Microsoft.Extensions.Time.Testing;

namespace KopiQueue.Tests;

/// <summary>The producer's coalescing wake-up (plan task 2.3).</summary>
public sealed class WakeSignalTests
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Fact]
    public async Task A_set_before_the_wait_releases_it_at_once_and_only_once()
    {
        var (signal, time) = (new WakeSignal(), new FakeTimeProvider());
        signal.Set();
        signal.Set();

        Assert.True(await signal.WaitAsync(TimeSpan.FromSeconds(1), time, Ct));
        var second = signal.WaitAsync(TimeSpan.FromSeconds(1), time, Ct);
        time.Advance(TimeSpan.FromSeconds(1));
        Assert.False(await second);
    }

    [Fact]
    public async Task A_burst_during_a_wait_releases_it_once()
    {
        var (signal, time) = (new WakeSignal(), new FakeTimeProvider());
        var wait = signal.WaitAsync(Timeout.InfiniteTimeSpan, time, Ct);

        for (var i = 0; i < 50; i++)
        {
            signal.Set();
        }

        Assert.True(await wait);
        var next = signal.WaitAsync(TimeSpan.FromSeconds(1), time, Ct);
        time.Advance(TimeSpan.FromSeconds(1));
        Assert.False(await next);
    }

    [Fact]
    public async Task Clear_forgets_a_pending_set()
    {
        var (signal, time) = (new WakeSignal(), new FakeTimeProvider());
        signal.Set();
        signal.Clear();

        var wait = signal.WaitAsync(TimeSpan.FromSeconds(1), time, Ct);
        time.Advance(TimeSpan.FromSeconds(1));
        Assert.False(await wait);
    }

    [Fact]
    public async Task Cancellation_throws_and_a_later_set_still_reaches_the_next_wait()
    {
        var (signal, time) = (new WakeSignal(), new FakeTimeProvider());
        using var cts = new CancellationTokenSource();
        var wait = signal.WaitAsync(Timeout.InfiniteTimeSpan, time, cts.Token);
        await cts.CancelAsync();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => wait);
        signal.Set();
        Assert.True(await signal.WaitAsync(TimeSpan.FromSeconds(1), time, Ct));
    }

    [Fact]
    public async Task A_set_racing_the_timeout_is_never_lost()
    {
        var time = new FakeTimeProvider();
        for (var i = 0; i < 2_000; i++)
        {
            var signal = new WakeSignal();
            var wait = signal.WaitAsync(TimeSpan.FromMilliseconds(1), time, Ct);
            var set = Task.Run(signal.Set, Ct);
            time.Advance(TimeSpan.FromMilliseconds(1));
            await set;

            // Either the wait saw the set, or the next one does.
            Assert.True(await wait || await signal.WaitAsync(TimeSpan.Zero, time, Ct), $"Iteration {i} lost a set.");
        }
    }
}
