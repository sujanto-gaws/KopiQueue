using System.Collections.Concurrent;
using KopiQueue.Execution;
using Microsoft.Extensions.Time.Testing;

namespace KopiQueue.Tests;

/// <summary>
/// The maintenance host's batch loop and leadership rules (plan task 3.2: FR-MNT-07; design §5.7): services run only
/// while leading, full batches repeat at once, partial ones wait the interval with jitter, and failures never end a loop.
/// </summary>
/// <remarks>
/// Time is a <see cref="FakeTimeProvider"/>. The loops run on their own threads, so tests advance the clock in small
/// steps until the expected state appears, with a real-time limit.
/// </remarks>
public sealed class MaintenanceHostTests
{
    private static readonly TimeSpan Step = TimeSpan.FromMilliseconds(100);
    private static readonly TimeSpan Interval = TimeSpan.FromSeconds(5);

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Fact]
    public async Task Services_run_only_while_the_client_leads()
    {
        var time = new FakeTimeProvider();
        var service = new FakeService(time, "cleaner");
        await using var host = Host(time, service);

        await Settle(time, Interval * 2);
        Assert.Equal(0, service.Batches);
        Assert.False(host.IsRunning);

        await host.OnLeadershipChangedAsync(true, Ct);
        await AdvanceUntil(time, () => service.Batches == 1);
        Assert.True(host.IsRunning);

        await host.OnLeadershipChangedAsync(false, Ct);
        var batches = service.Batches;
        await Settle(time, Interval * 3);
        Assert.Equal(batches, service.Batches);
        Assert.False(host.IsRunning);
    }

    [Fact]
    public async Task A_batch_that_is_not_full_waits_the_interval()
    {
        var time = new FakeTimeProvider();
        var service = new FakeService(time, "cleaner") { Handles = 3 };
        await using var host = Host(time, service);

        await host.OnLeadershipChangedAsync(true, Ct);
        await AdvanceUntil(time, () => service.Batches == 1);
        var waited = await AdvanceUntil(time, () => service.Batches == 2);

        Assert.InRange(waited, Interval - Step, Interval + Step);
    }

    [Fact]
    public async Task Full_batches_run_again_at_once_until_one_is_not_full()
    {
        var time = new FakeTimeProvider();
        var service = new FakeService(time, "scheduler") { BatchSize = 10 };
        service.Script.Enqueue(10);
        service.Script.Enqueue(10);
        service.Script.Enqueue(10);
        service.Script.Enqueue(4);
        await using var host = Host(time, service);

        var start = time.GetUtcNow();
        await host.OnLeadershipChangedAsync(true, Ct);

        // The backlog drains without waiting, so it needs no fake time at all. Advancing the clock here would make the
        // check depend on how quickly the loop's thread is scheduled.
        await WaitUntil(() => service.Batches == 4);
        Assert.All(service.At.Take(4), at => Assert.Equal(start, at));
        await AdvanceUntil(time, () => service.Batches == 5);
        Assert.InRange(service.At[4] - service.At[3], Interval - Step, Interval + Step);
    }

    [Fact]
    public async Task A_failing_batch_is_retried_after_the_interval_and_other_services_carry_on()
    {
        var time = new FakeTimeProvider();
        var failing = new FakeService(time, "failing") { Fails = true };
        var healthy = new FakeService(time, "healthy");
        await using var host = Host(time, failing, healthy);

        await host.OnLeadershipChangedAsync(true, Ct);
        await AdvanceUntil(time, () => failing.Batches == 3 && healthy.Batches >= 3);

        Assert.InRange(failing.At[2] - failing.At[1], Interval - Step, Interval + Step);
        Assert.True(host.IsRunning);
    }

    [Fact]
    public async Task Losing_leadership_cancels_a_batch_in_progress_and_waits_for_it()
    {
        var time = new FakeTimeProvider();
        var service = new FakeService(time, "slow") { Hangs = true };
        await using var host = Host(time, service);
        await host.OnLeadershipChangedAsync(true, Ct);
        await AdvanceUntil(time, () => service.Batches == 1);

        await host.OnLeadershipChangedAsync(false, Ct);

        Assert.True(service.SawCancellation);
        Assert.False(host.IsRunning);
    }

    [Fact]
    public async Task Leading_again_restarts_the_services()
    {
        var time = new FakeTimeProvider();
        var service = new FakeService(time, "cleaner");
        await using var host = Host(time, service);
        await host.OnLeadershipChangedAsync(true, Ct);
        await AdvanceUntil(time, () => service.Batches == 1);
        await host.OnLeadershipChangedAsync(false, Ct);

        await host.OnLeadershipChangedAsync(true, Ct);
        await host.OnLeadershipChangedAsync(true, Ct); // reported twice: still one loop
        await AdvanceUntil(time, () => service.Batches == 2);
        await Settle(time, Interval - Step - Step);

        Assert.Equal(2, service.Batches);
    }

    [Fact]
    public async Task Waits_vary_within_ten_percent_of_the_interval()
    {
        var time = new FakeTimeProvider();
        var service = new FakeService(time, "cleaner");
        await using var host = new MaintenanceHost([service], new MaintenanceHostOptions { Jitter = 0.1 }, time);

        await host.OnLeadershipChangedAsync(true, Ct);
        await AdvanceUntil(time, () => service.Batches == 8);

        var waits = service.At.Zip(service.At.Skip(1), (a, b) => b - a).ToList();
        Assert.All(waits, w => Assert.InRange(w, (Interval * 0.9) - Step, (Interval * 1.1) + Step));
        Assert.True(waits.Distinct().Count() > 1, "The waits are jittered.");
    }

    [Fact]
    public void A_service_without_a_positive_interval_or_batch_size_is_refused()
    {
        var time = new FakeTimeProvider();

        Assert.Throws<ArgumentOutOfRangeException>(() => new MaintenanceHost([new FakeService(time, "x") { IntervalOverride = TimeSpan.Zero }]));
        Assert.Throws<ArgumentOutOfRangeException>(() => new MaintenanceHost([new FakeService(time, "x") { BatchSize = 0 }]));
    }

    private static MaintenanceHost Host(FakeTimeProvider time, params FakeService[] services) =>
        new(services, new MaintenanceHostOptions { Jitter = 0 }, time);

    private static async Task<TimeSpan> AdvanceUntil(FakeTimeProvider time, Func<bool> condition)
    {
        var start = time.GetUtcNow();
        var limit = DateTime.UtcNow.AddSeconds(20);
        while (!condition())
        {
            Assert.True(DateTime.UtcNow < limit, $"The condition was not met within 20 s; fake time advanced {time.GetUtcNow() - start}.");
            await Task.Delay(5, Ct);
            if (!condition())
            {
                time.Advance(Step);
            }
        }

        return time.GetUtcNow() - start;
    }

    /// <summary>Waits in real time, without advancing the fake clock, until <paramref name="condition"/> holds.</summary>
    private static async Task WaitUntil(Func<bool> condition)
    {
        var limit = DateTime.UtcNow.AddSeconds(20);
        while (!condition())
        {
            Assert.True(DateTime.UtcNow < limit, "The condition was not met within 20 s without advancing fake time.");
            await Task.Delay(5, Ct);
        }
    }

    private static async Task Settle(FakeTimeProvider time, TimeSpan duration)
    {
        for (var passed = TimeSpan.Zero; passed < duration; passed += Step)
        {
            await Task.Delay(5, Ct);
            time.Advance(Step);
        }

        await Task.Delay(20, Ct);
    }

    /// <summary>A scripted service that records when each batch started.</summary>
    internal sealed class FakeService(TimeProvider time, string name) : IMaintenanceService
    {
        private int _batches;

        public string Name { get; } = name;

        public TimeSpan Interval => IntervalOverride ?? MaintenanceHostTests.Interval;

        public TimeSpan? IntervalOverride { get; init; }

        public int BatchSize { get; init; } = 100;

        /// <summary>Rows each batch handles once <see cref="Script"/> is empty.</summary>
        public int Handles { get; init; }

        public ConcurrentQueue<int> Script { get; } = new();

        public bool Fails { get; init; }

        public bool Hangs { get; init; }

        public bool SawCancellation { get; private set; }

        public int Batches => Volatile.Read(ref _batches);

        public List<DateTimeOffset> At { get; } = [];

        public async Task<int> RunBatchAsync(CancellationToken cancellationToken)
        {
            lock (At)
            {
                At.Add(time.GetUtcNow());
            }

            Interlocked.Increment(ref _batches);
            if (Hangs)
            {
                try
                {
                    await Task.Delay(Timeout.Infinite, cancellationToken);
                }
                catch (OperationCanceledException)
                {
                    SawCancellation = true;
                    throw;
                }
            }

            if (Fails)
            {
                throw new InvalidOperationException("The batch failed.");
            }

            return Script.TryDequeue(out var rows) ? rows : Handles;
        }
    }
}
