using System.Globalization;
using System.Text.Json.Nodes;
using KopiQueue.Drivers;
using KopiQueue.Execution;
using KopiQueue.Serialization;
using Microsoft.Extensions.Time.Testing;

namespace KopiQueue.Tests;

/// <summary>
/// Periodic jobs (plan task 4.3: FR-PER-01..04, design §5.7): schedules, the runtime bundle, and the leader's enqueuer
/// against a fake clock and driver.
/// </summary>
public sealed class PeriodicJobTests
{
    private static readonly DateTimeOffset Start = At("2026-11-26T12:34:56Z");

    private static readonly SystemTextJsonArgsSerializer Json = new(WebJsonContext.Default);

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Theory]
    [InlineData("2026-11-26T12:34:56Z", 60, "2026-11-26T12:35:00Z", "2026-11-26T12:34:00Z")]
    [InlineData("2026-11-26T12:35:00Z", 60, "2026-11-26T12:36:00Z", "2026-11-26T12:35:00Z")]
    [InlineData("2026-11-26T12:34:56Z", 900, "2026-11-26T12:45:00Z", "2026-11-26T12:30:00Z")]
    [InlineData("2026-11-26T23:59:59Z", 86400, "2026-11-27T00:00:00Z", "2026-11-26T00:00:00Z")]
    [InlineData("2026-11-26T12:34:56.5Z", 1, "2026-11-26T12:34:57Z", "2026-11-26T12:34:56Z")]
    public void An_interval_ticks_at_multiples_of_itself_since_the_epoch(string now, int seconds, string next, string latest)
    {
        var schedule = PeriodicSchedule.Every(TimeSpan.FromSeconds(seconds));

        Assert.Equal(At(next), schedule.NextAfter(At(now)));
        Assert.Equal(At(latest), schedule.LatestAtOrBefore(At(now)));
    }

    [Fact]
    public void Cron_schedules_take_five_or_six_fields_macros_and_a_time_zone()
    {
        Assert.Equal(At("2026-11-27T02:00:00Z"), PeriodicSchedule.Cron("0 2 * * *").NextAfter(Start));
        Assert.Equal(At("2026-11-26T02:00:00Z"), PeriodicSchedule.Cron("0 2 * * *").LatestAtOrBefore(Start));
        Assert.Equal(At("2026-11-26T12:35:00Z"), PeriodicSchedule.Cron("*/30 * * * * *").NextAfter(Start));
        Assert.Equal(At("2026-11-26T13:00:00Z"), PeriodicSchedule.Cron("@hourly").NextAfter(Start));
        Assert.Equal(At("2026-11-27T00:00:00Z"), PeriodicSchedule.Cron("@daily").NextAfter(Start));

        // 09:30 in Singapore (UTC+8, no daylight saving) is 01:30 UTC.
        var singapore = TimeZoneInfo.FindSystemTimeZoneById("Asia/Singapore");
        Assert.Equal(At("2026-11-27T01:30:00Z"), PeriodicSchedule.Cron("30 9 * * *", singapore).NextAfter(Start));
        Assert.Equal(At("2026-11-27T01:30:00Z"), PeriodicSchedule.Cron("CRON_TZ=Asia/Singapore 30 9 * * *").NextAfter(Start));
    }

    [Fact]
    public void Cron_ticks_follow_daylight_saving_time_in_their_zone()
    {
        // Berlin leaves summer time on 2026-10-25: 02:00 local is 00:00 UTC before the change and 01:00 UTC after.
        var berlin = PeriodicSchedule.Cron("CRON_TZ=Europe/Berlin 0 2 * * *");

        Assert.Equal(At("2026-10-24T00:00:00Z"), berlin.NextAfter(At("2026-10-23T12:00:00Z")));
        Assert.Equal(At("2026-10-26T01:00:00Z"), berlin.NextAfter(At("2026-10-25T12:00:00Z")));
    }

    [Fact]
    public void Invalid_schedules_are_rejected()
    {
        Assert.Throws<ArgumentOutOfRangeException>(() => PeriodicSchedule.Every(TimeSpan.FromMilliseconds(999)));
        Assert.Throws<FormatException>(() => PeriodicSchedule.Cron("0 2 * *"));
        Assert.Throws<FormatException>(() => PeriodicSchedule.Cron("0 0 2 * * * *"));
        Assert.Throws<FormatException>(() => PeriodicSchedule.Cron("99 2 * * *"));
        Assert.Throws<FormatException>(() => PeriodicSchedule.Cron("CRON_TZ=UTC"));
        Assert.Throws<ArgumentException>(() => PeriodicSchedule.Cron("CRON_TZ=UTC 0 2 * * *", TimeZoneInfo.Utc));
        Assert.Throws<TimeZoneNotFoundException>(() => PeriodicSchedule.Cron("CRON_TZ=Nowhere/Atlantis 0 2 * * *"));
    }

    [Fact]
    public void The_bundle_adds_and_removes_jobs_and_rejects_duplicate_ids()
    {
        var bundle = new KopiClient(new KopiClientTests.RecordingDriver()).PeriodicJobs;
        var a = bundle.Add(Job(Every(60)));
        var named = bundle.AddMany([new PeriodicJob(Every(60), () => null) { Id = "nightly" }, new PeriodicJob(Every(60), () => null)]);

        Assert.Equal(3, bundle.Count);
        Assert.Throws<ArgumentException>(() => bundle.AddMany([new PeriodicJob(Every(60), () => null), new PeriodicJob(Every(60), () => null) { Id = "nightly" }]));
        Assert.Equal(3, bundle.Count); // all or none
        Assert.True(bundle.RemoveById("nightly"));
        Assert.False(bundle.RemoveById("nightly"));
        Assert.True(bundle.Remove(a));
        Assert.False(bundle.Remove(a));
        Assert.Equal([named[1]], bundle.Snapshot());
        bundle.Clear();
        Assert.Equal(0, bundle.Count);
        Assert.Throws<ArgumentException>(() => new KopiClient(
            new KopiClientTests.RecordingDriver(),
            new KopiClientOptions { PeriodicJobs = [new PeriodicJob(Every(60), () => null) { Id = "x" }, new PeriodicJob(Every(60), () => null) { Id = "x" }] }));
    }

    [Fact]
    public async Task Each_tick_is_inserted_once_with_its_own_key_and_periodic_metadata()
    {
        var (driver, enqueuer, bundle, time) = Create();
        bundle.Add(Job(Every(60)));
        using var leading = new CancellationTokenSource();

        Assert.Equal(0, await enqueuer.RunBatchAsync(leading.Token)); // 12:34:56: the 12:34 tick is 56 s old, past the 20 s window
        time.SetUtcNow(At("2026-11-26T12:35:00Z"));
        Assert.Equal(1, await enqueuer.RunBatchAsync(leading.Token));
        Assert.Equal(0, await enqueuer.RunBatchAsync(leading.Token)); // the same tick again: nothing
        time.SetUtcNow(At("2026-11-26T12:36:00.2Z"));
        Assert.Equal(1, await enqueuer.RunBatchAsync(leading.Token));

        Assert.Equal(2, driver.Inserts.Count);
        var first = driver.Inserts[0];
        Assert.Equal(("rebuild_index", "default", JobState.Available), (first.Kind, first.Queue, first.State));
        Assert.True(JsonNode.Parse(first.MetadataJson)!["periodic"]!.GetValue<bool>());
        Assert.Equal(PeriodicEnqueuer.TickKey(first, At("2026-11-26T12:35:00Z")), first.UniqueKey);
        Assert.Equal(PeriodicEnqueuer.TickKey(first, At("2026-11-26T12:36:00Z")), driver.Inserts[1].UniqueKey);
        Assert.Equal((byte)0b1111_1111, first.UniqueStates);
    }

    [Fact]
    public async Task A_new_leader_inserts_a_recent_tick_the_previous_leader_may_have_missed()
    {
        var (driver, enqueuer, bundle, time) = Create();
        bundle.Add(Job(Every(60)));
        time.SetUtcNow(At("2026-11-26T12:35:15Z")); // 15 s after the 12:35 tick, within the 20 s window
        using var leading = new CancellationTokenSource();

        Assert.Equal(1, await enqueuer.RunBatchAsync(leading.Token));

        Assert.Equal(PeriodicEnqueuer.TickKey(driver.Inserts[0], At("2026-11-26T12:35:00Z")), driver.Inserts[0].UniqueKey);
    }

    [Fact]
    public async Task An_enqueuer_far_behind_inserts_one_tick_and_continues_from_now()
    {
        var (driver, enqueuer, bundle, time) = Create();
        bundle.Add(Job(Every(60)));
        using var leading = new CancellationTokenSource();
        await enqueuer.RunBatchAsync(leading.Token);

        time.SetUtcNow(At("2026-11-26T12:40:30Z")); // five ticks late
        Assert.Equal(1, await enqueuer.RunBatchAsync(leading.Token));
        time.SetUtcNow(At("2026-11-26T12:40:59Z"));
        Assert.Equal(0, await enqueuer.RunBatchAsync(leading.Token));
        time.SetUtcNow(At("2026-11-26T12:41:00Z"));
        Assert.Equal(1, await enqueuer.RunBatchAsync(leading.Token));

        Assert.Equal(PeriodicEnqueuer.TickKey(driver.Inserts[0], At("2026-11-26T12:35:00Z")), driver.Inserts[0].UniqueKey);
        Assert.Equal(PeriodicEnqueuer.TickKey(driver.Inserts[1], At("2026-11-26T12:41:00Z")), driver.Inserts[1].UniqueKey);
    }

    [Fact]
    public async Task Run_on_start_inserts_once_per_leadership_and_on_a_runtime_add_without_a_tick_key()
    {
        var (driver, enqueuer, bundle, _) = Create();
        bundle.Add(Job(PeriodicSchedule.Cron("0 2 * * *"), runOnStart: true));
        using var first = new CancellationTokenSource();
        using var second = new CancellationTokenSource();

        Assert.Equal(1, await enqueuer.RunBatchAsync(first.Token));
        Assert.Equal(0, await enqueuer.RunBatchAsync(first.Token));
        bundle.Add(Job(PeriodicSchedule.Cron("0 3 * * *"), "added", runOnStart: true));
        Assert.Equal(1, await enqueuer.RunBatchAsync(first.Token));
        Assert.Equal(2, await enqueuer.RunBatchAsync(second.Token)); // a new leadership: both again

        Assert.Equal(4, driver.Inserts.Count);
        Assert.All(driver.Inserts, i => Assert.Null(i.UniqueKey));
        Assert.All(driver.Inserts, i => Assert.True(JsonNode.Parse(i.MetadataJson)!["periodic"]!.GetValue<bool>()));
    }

    [Fact]
    public async Task A_removed_job_stops_and_skipped_or_failing_constructors_do_not_hold_up_the_others()
    {
        var (driver, enqueuer, bundle, time) = Create();
        var removed = bundle.Add(Job(Every(60), "removed"));
        bundle.Add(new PeriodicJob(Every(60), () => null) { Id = "skips" });
        bundle.Add(new PeriodicJob(Every(60), () => throw new InvalidOperationException("boom")) { Id = "throws" });
        bundle.Add(new PeriodicJob(Every(60), () => new InsertRequest(new RebuildIndexArgs("bad"), new InsertOptions { Priority = 9 })));
        bundle.Add(Job(Every(60), "kept"));
        using var leading = new CancellationTokenSource();
        await enqueuer.RunBatchAsync(leading.Token);

        time.SetUtcNow(At("2026-11-26T12:35:00Z"));
        Assert.Equal(2, await enqueuer.RunBatchAsync(leading.Token));
        bundle.Remove(removed);
        time.SetUtcNow(At("2026-11-26T12:36:00Z"));
        Assert.Equal(1, await enqueuer.RunBatchAsync(leading.Token));

        Assert.Equal(["removed", "kept", "kept"], driver.Inserts.Select(i => JsonNode.Parse(i.ArgsJson)!["index"]!.GetValue<string>()));
    }

    [Fact]
    public async Task A_failed_insert_keeps_its_ticks_for_the_next_try()
    {
        var (driver, enqueuer, bundle, time) = Create();
        bundle.Add(Job(Every(60), runOnStart: true));
        driver.FailWith = new InvalidOperationException("database down");
        using var leading = new CancellationTokenSource();

        await Assert.ThrowsAsync<InvalidOperationException>(() => enqueuer.RunBatchAsync(leading.Token));
        time.SetUtcNow(At("2026-11-26T12:35:00Z"));
        await Assert.ThrowsAsync<InvalidOperationException>(() => enqueuer.RunBatchAsync(leading.Token));
        driver.FailWith = null;
        time.SetUtcNow(At("2026-11-26T12:35:05Z"));
        Assert.Equal(2, await enqueuer.RunBatchAsync(leading.Token));

        Assert.Null(driver.Inserts[0].UniqueKey); // the start insert
        Assert.Equal(PeriodicEnqueuer.TickKey(driver.Inserts[1], At("2026-11-26T12:35:00Z")), driver.Inserts[1].UniqueKey);
    }

    [Fact]
    public async Task Uniqueness_in_the_constructors_options_replaces_the_tick_key()
    {
        var (driver, enqueuer, bundle, time) = Create();
        bundle.Add(PeriodicJob.Create(Every(60), () => new RebuildIndexArgs("u"), new InsertOptions { Unique = new() { ByArgs = true } }));
        using var leading = new CancellationTokenSource();
        await enqueuer.RunBatchAsync(leading.Token);
        time.SetUtcNow(At("2026-11-26T12:35:00Z"));

        await enqueuer.RunBatchAsync(leading.Token);

        var insert = Assert.Single(driver.Inserts);
        Assert.Equal(UniqueKey.Compute("rebuild_index", insert.ArgsJson, "default", time.GetUtcNow(), new UniqueOptions { ByArgs = true }), insert.UniqueKey);
        Assert.Equal(UniqueStates.Default, insert.UniqueStates);
    }

    [Fact]
    public async Task The_enqueuer_waits_until_the_next_tick_or_a_change()
    {
        var (_, enqueuer, bundle, time) = Create();
        bundle.Add(Job(Every(60)));
        using var leading = new CancellationTokenSource();
        await enqueuer.RunBatchAsync(leading.Token);

        var untilTick = enqueuer.WaitAsync(TimeSpan.FromSeconds(5), time, Ct);
        time.Advance(TimeSpan.FromSeconds(3.9));
        Assert.False(untilTick.IsCompleted); // the host's 5 s is not used: the tick is 4 s away
        time.Advance(TimeSpan.FromSeconds(0.1));
        await untilTick.WaitAsync(TimeSpan.FromSeconds(5), Ct);

        await enqueuer.RunBatchAsync(leading.Token);
        var untilChange = enqueuer.WaitAsync(TimeSpan.FromSeconds(5), time, Ct);
        time.Advance(TimeSpan.FromSeconds(30));
        Assert.False(untilChange.IsCompleted);
        bundle.Add(Job(Every(3600), "new"));
        await untilChange.WaitAsync(TimeSpan.FromSeconds(5), Ct);
    }

    private static DateTimeOffset At(string text) => DateTimeOffset.Parse(text, CultureInfo.InvariantCulture, DateTimeStyles.AdjustToUniversal);

    private static PeriodicSchedule Every(int seconds) => PeriodicSchedule.Every(TimeSpan.FromSeconds(seconds));

    private static PeriodicJob Job(PeriodicSchedule schedule, string index = "search", bool runOnStart = false) =>
        new(schedule, () => new InsertRequest(new RebuildIndexArgs(index))) { RunOnStart = runOnStart };

    private static (KopiClientTests.RecordingDriver Driver, PeriodicEnqueuer Enqueuer, PeriodicJobBundle Bundle, FakeTimeProvider Time) Create()
    {
        var driver = new KopiClientTests.RecordingDriver();
        var time = new FakeTimeProvider(Start);
        var client = new KopiClient(driver, new KopiClientOptions { ArgsSerializer = Json, TimeProvider = time });
        var enqueuer = new PeriodicEnqueuer(
            driver, client.PeriodicJobs, client.Prepare, new PeriodicEnqueuerOptions { CatchUpWindow = TimeSpan.FromSeconds(20) }, timeProvider: time);
        return (driver, enqueuer, client.PeriodicJobs, time);
    }
}
