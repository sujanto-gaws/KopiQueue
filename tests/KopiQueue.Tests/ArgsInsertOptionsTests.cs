using System.Text.Json.Nodes;
using KopiQueue.Execution;
using KopiQueue.Serialization;
using Microsoft.Extensions.Time.Testing;
using static KopiQueue.Tests.KopiClientTests;

namespace KopiQueue.Tests;

/// <summary>Default insert options from the args type (plan task 4.5: FR-DEF-05).</summary>
public sealed class ArgsInsertOptionsTests
{
    private static readonly DateTimeOffset Now = new(2026, 11, 27, 9, 0, 0, TimeSpan.Zero);

    private static readonly SystemTextJsonArgsSerializer Json = new(WebJsonContext.Default);

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Fact]
    public async Task An_insert_without_options_takes_every_option_from_the_args_and_only_the_args_are_serialised()
    {
        var driver = new RecordingDriver();

        await Client(driver).InsertAsync(new TenantReportArgs("acme"), cancellationToken: Ct);

        var insert = Assert.Single(driver.Inserts);
        Assert.Equal(("reports-acme", 3, 5), (insert.Queue, insert.Priority, insert.MaxAttempts));
        Assert.Equal(["report"], insert.Tags);
        Assert.Equal("""{"tenant":"acme"}""", insert.MetadataJson);
        Assert.NotNull(insert.UniqueKey);
        Assert.Equal("""{"tenant":"acme"}""", insert.ArgsJson);
    }

    [Fact]
    public async Task Each_option_the_insert_sets_wins_and_the_rest_come_from_the_args()
    {
        var driver = new RecordingDriver();
        var later = Now.AddHours(1);

        await Client(driver).InsertAsync(
            new TenantReportArgs("acme"),
            new InsertOptions { Queue = "urgent", ScheduledAt = later, Tags = ["manual"], Metadata = new JsonObject { ["by"] = "ops" } },
            Ct);

        var insert = Assert.Single(driver.Inserts);
        Assert.Equal(("urgent", 3, 5, JobState.Scheduled, (DateTimeOffset?)later), (insert.Queue, insert.Priority, insert.MaxAttempts, insert.State, insert.ScheduledAt));
        Assert.Equal(["manual"], insert.Tags); // replaced whole, not merged
        Assert.Equal("""{"by":"ops"}""", insert.MetadataJson);
        Assert.NotNull(insert.UniqueKey);
    }

    [Fact]
    public async Task Args_without_defaults_take_the_clients()
    {
        var driver = new RecordingDriver();

        await Client(driver).InsertAsync(new NoDefaultsArgs(1), new InsertOptions { Priority = 2 }, Ct);

        var insert = Assert.Single(driver.Inserts);
        Assert.Equal((InsertDefaults.Queue, 2, InsertDefaults.MaxAttempts), (insert.Queue, insert.Priority, insert.MaxAttempts));
        Assert.Null(insert.UniqueKey);
    }

    [Fact]
    public async Task A_bulk_insert_gives_each_job_its_own_types_defaults()
    {
        var driver = new RecordingDriver();

        await Client(driver).InsertManyAsync(
            [new(new TenantReportArgs("a")), new(new RebuildIndexArgs("search")), new(new TenantReportArgs("b"), new InsertOptions { Priority = 1 })],
            cancellationToken: Ct);

        Assert.Equal(["reports-a", "default", "reports-b"], driver.Inserts.Select(i => i.Queue));
        Assert.Equal([3, 1, 1], driver.Inserts.Select(i => i.Priority));
    }

    [Fact]
    public async Task An_invalid_default_fails_the_insert_before_any_sql()
    {
        var driver = new RecordingDriver();
        var client = Client(driver);

        var single = await Assert.ThrowsAsync<ArgumentException>(() => client.InsertAsync(new BadDefaultsArgs(1), cancellationToken: Ct));
        var many = await Assert.ThrowsAsync<ArgumentException>(() => client.InsertManyAsync([new(new NoDefaultsArgs(1)), new(new BadDefaultsArgs(2))], cancellationToken: Ct));
        await client.InsertAsync(new BadDefaultsArgs(3), new InsertOptions { Priority = 2 }, Ct); // the insert's own option wins

        Assert.Contains("Priority", single.Message, StringComparison.Ordinal);
        Assert.StartsWith("Job 1: ", many.Message, StringComparison.Ordinal);
        Assert.Equal(2, Assert.Single(driver.Inserts).Priority);
    }

    [Fact]
    public async Task A_unique_default_is_refused_by_the_fast_insert_as_a_unique_option_is()
    {
        var driver = new RecordingDriver();

        await Assert.ThrowsAsync<NotSupportedException>(() => Client(driver).InsertManyFastAsync([new(new TenantReportArgs("acme"))], cancellationToken: Ct));

        Assert.Equal(0, driver.Calls);
    }

    [Fact]
    public async Task A_periodic_job_takes_the_args_defaults_and_their_uniqueness_replaces_the_tick_key()
    {
        var driver = new RecordingDriver();
        var time = new FakeTimeProvider(Now);
        var client = new KopiClient(driver, new KopiClientOptions { ArgsSerializer = Json, TimeProvider = time });
        client.PeriodicJobs.Add(PeriodicJob.Create(PeriodicSchedule.Every(TimeSpan.FromMinutes(1)), () => new TenantReportArgs("acme")));
        var enqueuer = new PeriodicEnqueuer(
            driver, client.PeriodicJobs, client.Prepare, new PeriodicEnqueuerOptions { CatchUpWindow = TimeSpan.FromSeconds(20) }, timeProvider: time);
        using var leading = new CancellationTokenSource();

        Assert.Equal(1, await enqueuer.RunBatchAsync(leading.Token)); // 09:00:00 is a tick, caught up at once

        var insert = Assert.Single(driver.Inserts);
        Assert.Equal(("reports-acme", 3), (insert.Queue, insert.Priority));
        var direct = client.Prepare(new InsertRequest(new TenantReportArgs("acme")), Now);
        Assert.Equal(direct.UniqueKey, insert.UniqueKey); // the args' own key, not the tick's
    }

    private static KopiClient Client(RecordingDriver driver) =>
        new(driver, new KopiClientOptions { TimeProvider = new FakeTimeProvider(Now), ArgsSerializer = Json });
}
