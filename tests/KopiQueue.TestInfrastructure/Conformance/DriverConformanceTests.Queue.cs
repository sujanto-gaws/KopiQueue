using KopiQueue.Drivers;
using Xunit;

namespace KopiQueue.TestInfrastructure.Conformance;

/// <summary>Queue cases (plan tasks 3.5 and 3.6: FR-QUE-02, FR-QUE-03, FR-ADM-04, FR-MNT-05).</summary>
/// <remarks>
/// The schema is shared by every case of a class, so each case names queues of its own and lists only those. Pausing
/// <c>"*"</c> touches other cases' queues too, which is harmless: the driver's fetch ignores pauses, which clients apply.
/// The cases commit, since the times follow the database's clock.
/// </remarks>
public abstract partial class DriverConformanceTests<TFixture>
{
    [Fact]
    public async Task Queues_are_created_once_and_marked_worked_on_every_refresh()
    {
        var executor = (await MigratedAsync()).Driver.GetExecutor();
        var (a, b) = (UniqueQueue(), UniqueQueue());

        var created = await executor.QueueCreateOrSetUpdatedAtManyAsync([a, b], Ct);
        Assert.Equal(new[] { a, b }.Order(StringComparer.Ordinal), created.Select(q => q.Name).Order(StringComparer.Ordinal));
        Assert.All(created, q =>
        {
            Assert.Null(q.PausedAt);
            Assert.Equal("{}", q.MetadataJson.Replace(" ", "", StringComparison.Ordinal));
            Assert.Equal(q.CreatedAt, q.UpdatedAt);
        });

        await Task.Delay(TimeSpan.FromMilliseconds(50), Ct);
        var refreshed = (await executor.QueueCreateOrSetUpdatedAtManyAsync([a], Ct)).Single();
        var first = created.Single(q => q.Name == a);
        Assert.Equal(first.CreatedAt, refreshed.CreatedAt);
        Assert.True(refreshed.UpdatedAt > first.UpdatedAt);
        Assert.Equal(refreshed.UpdatedAt, (await executor.QueueGetAsync(a, Ct))!.UpdatedAt);
    }

    [Fact]
    public async Task Queues_are_read_one_at_a_time_or_listed_a_page_at_a_time_in_name_order()
    {
        var executor = (await MigratedAsync()).Driver.GetExecutor();
        var prefix = UniqueQueue();
        string[] names = [prefix + "_a", prefix + "_b", prefix + "_c"];
        await executor.QueueCreateOrSetUpdatedAtManyAsync(names, Ct);

        Assert.Equal(names[1], (await executor.QueueGetAsync(names[1], Ct))?.Name);
        Assert.Null(await executor.QueueGetAsync(prefix + "_missing", Ct));

        var mine = new[] { names[2], names[0], names[1], prefix + "_missing" };
        Assert.Equal(names, (await executor.QueueListAsync(new QueueListQuery { Max = 10, Names = mine }, Ct)).Select(q => q.Name));
        var page = await executor.QueueListAsync(new QueueListQuery { Max = 2, Names = mine }, Ct);
        Assert.Equal(names[..2], page.Select(q => q.Name));
        var next = await executor.QueueListAsync(new QueueListQuery { Max = 2, Names = mine, After = page[^1].Name }, Ct);
        Assert.Equal(names[2..], next.Select(q => q.Name));
        Assert.NotEmpty(await executor.QueueListAsync(new QueueListQuery { Max = 1 }, Ct));
    }

    [Fact]
    public async Task Pausing_and_resuming_set_paused_at_once_and_report_unknown_queues()
    {
        var executor = (await MigratedAsync()).Driver.GetExecutor();
        var (a, b) = (UniqueQueue(), UniqueQueue());
        await executor.QueueCreateOrSetUpdatedAtManyAsync([a, b], Ct);
        try
        {
            Assert.True(await executor.QueuePauseAsync(a, Ct));
            var paused = (await executor.QueueGetAsync(a, Ct))!;
            Assert.NotNull(paused.PausedAt);
            Assert.Equal(paused.PausedAt, paused.UpdatedAt);
            Assert.Null((await executor.QueueGetAsync(b, Ct))!.PausedAt);

            await Task.Delay(TimeSpan.FromMilliseconds(50), Ct);
            Assert.True(await executor.QueuePauseAsync(a, Ct)); // already paused: nothing changes
            var again = (await executor.QueueGetAsync(a, Ct))!;
            Assert.Equal(paused.PausedAt, again.PausedAt);
            Assert.Equal(paused.UpdatedAt, again.UpdatedAt);

            Assert.False(await executor.QueuePauseAsync(UniqueQueue(), Ct));
            Assert.False(await executor.QueueResumeAsync(UniqueQueue(), Ct));

            Assert.True(await executor.QueuePauseAsync("*", Ct));
            Assert.NotNull((await executor.QueueGetAsync(b, Ct))!.PausedAt);
            Assert.Equal(paused.PausedAt, (await executor.QueueGetAsync(a, Ct))!.PausedAt);

            Assert.True(await executor.QueueResumeAsync(a, Ct));
            var resumed = (await executor.QueueGetAsync(a, Ct))!;
            Assert.Null(resumed.PausedAt);
            Assert.True(resumed.UpdatedAt > paused.UpdatedAt);
            Assert.NotNull((await executor.QueueGetAsync(b, Ct))!.PausedAt);

            Assert.True(await executor.QueueResumeAsync("*", Ct));
            Assert.Null((await executor.QueueGetAsync(b, Ct))!.PausedAt);
        }
        finally
        {
            await executor.QueueResumeAsync("*", Ct);
        }
    }

    [Fact]
    public async Task Updating_a_queue_replaces_its_metadata()
    {
        var executor = (await MigratedAsync()).Driver.GetExecutor();
        var a = UniqueQueue();
        var created = (await executor.QueueCreateOrSetUpdatedAtManyAsync([a], Ct)).Single();
        await Task.Delay(TimeSpan.FromMilliseconds(50), Ct);

        var updated = await executor.QueueUpdateAsync(new QueueUpdateParams { Name = a, MetadataJson = """{"team":"billing","n":1}""" }, Ct);
        Assert.NotNull(updated);
        Assert.Equal(created.CreatedAt, updated.CreatedAt);
        Assert.True(updated.UpdatedAt > created.UpdatedAt);
        using (var json = System.Text.Json.JsonDocument.Parse(updated.MetadataJson))
        {
            Assert.Equal("billing", json.RootElement.GetProperty("team").GetString());
            Assert.Equal(1, json.RootElement.GetProperty("n").GetInt32());
        }

        var replaced = await executor.QueueUpdateAsync(new QueueUpdateParams { Name = a, MetadataJson = "{}" }, Ct);
        Assert.Equal("{}", replaced!.MetadataJson.Replace(" ", "", StringComparison.Ordinal));
        Assert.Null(await executor.QueueUpdateAsync(new QueueUpdateParams { Name = UniqueQueue(), MetadataJson = "{}" }, Ct));
    }

    [Fact]
    public async Task Queues_not_marked_worked_within_the_retention_are_deleted()
    {
        var executor = (await MigratedAsync()).Driver.GetExecutor();
        var (stale, fresh) = (UniqueQueue(), UniqueQueue());
        await executor.QueueCreateOrSetUpdatedAtManyAsync([stale, fresh], Ct);
        await Task.Delay(TimeSpan.FromSeconds(3.5), Ct);
        await executor.QueueCreateOrSetUpdatedAtManyAsync([fresh], Ct);

        // Other cases' queues may go too; this case looks only at its own. A stall here only ages the stale queue more;
        // the fresh one stays younger than the retention unless the runner stalls for 3 s between these two calls.
        var deleted = await executor.QueueDeleteExpiredAsync(new QueueDeleteExpiredParams { Retention = TimeSpan.FromSeconds(3), Max = 1_000 }, Ct);

        Assert.Contains(stale, deleted);
        Assert.DoesNotContain(fresh, deleted);
        Assert.Null(await executor.QueueGetAsync(stale, Ct));
        Assert.NotNull(await executor.QueueGetAsync(fresh, Ct));
    }
}
