using KopiQueue.Drivers;
using static KopiQueue.Tests.KopiClientTests;

namespace KopiQueue.Tests;

/// <summary>The client side of the admin API (plan task 4.4: FR-ADM-02, FR-ADM-03): checks, pages and cursors.</summary>
public sealed class JobAdminTests
{
    private static readonly DateTimeOffset Base = new(2026, 11, 27, 8, 0, 0, TimeSpan.Zero);

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Fact]
    public async Task A_default_list_asks_for_one_more_than_100_jobs_by_id_from_the_start()
    {
        var driver = new RecordingDriver();

        var result = await new KopiClient(driver).JobListAsync(cancellationToken: Ct);

        var query = Assert.Single(driver.Lists);
        Assert.Equal((101, JobListOrderBy.Id, false, (JobListPosition?)null), (query.Max, query.OrderBy, query.Descending, query.After));
        Assert.Empty(result.Jobs);
        Assert.Null(result.NextCursor);
    }

    [Fact]
    public async Task A_full_page_gives_a_cursor_that_starts_the_next_page_after_its_last_job()
    {
        var driver = new RecordingDriver { ListResult = [Row(1, finalizedAt: Base), Row(2, finalizedAt: Base.AddTicks(7)), Row(3)] };
        var client = new KopiClient(driver);
        var parameters = new JobListParams { Kinds = ["k"], Limit = 2, OrderBy = JobListOrderBy.FinalizedAt, Descending = true };

        var first = await client.JobListAsync(parameters, Ct);
        await client.JobListAsync(new JobListParams { Kinds = ["k"], Limit = 2, OrderBy = JobListOrderBy.FinalizedAt, Descending = true, After = first.NextCursor }, Ct);

        Assert.Equal([1L, 2L], first.Jobs.Select(j => j.Id));
        Assert.NotNull(first.NextCursor);
        Assert.Equal(new JobListPosition(2, Base.AddTicks(7)), driver.Lists[1].After);
        Assert.Same(parameters, driver.Lists[0].Filter);
    }

    [Fact]
    public async Task A_cursor_keeps_a_missing_time_and_a_page_that_is_not_full_has_no_cursor()
    {
        var driver = new RecordingDriver { ListResult = [Row(5), Row(9)] };
        var client = new KopiClient(driver);

        var first = await client.JobListAsync(new JobListParams { Limit = 1, OrderBy = JobListOrderBy.AttemptedAt }, Ct);
        await client.JobListAsync(new JobListParams { Limit = 1, OrderBy = JobListOrderBy.AttemptedAt, After = first.NextCursor }, Ct);
        var whole = await client.JobListAsync(new JobListParams { Limit = 2 }, Ct);

        Assert.Equal(new JobListPosition(5, null), driver.Lists[1].After);
        Assert.Equal(2, whole.Jobs.Count);
        Assert.Null(whole.NextCursor);
    }

    [Theory]
    [InlineData(JobListOrderBy.Id, true)]
    [InlineData(JobListOrderBy.CreatedAt, false)]
    public async Task A_cursor_for_another_order_or_direction_is_refused(JobListOrderBy orderBy, bool descending)
    {
        var driver = new RecordingDriver { ListResult = [Row(1), Row(2)] };
        var client = new KopiClient(driver);
        var cursor = (await client.JobListAsync(new JobListParams { Limit = 1 }, Ct)).NextCursor;

        var e = await Assert.ThrowsAsync<ArgumentException>(() => client.JobListAsync(new JobListParams { OrderBy = orderBy, Descending = descending, After = cursor }, Ct));

        Assert.Contains("another OrderBy or Descending", e.Message, StringComparison.Ordinal);
        Assert.Single(driver.Lists);
    }

    [Theory]
    [InlineData("")]
    [InlineData("abc")]
    [InlineData("1.")]
    [InlineData("1.!!!")]
    [InlineData("1.MDph")] // "0:a" has too few parts
    [InlineData("2.MDphOi06MQ")] // another version
    public async Task A_string_that_is_not_a_cursor_is_refused(string cursor)
    {
        var driver = new RecordingDriver();

        await Assert.ThrowsAsync<ArgumentException>(() => new KopiClient(driver).JobListAsync(new JobListParams { After = cursor }, Ct));

        Assert.Empty(driver.Lists);
    }

    public static TheoryData<JobListParams> BadLists => new()
    {
        new JobListParams { Limit = 0 },
        new JobListParams { Limit = 10_001 },
        new JobListParams { OrderBy = (JobListOrderBy)99 },
        new JobListParams { Kinds = ["k", ""] },
        new JobListParams { Queues = [""] },
        new JobListParams { States = [(JobState)42] },
        new JobListParams { Priorities = [1, 5] },
        new JobListParams { Priorities = [0] },
    };

    [Theory]
    [MemberData(nameof(BadLists))]
    public async Task A_list_with_a_bad_parameter_is_refused_before_any_sql(JobListParams parameters)
    {
        var driver = new RecordingDriver();

        await Assert.ThrowsAnyAsync<ArgumentException>(() => new KopiClient(driver).JobListAsync(parameters, Ct));

        Assert.Equal(0, driver.Calls);
    }

    [Fact]
    public async Task An_empty_filter_list_matches_nothing_without_any_sql()
    {
        var driver = new RecordingDriver { ListResult = [Row(1)] };
        var client = new KopiClient(driver);

        var listed = await client.JobListAsync(new JobListParams { Ids = [] }, Ct);
        var deleted = await client.JobDeleteManyAsync(new JobDeleteManyParams { States = [] }, Ct);

        Assert.Empty(listed.Jobs);
        Assert.Null(listed.NextCursor);
        Assert.Empty(deleted);
        Assert.Equal(0, driver.Calls);
    }

    public static TheoryData<JobDeleteManyParams> BadDeletes => new()
    {
        new JobDeleteManyParams { Limit = 0 },
        new JobDeleteManyParams { Limit = 10_001 },
        new JobDeleteManyParams { States = [JobState.Completed, JobState.Running] },
        new JobDeleteManyParams { Priorities = [9] },
    };

    [Theory]
    [MemberData(nameof(BadDeletes))]
    public async Task A_delete_many_with_a_bad_parameter_is_refused_before_any_sql(JobDeleteManyParams parameters)
    {
        var driver = new RecordingDriver();

        await Assert.ThrowsAnyAsync<ArgumentException>(() => new KopiClient(driver).JobDeleteManyAsync(parameters, Ct));

        Assert.Equal(0, driver.Calls);
    }

    [Fact]
    public async Task A_delete_many_passes_its_filter_and_limit()
    {
        var driver = new RecordingDriver();
        var parameters = new JobDeleteManyParams { Kinds = ["k"], States = [JobState.Discarded], Limit = 250 };

        await new KopiClient(driver).JobDeleteManyAsync(parameters, Ct);

        var query = Assert.Single(driver.DeleteManys);
        Assert.Same(parameters, query.Filter);
        Assert.Equal(250, query.Max);
    }

    [Fact]
    public async Task Retrying_an_unknown_job_throws_and_deleting_one_gives_null()
    {
        var client = new KopiClient(new RecordingDriver());

        await Assert.ThrowsAsync<KopiQueueNotFoundException>(() => client.JobRetryAsync(42, Ct));
        Assert.Null(await client.JobDeleteAsync(42, Ct));
        Assert.Null(await client.JobGetAsync(42, Ct));
    }

    private static JobRow Row(long id, DateTimeOffset? finalizedAt = null) => new()
    {
        Id = id,
        Kind = "k",
        Queue = "default",
        State = finalizedAt is null ? JobState.Available : JobState.Completed,
        Priority = 1,
        ArgsJson = "{}",
        MetadataJson = "{}",
        Tags = [],
        Attempt = 0,
        MaxAttempts = 25,
        CreatedAt = Base,
        ScheduledAt = Base,
        FinalizedAt = finalizedAt,
    };
}
