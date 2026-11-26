using System.Text.Json.Nodes;

namespace KopiQueue.Tests;

/// <summary>Insert options: defaults, resolution and validation (plan task 1.5: FR-INS-05, FR-INS-09).</summary>
public sealed class JobValidationTests
{
    private static readonly DateTimeOffset Now = new(2026, 11, 26, 12, 0, 0, TimeSpan.Zero);

    [Fact]
    public void No_options_means_the_defaults()
    {
        var p = JobValidation.ToInsertParams("k", "{}", null, Now);

        Assert.Equal(("k", "{}", "default", JobState.Available, 1, 25), (p.Kind, p.ArgsJson, p.Queue, p.State, p.Priority, p.MaxAttempts));
        Assert.Null(p.ScheduledAt);
        Assert.Equal("{}", p.MetadataJson);
        Assert.Empty(p.Tags);
        Assert.Null(p.UniqueKey);
        Assert.Equal(p.Queue, JobValidation.ToInsertParams("k", "{}", new InsertOptions(), Now).Queue);
    }

    [Fact]
    public void Every_option_is_carried_over()
    {
        var at = Now.AddMinutes(5);
        var options = new InsertOptions
        {
            Queue = "email-2",
            Priority = 4,
            ScheduledAt = at,
            MaxAttempts = 3,
            Tags = ["welcome", "batch_7"],
            Metadata = new JsonObject { ["source"] = "signup", ["n"] = 1 },
        };

        var p = JobValidation.ToInsertParams("k", "[1]", options, Now);

        Assert.Equal(("email-2", JobState.Scheduled, 4, 3, at), (p.Queue, p.State, p.Priority, p.MaxAttempts, p.ScheduledAt));
        Assert.Equal(["welcome", "batch_7"], p.Tags);
        Assert.Equal("""{"source":"signup","n":1}""", p.MetadataJson);
    }

    [Fact]
    public void Only_a_future_time_schedules_the_job()
    {
        Assert.Equal(JobState.Available, JobValidation.ToInsertParams("k", "{}", new InsertOptions { ScheduledAt = Now }, Now).State);
        Assert.Equal(JobState.Available, JobValidation.ToInsertParams("k", "{}", new InsertOptions { ScheduledAt = Now.AddDays(-1) }, Now).State);
        Assert.Equal(JobState.Scheduled, JobValidation.ToInsertParams("k", "{}", new InsertOptions { ScheduledAt = Now.AddTicks(1) }, Now).State);
    }

    [Fact]
    public void The_tags_are_copied()
    {
        var tags = new List<string> { "abc" };
        var p = JobValidation.ToInsertParams("k", "{}", new InsertOptions { Tags = tags }, Now);
        tags.Add("def");

        Assert.Equal(["abc"], p.Tags);
    }

    [Theory]
    [InlineData("default")]
    [InlineData("a")]
    [InlineData("0")]
    [InlineData("email_high-priority2")]
    [InlineData("a--b__c")]
    public void Valid_queue_names(string queue)
    {
        Assert.Null(JobValidation.QueueError(queue));
        Assert.Equal(queue, JobValidation.ToInsertParams("k", "{}", new InsertOptions { Queue = queue }, Now).Queue);
    }

    [Theory]
    [InlineData("", "1-127 characters")]
    [InlineData("Email", "lower-case")]
    [InlineData("e mail", "lower-case")]
    [InlineData("e.mail", "lower-case")]
    [InlineData("émail", "lower-case")]
    [InlineData("_email", "start and end")]
    [InlineData("email-", "start and end")]
    public void Invalid_queue_names(string queue, string expected)
    {
        var ex = Assert.Throws<ArgumentException>(() => JobValidation.ToInsertParams("k", "{}", new InsertOptions { Queue = queue }, Now));

        Assert.Equal("options", ex.ParamName);
        Assert.Contains("Invalid insert option Queue", ex.Message, StringComparison.Ordinal);
        Assert.Contains(expected, ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void Queue_names_may_be_127_characters()
    {
        Assert.Null(JobValidation.QueueError(new string('q', 127)));
        Assert.Contains("128 characters long", JobValidation.QueueError(new string('q', 128)), StringComparison.Ordinal);
        Assert.Contains("null", JobValidation.QueueError(null), StringComparison.Ordinal);
    }

    [Theory]
    [InlineData(0)]
    [InlineData(5)]
    [InlineData(-1)]
    public void Priority_must_be_1_to_4(int priority)
    {
        var ex = Assert.Throws<ArgumentException>(() => JobValidation.ToInsertParams("k", "{}", new InsertOptions { Priority = priority }, Now));

        Assert.Contains("the priority must be 1-4", ex.Message, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData(0)]
    [InlineData(-3)]
    [InlineData(32768)]
    public void Max_attempts_must_be_1_to_32767(int maxAttempts)
    {
        var ex = Assert.Throws<ArgumentException>(() => JobValidation.ToInsertParams("k", "{}", new InsertOptions { MaxAttempts = maxAttempts }, Now));

        Assert.Contains("max attempts must be 1-32767", ex.Message, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData(1)]
    [InlineData(4)]
    public void Priority_bounds_are_allowed(int priority) =>
        Assert.Equal(priority, JobValidation.ToInsertParams("k", "{}", new InsertOptions { Priority = priority, MaxAttempts = short.MaxValue }, Now).Priority);

    [Theory]
    [InlineData("abc")]
    [InlineData("a-b")]
    [InlineData("_a_")]
    [InlineData("Tag_With-Dashes-9")]
    [InlineData("a--b")]
    public void Valid_tags(string tag) => Assert.Null(JobValidation.TagError(tag));

    [Theory]
    [InlineData("t1", "3-255 characters")]
    [InlineData("", "3-255 characters")]
    [InlineData("-ab", "must not start or end with '-'")]
    [InlineData("ab-", "must not start or end with '-'")]
    [InlineData("a,b", "only ASCII letters")]
    [InlineData("a b", "only ASCII letters")]
    [InlineData("tag.x", "only ASCII letters")]
    [InlineData("tàg", "only ASCII letters")]
    public void Invalid_tags(string tag, string expected)
    {
        var ex = Assert.Throws<ArgumentException>(() => JobValidation.ToInsertParams("k", "{}", new InsertOptions { Tags = ["good", tag] }, Now));

        Assert.Contains("Invalid insert option Tags", ex.Message, StringComparison.Ordinal);
        Assert.Contains(expected, ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void Tags_may_be_255_characters()
    {
        Assert.Null(JobValidation.TagError(new string('t', 255)));
        Assert.NotNull(JobValidation.TagError(new string('t', 256)));
        Assert.NotNull(JobValidation.TagError(null));
    }

    [Fact]
    public void Kind_and_args_are_required()
    {
        Assert.Throws<ArgumentNullException>(() => JobValidation.ToInsertParams(null!, "{}", null, Now));
        Assert.Throws<ArgumentNullException>(() => JobValidation.ToInsertParams("k", null!, null, Now));
    }
}
