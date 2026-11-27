using System.Security.Cryptography;
using System.Text;
using KopiQueue.Drivers;
using KopiQueue.Serialization;
using Microsoft.Extensions.Time.Testing;

namespace KopiQueue.Tests;

/// <summary>
/// Unique inserts through the client (plan task 4.2: FR-UNQ-01, FR-UNQ-03, FR-UNQ-04, FR-UNQ-05): the options become
/// the driver's unique key and states, before any SQL runs.
/// </summary>
public sealed class UniqueInsertTests
{
    private static readonly DateTimeOffset Now = new(2026, 11, 26, 12, 34, 56, TimeSpan.Zero);

    private static readonly SystemTextJsonArgsSerializer Json = new(WebJsonContext.Default);

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Fact]
    public async Task Unique_options_become_the_key_and_states_of_the_insert()
    {
        var (driver, client, _) = Create();
        var options = new UniqueOptions { ByArgs = true, ByQueue = true, ByPeriod = TimeSpan.FromHours(1) };

        await client.InsertAsync(new SendEmailArgs("a@example.com", "welcome", [1]), new InsertOptions { Queue = "email", Unique = options }, Ct);

        var insert = Assert.Single(driver.Inserts);
        var expected = UniqueKey.Compute("send_email", insert.ArgsJson, "email", Now, options);
        Assert.Equal(expected, insert.UniqueKey);
        Assert.Equal(UniqueStates.Default, insert.UniqueStates);
        Assert.Equal(
            "&kind=send_email&args={\"ids\":[1],\"templateId\":\"welcome\",\"to\":\"a@example.com\"}&period=2026-11-26T12:00:00Z&queue=email",
            UniqueKey.Preimage("send_email", insert.ArgsJson, "email", Now, options));
    }

    [Fact]
    public async Task Only_members_marked_UniqueKey_are_part_of_the_key_under_their_json_names()
    {
        var (driver, client, _) = Create();
        var unique = new InsertOptions { Unique = new() { ByArgs = true } };

        await client.InsertAsync(new SyncCustomerArgs(7, "acme", "alice"), unique, Ct);
        await client.InsertAsync(new SyncCustomerArgs(7, "acme", "bob"), unique, Ct);
        await client.InsertManyAsync([new(new SyncCustomerArgs(8, "acme", "alice"), unique)], cancellationToken: Ct);

        var keys = driver.Inserts.Select(i => Convert.ToHexString(i.UniqueKey!)).ToList();
        Assert.Equal(keys[0], keys[1]); // who asked is not part of the key
        Assert.NotEqual(keys[0], keys[2]);
        Assert.Equal(
            "&kind=sync_customer&args={\"customerId\":7,\"tenant\":\"acme\"}",
            UniqueKey.Preimage("sync_customer", driver.Inserts[0].ArgsJson, "default", Now, unique.Unique!, Json.GetUniqueMemberNames(typeof(SyncCustomerArgs))));
    }

    [Fact]
    public void The_serialiser_names_marked_members_as_it_writes_them()
    {
        Assert.Equal(["customerId", "tenant"], Json.GetUniqueMemberNames(typeof(SyncCustomerArgs)).Order(StringComparer.Ordinal));
        Assert.Equal(
            ["CustomerId", "tenant"],
            new SystemTextJsonArgsSerializer(PlainJsonContext.Default).GetUniqueMemberNames(typeof(SyncCustomerArgs)).Order(StringComparer.Ordinal));
        Assert.Empty(Json.GetUniqueMemberNames(typeof(SendEmailArgs)));
    }

    [Fact]
    public async Task A_serialiser_that_cannot_name_marked_members_fails_only_when_they_matter()
    {
        var (driver, client, _) = Create(new DelegatingSerializer(Json));

        // No marked members: all members are used.
        await client.InsertAsync(new SendEmailArgs("a", "t", []), new InsertOptions { Unique = new() { ByArgs = true } }, Ct);

        // Marked members, but the args are not part of the key.
        await client.InsertAsync(new SyncCustomerArgs(1, "t", "x"), new InsertOptions { Unique = new() { ByQueue = true } }, Ct);

        var failure = await Assert.ThrowsAsync<NotSupportedException>(
            () => client.InsertAsync(new SyncCustomerArgs(1, "t", "x"), new InsertOptions { Unique = new() { ByArgs = true } }, Ct));
        Assert.Contains("GetUniqueMemberNames", failure.Message, StringComparison.Ordinal);
        Assert.Equal(2, driver.Inserts.Count);
    }

    [Fact]
    public async Task Options_with_nothing_set_make_no_job_unique_and_states_alone_make_the_kind_unique()
    {
        var (driver, client, _) = Create();

        await client.InsertAsync(new SendEmailArgs("a", "t", []), new InsertOptions { Unique = new() }, Ct);
        await client.InsertAsync(new SendEmailArgs("a", "t", []), new InsertOptions { Unique = new() { ByState = Enum.GetValues<JobState>() } }, Ct);

        Assert.Null(driver.Inserts[0].UniqueKey);
        Assert.Null(driver.Inserts[0].UniqueStates);
        Assert.Equal(SHA256.HashData(Encoding.UTF8.GetBytes("&kind=send_email")), driver.Inserts[1].UniqueKey);
        Assert.Equal((byte)0b1111_1111, driver.Inserts[1].UniqueStates);
    }

    [Fact]
    public async Task Invalid_unique_options_are_rejected_before_any_sql_naming_the_option_and_the_job()
    {
        var (driver, client, _) = Create();

        var states = await Assert.ThrowsAsync<ArgumentException>(() => client.InsertAsync(
            new SendEmailArgs("a", "t", []), new InsertOptions { Unique = new() { ByState = [JobState.Available, JobState.Running] } }, Ct));
        var period = await Assert.ThrowsAsync<ArgumentException>(() => client.InsertManyAsync(
            [new(new SendEmailArgs("a", "t", [])), new(new SendEmailArgs("a", "t", []), new InsertOptions { Unique = new() { ByPeriod = TimeSpan.FromSeconds(-1) } })],
            cancellationToken: Ct));
        var kind = await Assert.ThrowsAsync<ArgumentException>(() => client.InsertAsync(
            new SendEmailArgs("a", "t", []), new InsertOptions { Unique = new() { ExcludeKind = true } }, Ct));

        Assert.StartsWith("Invalid insert option Unique: unique states must include available, pending, running and scheduled; missing: pending, scheduled.", states.Message, StringComparison.Ordinal);
        Assert.StartsWith("Job 1: Invalid insert option Unique: the unique period must be positive", period.Message, StringComparison.Ordinal);
        Assert.Contains("excludes its kind needs another dimension", kind.Message, StringComparison.Ordinal);
        Assert.Empty(driver.Inserts);
    }

    [Fact]
    public async Task The_fast_insert_rejects_unique_jobs_before_any_sql()
    {
        var (driver, client, _) = Create();

        var failure = await Assert.ThrowsAsync<NotSupportedException>(() => client.InsertManyFastAsync(
            [new(new SendEmailArgs("a", "t", [])), new(new SendEmailArgs("a", "t", []), new InsertOptions { Unique = new() { ByArgs = true } })],
            cancellationToken: Ct));

        Assert.StartsWith("Job 1 is unique", failure.Message, StringComparison.Ordinal);
        Assert.Equal(0, driver.Calls);
    }

    [Fact]
    public async Task The_period_follows_the_clients_clock()
    {
        var (driver, client, time) = Create();
        var hourly = new InsertOptions { Unique = new() { ByPeriod = TimeSpan.FromHours(1) } };

        await client.InsertAsync(new SendEmailArgs("a", "t", []), hourly, Ct);
        time.Advance(TimeSpan.FromMinutes(25)); // 12:59:56
        await client.InsertAsync(new SendEmailArgs("a", "t", []), hourly, Ct);
        time.Advance(TimeSpan.FromSeconds(4)); // 13:00:00
        await client.InsertAsync(new SendEmailArgs("a", "t", []), hourly, Ct);

        var keys = driver.Inserts.Select(i => Convert.ToHexString(i.UniqueKey!)).ToList();
        Assert.Equal(keys[0], keys[1]);
        Assert.NotEqual(keys[1], keys[2]);
    }

    [Fact]
    public async Task The_encoding_option_chooses_how_args_values_are_written()
    {
        var goDriver = new KopiClientTests.RecordingDriver();
        var verbatimDriver = new KopiClientTests.RecordingDriver();
        var args = new SendEmailArgs("<a&b>", "t", []);
        var unique = new InsertOptions { Unique = new() { ByArgs = true } };

        await new KopiClient(goDriver, new KopiClientOptions { ArgsSerializer = Json }).InsertAsync(args, unique, Ct);
        await new KopiClient(verbatimDriver, new KopiClientOptions { ArgsSerializer = Json, UniqueArgsEncoding = UniqueArgsEncoding.Verbatim })
            .InsertAsync(args, unique, Ct);

        var json = goDriver.Inserts[0].ArgsJson;
        Assert.Equal(UniqueKey.Compute("send_email", json, "default", Now, unique.Unique!), goDriver.Inserts[0].UniqueKey);
        Assert.Equal(
            UniqueKey.Compute("send_email", json, "default", Now, unique.Unique!, encoding: UniqueArgsEncoding.Verbatim),
            verbatimDriver.Inserts[0].UniqueKey);
        Assert.NotEqual(goDriver.Inserts[0].UniqueKey, verbatimDriver.Inserts[0].UniqueKey);
        Assert.Throws<ArgumentOutOfRangeException>(
            () => new KopiClient(goDriver, new KopiClientOptions { UniqueArgsEncoding = (UniqueArgsEncoding)7 }));
    }

    [Fact]
    public async Task A_duplicate_the_driver_reports_is_returned_as_skipped()
    {
        var driver = new KopiClientTests.RecordingDriver { SkipAsDuplicate = true };
        var client = new KopiClient(driver, new KopiClientOptions { ArgsSerializer = Json, TimeProvider = new FakeTimeProvider(Now) });

        var single = await client.InsertAsync(new SendEmailArgs("a", "t", []), new InsertOptions { Unique = new() { ByArgs = true } }, Ct);
        var many = await client.InsertManyAsync([new(new SendEmailArgs("a", "t", []), new InsertOptions { Unique = new() { ByArgs = true } })], cancellationToken: Ct);

        Assert.True(single.UniqueSkippedAsDuplicate);
        Assert.True(Assert.Single(many).UniqueSkippedAsDuplicate);
    }

    private static (KopiClientTests.RecordingDriver Driver, KopiClient Client, FakeTimeProvider Time) Create(IJobArgsSerializer? serializer = null)
    {
        var driver = new KopiClientTests.RecordingDriver();
        var time = new FakeTimeProvider(Now);
        return (driver, new KopiClient(driver, new KopiClientOptions { ArgsSerializer = serializer ?? Json, TimeProvider = time }), time);
    }

    /// <summary>A custom serialiser that leaves <see cref="IJobArgsSerializer.GetUniqueMemberNames"/> to its default.</summary>
    private sealed class DelegatingSerializer(IJobArgsSerializer inner) : IJobArgsSerializer
    {
        public string Serialize(IJobArgs args, Type argsType) => inner.Serialize(args, argsType);

        public IJobArgs Deserialize(string json, Type argsType) => inner.Deserialize(json, argsType);
    }
}
