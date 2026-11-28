using Xunit;

namespace KopiQueue.TestInfrastructure.Integration;

/// <summary>
/// Unique inserts on a real database (plan task 4.2: FR-UNQ-02, FR-UNQ-03, FR-UNQ-06): the client stores the unique key
/// and states River v0.51.2 computes for the same job (spike 0.8's golden vectors), and concurrent inserts store each
/// unique job once.
/// </summary>
public abstract class UniqueTests<TFixture>(TFixture db)
    where TFixture : IntegrationFixture
{
    /// <summary>The vector "field tags": kind golden_tagged, unique by args, members region and customer.</summary>
    protected const string FieldTagsKey = "f0fa6311cccc42fd4aadc7891f279bd48b2180c8d462a2158da9b28256c97434";

    /// <summary>The vector "all dims": kind golden_gen, unique by args, a 1 h period and the queue.</summary>
    private const string AllDimsKey = "a0439f8b3436dd471e8277e3d99ca48585c5a4737d11a73946b802d3c5f2120f";

    /// <summary>The default unique states (design §5.6) as River's bit string: all but cancelled and discarded.</summary>
    private const string DefaultStates = "11110101";

    /// <summary>The driver's fixture.</summary>
    protected TFixture Fixture => db;

    /// <summary>The current test's cancellation token.</summary>
    protected static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Fact]
    public async Task The_client_stores_Rivers_key_for_marked_members()
    {
        await using var schema = await db.CreateSchemaAsync(Ct);
        var driver = schema.Driver;
        var client = new KopiClient(driver);

        var result = await client.InsertAsync(
            new GoldenTaggedArgs("eu", new GoldenCustomer("x", 1), "ignored"), new InsertOptions { Unique = new() { ByArgs = true } }, Ct);

        Assert.Equal((FieldTagsKey, DefaultStates), await UniqueOfAsync(schema, result.Job.Id));
    }

    [Fact]
    public async Task The_client_stores_Rivers_key_by_args_period_and_queue()
    {
        await using var schema = await db.CreateSchemaAsync(Ct);
        var driver = schema.Driver;
        var time = new FixedTime(DateTimeOffset.Parse("2026-11-26T01:06:14.88365Z", System.Globalization.CultureInfo.InvariantCulture));
        var client = new KopiClient(driver, new KopiClientOptions { TimeProvider = time });
        var options = new InsertOptions { Queue = "q_all", Unique = new() { ByArgs = true, ByPeriod = TimeSpan.FromHours(1), ByQueue = true } };

        var result = await client.InsertAsync(new GoldenGenArgs("all dims", new GoldenValue("all")), options, Ct);

        Assert.Equal((AllDimsKey, DefaultStates), await UniqueOfAsync(schema, result.Job.Id));
    }

    [Fact]
    public async Task At_most_one_job_per_customer_waits_however_many_clients_insert_at_once()
    {
        // UC-4: concurrent inserts from separate clients, each on its own connection, store one job.
        await using var schema = await db.CreateSchemaAsync(Ct);
        var driver = schema.Driver;
        var unique = new InsertOptions { Unique = new() { ByArgs = true } };

        var results = await Task.WhenAll(Enumerable.Range(0, 16).Select(i => Task.Run(
            () => new KopiClient(driver).InsertAsync(new GoldenTaggedArgs("eu", new GoldenCustomer("c", 42), $"request {i}"), unique, Ct),
            Ct)));

        Assert.Single(results, r => !r.UniqueSkippedAsDuplicate);
        Assert.Single(results.Select(r => r.Job.Id).Distinct());
        Assert.Single(await schema.JobsAsync(Ct));
    }

    [Fact]
    public async Task Concurrent_bulk_inserts_of_the_same_unique_jobs_in_different_orders_store_each_once_without_deadlocking()
    {
        // Plan task 4.6: eight clients insert the same 40 unique jobs at once, each batch in its own order, five times.
        await using var schema = await db.CreateSchemaAsync(Ct);
        var driver = schema.Driver;

        for (var round = 0; round < 5; round++)
        {
            var customers = Enumerable.Range(round * 100, 40).Select(i => (long)i).ToArray();
            var batches = await Task.WhenAll(Enumerable.Range(0, 8).Select(client => Task.Run(
                () => new KopiClient(driver).InsertManyAsync(
                    [.. customers.OrderBy(c => HashCode.Combine(c, client, round)).Select(c => new InsertRequest(new SyncCustomerArgs(c, "bulk")))],
                    cancellationToken: Ct),
                Ct)));

            foreach (var customer in customers)
            {
                // Each customer: one insert stored it, the seven others got that same job back as a duplicate.
                var results = batches.Select(b => b.Single(r => CustomerOf(r.Job) == customer)).ToArray();
                Assert.Single(results, r => !r.UniqueSkippedAsDuplicate);
                Assert.Single(results.Select(r => r.Job.Id).Distinct());
            }
        }

        var stored = (await schema.JobsAsync(Ct)).Where(j => j.Kind == "sync_customer_defaults").ToList();
        var keys = new HashSet<string>(StringComparer.Ordinal);
        foreach (var job in stored)
        {
            keys.Add((await UniqueOfAsync(schema, job.Id)).Key);
        }

        Assert.Equal((200, 200), (stored.Count, keys.Count));
    }

    [Fact]
    public async Task Uniqueness_from_the_args_type_keeps_one_pending_job_per_customer_without_options_at_the_call_site()
    {
        // UC-4 with args-level defaults (plan task 4.5, FR-DEF-05): the args type declares its queue and uniqueness.
        await using var schema = await db.CreateSchemaAsync(Ct);
        var driver = schema.Driver;
        var client = new KopiClient(driver);

        var first = await client.InsertAsync(new SyncCustomerArgs(42, "web"), cancellationToken: Ct);
        var again = await client.InsertAsync(new SyncCustomerArgs(42, "web"), cancellationToken: Ct);
        var other = await client.InsertAsync(new SyncCustomerArgs(43, "web"), cancellationToken: Ct);
        var many = await client.InsertManyAsync([new(new SyncCustomerArgs(42, "web")), new(new SyncCustomerArgs(44, "web"))], cancellationToken: Ct);

        Assert.Equal(("sync", 3), (first.Job.Queue, first.Job.MaxAttempts));
        Assert.True(again.UniqueSkippedAsDuplicate);
        Assert.Equal(first.Job.Id, again.Job.Id);
        Assert.False(other.UniqueSkippedAsDuplicate);
        Assert.Equal([true, false], many.Select(r => r.UniqueSkippedAsDuplicate));
        Assert.Equal(3, await schema.CountAsync(j => j.Kind == "sync_customer_defaults", Ct));
        Assert.Equal(DefaultStates, (await UniqueOfAsync(schema, first.Job.Id)).States);
    }

    private static long CustomerOf(JobRow job)
    {
        using var json = System.Text.Json.JsonDocument.Parse(job.ArgsJson);
        return json.RootElement.GetProperty("customerId").GetInt64();
    }

    /// <summary>Job <paramref name="id"/>'s unique key in lower-case hex and its states as River's bit string.</summary>
    private static async Task<(string Key, string States)> UniqueOfAsync(IntegrationSchema schema, long id)
    {
        var (key, states) = await schema.UniqueAsync(id, Ct);
        Assert.NotNull(key);
        Assert.NotNull(states);
        return (Convert.ToHexStringLower(key), Convert.ToString(states.Value, 2).PadLeft(8, '0'));
    }

    /// <summary>Args that declare their own queue and uniqueness.</summary>
    [JobKind("sync_customer_defaults")]
    public sealed record SyncCustomerArgs(long CustomerId, string RequestedBy) : IJobArgsWithInsertOptions
    {
        public InsertOptions GetInsertOptions() => new() { Queue = "sync", MaxAttempts = 3, Unique = new() { ByArgs = true } };
    }

    /// <summary>The golden vectors' <c>golden_tagged</c> args; Go tagged region, customer and omit as unique.</summary>
    [JobKind("golden_tagged")]
    public sealed record GoldenTaggedArgs([property: UniqueKey] string Region, [property: UniqueKey] GoldenCustomer Customer, string Note) : IJobArgs;

    public sealed record GoldenCustomer(string B, int A);

    /// <summary>The golden vectors' <c>golden_gen</c> args.</summary>
    [JobKind("golden_gen")]
    public sealed record GoldenGenArgs(string V, GoldenValue P) : IJobArgs;

    public sealed record GoldenValue(string V);

    private sealed class FixedTime(DateTimeOffset now) : TimeProvider
    {
        public override DateTimeOffset GetUtcNow() => now;
    }
}
