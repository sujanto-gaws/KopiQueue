using KopiQueue.Drivers;
using KopiQueue.Migrations;
using KopiQueue.TestInfrastructure.Conformance;
using Xunit;

namespace KopiQueue.TestInfrastructure.Integration;

/// <summary>
/// A database for the shared client integration suites (plan task 5.8: FR-DB-06): working clients, end to end, on any
/// driver. Each test takes a fresh, migrated schema with <see cref="CreateSchemaAsync"/> and disposes it when done
/// (<c>await using</c>), which releases its connections.
/// </summary>
/// <remarks>
/// A driver's test project derives one fixture from this and one class from each suite. The suites use the driver's
/// own executor wherever they can, and <see cref="IntegrationSchema"/>'s few typed helpers for what no executor
/// operation does, such as planting another client's leader row. A database whose plain reads wait for writers (SQL
/// Server without READ_COMMITTED_SNAPSHOT) would make the suites' counts wait on running clients, so a fixture provides
/// one where reads don't, as PostgreSQL's are.
/// </remarks>
public abstract class IntegrationFixture : IAsyncLifetime
{
    private readonly List<IntegrationSchema> _schemas = [];

    /// <summary>Whether clients on this database get notifications (LISTEN/NOTIFY), rather than only polling.</summary>
    public abstract bool HasNotifications { get; }

    /// <summary>Skips the calling test when no database server is available.</summary>
    public abstract void SkipIfUnavailable();

    /// <summary>Creates a fresh schema, migrated to the latest version, for one test.</summary>
    public async Task<IntegrationSchema> CreateSchemaAsync(CancellationToken cancellationToken)
    {
        SkipIfUnavailable();
        var schema = await CreateSchemaCoreAsync(cancellationToken);
        lock (_schemas)
        {
            _schemas.Add(schema);
        }

        await schema.Driver.CreateMigrator().MigrateAsync(MigrationDirection.Up, cancellationToken: cancellationToken);
        return schema;
    }

    /// <inheritdoc />
    public abstract ValueTask InitializeAsync();

    /// <inheritdoc />
    public virtual async ValueTask DisposeAsync()
    {
        GC.SuppressFinalize(this);
        IntegrationSchema[] schemas;
        lock (_schemas)
        {
            schemas = [.. _schemas];
        }

        foreach (var schema in schemas)
        {
            await schema.DisposeAsync();
        }
    }

    /// <summary>Creates an empty schema and a driver for it.</summary>
    protected abstract Task<IntegrationSchema> CreateSchemaCoreAsync(CancellationToken cancellationToken);
}

/// <summary>
/// One test's schema: its driver, and the typed helpers the suites need beyond the driver's executor. Disposing it
/// releases its connections; the fixture disposes any left at its end. Disposing twice is harmless.
/// </summary>
public abstract class IntegrationSchema : IAsyncDisposable
{
    /// <summary>The driver for this schema.</summary>
    public abstract IKopiDriver Driver { get; }

    /// <summary>The schema's name.</summary>
    public abstract string Name { get; }

    /// <summary>Registers this schema's database on <paramref name="builder"/>, as an application does (<c>UseNpgsql</c>, <c>UseSqlServer</c>).</summary>
    public abstract KopiQueueBuilder UseIn(KopiQueueBuilder builder);

    /// <summary>Starts a caller's transaction on a connection of its own.</summary>
    public abstract Task<CallerTransaction> BeginTransactionAsync(CancellationToken cancellationToken);

    /// <summary>Every job in the schema, through the driver's admin list.</summary>
    public async Task<IReadOnlyList<JobRow>> JobsAsync(CancellationToken cancellationToken) =>
        await Driver.GetExecutor().JobListAsync(new JobListQuery { Filter = new JobListParams(), Max = 1_000_000 }, cancellationToken);

    /// <summary>How many jobs match <paramref name="where"/>.</summary>
    public async Task<int> CountAsync(Func<JobRow, bool> where, CancellationToken cancellationToken) =>
        (await JobsAsync(cancellationToken)).Count(where);

    /// <summary>Waits until <paramref name="count"/> or more jobs match <paramref name="where"/>, failing after <paramref name="limit"/>.</summary>
    public async Task WaitForCountAsync(Func<JobRow, bool> where, int count, TimeSpan limit, CancellationToken cancellationToken)
    {
        var deadline = DateTime.UtcNow + limit;
        int seen;
        while ((seen = await CountAsync(where, cancellationToken)) < count)
        {
            Assert.True(DateTime.UtcNow < deadline, $"Only {seen} of {count} jobs matched after {limit}.");
            await Task.Delay(50, cancellationToken);
        }
    }

    /// <summary>Waits until job <paramref name="id"/> is in <paramref name="state"/>, failing after 15 s.</summary>
    public async Task<JobRow> WaitForStateAsync(long id, JobState state, CancellationToken cancellationToken)
    {
        var deadline = DateTime.UtcNow.AddSeconds(15);
        while (true)
        {
            var row = (await Driver.GetExecutor().JobGetByIdAsync(id, cancellationToken))!;
            if (row.State == state)
            {
                return row;
            }

            Assert.True(DateTime.UtcNow < deadline, $"Job {id} is {row.State}, not {state}, after 15 s.");
            await Task.Delay(20, cancellationToken);
        }
    }

    /// <summary>The names of every queue row, in order, through the driver's queue list.</summary>
    public async Task<IReadOnlyList<string>> QueuesAsync(CancellationToken cancellationToken) =>
        [.. (await Driver.GetExecutor().QueueListAsync(new QueueListQuery { Max = 10_000 }, cancellationToken)).Select(q => q.Name)];

    /// <summary>Waits until the leader's ID (<c>null</c> without one) meets <paramref name="condition"/>, failing after <paramref name="limit"/>.</summary>
    public async Task<string?> WaitForLeaderAsync(Func<string?, bool> condition, TimeSpan limit, CancellationToken cancellationToken)
    {
        var deadline = DateTime.UtcNow + limit;
        string? leader;
        while (!condition(leader = await LeaderAsync(cancellationToken)))
        {
            Assert.True(DateTime.UtcNow < deadline, $"The leader is {leader ?? "nobody"} after {limit}.");
            await Task.Delay(50, cancellationToken);
        }

        return leader;
    }

    /// <summary>The database's current time.</summary>
    public abstract Task<DateTimeOffset> NowAsync(CancellationToken cancellationToken);

    /// <summary>Plants a leader row for another client, expiring <paramref name="expiresIn"/> from the database's now.</summary>
    public abstract Task SetLeaderAsync(string leaderId, TimeSpan expiresIn, CancellationToken cancellationToken);

    /// <summary>Deletes the leader row, as if its lease had been taken away.</summary>
    public abstract Task DeleteLeaderAsync(CancellationToken cancellationToken);

    /// <summary>The current leader's ID, or <c>null</c> without a leader row.</summary>
    public abstract Task<string?> LeaderAsync(CancellationToken cancellationToken);

    /// <summary>Inserts a queue row last updated <paramref name="age"/> ago, paused then if <paramref name="paused"/>.</summary>
    public abstract Task InsertQueueAsync(string name, TimeSpan age, bool paused, CancellationToken cancellationToken);

    /// <summary>Locks queue <paramref name="name"/>'s row, as another client's transaction updating it would, until disposed.</summary>
    public abstract Task<IAsyncDisposable> LockQueueRowAsync(string name, CancellationToken cancellationToken);

    /// <summary>Sets job <paramref name="id"/>'s unique key and states, as an insert by another client could have.</summary>
    public abstract Task SetUniqueAsync(long id, byte[] key, byte states, CancellationToken cancellationToken);

    /// <summary>Job <paramref name="id"/>'s unique key and states as stored.</summary>
    public abstract Task<(byte[]? Key, byte? States)> UniqueAsync(long id, CancellationToken cancellationToken);

    /// <summary>Whether <paramref name="exception"/> is the database refusing a duplicate in a unique index.</summary>
    public abstract bool IsUniqueViolation(Exception exception);

    /// <summary>Sets <c>metadata.cancel_attempted_at</c> on job <paramref name="id"/> directly, as another tool could.</summary>
    public abstract Task MarkCancelAttemptedAsync(long id, CancellationToken cancellationToken);

    /// <inheritdoc />
    public abstract ValueTask DisposeAsync();
}
