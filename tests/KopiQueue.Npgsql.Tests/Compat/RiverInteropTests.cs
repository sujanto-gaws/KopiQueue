using System.Collections.Concurrent;
using System.Text.Json;
using KopiQueue.Drivers;
using KopiQueue.Migrations;
using KopiQueue.TestInfrastructure;
using Npgsql;

namespace KopiQueue.Npgsql.Tests.Compat;

/// <summary>
/// KopiQueue and a real River v0.51.2 client on one schema (plan task 2.10, NFR-COMP-05): each works the other's jobs,
/// hears the other's notifications, and stores the same outcomes. River runs as a black box (ADR-008).
/// </summary>
public sealed class RiverInteropTests(PostgresFixture db) : IClassFixture<PostgresFixture>
{
    private static readonly TimeSpan LongPoll = TimeSpan.FromSeconds(30);

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Fact]
    public async Task River_works_jobs_KopiQueue_inserts_and_hears_their_notification()
    {
        db.SkipIfUnavailable();
        var river = RiverHarness.GetOrSkip(db.ConnectionString);
        await using var ds = db.CreateDataSource();
        var (driver, schema) = await MigratedAsync(ds);

        // River polls only every 30 s, so picking the jobs up within 10 s needs KopiQueue's insert notification.
        await using var worker = await river.StartWorkerAsync(schema, "compat", expect: 3, poll: LongPoll, cancellationToken: Ct);
        var client = new KopiClient(driver);
        var ids = new List<long>();
        for (var i = 0; i < 3; i++)
        {
            ids.Add((await client.InsertAsync(new CompatArgs("ok"), new InsertOptions { Queue = "compat" }, Ct)).Job.Id);
        }

        await worker.WaitForExitAsync(TimeSpan.FromSeconds(10), Ct);

        foreach (var id in ids)
        {
            var job = await GetAsync(driver, id);
            Assert.Equal((JobState.Completed, 1), (job.State, job.Attempt));
            Assert.Equal(["river-compat"], job.AttemptedBy);
            Assert.NotNull(job.FinalizedAt);
        }
    }

    [Fact]
    public async Task KopiQueue_works_jobs_an_insert_only_River_client_inserts_and_hears_their_notification()
    {
        db.SkipIfUnavailable();
        var river = RiverHarness.GetOrSkip(db.ConnectionString);
        await using var ds = db.CreateDataSource();
        var (driver, schema) = await MigratedAsync(ds);
        var worker = new CompatWorker();
        var client = Client(driver, worker, LongPoll * 10);
        await client.StartAsync(Ct);
        try
        {
            await Task.Delay(300, Ct); // the initial fetch has run and the listener is up

            // One transaction, so one notification after all three exist (River throttles per queue: see below).
            var ids = await river.InsertAsync(schema, "compat", "ok", count: 3, many: true, cancellationToken: Ct);

            foreach (var id in ids)
            {
                var job = await WaitForAsync(driver, id, j => j.State == JobState.Completed, TimeSpan.FromSeconds(10));
                Assert.Equal((1, "kopi-compat"), (job.Attempt, Assert.Single(job.AttemptedBy)));
            }
        }
        finally
        {
            await client.StopAsync(Ct);
        }
    }

    [Fact]
    public async Task An_insert_only_River_client_notifies_at_most_once_per_queue_in_a_burst()
    {
        db.SkipIfUnavailable();
        var river = RiverHarness.GetOrSkip(db.ConnectionString);
        await using var ds = db.CreateDataSource();
        var (_, schema) = await MigratedAsync(ds);
        await using var listener = await ListenAsync(ds, schema, "river_insert");

        await river.InsertAsync(schema, "compat", "ok", count: 3, cancellationToken: Ct); // one process, three inserts
        await Task.Delay(500, Ct);
        var first = listener.Heard.Count;
        await Task.Delay(TimeSpan.FromSeconds(3), Ct);
        await river.InsertAsync(schema, "compat", "ok", count: 1, cancellationToken: Ct); // a new process, later
        await Task.Delay(500, Ct);
        var second = listener.Heard.Count - first;
        await river.InsertAsync(schema, "other", "ok", count: 1, cancellationToken: Ct); // another queue
        await Task.Delay(500, Ct);

        var other = listener.Heard.Count - first - second;
        Report($"insert-only River: burst of 3 -> {first} notification(s); a later insert -> {second}; another queue -> {other}");
        foreach (var (_, payload) in listener.Heard)
        {
            Report($"NOTIFY <schema>.river_insert: {payload}");
        }

        // An insert-only River client does notify, but only once per queue for a burst of inserts: a KopiQueue producer
        // woken by the first notification can miss the rest of the burst until its next poll.
        Assert.Equal((1, 1, 1), (first, second, other));
        Assert.Equal(
            ["compat", "compat", "other"],
            listener.Heard.Select(h =>
            {
                using var payload = JsonDocument.Parse(h.Payload);
                return payload.RootElement.GetProperty("queue").GetString();
            }));
    }

    [Fact]
    public async Task Both_libraries_store_the_same_outcome_for_each_way_a_job_can_end()
    {
        db.SkipIfUnavailable();
        var river = RiverHarness.GetOrSkip(db.ConnectionString);
        await using var ds = db.CreateDataSource();
        var (driver, schema) = await MigratedAsync(ds);
        string[] scripts = ["ok", "fail", "fail-last", "snooze", "cancel"];
        var inserter = new KopiClient(driver);
        var byRiver = new Dictionary<string, long>();
        var byKopi = new Dictionary<string, long>();
        foreach (var script in scripts)
        {
            var options = (string queue) => new InsertOptions { Queue = queue, MaxAttempts = script == "fail-last" ? 1 : 25 };
            byRiver[script] = (await inserter.InsertAsync(new CompatArgs(script), options("by_river"), Ct)).Job.Id;
            byKopi[script] = (await inserter.InsertAsync(new CompatArgs(script), options("by_kopi"), Ct)).Job.Id;
        }

        await using (var worker = await river.StartWorkerAsync(schema, "by_river", expect: scripts.Length, cancellationToken: Ct))
        {
            await worker.WaitForExitAsync(TimeSpan.FromSeconds(30), Ct);
        }

        var kopiWorker = new CompatWorker();
        var client = Client(driver, kopiWorker, TimeSpan.FromMilliseconds(100), queue: "by_kopi");
        await client.StartAsync(Ct);
        var limit = DateTime.UtcNow.AddSeconds(30);
        while (kopiWorker.Ran.Count < scripts.Length)
        {
            Assert.True(DateTime.UtcNow < limit, "KopiQueue did not work every job.");
            await Task.Delay(20, Ct);
        }

        await client.StopAsync(Ct); // writes the pending outcomes

        foreach (var script in scripts)
        {
            var riverOutcome = Outcome(await GetAsync(driver, byRiver[script]));
            var kopiOutcome = Outcome(await GetAsync(driver, byKopi[script]));
            Report($"{script,-10} River:      {riverOutcome}");
            Report($"{script,-10} KopiQueue:  {kopiOutcome}");

            // Known difference, left for the clean-room rewrite of the executor (PRs #111-#113): River stores a cancelled
            // job's error as "JobCancelError: <reason>", KopiQueue as "<reason>".
            Assert.Equal(riverOutcome.Replace("'JobCancelError: ", "'", StringComparison.Ordinal), kopiOutcome);
        }
    }

    [Fact]
    public async Task A_working_River_client_holds_the_leader_row_and_announces_only_its_resignation()
    {
        db.SkipIfUnavailable();
        var river = RiverHarness.GetOrSkip(db.ConnectionString);
        await using var ds = db.CreateDataSource();
        var (_, schema) = await MigratedAsync(ds);
        var heard = new ConcurrentQueue<(string Channel, string Payload)>();
        await using var listen = await ds.OpenConnectionAsync(Ct);
        listen.Notification += (_, e) => heard.Enqueue((e.Channel, e.Payload));
        foreach (var topic in new[] { "river_leadership", "river_control", "river_insert" })
        {
            await using var cmd = new NpgsqlCommand($"LISTEN \"{schema}.{topic}\"", listen);
            await cmd.ExecuteNonQueryAsync(Ct);
        }

        using var stopListening = CancellationTokenSource.CreateLinkedTokenSource(Ct);
        var listening = Task.Run(async () =>
        {
            try
            {
                while (true)
                {
                    await listen.WaitAsync(stopListening.Token);
                }
            }
            catch (OperationCanceledException)
            {
            }
        }, Ct);

        string? leader = null;
        await using (var worker = await river.StartWorkerAsync(schema, "compat", expect: 0, minRun: TimeSpan.FromSeconds(4), cancellationToken: Ct))
        {
            var limit = DateTime.UtcNow.AddSeconds(4);
            while (leader is null && DateTime.UtcNow < limit)
            {
                await using var cmd = ds.CreateCommand($"SELECT leader_id FROM {schema}.river_leader");
                leader = (string?)await cmd.ExecuteScalarAsync(Ct);
                await Task.Delay(50, Ct);
            }

            await worker.WaitForExitAsync(TimeSpan.FromSeconds(30), Ct);
        }

        await Task.Delay(500, Ct);
        await stopListening.CancelAsync();
        await listening;
        await using var after = ds.CreateCommand($"SELECT count(*) FROM {schema}.river_leader");
        var leadersAfterStop = (long)(await after.ExecuteScalarAsync(Ct))!;

        Report($"leader while running: {leader ?? "(none)"}; leader rows after stop: {leadersAfterStop}");
        foreach (var (channel, payload) in heard)
        {
            Report($"NOTIFY {channel.Replace(schema, "<schema>", StringComparison.Ordinal)}: {payload}");
        }

        Assert.Equal("river-compat", leader);  // the leader holds the river_leader row while it runs
        Assert.Equal(0, leadersAfterStop);      // and deletes it when it stops
        var (announcedOn, announced) = Assert.Single(heard); // nothing is announced on election, only the resignation
        Assert.Equal($"{schema}.river_leadership", announcedOn);
        using var resignation = JsonDocument.Parse(announced);
        Assert.Equal(
            ("river-compat", "resigned"),
            (resignation.RootElement.GetProperty("leader_id").GetString(), resignation.RootElement.GetProperty("action").GetString()));
    }

    private static async Task<Listener> ListenAsync(NpgsqlDataSource ds, string schema, params string[] topics)
    {
        var connection = await ds.OpenConnectionAsync(Ct);
        var listener = new Listener(connection);
        foreach (var topic in topics)
        {
            await using var cmd = new NpgsqlCommand($"LISTEN \"{schema}.{topic}\"", connection);
            await cmd.ExecuteNonQueryAsync(Ct);
        }

        listener.Start();
        return listener;
    }

    /// <summary>Collects the notifications a connection hears until disposed.</summary>
    private sealed class Listener(NpgsqlConnection connection) : IAsyncDisposable
    {
        private readonly CancellationTokenSource _stop = CancellationTokenSource.CreateLinkedTokenSource(Ct);
        private Task _loop = Task.CompletedTask;

        public ConcurrentQueue<(string Channel, string Payload)> Heard { get; } = new();

        public void Start()
        {
            connection.Notification += (_, e) => Heard.Enqueue((e.Channel, e.Payload));
            _loop = Task.Run(async () =>
            {
                try
                {
                    while (true)
                    {
                        await connection.WaitAsync(_stop.Token);
                    }
                }
                catch (OperationCanceledException)
                {
                }
            }, Ct);
        }

        public async ValueTask DisposeAsync()
        {
            await _stop.CancelAsync();
            await _loop;
            await connection.DisposeAsync();
            _stop.Dispose();
        }
    }

    /// <summary>Writes to the test output and, when <c>KOPI_COMPAT_REPORT</c> names a file, appends there too.</summary>
    private static void Report(string line)
    {
        TestContext.Current.TestOutputHelper?.WriteLine(line);
        if (Environment.GetEnvironmentVariable("KOPI_COMPAT_REPORT") is { Length: > 0 } path)
        {
            File.AppendAllLines(path, [line]);
        }
    }

    /// <summary>What a stored job says about how its attempt ended, in a form both libraries' rows can be compared by.</summary>
    private static string Outcome(JobRow job)
    {
        var reference = job.AttemptedAt ?? job.CreatedAt;
        var scheduled = (job.ScheduledAt - reference).TotalMinutes switch
        {
            < 1 => "now",
            > 9 and < 11 => "+10m",
            var m => $"{m:0.#}m",
        };
        using var metadata = JsonDocument.Parse(job.MetadataJson);
        var snoozes = metadata.RootElement.TryGetProperty("snoozes", out var s) ? s.GetRawText() : "-";
        // Traces are free-form: River records none for a returned Go error, KopiQueue the exception's stack trace.
        var errors = string.Join("; ", job.Errors.Select(e => $"#{e.Attempt} '{e.Error}'"));
        return $"state={job.State} attempt={job.Attempt}/{job.MaxAttempts} scheduled={scheduled} finalized={job.FinalizedAt is not null} " +
               $"snoozes={snoozes} errors=[{errors}]";
    }

    private static KopiClient Client(IKopiDriver driver, CompatWorker worker, TimeSpan poll, string queue = "compat") => new(driver, new KopiClientOptions
    {
        Queues = new Dictionary<string, QueueOptions> { [queue] = new() { MaxWorkers = 5 } },
        Workers = new WorkerRegistry().Add(worker),
        ClientId = "kopi-compat",
        FetchPollInterval = poll,
        FetchCooldown = TimeSpan.FromMilliseconds(5),
    });

    private static async Task<JobRow> GetAsync(IKopiDriver driver, long id) =>
        await driver.GetExecutor().JobGetByIdAsync(id, Ct) ?? throw new InvalidOperationException($"Job {id} is missing.");

    private static async Task<JobRow> WaitForAsync(IKopiDriver driver, long id, Func<JobRow, bool> condition, TimeSpan limit)
    {
        var deadline = DateTime.UtcNow + limit;
        while (true)
        {
            var job = await GetAsync(driver, id);
            if (condition(job))
            {
                return job;
            }

            Assert.True(DateTime.UtcNow < deadline, $"Job {id} is {job.State} after {limit}.");
            await Task.Delay(20, Ct);
        }
    }

    private static async Task<(IKopiDriver Driver, string Schema)> MigratedAsync(NpgsqlDataSource ds)
    {
        var schema = "x_" + Guid.NewGuid().ToString("N")[..12];
        await using (var cmd = ds.CreateCommand($"CREATE SCHEMA {schema}"))
        {
            await cmd.ExecuteNonQueryAsync(Ct);
        }

        var driver = new NpgsqlDriver(ds, new NpgsqlDriverOptions { Schema = schema });
        await driver.CreateMigrator().MigrateAsync(MigrationDirection.Up, cancellationToken: Ct);
        return (driver, schema);
    }

    /// <summary>Shared with the Go harness: kind <c>compat_task</c>, args <c>{"what": ...}</c>.</summary>
    [JobKind("compat_task")]
    public sealed record CompatArgs(string What) : IJobArgs;

    /// <summary>The same scripts as the Go harness's worker.</summary>
    private sealed class CompatWorker : Worker<CompatArgs>
    {
        public ConcurrentBag<long> Ran { get; } = [];

        public override TimeSpan? NextRetry(Job<CompatArgs> job) => TimeSpan.FromMinutes(10);

        public override Task WorkAsync(Job<CompatArgs> job, CancellationToken cancellationToken)
        {
            Ran.Add(job.Id);
            return job.Args.What switch
            {
                "ok" => Task.CompletedTask,
                "fail" or "fail-last" => throw new InvalidOperationException("compat failure"),
                "snooze" => throw JobControl.Snooze(TimeSpan.FromMinutes(10)),
                "cancel" => throw JobControl.Cancel("compat cancel"),
                _ => throw new ArgumentOutOfRangeException(nameof(job), job.Args.What, "Unknown script."),
            };
        }
    }
}
