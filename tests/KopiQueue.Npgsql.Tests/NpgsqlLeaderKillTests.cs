using System.Diagnostics;
using KopiQueue.Drivers;
using KopiQueue.Migrations;
using KopiQueue.TestInfrastructure;
using Npgsql;

namespace KopiQueue.Npgsql.Tests;

/// <summary>
/// M3's exit criterion "a 3-node cluster survives killing the leader" (plan task 3.8: FR-MNT-01; design §5.5), with real
/// processes: three <c>KopiQueue.TestNode</c> clients work one queue, the leader's process is killed outright, so it
/// never resigns, and a survivor takes the lease once it expires, runs maintenance, and the cluster keeps working jobs.
/// Each node uses a 500 ms elect interval and a 3 s lease, a 2 s job timeout and a 3 s <c>RescueStuckJobsAfter</c>.
/// M3's exit criterion "stuck jobs are rescued" (task 3.4, FR-MNT-03, NFR-REL-02, UC-9) kills the node running a job,
/// and a survivor rescues the job and completes it.
/// </summary>
public sealed class NpgsqlLeaderKillTests(PostgresFixture db) : IClassFixture<PostgresFixture>
{
    private const string NodeConnectionVariable = "KOPI_NODE_PG";
    private static readonly TimeSpan LeaseTtl = TimeSpan.FromSeconds(3);

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Fact]
    public async Task A_three_node_cluster_survives_killing_the_leaders_process()
    {
        db.SkipIfUnavailable();
        await using var ds = db.CreateDataSource();
        var (driver, schema) = await MigratedAsync(ds);
        var inserter = new KopiClient(driver);

        var nodes = new Dictionary<string, Node>(StringComparer.Ordinal);
        try
        {
            foreach (var id in new[] { "node-a", "node-b", "node-c" })
            {
                nodes[id] = Node.Start(db.ConnectionString, schema, id);
            }

            foreach (var node in nodes.Values)
            {
                await node.WaitReadyAsync(TimeSpan.FromSeconds(30));
            }

            var leader = await WaitForLeaderAsync(ds, schema, l => l is not null && nodes.ContainsKey(l), TimeSpan.FromSeconds(10));
            await InsertAndWaitWorkedAsync(inserter, ds, schema, first: 0, count: 20);

            // Kill the leader: no resignation, no clean stop, its lease just stops being renewed.
            var killed = Stopwatch.StartNew();
            nodes[leader!].Kill();
            var survivors = nodes.Keys.Where(id => id != leader).ToHashSet(StringComparer.Ordinal);

            // Only a leader deletes it, so its deletion shows a survivor runs maintenance after the takeover.
            await AddStaleQueueAsync(ds, schema, "abandoned");

            // Nobody takes an unexpired lease: the dead leader keeps it for most of its 3 s.
            await Task.Delay(TimeSpan.FromSeconds(1), Ct);
            Assert.Equal(leader, await LeaderAsync(ds, schema));
            Assert.Contains("abandoned", await QueuesAsync(ds, schema));

            var successor = await WaitForLeaderAsync(ds, schema, l => l is not null && survivors.Contains(l), TimeSpan.FromSeconds(15));
            var tookOver = killed.Elapsed;

            // At most one elect interval before the lease ran out, at the earliest (the last renewal came up to one interval
            // before the kill); and well within a second lease once it had.
            Assert.InRange(tookOver, LeaseTtl - TimeSpan.FromSeconds(1), LeaseTtl + TimeSpan.FromSeconds(5));
            await WaitUntilAsync(async () => !(await QueuesAsync(ds, schema)).Contains("abandoned"), TimeSpan.FromSeconds(10), "The new leader did not delete the abandoned queue.");

            // The two survivors work everything inserted after the kill; the dead node takes no part.
            var workedBy = await InsertAndWaitWorkedAsync(inserter, ds, schema, first: 20, count: 20);
            Assert.All(workedBy, by => Assert.Contains(by, survivors));
            Assert.Equal(successor, await LeaderAsync(ds, schema));

            // A clean stop of the survivors still resigns the lease.
            foreach (var id in survivors)
            {
                Assert.Equal(0, await nodes[id].StopAsync(TimeSpan.FromSeconds(30)));
            }

            Assert.Null(await LeaderAsync(ds, schema));
        }
        finally
        {
            foreach (var node in nodes.Values)
            {
                node.Dispose();
            }
        }
    }

    [Fact]
    public async Task Killing_the_leaders_process_neither_repeats_a_periodic_tick_nor_skips_two_in_a_row()
    {
        // M4 (plan task 4.6, FR-PER-04, UC-3): three nodes share a periodic job every 4 s; the leader's process is killed,
        // so it never resigns. Each tick's job carries the tick's key (task 4.3), so the test rebuilds every tick's key and
        // reads exactly which ticks were inserted. The catch-up window (3.5 s) is shorter than a tick, so normally none is
        // skipped; FR-PER-04 asks that no more than one is, which a loaded machine stretching the vacancy can still meet.
        db.SkipIfUnavailable();
        await using var ds = db.CreateDataSource();
        var (_, schema) = await MigratedAsync(ds);
        var nodes = new Dictionary<string, Node>(StringComparer.Ordinal);
        try
        {
            foreach (var id in new[] { "node-a", "node-b", "node-c" })
            {
                nodes[id] = Node.Start(db.ConnectionString, schema, id, periodic: true);
            }

            foreach (var node in nodes.Values)
            {
                await node.WaitReadyAsync(TimeSpan.FromSeconds(30));
            }

            var leader = await WaitForLeaderAsync(ds, schema, l => l is not null && nodes.ContainsKey(l), TimeSpan.FromSeconds(10));
            await WaitUntilAsync(async () => (await TicksAsync(ds, schema)).Count >= 2, TimeSpan.FromSeconds(20), "Fewer than 2 ticks");

            nodes[leader!].Kill();
            var killedAt = DateTimeOffset.UtcNow;
            var survivors = nodes.Keys.Where(id => id != leader).ToHashSet(StringComparer.Ordinal);
            await WaitForLeaderAsync(ds, schema, l => l is not null && survivors.Contains(l), TimeSpan.FromSeconds(15));
            await WaitUntilAsync(
                async () => (await TicksAsync(ds, schema)).Count(t => t.CreatedAt > killedAt) >= 3, TimeSpan.FromSeconds(30), "Fewer than 3 ticks after the kill");

            foreach (var id in survivors)
            {
                Assert.Equal(0, await nodes[id].StopAsync(TimeSpan.FromSeconds(30)));
            }
        }
        finally
        {
            foreach (var node in nodes.Values)
            {
                node.Dispose();
            }
        }

        var rows = await TicksAsync(ds, schema);
        var template = new JobInsertParams
        {
            Kind = "node_tick", ArgsJson = rows[0].ArgsJson, Queue = "node_ticks", State = JobState.Available, Priority = 1, MaxAttempts = 25,
        };
        var interval = TimeSpan.FromSeconds(4);
        var first = new DateTimeOffset(rows.Min(r => r.CreatedAt).UtcTicks / interval.Ticks * interval.Ticks, TimeSpan.Zero) - interval;
        var tickOf = new Dictionary<string, DateTimeOffset>(StringComparer.Ordinal);
        for (var tick = first; tick <= rows.Max(r => r.CreatedAt) + interval; tick += interval)
        {
            tickOf[Convert.ToHexString(KopiQueue.Execution.PeriodicEnqueuer.TickKey(template, tick))] = tick;
        }

        Assert.All(rows, r => Assert.True(tickOf.ContainsKey(r.Key), $"A job has a key that is no tick's: {r.Key}"));
        Assert.Equal(rows.Count, rows.Select(r => r.Key).Distinct(StringComparer.Ordinal).Count()); // no tick twice
        var ticks = rows.Select(r => tickOf[r.Key]).Order().ToList();
        var gaps = ticks.Zip(ticks.Skip(1), (a, b) => (int)Math.Round((b - a) / interval) - 1).ToList();
        Assert.True(gaps.All(g => g <= 1), $"Ticks skipped in a row: {string.Join(", ", gaps)} (ticks {string.Join(", ", ticks.Select(t => t.ToString("HH:mm:ss", System.Globalization.CultureInfo.InvariantCulture)))}).");
    }

    [Fact]
    public async Task A_job_left_running_by_a_killed_node_is_rescued_by_the_new_leader_and_completed_by_a_survivor()
    {
        db.SkipIfUnavailable();
        await using var ds = db.CreateDataSource();
        var (driver, schema) = await MigratedAsync(ds);
        var inserter = new KopiClient(driver);
        var executor = ((IKopiDriver)driver).GetExecutor();

        var nodes = new Dictionary<string, Node>(StringComparer.Ordinal);
        try
        {
            // node-a starts alone, so it leads and is the only node that can fetch the job.
            nodes["node-a"] = Node.Start(db.ConnectionString, schema, "node-a");
            await nodes["node-a"].WaitReadyAsync(TimeSpan.FromSeconds(30));
            await WaitForLeaderAsync(ds, schema, l => l == "node-a", TimeSpan.FromSeconds(10));
            var job = (await inserter.InsertAsync(new HangArgs(), new InsertOptions { Queue = "node" }, Ct)).Job;
            await WaitUntilAsync(
                async () => await RunningOnAsync(ds, schema, job.Id) == "node-a",
                TimeSpan.FromSeconds(10),
                "node-a did not start the job");

            foreach (var id in new[] { "node-b", "node-c" })
            {
                nodes[id] = Node.Start(db.ConnectionString, schema, id);
                await nodes[id].WaitReadyAsync(TimeSpan.FromSeconds(30));
            }

            // Once the job is older than the 3 s threshold, kill the node running it: the job stays running in the table.
            await WaitUntilAsync(async () => await AttemptAgeAsync(ds, schema, job.Id) > TimeSpan.FromSeconds(3), TimeSpan.FromSeconds(10), "The job did not age");
            nodes["node-a"].Kill();
            var survivors = new HashSet<string>(["node-b", "node-c"], StringComparer.Ordinal);

            // A survivor takes the lease once it expires, rescues the job at once, and a survivor completes it.
            await WaitUntilAsync(
                async () => (await executor.JobGetByIdAsync(job.Id, Ct))!.State == JobState.Completed,
                TimeSpan.FromSeconds(25),
                "The stuck job was not rescued and completed");

            var done = (await executor.JobGetByIdAsync(job.Id, Ct))!;
            Assert.Equal(2, done.Attempt); // the stuck attempt counted
            Assert.Equal("node-a", done.AttemptedBy[0]);
            Assert.Contains(done.AttemptedBy[^1], survivors);
            var error = Assert.Single(done.Errors);
            Assert.Equal((1, (string?)null), (error.Attempt, error.Trace));
            Assert.StartsWith("The job was still running 00:00:03 after this attempt started", error.Error, StringComparison.Ordinal);
            Assert.Contains((await LeaderAsync(ds, schema))!, survivors);

            foreach (var id in survivors)
            {
                Assert.Equal(0, await nodes[id].StopAsync(TimeSpan.FromSeconds(30)));
            }
        }
        finally
        {
            foreach (var node in nodes.Values)
            {
                node.Dispose();
            }
        }
    }

    /// <summary>The client running job <paramref name="id"/>, or <c>null</c> if it is not running.</summary>
    private static async Task<string?> RunningOnAsync(NpgsqlDataSource ds, string schema, long id)
    {
        await using var cmd = ds.CreateCommand($"SELECT attempted_by[cardinality(attempted_by)] FROM {schema}.river_job WHERE id = $1 AND state = 'running'");
        cmd.Parameters.Add(new() { Value = id });
        return await cmd.ExecuteScalarAsync(Ct) as string;
    }

    /// <summary>How long ago, by the database's clock, job <paramref name="id"/>'s attempt started.</summary>
    private static async Task<TimeSpan> AttemptAgeAsync(NpgsqlDataSource ds, string schema, long id)
    {
        await using var cmd = ds.CreateCommand($"SELECT now() - attempted_at FROM {schema}.river_job WHERE id = $1");
        cmd.Parameters.Add(new() { Value = id });
        return (TimeSpan)(await cmd.ExecuteScalarAsync(Ct))!;
    }

    /// <summary>Inserts jobs <paramref name="first"/>.. into the nodes' queue and waits until all are completed.</summary>
    /// <returns>The client that worked each job.</returns>
    private static async Task<List<string>> InsertAndWaitWorkedAsync(KopiClient inserter, NpgsqlDataSource ds, string schema, int first, int count)
    {
        var inserted = await inserter.InsertManyAsync(
            Enumerable.Range(first, count).Select(n => new InsertRequest(new NodeArgs(n), new InsertOptions { Queue = "node" })),
            cancellationToken: Ct);
        var ids = inserted.Select(r => r.Job.Id).ToArray();

        var workedBy = new List<string>();
        await WaitUntilAsync(
            async () =>
            {
                workedBy.Clear();
                await using var cmd = ds.CreateCommand(
                    $"SELECT attempted_by[cardinality(attempted_by)] FROM {schema}.river_job WHERE id = ANY($1) AND state = 'completed'");
                cmd.Parameters.Add(new() { Value = ids });
                await using var reader = await cmd.ExecuteReaderAsync(Ct);
                while (await reader.ReadAsync(Ct))
                {
                    workedBy.Add(reader.GetString(0));
                }

                return workedBy.Count == ids.Length;
            },
            TimeSpan.FromSeconds(20),
            $"Not all of jobs {first}..{first + count - 1} were completed.");
        return workedBy;
    }

    private static async Task AddStaleQueueAsync(NpgsqlDataSource ds, string schema, string name)
    {
        await using var cmd = ds.CreateCommand(
            $"INSERT INTO {schema}.river_queue (name, created_at, updated_at) VALUES ($1, now() - interval '2 days', now() - interval '2 days')");
        cmd.Parameters.Add(new() { Value = name });
        await cmd.ExecuteNonQueryAsync(Ct);
    }

    private static async Task<List<string>> QueuesAsync(NpgsqlDataSource ds, string schema)
    {
        var names = new List<string>();
        await using var cmd = ds.CreateCommand($"SELECT name FROM {schema}.river_queue");
        await using var reader = await cmd.ExecuteReaderAsync(Ct);
        while (await reader.ReadAsync(Ct))
        {
            names.Add(reader.GetString(0));
        }

        return names;
    }

    /// <summary>The periodic jobs inserted so far: their unique key in hex, args and creation time, oldest first.</summary>
    private static async Task<List<(string Key, string ArgsJson, DateTimeOffset CreatedAt)>> TicksAsync(NpgsqlDataSource ds, string schema)
    {
        await using var cmd = ds.CreateCommand(
            $"SELECT encode(unique_key, 'hex'), args::text, created_at FROM {schema}.river_job WHERE kind = 'node_tick' ORDER BY created_at");
        await using var reader = await cmd.ExecuteReaderAsync(Ct);
        var rows = new List<(string, string, DateTimeOffset)>();
        while (await reader.ReadAsync(Ct))
        {
            rows.Add((reader.GetString(0).ToUpperInvariant(), reader.GetString(1), reader.GetFieldValue<DateTimeOffset>(2)));
        }

        return rows;
    }

    private static async Task<string?> LeaderAsync(NpgsqlDataSource ds, string schema)
    {
        await using var cmd = ds.CreateCommand($"SELECT leader_id FROM {schema}.river_leader");
        return await cmd.ExecuteScalarAsync(Ct) as string;
    }

    private static async Task<string?> WaitForLeaderAsync(NpgsqlDataSource ds, string schema, Func<string?, bool> condition, TimeSpan limit)
    {
        string? leader = null;
        await WaitUntilAsync(async () => condition(leader = await LeaderAsync(ds, schema)), limit, "No expected leader");
        return leader;
    }

    private static async Task WaitUntilAsync(Func<Task<bool>> condition, TimeSpan limit, string message)
    {
        var deadline = DateTime.UtcNow + limit;
        while (!await condition())
        {
            Assert.True(DateTime.UtcNow < deadline, $"{message} after {limit}.");
            await Task.Delay(50, Ct);
        }
    }

    private static async Task<(NpgsqlDriver Driver, string Schema)> MigratedAsync(NpgsqlDataSource ds)
    {
        var schema = "k_" + Guid.NewGuid().ToString("N")[..12];
        await using (var cmd = ds.CreateCommand($"CREATE SCHEMA {schema}"))
        {
            await cmd.ExecuteNonQueryAsync(Ct);
        }

        var driver = new NpgsqlDriver(ds, new NpgsqlDriverOptions { Schema = schema });
        await driver.CreateMigrator().MigrateAsync(MigrationDirection.Up, cancellationToken: Ct);
        return (driver, schema);
    }

    /// <summary>The args <c>KopiQueue.TestNode</c> works; the kind must match its declaration.</summary>
    [JobKind("node_work")]
    public sealed record NodeArgs(int N) : IJobArgs;

    /// <summary>The args of <c>KopiQueue.TestNode</c>'s worker that hangs in its first attempt.</summary>
    [JobKind("node_hang")]
    public sealed record HangArgs : IJobArgs;

    /// <summary>One <c>KopiQueue.TestNode</c> process.</summary>
    private sealed class Node : IDisposable
    {
        private readonly Process _process;
        private readonly TaskCompletionSource _ready = new(TaskCreationOptions.RunContinuationsAsynchronously);
        private readonly List<string> _output = [];

        private Node(Process process, string id)
        {
            _process = process;
            Id = id;
        }

        public string Id { get; }

        public static Node Start(string connectionString, string schema, string id, bool periodic = false)
        {
            var start = new ProcessStartInfo(Environment.GetEnvironmentVariable("DOTNET_HOST_PATH") is { Length: > 0 } host ? host : "dotnet")
            {
                RedirectStandardInput = true,
                RedirectStandardOutput = true,
                RedirectStandardError = true,
                UseShellExecute = false,
            };
            start.ArgumentList.Add(Path.Combine(AppContext.BaseDirectory, "KopiQueue.TestNode.dll"));
            start.ArgumentList.Add(schema);
            start.ArgumentList.Add(id);
            if (periodic)
            {
                start.ArgumentList.Add("periodic");
            }
            start.Environment[NodeConnectionVariable] = connectionString;

            var node = new Node(new Process { StartInfo = start }, id);
            node._process.OutputDataReceived += (_, e) => node.OnLine(e.Data);
            node._process.ErrorDataReceived += (_, e) => node.OnLine(e.Data);
            node._process.Start();
            node._process.BeginOutputReadLine();
            node._process.BeginErrorReadLine();
            return node;
        }

        public async Task WaitReadyAsync(TimeSpan limit)
        {
            var exited = _process.WaitForExitAsync(Ct);
            var finished = await Task.WhenAny(_ready.Task, exited, Task.Delay(limit, Ct));
            Assert.True(finished == _ready.Task, $"{Id} did not start within {limit}: {Output()}");
        }

        /// <summary>Kills the process outright, as a crash or <c>kill -9</c> would.</summary>
        public void Kill()
        {
            _process.Kill(entireProcessTree: true);
            Assert.True(_process.WaitForExit(TimeSpan.FromSeconds(10)), $"{Id} did not die.");
        }

        /// <summary>Asks the node to stop cleanly and returns its exit code.</summary>
        public async Task<int> StopAsync(TimeSpan limit)
        {
            await _process.StandardInput.WriteLineAsync("stop");
            await _process.StandardInput.FlushAsync(Ct);
            using var timeout = CancellationTokenSource.CreateLinkedTokenSource(Ct);
            timeout.CancelAfter(limit);
            await _process.WaitForExitAsync(timeout.Token);
            Assert.True(_process.ExitCode == 0, $"{Id} exited with {_process.ExitCode}: {Output()}");
            return _process.ExitCode;
        }

        public void Dispose()
        {
            try
            {
                if (!_process.HasExited)
                {
                    _process.Kill(entireProcessTree: true);
                    _process.WaitForExit(TimeSpan.FromSeconds(10));
                }
            }
            catch (InvalidOperationException)
            {
                // Never started.
            }

            _process.Dispose();
        }

        private void OnLine(string? line)
        {
            if (line is null)
            {
                return;
            }

            lock (_output)
            {
                _output.Add(line);
            }

            if (line == "ready")
            {
                _ready.TrySetResult();
            }
        }

        private string Output()
        {
            lock (_output)
            {
                return string.Join(Environment.NewLine, _output);
            }
        }
    }
}
