using System.Collections.Concurrent;
using System.Diagnostics;
using KopiQueue.Drivers;

namespace KopiQueue.Benchmarks;

/// <summary>
/// SQL Server measurements through the driver's own statements (plan task 5.9, NFR-PERF-07). Each step reports its
/// round trips from the client and its cost on the server, from the plan cache.
/// </summary>
internal sealed class SqlServerBenchmarks(BenchDatabase db, BenchSettings settings)
{
    private const int FetchSize = 25;

    public async Task RunAsync()
    {
        Report.Line($"# KopiQueue SQL Server benchmarks");
        Report.Line("");
        Report.Line($"- Server: {await db.ScalarAsync<string>("SELECT CAST(SERVERPROPERTY('ProductVersion') AS nvarchar(50)) + N' ' + CAST(SERVERPROPERTY('Edition') AS nvarchar(100))")}");
        Report.Line($"- READ_COMMITTED_SNAPSHOT: {(settings.Rcsi ? "on" : "off")}; {Environment.ProcessorCount} logical processors on the client; {(settings.Quick ? "quick run (a tenth of the rows)" : "full run")}");

        var history = settings.Scale(200_000);
        var busy = settings.Scale(50_000);
        await db.SeedAsync(history, "completed", "history");
        await db.SeedAsync(busy, "available", "busy");
        Report.Line($"- Table: {Report.N(history)} completed jobs (history) and {Report.N(busy)} available jobs in queue `busy`");

        if (settings.Runs("fetch"))
        {
            await FetchAsync();
        }

        if (settings.Runs("insert"))
        {
            await InsertAsync();
        }

        if (settings.Runs("e2e"))
        {
            await EndToEndAsync(200);
            await EndToEndAsync(1_000);
        }

        var questions = new SqlServerDesignQuestions(db, settings);
        if (settings.Runs("dq6"))
        {
            await questions.BatchParametersAsync();
        }

        if (settings.Runs("dq7"))
        {
            await questions.StateColumnTypeAsync();
        }

        if (settings.Runs("latency"))
        {
            // Arrivals are spread over the poll interval, so the median needs many samples: with 15, its standard
            // error is about an eighth of the interval.
            await PickupLatencyAsync(TimeSpan.FromMilliseconds(100), settings.Quick ? 10 : 100);
            await PickupLatencyAsync(TimeSpan.FromSeconds(1), settings.Quick ? 5 : 60);
        }
    }

    /// <summary>An empty poll, then fetching and completing batches of 25 from a busy queue.</summary>
    private async Task FetchAsync()
    {
        var executor = db.Driver.GetExecutor();

        Report.Heading("Empty poll");
        var polls = settings.Scale(2_000);
        var before = await db.SnapshotAsync();
        var times = await TimeAsync(polls, () => executor.JobGetAvailableAsync(Fetch("idle"), CancellationToken.None));
        Report.Line($"Round trip: {Report.Distribution(times)}\n");
        Statements((await db.SnapshotAsync()).Since(before));

        Report.Heading($"Fetch {FetchSize} from a busy queue, then complete them");
        var fetches = settings.Scale(200);
        var batches = new List<IReadOnlyList<JobRow>>(fetches);
        before = await db.SnapshotAsync();
        times = await TimeAsync(fetches, async () => batches.Add(await executor.JobGetAvailableAsync(Fetch("busy"), CancellationToken.None)));
        var afterFetch = await db.SnapshotAsync();
        Report.Line($"Fetch round trip: {Report.Distribution(times)}; {Report.N(batches.Sum(b => b.Count))} jobs");

        var next = 0;
        times = await TimeAsync(batches.Count, () => executor.JobSetStateIfRunningManyAsync(
            [.. batches[next++].Select(j => new JobSetStateParams { Id = j.Id, State = JobState.Completed, FinalizedAt = DateTimeOffset.UtcNow })],
            CancellationToken.None));
        Report.Line($"Complete round trip: {Report.Distribution(times)}\n");
        Report.Line("Fetch:\n");
        var fetched = batches.Sum(b => b.Count);
        Statements(afterFetch.Since(before), perRow: fetched);
        Report.Line("Complete:\n");
        Statements((await db.SnapshotAsync()).Since(afterFetch), perRow: fetched);
    }

    /// <summary><c>JobInsertFastAsync</c> and <c>JobInsertManyAsync</c> in batches of 1,000.</summary>
    private async Task InsertAsync()
    {
        var executor = db.Driver.GetExecutor();
        var batch = Enumerable.Range(0, 1_000).Select(i => NoOp("bulk", i)).ToArray();

        foreach (var (name, insert) in new (string, Func<Task>)[]
        {
            ("JobInsertFastAsync", () => executor.JobInsertFastAsync(batch, CancellationToken.None)),
            ("JobInsertManyAsync", () => executor.JobInsertManyAsync(batch, CancellationToken.None)),
        })
        {
            Report.Heading($"Bulk insert: {name}, 1,000 jobs a call");
            await insert(); // warm up: plan compilation and the pool
            var calls = settings.Scale(50);
            var before = await db.SnapshotAsync();
            var watch = Stopwatch.StartNew();
            var times = await TimeAsync(calls, insert);
            watch.Stop();
            Report.Line($"{Report.N(calls * batch.Length / watch.Elapsed.TotalSeconds)} jobs/s, args of about 60 bytes; target ≥ 20,000 jobs/s (NFR-PERF-07); call: {Report.Distribution(times)}\n");
            Statements((await db.SnapshotAsync()).Since(before), perRow: calls * batch.Length);
        }
    }

    /// <summary>One client works pre-inserted no-op jobs: jobs per second until the last is completed in the table.</summary>
    private async Task EndToEndAsync(int maxWorkers)
    {
        var queue = $"e2e_{maxWorkers}";
        var count = settings.Scale(50_000);
        Report.Heading($"End to end: one client, {Report.N(maxWorkers)} workers, {Report.N(count)} no-op jobs");

        var executor = db.Driver.GetExecutor();
        foreach (var chunk in Enumerable.Range(0, count).Chunk(1_000))
        {
            await executor.JobInsertFastAsync([.. chunk.Select(i => NoOp(queue, i))], CancellationToken.None);
        }

        var worker = new NoOpWorker();
        var before = await db.SnapshotAsync();
        var watch = Stopwatch.StartNew();
        var client = Client(queue, maxWorkers, worker, pollInterval: null);
        await client.StartAsync();
        try
        {
            while (await db.ScalarAsync<int>($"""
                SELECT (SELECT COUNT(*) FROM {BenchDatabase.Job} WHERE state = 'available' AND queue = @queue)
                     + (SELECT COUNT(*) FROM {BenchDatabase.Job} WHERE state = 'running' AND queue = @queue)
                """, ("@queue", queue)) > 0)
            {
                await Task.Delay(50);
            }

            watch.Stop();
        }
        finally
        {
            await client.StopAsync();
        }

        Report.Line($"{Report.N(count / watch.Elapsed.TotalSeconds)} jobs/s ({Report.N(count)} in {Report.N(watch.Elapsed.TotalSeconds, 2)} s); {Report.N(worker.Worked)} worked\n");
        Statements((await db.SnapshotAsync()).Since(before), perRow: count, top: 6);
    }

    /// <summary>A polling client; jobs inserted elsewhere, as another process would. Insert committed → work started.</summary>
    private async Task PickupLatencyAsync(TimeSpan pollInterval, int samples)
    {
        var queue = $"lat_{(int)pollInterval.TotalMilliseconds}";
        Report.Heading($"Pickup latency: poll every {Report.N(pollInterval.TotalMilliseconds)} ms, jobs inserted by another connection");

        var worker = new NoOpWorker { RecordStarts = true };
        var committed = new ConcurrentDictionary<long, long>();
        var executor = db.Driver.GetExecutor();
        var client = Client(queue, 10, worker, pollInterval);
        await client.StartAsync();
        try
        {
            await Task.Delay(pollInterval * 2);
            for (var i = 0; i < samples; i++)
            {
                await Task.Delay(Random.Shared.Next((int)(pollInterval.TotalMilliseconds / 2), (int)(pollInterval.TotalMilliseconds * 2)));
                var row = (await executor.JobInsertManyAsync([NoOp(queue, i)], CancellationToken.None))[0].Job;
                committed[row.Id] = Stopwatch.GetTimestamp(); // autocommit: committed when the call returns
            }

            await Task.Delay(pollInterval * 3);
        }
        finally
        {
            await client.StopAsync();
        }

        var latencies = committed
            .Where(c => worker.Started.ContainsKey(c.Key))
            .Select(c => Math.Max(0, Stopwatch.GetElapsedTime(c.Value, worker.Started[c.Key]).TotalMilliseconds))
            .ToArray();
        // A job arrives at a random point of the poll cycle, so the wait is spread evenly over the interval: the mean is
        // the steadier estimate of the centre (its standard error is the interval / √(12 n); the median's is larger).
        Report.Line($"{Report.Distribution(latencies)}; mean {Report.N(latencies.DefaultIfEmpty().Average(), 1)} ms; " +
                    $"target p50 ≤ {Report.N(pollInterval.TotalMilliseconds / 2 + 30)} ms (NFR-PERF-07)");
    }

    /// <summary>A client working <paramref name="queue"/>: polling every second unless told otherwise, as by default.</summary>
    private KopiClient Client(string queue, int maxWorkers, NoOpWorker worker, TimeSpan? pollInterval) =>
        new(db.Driver, new KopiClientOptions
        {
            Queues = new Dictionary<string, QueueOptions> { [queue] = new() { MaxWorkers = maxWorkers } },
            Workers = new WorkerRegistry().Add(worker),
            ClientId = "bench",
            FetchPollInterval = pollInterval ?? TimeSpan.FromSeconds(1),
        });

    private static JobGetAvailableParams Fetch(string queue) => new() { Queue = queue, Max = FetchSize, ClientId = "bench" };

    private static JobInsertParams NoOp(string queue, int i) => new()
    {
        Kind = NoOpArgs.Kind,
        ArgsJson = Args(i),
        Queue = queue,
        State = JobState.Available,
        Priority = 1,
        MaxAttempts = 25,
    };

    /// <summary>Args of about 60 bytes, the size NFR-PERF-07's insert target is stated for.</summary>
    public static string Args(int i) => $$"""{"n":{{i}},"to":"user{{i}}@example.com","template":"welcome"}""";

    private static async Task<List<double>> TimeAsync(int count, Func<Task> action)
    {
        var times = new List<double>(count);
        for (var i = 0; i < count; i++)
        {
            var start = Stopwatch.GetTimestamp();
            await action();
            times.Add(Stopwatch.GetElapsedTime(start).TotalMilliseconds);
        }

        return times;
    }

    /// <summary>Per-execution server cost of each statement; with <paramref name="perRow"/>, the step's jobs, also reads per job.</summary>
    private static void Statements(IReadOnlyList<(string Statement, StatementCost Cost)> statements, int? perRow = null, int top = 4)
    {
        Report.Table(
            ["Statement", "Executions", "Logical reads / execution", "Reads / job", "CPU µs / execution", "Plan operators"],
            statements.Take(top).Select(s => (IReadOnlyList<string>)
            [
                $"`{s.Statement.Replace("|", "\\|", StringComparison.Ordinal)}`",
                Report.N(s.Cost.Executions),
                Report.N((double)s.Cost.Reads / s.Cost.Executions, 1),
                perRow is { } rows ? Report.N((double)s.Cost.Reads / rows, 1) : "",
                Report.N((double)s.Cost.CpuMicroseconds / s.Cost.Executions),
                s.Cost.Operators,
            ]));
    }
}
