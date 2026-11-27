using KopiQueue;
using KopiQueue.Npgsql;
using Npgsql;

// A working KopiQueue client in its own process, so a test can kill it outright: no resignation, no clean stop.
//
//   KopiQueue.TestNode <schema> <client-id> [periodic]
//
// The connection string comes from KOPI_NODE_PG, so its password never shows on a command line. The node works the
// "node" queue, prints "ready" once started, and stops cleanly when its standard input closes or reads "stop".
// Jobs time out after 2 s and are taken for stuck after 3 s, so a test can watch the rescuer (task 3.4) at work. With
// "periodic", the node also has a periodic job every 4 s, inserted into the "node_ticks" queue, which no node works
// (task 4.6): with a 3 s lease and a 500 ms elect interval, its catch-up window is 3.5 s, shorter than a tick.
var (schema, clientId, periodic) = args switch
{
    [var s, var c] => (s, c, false),
    [var s, var c, "periodic"] => (s, c, true),
    _ => (null, null, false),
};
if (schema is null || clientId is null || Environment.GetEnvironmentVariable("KOPI_NODE_PG") is not { Length: > 0 } connectionString)
{
    await Console.Error.WriteLineAsync("usage: KOPI_NODE_PG=<connection string> KopiQueue.TestNode <schema> <client-id> [periodic]");
    return 2;
}

await using var dataSource = NpgsqlDataSource.Create(connectionString);
var client = new KopiClient(new NpgsqlDriver(dataSource, new NpgsqlDriverOptions { Schema = schema }), new KopiClientOptions
{
    ClientId = clientId,
    Queues = new Dictionary<string, QueueOptions>(StringComparer.Ordinal) { ["node"] = new() { MaxWorkers = 2 } },
    Workers = new WorkerRegistry().Add(new NodeWorker()).Add(new HangingWorker()),
    JobTimeout = TimeSpan.FromSeconds(2),
    RescueStuckJobsAfter = TimeSpan.FromSeconds(3),
    ElectInterval = TimeSpan.FromMilliseconds(500),
    LeaderTtl = TimeSpan.FromSeconds(3),
    FetchPollInterval = TimeSpan.FromMilliseconds(500),
    PeriodicJobs = periodic
        ? [PeriodicJob.Create(PeriodicSchedule.Every(TimeSpan.FromSeconds(4)), () => new TickArgs(), new InsertOptions { Queue = "node_ticks" })]
        : null,
});

await client.StartAsync();
Console.WriteLine("ready");

while (await Console.In.ReadLineAsync() is { } line && line != "stop")
{
}

await client.StopAsync();
return 0;

/// <summary>The periodic job's args; the test declares the same kind.</summary>
[JobKind("node_tick")]
internal sealed record TickArgs : IJobArgs;

/// <summary>The args the test inserts; the test declares the same kind.</summary>
[JobKind("node_work")]
internal sealed record NodeArgs(int N) : IJobArgs;

internal sealed class NodeWorker : Worker<NodeArgs>
{
    public override Task WorkAsync(Job<NodeArgs> job, CancellationToken cancellationToken) => Task.CompletedTask;
}

/// <summary>Hangs in its first attempt, even past its timeout, as a worker of a crashed process would; completes after.</summary>
[JobKind("node_hang")]
internal sealed record HangArgs : IJobArgs;

internal sealed class HangingWorker : Worker<HangArgs>
{
    public override Task WorkAsync(Job<HangArgs> job, CancellationToken cancellationToken) =>
        job.Attempt == 1 ? Task.Delay(System.Threading.Timeout.Infinite, CancellationToken.None) : Task.CompletedTask;
}
