using KopiQueue;
using KopiQueue.Npgsql;
using Npgsql;

// A working KopiQueue client in its own process, so a test can kill it outright: no resignation, no clean stop.
//
//   KopiQueue.TestNode <schema> <client-id>
//
// The connection string comes from KOPI_NODE_PG, so its password never shows on a command line. The node works the
// "node" queue, prints "ready" once started, and stops cleanly when its standard input closes or reads "stop".
if (args is not [var schema, var clientId] || Environment.GetEnvironmentVariable("KOPI_NODE_PG") is not { Length: > 0 } connectionString)
{
    await Console.Error.WriteLineAsync("usage: KOPI_NODE_PG=<connection string> KopiQueue.TestNode <schema> <client-id>");
    return 2;
}

await using var dataSource = NpgsqlDataSource.Create(connectionString);
var client = new KopiClient(new NpgsqlDriver(dataSource, new NpgsqlDriverOptions { Schema = schema }), new KopiClientOptions
{
    ClientId = clientId,
    Queues = new Dictionary<string, QueueOptions>(StringComparer.Ordinal) { ["node"] = new() { MaxWorkers = 2 } },
    Workers = new WorkerRegistry().Add(new NodeWorker()),
    ElectInterval = TimeSpan.FromMilliseconds(500),
    LeaderTtl = TimeSpan.FromSeconds(3),
    FetchPollInterval = TimeSpan.FromMilliseconds(500),
});

await client.StartAsync();
Console.WriteLine("ready");

while (await Console.In.ReadLineAsync() is { } line && line != "stop")
{
}

await client.StopAsync();
return 0;

/// <summary>The args the test inserts; the test declares the same kind.</summary>
[JobKind("node_work")]
internal sealed record NodeArgs(int N) : IJobArgs;

internal sealed class NodeWorker : Worker<NodeArgs>
{
    public override Task WorkAsync(Job<NodeArgs> job, CancellationToken cancellationToken) => Task.CompletedTask;
}
