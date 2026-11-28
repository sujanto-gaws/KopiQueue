using KopiQueue.Benchmarks;

// kopi-bench sqlserver [--rcsi off] [--quick] [--keep] [--only fetch,insert,e2e,dq6,dq7,latency]
// Creates a throwaway database on the server in KOPI_BENCH_MSSQL (falling back to KOPI_TEST_MSSQL), measures, and
// drops it. The report is Markdown on standard output.
if (args.Length == 0 || args[0] != "sqlserver")
{
    await Console.Error.WriteLineAsync("usage: kopi-bench sqlserver [--rcsi off] [--quick] [--keep] [--only fetch,insert,e2e,dq6,dq7,latency]");
    return 2;
}

var connectionString = Environment.GetEnvironmentVariable("KOPI_BENCH_MSSQL") ?? Environment.GetEnvironmentVariable("KOPI_TEST_MSSQL");
if (string.IsNullOrWhiteSpace(connectionString))
{
    await Console.Error.WriteLineAsync("Set KOPI_BENCH_MSSQL (or KOPI_TEST_MSSQL) to a SQL Server connection string.");
    return 2;
}

var settings = new BenchSettings
{
    Rcsi = !(args.SkipWhile(a => a != "--rcsi").Skip(1).FirstOrDefault() is "off"),
    Quick = args.Contains("--quick"),
    Keep = args.Contains("--keep"),
    Only = args.SkipWhile(a => a != "--only").Skip(1).FirstOrDefault()?.Split(',') ?? [],
};

await using var database = await BenchDatabase.CreateAsync(connectionString, settings);
await new SqlServerBenchmarks(database, settings).RunAsync();
return 0;
