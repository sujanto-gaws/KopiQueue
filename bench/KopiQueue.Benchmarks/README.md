# KopiQueue benchmarks

Macro benchmarks against a real database: what a job costs the server, and how fast KopiQueue goes end to end. They
are the measurements behind the performance requirements (NFR-PERF-*) and the reports in `docs/benchmarks/`.
BenchmarkDotNet micro benchmarks (allocation per job, NFR-PERF-05) will join them later (plan task 7.1).

## SQL Server

```sh
export KOPI_BENCH_MSSQL="Server=localhost;Integrated Security=true;TrustServerCertificate=true"  # or KOPI_TEST_MSSQL
dotnet run --project bench/KopiQueue.Benchmarks -c Release -- sqlserver > report.md
```

It creates a database named `kopi_bench_<UTC time>` on that server, migrates it, measures, and drops it. The login
needs `CREATE DATABASE` and `VIEW SERVER STATE`, for the plan-cache statistics. A full run takes about five minutes.

Options:

- `--only fetch,insert,e2e,dq6,dq7,latency`: run only those steps.
- `--quick`: a tenth of the rows and repetitions, for a smoke run.
- `--rcsi off`: READ_COMMITTED_SNAPSHOT off. It is on by default, as on Azure SQL Database.
- `--keep`: keep the database, to look at it afterwards. Drop it yourself.

The steps:

| Step | What it measures |
|------|------------------|
| `fetch` | An empty poll, then fetching 25 jobs at a time from a busy queue and completing them, through the driver |
| `insert` | `JobInsertFastAsync` and `JobInsertManyAsync`, 1,000 jobs a call |
| `e2e` | One client working 50,000 pre-inserted no-op jobs, with 200 and with 1,000 workers |
| `dq6` | Design question 6: the driver's OPENJSON insert against the same insert fed by a table-valued parameter |
| `dq7` | Design question 7: `state` as `varchar(16)` or `tinyint`, on two tables that differ only in that column |
| `latency` | Pickup latency of a client polling every 100 ms and every 1 s, for jobs another connection inserts |

Server costs come from the plan cache (`sys.dm_exec_query_stats`), as deltas over each step: logical reads and CPU per
execution, reads per job, and the plan's operators. During the end-to-end runs, statements recompile as the table
changes and evicted entries take their counts with them, so those tables show where the work goes rather than exact
totals. The fetch and insert steps don't have that problem.

The numbers depend on the machine. Compare runs on the same one, and read the server version and settings at the top
of each report.
