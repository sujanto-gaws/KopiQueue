# SQL Server benchmarks, M5 (task 5.9)

**Issue:** [#55](https://github.com/sujanto-gaws/KopiQueue/issues/55) · **Tool:** [`bench/KopiQueue.Benchmarks`](../../bench/KopiQueue.Benchmarks/README.md) · **Raw report:** [sqlserver-m5-raw.md](sqlserver-m5-raw.md) · **Requirements:** NFR-PERF-07

Task 5.9 covers four things:

- Re-measure spike 0.10's SQL Server numbers through the real driver.
- Reduce the busy-fetch cost.
- Settle DQ-6 (OPENJSON or table-valued parameters) and DQ-7 (the `state` column's type).
- Record the first SQL Server baseline against NFR-PERF-07.

## Setup

- **Server and client:** SQL Server 2019 Developer (15.0.2210.4) on the development laptop, 16 logical processors, with the client on the same machine.
- **Database:** READ_COMMITTED_SNAPSHOT on, as on Azure SQL Database.
- **Data:** 200,000 completed jobs and 50,000 available jobs in a throwaway database, migrated by the driver.
- **Measurement:** every statement is the driver's own, called through `IKopiExecutor` or a working `KopiClient`. Server costs are plan-cache deltas over each step (`sys.dm_exec_query_stats`). The investigation below used `SET STATISTICS IO` on the same statements.
- **Noise:** this is a laptop, so the numbers vary between runs by up to about 20% (bulk insert ran at 24,900 and 30,500 jobs/s in two runs of the same code). `bench.yml` runs the benchmarks weekly on SQL Server 2019, 2022 and 2025 on GitHub's runners. Its reports, in the job summaries, are the numbers to track from now on.

## NFR-PERF-07

| Target | Result | |
|--------|--------|---|
| Empty poll ≤ 10 logical reads | 4 reads, about 0.1 ms of CPU; p50 round trip 0.4 ms | Met |
| End-to-end no-op throughput ≥ 5,000 jobs/s per client | 7,900 jobs/s with 1,000 workers; 1,800 with 200 | Met with enough workers |
| Batched insert ≥ 30,000 jobs/s | 25,000 to 30,500 jobs/s with `{"n":1}` args; about 20,000 with 60-byte args | **Not met** (see DQ-6) |
| Cross-process pickup p50 ≤ interval / 2 + 30 ms | 100 ms poll: p50 61 ms (target 80). 1 s poll: p50 497 ms, mean 505 ms (target 530) | Met |

- **Workers bound end-to-end throughput.** A producer waits `FetchCooldown` (100 ms) between fetches, so one queue fetches at most ten times a second. With 200 workers the ceiling is 2,000 jobs/s, and 1,800 was measured. 5,000 jobs/s needs at least 500 workers on the queue, or a shorter cooldown.
- **Pickup latency needs many samples.** A job arrives at a random point of the poll cycle, so its wait is spread evenly over the interval. With 15 samples, the median's standard error is about an eighth of the interval: two such runs gave 418 and 546 ms at 1 s. The benchmark now takes 100 and 60 samples and also reports the mean, whose standard error is smaller.

### On CI (GitHub runners)

These numbers come from `bench.yml` on this task's pull request ([#157](https://github.com/sujanto-gaws/KopiQueue/pull/157)), with one run per version. The runners are shared machines, so their speeds are noisy. The logical reads are not: fetch and complete cost 545 and 554 reads per 25 jobs on every version.

| | 2019 (15.0.4510) | 2022 (16.0.4325) | 2025 (17.0.5065) |
|---|---|---|---|
| Fast insert, `{"n":1}` args, jobs/s | 52,200 | 29,900 | 27,100 |
| End to end, 1,000 workers, jobs/s | 9,000 | 7,300 | 7,000 |
| Pickup, 100 ms poll: p50 / mean | 56 / 54 ms | 53 / 54 ms | 57 / 58 ms |
| Pickup, 1 s poll: p50 / mean | 502 / 510 ms | 431 / 473 ms | 449 / 477 ms |
| DQ-6, 1,000 jobs: OPENJSON / table-valued, jobs/s | 41,700 / 60,300 | 21,900 / 29,500 | 21,500 / 29,500 |
| DQ-6, 100 jobs: OPENJSON / table-valued, jobs/s | 19,200 / 22,400 | 15,600 / 15,800 | 11,800 / 10,600 |

## Fetch and complete cost

Spike 0.10 measured about 950 logical reads to fetch 25 jobs, on a simplified table of its own. Through the driver, before this task:

| Statement, 25 jobs | Before | After | Change |
|--------------------|--------|-------|--------|
| Fetch (`job_get_available.sql`) | 750 reads (30 a job), 2.9 ms CPU | 545 reads (22 a job), 2.5 ms CPU | −27% |
| Complete (`job_set_state_if_running_many.sql`) | 784 + 125 reads (36 a job), 2.6 ms CPU | 554 reads (22 a job), 2.0 ms CPU | −39% |
| Bulk insert, per job | 13.6 reads | 10.6 reads | −22% |

**What the reads were.** The fetch's cost is linear in the batch: 17.4 reads a job on the job table and 6.1 on worktables, with almost nothing fixed. (Fetches of 1, 5, 25, 100, 400 and 1,000 jobs were measured.) Two things were tried and ruled out:

- **Ghost records.** Each fetch removes rows from the front of the filtered fetch index, but the reads did not grow over 60 fetches in a row.
- **The `attempted_by` rewrite.** Replacing it with a plain `JSON_MODIFY` append moved reads between the job table and the worktables, with no net saving.

**What changed:**

1. **The unique index covers only jobs with a key** (`WHERE unique_key IS NOT NULL`, migration 001).
   - `unique_slot` is computed from `state`, so every state change of every job deleted and re-inserted its entry, about 6 reads a job on each fetch and complete, and more on insert. Jobs without a key now have no entry.
   - Uniqueness is unchanged: a job that doesn't hold its key had `unique_slot = id` and could never collide anyway.
   - The two lookups that force a seek on this index (`job_insert_unique.sql`, `job_schedule.sql`) now state `unique_key IS NOT NULL`, so the filtered index matches them.
   - The SQL Server schema hasn't been released, so migration 001 changed in place.
2. **The complete statement skips its second query when every job changed.**
   - It returns the jobs it didn't change (rescued, cancelled or deleted meanwhile) from a second `SELECT`, which read all 25 jobs again even when every one had changed. It now runs only when `@@ROWCOUNT` is below the batch's size.
   - The table of changed IDs became a heap, saving 2 reads per changed job.

**Tried and not taken:**

- **A lock-then-update pair of statements** (select the IDs with `UPDLOCK, READPAST`, then update by ID in one transaction) cost the same reads. The worktables come from the wide update's index maintenance, not from Halloween protection.
- **A plain-append `attempted_by`** saved nothing either (see above).

**What is left** is inherent to moving a job between states: the clustered row, the delete from one filtered index and the insert into another, and the update's worktables. PostgreSQL does the same work in its own way (heap tuple and index entries).

## DQ-6: OPENJSON or table-valued parameters

The driver's OPENJSON insert was compared against the same `INSERT ... SELECT` fed by a table-valued parameter.
- The rows were streamed as `SqlDataRecord`s, so no `DataTable` was built.
- Both sides include building the parameter on the client.
- The calls alternated, so neither side ran with a warmer cache.
- The args were about 60 bytes (`{"n":1,"to":"user1@example.com","template":"welcome"}`).

| Jobs a statement | Parameter | Jobs/s | p50 call | Server CPU / statement | Reads / statement |
|------------------|-----------|--------|----------|------------------------|-------------------|
| 100 | OPENJSON | 13,700 | 7.1 ms | 5.5 ms | 2,286 |
| 100 | Table-valued | 15,100 | 6.3 ms | 2.7 ms | 1,003 |
| 1,000 | OPENJSON | 19,800 | 50.5 ms | 43.9 ms | 21,949 |
| 1,000 | Table-valued | 29,500 | 33.5 ms | 22.2 ms | 9,676 |

A second run gave the same picture: 21,500 against 30,400 jobs/s at 1,000 jobs.

- **Speed:** a table-valued parameter is about 1.4–1.5 times faster for large batches, at half the server CPU and less than half the reads. On CI the gain at 1,000 jobs was 1.35–1.45 times on all three versions. At 100 jobs a statement there's no clear winner: about 10% faster locally, and between 10% slower and 17% faster on CI.
- **Cost of switching:** a user-defined table type in the schema, created by the migration under the schema's prefix. The application's login also needs `EXECUTE` on that type, which `db_datareader` and `db_datawriter` don't grant.
- **Decision:** left to the owner. OPENJSON stays the default (ADR-014) until then. With realistic args, it is the reason batched insert misses NFR-PERF-07.

## DQ-7: `state` as `varchar(16)` or `tinyint`

The comparison used two tables that differ only in the `state` column. Each had the job table's indexes that involve `state`, the same 200,000 completed and 50,000 available rows, and the driver's fetch and complete reduced to those columns, alternating.

| `state` | Average row | Pages: table, fetch, running, finalized | Fetch 25: reads, CPU | Complete 25: reads, CPU |
|---------|-------------|------------------------------------------|----------------------|-------------------------|
| `varchar(16)` | 123 bytes | 3,760, 316, 25, 1,258 | 448, 1.7 ms | 203, 0.6 ms |
| `tinyint` | 113 bytes | 3,450, 316, 25, 950 | 448, 1.6 ms | 202, 0.5 ms |

- **Fetching and completing cost the same.** The fetch index is filtered on `state`, but `state` isn't in its key.
- **`tinyint` is smaller:** about 8% of the table, and a quarter of the finalized index, the only index with `state` in its key.
- **Decision: `varchar(16)`.** That saving doesn't pay for state values nobody can read in an ad-hoc query, or for a schema that differs from PostgreSQL's in yet another way. DQ-7 is closed.

## Limitations

- **One machine, a single run of each configuration,** with the client on the same machine as the server.
- **Not in the benchmark:** Azure SQL Database (task 7.4), lock-memory pressure, and tables beyond 250,000 rows.
- **End-to-end statement counts are approximate.** Statements recompile as the table changes, and an evicted plan-cache entry takes its counts with it. So the end-to-end statement tables in the raw report show where the work goes rather than exact totals. The fetch, complete and insert steps don't have that problem.
