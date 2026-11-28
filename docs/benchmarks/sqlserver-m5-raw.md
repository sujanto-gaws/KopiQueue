# KopiQueue SQL Server benchmarks: raw report, M5 (task 5.9)

The output of `dotnet run --project bench/KopiQueue.Benchmarks -c Release -- sqlserver` on the development machine, 2026-11-28, after the changes of task 5.9. Summary and analysis: [sqlserver-m5.md](sqlserver-m5.md).

- Server: 15.0.2210.4 Developer Edition (64-bit)
- READ_COMMITTED_SNAPSHOT: on; 16 logical processors on the client; full run
- Table: 200,000 completed jobs (history) and 50,000 available jobs in queue `busy`

## Empty poll

Round trip: p50 0.4 ms, p95 0.9 ms, p99 1.2 ms, max 404.6 ms (2000 samples)

| Statement | Executions | Logical reads / execution | Reads / job | CPU µs / execution | Plan operators |
|---|---|---|---|---|---|
| `WITH locked AS ( SELECT TOP (@max) * FROM [dbo].kopi_job WITH (UPDLOCK … .scheduled_at, inserted.finalized_at` | 2,000 | 4.0 |  | 101 | Assert ×1, Clustered Index Seek ×1, Clustered Index Update ×1, Collapse ×1, Compute Scalar ×6, Concatenation ×1, Constant Scan ×1, Filter ×3, Index Seek ×1, Index Update ×1, Nested Loops ×2, Segment ×1, Sequence ×1, Sequence Project ×1, Sort ×3, Split ×1, Stream Aggregate ×1, Table Spool ×4, Table-valued function ×1, Top ×2 |
| `SELECT CAST(SESSIONPROPERTY('ANSI_NULLS') AS int), CAST(SESSIONPROPERT … WHERE database_id = DB_ID()) AS int)` | 1 | 10.0 |  | 157 | Assert ×1, Clustered Index Seek ×4, Compute Scalar ×1, Constant Scan ×1, Filter ×2, Nested Loops ×7, Row Count Spool ×2, Stream Aggregate ×1, Table-valued function ×3 |


## Fetch 25 from a busy queue, then complete them

Fetch round trip: p50 3.8 ms, p95 5.3 ms, p99 5.7 ms, max 14.4 ms (200 samples); 5,000 jobs
Complete round trip: p50 4.5 ms, p95 5.7 ms, p99 6.2 ms, max 189.6 ms (200 samples)

Fetch:

| Statement | Executions | Logical reads / execution | Reads / job | CPU µs / execution | Plan operators |
|---|---|---|---|---|---|
| `WITH locked AS ( SELECT TOP (@max) * FROM [dbo].kopi_job WITH (UPDLOCK … .scheduled_at, inserted.finalized_at` | 200 | 545.2 | 21.8 | 2,478 | Assert ×1, Clustered Index Seek ×1, Clustered Index Update ×1, Collapse ×1, Compute Scalar ×6, Concatenation ×1, Constant Scan ×1, Filter ×3, Index Seek ×1, Index Update ×1, Nested Loops ×2, Segment ×1, Sequence ×1, Sequence Project ×1, Sort ×3, Split ×1, Stream Aggregate ×1, Table Spool ×4, Table-valued function ×1, Top ×2 |

Complete:

| Statement | Executions | Logical reads / execution | Reads / job | CPU µs / execution | Plan operators |
|---|---|---|---|---|---|
| `UPDATE j SET attempt = CASE WHEN j.state = 'running' AND c.cancel = 0  …  'running' OR u.metadata IS NOT NULL` | 200 | 554.0 | 22.2 | 1,866 | Assert ×1, Clustered Index Seek ×1, Clustered Index Update ×1, Collapse ×1, Compute Scalar ×8, Concatenation ×1, Constant Scan ×1, Filter ×5, Index Update ×1, Nested Loops ×5, Sequence ×1, Sort ×2, Split ×1, Stream Aggregate ×1, Table Insert ×1, Table Spool ×3, Table-valued function ×5 |
| `IF @@ROWCOUNT < (SELECT COUNT(*) FROM OPENJSON(@batch))` | 200 | 0.0 | 0.0 | 120 | Compute Scalar ×2, Stream Aggregate ×1, Table-valued function ×1 |


## Bulk insert: JobInsertFastAsync, 1,000 jobs a call

24,929 jobs/s; call: p50 39.6 ms, p95 52.4 ms, p99 60.0 ms, max 60.0 ms (50 samples)

| Statement | Executions | Logical reads / execution | Reads / job | CPU µs / execution | Plan operators |
|---|---|---|---|---|---|
| `INSERT INTO [dbo].kopi_job (args, created_at, kind, max_attempts, meta … max) '$.t' ) AS i ORDER BY i.ordinal` | 50 | 10,614.0 | 10.6 | 33,504 | Assert ×1, Clustered Index Insert ×1, Compute Scalar ×6, Sort ×1, Table-valued function ×1 |


## Bulk insert: JobInsertManyAsync, 1,000 jobs a call

25,288 jobs/s; call: p50 37.6 ms, p95 49.7 ms, p99 74.4 ms, max 74.4 ms (50 samples)

| Statement | Executions | Logical reads / execution | Reads / job | CPU µs / execution | Plan operators |
|---|---|---|---|---|---|
| `INSERT INTO [dbo].kopi_job (args, created_at, kind, max_attempts, meta … max) '$.t' ) AS i ORDER BY i.ordinal` | 50 | 10,613.9 | 10.6 | 33,303 | Assert ×1, Clustered Index Insert ×1, Compute Scalar ×6, Sort ×1, Table-valued function ×1 |


## End to end: one client, 200 workers, 50,000 no-op jobs

1,817 jobs/s (50,000 in 27.51 s); 50,000 worked

| Statement | Executions | Logical reads / execution | Reads / job | CPU µs / execution | Plan operators |
|---|---|---|---|---|---|
| `UPDATE j SET attempt = CASE WHEN j.state = 'running' AND c.cancel = 0  …  'running' OR u.metadata IS NOT NULL` | 248 | 4,304.8 | 21.4 | 12,420 | Assert ×1, Clustered Index Seek ×1, Clustered Index Update ×1, Collapse ×1, Compute Scalar ×8, Concatenation ×1, Constant Scan ×1, Filter ×5, Index Update ×1, Nested Loops ×5, Sequence ×1, Sort ×2, Split ×1, Stream Aggregate ×1, Table Insert ×1, Table Spool ×3, Table-valued function ×5 |
| `WITH locked AS ( SELECT TOP (@max) * FROM [dbo].kopi_job WITH (UPDLOCK … .scheduled_at, inserted.finalized_at` | 43 | 4,123.3 | 3.5 | 15,810 | Assert ×1, Clustered Index Seek ×1, Clustered Index Update ×1, Collapse ×1, Compute Scalar ×6, Concatenation ×1, Constant Scan ×1, Filter ×3, Index Seek ×1, Index Update ×1, Nested Loops ×2, Segment ×1, Sequence ×1, Sequence Project ×1, Sort ×4, Split ×1, Stream Aggregate ×1, Table Spool ×4, Table-valued function ×1, Top ×2 |
| `SELECT (SELECT COUNT(*) FROM [dbo].[kopi_job] WHERE state = 'available … tate = 'running' AND queue = @queue)` | 79 | 659.2 | 1.0 | 5,433 | Clustered Index Seek ×1, Compute Scalar ×3, Index Scan ×1, Index Seek ×1, Nested Loops ×2, Stream Aggregate ×2 |
| `SELECT TOP (@max) q.name, q.created_at, q.updated_at, q.paused_at, q.m … har(128) '$') AS n)) ORDER BY q.name` | 27 | 2.0 | 0.0 | 181 | Clustered Index Scan ×1, Compute Scalar ×1, Concatenation ×1, Constant Scan ×1, Filter ×2, Nested Loops ×1, Table-valued function ×1, Top ×1 |
| `INSERT INTO [dbo].kopi_queue (name, created_at, updated_at) SELECT n.n … ERE q.name = n.name) ORDER BY n.name` | 1 | 35.0 | 0.0 | 616 | Assert ×1, Clustered Index Insert ×1, Clustered Index Scan ×1, Clustered Index Seek ×1, Compute Scalar ×2, Nested Loops ×1, Table Spool ×1 |
| `INSERT INTO [dbo].kopi_leader (name, leader_id, elected_at, expires_at … K, HOLDLOCK) WHERE name = 'default')` | 1 | 16.0 | 0.0 | 212 | Assert ×1, Clustered Index Insert ×1, Clustered Index Scan ×1, Compute Scalar ×2, Constant Scan ×1, Nested Loops ×1, Top ×1 |


## End to end: one client, 1,000 workers, 50,000 no-op jobs

7,902 jobs/s (50,000 in 6.33 s); 50,000 worked

| Statement | Executions | Logical reads / execution | Reads / job | CPU µs / execution | Plan operators |
|---|---|---|---|---|---|
| `UPDATE j SET attempt = CASE WHEN j.state = 'running' AND c.cancel = 0  …  'running' OR u.metadata IS NOT NULL` | 53 | 20,183.6 | 21.4 | 57,822 | Assert ×1, Clustered Index Seek ×1, Clustered Index Update ×1, Collapse ×1, Compute Scalar ×8, Concatenation ×1, Constant Scan ×1, Filter ×5, Index Update ×1, Nested Loops ×5, Sequence ×1, Sort ×2, Split ×1, Stream Aggregate ×1, Table Insert ×1, Table Spool ×3, Table-valued function ×5 |
| `WITH locked AS ( SELECT TOP (@max) * FROM [dbo].kopi_job WITH (UPDLOCK … .scheduled_at, inserted.finalized_at` | 5 | 13,487.8 | 1.3 | 53,138 | Assert ×1, Clustered Index Seek ×1, Clustered Index Update ×1, Collapse ×1, Compute Scalar ×6, Concatenation ×1, Constant Scan ×1, Filter ×3, Index Seek ×1, Index Update ×1, Nested Loops ×2, Segment ×1, Sequence ×1, Sequence Project ×1, Sort ×4, Split ×1, Stream Aggregate ×1, Table Spool ×4, Table-valued function ×1, Top ×2 |
| `SELECT (SELECT COUNT(*) FROM [dbo].[kopi_job] WHERE state = 'available … tate = 'running' AND queue = @queue)` | 13 | 2,710.9 | 0.7 | 15,202 | Clustered Index Seek ×1, Compute Scalar ×3, Index Scan ×1, Index Seek ×1, Nested Loops ×2, Stream Aggregate ×2 |
| `INSERT INTO @names_table (name) SELECT n.name FROM OPENJSON(@names) WITH (name nvarchar(128) '$') AS n` | 1 | 14.0 | 0.0 | 209 | Clustered Index Insert ×1, Compute Scalar ×1, Table-valued function ×1 |
| `SELECT TOP (@max) q.name, q.created_at, q.updated_at, q.paused_at, q.m … har(128) '$') AS n)) ORDER BY q.name` | 6 | 2.0 | 0.0 | 171 | Clustered Index Scan ×1, Compute Scalar ×1, Concatenation ×1, Constant Scan ×1, Filter ×2, Nested Loops ×1, Table-valued function ×1, Top ×1 |
| `INSERT INTO [dbo].kopi_queue (name, created_at, updated_at) SELECT n.n … ERE q.name = n.name) ORDER BY n.name` | 1 | 11.0 | 0.0 | 90 | Assert ×1, Clustered Index Insert ×1, Clustered Index Scan ×1, Clustered Index Seek ×1, Compute Scalar ×2, Nested Loops ×1, Table Spool ×1 |


## DQ-6: OPENJSON or a table-valued parameter, 100 jobs a statement

| Parameter | Jobs/s | Call | Server CPU µs / statement | Logical reads / statement |
|---|---|---|---|---|
| OPENJSON (the driver) | 13,709 | p50 7.1 ms, p95 9.4 ms, p99 10.6 ms, max 29.2 ms (300 samples) | 5,487 | 2,286 |
| Table-valued parameter | 15,107 | p50 6.3 ms, p95 8.2 ms, p99 9.7 ms, max 47.7 ms (300 samples) | 2,651 | 1,003 |


## DQ-6: OPENJSON or a table-valued parameter, 1,000 jobs a statement

| Parameter | Jobs/s | Call | Server CPU µs / statement | Logical reads / statement |
|---|---|---|---|---|
| OPENJSON (the driver) | 19,762 | p50 50.5 ms, p95 59.3 ms, p99 60.6 ms, max 60.6 ms (40 samples) | 43,861 | 21,949 |
| Table-valued parameter | 29,513 | p50 33.5 ms, p95 39.3 ms, p99 43.8 ms, max 43.8 ms (40 samples) | 22,160 | 9,676 |


## DQ-7: state as varchar(16) or tinyint

| state | Row bytes, average | Pages: table, fetch, running, finalized | Fetch 25: reads, CPU µs | Complete 25: reads, CPU µs |
|---|---|---|---|---|
| varchar(16) | 123.2 | 3,760, 316, 25, 1,258 | 447.8, 1,677 | 203.1, 646 |
| tinyint | 113.0 | 3,450, 316, 25, 950 | 447.8, 1,614 | 202.0, 549 |


## Pickup latency: poll every 100 ms, jobs inserted by another connection

p50 60.6 ms, p95 108.6 ms, p99 109.8 ms, max 110.0 ms (100 samples); mean 61.6 ms; target p50 ≤ 80 ms (NFR-PERF-07)

## Pickup latency: poll every 1,000 ms, jobs inserted by another connection

p50 497.1 ms, p95 973.8 ms, p99 1,006.3 ms, max 1,006.3 ms (60 samples); mean 505.1 ms; target p50 ≤ 530 ms (NFR-PERF-07)
