# SQL Server spike results (2026-11-26 00:50 UTC)

Server 15.0.2210.4 Developer Edition (64-bit), 16 logical CPUs, 32,125 MB RAM. Client .NET 11.0.0, same machine.

## 0.9a Concurrent fetch + complete: kopi_spike_rc (20,000 jobs, 16 fetchers x limit 25, 2 completers)

- **result:** PASS: every job fetched exactly once and completed
- **rows:** total=20,000 completed=20,000 attempt min/max=1/1 fetched-distinct=20,000 duplicates=0 completer-updated=20,000
- **deadlocks:** fetch=0 complete=0
- **other errors:** none
- **throughput:** 9,563 jobs/s end to end (2.1 s, no-op work)

## 0.9a Concurrent fetch + complete: kopi_spike_rcsi (20,000 jobs, 16 fetchers x limit 25, 2 completers)

- **result:** PASS: every job fetched exactly once and completed
- **rows:** total=20,000 completed=20,000 attempt min/max=1/1 fetched-distinct=20,000 duplicates=0 completer-updated=20,000
- **deadlocks:** fetch=0 complete=0
- **other errors:** none
- **throughput:** 9,576 jobs/s end to end (2.1 s, no-op work)

## 0.9a Concurrent fetch + complete: kopi_spike_rcsi (20,000 jobs, 64 fetchers x limit 5, 4 completers)

- **result:** PASS: every job fetched exactly once and completed
- **rows:** total=20,000 completed=20,000 attempt min/max=1/1 fetched-distinct=20,000 duplicates=0 completer-updated=20,000
- **deadlocks:** fetch=0 complete=0
- **other errors:** none
- **throughput:** 4,506 jobs/s end to end (4.4 s, no-op work)

## 0.9b READPAST and isolation: kopi_spike_rc

- **locked rows skipped:** A took 10 (uncommitted); B then got 10 in 1 ms, overlap=0; B again got 0
- **after A rolls back:** B got 10, same rows A had: True
- **uncommitted insert:** fetch before commit: 0 rows in 0 ms; after commit: 5 rows
- **pool leak (SET ... SNAPSHOT):** same pooled session: True; isolation after reuse: SNAPSHOT
- **pool leak (SqlTransaction Snapshot):** same connection after commit: SNAPSHOT; next pooled open: SNAPSHOT
- **fetch on SNAPSHOT session:** SqlException 650: You can only specify the READPAST lock in the READ COMMITTED or REPEATABLE READ isolation levels.
- **fetch with SET READ COMMITTED prefix:** OK, 2 rows; session level afterwards: SNAPSHOT

## 0.9b READPAST and isolation: kopi_spike_rcsi

- **locked rows skipped:** A took 10 (uncommitted); B then got 10 in 1 ms, overlap=0; B again got 0
- **after A rolls back:** B got 10, same rows A had: True
- **uncommitted insert:** fetch before commit: 0 rows in 0 ms; after commit: 5 rows
- **pool leak (SET ... SNAPSHOT):** same pooled session: True; isolation after reuse: SNAPSHOT
- **pool leak (SqlTransaction Snapshot):** same connection after commit: SNAPSHOT; next pooled open: SNAPSHOT
- **fetch on SNAPSHOT session:** SqlException 650: You can only specify the READPAST lock in the READ COMMITTED or REPEATABLE READ isolation levels.
- **fetch with SET READ COMMITTED prefix:** OK, 2 rows; session level afterwards: SNAPSHOT

## 0.9c Lock escalation: kopi_spike_rc

- **complete batch  1,000:** row locks: DATABASE/S=1 KEY/X=4000 OBJECT/IX=1 PAGE/IX=58
- **complete batch  2,000:** row locks: DATABASE/S=1 KEY/X=8000 OBJECT/IX=1 PAGE/IX=113
- **complete batch  4,000:** row locks: DATABASE/S=1 KEY/X=16000 OBJECT/IX=1 PAGE/IX=225
- **complete batch  4,900:** row locks: DATABASE/S=1 KEY/X=19600 OBJECT/IX=1 PAGE/IX=274
- **complete batch  6,000:** ESCALATED: DATABASE/S=1 OBJECT/X=16
- **complete batch 10,000:** ESCALATED: DATABASE/S=1 OBJECT/X=16
- **fetch limit  1,000:** row locks: DATABASE/S=1 KEY/X=4000 OBJECT/IX=1 PAGE/IX=63
- **fetch limit  4,900:** row locks: DATABASE/S=1 KEY/X=19600 OBJECT/IX=1 PAGE/IX=285
- **fetch limit  6,000:** ESCALATED: DATABASE/S=1 OBJECT/X=16

## 0.9d Fetch plan diagnosis: kopi_spike_rc

- **limit    25:** 25 rows; row locks: DATABASE/S=1 KEY/X=100 OBJECT/IX=1 PAGE/IX=6
- **limit    25 plan:** ops: Sequence(est 25) > Index Update(est 25) > Collapse(est 25) > Sort(est 33) > Filter(est 33) > Table Spool(est 50) > Split(est 50) > Assert(est 25) > Compute Scalar(est 25) > Clustered Index Update(est 25) > Compute Scalar(est 25) > Compute Scalar(est 25) > Compute Scalar(est 25) > Top(est 25) > Compute Scalar(est 25) > Nested Loops(est 25) > Index Seek(est 25) > Clustered Index Seek(est 1) > Filter(est 25) > Table Spool(est 50); indexes: kopi_job_unique_idx, kopi_job_pk, kopi_job_fetch_idx, kopi_job_running_idx, kopi_job_finalized_idx; max ActualRowsRead=50
- **limit 1,000:** 1000 rows; ESCALATED: ALLOCATION_UNIT/S=1 DATABASE/S=2 OBJECT/X=16
- **limit 1,000 plan:** ops: Sequence(est 1,000) > Index Update(est 1,000) > Collapse(est 1,000) > Sort(est 1,333) > Filter(est 1,333) > Table Spool(est 2,000) > Split(est 2,000) > Assert(est 1,000) > Compute Scalar(est 1,000) > Clustered Index Update(est 1,000) > Compute Scalar(est 1,000) > Compute Scalar(est 1,000) > Compute Scalar(est 1,000) > Top(est 1,000) > Sort(est 1,000) > Compute Scalar(est 12,000) > Clustered Index Scan(est 12,000) > Filter(est 1,000) > Table Spool(est 2,000); indexes: kopi_job_unique_idx, kopi_job_pk, kopi_job_fetch_idx, kopi_job_running_idx, kopi_job_finalized_idx; max ActualRowsRead=36,000
- **limit 4,000:** 4000 rows; ESCALATED: ALLOCATION_UNIT/S=1 DATABASE/S=2 OBJECT/X=16
- **limit 4,000 plan:** ops: Sequence(est 4,000) > Index Update(est 4,000) > Collapse(est 4,000) > Sort(est 5,333) > Filter(est 5,333) > Table Spool(est 8,000) > Split(est 8,000) > Assert(est 4,000) > Compute Scalar(est 4,000) > Clustered Index Update(est 4,000) > Compute Scalar(est 4,000) > Compute Scalar(est 4,000) > Compute Scalar(est 4,000) > Top(est 4,000) > Sort(est 4,000) > Compute Scalar(est 12,000) > Clustered Index Scan(est 12,000) > Filter(est 4,000) > Table Spool(est 8,000); indexes: kopi_job_unique_idx, kopi_job_pk, kopi_job_fetch_idx, kopi_job_running_idx, kopi_job_finalized_idx; max ActualRowsRead=36,000

## 0.9d Fetch plan diagnosis: kopi_spike_rc (variant: INDEX(kopi_job_fetch_idx))

- **limit    25:** 25 rows; row locks: DATABASE/S=1 KEY/X=100 OBJECT/IX=1 PAGE/IX=6
- **limit    25 plan:** ops: Sequence(est 25) > Index Update(est 25) > Collapse(est 25) > Sort(est 33) > Filter(est 33) > Table Spool(est 50) > Split(est 50) > Assert(est 25) > Compute Scalar(est 25) > Clustered Index Update(est 25) > Compute Scalar(est 25) > Compute Scalar(est 25) > Compute Scalar(est 25) > Top(est 25) > Compute Scalar(est 25) > Nested Loops(est 25) > Index Seek(est 25) > Clustered Index Seek(est 1) > Filter(est 25) > Table Spool(est 50); indexes: kopi_job_unique_idx, kopi_job_pk, kopi_job_fetch_idx, kopi_job_running_idx, kopi_job_finalized_idx; max ActualRowsRead=50
- **limit 1,000:** 1000 rows; row locks: DATABASE/S=1 KEY/X=4000 OBJECT/IX=1 PAGE/IX=62
- **limit 1,000 plan:** ops: Sequence(est 1,000) > Index Update(est 1,000) > Collapse(est 1,000) > Sort(est 1,333) > Filter(est 1,333) > Table Spool(est 2,000) > Split(est 2,000) > Assert(est 1,000) > Compute Scalar(est 1,000) > Clustered Index Update(est 1,000) > Compute Scalar(est 1,000) > Compute Scalar(est 1,000) > Compute Scalar(est 1,000) > Top(est 1,000) > Compute Scalar(est 1,000) > Nested Loops(est 1,000) > Index Seek(est 1,000) > Clustered Index Seek(est 1) > Filter(est 1,000) > Table Spool(est 2,000); indexes: kopi_job_unique_idx, kopi_job_pk, kopi_job_fetch_idx, kopi_job_running_idx, kopi_job_finalized_idx; max ActualRowsRead=2,000
- **limit 4,000:** 4000 rows; row locks: DATABASE/S=1 KEY/X=16000 OBJECT/IX=1 PAGE/IX=233
- **limit 4,000 plan:** ops: Sequence(est 4,000) > Index Update(est 4,000) > Collapse(est 4,000) > Sort(est 5,333) > Filter(est 5,333) > Table Spool(est 8,000) > Split(est 8,000) > Assert(est 4,000) > Compute Scalar(est 4,000) > Clustered Index Update(est 4,000) > Compute Scalar(est 4,000) > Compute Scalar(est 4,000) > Compute Scalar(est 4,000) > Table Spool(est 4,000) > Top(est 4,000) > Compute Scalar(est 4,000) > Nested Loops(est 4,000) > Index Seek(est 4,000) > Clustered Index Seek(est 1) > Filter(est 4,000) > Table Spool(est 8,000); indexes: kopi_job_unique_idx, kopi_job_pk, kopi_job_fetch_idx, kopi_job_running_idx, kopi_job_finalized_idx; max ActualRowsRead=8,000

## 0.9d Fetch plan diagnosis: kopi_spike_rc (variant: FORCESEEK(kopi_job_fetch_idx(queue)))

- **limit    25:** 25 rows; row locks: DATABASE/S=1 KEY/X=100 OBJECT/IX=1 PAGE/IX=6
- **limit    25 plan:** ops: Sequence(est 25) > Index Update(est 25) > Collapse(est 25) > Sort(est 33) > Filter(est 33) > Table Spool(est 50) > Split(est 50) > Assert(est 25) > Compute Scalar(est 25) > Clustered Index Update(est 25) > Compute Scalar(est 25) > Compute Scalar(est 25) > Compute Scalar(est 25) > Top(est 25) > Compute Scalar(est 25) > Nested Loops(est 25) > Index Seek(est 25) > Clustered Index Seek(est 1) > Filter(est 25) > Table Spool(est 50); indexes: kopi_job_unique_idx, kopi_job_pk, kopi_job_fetch_idx, kopi_job_running_idx, kopi_job_finalized_idx; max ActualRowsRead=50
- **limit 1,000:** 1000 rows; row locks: DATABASE/S=1 KEY/X=4000 OBJECT/IX=1 PAGE/IX=62
- **limit 1,000 plan:** ops: Sequence(est 1,000) > Index Update(est 1,000) > Collapse(est 1,000) > Sort(est 1,333) > Filter(est 1,333) > Table Spool(est 2,000) > Split(est 2,000) > Assert(est 1,000) > Compute Scalar(est 1,000) > Clustered Index Update(est 1,000) > Compute Scalar(est 1,000) > Compute Scalar(est 1,000) > Compute Scalar(est 1,000) > Top(est 1,000) > Compute Scalar(est 1,000) > Nested Loops(est 1,000) > Index Seek(est 1,000) > Clustered Index Seek(est 1) > Filter(est 1,000) > Table Spool(est 2,000); indexes: kopi_job_unique_idx, kopi_job_pk, kopi_job_fetch_idx, kopi_job_running_idx, kopi_job_finalized_idx; max ActualRowsRead=2,000
- **limit 4,000:** 4000 rows; row locks: DATABASE/S=1 KEY/X=16000 OBJECT/IX=1 PAGE/IX=233
- **limit 4,000 plan:** ops: Sequence(est 4,000) > Index Update(est 4,000) > Collapse(est 4,000) > Sort(est 5,333) > Filter(est 5,333) > Table Spool(est 8,000) > Split(est 8,000) > Assert(est 4,000) > Compute Scalar(est 4,000) > Clustered Index Update(est 4,000) > Compute Scalar(est 4,000) > Compute Scalar(est 4,000) > Compute Scalar(est 4,000) > Table Spool(est 4,000) > Top(est 4,000) > Compute Scalar(est 4,000) > Nested Loops(est 4,000) > Index Seek(est 4,000) > Clustered Index Seek(est 1) > Filter(est 4,000) > Table Spool(est 8,000); indexes: kopi_job_unique_idx, kopi_job_pk, kopi_job_fetch_idx, kopi_job_running_idx, kopi_job_finalized_idx; max ActualRowsRead=8,000

## 0.10a Unique inserts under concurrency: kopi_spike_rc

- **HoldLock hot key, 100x10, autocommit, empty table:** PASS: inserted=10 (expected 10) skipped=990 neither=0 errors=0 duplicate-occupied-keys=0; per-insert n=1000 p50=4.0ms p95=81.2ms p99=96.9ms max=102.3ms
- **HoldLock distinct keys, 100x10, autocommit, empty table:** PASS: inserted=1000 (expected 1000) skipped=0 neither=0 errors=0 duplicate-occupied-keys=0; per-insert n=1000 p50=7.7ms p95=16.5ms p99=29.5ms max=31.6ms
- **HoldLock hot key, 20x3, caller tx held 50 ms, empty table:** PASS: inserted=3 (expected 3) skipped=57 neither=0 errors=0 duplicate-occupied-keys=0; per-insert n=60 p50=625.7ms p95=1187.1ms p99=1249.8ms max=1249.8ms
- **HoldLock distinct keys, 50x3, caller tx held 50 ms, empty table:** PASS: inserted=150 (expected 150) skipped=0 neither=0 errors=0 duplicate-occupied-keys=0; per-insert n=150 p50=124.3ms p95=2111.2ms p99=2483.4ms max=2544.4ms
- **HoldLock distinct keys, 50x3, caller tx held 50 ms, 20,000 existing keys:** PASS: inserted=150 (expected 150) skipped=0 neither=0 errors=0 duplicate-occupied-keys=0; per-insert n=150 p50=60.3ms p95=63.1ms p99=119.1ms max=123.0ms
- **HoldLock XACT_ABORT ON duplicate (READ COMMITTED caller):** Skipped; XACT_STATE=1; follow-up insert and commit OK
- **HoldLock SNAPSHOT caller, XACT_ABORT OFF, new key, no conflict:** Inserted; XACT_STATE=1; commit OK
- **HoldLock SNAPSHOT caller, XACT_ABORT OFF, key committed by another tx after snapshot start:** Skipped; XACT_STATE=1; commit OK
- **HoldLock SNAPSHOT caller, XACT_ABORT ON, new key, no conflict:** Inserted; XACT_STATE=1; commit OK
- **HoldLock SNAPSHOT caller, XACT_ABORT ON, key committed by another tx after snapshot start:** Skipped; XACT_STATE=1; commit OK
- **AppLock hot key, 100x10, autocommit, empty table:** PASS: inserted=10 (expected 10) skipped=990 neither=0 errors=0 duplicate-occupied-keys=0; per-insert n=1000 p50=23.4ms p95=53.1ms p99=71.0ms max=76.8ms
- **AppLock distinct keys, 100x10, autocommit, empty table:** PASS: inserted=1000 (expected 1000) skipped=0 neither=0 errors=0 duplicate-occupied-keys=0; per-insert n=1000 p50=9.7ms p95=15.4ms p99=27.4ms max=41.2ms
- **AppLock hot key, 20x3, caller tx held 50 ms, empty table:** PASS: inserted=3 (expected 3) skipped=57 neither=0 errors=0 duplicate-occupied-keys=0; per-insert n=60 p50=623.6ms p95=1184.2ms p99=1245.7ms max=1245.7ms
- **AppLock distinct keys, 50x3, caller tx held 50 ms, empty table:** PASS: inserted=150 (expected 150) skipped=0 neither=0 errors=0 duplicate-occupied-keys=0; per-insert n=150 p50=61.4ms p95=64.6ms p99=64.8ms max=64.9ms
- **AppLock distinct keys, 50x3, caller tx held 50 ms, 20,000 existing keys:** PASS: inserted=150 (expected 150) skipped=0 neither=0 errors=0 duplicate-occupied-keys=0; per-insert n=150 p50=60.0ms p95=62.0ms p99=62.3ms max=62.3ms
- **AppLock XACT_ABORT ON duplicate (READ COMMITTED caller):** Skipped; XACT_STATE=1; follow-up insert and commit OK
- **AppLock SNAPSHOT caller, XACT_ABORT OFF, new key, no conflict:** Inserted; XACT_STATE=1; commit OK
- **AppLock SNAPSHOT caller, XACT_ABORT OFF, key committed by another tx after snapshot start:** Skipped; XACT_STATE=1; commit OK
- **AppLock SNAPSHOT caller, XACT_ABORT ON, new key, no conflict:** Inserted; XACT_STATE=1; commit OK
- **AppLock SNAPSHOT caller, XACT_ABORT ON, key committed by another tx after snapshot start:** Skipped; XACT_STATE=1; commit OK

## 0.10a Unique inserts under concurrency: kopi_spike_rcsi

- **HoldLock hot key, 100x10, autocommit, empty table:** PASS: inserted=10 (expected 10) skipped=990 neither=0 errors=0 duplicate-occupied-keys=0; per-insert n=1000 p50=3.9ms p95=76.0ms p99=91.0ms max=92.3ms
- **HoldLock distinct keys, 100x10, autocommit, empty table:** PASS: inserted=1000 (expected 1000) skipped=0 neither=0 errors=0 duplicate-occupied-keys=0; per-insert n=1000 p50=8.0ms p95=16.0ms p99=25.7ms max=33.6ms
- **HoldLock hot key, 20x3, caller tx held 50 ms, empty table:** PASS: inserted=3 (expected 3) skipped=57 neither=0 errors=0 duplicate-occupied-keys=0; per-insert n=60 p50=621.0ms p95=1178.7ms p99=1241.0ms max=1241.0ms
- **HoldLock distinct keys, 50x3, caller tx held 50 ms, empty table:** PASS: inserted=150 (expected 150) skipped=0 neither=0 errors=0 duplicate-occupied-keys=0; per-insert n=150 p50=121.6ms p95=2659.3ms p99=3031.6ms max=3094.8ms
- **HoldLock distinct keys, 50x3, caller tx held 50 ms, 20,000 existing keys:** PASS: inserted=150 (expected 150) skipped=0 neither=0 errors=0 duplicate-occupied-keys=0; per-insert n=150 p50=57.8ms p95=63.8ms p99=119.3ms max=119.6ms
- **HoldLock XACT_ABORT ON duplicate (READ COMMITTED caller):** Skipped; XACT_STATE=1; follow-up insert and commit OK
- **HoldLock SNAPSHOT caller, XACT_ABORT OFF, new key, no conflict:** Inserted; XACT_STATE=1; commit OK
- **HoldLock SNAPSHOT caller, XACT_ABORT OFF, key committed by another tx after snapshot start:** Skipped; XACT_STATE=1; commit OK
- **HoldLock SNAPSHOT caller, XACT_ABORT ON, new key, no conflict:** Inserted; XACT_STATE=1; commit OK
- **HoldLock SNAPSHOT caller, XACT_ABORT ON, key committed by another tx after snapshot start:** Skipped; XACT_STATE=1; commit OK
- **AppLock hot key, 100x10, autocommit, empty table:** PASS: inserted=10 (expected 10) skipped=990 neither=0 errors=0 duplicate-occupied-keys=0; per-insert n=1000 p50=21.9ms p95=48.8ms p99=68.0ms max=75.5ms
- **AppLock distinct keys, 100x10, autocommit, empty table:** PASS: inserted=1000 (expected 1000) skipped=0 neither=0 errors=0 duplicate-occupied-keys=0; per-insert n=1000 p50=9.1ms p95=15.0ms p99=26.7ms max=40.4ms
- **AppLock hot key, 20x3, caller tx held 50 ms, empty table:** PASS: inserted=3 (expected 3) skipped=57 neither=0 errors=0 duplicate-occupied-keys=0; per-insert n=60 p50=622.6ms p95=1182.9ms p99=1244.7ms max=1244.7ms
- **AppLock distinct keys, 50x3, caller tx held 50 ms, empty table:** PASS: inserted=150 (expected 150) skipped=0 neither=0 errors=0 duplicate-occupied-keys=0; per-insert n=150 p50=59.8ms p95=67.1ms p99=67.2ms max=67.3ms
- **AppLock distinct keys, 50x3, caller tx held 50 ms, 20,000 existing keys:** PASS: inserted=150 (expected 150) skipped=0 neither=0 errors=0 duplicate-occupied-keys=0; per-insert n=150 p50=61.7ms p95=62.9ms p99=76.3ms max=76.9ms
- **AppLock XACT_ABORT ON duplicate (READ COMMITTED caller):** Skipped; XACT_STATE=1; follow-up insert and commit OK
- **AppLock SNAPSHOT caller, XACT_ABORT OFF, new key, no conflict:** Inserted; XACT_STATE=1; commit OK
- **AppLock SNAPSHOT caller, XACT_ABORT OFF, key committed by another tx after snapshot start:** Skipped; XACT_STATE=1; commit OK
- **AppLock SNAPSHOT caller, XACT_ABORT ON, new key, no conflict:** Inserted; XACT_STATE=1; commit OK
- **AppLock SNAPSHOT caller, XACT_ABORT ON, key committed by another tx after snapshot start:** Skipped; XACT_STATE=1; commit OK

## 0.10b SqlBulkCopy in a caller transaction: kopi_spike_rc

- **4,000 rows, CheckConstraints=True, LOCK_ESCALATION=TABLE:** in-tx count=4,000, after rollback=0, after commit=4,000; 37,546 rows/s; locks: ALLOCATION_UNIT/S=6 DATABASE/S=2 EXTENT/X=36 KEY/X=580 METADATA/Sch-S=1 OBJECT/IX=1 PAGE/IX=6 PAGE/X=488; concurrent fetch on another queue: BLOCKED (timed out after 3014 ms); untrusted CHECK constraints afterwards: 0
- **50,000 rows, CheckConstraints=True, LOCK_ESCALATION=TABLE:** in-tx count=50,000, after rollback=0, after commit=50,000; 38,280 rows/s; locks: ALLOCATION_UNIT/S=6 DATABASE/S=2 EXTENT/X=321 KEY/X=3377 METADATA/Sch-S=1 OBJECT/IX=2 PAGE/IX=6 PAGE/X=4975; concurrent fetch on another queue: BLOCKED (timed out after 3009 ms); untrusted CHECK constraints afterwards: 0
- **50,000 rows, CheckConstraints=False, LOCK_ESCALATION=TABLE:** in-tx count=50,000, after rollback=0, after commit=50,000; 43,973 rows/s; locks: ALLOCATION_UNIT/S=6 DATABASE/S=2 EXTENT/X=321 KEY/X=3377 METADATA/Sch-S=1 OBJECT/IX=2 PAGE/IX=6 PAGE/X=4975; concurrent fetch on another queue: BLOCKED (timed out after 3001 ms); untrusted CHECK constraints afterwards: 8
- **50,000 rows, CheckConstraints=True, LOCK_ESCALATION=DISABLE:** in-tx count=50,000, after rollback=0, after commit=50,000; 35,088 rows/s; locks: ALLOCATION_UNIT/S=6 DATABASE/S=2 EXTENT/X=321 KEY/X=3377 METADATA/Sch-S=1 OBJECT/IX=2 PAGE/IX=6 PAGE/X=4975; concurrent fetch on another queue: BLOCKED (timed out after 3003 ms); untrusted CHECK constraints afterwards: 0
- **OPENJSON batched insert, 50,000 rows / 1,000 per batch, ids returned:** 50,000 ids; 41,825 rows/s; locks at commit: DATABASE/S=1 KEY/X=200000 OBJECT/IX=2 PAGE/IX=1917; concurrent fetch on another queue: not blocked (1 row in 185 ms)

## 0.10c Poll cost and pickup latency: kopi_spike_rc

- **table:** 200,000 completed (history) + 50,000 available in queue 'busy'; polling empty queue 'idle'
- **empty poll, client round trip:** n=2000 p50=0.1ms p95=0.4ms p99=0.6ms max=294.6ms
- **empty poll, server:** executions=2,000, avg logical reads=3.0, avg CPU=70 µs, indexes used: kopi_job_unique_idx, kopi_job_pk, kopi_job_fetch_idx, kopi_job_running_idx, kopi_job_finalized_idx; seeks=1 scans=0
- **fetch 25 from busy queue, client round trip:** n=200 p50=3.2ms p95=4.5ms p99=5.0ms max=13.7ms
- **fetch 25 from busy queue, server:** executions=200, avg logical reads=949.9, avg CPU=2497 µs, indexes used: kopi_job_unique_idx, kopi_job_pk, kopi_job_fetch_idx, kopi_job_running_idx, kopi_job_finalized_idx; seeks=1 scans=0
- **pickup latency, poll every 100 ms:** n=40 p50=76.1ms p95=110.8ms p99=123.7ms max=123.7ms
- **pickup latency, poll every 1000 ms:** n=15 p50=514.7ms p95=997.0ms p99=997.0ms max=997.0ms
