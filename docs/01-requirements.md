# KopiQueue — Requirements Specification

**Version:** 0.4 (draft) · **Date:** 2026-11-26 · **Related:** [Design](02-design.md), [Plan](03-plan.md)

---

## 1. Purpose

KopiQueue (KQ) is an open-source (MIT) .NET library for running background jobs that are stored in a relational
database: **PostgreSQL** or **Microsoft SQL Server**. Its feature set and behaviour follow riverqueue/river, the Go job queue.
On PostgreSQL, KQ is schema-compatible with River. The main reason to use it is
**transactional enqueueing**: a job is inserted in the same database transaction as the application data
that caused it. The job then exists only if that transaction commits, which removes the "dual write" problem
of publishing to an external broker such as Redis or RabbitMQ.

This document says *what* KQ must do. [02-design.md](02-design.md) says *how*.

## 2. Background

### 2.1 What River provides (reference feature set)

| Area | River capability |
|------|------------------|
| Storage | Postgres-centric. All state lives in a small set of tables (`river_job`, `river_queue`, `river_leader`, `river_migration`) |
| Enqueue | Insert inside the caller's transaction. Fast bulk insert via `COPY` |
| Typed jobs | Strongly typed `JobArgs` with a `Kind()`, and a typed `Worker[T]` |
| Execution | Per-queue worker pools. Job locking via `FOR UPDATE SKIP LOCKED`. `LISTEN/NOTIFY` for low latency, with a polling fallback |
| Reliability | At-least-once delivery. Retries with exponential backoff and jitter. Max attempts. Discarding. Rescue of stuck jobs |
| Control | Cancel, snooze, scheduled jobs, priorities 1–4, pause/resume queues |
| Uniqueness | Unique jobs by args, period, queue and state, enforced by a unique index |
| Periodic | Cron-like and interval-based periodic jobs, enqueued by the elected leader |
| Maintenance | Leader election. Cleaning of completed/cancelled/discarded jobs. Rescuer. Scheduler. Queue cleaner. Reindexer |
| Ops | Schema migrations (library and CLI), event subscriptions, hooks/middleware, job timeouts, test helpers, a web UI (separate project) |

### 2.2 The .NET landscape and how KQ differs

Hangfire, Quartz.NET, MassTransit, Wolverine, TickerQ and Coravel already cover parts of this space.
KQ aims to be different in these ways:

1. **Database-native and transactional by default.** PostgreSQL and SQL Server are both first-class. Works with an `NpgsqlTransaction`, a `SqlTransaction` or an EF Core `DbContext`.
2. **Strongly typed, data-only jobs.** Job args are JSON-serialisable records. Jobs are not serialised delegates or expression trees.
3. **No extra infrastructure.** No broker, no Redis, no separate scheduler process.
4. **Modern .NET.** Generic Host and DI, `System.Text.Json` with source generation, trimming and Native AOT friendly, OpenTelemetry built in.
5. **River schema compatibility on PostgreSQL (decided, OQ-1).** Go and .NET services can share one queue, and River's web UI can work against a KQ database.
6. **SQL Server for Microsoft-stack shops.** It is the database most .NET teams already run. Existing SQL Server job queues either use expression-based jobs (Hangfire) or offer no transactional enqueue from EF Core.

## 3. Goals and non-goals

### 3.1 Goals

- G1. Match River's core open-source feature set with idiomatic .NET APIs.
- G2. Exactly one database round trip to enqueue a job inside the caller's transaction.
- G3. Run safely as many instances across many hosts, with no external coordinator.
- G4. Fit naturally with `Microsoft.Extensions.*` (DI, Hosting, Logging, Options, Diagnostics).
- G5. Be easy to test: helpers for checking what was inserted and for running workers in isolation.
- G6. Production-grade observability through logs, metrics and traces.

### 3.2 Non-goals (v1.0)

- NG1. Databases other than PostgreSQL and SQL Server. The driver abstraction leaves room for SQLite later, see the [Plan](03-plan.md).
- NG2. A built-in web dashboard. Post-1.0 we will either validate River UI compatibility or build our own.
- NG3. River Pro features: workflows (DAGs), sequences, global concurrency limits, encrypted args. The schema must still not block them (for example, the `pending` state is kept).
- NG4. Exactly-once execution. KQ guarantees at-least-once, so workers must be idempotent.
- NG5. Message-bus semantics such as pub/sub fan-out, sagas or routing topologies.
- NG6. .NET Framework support. Only modern .NET is targeted, see NFR-COMP-01.

## 4. Stakeholders and personas

| Persona | Needs |
|---------|-------|
| **Application developer** | Define a job, enqueue it in a transaction, write a worker using DI, test it |
| **Platform / SRE** | Deploy N replicas safely, see throughput, latency, failures and queue depth, pause queues during incidents, run migrations in CI/CD |
| **Library maintainer** (us) | A small API surface that can evolve, a predictable release cadence, reproducible integration tests |
| **Polyglot team** (optional) | Go services using River and .NET services using KQ on one PostgreSQL database |
| **Microsoft-stack team** | SQL Server or Azure SQL as the only database; enqueue from EF Core in the same transaction as `SaveChanges` |

## 5. Use cases

| ID | Use case |
|----|----------|
| UC-1 | When an order is created, enqueue a `SendOrderConfirmation` job in the same transaction as the `orders` row |
| UC-2 | Schedule a "trial expiring" reminder 14 days in the future |
| UC-3 | Run `RebuildSearchIndex` every night at 02:00 UTC, once per cluster, not once per replica |
| UC-4 | Ensure at most one `SyncCustomer(customerId)` job is pending at a time (unique job) |
| UC-5 | Bulk-insert 100,000 `ImportRow` jobs quickly |
| UC-6 | A webhook delivery fails, so retry it with backoff and give up after N attempts |
| UC-7 | An operator pauses the `email` queue during a provider outage, then resumes it |
| UC-8 | A worker learns that a job is obsolete and cancels it. Or the upstream system is rate-limited, so it snoozes the job for 5 minutes |
| UC-9 | A replica crashes mid-job. The job is rescued and retried |
| UC-10 | A developer writes a unit test asserting that a job of kind X was inserted with args Y |

---

## 6. Functional requirements

Priority: **P0** = required for 1.0 MVP · **P1** = required for 1.0 GA · **P2** = post-1.0.

### 6.1 Job definition — `FR-DEF`

| ID | Requirement | Pri |
|----|-------------|-----|
| FR-DEF-01 | A job type is defined by an args type, usually a `record`. It MUST be JSON-serialisable and MUST declare a stable, unique **kind** string | P0 |
| FR-DEF-02 | A worker is defined by implementing `IWorker<TArgs>`, or by deriving from `Worker<TArgs>`, with an async `WorkAsync(Job<TArgs>, CancellationToken)` method | P0 |
| FR-DEF-03 | Workers MUST be resolved from DI in a new scope per job execution, so scoped services such as a `DbContext` are supported | P0 |
| FR-DEF-04 | A worker MAY override per-job behaviour: `Timeout`, `NextRetry` (custom backoff) and `MaxAttempts` default | P0 |
| FR-DEF-05 | An args type MAY supply default insert options (queue, priority, max attempts, tags, uniqueness), like River's `JobArgsWithInsertOpts` | P1 |
| FR-DEF-06 | Kind strings MUST be validated at startup. Duplicate kinds and kinds without a worker are configuration errors, raised when the client starts working | P0 |
| FR-DEF-07 | Support renaming a kind without losing in-flight jobs, through alias kinds | P2 |

### 6.2 Insertion — `FR-INS`

| ID | Requirement | Pri |
|----|-------------|-----|
| FR-INS-01 | Insert a single job with `InsertAsync(args, options?)` on a connection the client manages | P0 |
| FR-INS-02 | Insert a single job **within a caller-supplied transaction**: `NpgsqlTransaction` (PostgreSQL) or `SqlTransaction` (SQL Server) | P0 |
| FR-INS-03 | Insert within an EF Core `DbContext`'s current transaction (separate package), for both the Npgsql and the SQL Server EF providers | P0 |
| FR-INS-04 | Bulk insert with `InsertManyAsync`, returning the inserted rows. A fast path that does not return rows is also offered: binary `COPY` (PostgreSQL) or batched OPENJSON inserts (SQL Server; SqlBulkCopy was rejected, ADR-014). Bulk inserts MUST NOT block producers on other queues | P0 |
| FR-INS-05 | Insert options: `Queue`, `Priority` (1–4, 1 = highest), `ScheduledAt`, `MaxAttempts`, `Tags`, `Metadata`, `Unique`. Validation matches River: `Queue` and `Kind` 1–127 characters; each tag 1–255 characters matching `\A[\w][\w\-]+[\w]\z` (spike 0.8) | P0 |
| FR-INS-06 | Insert returns the job row (id, state, ...) and a flag saying whether the insert was **skipped as a duplicate** because of uniqueness | P0 |
| FR-INS-07 | After insert, the client MUST notify listening producers of the queue for low-latency pickup. If the transaction rolls back, no notification is delivered. PostgreSQL: across all clients. SQL Server: local process only, see FR-DB-04 | P0 |
| FR-INS-08 | Insert-only mode: a client with no queues or workers configured MUST be able to insert jobs, for example from web front-ends | P0 |
| FR-INS-09 | Validate inputs: queue name format and length, kind length, priority range, `MaxAttempts >= 1`, tag format | P0 |
| FR-INS-10 | Insert middleware/hooks can change the parameters before insert, for example to add metadata or tracing context | P1 |

### 6.3 Queues — `FR-QUE`

| ID | Requirement | Pri |
|----|-------------|-----|
| FR-QUE-01 | Configure any number of named queues, each with its own `MaxWorkers` concurrency | P0 |
| FR-QUE-02 | Queues are tracked in a `queue` table: created when first used, with `paused_at` and `metadata` | P0 |
| FR-QUE-03 | Pause and resume a queue, either everywhere (all clients) or locally. A paused queue fetches no new jobs, and running jobs finish | P0 |
| FR-QUE-04 | Pause/resume MUST reach other clients within one poll interval, or sooner when notifications are available | P0 |
| FR-QUE-05 | Add or remove queues on a running client | P2 |
| FR-QUE-06 | Per-queue fetch settings: poll interval and fetch cooldown | P1 |

### 6.4 Execution — `FR-EXE`

| ID | Requirement | Pri |
|----|-------------|-----|
| FR-EXE-01 | Jobs are fetched with row-level locking that skips rows other clients have locked. No two clients may lock the same `available` job | P0 |
| FR-EXE-02 | Fetch order within a queue: `priority ASC, scheduled_at ASC, id ASC` | P0 |
| FR-EXE-03 | Fetching is driven by notifications where the driver supports them (PostgreSQL LISTEN/NOTIFY), with a periodic poll as fallback. It MUST work in **poll-only mode**: always on SQL Server, and on PostgreSQL when LISTEN is unavailable, for example behind PgBouncer in transaction-pooling mode | P0 |
| FR-EXE-04 | Never run more than `MaxWorkers` concurrent executions per queue per client | P0 |
| FR-EXE-05 | Per-job timeout. A default comes from client config, a worker can override it, and an infinite timeout is allowed. On timeout the job's `CancellationToken` is cancelled and the attempt counts as an error | P0 |
| FR-EXE-06 | Job completion is batched to reduce writes. A completed result MUST be durably written before the job is reported as completed through events | P0 |
| FR-EXE-07 | A worker can reach the current job context (id, attempt, metadata) and the client from inside `WorkAsync` | P0 |
| FR-EXE-08 | Unhandled exceptions are captured and recorded in the job's `errors` history (attempt, timestamp, message, trace). They do not crash the host | P0 |
| FR-EXE-09 | Worker middleware wraps execution in a pipeline, for example for logging, tenancy context or metrics | P1 |
| FR-EXE-10 | A worker can record output data on the job, stored in metadata | P2 |

### 6.5 Retries, errors and job control — `FR-ERR`

| ID | Requirement | Pri |
|----|-------------|-----|
| FR-ERR-01 | A failed attempt moves the job to `retryable` with `scheduled_at` set to the next retry time. Once attempts are exhausted it moves to `discarded` | P0 |
| FR-ERR-02 | Default retry policy: `attempt^4` seconds plus ±10% jitter. Default `MaxAttempts = 25`. Both are configurable, and the policy can be replaced through an interface | P0 |
| FR-ERR-03 | **Cancel from a worker**: the job moves to `cancelled` permanently, with no further retries | P0 |
| FR-ERR-04 | **Snooze from a worker**: the job is rescheduled after a duration and the snooze does not use up an attempt | P0 |
| FR-ERR-05 | Global error handler hooks for errors and for exceptions that escaped normal handling. A hook can override the outcome, for example cancel instead of retry | P1 |
| FR-ERR-06 | **Cancel from outside**: `JobCancelAsync(id)`. A job that has not started is finalised at once. A running job gets cancellation propagated to its `CancellationToken`, on any client, through a control notification | P0 |
| FR-ERR-07 | **Retry from outside**: `JobRetryAsync(id)` makes a finalised or scheduled job available now | P1 |

### 6.6 Scheduling — `FR-SCH`

| ID | Requirement | Pri |
|----|-------------|-----|
| FR-SCH-01 | A job inserted with a future `ScheduledAt` is in state `scheduled`. The leader's scheduler moves due `scheduled` and `retryable` jobs to `available` | P0 |
| FR-SCH-02 | Scheduled jobs become eligible within ≤ 5 s of their due time (default), configurable | P0 |

### 6.7 Unique jobs — `FR-UNQ`

| ID | Requirement | Pri |
|----|-------------|-----|
| FR-UNQ-01 | Uniqueness options: `ByArgs` (all args, or only fields marked as unique), `ByPeriod` (time bucket), `ByQueue`, `ByState` (set of states considered "occupied") | P0 |
| FR-UNQ-02 | Uniqueness MUST be enforced by the database (a unique index on the hashed unique key plus the states in which it applies), not by advisory locks or by reading first | P0 |
| FR-UNQ-03 | A duplicate insert returns the existing job with `UniqueSkippedAsDuplicate = true`, and does not throw | P0 |
| FR-UNQ-04 | Default unique states: `available, completed, pending, retryable, running, scheduled`. A custom set MUST include `available, pending, running, scheduled` | P0 |
| FR-UNQ-05 | Unique inserts work in bulk insert too | P1 |
| FR-UNQ-06 | PostgreSQL: for the same kind, args JSON and options, KQ MUST produce the same `unique_key` and `unique_states` as River, so that KQ and River producers sharing one schema dedupe against each other. Verified against golden vectors generated by River (spike 0.8). Whether KQ re-encodes args in Go-compatible JSON is DQ-8 | P0 |

### 6.8 Periodic jobs — `FR-PER`

| ID | Requirement | Pri |
|----|-------------|-----|
| FR-PER-01 | Register periodic jobs with either a fixed interval or a cron expression (5 fields, with an optional seconds field) in a configured time zone | P0 |
| FR-PER-02 | Periodic jobs are enqueued **only by the elected leader**, so a cluster runs one job per tick | P0 |
| FR-PER-03 | `RunOnStart` option. Periodic jobs can be added and removed at runtime | P1 |
| FR-PER-04 | Leader failover MUST NOT produce duplicate ticks, and SHOULD NOT skip more than one tick. Periodic jobs SHOULD be inserted as unique by period | P1 |

### 6.9 Leadership and maintenance — `FR-MNT`

| ID | Requirement | Pri |
|----|-------------|-----|
| FR-MNT-01 | Leader election through a database lease (`leader` table) with a TTL and periodic renewal. Leadership is resigned on graceful shutdown | P0 |
| FR-MNT-02 | **Job scheduler** (leader): promotes due `scheduled`/`retryable` jobs to `available` and notifies | P0 |
| FR-MNT-03 | **Job rescuer** (leader): finds jobs `running` longer than `RescueStuckJobsAfter` and retries or discards them | P0 |
| FR-MNT-04 | **Job cleaner** (leader): deletes finalised jobs past their retention. Defaults: completed 24 h, cancelled 24 h, discarded 7 d. Each can be disabled | P0 |
| FR-MNT-05 | **Queue cleaner** (leader): deletes queue rows not updated within the retention period | P1 |
| FR-MNT-06 | **Reindexer** (leader, opt-in): runs `REINDEX INDEX CONCURRENTLY` on bloat-prone indexes on a schedule | P2 |
| FR-MNT-07 | Maintenance runs in bounded batches so it never takes long-running locks | P0 |

### 6.10 Events and observability — `FR-OBS`

| ID | Requirement | Pri |
|----|-------------|-----|
| FR-OBS-01 | Subscribe to in-process events (`JobCompleted`, `JobFailed`, `JobCancelled`, `JobSnoozed`, `QueuePaused`, `QueueResumed`), with the job row and timing stats (queue wait, run duration, complete duration) | P1 |
| FR-OBS-02 | `ILogger` structured logging with source-generated log messages and stable event IDs | P0 |
| FR-OBS-03 | `System.Diagnostics.Metrics` meter: jobs inserted, started, completed, failed, cancelled and snoozed; execution duration histogram; queue latency; active workers. Tagged with queue and kind | P1 |
| FR-OBS-04 | `ActivitySource` tracing. Trace context is propagated from insert to execution through job metadata | P1 |
| FR-OBS-05 | Health checks (`Microsoft.Extensions.Diagnostics.HealthChecks`) for the database connection, the producers and the notifier | P1 |

### 6.11 Admin / query API — `FR-ADM`

| ID | Requirement | Pri |
|----|-------------|-----|
| FR-ADM-01 | `JobGetAsync(id)` | P0 |
| FR-ADM-02 | `JobListAsync(params)` with filters (kinds, queues, states, ids, priorities, metadata contains), sorting, and **cursor (keyset) pagination** | P1 |
| FR-ADM-03 | `JobDeleteAsync(id)` (a running job cannot be deleted). Bulk `JobDeleteManyAsync(filter)` | P1 |
| FR-ADM-04 | `QueueGetAsync`, `QueueListAsync`, `QueuePauseAsync`, `QueueResumeAsync`, `QueueUpdateAsync(metadata)` | P0 |
| FR-ADM-05 | Every admin operation that changes data has a variant taking a caller transaction | P1 |

### 6.12 Schema migrations — `FR-MIG`

| ID | Requirement | Pri |
|----|-------------|-----|
| FR-MIG-01 | Versioned, forward and backward migrations shipped inside the library. Applied versions are tracked in a migrations table with a "line" (`main`) | P0 |
| FR-MIG-02 | Programmatic API: `MigrateAsync(direction, targetVersion?)`, `GetVersionsAsync`, `ValidateAsync` (is the schema current?) | P0 |
| FR-MIG-03 | A `dotnet` CLI tool (`kopi migrate up|down|list|get --provider postgres|sqlserver`). `get` prints raw SQL so teams can embed it in Flyway, DbUp, SSDT/DACPAC or EF migrations | P0 |
| FR-MIG-04 | An EF Core migration helper (`migrationBuilder.AddKopiQueueSchema(version)`) | P1 |
| FR-MIG-05 | Custom database **schema** support (not only `public` / `dbo`) | P1 |
| FR-MIG-06 | Migrations MUST be safe to run concurrently (PostgreSQL advisory lock, SQL Server `sp_getapplock`) and MUST each run in a transaction where the database allows | P0 |
| FR-MIG-07 | Optionally run migrations on startup, off by default | P1 |

### 6.13 Hosting and configuration — `FR-HST`

| ID | Requirement | Pri |
|----|-------------|-----|
| FR-HST-01 | `services.AddKopiQueue(...)` registers the client and an `IHostedService` that starts and stops it with the host | P0 |
| FR-HST-02 | Configuration through `IOptions<KopiQueueOptions>`, bindable from `IConfiguration` (appsettings) and validated at startup | P0 |
| FR-HST-03 | **Graceful shutdown**: on stop, stop fetching, wait for running jobs up to a soft timeout, then cancel job tokens (hard stop), then flush the completer and resign leadership | P0 |
| FR-HST-04 | Use an application-supplied connection source (`NpgsqlDataSource`, or a SQL Server connection string or connection factory), either a keyed DI service or passed explicitly | P0 |
| FR-HST-05 | Several independent clients in one process (for example two databases) through named or keyed registration | P2 |

### 6.14 Testing support — `FR-TST`

| ID | Requirement | Pri |
|----|-------------|-----|
| FR-TST-01 | `KopiQueue.Testing` package with assertions: `RequireInserted<TArgs>(predicate)`, `RequireManyInserted`, `RequireNotInserted`, all usable within a transaction | P0 |
| FR-TST-02 | `TestWorker<TWorker>` harness to run one job through a worker with the real middleware pipeline, with or without a database | P1 |
| FR-TST-03 | An in-memory/fake insert client for pure unit tests | P1 |
| FR-TST-04 | Helpers for isolated test schemas or databases, for parallel integration tests | P2 |

### 6.15 Database support — `FR-DB`

| ID | Requirement | Pri |
|----|-------------|-----|
| FR-DB-01 | **PostgreSQL driver** (`KopiQueue.Npgsql`) implements every FR in this document | P0 |
| FR-DB-02 | **SQL Server driver** (`KopiQueue.SqlServer`, on `Microsoft.Data.SqlClient`) implements every P0 and P1 FR, except where this table says otherwise | P0 |
| FR-DB-03 | Both drivers expose the **same public API and semantics**: job states, retry, uniqueness, leadership, maintenance. Application code changes only its driver registration (`UseNpgsql()` / `UseSqlServer()`) | P0 |
| FR-DB-04 | SQL Server has no LISTEN/NOTIFY equivalent that works on every target. The SQL Server driver therefore runs **poll-only across processes**, with an **in-process wake-up** after inserts made by the client itself. The poll interval is configurable down to 100 ms | P0 |
| FR-DB-05 | Cross-process control messages on SQL Server (external cancel, pause/resume) are delivered by polling. They MUST take effect within one poll interval | P0 |
| FR-DB-06 | Every driver passes one shared **driver conformance test suite**. A feature is done only when it passes on both drivers | P0 |
| FR-DB-07 | An optional Service Broker based notifier for SQL Server, for lower cross-process latency | P2 |
| FR-DB-08 | River schema compatibility (NFR-COMP-05) applies to the PostgreSQL driver only. The SQL Server schema is KQ's own and is versioned independently | P0 |

---

## 7. Non-functional requirements

### 7.1 Performance — `NFR-PERF`

Targets are measured on a reference environment: Postgres 17, 8 vCPU / 32 GB, a client on the same network, a no-op worker.
They are **initial targets to be validated in milestone M6**.

| ID | Requirement |
|----|-------------|
| NFR-PERF-01 | Single insert inside an existing transaction: one round trip, p50 < 1 ms of added client overhead |
| NFR-PERF-02 | Bulk insert (COPY): ≥ 50,000 jobs/s from one client |
| NFR-PERF-03 | Throughput with no-op workers: ≥ 10,000 jobs/s sustained with one client, scaling roughly linearly to 3 clients |
| NFR-PERF-04 | Pickup latency (insert committed → `WorkAsync` called) with LISTEN/NOTIFY on an idle queue: p50 < 20 ms, p99 < 100 ms. Poll-only, which covers all SQL Server cross-process traffic: bounded by the poll interval |
| NFR-PERF-05 | Steady-state allocation per job executed: target < 2 KB, excluding user args. Tracked with BenchmarkDotNet |
| NFR-PERF-06 | An idle client issues no more than about 1 query per second per queue, plus maintenance |
| NFR-PERF-07 | SQL Server initial targets (spike 0.10, to be re-validated on CI hardware in M5/M7): end-to-end no-op throughput ≥ 5,000 jobs/s per client; batched insert ≥ 30,000 jobs/s; cross-process pickup p50 ≤ poll interval / 2 + 30 ms; an empty poll ≤ 10 logical reads |

### 7.2 Reliability and correctness — `NFR-REL`

| ID | Requirement |
|----|-------------|
| NFR-REL-01 | **At-least-once** delivery: a job that is committed is eventually executed unless it is cancelled or deleted. Workers MUST be documented as needing idempotency |
| NFR-REL-02 | A process crash or network partition MUST NOT lose jobs. Stuck jobs are rescued (FR-MNT-03) |
| NFR-REL-03 | No job reaches a finalised state (`completed`, `cancelled`, `discarded`) unless its final outcome has been durably recorded |
| NFR-REL-04 | A database outage MUST NOT crash the host. The client retries with backoff and reports itself unhealthy |
| NFR-REL-04a | A silently dead LISTEN connection (no FIN or RST, for example a hung load balancer) MUST be detected within 15 s by default, and producers MUST be woken to resync after every reconnect, because notifications sent while disconnected are lost (spike 0.7) |
| NFR-REL-05 | Correctness under concurrency is verified with multi-client stress tests and fault injection (killed connections, killed processes), on both drivers |
| NFR-REL-06 | SQL Server: behaviour must be correct whether READ_COMMITTED_SNAPSHOT is on or off (it is on by default in Azure SQL Database), when the caller's transaction uses SNAPSHOT isolation, and when pooled sessions carry a SNAPSHOT isolation level left by application code (spike 0.9) |

### 7.3 Compatibility — `NFR-COMP`

| ID | Requirement |
|----|-------------|
| NFR-COMP-01 | Target the **current .NET LTS (net10.0)**, plus the newest STS when it is released. Drop each TFM once it is out of support |
| NFR-COMP-02 | PostgreSQL **14 – 18** (every community-supported major version at GA) |
| NFR-COMP-02a | SQL Server **2019, 2022, 2025**, Azure SQL Database and Azure SQL Managed Instance. Database compatibility level ≥ 150 |
| NFR-COMP-03 | Npgsql and Microsoft.Data.SqlClient current major versions. EF Core current major version (for the EF package) |
| NFR-COMP-04 | Works through PgBouncer in transaction mode in poll-only mode, and through managed Postgres (RDS, Aurora, Azure Flexible Server, Cloud SQL, Supabase, Neon) |
| NFR-COMP-05 | PostgreSQL driver: table layout and semantics compatible with a pinned River schema version (OQ-1, decided). **Pinned: migration line `main` v8, River v0.51.2.** This covers wire behaviour too: schema-qualified notification channels and the unique-key algorithm |
| NFR-COMP-06 | Trimming- and Native-AOT-compatible core when args types are registered through a `JsonSerializerContext`. No warnings with `IsAotCompatible=true` |

### 7.4 Usability and API quality — `NFR-API`

| ID | Requirement |
|----|-------------|
| NFR-API-01 | Nullable reference types enabled. Public API tracked with `PublicApiAnalyzers`. No breaking changes within a major version |
| NFR-API-02 | All async APIs take a `CancellationToken` and return `Task` or `ValueTask`. No sync-over-async |
| NFR-API-03 | XML docs on 100% of the public API. A docs site with quick-start, guides and API reference |
| NFR-API-04 | A minimal "hello world" (define a job, register it, enqueue, run) in under 30 lines |

### 7.5 Security — `NFR-SEC`

| ID | Requirement |
|----|-------------|
| NFR-SEC-01 | All SQL is parameterised. Identifiers (schema names) are validated and quoted |
| NFR-SEC-02 | Args are deserialised only into the registered type for a kind. No polymorphic or type-name deserialisation |
| NFR-SEC-03 | Error messages and stack traces stored in `errors` can be truncated or redacted through a hook |
| NFR-SEC-04 | Least-privilege mode: runtime needs only DML on KQ tables, plus LISTEN/NOTIFY on PostgreSQL. DDL is needed only for migrations |
| NFR-SEC-05 | Packages are signed, built deterministically with SourceLink, and ship an SBOM. Dependencies are kept minimal |

### 7.6 Maintainability and operations — `NFR-OPS`

| ID | Requirement |
|----|-------------|
| NFR-OPS-01 | Core package depends only on `Microsoft.Extensions.*.Abstractions` and `System.*`. Npgsql and Microsoft.Data.SqlClient are isolated in their driver packages |
| NFR-OPS-02 | Test coverage ≥ 85% line coverage on core. Every FR has at least one integration test |
| NFR-OPS-03 | Semantic Versioning, with a CHANGELOG and documented upgrade and migration steps for each schema version |

---

## 8. Constraints and assumptions

- C1. PostgreSQL and SQL Server are the supported stores for 1.0.
- C2. **Clean-room implementation.** River is MPL-2.0. KQ will not copy River source files, including its SQL files. We re-implement from public documentation and observed behaviour. For schema compatibility we reproduce the *shape* of the schema, which is an interface, and write our own DDL. We get that shape by applying River's published migrations with River's own CLI to a scratch database and inspecting the result, not by reading River's SQL source. To be confirmed by legal review, see the [Plan](03-plan.md) risks.
- C3. Licence for KQ: **MIT** (OQ-3, decided).
- A1. Users can run migrations, either at deploy time or through the CLI.
- A2. Clocks: database `now()` is the source of truth for scheduling. Client clocks are used only for local timers.

## 9. Acceptance criteria — 1.0 GA

1. Every P0 and P1 requirement is implemented, has an integration test on **both drivers** (except PostgreSQL-only items), and is documented.
2. Performance targets NFR-PERF-01..04 are met, or deviations are documented and accepted.
3. The 72-hour soak test passes: 3 clients, mixed workload, random process kills, and an invariant checker confirms no lost or double-finalised jobs.
4. The sample apps (ASP.NET Core minimal API with EF Core, and a Worker Service) run end to end on PostgreSQL and on SQL Server.
5. The public API is reviewed and frozen, with `PublicAPI.Shipped.txt` committed.
6. A cross-compatibility test where River (Go) inserts and KQ works the jobs, and the reverse, passes on PostgreSQL at the pinned schema version.
7. The driver conformance suite passes on PostgreSQL 14–18, SQL Server 2019/2022/2025, and Azure SQL Database.

## 10. Open questions

| ID | Question | Proposed answer |
|----|----------|-----------------|
| OQ-1 | Should KQ be **schema-compatible with River**? | **Decided 2026-11-26: yes** (PostgreSQL driver), pinned to one River schema version. Default table prefix is `river_` on PostgreSQL and `kopi_` on SQL Server |
| OQ-2 | Product and package name: "KopiQueue"? NuGet ID prefix `KopiQueue.*` | **Decided 2026-11-26:** name `KopiQueue`. The first release claims all 8 package IDs. Apply for the `KopiQueue.*` prefix reservation once public packages exist (plan §6.1) |
| OQ-3 | Licence | **Decided 2026-11-26: MIT** |
| OQ-4 | Should a SQLite driver for local dev and tests be in 1.x? | Post-1.0 (P2) |
| OQ-5 | Dashboard: rely on River UI (PostgreSQL only, enabled by OQ-1) or build a Blazor UI that also covers SQL Server? | Validate River UI first, decide after 1.0. SQL Server users need our own UI either way |
| OQ-6 | Minimum supported Postgres: 14 or 15? | 14 for wider managed-host coverage, raised as versions reach end of life |
| OQ-7 | Minimum SQL Server: 2019 or 2022? | 2019, since it is still in extended support and widely deployed. Use the native `json` type only on 2025 / Azure SQL, behind a feature check |
| OQ-8 | Is a Service Broker notifier (FR-DB-07) worth its operational cost? | Decide post-1.0 from user demand and latency data |
