# KopiQueue — Project Plan

**Version:** 0.4 (draft) · **Date:** 2026-11-26 · **Related:** [Requirements](01-requirements.md), [Design](02-design.md)

---

## 1. Summary

The plan delivers KopiQueue 1.0 GA on **PostgreSQL and SQL Server** in **8 milestones over about 28 weeks**.
Each milestone ends with a usable preview package on NuGet.

| Milestone | Weeks | Release | Theme |
|-----------|-------|---------|-------|
| M0 Foundations | 1–2 | — | Repo, CI, build, PostgreSQL and SQL Server test infrastructure, spikes |
| M1 Schema and insert | 3–5 | `0.1.0-alpha` | Migrations, PostgreSQL driver, transactional and bulk insert, EF Core insert, **driver conformance suite** |
| M2 Execution engine | 6–9 | `0.2.0-alpha` | Producers, executors, completer, notifier and LocalWaker, retries, cancel/snooze, hosting |
| M3 Leadership and maintenance | 10–12 | `0.3.0-alpha` | Elector, scheduler, rescuer, cleaners, pause/resume, external cancel |
| M4 Unique and periodic | 13–15 | `0.4.0-beta` | Unique jobs, periodic/cron jobs, admin and query API |
| **M5 SQL Server driver** | 16–18 | `0.5.0-beta` | `KopiQueue.SqlServer` passes the full conformance and integration suites |
| M6 DX and observability | 19–22 | `0.6.0-beta` | Testing package, middleware, events, OTel, health checks, CLI, docs site, **API freeze** |
| M7 Hardening and GA | 23–28 | `1.0.0-rc.1` → `1.0.0` | Performance, soak and chaos tests on both drivers, River compatibility suite, Azure SQL and managed PG checks, security review |

**Why SQL Server comes after M4, not in parallel:** the PostgreSQL driver is the reference implementation (ADR-001).
Building the core against one driver first keeps the abstraction honest. The conformance suite written in M1–M4 then becomes the specification the SQL Server driver must pass.
The SQL Server *risks* are still handled early: spikes 0.9 and 0.10 in M0 proved the hard parts (READPAST under RCSI, the unique-slot index, bulk insert in a caller transaction). The design changes they forced are already in the design doc (§8.1, ADR-011, ADR-014).
Also, the M1 driver interface review includes SQL Server, so nothing PostgreSQL-specific leaks into `IKopiExecutor`.
The **API freeze waits until after M5**, so that lessons from SQL Server can still change the API.

**Team assumption:** 2 senior .NET engineers full time, plus 0.5 FTE for QA, docs and DevRel. With a single engineer, expect about 1.8× the duration.

**Target frameworks:** `net10.0` (LTS) and `net11.0` (current STS). .NET 8 and 9 left support in November 2026.

## 2. Decisions

| Decision | Status | Outcome |
|----------|--------|---------|
| OQ-1 River schema compatibility | **Decided 2026-11-26** | Yes, on PostgreSQL, pinned to one River schema version |
| OQ-3 Licence | **Decided 2026-11-26** | MIT |
| SQL Server support | **Decided 2026-11-26** | In 1.0 scope (FR-DB-*), minimum SQL Server 2019 (OQ-7, proposed) |
| OQ-2 Name and NuGet prefix | **Decided 2026-11-26** | Name `KopiQueue`. Publish first (the first release claims all 8 IDs), then apply for the `KopiQueue.*` prefix reservation once public packages exist (§6.1) |
| Clean-room protocol (ADR-008) | M0 task 0.6 | A written policy in CONTRIBUTING.md |
| OQ-6 Minimum Postgres version | Proposed | 14 |

## 3. Repository structure

```
kopi-queue/
├─ docs/                           # these documents, ADRs, spike reports, user guides
├─ src/
│  ├─ KopiQueue/
│  ├─ KopiQueue.Npgsql/            # Sql/*.sql embedded resources (River-compatible schema)
│  ├─ KopiQueue.SqlServer/         # Sql/*.sql embedded resources (KQ schema)
│  ├─ KopiQueue.Hosting/
│  ├─ KopiQueue.EntityFrameworkCore/
│  ├─ KopiQueue.OpenTelemetry/
│  ├─ KopiQueue.Testing/
│  └─ KopiQueue.Cli/
├─ tests/
│  ├─ KopiQueue.Tests/                   # unit
│  ├─ KopiQueue.TestInfrastructure/      # DB fixtures (env var or Testcontainers), shared conformance suite
│  ├─ KopiQueue.Npgsql.Tests/            # conformance suite + PG-specific tests
│  ├─ KopiQueue.SqlServer.Tests/         # conformance suite + SQL Server-specific tests
│  ├─ KopiQueue.IntegrationTests/        # end to end with host, parameterised by driver
│  ├─ KopiQueue.StressTests/             # multi-client, chaos; nightly
│  └─ KopiQueue.CompatTests/             # River ↔ KQ (Go harness in tests/compat/go)
├─ spikes/                         # throwaway M0 spike code (not packed)
├─ bench/KopiQueue.Benchmarks/
├─ samples/
│  ├─ MinimalApi.EfCore/           # runs on either database via configuration
│  └─ WorkerService/
├─ Directory.Build.props
├─ Directory.Packages.props
├─ global.json
└─ .github/workflows/
```

## 4. Milestones and work breakdown

Each task lists the requirement IDs it satisfies. Estimates are in engineer-days (ed).

### M0 — Foundations (weeks 1–2)

| # | Task | ed | Req |
|---|------|----|-----|
| 0.1 | Solution skeleton, `Directory.Build.props` (nullable, `TreatWarningsAsErrors`, `AnalysisLevel=latest-recommended`, `IsAotCompatible` on core and drivers) | 1 | NFR-API-01 |
| 0.2 | Central package management, `global.json`, MinVer for versions from git tags | 0.5 | NFR-OPS-03 |
| 0.3 | GitHub Actions: build, unit tests, PostgreSQL matrix (14–18) and SQL Server matrix (2019/2022/2025) via service containers, pack, coverage | 3 | NFR-COMP-02, -02a |
| 0.4 | Test DB fixtures for both databases: an existing server from `KOPI_TEST_PG` / `KOPI_TEST_MSSQL`, otherwise Testcontainers; a database per test class; transaction rollback per test | 3 | §11 Design |
| 0.5 | `PublicApiAnalyzers`, SourceLink, deterministic builds, symbol packages | 0.5 | NFR-API-01, NFR-SEC-05 |
| 0.6 | LICENSE (MIT), CONTRIBUTING.md (clean-room policy), CODE_OF_CONDUCT, SECURITY.md, issue templates | 0.5 | ADR-008 |
| 0.7 | Spike: Npgsql LISTEN/NOTIFY with `NpgsqlDataSource`, reconnect behaviour, PgBouncer behaviour | 2 | FR-EXE-03 |
| 0.8 | Spike: derive River's schema shape at the pinned version by running River's CLI against a scratch DB and introspecting it. Report: [docs/spikes/0.8](spikes/0.8-river-schema-and-unique-keys.md) | 2 | NFR-COMP-05 |
| 0.9 | Spike: SQL Server fetch under READ COMMITTED with RCSI off and on, and from a SNAPSHOT caller; concurrent fetchers never get the same row; lock escalation check | 2 | NFR-REL-06, FR-EXE-01 |
| 0.10 | Spike: SQL Server `unique_slot` computed column and index (ADR-011) under 100 concurrent inserts; SqlBulkCopy inside a caller `SqlTransaction`; poll cost and latency at 100 ms / 1 s | 2 | FR-UNQ-02, FR-INS-04, NFR-PERF-07 |

**Exit criteria:** green CI on both database matrices; spike reports written in `docs/spikes/`; SQL Server performance targets set (NFR-PERF-07).

### M1 — Schema and insert (weeks 3–5) → `0.1.0-alpha`

| # | Task | ed | Req |
|---|------|----|-----|
| 1.1 | Migration framework: embedded versioned up/down SQL, migration tracking, a driver-supplied lock, per-migration transaction | 3 | FR-MIG-01, -06 |
| 1.2 | PostgreSQL migration DDL (River-compatible, pinned `main` v8) with schema and prefix substitution. CI check: KQ-migrated and River-migrated catalogs are identical (`introspect.sql` diff) | 3.5 | §4.1 Design, FR-MIG-05, NFR-COMP-05 |
| 1.3 | `Migrator` API: `MigrateAsync`, `GetVersionsAsync`, `ValidateAsync` | 1 | FR-MIG-02 |
| 1.4 | `IKopiDriver`/`IKopiExecutor`, `DriverCapabilities`, and the Npgsql driver; **interface review against the SQL Server spike findings** | 4 | §8 Design, FR-DB-03 |
| 1.5 | Job model: `IJobArgs`, `[JobKind]`, kind registry, insert options and validation | 2 | FR-DEF-01, FR-INS-05, -09 |
| 1.6 | `InsertAsync` (pooled and caller transaction), with `pg_notify` inside the transaction | 2 | FR-INS-01, -02, -07 |
| 1.7 | `InsertManyAsync` and `InsertManyFastAsync` (COPY) | 3 | FR-INS-04 |
| 1.8 | Insert-only client mode | 1 | FR-INS-08 |
| 1.9 | EF Core insert extension (provider-neutral) | 1.5 | FR-INS-03 |
| 1.10 | `System.Text.Json` serialiser with `JsonSerializerContext` support | 1.5 | §9 Design, NFR-COMP-06 |
| 1.11 | **Driver conformance suite** scaffolding (`DriverConformanceTests<TFixture>`) plus the insert and migration cases | 3 | FR-DB-06, ADR-012 |

**Exit criteria:** an app can migrate the database and enqueue jobs transactionally with Npgsql and EF Core; River can read the rows.

### M2 — Execution engine (weeks 6–9) → `0.2.0-alpha`

| # | Task | ed | Req |
|---|------|----|-----|
| 2.1 | `IWorker<T>`/`Worker<T>`, worker registry, startup validation | 2 | FR-DEF-02, -04, -06 |
| 2.2 | Notifier (dedicated non-pooled LISTEN connection, `Keepalive`, reconnect with background dispose, wake-all resync, quoted schema-qualified channels, `Auto`/`PollOnly`) and **LocalWaker** | 4.5 | FR-EXE-03, FR-DB-04, NFR-REL-04a |
| 2.3 | Producer: fetch loop, cooldown, poll timer, slot semaphore, refetch when a batch is full | 5 | FR-EXE-01..04 |
| 2.4 | Executor: DI scope per job, deserialisation, timeout, outcome classification, error recording | 4 | FR-DEF-03, FR-EXE-05, -07, -08 |
| 2.5 | `JobControl.Cancel`/`Snooze`, retry policy | 2 | FR-ERR-01..04 |
| 2.6 | BatchCompleter and the Async/Inline variants | 4 | FR-EXE-06, NFR-REL-03 |
| 2.7 | `KopiClient` lifecycle, graceful soft and hard stop | 3 | FR-HST-03 |
| 2.8 | `KopiQueue.Hosting`: `AddKopiQueue`, `IHostedService`, options binding and validation | 3 | FR-HST-01, -02, -04 |
| 2.9 | Tests (conformance cases for fetch and complete; integration tests for concurrency, timeouts, retries, shutdown) | 5 | — |
| 2.10 | **River interop suite**: KQ and a River client (Go harness from spike 0.8) on one schema. KQ insert → River works it and vice versa; notifications in both directions; does an insert-only River client notify; leadership channel usage | 2 | NFR-COMP-05 |
| 2.11 | PgBouncer in CI (Docker): transaction mode in poll-only mode, notifier bypass via `NotifierConnectionString`, session mode | 1 | NFR-COMP-04 |

**Exit criteria:** the WorkerService sample processes jobs end to end on PostgreSQL, including in `PollOnly` mode.

### M3 — Leadership and maintenance (weeks 10–12) → `0.3.0-alpha`

| # | Task | ed | Req |
|---|------|----|-----|
| 3.1 | Elector: attempt, renew, resign, leadership channel, safety margin | 3 | FR-MNT-01 |
| 3.2 | Maintenance host and batch loop with jitter | 2 | FR-MNT-07 |
| 3.3 | Scheduler | 2 | FR-SCH-01, -02, FR-MNT-02 |
| 3.4 | Rescuer | 2 | FR-MNT-03 |
| 3.5 | Job cleaner and queue cleaner | 2 | FR-MNT-04, -05 |
| 3.6 | Queue table maintenance, pause/resume via notification **and** polling | 3 | FR-QUE-02..04, FR-ADM-04, FR-DB-05 |
| 3.7 | External job cancel, including the poll-based path | 3 | FR-ERR-06, FR-DB-05 |
| 3.8 | Tests: leader failover, rescue after crash, retention; conformance cases | 4 | — |

**Exit criteria:** a 3-node cluster survives killing the leader; stuck jobs are rescued; pause propagates within the poll interval when notifications are off.

### M4 — Unique and periodic jobs, admin API (weeks 13–15) → `0.4.0-beta`

| # | Task | ed | Req |
|---|------|----|-----|
| 4.1 | Unique key builder and state bitmask (shared by both drivers). Algorithm from spike 0.8; **acceptance: the 22 River golden vectors**; decide DQ-8 (Go-compatible encoding) | 3.5 | FR-UNQ-01, -04, -06 |
| 4.2 | Unique insert with `ON CONFLICT`, the skipped flag, bulk unique insert | 3 | FR-UNQ-02, -03, -05 |
| 4.3 | Periodic jobs (interval and cron via Cronos), time zones, `RunOnStart`, unique per tick | 4 | FR-PER-01..04 |
| 4.4 | Admin API with keyset cursors and transaction variants | 4 | FR-ADM-01..05, FR-ERR-07 |
| 4.5 | Args-level default insert options | 1 | FR-DEF-05 |
| 4.6 | Tests: concurrent unique inserts, periodic jobs across failover, pagination; conformance cases | 3 | — |

**Exit criteria:** UC-3, UC-4 and UC-7 are demonstrated; the conformance suite covers every `IKopiExecutor` operation.

### M5 — SQL Server driver (weeks 16–18) → `0.5.0-beta`

| # | Task | ed | Req |
|---|------|----|-----|
| 5.1 | SQL Server migration DDL (§4.4 Design), `sp_getapplock` locking, schema/prefix substitution, SET-options check | 4 | FR-MIG-*, FR-DB-02 |
| 5.2 | `SqlServerDriver`/executor: connection factory, `SqlTransaction` handling, `JobRow` reader, UTC conversion | 3 | FR-DB-02, FR-INS-02 |
| 5.3 | Insert, fetch and complete statements (§8.1 Design), OPENJSON batches | 5 | FR-EXE-01, -06 |
| 5.4 | Unique insert (`sp_getapplock` guard, `READCOMMITTEDLOCK` reads, `unique_slot`) | 3 | FR-UNQ-*, ADR-011 |
| 5.5 | Batched OPENJSON bulk insert (≤ 1,000 rows/statement) for `InsertManyAsync` / `InsertManyFastAsync` | 1.5 | FR-INS-04, ADR-014 |
| 5.6 | Leader, maintenance, queue and admin statements (`TOP`/`READPAST`, keyset pagination) | 4 | FR-MNT-*, FR-ADM-* |
| 5.7 | Poll-only control paths and EF Core commit interceptor for LocalWaker | 2 | FR-DB-04, -05 |
| 5.8 | Wire the SQL Server fixtures into the conformance, integration and sample suites; fix every failure. Add **plan-shape tests** (seek, no lock escalation) and **pooled-SNAPSHOT-session tests** for every locking statement (spike 0.9 rules) | 4.5 | FR-DB-06, NFR-REL-06 |
| 5.9 | Benchmark OPENJSON vs TVP (DQ-6) and the state column type (DQ-7); reduce busy-fetch cost (about 950 logical reads per 25 jobs in spike 0.10); first SQL Server performance baseline | 2.5 | NFR-PERF-07 |

**Exit criteria:** the conformance and integration suites pass on SQL Server 2019/2022/2025 in CI; both samples run on SQL Server by configuration alone.

### M6 — Developer experience and observability (weeks 19–22) → `0.6.0-beta` + **API freeze**

| # | Task | ed | Req |
|---|------|----|-----|
| 6.1 | Worker and insert middleware pipelines | 2 | FR-EXE-09, FR-INS-10 |
| 6.2 | Error handler hooks | 1 | FR-ERR-05 |
| 6.3 | Event subscriptions | 2 | FR-OBS-01 |
| 6.4 | Logging, metrics and tracing (`db.system` attribute per driver) | 3 | FR-OBS-02..04 |
| 6.5 | `KopiQueue.OpenTelemetry` and health checks, including notifier health (connected, last reconnect) | 1.5 | FR-OBS-05, NFR-REL-04a |
| 6.6 | `KopiQueue.Testing`: assertions, `TestWorker`, fake client (works on both drivers) | 4 | FR-TST-01..03 |
| 6.7 | CLI `kopi` with `--provider`, EF migration helper for both providers, migrate-on-startup option | 3.5 | FR-MIG-03, -04, -07 |
| 6.8 | Docs site, including database pages: "PostgreSQL" and "SQL Server / Azure SQL" (polling latency, isolation, permissions) | 6 | NFR-API-03, -04 |
| 6.9 | API review; commit `PublicAPI.Shipped.txt` | 2 | NFR-API-01 |

**Exit criteria:** the public API is frozen.

### M7 — Hardening and GA (weeks 23–28) → `1.0.0-rc.1` → `1.0.0`

| # | Task | ed | Req |
|---|------|----|-----|
| 7.1 | Benchmarks and tuning on both drivers | 8 | NFR-PERF-01..07 |
| 7.2 | Stress and chaos suite on both drivers (killed connections and processes, DB restart, SQL Server failover); **72 h soak per driver before GA** | 6 | NFR-REL-01..06 |
| 7.3 | River compatibility suite (Go ↔ KQ) and a River UI smoke test | 4 | NFR-COMP-05 |
| 7.4 | Managed-service verification: RDS, Azure Flexible, Neon/Supabase, PgBouncer; **Azure SQL Database and Managed Instance** | 4 | NFR-COMP-02a, -04 |
| 7.5 | Trimming and Native AOT validation | 2 | NFR-COMP-06 |
| 7.6 | Security review on both drivers | 2.5 | NFR-SEC-01..05 |
| 7.7 | Docs polish, upgrade guide, Hangfire/Quartz migration guides, CHANGELOG | 3 | NFR-API-03 |
| 7.8 | Bug-fix buffer | 6 | — |

**Exit criteria:** the acceptance criteria in §9 of [Requirements](01-requirements.md) are met.

### Post-1.0 backlog (P2, not scheduled)

- SQL Server Service Broker notifier (FR-DB-07, OQ-8).
- Native `json` column type on SQL Server 2025 / Azure SQL (OQ-7).
- SQLite driver (OQ-4). Dashboard (OQ-5).
- FR-QUE-05, FR-HST-05, FR-DEF-07, FR-EXE-10, FR-MNT-06, FR-TST-04.
- Workflows, sequences, concurrency limits (NG3; reconsider after adoption).

## 5. Effort roll-up

| Milestone | ed |
|-----------|----|
| M0 | 16.5 |
| M1 | 25.5 |
| M2 | 35.5 |
| M3 | 21 |
| M4 | 18.5 |
| M5 | 29.5 |
| M6 | 25 |
| M7 | 35.5 |
| **Total** | **≈ 207 ed** |

At 2 FTE that is about 101 working days, or 20 weeks, of pure task time. The 28-week schedule leaves about 29% slack.
Adding SQL Server costs about 44 ed and 4 calendar weeks compared with plan v0.1.

## 6. Engineering practices

| Area | Practice |
|------|----------|
| Branching | Trunk-based. Short-lived feature branches. Squash merge. Conventional Commits |
| Reviews | One approving review required. Two for SQL, public API or concurrency changes. SQL changes need a reviewer who knows that database |
| Definition of done | Code, tests (conformance cases for any new executor operation, **green on both drivers** once M5 is done), XML docs, a CHANGELOG entry, requirement IDs in the PR description |
| Quality gates | Analyzers clean, `PublicAPI.Unshipped.txt` updated, coverage does not drop, no new AOT/trim warnings |
| Versioning | SemVer from git tags (MinVer). Schema versions are tracked per driver, with a documented mapping to package versions |
| Releases | Tag on `main` → full CI → the tested packages are pushed to NuGet (trusted publishing), then a GitHub release is created. See §6.1. No author signing: nuget.org adds its repository signature |
| Supply chain | Central package management, Dependabot, vulnerable-package check, a CycloneDX SBOM per release |

### 6.1 Publishing to NuGet

Releases are published by [`.github/workflows/publish.yml`](../.github/workflows/publish.yml):

1. **Trigger:** a tag `vX.Y.Z` or `vX.Y.Z-pre.N` pushed on a commit on `main`. MinVer sets the package version from the tag.
2. **Checks:** the workflow runs the full CI ([`ci.yml`](../.github/workflows/ci.yml): build, pack, PostgreSQL 14–18, SQL Server 2019–2025) as a reusable workflow. It then publishes **the exact `.nupkg` files CI built and tested**; nothing is rebuilt. Two guards run before the push:
   - the tag must be on `main`;
   - all 8 package IDs must exist at the tag's version.
3. **Push:** to nuget.org, with symbol packages, using `--skip-duplicate`, so a re-run after a partial failure is safe. Then a GitHub release is created, with generated notes and the packages attached. A tag containing `-` is marked as a prerelease.
4. **Gate:** the job runs in the GitHub environment `nuget`. Required reviewers on that environment turn every release into a one-click approval.

**Credentials.** No long-lived key is needed once trusted publishing works:

| Option | Setup |
|--------|-------|
| **Trusted publishing** (preferred) | On nuget.org, open your profile, then *Trusted Publishing*, and add a policy: owner `sujanto-gaws`, repository `KopiQueue`, workflow file `publish.yml`, environment `nuget`. In GitHub, set the **repository variable** `NUGET_USER` to the nuget.org profile name (not the email address). The workflow then exchanges its OIDC token for an API key that is valid for 1 hour |
| API key (fallback) | Create a nuget.org API key with only *Push new packages and package versions* rights, glob `KopiQueue*`, and the shortest expiry that is practical. Store it as the **environment secret** `NUGET_API_KEY` on `nuget`. Used only when `NUGET_USER` is not set. Delete it once trusted publishing has succeeded |

nuget.org's documentation does not say whether a trusted publishing policy covers the *first* push of a brand-new package ID. If the first release fails at the push step, use the API-key fallback for that one release.

**Release checklist**

1. `main` is green. `CHANGELOG.md` is updated, and `PublicAPI.Unshipped.txt` entries move to `Shipped` for stable releases.
2. Tag and push: `git tag v0.1.0-alpha.1 && git push origin v0.1.0-alpha.1`.
3. If the environment has reviewers, approve the `publish` run.
4. Check the packages on nuget.org. Validation and indexing take a few minutes.

**Versions are permanent.** A published version cannot be deleted, only unlisted, and its number can never be reused. A broken release is fixed by publishing the next version.

**Package names and the prefix (OQ-2).** The first release claims all 8 IDs: `KopiQueue`, `.Npgsql`, `.SqlServer`, `.Hosting`, `.EntityFrameworkCore`, `.OpenTelemetry`, `.Testing` and `.Cli`. Once there are public packages, which are the evidence nuget.org's criteria ask for, apply for the `KopiQueue.*` prefix reservation (email account@nuget.org) to block look-alike IDs and get the verified mark. The planned first release is `0.1.0-alpha.1` at the end of M1. An earlier placeholder prerelease is only worth it if a squatter appears.

## 7. Test strategy

| Suite | Runs | Scope | Target |
|-------|------|-------|--------|
| Unit | Every PR | Pure logic with `FakeTimeProvider` | < 30 s |
| Driver conformance | Every PR: PG 14–18 and SQL Server 2019/2022/2025. Nightly: Azure SQL | Every executor operation; transaction rollback per test | < 3 min per database version |
| Integration | Every PR (PG 18 + SQL Server 2022), nightly (full matrix) | Host with real producers, completer and elector | < 5 min per driver |
| Stress/chaos | Nightly, plus 72 h per driver before GA | Multi-process clients, kill -9, killed sessions (`pg_terminate_backend` / `KILL`), DB restart; invariant checker | No invariant violations |
| River compatibility | Nightly | Go River ↔ KQ on a shared PostgreSQL database | Pass |
| Benchmarks | Weekly and before each release | BenchmarkDotNet plus `kopi bench`, per driver | Regressions > 10% fail the release checklist |

**Local development:** tests read `KOPI_TEST_PG` / `KOPI_TEST_MSSQL` connection strings for existing local servers. When these are unset they start Testcontainers, which needs Docker.

**Invariants checked by the stress suite** (both drivers):

1. No job id is ever executing on two clients at once.
2. Every inserted job ends up finalised or still retryable. None is lost.
3. `finalized_at` is consistent with the job state.
4. There is at most one leader at any time, apart from the documented overlap window.
5. There is never more than one occupied row per unique key.

## 8. Risks

| # | Risk | Likelihood | Impact | Mitigation |
|---|------|-----------|--------|------------|
| R1 | Licence contamination from MPL-2.0 River code | Low | High | Clean-room policy; schema derived by introspection (spike 0.8); legal review |
| R2 | The River schema changes upstream | Medium | Medium | A pinned schema version; nightly compatibility suite |
| R3 | LISTEN/NOTIFY is unreliable in some hosted PostgreSQL environments | High | Medium | First-class `PollOnly` mode, documented and tested |
| R4 | Performance targets missed | Medium | Medium | Benchmarks from M2; batch completer; prepared statements |
| R5 | Concurrency bugs (double execution, lost jobs) | Medium | High | DB-enforced invariants, chaos suite, two reviewers |
| R6 | Table and index bloat (PG) or fragmentation (SQL Server) at high throughput | Medium | Medium | Cleaner batches, filtered indexes, optional reindexer, documented tuning |
| R7 | API churn after previews | Medium | Medium | API freeze at M6 (after SQL Server), `[Experimental]` seams |
| R8 | Scope creep | High | Medium | NG3 explicit; changes go through the requirements doc |
| R9 | Team capacity (bus factor of 2; two databases to know deeply) | Medium | High | ADRs, paired reviews, per-database docs pages |
| R10 | **SQL Server locking semantics** (READPAST under RCSI/SNAPSHOT, lock escalation, deadlocks between fetch and completer) | Medium → **Low** after spikes 0.9/0.10 | High | Done: READPAST verified under RCSI on and off; pool-leaked SNAPSHOT handled by a batch-scoped SET; plan flip and escalation fixed with FORCESEEK and a 2,000-row cap; no deadlocks seen. Remaining: plan-shape tests (5.8), deadlock retry (1205), chaos suite, Azure SQL check |
| R11 | **SQL Server pickup latency** from cross-process polling disappoints users | Medium | Medium | LocalWaker plus EF interceptor; poll interval down to 100 ms; documented trade-off; Service Broker on the post-1.0 backlog |
| R12 | Driver abstraction leaks PostgreSQL assumptions and SQL Server needs core changes late | Medium | High | SQL Server spikes in M0; interface review in M1; conformance suite from M1; API freeze after M5 |

## 9. Communication and governance

- **Weekly:** a 30-minute sync covering milestone burn-down, blockers and decisions. Decisions are recorded as ADRs.
- **Per milestone:** a demo of the samples on each supported database, a preview release and a retro.
- **Requirement changes:** a PR against `01-requirements.md` that bumps the doc version and updates this plan.

## 10. Immediate next steps

1. ~~Reserve the `KopiQueue` NuGet prefix~~ → **publish first, reserve later** (OQ-2, §6.1). Publish workflow added. Before the first release:
   - create the nuget.org account or organization;
   - add the trusted publishing policy and the `NUGET_USER` variable (`NUGET_USER` set 2026-11-26);
   - ~~create the `nuget` environment~~ (done 2026-11-26).
   First release: `v0.1.0-alpha.1` at the end of M1 ([#81](https://github.com/sujanto-gaws/KopiQueue/issues/81)).
2. ~~M0 tasks 0.1–0.6~~: **done 2026-11-26.** Solution skeleton (8 src projects, `net10.0;net11.0`), central packages, MinVer, PublicApiAnalyzers, repo-local `nuget.config`, CI workflow, DB fixtures, licence and contributor docs. Tests run on Microsoft.Testing.Platform (xunit.v3 4.x requires it). CI first ran green on 2026-11-26 (github.com/sujanto-gaws/KopiQueue).
3. ~~Spikes 0.9–0.10 (SQL Server)~~: **done 2026-11-26** ([0.9](spikes/0.9-sqlserver-fetch-and-locking.md), [0.10](spikes/0.10-sqlserver-unique-bulk-poll.md)). Design updated (§8.1, ADR-011 revised, ADR-014), NFR-PERF-07 set.
4. ~~Spikes 0.7–0.8 (PostgreSQL)~~: **done 2026-11-26** ([0.7](spikes/0.7-npgsql-listen-notify.md), [0.8](spikes/0.8-river-schema-and-unique-keys.md)). River schema pinned at `main` v8 (v0.51.2); unique-key algorithm reconstructed and verified on 22 golden vectors; notifier rules (non-pooled, Keepalive, resync). Design updated (§3, §4.1, §4.3, §5.6, §7, ADR-002, DQ-4 closed, DQ-8 new). **PgBouncer not run** (no Docker): moved to task 2.11.
5. ~~M0~~: **done**; the PgBouncer check moved to M2 (task 2.11).
6. ~~M1 tasks 1.1–1.11~~: **done 2026-11-26** (migrations, Npgsql driver, insert and bulk insert, EF Core insert, args serialisation, conformance suite; DQ-2 closed). `CHANGELOG.md` cut for `0.1.0-alpha.1`. Next: tag the release, then M2 (execution engine), starting with task 2.1 and DQ-1/DQ-3.
