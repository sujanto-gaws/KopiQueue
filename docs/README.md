# KopiQueue — Project Documentation

KopiQueue is a background job queue for .NET, backed by PostgreSQL or SQL Server. Its design follows
[riverqueue/river](https://github.com/riverqueue/river), a job queue written in Go. On PostgreSQL it is schema-compatible with River.

| # | Document | Purpose | Status |
|---|----------|---------|--------|
| 1 | [Requirements](01-requirements.md) | What we are building and why: goals, scope, functional and non-functional requirements, acceptance criteria | Draft v0.5 |
| 2 | [Design](02-design.md) | How it works: architecture, packages, database schemas, job lifecycle, algorithms, public API, key decisions | Draft v0.5 |
| 3 | [Plan](03-plan.md) | How we deliver it: milestones, work breakdown, engineering practices, testing, risks, release | Draft v0.5 |

Spike reports (M0): [0.7 Npgsql LISTEN/NOTIFY](spikes/0.7-npgsql-listen-notify.md) · [0.8 River schema and unique keys](spikes/0.8-river-schema-and-unique-keys.md) · [0.9 SQL Server fetch and locking](spikes/0.9-sqlserver-fetch-and-locking.md) · [0.10 SQL Server unique, bulk, polling](spikes/0.10-sqlserver-unique-bulk-poll.md) · raw results: [PostgreSQL](spikes/postgres-raw-results.md), [SQL Server](spikes/sqlserver-raw-results.md)

Benchmark reports: [SQL Server, M5](benchmarks/sqlserver-m5.md) (task 5.9; the tool is `bench/KopiQueue.Benchmarks`)

## Reading order

New contributors should read them in order. Requirements carry stable IDs (`FR-INS-01`, `NFR-PERF-02`, ...).
The design and the plan refer back to those IDs, so every requirement can be traced to a component and to a milestone.

## Conventions

- **MUST / SHOULD / MAY** follow RFC 2119.
- "River" means the upstream Go project. "KopiQueue" or "KQ" means this project.
- Code samples are illustrative until the API is frozen at milestone M6. See [Plan](03-plan.md).
- Open questions are tracked at the end of each document as `OQ-n`.

## Document history

| Version | Date | Change |
|---------|------|--------|
| 0.1 | 2026-11-26 | Initial drafts of requirements, design and plan |
| 0.2 | 2026-11-26 | Decided: River compatibility (PostgreSQL), MIT licence. Added SQL Server as a 1.0 driver (FR-DB-*, design §4.4 and §8.1, ADR-010..013, milestone M5). M0 started |
| 0.3 | 2026-11-26 | SQL Server spikes 0.9/0.10 done: fetch rules (isolation prefix, FORCESEEK, 2,000-row cap), unique inserts via applock (ADR-011 revised), SqlBulkCopy dropped for OPENJSON batches (ADR-014), NFR-PERF-07 targets |
| 0.4 | 2026-11-26 | PostgreSQL spikes 0.7/0.8 done: River schema pinned (`main` v8, River v0.51.2) with DDL fixes, schema-qualified channels, unique-key algorithm (FR-UNQ-06, DQ-8), notifier rules (NFR-REL-04a). M0 complete |
| 0.5 | 2026-11-27 | Clean-room tightened (ADR-008, C2): River is a reference only, and KQ never runs River, its CLI or River UI; compatibility is checked against recorded goldens (plan 7.3 and §7, acceptance criterion 6, OQ-5 decided: own dashboard). M1 released, M2 done, M3 in progress (plan §10) |
