# Changelog

All notable changes to KopiQueue are documented here. The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and versions follow [Semantic Versioning](https://semver.org/).

## [Unreleased]

### Added

- Workers (FR-DEF-02, FR-DEF-04, FR-DEF-06): `IWorker<TArgs>` and the `Worker<TArgs>` base class with `WorkAsync(Job<TArgs>, CancellationToken)` and overridable `Timeout` and `NextRetry`; `Job<TArgs>`, the fetched row with its deserialised args; and `WorkerRegistry`, one worker per kind, added as a type created in each job's DI scope (`Add<TArgs, TWorker>()`, or `Add<TWorker>()` outside trimmed and AOT apps) or as a shared instance. Duplicate kinds, missing or invalid kinds and abstract worker types fail at registration with `KopiQueueConfigurationException`. Nothing runs workers yet; the executor follows in 0.2.0-alpha. ([#22](https://github.com/sujanto-gaws/KopiQueue/issues/22))

### Changed

- Requirements 0.5: a worker no longer sets a `MaxAttempts` default (FR-DEF-04); per-type defaults come from the args type (FR-DEF-05). DQ-1 is closed: `WorkAsync` returns `Task`. ([#22](https://github.com/sujanto-gaws/KopiQueue/issues/22), [#73](https://github.com/sujanto-gaws/KopiQueue/issues/73))

## [0.1.0-alpha.1] - 2026-11-26

First preview (milestone M1, schema and insert): migrate a PostgreSQL database to the River-compatible schema and insert jobs, alone, in bulk, or in the caller's Npgsql or EF Core transaction. Workers and job execution come in 0.2.0-alpha.

### Added

- Driver conformance suite (FR-DB-06, ADR-012): `DriverConformanceTests<TFixture>` holds the insert and migration cases every driver must pass, and the PostgreSQL driver passes them. A new driver adds a fixture and a one-line subclass. ([#21](https://github.com/sujanto-gaws/KopiQueue/issues/21))
- Args serialisation (NFR-COMP-06): `IJobArgsSerializer` and `SystemTextJsonArgsSerializer`, made from a `JsonSerializerContext` for trimmed and Native AOT apps, from `JsonSerializerOptions`, or the reflection-based `Default`; set through the new `KopiClientOptions.ArgsSerializer`. `KopiClientOptions` also takes the `TimeProvider`, which replaces `KopiClient`'s `timeProvider` constructor parameter. ([#20](https://github.com/sujanto-gaws/KopiQueue/issues/20))
- `KopiQueue.EntityFrameworkCore`: `InsertAsync`, `InsertManyAsync` and `InsertManyFastAsync` extension methods that insert jobs in a `DbContext`'s current transaction, so they commit or roll back with its changes. Provider-neutral (EF Core Relational only); a context without a transaction is rejected. ([#19](https://github.com/sujanto-gaws/KopiQueue/issues/19))
- Insert-only use (FR-INS-08): `KopiClient` inserts with no queues, workers or start step, and holds no connection or background work between inserts, so a web front-end can share one client or create one per request. ([#18](https://github.com/sujanto-gaws/KopiQueue/issues/18))
- Bulk insert: `InsertManyAsync` returns the stored jobs in order, and `InsertManyFastAsync` uses PostgreSQL binary `COPY` and returns the count. Both take `InsertRequest(args, options)` items of any job types, validate every job before inserting any, are all-or-nothing, and notify each queue once on commit. ([#17](https://github.com/sujanto-gaws/KopiQueue/issues/17))
- `KopiClient` and `IKopiClient` with `InsertAsync`, on a pooled connection or inside the caller's transaction, returning `InsertResult<TArgs>`. On PostgreSQL the insert notifies `<schema>.river_insert` for each queue with available jobs, in the same transaction, so listeners hear of it only on commit. ([#16](https://github.com/sujanto-gaws/KopiQueue/issues/16))
- Job model: the `IJobArgs` marker and `[JobKind]` attribute; `JobKindRegistry` (kind lookup, duplicate-kind detection); `InsertOptions` (`Queue`, `Priority`, `ScheduledAt`, `MaxAttempts`, `Tags`, `Metadata`) with `InsertDefaults`, validated before insert; `KopiQueueConfigurationException`. ([#15](https://github.com/sujanto-gaws/KopiQueue/issues/15))
- Driver API (experimental, `KQ0001`): `IKopiDriver`, `IKopiExecutor` with `JobInsertManyAsync` and `JobGetByIdAsync`, and `DriverCapabilities`, plus the job model `JobRow`, `JobState` and `AttemptError`. PostgreSQL driver `NpgsqlDriver` with `NpgsqlDriverOptions` (`Schema`, `TablePrefix`), on pooled connections or a caller's transaction. ([#14](https://github.com/sujanto-gaws/KopiQueue/issues/14))
- Migrator API: `KopiMigrator` with `MigrateAsync` (up or down, target version, dry run, lock timeout), `GetVersionsAsync` and `ValidateAsync`. For PostgreSQL, create one with `new NpgsqlDriver(dataSource, options).CreateMigrator()`. ([#13](https://github.com/sujanto-gaws/KopiQueue/issues/13))
- PostgreSQL: the River-compatible migration line `main`, versions 1–8, matching River v0.51.2. Every version leaves the same catalog as River's CLI, up and down, so KopiQueue and River can each continue a schema the other migrated. Works with a custom schema and table prefix. ([#12](https://github.com/sujanto-gaws/KopiQueue/issues/12))
- Migration engine: embedded, versioned up/down SQL per migration line, a lock so concurrent runners apply each version once, and one transaction per migration together with its version row. Failures raise `KopiQueueMigrationException`. ([#11](https://github.com/sujanto-gaws/KopiQueue/issues/11))

[Unreleased]: https://github.com/sujanto-gaws/KopiQueue/compare/v0.1.0-alpha.1...HEAD
[0.1.0-alpha.1]: https://github.com/sujanto-gaws/KopiQueue/releases/tag/v0.1.0-alpha.1
