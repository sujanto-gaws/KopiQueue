# Changelog

All notable changes to KopiQueue are documented here. The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and versions follow [Semantic Versioning](https://semver.org/).

## [Unreleased]

### Added

- `KopiClient` and `IKopiClient` with `InsertAsync`, on a pooled connection or inside the caller's transaction, returning `InsertResult<TArgs>`. On PostgreSQL the insert notifies `<schema>.river_insert` for each queue with available jobs, in the same transaction, so listeners hear of it only on commit. ([#16](https://github.com/sujanto-gaws/KopiQueue/issues/16))
- Job model: the `IJobArgs` marker and `[JobKind]` attribute; `JobKindRegistry` (kind lookup, duplicate-kind detection); `InsertOptions` (`Queue`, `Priority`, `ScheduledAt`, `MaxAttempts`, `Tags`, `Metadata`) with `InsertDefaults`, validated before insert; `KopiQueueConfigurationException`. ([#15](https://github.com/sujanto-gaws/KopiQueue/issues/15))
- Driver API (experimental, `KQ0001`): `IKopiDriver`, `IKopiExecutor` with `JobInsertManyAsync` and `JobGetByIdAsync`, and `DriverCapabilities`, plus the job model `JobRow`, `JobState` and `AttemptError`. PostgreSQL driver `NpgsqlDriver` with `NpgsqlDriverOptions` (`Schema`, `TablePrefix`), on pooled connections or a caller's transaction. ([#14](https://github.com/sujanto-gaws/KopiQueue/issues/14))
- Migrator API: `KopiMigrator` with `MigrateAsync` (up or down, target version, dry run, lock timeout), `GetVersionsAsync` and `ValidateAsync`. For PostgreSQL, create one with `new NpgsqlDriver(dataSource, options).CreateMigrator()`. ([#13](https://github.com/sujanto-gaws/KopiQueue/issues/13))
- PostgreSQL: the River-compatible migration line `main`, versions 1–8, matching River v0.51.2. Every version leaves the same catalog as River's CLI, up and down, so KopiQueue and River can each continue a schema the other migrated. Works with a custom schema and table prefix. ([#12](https://github.com/sujanto-gaws/KopiQueue/issues/12))
- Migration engine: embedded, versioned up/down SQL per migration line, a lock so concurrent runners apply each version once, and one transaction per migration together with its version row. Failures raise `KopiQueueMigrationException`. ([#11](https://github.com/sujanto-gaws/KopiQueue/issues/11))
