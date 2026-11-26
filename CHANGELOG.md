# Changelog

All notable changes to KopiQueue are documented here. The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and versions follow [Semantic Versioning](https://semver.org/).

## [Unreleased]

### Added

- Migrator API: `KopiMigrator` with `MigrateAsync` (up or down, target version, dry run, lock timeout), `GetVersionsAsync` and `ValidateAsync`. For PostgreSQL, create one with `NpgsqlKopiMigrator.Create(dataSource, options)`, with optional `Schema` and `TablePrefix`. ([#13](https://github.com/sujanto-gaws/KopiQueue/issues/13))
- PostgreSQL: the River-compatible migration line `main`, versions 1–8, matching River v0.51.2. Every version leaves the same catalog as River's CLI, up and down, so KopiQueue and River can each continue a schema the other migrated. Works with a custom schema and table prefix. ([#12](https://github.com/sujanto-gaws/KopiQueue/issues/12))
- Migration engine: embedded, versioned up/down SQL per migration line, a lock so concurrent runners apply each version once, and one transaction per migration together with its version row. Failures raise `KopiQueueMigrationException`. ([#11](https://github.com/sujanto-gaws/KopiQueue/issues/11))
