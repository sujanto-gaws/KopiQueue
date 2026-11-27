# Contributing to KopiQueue

Thank you for helping. Please read this whole page before opening a pull request. **The clean-room policy is mandatory.**

## Clean-room policy (ADR-008)

KopiQueue reimplements the behaviour of [River](https://github.com/riverqueue/river). River is licensed under
**MPL-2.0**, a file-level copyleft licence. KopiQueue is **MIT**. To keep the two licences separate:

1. **Do not copy, translate or closely paraphrase River source code**, including Go code, SQL migrations, queries and tests. "Porting" a file line by line counts as copying.
2. **Allowed sources for behaviour:** River's public documentation, its public API reference (names and signatures), and blog posts and talks. River is a **reference only**: KopiQueue's code, tests and CI never run a River client or River's CLI.
3. **Schema compatibility (PostgreSQL)** is checked against the catalog goldens in `tests/KopiQueue.Npgsql.Tests/Migrations/River/v0.51.2/`, which were recorded once from River's CLI and are now fixed data. Our DDL is written by hand. Behaviour that the docs and the goldens don't settle is an open question for the issue, not a reason to run River.
4. If you have recently read River's implementation of a feature in detail, say so in your PR, and let another contributor write that component.
5. Each PR description must include the confirmation line from the PR template.

When in doubt, ask in the issue before writing code.

## Development setup

- .NET SDK 11 (pinned in `global.json`). Libraries target `net10.0` and `net11.0`.
- Packages restore only from nuget.org (`nuget.config`).
- Tests run on Microsoft.Testing.Platform: `dotnet test --solution KopiQueue.slnx`.

### Databases for tests

Point the tests at existing servers with admin connection strings. Each test class creates and drops its own database.

| Variable | Example |
|----------|---------|
| `KOPI_TEST_PG` | `Host=localhost;Port=5432;Username=postgres;Password=...` |
| `KOPI_TEST_MSSQL` | `Server=localhost;Integrated Security=true;TrustServerCertificate=true` |

If a variable is unset, the fixture starts a Testcontainers container. Override the image with `KOPI_TEST_PG_IMAGE` / `KOPI_TEST_MSSQL_IMAGE`. Without Docker, the tests are skipped locally and fail in CI.

## Pull requests

- Trunk-based: branch from `main` and keep PRs small. We squash merge. Use [Conventional Commits](https://www.conventionalcommits.org/) for titles (`feat(sqlserver): ...`).
- Reference requirement IDs from `docs/01-requirements.md` (for example `FR-INS-02`).
- **Definition of done:** tests, XML docs on public API, `PublicAPI.Unshipped.txt` updated, a CHANGELOG entry, and a green CI on every database version in the matrix.
- Changes to SQL, the public API or concurrency code need **two** approving reviews. SQL changes need a reviewer who knows that database.
- The build treats warnings as errors. Fix analyzer findings rather than suppressing them. If a suppression is truly needed, add a justification.

## Releases

Maintainers release by pushing a `vX.Y.Z[-pre.N]` tag on `main`. The `publish` workflow runs the full CI and then pushes the tested packages to nuget.org. The process and the one-time setup are in [docs/03-plan.md §6.1](docs/03-plan.md#61-publishing-to-nuget).

## Design changes

Requirement changes go through a PR against `docs/01-requirements.md`. Architectural decisions are recorded as ADRs in `docs/02-design.md` §12.
