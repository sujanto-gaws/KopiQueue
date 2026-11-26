# KopiQueue

A transactional background job queue for .NET, backed by **PostgreSQL** or **SQL Server**.
It follows the design of [River](https://github.com/riverqueue/river), and on PostgreSQL it is schema-compatible with River.

> **Status: pre-alpha (milestone M0, foundations).** There is no usable API yet. See the [plan](docs/03-plan.md).

## Why

- **Enqueue jobs in your own transaction.** A job exists only if your business data commits. No dual writes, no broker.
- **Strongly typed jobs**: data-only args records and DI-friendly workers.
- **Safe on many replicas**: skip-locked fetching, leader-elected maintenance, at-least-once delivery.

## Packages (planned)

| Package | Purpose |
|---------|---------|
| `KopiQueue` | Core client, job model and execution engine |
| `KopiQueue.Npgsql` | PostgreSQL driver (River-compatible schema) |
| `KopiQueue.SqlServer` | SQL Server / Azure SQL driver |
| `KopiQueue.Hosting` | `AddKopiQueue()` for the .NET Generic Host |
| `KopiQueue.EntityFrameworkCore` | Enqueue inside a `DbContext` transaction |
| `KopiQueue.OpenTelemetry` | Tracing and metrics registration |
| `KopiQueue.Testing` | Test assertions and worker harness |
| `KopiQueue.Cli` | The `kopi` tool for schema migrations |

## Documentation

- [Requirements](docs/01-requirements.md) · [Design](docs/02-design.md) · [Plan](docs/03-plan.md)

## Building and testing

Requirements: .NET SDK 11 (see `global.json`). Database tests need either an existing server or Docker.

```bash
dotnet build
# Use existing servers (admin connection strings); each test class creates and drops its own database:
export KOPI_TEST_PG="Host=localhost;Port=5432;Username=postgres;Password=..."
export KOPI_TEST_MSSQL="Server=localhost;Integrated Security=true;TrustServerCertificate=true"
dotnet test
```

If a variable is unset, the tests start a Testcontainers container. If Docker is unavailable they are **skipped** locally, and **fail** in CI (`CI=true`).

## License

[MIT](LICENSE). KopiQueue is an independent, clean-room implementation and is not affiliated with the River project.
