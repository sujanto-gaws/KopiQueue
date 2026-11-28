# WorkerService sample

A .NET Worker Service that inserts and works jobs with `KopiQueue.Hosting`, on PostgreSQL or SQL Server. Configuration
alone picks the database: `Sample:Database` is `PostgreSql` (the default) or `SqlServer`.

- `AddKopiQueue` with the driver for that database. That registration is the only code that differs between the two:
  - PostgreSQL: `UseNpgsql()`, on the `NpgsqlDataSource` registered in DI.
  - SQL Server: `UseSqlServer(connectionString)`.
- Queues, timeouts and the notification mode come from the `KopiQueue` section of `appsettings.json`.
- Two workers:
  - `SendEmailWorker` takes a dependency from DI, in a new scope per job.
  - `FlakyWorker` fails its first attempt and is retried about a second later.
- `DemoInserter` inserts a job of each kind every 3 seconds.
- Args are serialised through a source-generated `JsonSerializerContext`, with reflection-based JSON turned off, as trimmed and Native AOT apps need.

## Run

You need a database. The sample migrates it at startup, which suits a demo; real deployments run `kopi migrate`.

On PostgreSQL:

```sh
export ConnectionStrings__Jobs="Host=localhost;Database=kopi_sample;Username=postgres;Password=..."
dotnet run --project samples/WorkerService
```

On SQL Server 2019 or later (the database must exist):

```sh
export ConnectionStrings__Jobs="Server=localhost;Database=kopi_sample;Integrated Security=true;TrustServerCertificate=true"
dotnet run --project samples/WorkerService -- --Sample:Database=SqlServer
```

SQL Server has no LISTEN/NOTIFY, so the client polls every `FetchPollInterval` (1 s by default) for jobs other
processes insert. Jobs this process inserts are fetched at once.

Options:

- **Poll-only mode** on PostgreSQL (for example behind PgBouncer in transaction mode): add `--KopiQueue:NotificationMode=PollOnly`.
- **A separate schema:** add `--Sample:Schema=kopi_sample`. The sample creates it if it's missing. On SQL Server the
  default is `dbo`.

Press Ctrl+C to stop. The client stops fetching, waits for running jobs, writes their outcomes and exits.
