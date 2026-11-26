# WorkerService sample

A .NET Worker Service that inserts and works jobs on PostgreSQL with `KopiQueue.Hosting`:

- `AddKopiQueue` with `UseNpgsql()`, on the `NpgsqlDataSource` registered in DI.
- Queues, timeouts and the notification mode come from the `KopiQueue` section of `appsettings.json`.
- Two workers:
  - `SendEmailWorker` takes a dependency from DI, in a new scope per job.
  - `FlakyWorker` fails its first attempt and is retried about a second later.
- `DemoInserter` inserts a job of each kind every 3 seconds.
- Args are serialised through a source-generated `JsonSerializerContext`, with reflection-based JSON turned off, as trimmed and Native AOT apps need.

## Run

You need a PostgreSQL database. The sample migrates it at startup, which suits a demo; real deployments run `kopi migrate`.

```sh
export ConnectionStrings__Jobs="Host=localhost;Database=kopi_sample;Username=postgres;Password=..."
dotnet run --project samples/WorkerService
```

Options:

- **Poll-only mode** (for example behind PgBouncer in transaction mode): `dotnet run --project samples/WorkerService -- --KopiQueue:NotificationMode=PollOnly`.
- **A separate schema:** add `--Sample:Schema=kopi_sample`.

Press Ctrl+C to stop. The client stops fetching, waits for running jobs, writes their outcomes and exits.
