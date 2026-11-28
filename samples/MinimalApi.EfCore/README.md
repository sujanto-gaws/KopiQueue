# MinimalApi.EfCore sample

An ASP.NET Core minimal API for a small shop, on PostgreSQL or SQL Server. Configuration alone picks the database:
`Sample:Database` is `PostgreSql` (the default) or `SqlServer`. It shows the transactional enqueue: an order saved with
EF Core and the job that confirms it commit together or not at all (FR-INS-03).

- **`POST /orders`** saves an order and inserts its `send_order_confirmation` job in the same EF Core transaction, with
  `kopi.InsertAsync(args, db)` from `KopiQueue.EntityFrameworkCore`.
  - A job never runs for an order that was rolled back, and an order never misses its job.
  - `?rollback=true` inserts both, then rolls back: `GET` finds neither.
- **`GET /orders/{id}`** shows the order. Its `confirmationSentAt` is set once the job has run.
- **`GET /jobs/{id}`** shows the job's state, attempts and errors, through KopiQueue's admin API (`JobGetAsync`).
- **`SendConfirmationWorker`** gets its own `ShopDb` from a DI scope per job. A job runs at least once, so the worker
  checks whether the confirmation was already sent.
- **Commit interceptor:** `AddKopiQueueCommitInterceptor()` on the context tells the client when a transaction with jobs
  commits, so the job starts at once rather than at the next poll. That matters most on SQL Server, which has no
  LISTEN/NOTIFY: other processes only see new jobs when they poll (every second by default).
- **What differs between the two databases** is only the registrations: EF Core's provider and KopiQueue's driver.
  - On PostgreSQL both share one `NpgsqlDataSource`, and so one connection pool.
  - The endpoints, the worker and the `KopiQueue` configuration section are the same on both.

## Run

You need a database. At startup the sample creates its own table (`shop_orders`) and migrates KopiQueue's, which suits
a demo. Real applications use EF Core migrations for their tables and `kopi migrate` for KopiQueue's.

On PostgreSQL:

```sh
export ConnectionStrings__Shop="Host=localhost;Database=kopi_shop;Username=postgres;Password=..."
dotnet run --project samples/MinimalApi.EfCore
```

On SQL Server 2019 or later (the database must exist):

```sh
export ConnectionStrings__Shop="Server=localhost;Database=kopi_shop;Integrated Security=true;TrustServerCertificate=true"
dotnet run --project samples/MinimalApi.EfCore -- --Sample:Database=SqlServer
```

Add `--Sample:Schema=shop` to put the shop's table and KopiQueue's tables in a schema of their own.

Then, with the URL the app prints:

```sh
curl -X POST http://localhost:5000/orders -H 'Content-Type: application/json' -d '{"email":"buyer@example.com","item":"kopi"}'
# {"orderId":1,"jobId":1}
curl http://localhost:5000/orders/1   # "confirmationSentAt" is set a moment later
curl http://localhost:5000/jobs/1     # "state":"Completed"

curl -X POST 'http://localhost:5000/orders?rollback=true' -H 'Content-Type: application/json' -d '{"email":"x@example.com","item":"never"}'
curl -i http://localhost:5000/jobs/2  # 404: the job was rolled back with its order
```
