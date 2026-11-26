# Spikes

Throwaway experiments for M0 tasks 0.7–0.10. This code is **not** part of the solution, is never packed, and may be deleted at any time.
Findings are written up in `docs/spikes/<task>-<topic>.md`. That write-up is the deliverable, not the code.

| Task | Topic | Status |
|------|-------|--------|
| 0.7 | Npgsql LISTEN/NOTIFY, reconnect, PgBouncer | **Done** (PgBouncer deferred to CI) · `NpgsqlSpikes` · [report](../docs/spikes/0.7-npgsql-listen-notify.md) |
| 0.8 | River schema shape via River CLI + introspection | **Done** · `RiverSchema` · [report](../docs/spikes/0.8-river-schema-and-unique-keys.md) |
| 0.9 | SQL Server READPAST fetch under RCSI / SNAPSHOT | **Done** · `SqlServerSpikes` · [report](../docs/spikes/0.9-sqlserver-fetch-and-locking.md) |
| 0.10 | SQL Server unique slot, bulk insert in caller tx, poll cost | **Done** · `SqlServerSpikes` · [report](../docs/spikes/0.10-sqlserver-unique-bulk-poll.md) |

Run the SQL Server spikes with `dotnet run --project spikes/SqlServerSpikes -c Release -- all` (or `fetch`, `isolation`, `escalation`, `plan`, `unique`, `snapshot`, `bulk`, `poll`).
They create and drop `kopi_spike_rc` / `kopi_spike_rcsi` on the server in `KOPI_SPIKE_MSSQL` (default: localhost, Windows auth). Pass `--keep` to keep the databases.

PostgreSQL spikes use `KOPI_SPIKE_PG`, falling back to `KOPI_TEST_PG`:

- **0.7:** `dotnet run --project spikes/NpgsqlSpikes -c Release -- all` (or `latency`, `tx`, `payload`, `names`, `pool`, `terminate`, `blackhole`). It creates and drops `kopi_spike_notify`. `SPIKE_TRACE=1` prints probe traces.
- **0.8**, with River's CLI (`go install github.com/riverqueue/river/cmd/river@v0.51.2`) and Go ≥ 1.24:
  1. `river migrate-up --database-url <url-of-scratch-db>`
  2. `psql -X -f spikes/RiverSchema/introspect.sql <scratch-db>`
  3. `cd spikes/RiverSchema/observe && RIVER_SPIKE_URL=<url> go run . insert|work|golden <out.jsonl>`
  4. `python -I -X utf8 spikes/RiverSchema/golden/verify_unique_keys.py spikes/RiverSchema/golden/river-v0.51.2-unique-keys.jsonl`

  The Go harness uses River only through its public API, and River's source is never read (clean-room, CONTRIBUTING.md). The golden vectors are River's *outputs* and are the M4 acceptance data.
