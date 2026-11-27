# KopiQueue: notes for Claude sessions

KopiQueue is a .NET job queue, schema-compatible with River on PostgreSQL. Start with `docs/README.md`; the requirements, design and plan are `docs/01-requirements.md`, `docs/02-design.md` and `docs/03-plan.md`.

## Clean-room rules (ADR-008, CONTRIBUTING.md)

River is MPL-2.0 and KopiQueue is MIT, so KopiQueue is written without River's implementation.

- **Never read River's source:** not on GitHub (files, trees, raw files, diffs, pull request files), not through `gh api`, not in a clone, not in the Go module cache, not through `go doc -src`, not in a mirror or source browser.
- **Never run River:** not its client, its CLI or River UI, not even as a black box.
- **Allowed sources:** River's documentation (https://riverqueue.com/docs), its public API names, the recorded goldens under `tests/KopiQueue.Npgsql.Tests/Migrations/River/`, and KopiQueue's own docs and code.
- **Where something is unclear,** decide from those sources, or ask on the issue. Don't guess at River's internals.
- `.claude/hooks/block-river-source.js` enforces the first two rules for every tool call, and `.claude/settings.json` adds deny rules for the module cache. Don't work around them; a blocked call means the source isn't allowed.

## The M3 tasks with behaviour specs

These were specified by a session that had read River's source, so the specs state behaviour only. Implement from the spec, the sources above and KopiQueue's code:

| Task | Issue | Spec in `docs/02-design.md` |
|---|---|---|
| 3.3 scheduler | #35 | §5.7, "Scheduler: behaviour spec" |
| 3.4 rescuer | #36 | §5.7, "Rescuer: behaviour spec" |
| 3.5 job cleaner (the queue cleaner is done) | #37 | §5.7, "Job cleaner: behaviour spec" |
| 3.7 external cancel | #39 | §5.8, "External cancel: behaviour spec" |

Each spec ends with open questions for the owner. Where one is unanswered, follow the spec's stated default and say so in the PR.

## Working in this repo

- Build: `dotnet build -c Release`. Unit tests: `dotnet test --project tests/KopiQueue.Tests -c Release`.
- PostgreSQL tests need `KOPI_TEST_PG` (a connection string with a password; never print it). SQL Server tests need `KOPI_TEST_MSSQL`. Don't touch any database other than the test one.
- Branch per task (`m3/<issue>-<slug>`), merge main into the branch to update it (no rebase, no force-push), and open a PR against main. Never merge PRs; the owner does.
- PR descriptions follow `.github/pull_request_template.md`, including the clean-room checkbox.
