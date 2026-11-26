"""Create GitHub milestones, labels and issues from docs/03-plan.md.

Usage (from the repo root, with an authenticated `gh`):
    python -I -X utf8 tools/plan-to-issues.py . --dry-run     # print what would be created
    python -I -X utf8 tools/plan-to-issues.py .               # create it
    python -I -X utf8 tools/plan-to-issues.py . --repo owner/name

Idempotent: labels, milestones and issues are matched by name or title and only missing ones are created.
Existing issues are never edited, so a changed task text in the plan does not update its issue.
A new plan task needs a short title in TITLES and areas in AREAS; the script stops if one is missing.
"""
import json
import re
import subprocess
import sys
import time

ROOT = sys.argv[1]
DRY = "--dry-run" in sys.argv
REPO = sys.argv[sys.argv.index("--repo") + 1] if "--repo" in sys.argv else "sujanto-gaws/KopiQueue"
BLOB = f"https://github.com/{REPO}/blob/main"
PLAN = f"{BLOB}/docs/03-plan.md"
DESIGN = f"{BLOB}/docs/02-design.md"
REQS = f"{BLOB}/docs/01-requirements.md"


def gh(*args, body=None):
    if DRY and args[0] == "api" and any(a in ("POST", "PATCH") for a in args):
        print("DRY", " ".join(args[:4]), (json.loads(body).get("title") if body else ""))
        return {"number": 0}
    p = subprocess.run(["gh", *args], input=body, capture_output=True, text=True, encoding="utf-8")
    if p.returncode != 0:
        sys.exit(f"gh {' '.join(args[:4])} failed: {p.stderr.strip()}")
    return json.loads(p.stdout) if p.stdout.strip() else None


def api(method, path, payload):
    out = gh("api", "-X", method, f"repos/{REPO}/{path}", "--input", "-", body=json.dumps(payload))
    if not DRY:
        time.sleep(1.2)  # stay well under GitHub's content-creation rate limits
    return out


# ---------------------------------------------------------------- parse the plan
plan = open(f"{ROOT}/docs/03-plan.md", encoding="utf-8").read()
sections = re.split(r"^### (M\d) — ", plan, flags=re.M)
milestones = {}  # "M1" -> dict
for i in range(1, len(sections), 2):
    key, text = sections[i], sections[i + 1]
    heading = text.splitlines()[0]
    m = re.match(r"(.+?) \(weeks ([\d–]+)\)(?: → (.+))?$", heading)
    name, weeks, release = m.group(1), m.group(2), (m.group(3) or "").replace("`", "")
    release = re.sub(r"\s*\+.*$", "", release).strip()
    exit_m = re.search(r"\*\*Exit criteria:\*\* (.+)", text)
    tasks = []
    for row in re.finditer(r"^\| (\d+\.\d+) \| (.+?) \| ([\d.]+) \| (.*?) \|$", text, flags=re.M):
        tasks.append({"id": row.group(1), "text": row.group(2), "ed": float(row.group(3)), "req": row.group(4)})
    milestones[key] = {"name": name, "weeks": weeks, "release": release,
                       "exit": exit_m.group(1).strip() if exit_m else "", "tasks": tasks}
assert len(milestones) == 8, milestones.keys()

# ---------------------------------------------------------------- curated metadata
TITLES = {
    "0.1": "Solution skeleton and Directory.Build.props", "0.2": "Central package management, global.json, MinVer",
    "0.3": "GitHub Actions CI with PostgreSQL and SQL Server matrices", "0.4": "Test database fixtures for both databases",
    "0.5": "PublicApiAnalyzers, SourceLink, deterministic builds, symbols", "0.6": "Licence, contributing, conduct, security, issue templates",
    "0.7": "Spike: Npgsql LISTEN/NOTIFY, reconnect, PgBouncer", "0.8": "Spike: River schema shape and unique keys",
    "0.9": "Spike: SQL Server fetch and locking (READPAST, RCSI, SNAPSHOT)", "0.10": "Spike: SQL Server unique slot, bulk insert, poll cost",
    "1.1": "Migration framework (versioned up/down, tracking, lock)", "1.2": "PostgreSQL migration DDL (River main v8) + catalog-diff CI check",
    "1.3": "Migrator API: MigrateAsync, GetVersionsAsync, ValidateAsync", "1.4": "Driver interfaces and the Npgsql driver",
    "1.5": "Job model: IJobArgs, [JobKind], kind registry, insert options", "1.6": "InsertAsync (pooled and caller transaction) with pg_notify",
    "1.7": "InsertManyAsync and InsertManyFastAsync (COPY)", "1.8": "Insert-only client mode",
    "1.9": "EF Core insert extension", "1.10": "System.Text.Json serialiser with JsonSerializerContext support",
    "1.11": "Driver conformance suite scaffolding + insert/migration cases",
    "2.1": "Worker interfaces, registry and startup validation", "2.2": "Notifier and LocalWaker",
    "2.3": "Producer: fetch loop, cooldown, poll timer, slots", "2.4": "Executor: DI scope, deserialisation, timeout, outcomes",
    "2.5": "JobControl Cancel/Snooze and retry policy", "2.6": "BatchCompleter and Async/Inline variants",
    "2.7": "KopiClient lifecycle, soft and hard stop", "2.8": "KopiQueue.Hosting: AddKopiQueue, hosted service, options",
    "2.9": "Execution tests: conformance and integration", "2.10": "River interop suite (KQ ↔ River on one schema)",
    "2.11": "PgBouncer in CI", "3.1": "Elector: attempt, renew, resign, safety margin",
    "3.2": "Maintenance host and batch loop", "3.3": "Scheduler", "3.4": "Rescuer", "3.5": "Job cleaner and queue cleaner",
    "3.6": "Queue maintenance, pause/resume via notification and polling", "3.7": "External job cancel (incl. poll-based path)",
    "3.8": "Tests: leader failover, rescue after crash, retention",
    "4.1": "Unique key builder and state bitmask (River golden vectors)", "4.2": "Unique insert with ON CONFLICT and bulk unique insert",
    "4.3": "Periodic jobs (interval and cron)", "4.4": "Admin API with keyset cursors", "4.5": "Args-level default insert options",
    "4.6": "Tests: concurrent unique inserts, periodic jobs, pagination",
    "5.1": "SQL Server migration DDL and migration lock", "5.2": "SqlServerDriver and executor foundations",
    "5.3": "SQL Server insert, fetch and complete statements", "5.4": "SQL Server unique insert (sp_getapplock)",
    "5.5": "SQL Server batched OPENJSON bulk insert", "5.6": "SQL Server leader, maintenance, queue and admin statements",
    "5.7": "SQL Server poll-only control paths and EF commit interceptor", "5.8": "SQL Server in the conformance/integration suites + plan-shape tests",
    "5.9": "SQL Server benchmarks (DQ-6, DQ-7) and busy-fetch cost",
    "6.1": "Worker and insert middleware pipelines", "6.2": "Error handler hooks", "6.3": "Event subscriptions",
    "6.4": "Logging, metrics and tracing", "6.5": "KopiQueue.OpenTelemetry and health checks",
    "6.6": "KopiQueue.Testing package", "6.7": "kopi CLI and EF migration helper", "6.8": "Documentation site",
    "6.9": "API review and API freeze",
    "7.1": "Benchmarks and tuning on both drivers", "7.2": "Stress, chaos and 72 h soak tests", "7.3": "River compatibility suite and River UI smoke test",
    "7.4": "Managed-service verification (PG hosts, PgBouncer, Azure SQL)", "7.5": "Trimming and Native AOT validation",
    "7.6": "Security review", "7.7": "Docs polish, upgrade and migration guides, CHANGELOG", "7.8": "Bug-fix buffer",
}
AREAS = {
    "0.1": ["ci"], "0.2": ["ci"], "0.3": ["ci"], "0.4": ["tests"], "0.5": ["ci"], "0.6": ["docs"],
    "0.7": ["notifier", "driver: postgres"], "0.8": ["river-compat"], "0.9": ["driver: sqlserver"], "0.10": ["driver: sqlserver"],
    "1.1": ["migrations"], "1.2": ["migrations", "driver: postgres", "river-compat"], "1.3": ["migrations"],
    "1.4": ["core", "driver: postgres"], "1.5": ["core"], "1.6": ["core", "driver: postgres"], "1.7": ["driver: postgres"],
    "1.8": ["core"], "1.9": ["efcore"], "1.10": ["core", "platforms"], "1.11": ["tests"],
    "2.1": ["execution"], "2.2": ["notifier", "driver: postgres"], "2.3": ["execution"], "2.4": ["execution"],
    "2.5": ["execution"], "2.6": ["execution"], "2.7": ["core"], "2.8": ["hosting"], "2.9": ["tests", "execution"],
    "2.10": ["river-compat", "tests"], "2.11": ["ci", "driver: postgres"],
    "3.1": ["maintenance"], "3.2": ["maintenance"], "3.3": ["maintenance"], "3.4": ["maintenance"], "3.5": ["maintenance"],
    "3.6": ["maintenance", "admin-api"], "3.7": ["admin-api", "execution"], "3.8": ["tests", "maintenance"],
    "4.1": ["unique", "river-compat"], "4.2": ["unique", "driver: postgres"], "4.3": ["periodic"], "4.4": ["admin-api"],
    "4.5": ["core"], "4.6": ["tests", "unique", "periodic"],
    "5.1": ["driver: sqlserver", "migrations"], "5.2": ["driver: sqlserver"], "5.3": ["driver: sqlserver"],
    "5.4": ["driver: sqlserver", "unique"], "5.5": ["driver: sqlserver"], "5.6": ["driver: sqlserver", "maintenance"],
    "5.7": ["driver: sqlserver", "efcore"], "5.8": ["driver: sqlserver", "tests"], "5.9": ["driver: sqlserver", "perf"],
    "6.1": ["core"], "6.2": ["core"], "6.3": ["observability"], "6.4": ["observability"], "6.5": ["observability"],
    "6.6": ["testing-pkg"], "6.7": ["cli", "migrations", "efcore"], "6.8": ["docs"], "6.9": ["core"],
    "7.1": ["perf"], "7.2": ["tests"], "7.3": ["river-compat"], "7.4": ["platforms"], "7.5": ["platforms"],
    "7.6": ["security"], "7.7": ["docs"], "7.8": ["core"],
}
TYPE_OVERRIDE = {"0.7": "spike", "0.8": "spike", "0.9": "spike", "0.10": "spike",
                 "1.11": "test", "2.9": "test", "2.10": "test", "2.11": "test", "3.8": "test", "4.6": "test",
                 "5.8": "test", "7.2": "test", "7.3": "test", "7.4": "test"}
M0_DONE = {
    "0.7": "Report: [docs/spikes/0.7](%s/docs/spikes/0.7-npgsql-listen-notify.md). The PgBouncer part needs Docker and moved to task 2.11." % BLOB,
    "0.8": "Report: [docs/spikes/0.8](%s/docs/spikes/0.8-river-schema-and-unique-keys.md)." % BLOB,
    "0.9": "Report: [docs/spikes/0.9](%s/docs/spikes/0.9-sqlserver-fetch-and-locking.md)." % BLOB,
    "0.10": "Report: [docs/spikes/0.10](%s/docs/spikes/0.10-sqlserver-unique-bulk-poll.md). SqlBulkCopy was rejected (ADR-014)." % BLOB,
}

AREA_COLOR, TYPE_COLORS = "c5def5", {"task": "1d76db", "spike": "5319e7", "test": "0e8a16", "design-question": "d4c5f9", "release": "fbca04"}
LABELS = {f"type: {k}": (v, d) for k, v, d in [
    ("task", TYPE_COLORS["task"], "Planned implementation work"), ("spike", TYPE_COLORS["spike"], "Time-boxed experiment; the report is the deliverable"),
    ("test", TYPE_COLORS["test"], "Test suites, conformance and verification work"),
    ("design-question", TYPE_COLORS["design-question"], "Open design question (DQ-n in the design doc)"),
    ("release", TYPE_COLORS["release"], "Release checklist")]}
for a, d in [("core", "KopiQueue core library"), ("migrations", "Schema migrations and the Migrator"), ("driver: postgres", "KopiQueue.Npgsql"),
             ("driver: sqlserver", "KopiQueue.SqlServer"), ("notifier", "LISTEN/NOTIFY notifier and LocalWaker"),
             ("execution", "Producers, executors, completer"), ("maintenance", "Leader election and maintenance services"),
             ("unique", "Unique jobs"), ("periodic", "Periodic and cron jobs"), ("admin-api", "Admin and query API"),
             ("hosting", "KopiQueue.Hosting"), ("efcore", "KopiQueue.EntityFrameworkCore"), ("observability", "Logging, metrics, tracing, health"),
             ("testing-pkg", "KopiQueue.Testing"), ("cli", "kopi CLI"), ("ci", "Build, CI and release automation"), ("tests", "Test suites and infrastructure"),
             ("docs", "Documentation"), ("perf", "Performance and benchmarks"), ("river-compat", "River schema and wire compatibility"),
             ("platforms", "Managed services, AOT, trimming"), ("security", "Security")]:
    LABELS[f"area: {a}"] = (AREA_COLOR, d)
for s, d in [("S", "≤ 1 engineer-day"), ("M", "≤ 3 engineer-days"), ("L", "≤ 5 engineer-days"), ("XL", "> 5 engineer-days")]:
    LABELS[f"size: {s}"] = ("bfdadc", d)
LABELS["post-1.0"] = ("ededed", "Backlog after 1.0 GA (P2)")


def size(ed):
    return "S" if ed <= 1 else "M" if ed <= 3 else "L" if ed <= 5 else "XL"


missing = [t["id"] for m in milestones.values() for t in m["tasks"] if t["id"] not in TITLES or t["id"] not in AREAS]
if missing:
    sys.exit(f"add TITLES/AREAS entries for plan tasks: {missing}")

# ---------------------------------------------------------------- create labels
existing_labels = {l["name"] for l in gh("label", "list", "--repo", REPO, "--limit", "200", "--json", "name")}
for name, (color, desc) in LABELS.items():
    if name not in existing_labels:
        if DRY:
            print("DRY label", name)
        else:
            gh("label", "create", name, "--repo", REPO, "--color", color, "--description", desc)
print(f"labels: {len(LABELS)} defined, {len(set(LABELS) - existing_labels)} created")

# ---------------------------------------------------------------- milestones
existing_ms = {m["title"]: m["number"] for m in gh("api", f"repos/{REPO}/milestones?state=all&per_page=100")}
ms_number = {}
for key, m in milestones.items():
    title = f"{key} {m['name']}" + (f" ({m['release'].split(' → ')[-1]})" if m["release"] else "")
    desc = (f"Weeks {m['weeks']} of the plan" + (f" · release {m['release']}" if m["release"] else "") +
            f"\n\nExit criteria: {re.sub(r'\[([^]]+)\]\([^)]+\)', r'\1', m['exit'])}\n\nPlan: {PLAN}")
    if title in existing_ms:
        ms_number[key] = existing_ms[title]
    else:
        ms_number[key] = api("POST", "milestones", {"title": title, "description": desc[:1000]})["number"]
    m["title"] = title
print("milestones:", ms_number)

# ---------------------------------------------------------------- issues
existing_issues = {i["title"]: i["number"] for i in gh("issue", "list", "--repo", REPO, "--state", "all", "--limit", "500", "--json", "title,number")}
created = []


def issue(title, body, labels, milestone=None, close_comment=None):
    if title in existing_issues:
        return existing_issues[title]
    payload = {"title": title, "body": body, "labels": labels}
    if milestone:
        payload["milestone"] = milestone
    n = api("POST", "issues", payload)["number"]
    existing_issues[title] = n
    created.append(n)
    if close_comment:
        api("POST", f"issues/{n}/comments", {"body": close_comment})
        api("PATCH", f"issues/{n}", {"state": "closed", "state_reason": "completed"})
    return n


task_issue = {}
for key, m in milestones.items():
    for t in m["tasks"]:
        typ = TYPE_OVERRIDE.get(t["id"], "task")
        text = re.sub(r"\]\((spikes/[^)]+)\)", rf"]({BLOB}/docs/\1)", t["text"])
        body = (f"**Milestone:** {key} {m['name']} · **Plan task:** {t['id']} · **Estimate:** {t['ed']:g} engineer-days"
                f" · **Requirements:** {t['req'] or '—'}\n\n### Task\n\n{text}\n\n"
                "### Definition of done ([plan §6](%s))\n\n"
                "- [ ] Code, tests and XML docs; conformance cases for any new executor operation\n"
                "- [ ] Analyzers clean, `PublicAPI.Unshipped.txt` updated, no new AOT/trim warnings\n"
                "- [ ] CHANGELOG entry; requirement IDs in the PR description\n\n"
                f"**Milestone exit criteria:** {re.sub(r'\]\(01-requirements.md\)', f']({REQS})', m['exit'])}\n\n"
                f"References: [Plan]({PLAN}) · [Design]({DESIGN}) · [Requirements]({REQS})") % PLAN
        if typ == "spike":
            body = body.replace("### Definition of done", "### Definition of done (spike: the report in `docs/spikes/` is the deliverable)", 1)
        labels = [f"type: {typ}", f"size: {size(t['ed'])}"] + [f"area: {a}" for a in AREAS[t["id"]]]
        close = None
        if key == "M0":
            close = "Done in M0 (initial commit f11aa92, CI green 2026-11-26). " + M0_DONE.get(t["id"], "")
        task_issue[t["id"]] = issue(f"[{t['id']}] {TITLES[t['id']]}", body, labels, ms_number[key], close)

# Design questions, linked to the task that decides them
design = open(f"{ROOT}/docs/02-design.md", encoding="utf-8").read()
DQ_TASK = {"DQ-1": ("M2", "2.1"), "DQ-2": ("M1", "1.6"), "DQ-3": ("M2", "2.4"), "DQ-5": ("M4", "4.3"),
           "DQ-6": ("M5", "5.9"), "DQ-7": ("M5", "5.9"), "DQ-8": ("M4", "4.1")}
DQ_TITLE = {"DQ-1": "Allow ValueTask for hot workers?", "DQ-2": "Accept NpgsqlBatch for pipelined inserts?",
            "DQ-3": "Unknown job kinds: discard or leave for other deployments?", "DQ-5": "Cron parser: own or Cronos?",
            "DQ-6": "SQL Server batch parameters: OPENJSON or TVPs?", "DQ-7": "SQL Server state column: varchar(16) or tinyint?",
            "DQ-8": "Go-compatible JSON encoding for unique keys?"}
for dq, (mkey, tid) in DQ_TASK.items():
    text = re.search(rf"^\| {dq} \| (.+) \|$", design, flags=re.M).group(1)
    body = (f"{text}\n\nDecide in #{task_issue[tid]} (plan task {tid}). Record the decision in the design doc "
            f"([DQ table]({DESIGN})), then close this issue.")
    labels = ["type: design-question"] + [f"area: {a}" for a in AREAS[tid]]
    issue(f"[{dq}] {DQ_TITLE[dq]}", body, labels, ms_number[mkey])

# One-time NuGet setup and per-milestone release checklists
issue("NuGet publishing: one-time setup",
      "Before the first release (plan [§6.1](%s)):\n\n"
      "- [ ] Create the nuget.org account or organization that will own the `KopiQueue*` packages (enable 2FA)\n"
      "- [ ] nuget.org → profile → *Trusted Publishing* → add policy: owner `sujanto-gaws`, repository `KopiQueue`, workflow `publish.yml`, environment `nuget`\n"
      "- [ ] GitHub → Settings → Environments → create `nuget` (optional: required reviewers)\n"
      "- [ ] GitHub → Settings → Variables → repository variable `NUGET_USER` = nuget.org profile name (not email)\n"
      "- [ ] Fallback only if the first push fails: API key (push only, glob `KopiQueue*`) as environment secret `NUGET_API_KEY`\n"
      "- [ ] After the first public release: apply for the `KopiQueue.*` prefix reservation (account@nuget.org), OQ-2\n" % PLAN,
      ["type: release", "area: ci"], ms_number["M1"])
for key, m in milestones.items():
    if not m["release"]:
        continue
    for ver in [v.strip() for v in m["release"].split("→")]:
        tag = "v" + (ver if re.search(r"\.\d+$", ver) or "-" not in ver else ver + ".1")
        body = (f"Release `{tag}` at the end of {key} {m['name']}.\n\n"
                f"- [ ] Milestone exit criteria met: {m['exit']}\n"
                "- [ ] All milestone issues closed or moved\n"
                "- [ ] `main` is green; CHANGELOG updated" + ("; `PublicAPI.Unshipped.txt` moved to `Shipped`" if "-" not in tag else "") + "\n"
                f"- [ ] `git tag {tag} && git push origin {tag}`; approve the `publish` run if the environment requires it\n"
                "- [ ] Packages visible on nuget.org; GitHub release created\n\n"
                f"Process: plan [§6.1]({PLAN}).")
        issue(f"Release {tag}", body, ["type: release"], ms_number[key])

# Post-1.0 backlog
for title, text in [
    ("SQL Server Service Broker notifier", "Push notifications on SQL Server instead of poll-only (FR-DB-07, OQ-8). Decide from user demand and latency data."),
    ("Native json column type on SQL Server 2025 / Azure SQL", "Use the native `json` type behind a feature check (OQ-7, ADR-013)."),
    ("SQLite driver for local development and tests", "OQ-4."),
    ("Dashboard: River UI validation or own Blazor UI", "OQ-5. SQL Server users need our own UI either way."),
    ("P2 requirements backlog", "FR-QUE-05, FR-HST-05, FR-DEF-07, FR-EXE-10, FR-MNT-06, FR-TST-04. See [Requirements](%s)." % REQS),
    ("Workflows, sequences and concurrency limits", "NG3; reconsider after adoption."),
]:
    issue(title, text + f"\n\nFrom the plan's post-1.0 backlog ([plan §4]({PLAN})).", ["post-1.0", "enhancement"])

# Close M0
if not DRY:
    gh("api", "-X", "PATCH", f"repos/{REPO}/milestones/{ms_number['M0']}", "--input", "-", body=json.dumps({"state": "closed"}))
print(f"issues created this run: {len(created)}; total tracked: {len(existing_issues)}")
