"""Create or update the KopiQueue GitHub Project from the repository's issues.

Usage (from the repo root, with `gh` authenticated with the `project` scope):
    python -I -X utf8 tools/sync-project.py                 # create the project if needed, then sync
    python -I -X utf8 tools/sync-project.py --dry-run       # show what would change

What it does (idempotent):
- creates the public project "KopiQueue" owned by the repository owner and links it to the repository;
- adds a number field "Estimate (ed)";
- adds every issue (open and closed) to the project;
- sets "Estimate (ed)" from the "**Estimate:** N engineer-days" line in the issue body, when empty;
- sets Status to "Done" for closed issues and "Todo" for open ones, only when Status is empty, so manual moves are kept.

Views (board, table, roadmap) and the built-in workflows cannot be created through the API: set them up in the web UI.
"""
import json
import re
import subprocess
import sys
import time

REPO = sys.argv[sys.argv.index("--repo") + 1] if "--repo" in sys.argv else "sujanto-gaws/KopiQueue"
OWNER = REPO.split("/")[0]
TITLE = "KopiQueue"
ESTIMATE = "Estimate (ed)"
DRY = "--dry-run" in sys.argv


def gh(*args, parse=True):
    p = subprocess.run(["gh", *args], capture_output=True, text=True, encoding="utf-8")
    if p.returncode != 0:
        sys.exit(f"gh {' '.join(args[:4])} failed: {p.stderr.strip()}")
    return json.loads(p.stdout) if parse and p.stdout.strip() else p.stdout


def mutate(*args):
    if DRY:
        print("DRY", " ".join(a for a in args if not a.startswith("PVT")))
        return {}
    out = gh(*args, "--format", "json") if args[1] in ("create", "item-add", "field-create") else gh(*args, parse=False)
    time.sleep(0.3)
    return out


# Project
projects = gh("project", "list", "--owner", OWNER, "--format", "json", "--limit", "100")["projects"]
project = next((p for p in projects if p["title"] == TITLE), None)
if project is None:
    if DRY:
        sys.exit("DRY: project does not exist yet; it would be created. Run without --dry-run.")
    project = mutate("project", "create", "--owner", OWNER, "--title", TITLE)
    gh("project", "edit", str(project["number"]), "--owner", OWNER, "--visibility", "PUBLIC",
       "--description", "Roadmap to KopiQueue 1.0: a background job queue for .NET on PostgreSQL and SQL Server", parse=False)
    gh("project", "link", str(project["number"]), "--owner", OWNER, "--repo", REPO, parse=False)
    print("created project", project["url"])
number, project_id = str(project["number"]), project["id"]

# Fields
fields = gh("project", "field-list", number, "--owner", OWNER, "--format", "json", "--limit", "100")["fields"]
if not any(f["name"] == ESTIMATE for f in fields):
    mutate("project", "field-create", number, "--owner", OWNER, "--name", ESTIMATE, "--data-type", "NUMBER")
    fields = gh("project", "field-list", number, "--owner", OWNER, "--format", "json", "--limit", "100")["fields"]
by_name = {f["name"]: f for f in fields}
status = by_name["Status"]
status_option = {o["name"]: o["id"] for o in status.get("options", [])}
estimate_id = by_name.get(ESTIMATE, {}).get("id")

# Items already in the project
items = gh("project", "item-list", number, "--owner", OWNER, "--format", "json", "--limit", "1000")["items"]
item_by_issue = {i["content"]["number"]: i for i in items if i.get("content", {}).get("type") == "Issue"}

issues = gh("issue", "list", "--repo", REPO, "--state", "all", "--limit", "1000", "--json", "number,url,state,body")
added = est_set = status_set = 0
for issue in sorted(issues, key=lambda i: i["number"]):
    item = item_by_issue.get(issue["number"])
    if item is None:
        item = mutate("project", "item-add", number, "--owner", OWNER, "--url", issue["url"])
        added += 1
    item_id = item.get("id")
    m = re.search(r"\*\*Estimate:\*\* ([\d.]+) engineer-days", issue["body"] or "")
    if m and estimate_id and item.get(ESTIMATE.lower()) is None:
        mutate("project", "item-edit", "--id", item_id or "PVT_new", "--project-id", project_id,
               "--field-id", estimate_id, "--number", m.group(1))
        est_set += 1
    if not item.get("status"):
        want = "Done" if issue["state"] == "CLOSED" else "Todo"
        mutate("project", "item-edit", "--id", item_id or "PVT_new", "--project-id", project_id,
               "--field-id", status["id"], "--single-select-option-id", status_option[want])
        status_set += 1

print(f"project {project.get('url', number)}: {len(issues)} issues; added {added}, estimates set {est_set}, statuses set {status_set}")
