#!/usr/bin/env python3
"""PANEL_ROW_PREOPEN for sessions 2026-09-09 through 2026-09-25.

The pre-open tree is the latest first-parent commit on origin/main that is
the head of a GitHub Actions run whose run_started_at is strictly before
09:30 America/New_York (13:30 UTC). That is the clock in Excel PR #353.
Committer time is not used.

A morning is PANEL_ROW_PREOPEN=yes only when that tree's
data/factor_mine/panel.json already contains a row dated that morning.
The per-day candidate file the sequential walk reads is
data/factor_mine/snapshots/{date}.json. A job-log push time is recorded
when a harvested producer log pushed the commit. A Pages checkout of an
already-pushed head is an Actions run_started_at, not a push range.
"""
from __future__ import annotations

import csv
import json
import subprocess
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path("/workspace")
CACHE = Path("/tmp/fm_audit_336")
MAIN = "origin/main"
OUT_CSV = ROOT / "research/audit/PANEL_ROW_PREOPEN.csv"
OUT_JSON = ROOT / "research/audit/panel_row_preopen.json"

# Excel PR #353 pins these three mornings. The script checks them.
EXCEL_PIN = {
    "2026-09-09": {
        "commit": "c1167245934f9925941ce2a731efed6fc3c58901",
        "server_time": "2026-09-09T13:29:07Z",
        "run_id": "34357360638",
        "latest_session": "2026-09-08",
        "blob": "d0e427d434d6d30da22ea6bf31133ed4648fb277",
    },
    "2026-09-10": {
        "commit": "a714ba9c06220f0a68f54c5fffd9f046e72f8276",
        "server_time": "2026-09-10T13:11:49Z",
        "run_id": "34481132123",
        "latest_session": "2026-09-09",
        "blob": "620cbd24964693505e96b90f2a28ee2b63386718",
    },
    "2026-09-11": {
        "commit": "ae97009391d281cf563278ee8681e557e4761e2a",
        "server_time": "2026-09-11T11:49:49Z",
        "run_id": "34595911068",
        "latest_session": "2026-09-09",
        "blob": "620cbd24964693505e96b90f2a28ee2b63386718",
    },
}

SESSIONS = (
    "2026-08-13", "2026-08-14", "2026-08-17", "2026-08-18", "2026-08-19",
    "2026-08-20", "2026-08-21", "2026-08-24", "2026-08-25", "2026-08-26",
    "2026-08-27", "2026-08-28", "2026-08-31", "2026-09-01", "2026-09-02",
    "2026-09-03", "2026-09-04", "2026-09-08", "2026-09-09", "2026-09-10",
    "2026-09-11", "2026-09-14", "2026-09-15", "2026-09-16", "2026-09-17",
    "2026-09-18", "2026-09-21", "2026-09-22", "2026-09-23", "2026-09-24",
    "2026-09-25",
)
FOCUS = tuple(day for day in SESSIONS if day >= "2026-09-09")

COLUMNS = [
    "date", "cutoff_utc", "preopen_commit", "preopen_server_time",
    "preopen_run_id", "preopen_event", "panel_blob", "panel_in_tree",
    "latest_session", "n_panel_rows", "rows_that_morning",
    "joblog_preopen_commit", "joblog_preopen_server_time",
    "joblog_rows_that_morning", "joblog_latest_session",
    "PANEL_ROW_PREOPEN", "snapshot_in_preopen", "snapshot_rows",
    "lineup_in_preopen",     "first_panel_commit", "first_panel_rows", "first_panel_server_time",
    "first_panel_server_kind", "first_panel_run_id",
    "first_snapshot_commit", "first_snapshot_server_time",
    "first_snapshot_server_kind", "candidate_list_after_open",
    "excel_pin_match",
]


def git_text(args: list[str]) -> str:
    proc = subprocess.run(
        ["git", *args], cwd=ROOT, capture_output=True, text=True, check=False,
    )
    if proc.returncode != 0:
        raise RuntimeError(proc.stderr[-400:] or f"git {' '.join(args)} failed")
    return proc.stdout


def parse_ts(text: str) -> datetime | None:
    raw = (text or "").strip()
    if not raw:
        return None
    if raw.endswith("Z"):
        raw = raw[:-1] + "+00:00"
    if "." in raw:
        head, tail = raw.split(".", 1)
        frac = tail
        tz = ""
        for i, ch in enumerate(tail):
            if ch in "+-":
                frac = tail[:i]
                tz = tail[i:]
                break
        raw = head + "." + frac[:6] + tz
    try:
        when = datetime.fromisoformat(raw)
    except ValueError:
        return None
    if when.tzinfo is None:
        when = when.replace(tzinfo=timezone.utc)
    return when.astimezone(timezone.utc)


def cutoff_for(day: str) -> datetime:
    """09:30 ET. August and September 2026 are EDT, so 13:30 UTC, strict."""
    return datetime.fromisoformat(day + "T13:30:00+00:00")


def load_actions() -> dict[str, list[dict]]:
    found: dict[str, list[dict]] = {}
    with (CACHE / "actions.tsv").open(encoding="utf-8") as fh:
        for line in fh:
            parts = line.rstrip("\n").split("\t")
            if len(parts) < 6:
                continue
            sha, event, started, _created, run_id, url = parts[:6]
            when = parse_ts(started)
            if not sha or when is None:
                continue
            found.setdefault(sha, []).append({
                "event": event,
                "started": when,
                "run_id": run_id,
                "url": url,
            })
    return found


def load_proofs() -> dict[str, dict]:
    """Earliest job-log push time per commit. Importing the CSV builder."""
    import sys
    sys.path.insert(0, str(ROOT / "research/audit"))
    import build_fullscan_file_proof as proof

    parents = proof.load_parents()
    by7, by8 = proof.load_indexes(parents)
    proofs, _stats = proof.load_proofs(parents, by7, by8)
    return proofs


def first_parent() -> list[str]:
    text = git_text(["rev-list", "--first-parent", MAIN])
    shas = [line.strip() for line in text.splitlines() if line.strip()]
    shas.reverse()
    return shas


def panel_changes(order: dict[str, int]) -> list[tuple[int, str, str]]:
    """Oldest-first (index, commit, blob) for panel.json on first-parent main."""
    text = git_text([
        "log", "--first-parent", "--reverse", "--raw", "--no-abbrev",
        "--pretty=format:C %H", MAIN, "--", "data/factor_mine/panel.json",
    ])
    out = []
    sha = ""
    for line in text.splitlines():
        if line.startswith("C "):
            sha = line.split()[1]
            continue
        if not line.startswith(":") or "\t" not in line:
            continue
        meta, path = line.split("\t", 1)
        if path != "data/factor_mine/panel.json":
            continue
        parts = meta.split()
        blob = parts[3] if len(parts) > 3 else ""
        if blob and set(blob) <= set("0"):
            blob = ""
        if sha not in order:
            continue
        out.append((order[sha], sha, blob))
    return out


def blob_json(cache: dict[str, dict | None], blob: str) -> dict | None:
    if not blob:
        return None
    if blob in cache:
        return cache[blob]
    raw = subprocess.run(
        ["git", "cat-file", "blob", blob],
        cwd=ROOT, capture_output=True, check=False,
    )
    if raw.returncode != 0:
        cache[blob] = None
        return None
    doc = json.loads(raw.stdout)
    cache[blob] = doc
    return doc


def panel_stats(doc: dict | None) -> dict:
    if not doc:
        return {"latest": "", "n_rows": 0, "by_date": {}}
    by_date: dict[str, int] = {}
    for row in doc.get("rows") or []:
        day = str(row.get("date") or "")[:10]
        if day:
            by_date[day] = by_date.get(day, 0) + 1
    sessions = [str(item)[:10] for item in (doc.get("session_dates") or []) if item]
    latest = max(sessions) if sessions else (max(by_date) if by_date else "")
    return {
        "latest": latest,
        "n_rows": len(doc.get("rows") or []),
        "by_date": by_date,
    }


def blob_at(changes: list[tuple[int, str, str]], index: int) -> tuple[str, str]:
    """Return (commit, blob) of the panel.json visible at this first-parent index."""
    chosen = ("", "")
    for idx, sha, blob in changes:
        if idx <= index:
            chosen = (sha, blob)
        else:
            break
    return chosen


def path_added(path: str) -> str:
    text = git_text([
        "log", "--first-parent", "--diff-filter=A", "--format=%H",
        MAIN, "--", path,
    ])
    lines = [line.strip() for line in text.splitlines() if line.strip()]
    return lines[-1] if lines else ""


def path_exists(commit: str, path: str) -> bool:
    if not commit:
        return False
    proc = subprocess.run(
        ["git", "cat-file", "-e", f"{commit}:{path}"],
        cwd=ROOT, capture_output=True, check=False,
    )
    return proc.returncode == 0


def snapshot_rows(commit: str, path: str) -> int:
    if not path_exists(commit, path):
        return 0
    raw = subprocess.run(
        ["git", "cat-file", "blob", f"{commit}:{path}"],
        cwd=ROOT, capture_output=True, check=False,
    )
    if raw.returncode != 0 or not raw.stdout:
        return 0
    doc = json.loads(raw.stdout)
    return sum(
        1 for row in (doc.get("rows") or [])
        if str(row.get("date") or "")[:10] == path.rsplit("/", 1)[-1][:10]
    )


def earliest_observation(
    order: dict[str, int],
    fp: list[str],
    actions: dict[str, list[dict]],
    proofs: dict[str, dict],
    start_index: int,
    contains,
) -> dict:
    """Earliest server time of a first-parent tree for which contains(index) is true.

    Job-log time wins when that commit was pushed by a harvested run.
    Otherwise the earliest Actions run_started_at on such a commit.
    """
    best_job = None
    best_run = None
    for index in range(start_index, len(fp)):
        if not contains(index):
            continue
        sha = fp[index]
        proof = proofs.get(sha)
        if proof is not None:
            when = proof["time"]
            rec = {
                "commit": sha,
                "time": when,
                "kind": "job_log",
                "run_id": proof.get("run_id") or "",
            }
            if best_job is None or when < best_job["time"]:
                best_job = rec
        for run in actions.get(sha) or []:
            rec = {
                "commit": sha,
                "time": run["started"],
                "kind": "actions_run_started_at",
                "run_id": run["run_id"],
            }
            if best_run is None or run["started"] < best_run["time"]:
                best_run = rec
    chosen = best_job or best_run
    if best_job and best_run and best_run["time"] < best_job["time"]:
        chosen = best_run
    if chosen is None:
        return {
            "commit": fp[start_index] if start_index < len(fp) else "",
            "time": "",
            "kind": "no_server_time",
            "run_id": "",
        }
    return {
        "commit": chosen["commit"],
        "time": chosen["time"].strftime("%Y-%m-%dT%H:%M:%SZ"),
        "kind": chosen["kind"],
        "run_id": chosen["run_id"],
    }


def preopen_commit(fp: list[str], actions: dict[str, list[dict]], cutoff: datetime):
    """Latest main commit that is a run head with run_started_at strictly before cutoff."""
    chosen = None
    chosen_run = None
    for sha in reversed(fp):
        runs = [
            run for run in actions.get(sha) or []
            if run["started"] < cutoff
        ]
        if not runs:
            continue
        runs.sort(key=lambda run: (run["started"], run["run_id"]))
        chosen = sha
        chosen_run = runs[-1]
        break
    return chosen, chosen_run


def joblog_preopen(fp: list[str], proofs: dict[str, dict], cutoff: datetime):
    """Latest main commit whose harvested push-log time is at or before cutoff."""
    chosen = None
    chosen_proof = None
    for sha in reversed(fp):
        proof = proofs.get(sha)
        if proof is None or proof["time"] > cutoff:
            continue
        chosen = sha
        chosen_proof = proof
        break
    return chosen, chosen_proof


def seen_before(stamp: str, kind: str, cutoff: datetime) -> bool:
    when = parse_ts(stamp)
    if when is None:
        return False
    if kind == "job_log":
        return when <= cutoff
    return when < cutoff


def main() -> None:
    print("[panel-row] loading actions and job logs", flush=True)
    actions = load_actions()
    proofs = load_proofs()
    fp = first_parent()
    order = {sha: i for i, sha in enumerate(fp)}
    changes = panel_changes(order)
    print(f"[panel-row] first-parent {len(fp)} panel commits {len(changes)}", flush=True)
    docs: dict[str, dict | None] = {}
    stats_by_blob = {}
    for _idx, _sha, blob in changes:
        if blob and blob not in stats_by_blob:
            stats_by_blob[blob] = panel_stats(blob_json(docs, blob))

    # First index whose panel contains each morning.
    first_row_index: dict[str, int] = {}
    for idx, _sha, blob in changes:
        stats = stats_by_blob.get(blob) or panel_stats(None)
        for day, count in stats["by_date"].items():
            if count and day not in first_row_index:
                first_row_index[day] = idx

    def contains_day(day: str):
        start = first_row_index.get(day)
        def check(index: int) -> bool:
            if start is None or index < start:
                return False
            _commit, blob = blob_at(changes, index)
            stats = stats_by_blob.get(blob) or panel_stats(None)
            return stats["by_date"].get(day, 0) > 0
        return check

    snap_added: dict[str, str] = {}
    for day in FOCUS:
        snap_added[day] = path_added(f"data/factor_mine/snapshots/{day}.json")

    rows_out = []
    for day in SESSIONS:
        cutoff = cutoff_for(day)
        sha, run = preopen_commit(fp, actions, cutoff)
        index = order.get(sha, -1) if sha else -1
        panel_commit, blob = blob_at(changes, index) if sha else ("", "")
        stats = stats_by_blob.get(blob) or panel_stats(None)
        morning = stats["by_date"].get(day, 0)
        job_sha, job_proof = joblog_preopen(fp, proofs, cutoff)
        job_index = order.get(job_sha, -1) if job_sha else -1
        _job_panel_commit, job_blob = blob_at(changes, job_index) if job_sha else ("", "")
        job_stats = stats_by_blob.get(job_blob) or panel_stats(None)
        job_morning = job_stats["by_date"].get(day, 0)
        snap_path = f"data/factor_mine/snapshots/{day}.json"
        lineup_path = f"data/factor_mine/lineups/{day}.json"
        snap_in = path_exists(sha, snap_path) if sha else False
        job_snap_in = path_exists(job_sha, snap_path) if job_sha else False
        lineup_in = path_exists(sha, lineup_path) if sha else False
        snap_n = snapshot_rows(sha, snap_path) if snap_in else 0
        if day in first_row_index:
            first_panel = earliest_observation(
                order, fp, actions, proofs, first_row_index[day], contains_day(day),
            )
            _intro_commit, intro_blob = blob_at(changes, first_row_index[day])
            intro_stats = stats_by_blob.get(intro_blob) or panel_stats(None)
            first_rows = intro_stats["by_date"].get(day, 0)
        else:
            first_panel = {
                "commit": "", "time": "", "kind": "no_panel_row", "run_id": "",
            }
            first_rows = 0
        snap_commit = snap_added.get(day, "")
        if snap_commit and snap_commit in order:
            def contains_snap(index: int, start=order[snap_commit]) -> bool:
                return index >= start
            first_snap = earliest_observation(
                order, fp, actions, proofs, order[snap_commit], contains_snap,
            )
        elif day >= "2026-09-09":
            first_snap = {
                "commit": snap_commit, "time": "", "kind": "no_server_time", "run_id": "",
            }
        else:
            first_snap = {
                "commit": "", "time": "", "kind": "no_snapshot_file", "run_id": "",
            }
        panel_before = morning > 0 or job_morning > 0 or seen_before(
            first_panel["time"], first_panel["kind"], cutoff,
        )
        snap_before = snap_in or job_snap_in or seen_before(
            first_snap["time"], first_snap["kind"], cutoff,
        )
        # Late when no server-timed copy of this morning's candidate list
        # exists at or before the open.
        after_open = "no" if (panel_before or snap_before) else "yes"
        pin = EXCEL_PIN.get(day)
        pin_match = ""
        if pin:
            pin_match = "yes" if (
                sha == pin["commit"]
                and run
                and run["started"].strftime("%Y-%m-%dT%H:%M:%SZ") == pin["server_time"]
                and stats["latest"] == pin["latest_session"]
                and blob == pin["blob"]
                and morning == 0
            ) else "no"
        rows_out.append({
            "date": day,
            "cutoff_utc": cutoff.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "preopen_commit": sha or "",
            "preopen_server_time": (
                run["started"].strftime("%Y-%m-%dT%H:%M:%SZ") if run else ""
            ),
            "preopen_run_id": run["run_id"] if run else "",
            "preopen_event": run["event"] if run else "",
            "panel_blob": blob,
            "panel_in_tree": "yes" if blob else "no",
            "latest_session": stats["latest"],
            "n_panel_rows": stats["n_rows"],
            "rows_that_morning": morning,
            "joblog_preopen_commit": job_sha or "",
            "joblog_preopen_server_time": (
                job_proof["time"].strftime("%Y-%m-%dT%H:%M:%SZ") if job_proof else ""
            ),
            "joblog_rows_that_morning": job_morning,
            "joblog_latest_session": job_stats["latest"],
            "PANEL_ROW_PREOPEN": "yes" if (morning or job_morning) else "no",
            "snapshot_in_preopen": "yes" if (snap_in or job_snap_in) else "no",
            "snapshot_rows": snap_n if snap_in else 0,
            "lineup_in_preopen": "yes" if lineup_in else "no",
            "first_panel_commit": first_panel["commit"],
            "first_panel_rows": first_rows,
            "first_panel_server_time": first_panel["time"],
            "first_panel_server_kind": first_panel["kind"],
            "first_panel_run_id": first_panel["run_id"],
            "first_snapshot_commit": first_snap["commit"],
            "first_snapshot_server_time": first_snap["time"],
            "first_snapshot_server_kind": first_snap["kind"],
            "candidate_list_after_open": after_open,
            "excel_pin_match": pin_match,
        })
        if day in FOCUS:
            print(
                f"[panel-row] {day} commit={(sha or '')[:12]} "
                f"server={rows_out[-1]['preopen_server_time']} "
                f"latest={stats['latest']} rows={morning} "
                f"snap={rows_out[-1]['snapshot_in_preopen']} "
                f"first_panel={first_panel['time']} {first_panel['kind']} "
                f"pin={pin_match}",
                flush=True,
            )

    OUT_CSV.parent.mkdir(parents=True, exist_ok=True)
    with OUT_CSV.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=COLUMNS)
        writer.writeheader()
        writer.writerows(rows_out)
    OUT_JSON.write_text(json.dumps(rows_out, indent=2) + "\n", encoding="utf-8")
    bad = [row["date"] for row in rows_out if row["excel_pin_match"] == "no"]
    if bad:
        raise SystemExit(f"Excel pin mismatch: {bad}")
    focus = [row for row in rows_out if row["date"] in FOCUS]
    yes = [row["date"] for row in focus if row["PANEL_ROW_PREOPEN"] == "yes"]
    print(f"[panel-row] PANEL_ROW_PREOPEN=yes {yes or 'none'} of {len(focus)}", flush=True)
    print(f"[panel-row] wrote {OUT_CSV}", flush=True)


if __name__ == "__main__":
    main()
