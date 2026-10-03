"""Stage 1 — harvest Cyrus Grok automations into frozen dated files.

NOT a GitHub Action. GH Actions cannot see automation run logs.

Run this from Cursor / Grok Bot with an Automations connector:

    python -m src.grok_automation_harvest --since 2026-08-13

Writes ``data/grok_automations/{date}_{task}.json`` (createTime UTC,
conversationId, raw text/JSON). Skips 13 Questions and Webull STANDTEST.

If ``automation_get_results`` is missing, the CLI exits 2 after writing a
coverage stub so Stage 2 can still replay from repo news.
"""
from __future__ import annotations

import argparse
import importlib
import json
import os
import sys
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable
from zoneinfo import ZoneInfo

ROOT = Path(__file__).resolve().parent.parent
OUT_DIR = ROOT / "data" / "grok_automations"
SCHEMA = "grok_automation_harvest_v1"
ET = ZoneInfo("America/New_York")

# Skip: 13 Questions (climate score) and Webull STANDTEST.
TASKS: dict[str, dict[str, str]] = {
    "9b838f02-f233-495a-b609-47ff7c9a61ba": {
        "slug": "news_parsing",
        "label": "News parsing (SEC/FDA/DOJ/Fed Register/White House)",
    },
    "5b4f01c3-fe5b-463a-a270-8bc9def8e26f": {
        "slug": "google_news",
        "label": "Google News prompt",
    },
    "23a24524-4eaa-4b9e-bd39-5b30e2ec74ae": {
        "slug": "hype_factor",
        "label": "Hype factor",
    },
    "883e005e-8986-4bcc-a7ee-2c835369c94e": {
        "slug": "macro_intelligence",
        "label": "Macro Intelligence Analyst",
    },
    "c49a2ad4-007f-4ecd-a4ab-840756060710": {
        "slug": "sector_fime",
        "label": "Sector pack Financials+Industrials+Materials+Energy",
    },
    "636e4bff-3b56-481f-bea7-802de312e0f9": {
        "slug": "sector_defensive",
        "label": "Sector pack Consumer Defensive+Utilities+Healthcare+RE",
    },
    "76044fbe-9312-4c50-8918-3b1caed159d8": {
        "slug": "sector_tech",
        "label": "Sector pack Tech/Comm/Cyclical",
    },
    "4453231e-ccad-4451-ab64-898f2b082493": {
        "slug": "human_sources",
        "label": "Human Sources / Overall Market A",
    },
    "161ba94e-6599-4915-885e-f7d8181b3602": {
        "slug": "overall_market",
        "label": "Human Sources / Overall Market B",
    },
}

SKIPPED = (
    "13 Questions (climate score only)",
    "Webull STANDTEST",
)


def parse_utc(value: Any) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        dt = value
    else:
        text = str(value).strip()
        if not text:
            return None
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        try:
            dt = datetime.fromisoformat(text)
        except ValueError:
            return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def et_date(dt: datetime) -> str:
    return dt.astimezone(ET).strftime("%Y-%m-%d")


def normalize_result(task_id: str, raw: dict, *, harvested_at: str) -> dict | None:
    meta = TASKS.get(task_id) or {"slug": "unknown", "label": task_id}
    created = parse_utc(
        raw.get("createTime") or raw.get("created_at") or raw.get("createdAt")
    )
    if created is None:
        return None
    cid = (
        raw.get("conversationId")
        or raw.get("conversation_id")
        or raw.get("conversationID")
        or ""
    )
    body = raw.get("raw")
    if body is None:
        body = raw.get("result") or raw.get("text") or raw.get("output") or raw
    return {
        "schema": SCHEMA,
        "taskId": task_id,
        "task": meta["slug"],
        "label": meta["label"],
        "date": et_date(created),
        "createTime": created.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "conversationId": str(cid),
        "harvested_at": harvested_at,
        "source": "automation_get_results",
        "raw": body,
    }


def find_get_results() -> Callable | None:
    """Local Automations connector only — never a GH Action secret path."""
    for spec in (
        os.environ.get("GROK_AUTOMATION_GET_RESULTS"),
        "automation_get_results",
        "cursor_automations.get_results",
        "src.automation_get_results",
    ):
        if not spec:
            continue
        try:
            if "." in spec:
                mod_name, fn_name = spec.rsplit(".", 1)
                mod = importlib.import_module(mod_name)
                fn = getattr(mod, fn_name, None)
            else:
                fn = importlib.import_module(spec)
                if not callable(fn):
                    fn = getattr(fn, "automation_get_results", None)
            if callable(fn):
                return fn
        except Exception:
            continue
    return None


def fetch_task(fn: Callable, task_id: str, since: str) -> list[dict]:
    try:
        out = fn(task_id, since=since)
    except TypeError:
        try:
            out = fn(taskId=task_id, since=since)
        except TypeError:
            out = fn(task_id)
    if out is None:
        return []
    if isinstance(out, dict):
        out = out.get("results") or out.get("items") or [out]
    return [r for r in out if isinstance(r, dict)]


def write_frozen(rows: list[dict], dest: Path = OUT_DIR) -> dict[str, int]:
    dest.mkdir(parents=True, exist_ok=True)
    grouped: dict[tuple[str, str], list[dict]] = defaultdict(list)
    for row in rows:
        grouped[(row["date"], row["task"])].append(row)
    counts: dict[str, int] = {}
    for (date, task), items in grouped.items():
        items.sort(key=lambda r: r.get("createTime") or "")
        path = dest / f"{date}_{task}.json"
        payload = {
            "schema": SCHEMA,
            "date": date,
            "task": task,
            "n": len(items),
            "results": items,
        }
        path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
        counts[f"{date}_{task}"] = len(items)
    return counts


def ingest_dir(path: Path, harvested_at: str) -> list[dict]:
    """Normalize already-downloaded connector dumps into the frozen schema."""
    rows: list[dict] = []
    files = sorted(path.glob("*.json")) if path.is_dir() else [path]
    for fp in files:
        try:
            raw = json.loads(fp.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            continue
        blobs = raw if isinstance(raw, list) else [raw]
        if isinstance(raw, dict) and isinstance(raw.get("results"), list):
            blobs = raw["results"]
        for blob in blobs:
            if not isinstance(blob, dict):
                continue
            task_id = (
                blob.get("taskId") or blob.get("task_id") or raw.get("taskId")
                if isinstance(raw, dict) else None
            )
            if not task_id:
                continue
            row = normalize_result(str(task_id), blob, harvested_at=harvested_at)
            if row:
                row["source"] = "ingest"
                rows.append(row)
    return rows


def coverage_stub(*, since: str, reason: str, harvested_at: str) -> dict:
    return {
        "schema": SCHEMA,
        "stage": 1,
        "since": since,
        "harvested_at": harvested_at,
        "automation_get_results": False,
        "reason": reason,
        "tasks": {tid: meta["slug"] for tid, meta in TASKS.items()},
        "skipped": list(SKIPPED),
        "n_results": 0,
        "n_days": 0,
        "files": [],
        "note": (
            "Stage 1 must be run by Grok Bot / Cursor with an Automations "
            "connector. GitHub Actions cannot see automation run logs. "
            "Stage 2 reads frozen files in data/grok_automations/ plus dated "
            "repo news only."
        ),
    }


def run(since: str = "2026-08-13", ingest: Path | None = None,
        dest: Path = OUT_DIR) -> dict:
    harvested_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    rows: list[dict] = []
    source = "none"
    reason = ""
    if ingest:
        rows = ingest_dir(ingest, harvested_at)
        source = "ingest"
        if not rows:
            reason = f"ingest dir had no usable results: {ingest}"
    else:
        fn = find_get_results()
        if fn is None:
            reason = (
                "automation_get_results unavailable in this environment "
                "(cloud VM / GH Actions cannot see automation run logs)"
            )
            stub = coverage_stub(since=since, reason=reason,
                                 harvested_at=harvested_at)
            dest.mkdir(parents=True, exist_ok=True)
            (dest / "_coverage.json").write_text(
                json.dumps(stub, indent=2), encoding="utf-8")
            return stub
        source = getattr(fn, "__name__", "automation_get_results")
        for task_id in TASKS:
            for raw in fetch_task(fn, task_id, since):
                row = normalize_result(task_id, raw, harvested_at=harvested_at)
                if row and row["date"] >= since:
                    rows.append(row)
    counts = write_frozen(rows, dest=dest) if rows else {}
    days = sorted({r["date"] for r in rows})
    report = {
        "schema": SCHEMA,
        "stage": 1,
        "since": since,
        "harvested_at": harvested_at,
        "automation_get_results": source != "none",
        "source": source,
        "reason": reason,
        "tasks": {tid: meta["slug"] for tid, meta in TASKS.items()},
        "skipped": list(SKIPPED),
        "n_results": len(rows),
        "n_days": len(days),
        "days": days,
        "files": counts,
        "note": (
            "Frozen dumps only. Stage 2 replay must not call live automations."
        ),
    }
    dest.mkdir(parents=True, exist_ok=True)
    (dest / "_coverage.json").write_text(
        json.dumps(report, indent=2), encoding="utf-8")
    return report


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--since", default="2026-08-13")
    ap.add_argument("--ingest", type=Path, default=None,
                    help="Normalize already-downloaded dumps (no live API)")
    ap.add_argument("--dest", type=Path, default=OUT_DIR)
    args = ap.parse_args(argv)
    report = run(since=args.since, ingest=args.ingest, dest=args.dest)
    print(json.dumps(report, indent=2))
    if not report.get("n_results"):
        print(
            "[harvest] no automation days dumped — Stage 2 will use dated "
            "repo news only. Re-run this CLI from Cursor with Automations.",
            file=sys.stderr,
        )
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
