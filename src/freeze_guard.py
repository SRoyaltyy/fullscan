"""Refuse to re-land a file that a decision freeze manifest already froze.

2026-10-12: ``01_daily/weather/2026-10-12_weather.json`` was frozen at
09:20 ET by ``research/input_freeze/2026-10-12/manifest.json`` and then
re-landed 15 times by Pre-Open heals, the leftover sweep and Stock Book
ALL. Every land also re-dispatched the ticket publish.

Rule: once ``research/input_freeze/<D>/manifest.json`` with status
``frozen`` is on main, any fullscan file listed in that manifest's
``files`` (``repo == "fullscan"``) whose path carries the date ``<D>`` is
read-only for the shared land helper (``scripts/safe_git_push.sh``). The
writer skips it, logs a ``::warning::``, appends a line to
``$GITHUB_STEP_SUMMARY`` and adds a row to the add-only
``frozen_write_skips`` list on ``data/day_board/<D>.json``. The run does
not fail. Undated rolling files (MANIFEST.json, suggestions.csv, ...) are
never blocked: they are rewritten every day by design.

The guard is fail-open: if it cannot read a manifest it prints a warning
and lets the land continue, so it can never sink a packet.

CLI (used by safe_git_push.sh):
  python3 src/freeze_guard.py filter --ref <sha> --msg "<commit msg>" PATH...
prints one blocked path per line on stdout; everything else on stderr.
"""
from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import sys
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

ROOT = Path(__file__).resolve().parent.parent
ET = ZoneInfo("America/New_York")
BOARD_DIR = ROOT / "data" / "day_board"
FIELD = "frozen_write_skips"
MAX_SKIPS = 200
_DATE = re.compile(r"(20\d\d-\d\d-\d\d)")


def manifest_rel(date: str) -> str:
    return f"research/input_freeze/{date}/manifest.json"


def _git_show(ref: str, rel: str, cwd: Path) -> bytes | None:
    r = subprocess.run(["git", "show", f"{ref}:{rel}"], cwd=str(cwd),
                       capture_output=True, timeout=60)
    return r.stdout if r.returncode == 0 else None


def frozen_set(manifest: dict, date: str) -> set[str]:
    """Fullscan paths dated ``date`` that this frozen manifest lists."""
    if not isinstance(manifest, dict) or manifest.get("status") != "frozen":
        return set()
    out: set[str] = set()
    for row in manifest.get("files") or []:
        if not isinstance(row, dict) or row.get("repo") != "fullscan":
            continue
        rel = str(row.get("source_path") or "").strip()
        if rel and date in rel:
            out.add(rel)
    return out


def blocked_paths(paths: list[str], ref: str, cwd: Path = ROOT,
                  loader=None) -> dict[str, str]:
    """``{path: date}`` for every path frozen by its date's manifest on ref."""
    load = loader or (lambda d: _git_show(ref, manifest_rel(d), cwd))
    cache: dict[str, set[str]] = {}
    out: dict[str, str] = {}
    for p in paths:
        for date in sorted(set(_DATE.findall(p))):
            if date not in cache:
                raw = load(date)
                if raw is None:
                    cache[date] = set()
                else:
                    try:
                        cache[date] = frozen_set(json.loads(raw), date)
                    except (ValueError, UnicodeError):
                        print(f"::warning title=freeze guard::{manifest_rel(date)} "
                              "unreadable — not guarding that date", file=sys.stderr)
                        cache[date] = set()
            if p in cache[date]:
                out[p] = date
                break
    return out


def record_skips(date: str, rows: list[dict], board_dir: Path = BOARD_DIR) -> Path | None:
    """Append rows to the add-only ``frozen_write_skips`` field. Never raises."""
    path = board_dir / f"{date}.json"
    try:
        board = json.loads(path.read_text(encoding="utf-8")) if path.is_file() else {}
        if not isinstance(board, dict):
            return None
        have = list(board.get(FIELD) or [])
        seen = {(r.get("path"), r.get("at")) for r in have if isinstance(r, dict)}
        for r in rows:
            if (r.get("path"), r.get("at")) not in seen:
                have.append(r)
        board[FIELD] = have[-MAX_SKIPS:]
        board.setdefault("date", date)
        board_dir.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(board, indent=2), encoding="utf-8")
        return path
    except (OSError, ValueError) as e:
        print(f"::warning title=freeze guard::could not note skip on {path}: {e}",
              file=sys.stderr)
        return None


def merge_skips(a: dict, b: dict) -> list[dict]:
    """Union of both boards' skip rows, oldest first (day_board merge)."""
    seen: set[tuple] = set()
    out: list[dict] = []
    for r in list(a.get(FIELD) or []) + list(b.get(FIELD) or []):
        if not isinstance(r, dict):
            continue
        k = (r.get("path"), r.get("at"))
        if k in seen:
            continue
        seen.add(k)
        out.append(r)
    out.sort(key=lambda r: str(r.get("at") or ""))
    return out[-MAX_SKIPS:]


def _summary(lines: list[str]) -> None:
    target = os.environ.get("GITHUB_STEP_SUMMARY") or ""
    if not target:
        return
    try:
        with open(target, "a", encoding="utf-8") as fh:
            fh.write("\n".join(lines) + "\n")
    except OSError:
        pass


def run_filter(paths: list[str], ref: str, msg: str, cwd: Path = ROOT,
               board_dir: Path | None = None, loader=None) -> list[str]:
    """Return blocked paths; log, summarise and note them on the day board."""
    blocked = blocked_paths(paths, ref, cwd, loader=loader)
    if not blocked:
        return []
    at = datetime.now(ET).isoformat(timespec="seconds")
    run = os.environ.get("GITHUB_RUN_ID") or ""
    wf = os.environ.get("GITHUB_WORKFLOW") or ""
    by_date: dict[str, list[dict]] = {}
    lines = ["### Frozen input write skipped (decision freeze manifest)", ""]
    for p, d in sorted(blocked.items()):
        print(f"::warning title=FROZEN WRITE SKIPPED::{p} is in {manifest_rel(d)}; "
              f"not re-landing it ({msg})", file=sys.stderr)
        print(f"[freeze-guard] SKIP frozen {p} (manifest {d}) — file on main unchanged",
              file=sys.stderr)
        lines.append(f"- `{p}` frozen by `{manifest_rel(d)}` — skipped ({msg})")
        by_date.setdefault(d, []).append({
            "path": p, "at": at, "commit_msg": msg, "workflow": wf,
            "run_id": run, "manifest": manifest_rel(d),
        })
    _summary(lines)
    for d, rows in by_date.items():
        record_skips(d, rows, board_dir or (cwd / "data" / "day_board"))
    return sorted(blocked)


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser()
    sub = ap.add_subparsers(dest="cmd", required=True)
    f = sub.add_parser("filter")
    f.add_argument("--ref", required=True)
    f.add_argument("--msg", default="")
    f.add_argument("paths", nargs="*")
    args = ap.parse_args(argv)
    try:
        for p in run_filter(args.paths, args.ref, args.msg, cwd=Path.cwd()):
            print(p)
    except Exception as e:  # noqa: BLE001 — fail open, never sink a land
        print(f"::warning title=freeze guard::guard error, not guarding: {e}",
              file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
