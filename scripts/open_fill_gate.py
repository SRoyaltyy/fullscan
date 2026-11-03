"""Decide whether a scheduled open-fill run should proceed. Read-only.

Prints ``run=true|false`` to ``$GITHUB_OUTPUT``. The session is today's
date in America/New_York. It proceeds only on a weekday at or after\n09:35 ET (so the EDT and EST crons are both safe), and only when the book log already holds
a sealed ``plan`` for that session and no ``open_fill`` for it yet, so the
backup cron is idempotent with the primary one and never targets a past day.

Optional no-send inputs (h1 only), all read-only: ``--skips``,
``--notes`` and ``--paper-status-dir``. When the session already has a
"sent share lock missing" skip, a ``fill_missing`` (no-send) note, or a
paper_open status of ``missed_deadline`` / ``submit: false``, the gate
exits early with run=false, so late backup crons stop re-running and stop
holding the h1-forward queue. Nothing is written except $GITHUB_OUTPUT.
"""
from __future__ import annotations

import json
import os
import sys
from datetime import datetime, time
from pathlib import Path
from zoneinfo import ZoneInfo


OPEN_FILL_AT = time(9, 35)


def _rows(path: Path | None):
    if path is None or not path.is_file():
        return
    for line in path.read_text(encoding="utf-8").splitlines():
        if line.strip():
            try:
                yield json.loads(line)
            except ValueError:
                continue


def no_send_reason(
    today: str,
    skips: Path | None = None,
    notes: Path | None = None,
    status_dir: Path | None = None,
) -> str | None:
    for row in _rows(skips):
        if str(row.get("date", ""))[:10] == today and "sent share lock missing" in str(row.get("reason", "")):
            return f"{today} already skipped: sent share lock missing"
    for row in _rows(notes):
        if str(row.get("date", ""))[:10] == today and row.get("kind") == "fill_missing":
            return f"{today} already has a no-send fill_missing note"
    if status_dir is not None:
        st = status_dir / f"{today}_status.json"
        if st.is_file():
            try:
                data = json.loads(st.read_text(encoding="utf-8"))
            except ValueError:
                data = {}
            if data.get("status") == "missed_deadline":
                return f"{today} paper_open status missed_deadline (no send)"
            if data.get("submit") is False:
                return f"{today} paper_open submit false (no send)"
    return None


def decide(
    log: Path,
    now: datetime,
    skips: Path | None = None,
    notes: Path | None = None,
    status_dir: Path | None = None,
) -> tuple[bool, str]:
    today = now.date().isoformat()
    if now.weekday() >= 5:
        return False, f"{today} is a weekend"
    if now.time() < OPEN_FILL_AT:
        return False, f"{now:%H:%M} ET is before 09:35 ET"
    if not log.is_file():
        return False, f"{log} missing"
    why = no_send_reason(today, skips, notes, status_dir)
    if why:
        return False, why
    has_plan = has_fill = False
    for line in log.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        row = json.loads(line)
        if str(row.get("date", ""))[:10] != today:
            continue
        kind = row.get("kind")
        has_plan |= kind == "plan"
        has_fill |= kind == "open_fill"
    if has_fill:
        return False, f"open_fill for {today} already sealed"
    if not has_plan:
        return False, f"no sealed plan for {today}"
    return True, f"plan for {today} sealed, open_fill pending"


def main() -> None:
    import argparse

    ap = argparse.ArgumentParser()
    ap.add_argument("log")
    ap.add_argument("--skips")
    ap.add_argument("--notes")
    ap.add_argument("--paper-status-dir")
    a = ap.parse_args()
    log = Path(a.log)
    opt = lambda v: Path(v) if v else None
    ny = ZoneInfo("America/New_York")
    # GATE_NOW (ISO, New York wall time) is for local tests only.
    raw = os.environ.get("GATE_NOW")
    now = datetime.fromisoformat(raw).replace(tzinfo=ny) if raw else datetime.now(ny)
    ok, why = decide(log, now, opt(a.skips), opt(a.notes), opt(a.paper_status_dir))
    print(f"open-fill gate: run={str(ok).lower()} ({why})")
    out = os.environ.get("GITHUB_OUTPUT")
    if out:
        with open(out, "a", encoding="utf-8") as fh:
            fh.write(f"run={str(ok).lower()}\n")


if __name__ == "__main__":
    main()
