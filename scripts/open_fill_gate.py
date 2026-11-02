"""Decide whether a scheduled open-fill run should proceed. Read-only.

Prints ``run=true|false`` to ``$GITHUB_OUTPUT``. The session is today's
date in America/New_York. It proceeds only on a weekday at or after\n09:35 ET (so the EDT and EST crons are both safe), and only when the book log already holds
a sealed ``plan`` for that session and no ``open_fill`` for it yet, so the
backup cron is idempotent with the primary one and never targets a past day.
"""
from __future__ import annotations

import json
import os
import sys
from datetime import datetime, time
from pathlib import Path
from zoneinfo import ZoneInfo


OPEN_FILL_AT = time(9, 35)


def decide(log: Path, now: datetime) -> tuple[bool, str]:
    today = now.date().isoformat()
    if now.weekday() >= 5:
        return False, f"{today} is a weekend"
    if now.time() < OPEN_FILL_AT:
        return False, f"{now:%H:%M} ET is before 09:35 ET"
    if not log.is_file():
        return False, f"{log} missing"
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
    log = Path(sys.argv[1])
    ny = ZoneInfo("America/New_York")
    # GATE_NOW (ISO, New York wall time) is for local tests only.
    raw = os.environ.get("GATE_NOW")
    now = datetime.fromisoformat(raw).replace(tzinfo=ny) if raw else datetime.now(ny)
    ok, why = decide(log, now)
    print(f"open-fill gate: run={str(ok).lower()} ({why})")
    out = os.environ.get("GITHUB_OUTPUT")
    if out:
        with open(out, "a", encoding="utf-8") as fh:
            fh.write(f"run={str(ok).lower()}\n")


if __name__ == "__main__":
    main()
