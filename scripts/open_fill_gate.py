"""Decide whether a scheduled open-fill run should proceed. Read-only.

Prints ``run=true|false`` to ``$GITHUB_OUTPUT``. The session is today's
date in America/New_York. It proceeds only when the book log already holds
a sealed ``plan`` for that session and no ``open_fill`` for it yet, so the
backup cron is idempotent with the primary one and never targets a past day.
"""
from __future__ import annotations

import json
import os
import sys
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo


def decide(log: Path, today: str) -> tuple[bool, str]:
    if datetime.fromisoformat(today).weekday() >= 5:
        return False, f"{today} is a weekend"
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
    today = os.environ.get("GATE_TODAY") or datetime.now(ZoneInfo("America/New_York")).date().isoformat()
    ok, why = decide(log, today)
    print(f"open-fill gate: run={str(ok).lower()} ({why})")
    out = os.environ.get("GITHUB_OUTPUT")
    if out:
        with open(out, "a", encoding="utf-8") as fh:
            fh.write(f"run={str(ok).lower()}\n")


if __name__ == "__main__":
    main()
