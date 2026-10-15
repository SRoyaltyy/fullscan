"""Freeze gate for the Sleeve merge live card (sleeve_merge_live.yml).

2026-10-14 (~09:20 ET) and 2026-10-15 (09:25 ET, commit 9bd8ea4c,
run 38740869857) the card rewrote ``data/sleeve_merge/today.json`` inside
the 03:00-09:40 ET input freeze. The frozen-input check then struck h1 and
Factor Mine both days. ``src/freeze_guard.py`` cannot catch it: it only
guards *dated* paths and today.json is undated.

Rule: for today's ET date, the card does not run (no checkout, no write,
no push) from 03:00 ET inclusive until 09:40 ET exclusive, whatever the
trigger (schedule, workflow_run, workflow_dispatch). A workflow_run trigger
before 03:00 is also skipped (the old "no clone before 09:00" rule for the
01:00 Stock Book). A dispatch for a past date is not gated: it writes the
dated card only through the existing past-day lock.

No override. The card's own outputs are unchanged after 09:40 ET.

CLI: ``python3 src/sleeve_card_freeze.py --event EVENT [--date D]``
prints ``skip=yes`` or ``skip=no`` (append it to $GITHUB_ENV).
"""
from __future__ import annotations

import argparse
import sys
from datetime import datetime
from zoneinfo import ZoneInfo

ET = ZoneInfo("America/New_York")
FREEZE_START = 300   # 03:00 ET inclusive
FREEZE_END = 940     # 09:40 ET exclusive


def in_freeze(now: datetime) -> bool:
    hm = now.astimezone(ET).hour * 100 + now.astimezone(ET).minute
    return FREEZE_START <= hm < FREEZE_END


def should_skip(event: str, now: datetime, run_date: str = "") -> tuple[bool, str]:
    et = now.astimezone(ET)
    today = et.strftime("%Y-%m-%d")
    date = (run_date or "").strip() or today
    hm = et.hour * 100 + et.minute
    if date != today:
        return False, f"run_date {date} is not today ({today}); not gated"
    if in_freeze(et):
        return True, (f"{et:%H:%M} ET is inside the 03:00-09:40 ET input freeze; "
                      "not writing data/sleeve_merge/today.json")
    if event == "workflow_run" and hm < FREEZE_START:
        return True, f"{et:%H:%M} ET workflow_run before 03:00 ET; wait for book"
    return False, f"{et:%H:%M} ET outside freeze"


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--event", required=True)
    ap.add_argument("--date", default="")
    ap.add_argument("--now", default="", help="ISO time (tests only)")
    a = ap.parse_args(argv)
    now = datetime.fromisoformat(a.now) if a.now else datetime.now(ET)
    skip, why = should_skip(a.event, now, a.date)
    print(f"[sleeve-card-freeze] {why}", file=sys.stderr)
    print(f"skip={'yes' if skip else 'no'}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
