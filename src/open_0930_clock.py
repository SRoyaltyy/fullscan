"""09:30 ET open window.

GitHub cron is UTC only. ``30 13 * * 1-5`` is 09:30 while EDT and
08:30 while EST; ``30 14`` is the winter pair. An 08:00–09:29 start
waits until the bell (sleeve merge ~09:25; EST 08:30 fire). Before
08:00 or after 16:00 is a skip so a 01:00 poke does not trade.

Does not change flatten_robust. Holiday / weekend = skip.
"""
from __future__ import annotations

import argparse
import time
from datetime import datetime
from zoneinfo import ZoneInfo

from src.skip_if_good import is_nyse_holiday

ET = ZoneInfo("America/New_York")
BELL_H, BELL_M = 9, 30
EARLIEST_H = 8          # EST 13:30 UTC fire is 08:30 — wait to the bell
LATE_H = 16             # after the close this is not the open pack
DEFAULT_MAX_WAIT_S = 20 * 60
EST_CATCH_WAIT_S = 70 * 60  # 08:20→09:30; workflows pass this explicitly


def now_et(when: datetime | None = None) -> datetime:
    if when is None:
        return datetime.now(ET)
    if when.tzinfo is None:
        return when.replace(tzinfo=ET)
    return when.astimezone(ET)


def is_session_day(when: datetime | None = None) -> bool:
    t = now_et(when)
    if t.weekday() >= 5:
        return False
    return not is_nyse_holiday(t.date())


def bell(when: datetime | None = None) -> datetime:
    t = now_et(when)
    return t.replace(hour=BELL_H, minute=BELL_M, second=0, microsecond=0)


def decision(when: datetime | None = None) -> str:
    """run | wait | skip."""
    t = now_et(when)
    if not is_session_day(t):
        return "skip"
    if t.hour < EARLIEST_H:
        return "skip"
    if t.hour >= LATE_H:
        return "skip"
    if t < bell(t):
        return "wait"
    return "run"


def wait_for_bell(when: datetime | None = None, *,
                  sleep: callable = time.sleep,
                  max_wait_s: int = DEFAULT_MAX_WAIT_S) -> str:
    """Block until 09:30 ET when we arrived after 08:00. Returns decision."""
    t = now_et(when)
    want = decision(t)
    if want != "wait":
        return want
    target = bell(t)
    delay = (target - t).total_seconds()
    if delay <= 0:
        return "run"
    if delay > max_wait_s:
        return "skip"
    sleep(delay + 0.05)
    return "run"


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--wait", action="store_true",
                    help="sleep until 09:30 ET when 08:00–09:29")
    ap.add_argument("--max-wait-s", type=int, default=DEFAULT_MAX_WAIT_S,
                    help="refuse a wait longer than this (default 20 min)")
    ap.add_argument("--now", default="",
                    help="ISO timestamp for tests (ET if naive)")
    args = ap.parse_args(argv)
    when = None
    if args.now:
        raw = datetime.fromisoformat(args.now)
        when = raw if raw.tzinfo else raw.replace(tzinfo=ET)
    if args.wait:
        got = wait_for_bell(when, max_wait_s=args.max_wait_s)
    else:
        got = decision(when)
    print(f"[open-0930] {got} et={now_et(when).isoformat(timespec='seconds')}")
    return 0 if got == "run" else 2


if __name__ == "__main__":
    raise SystemExit(main())
