"""Add-only restart of the holdup book from a named open. Wrapper, not engine.

Why: holdup's next session is ``next_session(book_dates[-1])``. The last real
book date is 2026-09-25, so prices.py keeps refreshing 2026-09-28 ("no new
session after 2026-09-25") and forward.py walks forward one ``missing`` line
per run (10-01, 10-02, 10-05 ...), always days behind the calendar, so it
never seals the live session.

``forward/holdup_restart.jsonl`` names the open the book restarts from. With
``install()``, in memory only, the session before that open counts as passed
when picking the next session, so prices and the planner target the restart
day. Sessions between the last ledger line and the restart get no ledger
line: no backfill, no missing lines, no rewrite. Holdings carry from the last
real book date. Only the holdup book (``FORWARD_BOOK`` unset/holdup) is
affected; h1 is never touched, and with no notes file every function is the
original.

Run ``holdup_restart.py <script> [args]``.
"""
from __future__ import annotations

import json
import runpy
import sys
from datetime import date, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from research.hot_n4_clean_v4.forward import ledger  # noqa: E402
from research.hot_n4_clean_v4.forward.book import HOLDUP, current_book  # noqa: E402

NOTES_NAME = "holdup_restart.jsonl"
_ORIG_BOOK_DATES = ledger.book_dates
MODULES = (
    "research.hot_n4_clean_v4.forward.ledger",
    "research.hot_n4_clean_v4.forward.forward",
    "research.hot_n4_clean_v4.forward.prices",
    "research.hot_n4_clean_v4.forward.openfill",
)


def notes_path() -> Path:
    return HOLDUP.folder / NOTES_NAME


def load_notes(path: Path | None = None) -> list[dict]:
    path = path or notes_path()
    if not path.is_file():
        return []
    rows = []
    for n, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
        if not line.strip():
            continue
        row = json.loads(line)
        if row.get("kind") != "restart" or row.get("recipe") != HOLDUP.recipe or not row.get("from"):
            raise RuntimeError(f"{NOTES_NAME} line {n} is not a valid restart note")
        rows.append(row)
    return rows


def _prev_session(day: str) -> str:
    from research.hot_n4_clean_v4.forward.forward import is_nyse_holiday

    cur = date.fromisoformat(day) - timedelta(days=1)
    for _ in range(14):
        if cur.weekday() < 5 and not is_nyse_holiday(cur):
            return cur.isoformat()
        cur -= timedelta(days=1)
    raise RuntimeError(f"no session before {day}")


def restart_floor(records: list[dict], notes: list[dict]) -> str | None:
    """Session before the latest restart open, if it is after every ledger date."""
    if not notes:
        return None
    start = max(n["from"] for n in notes)
    last = max((r["date"] for r in records), default="")
    if start <= last:
        return None  # ledger already reached the restart; notes are inert
    return _prev_session(start)


def book_dates(records: list[dict]) -> list[str]:
    dates = _ORIG_BOOK_DATES(records)
    if current_book() != HOLDUP or not dates:
        return dates
    floor = restart_floor(records, load_notes())
    if floor and floor > dates[-1]:
        return dates + [floor]
    return dates


def install() -> None:
    import importlib

    for name in MODULES:
        try:
            mod = importlib.import_module(name)
        except ImportError:
            continue
        if getattr(mod, "book_dates", None) is _ORIG_BOOK_DATES:
            mod.book_dates = book_dates


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("usage: holdup_restart.py <script.py> [args]", file=sys.stderr)
        raise SystemExit(2)
    script = Path(sys.argv[1]).resolve()
    install()
    sys.argv = [str(script)] + sys.argv[2:]
    runpy.run_path(str(script), run_name="__main__")
