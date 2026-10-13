"""Add-only notes that an h1 session's fill is missing. Wrapper, not engine.

``forward_h1/fill_missing_notes.jsonl`` lists sessions that are closed as
missing without any ledger line: a sealed plan whose open fill never
happened (``fill_missing``, which must name the plan's seq and sha256), or a
session that was never sealed (``session_missing``). Nothing in
LEDGER.jsonl or h1_log.jsonl is written or rewritten for them. No backfill,
no Webull.

``install()`` changes how the h1 book reads its own records, in memory only:
a noted plan no longer counts as the open plan, and noted sessions count as
passed when picking the next session. Holdings carry from the last real
book date. Holdup (``FORWARD_BOOK`` unset) is never affected, and with no
notes file every function is the original.

Run ``h1_fill_missing.py <script> [args]`` to run seal gate, forward,
prices, or h1_closes with the notes applied.
"""
from __future__ import annotations

import json
import runpy
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from research.hot_n4_clean_v4.forward import ledger  # noqa: E402
from research.hot_n4_clean_v4.forward.book import H1, current_book  # noqa: E402

NOTES_NAME = "fill_missing_notes.jsonl"
KINDS = ("fill_missing", "session_missing")
_ORIG_OPEN_PLAN = ledger.open_plan
_ORIG_BOOK_DATES = ledger.book_dates


def notes_path() -> Path:
    return H1.folder / NOTES_NAME


def load_notes(path: Path | None = None) -> list[dict]:
    path = path or notes_path()
    if not path.is_file():
        return []
    rows = []
    for n, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
        row = json.loads(line)
        if row.get("kind") not in KINDS or row.get("recipe") != H1.recipe or not row.get("date"):
            raise RuntimeError(f"{NOTES_NAME} line {n} is not a valid fill-missing note")
        rows.append(row)
    return rows


def _active() -> bool:
    return current_book() == H1


def check_notes(records: list[dict], notes: list[dict]) -> None:
    """A fill_missing note must match its sealed plan; neither may cover a filled day."""
    for note in notes:
        day = note["date"]
        if any(r["date"] == day and r["kind"] in ("open_fill", "fill", "mark", "session") for r in records):
            raise RuntimeError(f"fill-missing note for {day} but the ledger already resolves {day}")
        plan = ledger.plan_on(records, day)
        if note["kind"] == "fill_missing":
            if plan is None or plan.get("sha256") != note.get("plan_sha256"):
                raise RuntimeError(f"fill-missing note for {day} does not match the sealed plan")
        elif plan is not None:
            raise RuntimeError(f"session_missing note for {day} but a plan is sealed")


def noted_dates(records: list[dict]) -> set[str]:
    if not _active():
        return set()
    notes = load_notes()
    check_notes(records, notes)
    return {n["date"] for n in notes}


def open_plan(records: list[dict]) -> dict | None:
    noted = noted_dates(records)
    return _ORIG_OPEN_PLAN([r for r in records if not (r["kind"] == "plan" and r["date"] in noted)])


def book_dates(records: list[dict]) -> list[str]:
    dates = _ORIG_BOOK_DATES(records)
    noted = noted_dates(records)
    if not noted or not dates:
        return dates
    last = dates[-1]
    return dates + sorted(d for d in noted if d > last)


MODULES = (
    "research.hot_n4_clean_v4.forward.ledger",
    "research.hot_n4_clean_v4.forward.forward",
    "research.hot_n4_clean_v4.forward.prices",
    "research.hot_n4_clean_v4.forward.openfill",
    "research.hot_n4_clean_v4.forward.h1_seal_gate",
)


def install() -> None:
    import importlib

    for name in MODULES:
        mod = importlib.import_module(name)
        if getattr(mod, "open_plan", None) is _ORIG_OPEN_PLAN:
            mod.open_plan = open_plan
        if getattr(mod, "book_dates", None) is _ORIG_BOOK_DATES:
            mod.book_dates = book_dates


def uninstall() -> None:
    import importlib

    for name in MODULES:
        mod = importlib.import_module(name)
        if getattr(mod, "open_plan", None) is open_plan:
            mod.open_plan = _ORIG_OPEN_PLAN
        if getattr(mod, "book_dates", None) is book_dates:
            mod.book_dates = _ORIG_BOOK_DATES


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("usage: h1_fill_missing.py <script.py> [args]", file=sys.stderr)
        raise SystemExit(2)
    script = Path(sys.argv[1]).resolve()
    install()
    sys.argv = [str(script)] + sys.argv[2:]
    runpy.run_path(str(script), run_name="__main__")
