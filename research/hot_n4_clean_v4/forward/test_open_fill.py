"""Mocked 2026-09-29 plan, 09:35 open-fill, and 21:30 fill for both recipes.

Proves a missing plan appends nothing, a missing open appends nothing, a
second run does not repeat a fill, and the post-close run does not append
a buy or sell the open-fill already wrote. The append-only check passes on
both sealed logs and fails if a seeded line is altered.
"""
from __future__ import annotations

import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from research.hot_n4_clean_v4.forward.append_check import _prefix, check_against  # noqa: E402
from research.hot_n4_clean_v4.forward.book import BOOKS, H1, HOLDUP, reset_book, use_book  # noqa: E402
from research.hot_n4_clean_v4.forward.ledger import append_records, load, log_path  # noqa: E402
from research.hot_n4_clean_v4.forward.openfill import (  # noqa: E402
    decide_fill,
    decide_open_fill,
    filled_symbols,
)
from research.hot_n4_clean_v4.forward.planfill import book_state, build_plan, fill_book  # noqa: E402
from research.hot_n4_clean_v4.run_study import load_fees, nyse_sessions  # noqa: E402

SESSION = "2026-09-29"
PRIOR = "2026-09-25"


def _index() -> dict[str, int]:
    calendar = nyse_sessions("2026-01-01", "2026-12-31")
    return {day: i for i, day in enumerate(calendar)}


def _session(recipe: str) -> dict:
    return {
        "buys": [],
        "cash_primary": 8000.0,
        "date": PRIOR,
        "equity_primary": 8100.0,
        "excluded_unexplained_legs": [],
        "holdings": [{
            "cost_primary": 100.0,
            "entry_date": PRIOR,
            "entry_px": 10.0,
            "last_px": 10.0,
            "min_hold": 1,
            "peak_px": 10.0,
            "shares": 10,
            "ticker": "BBB",
        }],
        "holdup_on": False,
        "kind": "session",
        "morning_s": None,
        "recipe": recipe,
        "sells": [],
        "unfilled": [],
    }


def _blob(op: float, close: float, include_today: bool) -> dict:
    dates = [PRIOR]
    opens = [10.0]
    closes = [10.0]
    if include_today:
        dates.append(SESSION)
        opens.append(op)
        closes.append(close)
    return {"adjusted": True, "close": closes, "date": dates, "open": opens}


def _bars(include_today: bool) -> dict:
    return {
        "feat": {},
        "stored": {
            "AAA": _blob(11.0, 13.0, include_today),
            "BBB": _blob(12.0, 12.5, include_today),
            "IWM": _blob(10.0, 10.2, include_today),
        },
    }


def _plan_body(recipe: str, state: dict, index: dict[str, int]) -> dict:
    payload = {
        "bar_cutoff": PRIOR,
        "candidates": [{
            "fv_avg_volume": 1_000_000.0,
            "fv_price": 20.0,
            "ohlc_hot_score": 5.0,
            "sources": ["yday_gainer"],
            "ticker": "AAA",
        }],
        "excluded_unexplained_legs": [],
        "morning_s": 1.0,
        "session": SESSION,
    }
    plan = build_plan(payload, state, index)
    if plan["recipe"] != recipe:
        raise SystemExit(f"plan recipe {plan['recipe']}")
    if [row["ticker"] for row in plan["picks"]] != ["AAA"]:
        raise SystemExit(f"picks {plan['picks']}")
    if [row["ticker"] for row in plan["planned_sells"]] != ["BBB"]:
        raise SystemExit(f"planned sells {plan['planned_sells']}")
    return plan


def _once(records: list[dict], folder: Path, bars: dict, fees: dict, index: dict, opens: dict) -> list[dict]:
    before = log_path(folder).read_bytes()
    bodies, why = decide_open_fill(records, SESSION, opens, bars, fees, index)
    if not bodies:
        raise SystemExit(f"open fill appended nothing ({why})")
    append_records(bodies, folder)
    again, _why = decide_open_fill(load(folder), SESSION, opens, bars, fees, index)
    if again:
        raise SystemExit("second open fill returned bodies")
    if log_path(folder).read_bytes() == before:
        raise SystemExit("open fill did not append")
    sealed = log_path(folder).read_bytes()
    if decide_open_fill(load(folder), SESSION, opens, bars, fees, index)[0]:
        raise SystemExit("third open fill returned bodies")
    if log_path(folder).read_bytes() != sealed:
        raise SystemExit("repeat open fill wrote")
    return bodies


def _symbols_unique(records: list[dict]) -> None:
    found = filled_symbols(records)
    if len(found) != len(set(found)):
        raise SystemExit(f"duplicate fill {found}")


def _run_book(book) -> None:
    token = use_book(book)
    try:
        index = _index()
        fees = load_fees()
        want_hold = 2 if book is HOLDUP else 1
        with tempfile.TemporaryDirectory() as tmp:
            folder = Path(tmp)
            append_records([_session(book.recipe)], folder)
            prior = load(folder)
            plan = _plan_body(book.recipe, book_state(prior), index)
            append_records([plan], folder)
            planned = log_path(folder).read_bytes()

            missing, why = decide_open_fill(load(folder), SESSION, {}, _bars(False), fees, index)
            if missing or why is None or "no trustworthy" not in why:
                raise SystemExit(f"missing open did not no-op ({why})")
            if log_path(folder).read_bytes() != planned:
                raise SystemExit("missing open wrote")

            bare = Path(tmp) / "bare"
            bare.mkdir()
            append_records([_session(book.recipe)], bare)
            quiet, quiet_why = decide_open_fill(
                load(bare), SESSION, {"AAA": 11.0, "BBB": 12.0, "IWM": 10.0}, _bars(False), fees, index,
            )
            if quiet or quiet_why is not None:
                raise SystemExit(f"missing plan wrote {quiet_why}")
            if len(load(bare)) != 1:
                raise SystemExit("missing plan appended")

            full_opens = {"AAA": 11.0, "BBB": 12.0, "IWM": 10.0}
            _once(load(folder), folder, _bars(False), fees, index, full_opens)
            opened = next(row for row in load(folder) if row["kind"] == "open_fill")
            if "equity_primary" in opened or opened.get("pnl_status") != "pending":
                raise SystemExit("open fill sealed a close P&L")
            aaa = next(row for row in opened["holdings"] if row["ticker"] == "AAA")
            if int(aaa["min_hold"]) != want_hold:
                raise SystemExit(f"min hold {aaa['min_hold']} {book.recipe}")
            if not any(row["ticker"] == "BBB" for row in opened["sells"]):
                raise SystemExit("BBB was not filled at the open")
            if not any(row["ticker"] == "AAA" for row in opened["buys"]):
                raise SystemExit("AAA was not filled at the open")

            close_bars = _bars(True)
            direct, _direct_closes, _state = fill_book(
                next(row for row in load(folder) if row["kind"] == "plan"),
                book_state(load(bare)),
                close_bars,
                fees,
                index,
            )
            before_mark = log_path(folder).read_bytes()
            mark_bodies, mark_why = decide_fill(load(folder), close_bars, fees, index)
            if mark_why or not mark_bodies or mark_bodies[0]["kind"] != "mark":
                raise SystemExit(f"mark {mark_why}")
            if mark_bodies[0]["added_buys"] or mark_bodies[0]["added_sells"]:
                raise SystemExit("close mark repeated an open fill")
            if float(mark_bodies[0]["equity_primary"]) != float(direct["equity_primary"]):
                raise SystemExit("mark equity is not the close mark")
            append_records(mark_bodies, folder)
            if not log_path(folder).read_bytes().startswith(before_mark):
                raise SystemExit("mark rewrote the open fill")
            rerun, _rerun_why = decide_fill(load(folder), close_bars, fees, index)
            if rerun:
                raise SystemExit("second fill returned bodies")
            if log_path(folder).read_bytes() == before_mark:
                raise SystemExit("mark did not append")
            sealed = log_path(folder).read_bytes()
            if decide_open_fill(load(folder), SESSION, full_opens, close_bars, fees, index)[0]:
                raise SystemExit("open fill after the mark returned bodies")
            if log_path(folder).read_bytes() != sealed:
                raise SystemExit("open fill after the mark wrote")
            _symbols_unique(load(folder))
            kinds = [row["kind"] for row in load(folder)]
            if kinds.count("open_fill") != 1 or kinds.count("mark") != 1 or "fill" in kinds:
                raise SystemExit(f"kinds {kinds}")
            if not any(row["kind"] == "close" and row["ticker"] == "BBB" for row in load(folder)):
                raise SystemExit("close record missing")

        with tempfile.TemporaryDirectory() as tmp:
            folder = Path(tmp)
            append_records([_session(book.recipe)], folder)
            plan = _plan_body(book.recipe, book_state(load(folder)), index)
            append_records([plan], folder)
            partial = {"BBB": 12.0, "IWM": 10.0}
            bodies, why = decide_open_fill(load(folder), SESSION, partial, _bars(False), fees, index)
            if why or not bodies:
                raise SystemExit(f"partial open {why}")
            if any(row["ticker"] == "AAA" for row in bodies[0]["buys"]):
                raise SystemExit("AAA filled without an open")
            append_records(bodies, folder)
            added, why = decide_fill(load(folder), _bars(True), fees, index)
            if why or added[0]["kind"] != "mark":
                raise SystemExit(f"partial mark {why}")
            if [row["ticker"] for row in added[0]["added_buys"]] != ["AAA"]:
                raise SystemExit(f"missing AAA was not added {added[0]['added_buys']}")
            if added[0]["added_sells"]:
                raise SystemExit("partial mark repeated BBB")
            append_records(added, folder)
            _symbols_unique(load(folder))

        with tempfile.TemporaryDirectory() as tmp:
            folder = Path(tmp)
            append_records([_session(book.recipe)], folder)
            plan = _plan_body(book.recipe, book_state(load(folder)), index)
            append_records([plan], folder)
            skipped, why = decide_open_fill(load(folder), SESSION, {}, _bars(False), fees, index)
            if skipped:
                raise SystemExit("empty open wrote a fill")
            late, why = decide_fill(load(folder), _bars(True), fees, index)
            if why or late[0]["kind"] != "fill":
                raise SystemExit(f"late fill {why} {late[:1]}")
            append_records(late, folder)
            if decide_open_fill(load(folder), SESSION, full_opens, _bars(True), fees, index)[0]:
                raise SystemExit("open fill after the 21:30 fill returned bodies")
            _symbols_unique(load(folder))
        print("open-fill", book.recipe, "ok")
    finally:
        reset_book(token)


def _tamper(path: Path) -> None:
    old = path.read_bytes()
    if not old.startswith(b"{"):
        raise SystemExit(f"{path} is not a seeded log")
    altered = bytes([old[0] ^ 1]) + old[1:]
    try:
        _prefix(old, altered, path.name)
    except SystemExit as exc:
        if "append-only" not in str(exc):
            raise
    else:
        raise SystemExit(f"altered seeded line passed {path.name}")
    try:
        _prefix(old, old.splitlines(keepends=True)[0], path.name)
    except SystemExit as exc:
        if "append-only" not in str(exc):
            raise
    else:
        raise SystemExit(f"deleted seeded lines passed {path.name}")


def _sealed() -> None:
    check_against("origin/main")
    for book in BOOKS:
        path = book.folder / book.log_name
        if not path.is_file():
            raise SystemExit(f"missing log {path}")
        _tamper(path)
    print("append-only ok on both logs")


def main() -> None:
    for book in (HOLDUP, H1):
        _run_book(book)
    _sealed()
    print("2026-09-29 open-fill ok")


if __name__ == "__main__":
    main()
