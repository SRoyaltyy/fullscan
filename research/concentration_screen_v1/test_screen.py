"""Drop lock and grid lock. Does not walk the tape."""
from __future__ import annotations

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from research.concentration_screen_v1.metrics import (  # noqa: E402
    drop_compound,
    is_positive,
    lists_from,
    period_stats,
    slice_book,
)
from research.concentration_screen_v1.protocol import (  # noqa: E402
    LUCK_N,
    NAMED,
    N_ROWS,
    assert_grid,
)
from research.factor_mine_recipe_search_v4.metrics import _drop_compound, compound  # noqa: E402

HERE = Path(__file__).resolve().parent


def _continuation() -> dict:
    """P2 already has $20,000. BEST added cash. CAPITAL is $10,000."""
    start = 20_000.0
    levels = [21_000.0, 22_000.0]
    sessions = ["2026-09-14", "2026-09-15"]
    pnl = [
        {"BEST": 3_000.0, "OTHER": -2_000.0},
        {"BEST": 500.0, "OTHER": 500.0},
    ]
    daily = []
    prev = start
    for session, level, bucket in zip(sessions, levels, pnl):
        daily.append({
            "bought": ["BEST"],
            "equity": level,
            "ret": level / prev - 1.0,
            "ret_15": level / prev - 1.0,
            "session": session,
            "sold": [],
        })
        prev = level
    return {
        "closed": [
            {"entry": sessions[0], "exit": sessions[1], "pnl": 1.0, "ticker": "BEST", "win": True},
        ],
        "daily": daily,
        "first": {"BEST": sessions[0], "OTHER": sessions[0]},
        "pnl_by_day": dict(zip(sessions, pnl)),
        "start_equity": start,
    }


def _capital_start_drop(book: dict, ticker: str) -> float:
    prev = 10_000.0
    kept = 10_000.0
    rets = []
    for day in book["daily"]:
        change = float(day["equity"]) - prev
        cut = float((book["pnl_by_day"].get(day["session"]) or {}).get(ticker) or 0.0)
        nxt = kept + (change - cut)
        rets.append((nxt / kept - 1.0) if kept else 0.0)
        prev = float(day["equity"])
        kept = nxt
    return compound(rets)


def test_positive_drop_cannot_raise_the_return() -> None:
    book = _continuation()
    contributed = sum(bucket["BEST"] for bucket in book["pnl_by_day"].values())
    whole = compound([day["ret"] for day in book["daily"]])
    dropped = drop_compound(book, ["BEST"])
    if contributed <= 0:
        raise SystemExit("fixture contribution")
    if abs(dropped - _drop_compound(book, "BEST")) > 1e-12:
        raise SystemExit("single drop left the fixed function")
    if not dropped < whole:
        raise SystemExit(f"drop raised the book {dropped} vs {whole}")
    expected = (22_000.0 - contributed) / 20_000.0 - 1.0
    if abs(dropped - expected) > 1e-12:
        raise SystemExit(f"drop {dropped} != prior-equity {expected}")
    if not _capital_start_drop(book, "BEST") > whole:
        raise SystemExit("fixture no longer shows the $10,000 overstatement")
    stat = period_stats(book)
    if stat["best"] != "BEST" or not stat["ex1"] < stat["ret"]:
        raise SystemExit("period stats raised the book")
    if abs(stat["start_equity"] - 20_000.0) > 1e-9:
        raise SystemExit("period did not start from prior equity")


def test_p2_slice_starts_at_the_p1_close() -> None:
    book = {
        "closed": [],
        "daily": [
            {"bought": [], "equity": 15_000.0, "ret": 0.5, "ret_15": 0.5, "session": "2026-09-11", "sold": []},
            {"bought": [], "equity": 16_000.0, "ret": 16_000.0 / 15_000.0 - 1.0, "ret_15": 0.0, "session": "2026-09-14", "sold": []},
        ],
        "first": {},
        "pnl_by_day": {
            "2026-09-11": {"BEST": 5_000.0},
            "2026-09-14": {"BEST": 1_000.0},
        },
        "start_equity": 10_000.0,
    }
    p2 = slice_book(book, ["2026-09-14"], 15_000.0)
    if abs(p2["start_equity"] - 15_000.0) > 1e-12:
        raise SystemExit("slice ignored the P1 close")
    dropped = drop_compound(p2, ["BEST"])
    whole = compound([day["ret"] for day in p2["daily"]])
    if not dropped < whole:
        raise SystemExit("P2 drop raised a positive contribution")
    if abs(dropped - ((16_000.0 - 1_000.0) / 15_000.0 - 1.0)) > 1e-12:
        raise SystemExit(f"P2 drop {dropped}")


def test_grid_and_results() -> None:
    assert_grid()
    if LUCK_N != 21796 or N_ROWS != 260:
        raise SystemExit("luck")
    payload = json.loads((HERE / "RESULTS.json").read_text(encoding="utf-8"))
    rows = payload["rows"]
    if len(rows) != N_ROWS or payload["luck_n"] != LUCK_N:
        raise SystemExit("results grid")
    grouped = lists_from(rows)
    if [row["id"] for row in grouped["A"]] != payload["lists"]["A"]:
        raise SystemExit("list A")
    if [row["id"] for row in grouped["B"]] != payload["lists"]["B"]:
        raise SystemExit("list B")
    for row in grouped["A"]:
        if not is_positive(row["p1"]["ex1"]) or not is_positive(row["p2"]["ex1"]):
            raise SystemExit(f"A not positive {row['id']}")
    for row in grouped["B"]:
        if not is_positive(row["p1"]["ex3"]) or not is_positive(row["p2"]["ex3"]):
            raise SystemExit(f"B not positive {row['id']}")
    named = f"g3:{NAMED}"
    if not any(row["id"] == named for row in rows):
        raise SystemExit("named missing from results")
    text = (HERE / "REPORT.md").read_text(encoding="utf-8")
    if "does not pick" not in text or "going forward" not in text:
        raise SystemExit("report wording")
    if str(LUCK_N) not in text or named not in text:
        raise SystemExit("report luck or named recipe")
    if (HERE / "FREEZE.json").exists():
        raise SystemExit("freeze")


def main() -> None:
    test_positive_drop_cannot_raise_the_return()
    test_p2_slice_starts_at_the_p1_close()
    test_grid_and_results()
    print("concentration_screen_v1 ok")


if __name__ == "__main__":
    main()
