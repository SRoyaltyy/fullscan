"""Drop-compound starts from equity already in the book. Does not walk a recipe."""
from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from research.factor_mine_recipe_search_v4.metrics import (  # noqa: E402
    CAPITAL,
    _drop_compound,
    compound,
    period_start,
    summarize,
)


def _continuation() -> dict:
    """A 09-14 window whose book is already at $20,000, not a fresh $10,000."""
    start = 20_000.0
    equity = [21_000.0, 22_000.0]
    sessions = ["2026-09-14", "2026-09-15"]
    pnl = [
        {"BEST": 3_000.0, "OTHER": -2_000.0},
        {"BEST": 500.0, "OTHER": 500.0},
    ]
    daily = []
    prev = start
    for session, level in zip(sessions, equity):
        daily.append({
            "bought": [],
            "equity": level,
            "ret": level / prev - 1.0,
            "ret_15": level / prev - 1.0,
            "session": session,
            "sold": [],
        })
        prev = level
    return {
        "closed": [],
        "daily": daily,
        "first": {"BEST": sessions[0], "OTHER": sessions[0]},
        "pnl_by_day": dict(zip(sessions, pnl)),
    }


def _capital_start_drop(book: dict, ticker: str) -> float:
    """The old reading: day one subtracts from $10,000 even when equity is higher."""
    prev = float(CAPITAL)
    kept = float(CAPITAL)
    rets = []
    for day in book["daily"]:
        change = float(day["equity"]) - prev
        cut = float((book["pnl_by_day"].get(day["session"]) or {}).get(ticker) or 0.0)
        nxt = kept + (change - cut)
        rets.append((nxt / kept - 1.0) if kept else 0.0)
        prev = float(day["equity"])
        kept = nxt
    return compound(rets)


def test_positive_removal_cannot_raise_the_window() -> None:
    book = _continuation()
    whole = compound([day["ret"] for day in book["daily"]])
    contributed = sum(bucket["BEST"] for bucket in book["pnl_by_day"].values())
    dropped = _drop_compound(book, "BEST")
    if contributed <= 0:
        raise SystemExit("fixture contribution")
    if not dropped < whole:
        raise SystemExit(f"drop raised the book {dropped} vs {whole}")
    end = book["daily"][-1]["equity"]
    expected = (end - contributed) / 20_000.0 - 1.0
    if abs(dropped - expected) > 1e-12:
        raise SystemExit(f"drop {dropped} != {expected}")
    buggy = _capital_start_drop(book, "BEST")
    if not buggy > whole:
        raise SystemExit("fixture no longer shows the $10,000 overstatement")
    stat = summarize(book)
    if stat["ex_best_ticker"] != "BEST" or not stat["ex_best"] < stat["compound"]:
        raise SystemExit(f"summarize {stat['ex_best']} vs {stat['compound']}")


def test_period_starts_from_prior_equity() -> None:
    book = _continuation()
    if abs(period_start(book) - 20_000.0) > 1e-9:
        raise SystemExit(f"inferred start {period_start(book)}")
    if abs(period_start(book) - float(CAPITAL)) < 1.0:
        raise SystemExit("period restarted at CAPITAL")
    book["start_equity"] = 20_000.0
    if abs(period_start(book) - 20_000.0) > 1e-12:
        raise SystemExit("explicit prior equity ignored")
    dropped = _drop_compound(book, "BEST")
    if abs(dropped - ((22_000.0 - 3_500.0) / 20_000.0 - 1.0)) > 1e-12:
        raise SystemExit(f"explicit start drop {dropped}")


def test_fresh_book_still_starts_at_capital() -> None:
    start = float(CAPITAL)
    book = {
        "closed": [],
        "daily": [
            {"equity": 11_000.0, "ret": 0.10, "ret_15": 0.10, "session": "2026-08-13"},
            {
                "equity": 12_000.0,
                "ret": 12_000.0 / 11_000.0 - 1.0,
                "ret_15": 12_000.0 / 11_000.0 - 1.0,
                "session": "2026-08-14",
            },
        ],
        "first": {"BEST": "2026-08-13"},
        "pnl_by_day": {
            "2026-08-13": {"BEST": 1_500.0},
            "2026-08-14": {"BEST": 200.0},
        },
        "start_equity": start,
    }
    dropped = _drop_compound(book, "BEST")
    expected = (12_000.0 - 1_700.0) / start - 1.0
    if abs(dropped - expected) > 1e-12:
        raise SystemExit(f"fresh drop {dropped} != {expected}")
    if not dropped < compound([day["ret"] for day in book["daily"]]):
        raise SystemExit("fresh positive removal raised the book")


def main() -> None:
    test_positive_removal_cannot_raise_the_window()
    test_period_starts_from_prior_equity()
    test_fresh_book_still_starts_at_capital()
    print("factor_mine_recipe_search_v4 metrics ok")


if __name__ == "__main__":
    main()
