"""Checks for the Group 3 scorer that do not read the price store.

Run: PYTHONHASHSEED=0 python3 -m src.test_lever_search_score
"""
from __future__ import annotations

from src.lever_search_score import compound, fee_15, luck_p, student_t_sf, walk_recipe


class _Store:
    def __init__(self, bars: dict) -> None:
        self.bars = bars

    def session_open(self, ticker: str, session: str) -> float:
        return float(self.bars[(ticker, session)]["open"])

    def session_close(self, ticker: str, session: str) -> float:
        return float(self.bars[(ticker, session)]["close"])


def test_student_t_tails() -> None:
    assert abs(student_t_sf(0.0, 10) - 0.5) < 1e-9
    # One-sided 5% of a near-normal t.
    assert abs(student_t_sf(1.64485362695, 100000) - 0.05) < 1e-4
    assert luck_p([0.01, -0.01]) == 1.0
    assert luck_p([0.2]) == 1.0
    assert compound([0.1, -0.1]) == -0.010000000000000009 or abs(compound([0.1, -0.1]) + 0.01) < 1e-12


def test_hold_one_sells_next_open_and_reprices_15bp() -> None:
    bars = {
        ("AAA", "2026-08-07"): {"open": 10.0, "close": 11.0},
        ("AAA", "2026-08-10"): {"open": 12.0, "close": 12.0},
    }
    row = {
        "ticker": "AAA",
        "sources": ["probable"],
        "src_rank": 0,
        "boxes": {"vol": "missing", "news": "missing", "ab": "missing"},
        "blue": False,
        "alarm": False,
        "zero_red": False,
        "cond_good": 0,
        "cond_bad": 0,
        "last_green": False,
        "last_red": False,
        "ohlc_ret_5": 1.0,
        "ohlc_rvol": 1.0,
        "ohlc_hot_score": 1.0,
        "ohlc_break_10": False,
        "candle_score": 0.0,
        "candle_capture": False,
    }
    recipe = {
        "name": "probable_h1",
        "universe": "probable",
        "hold": 1,
        "side": "long",
        "top_n": 8,
        "require": {},
        "forbid": {},
        "rank": None,
        "exit_when": {},
        "trades_at_open": True,
    }
    fees = {
        "commission_per_share": 0.0049,
        "commission_min_per_order": 0.99,
        "commission_max_pct_of_amount": 0.005,
        "platform_per_share": 0.005,
        "platform_min_per_order": 1.00,
        "platform_max_pct_of_amount": 0.005,
        "settlement_per_share": 0.003,
        "regulatory_pct_of_amount_sell_only": 0.000008,
        "regulatory_min_per_order": 0.01,
        "taf_per_share_sell_only": 0.000166,
        "taf_min_per_order": 0.01,
        "taf_max_per_order": 8.30,
    }
    book = walk_recipe(
        recipe,
        ["2026-08-07", "2026-08-10"],
        {"2026-08-07": [row], "2026-08-10": [row]},
        _Store(bars),
        {"2026-08-07": True, "2026-08-10": True},
        fees,
    )
    first, second = book["days"]
    assert [fill["side"] for fill in first["fills"]] == ["BUY"]
    # The lot sells at the next open. The name still matches, so it is bought again.
    assert [fill["side"] for fill in second["fills"]] == ["SELL", "BUY"]
    assert first["fills"][0]["price"] == 10.0
    assert second["fills"][0]["price"] == 12.0
    assert second["fills"][1]["price"] == 12.0
    assert first["ret_flat_15bp"] != first["ret_futubull"]
    shares = first["fills"][0]["shares"]
    assert fee_15(shares, 10.0) == round(shares * 10.0 * 0.000075, 4)
    assert book["n_under_3"] == 0


def test_keep_held_does_not_renew_a_selected_name() -> None:
    """Live path: still selected and already held means no sell, no buy, no fee.

    The default fill stays the sell-then-rebuy renewal. A name that leaves
    the list still sells through lot_should_sell. Hard-red is not a sell.
    """
    bars = {
        ("AAA", "2026-08-07"): {"open": 10.0, "close": 11.0},
        ("AAA", "2026-08-10"): {"open": 12.0, "close": 12.0},
        ("BBB", "2026-08-10"): {"open": 8.0, "close": 8.0},
    }
    row = {
        "ticker": "AAA",
        "sources": ["probable"],
        "src_rank": 0,
        "boxes": {"vol": "missing", "news": "missing", "ab": "missing"},
        "blue": False,
        "alarm": False,
        "zero_red": False,
        "cond_good": 0,
        "cond_bad": 0,
        "last_green": False,
        "last_red": False,
        "ohlc_ret_5": 1.0,
        "ohlc_rvol": 1.0,
        "ohlc_hot_score": 1.0,
        "ohlc_break_10": False,
        "candle_score": 0.0,
        "candle_capture": False,
    }
    other = dict(row, ticker="BBB", src_rank=1)
    recipe = {
        "name": "probable_h1",
        "universe": "probable",
        "hold": 1,
        "side": "long",
        "top_n": 8,
        "require": {},
        "forbid": {},
        "rank": None,
        "exit_when": {},
        "sell": "list",
        "trades_at_open": True,
    }
    fees = {
        "commission_per_share": 0.0049,
        "commission_min_per_order": 0.99,
        "commission_max_pct_of_amount": 0.005,
        "platform_per_share": 0.005,
        "platform_min_per_order": 1.00,
        "platform_max_pct_of_amount": 0.005,
        "settlement_per_share": 0.003,
        "regulatory_pct_of_amount_sell_only": 0.000008,
        "regulatory_min_per_order": 0.01,
        "taf_per_share_sell_only": 0.000166,
        "taf_min_per_order": 0.01,
        "taf_max_per_order": 8.30,
    }
    sessions = ["2026-08-07", "2026-08-10"]
    rows = {"2026-08-07": [row], "2026-08-10": [row]}
    ok = {"2026-08-07": True, "2026-08-10": True}
    store = _Store(bars)
    kept = walk_recipe(recipe, sessions, rows, store, ok, fees, fill="keep_held")
    renewed = walk_recipe(recipe, sessions, rows, store, ok, fees)
    assert [fill["side"] for fill in kept["days"][0]["fills"]] == ["BUY"]
    assert kept["days"][1]["fills"] == []
    assert kept["days"][1]["n_fills"] == 0
    assert [fill["side"] for fill in renewed["days"][1]["fills"]] == ["SELL", "BUY"]
    assert kept["days"][1]["ret_futubull"] != renewed["days"][1]["ret_futubull"]
    dropped_rows = {"2026-08-07": [row], "2026-08-10": [other]}
    dropped = walk_recipe(
        recipe, sessions, dropped_rows, store, ok, fees, fill="keep_held",
    )
    assert [fill["side"] for fill in dropped["days"][1]["fills"]] == ["SELL", "BUY"]
    assert dropped["days"][1]["fills"][0]["ticker"] == "AAA"
    assert dropped["days"][1]["fills"][1]["ticker"] == "BBB"


def test_sit_out_buys_nothing() -> None:
    bars = {("AAA", "2026-08-07"): {"open": 2.0, "close": 2.5}}
    row = {
        "ticker": "AAA", "sources": ["stock_book"], "src_rank": 0,
        "boxes": {}, "blue": False, "alarm": False, "zero_red": False,
        "cond_good": 0, "cond_bad": 0, "last_green": False, "last_red": False,
        "ohlc_ret_5": 0, "ohlc_rvol": 1, "ohlc_hot_score": 0, "ohlc_break_10": False,
        "candle_score": 0, "candle_capture": False,
    }
    recipe = {
        "name": "union_h1", "universe": "union", "hold": 1, "side": "long",
        "top_n": 8, "require": {}, "forbid": {}, "rank": None, "exit_when": {},
    }
    book = walk_recipe(
        recipe, ["2026-08-07"], {"2026-08-07": [row]}, _Store(bars),
        {"2026-08-07": False}, {},
    )
    assert book["days"][0]["sat_out"] is True
    assert book["days"][0]["fills"] == []
    assert book["days"][0]["ret_futubull"] == 0.0


def main() -> None:
    tests = [
        test_student_t_tails,
        test_hold_one_sells_next_open_and_reprices_15bp,
        test_keep_held_does_not_renew_a_selected_name,
        test_sit_out_buys_nothing,
    ]
    failed = 0
    for fn in tests:
        try:
            fn()
            print(f"ok  {fn.__name__}")
        except Exception as exc:  # noqa: BLE001
            failed += 1
            print(f"FAIL {fn.__name__}: {exc}")
    if failed:
        raise SystemExit(f"{failed} test(s) failed")
    print(f"{len(tests)} tests passed")


if __name__ == "__main__":
    main()
