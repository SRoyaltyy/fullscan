"""Hold-lock and HARD_RED rules. No parquet required."""
from __future__ import annotations

from src.boring_winners_lab import Spec, extras_for, filter_color, step_hold, window_perf, ORIG_WINDOW


def test_hold_1_replaces_daily():
    held = {"AAA": {"age": 0, "src": "book"}}
    rec = step_hold(
        held,
        [{"ticker": "BBB", "source": "book", "stack": "blue"}],
        hold=1, seats=1, hard_red="none", market_hard=False,
        rets={"AAA": 1.0, "BBB": 2.0},
    )
    assert rec["sold"] == ["AAA"]
    assert rec["bought"] == ["BBB"]
    assert rec["held"] == ["BBB"]
    assert rec["day_pnl"] == 2.0


def test_hold_3_locks_seat_skips_today_buy():
    held = {"AAA": {"age": 0, "src": "book"}}
    rec = step_hold(
        held,
        [{"ticker": "BBB", "source": "extra", "stack": "blue"}],
        hold=3, seats=1, hard_red="none", market_hard=False,
        rets={"AAA": -1.0, "BBB": 4.0},
    )
    assert rec["sold"] == []
    assert rec["bought"] == []
    assert rec["n_skip"] == 1
    assert rec["skipped"][0]["why"] == "no_seat"
    assert rec["held"] == ["AAA"]
    assert rec["day_pnl"] == -1.0
    assert held["AAA"]["age"] == 1


def test_hold_3_sells_after_age():
    held = {"AAA": {"age": 2, "src": "book"}}  # becomes 3 this morning
    rec = step_hold(
        held,
        [{"ticker": "BBB", "source": "book", "stack": "blue"}],
        hold=3, seats=1, hard_red="none", market_hard=False,
        rets={"BBB": 1.5},
    )
    assert rec["sold"] == ["AAA"]
    assert rec["bought"] == ["BBB"]
    assert rec["held"] == ["BBB"]


def test_stand_down_blocks_new_buys():
    held = {"AAA": {"age": 0, "src": "book"}}
    rec = step_hold(
        held,
        [{"ticker": "AAA", "source": "book", "stack": "blue"},
         {"ticker": "BBB", "source": "extra", "stack": "blue"}],
        hold=1, seats=2, hard_red="stand_down", market_hard=True,
        rets={"AAA": 0.5, "BBB": 1.0},
    )
    assert rec["bought"] == []
    assert any(s["why"] == "stand_down" for s in rec["skipped"])
    assert "AAA" in rec["held"]
    assert "BBB" not in rec["held"]


def test_haircut_only_on_new_buy():
    held = {"OLD": {"age": 0, "src": "book", "entry_px": 100.0}}
    rec = step_hold(
        held,
        [{"ticker": "OLD", "source": "book", "stack": "blue"},
         {"ticker": "NEW", "source": "extra", "stack": "blue"}],
        hold=1, seats=2, hard_red="haircut_5", market_hard=True,
        rets={"OLD": 1.0, "NEW": -2.0},
        bars={
            "OLD": {"open": 100.0, "high": 102.0, "low": 99.0, "close": 101.0},
            "NEW": {"open": 100.0, "high": 101.0, "low": 94.0, "close": 98.0},
        },
        prev_closes={"OLD": 100.0},
    )
    by = {n["ticker"]: n["ret_1d"] for n in rec["names"]}
    assert rec["bought"] == ["NEW"]
    assert rec["buys"][0]["buy_kind"] == "limit_5"
    assert rec["buys"][0]["buy_px"] == 95.0
    assert by["OLD"] == 1.0  # 101/100 - 1
    assert abs(by["NEW"] - (98 / 95 - 1) * 100) < 1e-6


def test_limit_5_skips_unless_dipped():
    rec = step_hold(
        {},
        [{"ticker": "DIP", "source": "book", "stack": "blue"},
         {"ticker": "UP", "source": "book", "stack": "blue"},
         {"ticker": "FAKE", "source": "book", "stack": "blue"}],
        hold=1, seats=3, hard_red="limit_5", market_hard=True,
        rets={"DIP": -6.0, "UP": 1.0, "FAKE": -8.0},
        bars={
            "DIP": {"open": 100.0, "high": 100.0, "low": 93.0, "close": 96.0},
            "UP": {"open": 50.0, "high": 52.0, "low": 49.5, "close": 51.0},
            # close-to-close is ugly but the low never printed through open×0.95
            "FAKE": {"open": 20.0, "high": 20.2, "low": 19.2, "close": 19.1},
        },
    )
    assert rec["bought"] == ["DIP"]
    assert rec["buys"][0]["buy_px"] == 95.0
    assert rec["buys"][0]["buy_when"].startswith("intraday")
    assert any(s["ticker"] == "UP" and s["why"] == "limit_miss" for s in rec["skipped"])
    assert any(s["ticker"] == "FAKE" and s["why"] == "limit_miss" for s in rec["skipped"])


def test_haircut_falls_back_to_close_without_print():
    rec = step_hold(
        {},
        [{"ticker": "NODIP", "source": "book", "stack": "blue"}],
        hold=1, seats=1, hard_red="haircut_5", market_hard=True,
        rets={"NODIP": -1.0},
        bars={"NODIP": {"open": 10.0, "high": 10.2, "low": 9.7, "close": 9.9}},
    )
    assert rec["bought"] == ["NODIP"]
    assert rec["buys"][0]["buy_kind"] == "close"
    assert rec["buys"][0]["buy_px"] == 9.9
    assert rec["buys"][0]["buy_when"] == "16:00 ET close"


def test_color_filter():
    packs = [
        {"ticker": "B", "row": {"blue": True, "cond": "bad"}},
        {"ticker": "G", "row": {"blue": False, "cond": "good"}},
        {"ticker": "X", "row": {"blue": False, "cond": "bad"}},
    ]
    assert [p["ticker"] for p in filter_color(packs, "blue")] == ["B"]
    assert [p["ticker"] for p in filter_color(packs, "green")] == ["G"]
    assert len(filter_color(packs, "all")) == 3


def test_extras_scale_with_book():
    assert extras_for(10) == 5
    assert extras_for(25) == 5
    assert extras_for(50) == 10


def test_spec_defaults_are_live_book():
    s = Spec("overlay_25_h1", "live")
    assert s.source == "overlay" and s.seats == 25 and s.hold == 1


def test_orig_window_uses_prior_close():
    assert ORIG_WINDOW == ("2026-08-13", "2026-08-21")
    daily = [
        {"date": "2026-08-12", "day_pnl": 1.0, "equity": 10100.0, "fees_cum": 8.0},
        {"date": "2026-08-13", "day_pnl": 2.0, "equity": 10302.0, "fees_cum": 12.0},
        {"date": "2026-08-14", "day_pnl": -1.0, "equity": 10198.98, "fees_cum": 14.0},
        {"date": "2026-08-24", "day_pnl": -5.0, "equity": 9689.0, "fees_cum": 20.0},
    ]
    w = window_perf(daily, *ORIG_WINDOW)
    assert w["n_days"] == 2
    assert w["start_eq"] == 10100.0
    assert w["end_eq"] == 10198.98
    assert abs(w["fees"] - 6.0) < 1e-9
    assert w["dates"] == ["2026-08-13", "2026-08-14"]


if __name__ == "__main__":
    test_hold_1_replaces_daily()
    test_hold_3_locks_seat_skips_today_buy()
    test_hold_3_sells_after_age()
    test_stand_down_blocks_new_buys()
    test_haircut_only_on_new_buy()
    test_limit_5_skips_unless_dipped()
    test_haircut_falls_back_to_close_without_print()
    test_color_filter()
    test_extras_scale_with_book()
    test_spec_defaults_are_live_book()
    test_orig_window_uses_prior_close()
    print("ok")
