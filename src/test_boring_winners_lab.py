"""Hold-lock and HARD_RED rules. No parquet required."""
from __future__ import annotations

from src.boring_winners_lab import Spec, extras_for, filter_color, step_hold


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
    held = {"AAA": {"age": 2, "src": "book"}}
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
    held = {"OLD": {"age": 0, "src": "book"}}
    rec = step_hold(
        held,
        [{"ticker": "OLD", "source": "book", "stack": "blue"},
         {"ticker": "NEW", "source": "extra", "stack": "blue"}],
        hold=1, seats=2, hard_red="haircut_5", market_hard=True,
        rets={"OLD": 1.0, "NEW": -2.0},
    )
    by = {n["ticker"]: n["ret_1d"] for n in rec["names"]}
    assert by["OLD"] == 1.0
    assert by["NEW"] == 3.0
    assert rec["day_pnl"] == 2.0


def test_limit_5_skips_unless_dipped():
    rec = step_hold(
        {},
        [{"ticker": "DIP", "source": "book", "stack": "blue"},
         {"ticker": "UP", "source": "book", "stack": "blue"}],
        hold=1, seats=2, hard_red="limit_5", market_hard=True,
        rets={"DIP": -6.0, "UP": 1.0},
    )
    assert rec["bought"] == ["DIP"]
    assert any(s["ticker"] == "UP" and s["why"] == "limit_miss" for s in rec["skipped"])


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


if __name__ == "__main__":
    test_hold_1_replaces_daily()
    test_hold_3_locks_seat_skips_today_buy()
    test_hold_3_sells_after_age()
    test_stand_down_blocks_new_buys()
    test_haircut_only_on_new_buy()
    test_limit_5_skips_unless_dipped()
    test_color_filter()
    test_extras_scale_with_book()
    test_spec_defaults_are_live_book()
    print("ok")
