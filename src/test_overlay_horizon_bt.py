"""Horizon backtest — leak clock, fees, concentration, leftover hold.

Run: python3 -m src.test_overlay_horizon_bt
"""
from __future__ import annotations

from pathlib import Path

from src import overlay_horizon_bt as oh
from src import finviz_style_flags as fsf


def test_hold_window_counts_entry_morning() -> None:
    cal = ["2026-08-17", "2026-08-18", "2026-08-19", "2026-08-20"]
    assert oh.hold_window(cal, "2026-08-17", 3) == [
        "2026-08-17", "2026-08-18", "2026-08-19",
    ]
    assert oh.hold_window(cal, "2026-08-17", 1) == ["2026-08-17"]
    assert oh.hold_window(cal, "2026-08-20", 3) == ["2026-08-20"]
    assert oh.hold_window(cal, "2026-08-21", 1) == []


def test_prior_export_never_same_day() -> None:
    cal = ["2026-04-26", "2026-08-13", "2026-08-14"]
    assert oh.prior_export(cal, "2026-08-13") == "2026-04-26"
    assert oh.prior_export(cal, "2026-08-14") == "2026-08-13"
    assert oh.prior_export(cal, "2026-04-26") is None
    assert oh.prior_export(cal, "2026-08-13") < "2026-08-13"


def test_fees_charge_both_sides() -> None:
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
    buy = oh.order_fees(10, 20.0, "buy", fees)
    sell = oh.order_fees(10, 20.0, "sell", fees)
    assert buy > 0
    assert sell > buy
    tr = oh.unit_trade(20.0, 21.0, fees, notional=1000)
    assert tr is not None
    assert tr["shares"] == 50
    assert tr["pnl"] < 50.0  # 50 * $1 minus two-sided fees
    assert tr["fee_in"] > 0 and tr["fee_out"] > 0


def test_concentration_rejects_one_or_two_days() -> None:
    # One day is the whole book — ungated-h5 shape.
    c = oh.concentration([100.0, 1.0, 1.0, 1.0])
    assert c["reject"] is True
    assert c["top2"] >= oh.TOP2_REJECT
    even = oh.concentration([10.0] * 10)
    assert even["reject"] is False
    assert even["top1"] == 0.1


def test_leftover_respects_min_hold() -> None:
    cal = ["d1", "d2", "d3"]
    bars = {
        "d1": {"AAA": {"open": 10.0, "close": 10.0}},
        "d2": {"AAA": {"open": 10.0, "close": 10.0}},
        "d3": {"AAA": {"open": 11.0, "close": 11.0}},
    }
    fees = {
        "commission_per_share": 0.0, "commission_min_per_order": 0.0,
        "commission_max_pct_of_amount": 1.0, "platform_per_share": 0.0,
        "platform_min_per_order": 0.0, "platform_max_pct_of_amount": 1.0,
        "settlement_per_share": 0.0, "regulatory_pct_of_amount_sell_only": 0.0,
        "regulatory_min_per_order": 0.0, "taf_per_share_sell_only": 0.0,
        "taf_min_per_order": 0.0, "taf_max_per_order": 0.0,
    }
    picks = {"d1": ["AAA"], "d2": [], "d3": []}
    scores = {"d1": 1.0, "d2": 1.0, "d3": 1.0}
    book = oh.leftover_book(cal, picks, bars, fees, hold=2, scores=scores)
    sells = [t for t in book["trades"] if t["side"] == "SELL"]
    assert sells
    assert sells[0]["date"] == "d3"
    assert sells[0]["held"] >= 2


def test_live_gate_blocks_new_buys() -> None:
    cal = ["d1", "d2"]
    bars = {
        "d1": {"AAA": {"open": 10.0, "close": 10.5}},
        "d2": {"BBB": {"open": 10.0, "close": 12.0},
               "AAA": {"open": 10.5, "close": 10.5}},
    }
    fees = {
        "commission_per_share": 0.0, "commission_min_per_order": 0.0,
        "commission_max_pct_of_amount": 1.0, "platform_per_share": 0.0,
        "platform_min_per_order": 0.0, "platform_max_pct_of_amount": 1.0,
        "settlement_per_share": 0.0, "regulatory_pct_of_amount_sell_only": 0.0,
        "regulatory_min_per_order": 0.0, "taf_per_share_sell_only": 0.0,
        "taf_min_per_order": 0.0, "taf_max_per_order": 0.0,
    }
    picks = {"d1": ["AAA"], "d2": ["BBB"]}
    scores = {"d1": 2.0, "d2": 2.0}
    live = {"d1": True, "d2": False}
    book = oh.leftover_book(
        cal, picks, bars, fees, hold=1, scores=scores, live_ok=live)
    buys = [t["ticker"] for t in book["trades"] if t["side"] == "BUY"]
    assert buys == ["AAA"]


def test_hard_red_sits() -> None:
    cal = ["d1"]
    bars = {"d1": {"AAA": {"open": 10.0, "close": 12.0}}}
    fees = {
        "commission_per_share": 0.0, "commission_min_per_order": 0.0,
        "commission_max_pct_of_amount": 1.0, "platform_per_share": 0.0,
        "platform_min_per_order": 0.0, "platform_max_pct_of_amount": 1.0,
        "settlement_per_share": 0.0, "regulatory_pct_of_amount_sell_only": 0.0,
        "regulatory_min_per_order": 0.0, "taf_per_share_sell_only": 0.0,
        "taf_min_per_order": 0.0, "taf_max_per_order": 0.0,
    }
    book = oh.leftover_book(
        cal, {"d1": ["AAA"]}, bars, fees, hold=1,
        scores={"d1": -5.0})
    buys = [t for t in book["trades"] if t["side"] == "BUY"]
    assert buys == []


def test_decide_fail_when_book_worse() -> None:
    ic = {"both_tape": True, "thin": False, "n": 40,
          "walk_forward": {"ok": True, "thin": False}}
    gate = oh.decide(ic, {"pnl": 100.0}, {"pnl": 50.0},
                     [2.0] * 10, n_avoided=40)
    assert gate["verdict"] == "FAIL"
    assert gate["excess_usd"] == -50.0


def test_decide_pass_requires_tape_and_book_and_not_concentrated() -> None:
    ic = {"both_tape": True, "thin": False, "n": 40,
          "walk_forward": {"ok": True, "thin": False}}
    gate = oh.decide(ic, {"pnl": 50.0}, {"pnl": 80.0},
                     [3.0] * 10, n_avoided=40)
    assert gate["verdict"] == "PASS"


def test_does_not_import_live_policy() -> None:
    text = Path(oh.__file__).read_text(encoding="utf-8")
    assert "LIVE_POLICY" not in text or "untouched" in text
    assert "from . import sleeve_merge" not in text
    assert "from src import sleeve_merge" not in text
    assert "flatten_robust" in text  # named as untouched / shaped
    assert fsf.HIGH_FPE == 35.0


def main() -> None:
    test_hold_window_counts_entry_morning()
    test_prior_export_never_same_day()
    test_fees_charge_both_sides()
    test_concentration_rejects_one_or_two_days()
    test_leftover_respects_min_hold()
    test_live_gate_blocks_new_buys()
    test_hard_red_sits()
    test_decide_fail_when_book_worse()
    test_decide_pass_requires_tape_and_book_and_not_concentrated()
    test_does_not_import_live_policy()
    print("test_overlay_horizon_bt: 10 ok")


if __name__ == "__main__":
    main()
