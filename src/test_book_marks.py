"""Lookback marks gate the BUY walk. No network.

Run: python -m src.test_book_marks
"""
from __future__ import annotations

import pandas as pd

from src.book_marks import annotate_one, boxes_from_row, veto_mask
from src.stock_book import _book_side, _buy_veto_mask


def _sig(**kw):
    base = dict(
        Ticker="X", s_join=0.9, s_sector=0.2, s_general=-0.07,
        s_news=0.0, s_ab=0.9, s_peer=0.5, s_heat=0.0, relvol=1.2,
    )
    base.update(kw)
    return base


def test_boxes_use_lookback_polarity() -> None:
    b = boxes_from_row(_sig())
    assert b["join"] == "good"
    assert b["gen"] == "bad"
    assert b["vol"] == "neutral"


def test_white_is_zero_red_not_a_hard_gate() -> None:
    # Modest gen-red → not white. Still a valid BUY if Cond/region hold.
    rec = annotate_one(boxes_from_row(_sig()), None)
    assert rec["lb_zero_red"] is False
    assert rec["lb_cond"] in ("good", "neutral")
    assert rec["lb_alarm"] is False


def test_alarm_is_purely_worse() -> None:
    today = boxes_from_row(_sig(s_join=0.2, s_ab=0.2, s_peer=0.2, s_sector=-0.4))
    yday = boxes_from_row(_sig(s_join=0.9, s_ab=0.9, s_peer=0.8, s_sector=0.3))
    rec = annotate_one(today, yday)
    assert rec["lb_alarm"] is True
    assert rec["lb_blue"] is False


def test_first_crack_is_a_fade() -> None:
    # Alarm on a still-green region → first_crack (mine: fade).
    today = boxes_from_row(_sig(s_join=0.9, s_ab=0.4, s_peer=0.2, s_sector=0.2,
                                s_news=0.0, s_general=-0.07))
    yday = boxes_from_row(_sig(s_join=0.9, s_ab=0.9, s_peer=0.8, s_sector=0.3,
                               s_news=0.2, s_general=0.2))
    rec = annotate_one(today, yday)
    assert rec["lb_alarm"] is True
    assert rec["lb_fade"] is True
    assert "first_crack" in rec["lb_tags"] or "first_crack" in rec["lb_setups"]


def test_buy_veto_drops_alarm_and_cond_red() -> None:
    rows = [
        dict(Ticker="OK", s_join=0.9, s_sector=0.2, s_general=-0.07,
             s_news=0.0, s_ab=0.9, s_peer=0.5, s_heat=0.0, relvol=1.2,
             size="mid", market_cap_m=2000, industry="a", sector="Energy",
             score_1d=0.8, core_1d=0.6, rebound=False, reasons="LEAD",
             context_label="LEAD", avg_vol_k=2000,
             lb_alarm=False, lb_fade=False, lb_cond="good", lb_region="good"),
        dict(Ticker="ALM", s_join=0.9, s_sector=0.2, s_general=-0.07,
             s_news=0.0, s_ab=0.9, s_peer=0.5, s_heat=0.0, relvol=1.2,
             size="mid", market_cap_m=2000, industry="b", sector="Energy",
             score_1d=1.2, core_1d=1.0, rebound=False, reasons="LEAD",
             context_label="LEAD", avg_vol_k=2000,
             lb_alarm=True, lb_fade=True, lb_cond="good", lb_region="good"),
        dict(Ticker="RED", s_join=-0.5, s_sector=-0.4, s_general=-0.4,
             s_news=0.0, s_ab=-0.4, s_peer=-0.3, s_heat=0.0, relvol=1.2,
             size="mid", market_cap_m=2000, industry="c", sector="Healthcare",
             score_1d=0.9, core_1d=0.7, rebound=False, reasons="",
             context_label="", avg_vol_k=2000,
             lb_alarm=False, lb_fade=False, lb_cond="bad", lb_region="bad"),
    ]
    df = pd.DataFrame(rows)
    v = veto_mask(df)
    assert bool(v[df.Ticker == "OK"].iloc[0]) is False
    assert bool(v[df.Ticker == "ALM"].iloc[0]) is True
    assert bool(v[df.Ticker == "RED"].iloc[0]) is True
    buys, _ = _book_side(df, "1d", 10)
    assert list(buys["Ticker"]) == ["OK"]
    assert bool(_buy_veto_mask(df)[df.Ticker == "ALM"].iloc[0]) is True


def main() -> None:
    tests = [
        test_boxes_use_lookback_polarity,
        test_white_is_zero_red_not_a_hard_gate,
        test_alarm_is_purely_worse,
        test_first_crack_is_a_fade,
        test_buy_veto_drops_alarm_and_cond_red,
    ]
    failed = 0
    for fn in tests:
        try:
            fn()
            print(f"ok  {fn.__name__}")
        except Exception as e:  # noqa: BLE001
            failed += 1
            print(f"FAIL {fn.__name__}: {e}")
    if failed:
        raise SystemExit(f"{failed} test(s) failed")
    print(f"{len(tests)} tests passed")


if __name__ == "__main__":
    main()
