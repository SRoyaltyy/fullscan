"""Unit tests for the mined-stack seater. No parquet required."""
from __future__ import annotations

import pandas as pd

from pathlib import Path

from src.boring_winners_backtest import (
    SEATS,
    SECTOR_CAP,
    a_printed,
    annotate_actions,
    book_gate_ok,
    fill_overlay,
    fill_returns_from_finviz,
    fill_seats,
    fill_short_overlay,
    pick_seats,
    pool_mask,
    short_pool_mask,
    stack_label,
    stack_masks,
)


def _row(**kw):
    base = dict(
        Ticker="X",
        ab="missing",
        peer="missing",
        join="missing",
        ab_good=False,
        peer_good=False,
        blue=False,
        fade=False,
        first_crack=False,
        fade_x=False,
        short_b="low",
        sma20_b="above",
        B_and=False,
        B_or=False,
        A=False,
        Aand=False,
        A_bad=False,
        hot=False,
        steady_f=False,
        white_f=False,
        join_f=False,
        cond_f=False,
        alarm_f=False,
        rsi_os=False,
        gap_dn=False,
        score=0,
        mine_score=0,
        inv_score=0,
        points=10,
        n_red=0,
        relvol_rk=2,
        relvol_b="normal",
        sector_name="Tech",
        ab_up=False,
    )
    base.update(kw)
    return base


def test_defaults_are_25_by_6():
    assert SEATS == 25
    assert SECTOR_CAP == 6


def test_lottery_when_no_blue_no_alarm():
    g = pd.DataFrame([
        _row(Ticker="RSI", rsi_os=True, mine_score=1),
        _row(Ticker="GAP", gap_dn=True, mine_score=1),
        _row(Ticker="FADE", rsi_os=True, fade_x=True, mine_score=1),
    ])
    mask, rule = pool_mask(g)
    assert rule == "rsi_oversold"
    assert set(g.loc[mask, "Ticker"]) == {"RSI"}


def test_steady_blue_leads_when_no_scalp():
    g = pd.DataFrame([
        _row(Ticker="CORE", blue=True, steady_f=True, mine_score=12),
        _row(Ticker="AL", alarm_f=True, mine_score=4),
    ])
    names = [n for n, m in stack_masks(g) if bool(m.any())]
    assert names[0] == "steady_blue"
    seats, rule = fill_seats(g, seats=1)
    assert list(seats["Ticker"]) == ["CORE"]
    assert rule.startswith("steady_blue")


def test_scalp_reserved_then_core():
    g = pd.DataFrame([
        _row(Ticker="HOT", hot=True, Aand=True, A=True, mine_score=10, sector_name="Tech"),
        _row(Ticker="CORE", blue=True, steady_f=True, mine_score=12, sector_name="Health"),
    ])
    seats, rule = fill_seats(g, seats=2)
    assert list(seats["Ticker"]) == ["HOT", "CORE"]
    assert rule.startswith("hot_ab_peer")


def test_fill_walks_stacks_without_dupes():
    rows = [
        _row(Ticker="SB", blue=True, steady_f=True, mine_score=12, sector_name="Tech"),
        _row(Ticker="BW", blue=True, white_f=True, mine_score=10, sector_name="Health"),
        _row(Ticker="BL", blue=True, mine_score=6, sector_name="Energy"),
        _row(Ticker="AL", alarm_f=True, mine_score=3, sector_name="Util"),
    ]
    g = pd.DataFrame(rows)
    seats, rule = fill_seats(g, seats=4)
    assert list(seats["Ticker"]) == ["SB", "BW", "BL", "AL"]
    assert list(seats["stack"]) == ["steady_blue", "blue_white", "blue", "alarm_rebound"]
    assert "steady_blue" in rule and "alarm_rebound" in rule


def test_a_printed_when_cameras_on():
    g = pd.DataFrame([_row(Ticker="AB", ab="good", ab_good=True, A=True)])
    assert a_printed(g) is True
    assert a_printed(pd.DataFrame([_row(Ticker="X")])) is False


def test_sector_cap_and_score_order():
    rows = []
    for i in range(6):
        rows.append(_row(Ticker=f"H{i}", sector_name="Health", mine_score=9 - i, score=9 - i, points=20 - i, blue=True))
    rows.append(_row(Ticker="FIN", sector_name="Financial", mine_score=8, score=8, points=15, blue=True))
    g = pd.DataFrame(rows)
    top = pick_seats(g, pd.Series([True] * len(g)), seats=5, cap=4)
    assert list(top["Ticker"]) == ["H0", "H1", "FIN", "H2", "H3"]


def test_pick_seats_fills_25_with_sector_cap_6():
    rows = []
    for sec, prefix in (("Health", "H"), ("Tech", "T"), ("Energy", "E"), ("Fin", "F"), ("Util", "U")):
        for i in range(8):
            rows.append(_row(Ticker=f"{prefix}{i}", sector_name=sec, mine_score=20 - i, score=20 - i, points=10, blue=True))
    g = pd.DataFrame(rows)
    top = pick_seats(g, pd.Series([True] * len(g)))
    assert len(top) == 25
    assert top.groupby("sector_name").size().max() <= 6


def test_turnover_buy_hold_sell():
    actions, bought, sold, held = annotate_actions({"AAA", "BBB"}, ["BBB", "CCC"])
    assert actions == ["hold", "buy"]
    assert bought == ["CCC"]
    assert sold == ["AAA"]
    assert held == ["BBB"]


def test_stack_label_order():
    assert stack_label(_row(hot=True, Aand=True, blue=True, steady_f=True)) == "hot_ab_peer"
    assert stack_label(_row(blue=True, steady_f=True)) == "steady_blue"
    assert stack_label(_row(blue=True, white_f=True)) == "blue_white"
    assert stack_label(_row(blue=True)) == "blue"
    assert stack_label(_row(Aand=True)) == "ab_and_peer"
    assert stack_label(_row(alarm_f=True)) == "alarm_rebound"
    assert stack_label(_row(fade_x=True, blue=True)) == "fade"
    assert stack_label(_row()) == "none"
    assert stack_label(None) == "book_only"


def test_book_gate_rejects_micro():
    assert book_gate_ok(500, 800, "small") is True
    assert book_gate_ok(200, 800, "small") is False
    assert book_gate_ok(500, 100, "small") is False
    assert book_gate_ok(5000, 800, "micro") is False


def test_overlay_drops_fade_from_book():
    g = pd.DataFrame([
        _row(Ticker="KEEP", blue=True, mine_score=6),
        _row(Ticker="FADE", blue=True, fade_x=True, mine_score=9),
    ])
    seats, rule, dropped = fill_overlay(g, ["KEEP", "FADE"], universe={})
    assert [s["ticker"] for s in seats] == ["KEEP"]
    assert [d["ticker"] for d in dropped] == ["FADE"]
    assert dropped[0]["why"] == "fade"
    assert rule == "blue"


def test_overlay_adds_gated_extra_and_swaps_weak_book():
    g = pd.DataFrame([
        _row(Ticker="WEAK", mine_score=0, sector_name="Tech"),
        _row(Ticker="ADD", blue=True, steady_f=True, mine_score=12, sector_name="Health"),
        _row(Ticker="JUNK", rsi_os=True, mine_score=1, sector_name="Energy"),
    ])
    universe = {
        "WEAK": {"Ticker": "WEAK", "market_cap_m": 800, "avg_vol_k": 900, "size": "mid", "liquid": True, "sector": "Tech", "industry": "Soft", "score_1d": 0.4},
        "ADD": {"Ticker": "ADD", "market_cap_m": 1200, "avg_vol_k": 700, "size": "mid", "liquid": True, "sector": "Health", "industry": "Bio", "score_1d": 0.9},
        "JUNK": {"Ticker": "JUNK", "market_cap_m": 900, "avg_vol_k": 600, "size": "mid", "liquid": True, "sector": "Energy", "industry": "Oil", "score_1d": 0.2},
        "MICRO": {"Ticker": "MICRO", "market_cap_m": 80, "avg_vol_k": 900, "size": "micro", "liquid": True, "sector": "Tech", "industry": "Soft", "score_1d": 1.0},
    }
    seats, rule, dropped = fill_overlay(g, ["WEAK"], universe=universe, seats=1, max_extras=5)
    names = [s["ticker"] for s in seats]
    assert names == ["ADD"]
    assert "JUNK" not in names
    assert seats[0]["source"] == "extra"
    assert any(d["ticker"] == "WEAK" and d["why"] == "swapped_for_mine_extra" for d in dropped)
    assert "steady_blue" in rule


def test_overlay_does_not_force_25_on_thin_book():
    g = pd.DataFrame([_row(Ticker="ONLY", blue=True, mine_score=5)])
    universe = {
        "ONLY": {"Ticker": "ONLY", "market_cap_m": 900, "avg_vol_k": 800, "size": "mid", "liquid": True, "sector": "Tech", "industry": "Soft", "score_1d": 0.5},
    }
    seats, _, _ = fill_overlay(g, ["ONLY"], universe=universe)
    assert [s["ticker"] for s in seats] == ["ONLY"]
    assert len(seats) == 1


def test_overlay_keeps_book_names_past_sector_cap():
    rows = [_row(Ticker=f"H{i}", blue=True, mine_score=5, sector_name="Healthcare") for i in range(8)]
    g = pd.DataFrame(rows)
    buys = [f"H{i}" for i in range(8)]
    seats, _, dropped = fill_overlay(g, buys, universe={}, cap=6)
    assert [s["ticker"] for s in seats] == buys
    assert dropped == []


def test_overlay_rejects_ungated_extra():
    g = pd.DataFrame([
        _row(Ticker="BOOK", blue=True, mine_score=5),
        _row(Ticker="PENNY", blue=True, steady_f=True, mine_score=20),
    ])
    universe = {
        "PENNY": {"Ticker": "PENNY", "market_cap_m": 40, "avg_vol_k": 80, "size": "micro", "liquid": True, "sector": "Health", "industry": "Bio", "score_1d": 2.0},
    }
    seats, _, _ = fill_overlay(g, ["BOOK"], universe=universe)
    assert [s["ticker"] for s in seats] == ["BOOK"]


def test_short_overlay_is_sell_and_fade():
    g = pd.DataFrame([
        _row(Ticker="FD", fade_x=True, inv_score=3),
        _row(Ticker="OK", fade_x=True, inv_score=4),
        _row(Ticker="BOOKONLY", fade_x=False, inv_score=5),
    ])
    shorts = fill_short_overlay(g, ["FD", "BOOKONLY"])
    assert [s["ticker"] for s in shorts] == ["FD"]


def test_short_is_fade_only():
    fade = pd.DataFrame([
        _row(Ticker="FD", fade_x=True, inv_score=3),
        _row(Ticker="OK", A=True, ab="good", A_bad=True),
    ])
    mask, rule = short_pool_mask(fade)
    assert rule == "fade"
    assert set(fade.loc[mask, "Ticker"]) == {"FD"}
    empty = pd.DataFrame([_row(Ticker="OK", A_bad=True, ab="bad")])
    _, rule = short_pool_mask(empty)
    assert rule == "none"


def test_finviz_fills_missing_1d_keeps_panel():
    import tempfile
    tmp = Path(tempfile.mkdtemp())
    (tmp / "finviz_2026-08-21.csv").write_text(
        "Ticker,Price,Change\nAAA,10,1%\nBBB,20,2%\n", encoding="utf-8"
    )
    (tmp / "finviz_2026-08-24.csv").write_text(
        "Ticker,Price,Change\nAAA,11,10%\nBBB,19,-5%\n", encoding="utf-8"
    )
    g = pd.DataFrame([
        {"Ticker": "AAA", "date": "2026-08-21", "ret_1d": None, "ret_2d": None, "ret_3d": None, "ret_1w": None},
        {"Ticker": "BBB", "date": "2026-08-21", "ret_1d": 1.25, "ret_2d": None, "ret_3d": None, "ret_1w": None},
    ])
    out = fill_returns_from_finviz(
        g, export_dir=tmp, session_cal=["2026-08-21", "2026-08-24"],
    )
    assert abs(float(out.loc[out.Ticker == "AAA", "ret_1d"].iloc[0]) - 10.0) < 1e-6
    assert abs(float(out.loc[out.Ticker == "BBB", "ret_1d"].iloc[0]) - 1.25) < 1e-6


def test_finviz_1d_falls_back_to_change():
    import tempfile
    tmp = Path(tempfile.mkdtemp())
    (tmp / "finviz_2026-08-21.csv").write_text(
        "Ticker,Price,Change\nCCC,,\n", encoding="utf-8"
    )
    (tmp / "finviz_2026-08-24.csv").write_text(
        "Ticker,Price,Change\nCCC,,3.5%\n", encoding="utf-8"
    )
    g = pd.DataFrame([
        {"Ticker": "CCC", "date": "2026-08-21", "ret_1d": None, "ret_2d": None, "ret_3d": None, "ret_1w": None},
    ])
    out = fill_returns_from_finviz(
        g, export_dir=tmp, session_cal=["2026-08-21", "2026-08-24"],
    )
    assert abs(float(out.loc[0, "ret_1d"]) - 3.5) < 1e-6


if __name__ == "__main__":
    test_defaults_are_25_by_6()
    test_lottery_when_no_blue_no_alarm()
    test_steady_blue_leads_when_no_scalp()
    test_scalp_reserved_then_core()
    test_fill_walks_stacks_without_dupes()
    test_a_printed_when_cameras_on()
    test_sector_cap_and_score_order()
    test_pick_seats_fills_25_with_sector_cap_6()
    test_turnover_buy_hold_sell()
    test_stack_label_order()
    test_book_gate_rejects_micro()
    test_overlay_drops_fade_from_book()
    test_overlay_adds_gated_extra_and_swaps_weak_book()
    test_overlay_does_not_force_25_on_thin_book()
    test_overlay_keeps_book_names_past_sector_cap()
    test_overlay_rejects_ungated_extra()
    test_short_overlay_is_sell_and_fade()
    test_short_is_fade_only()
    test_finviz_fills_missing_1d_keeps_panel()
    test_finviz_1d_falls_back_to_change()
    print("ok")
