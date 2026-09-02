"""Unit tests for the mined-stack seater. No parquet required."""
from __future__ import annotations

import pandas as pd

from src.boring_winners_backtest import (
    SEATS,
    SECTOR_CAP,
    a_printed,
    annotate_actions,
    fill_seats,
    pick_seats,
    pool_mask,
    short_pool_mask,
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
    test_short_is_fade_only()
    print("ok")
