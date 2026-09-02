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


def test_join_band_when_no_hit_camera():
    g = pd.DataFrame([
        _row(Ticker="KEEP", B_and=True, join_f=True, mine_score=2),
        _row(Ticker="DROP", B_and=True, join_f=False, mine_score=2),
        _row(Ticker="FADE", B_and=True, join_f=True, fade_x=True, mine_score=2),
        _row(Ticker="HOT", B_and=True, join_f=True, hot=True, mine_score=2),
    ])
    mask, rule = pool_mask(g)
    assert rule == "join_band"
    assert set(g.loc[mask, "Ticker"]) == {"KEEP"}


def test_hot_ab_peer_beats_loose_a():
    g = pd.DataFrame([
        _row(Ticker="HOT", hot=True, Aand=True, A=True, ab="good", peer="good", ab_good=True, peer_good=True, mine_score=12),
        _row(Ticker="AONLY", A=True, ab="good", ab_good=True, B_or=True, mine_score=3),
        _row(Ticker="BL", blue=True, mine_score=3),
    ])
    names = [n for n, m in stack_masks(g) if bool(m.any())]
    assert names[0] == "hot_ab_peer"
    seats, rule = fill_seats(g, seats=1)
    assert list(seats["Ticker"]) == ["HOT"]
    assert rule.startswith("hot_ab_peer")


def test_fill_walks_stacks_without_dupes():
    rows = [
        _row(Ticker="H1", hot=True, Aand=True, A=True, ab="good", peer="good", mine_score=12, sector_name="Tech"),
        _row(Ticker="H2", hot=True, Aand=True, A=True, ab="good", peer="good", mine_score=11, sector_name="Health"),
        _row(Ticker="AND", Aand=True, A=True, ab="good", peer="good", mine_score=8, sector_name="Energy"),
        _row(Ticker="BL", blue=True, mine_score=3, sector_name="Util"),
    ]
    g = pd.DataFrame(rows)
    seats, rule = fill_seats(g, seats=4)
    assert list(seats["Ticker"]) == ["H1", "H2", "AND", "BL"]
    assert "hot_ab_peer" in rule and "ab_and_peer" in rule and "blue" in rule
    assert list(seats["stack"]) == ["hot_ab_peer", "hot_ab_peer", "ab_and_peer", "blue"]


def test_a_printed_when_cameras_on():
    g = pd.DataFrame([_row(Ticker="AB", ab="good", ab_good=True, A=True)])
    assert a_printed(g) is True
    g2 = pd.DataFrame([_row(Ticker="X")])
    assert a_printed(g2) is False


def test_sector_cap_and_score_order():
    rows = []
    for i in range(6):
        rows.append(_row(Ticker=f"H{i}", sector_name="Health", mine_score=9 - i, score=9 - i, points=20 - i, A=True, ab_good=True))
    rows.append(_row(Ticker="FIN", sector_name="Financial", mine_score=8, score=8, points=15, A=True, ab_good=True))
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


def test_short_pool_prefers_fade_then_bad_a():
    fade = pd.DataFrame([
        _row(Ticker="FD", fade_x=True, inv_score=3),
        _row(Ticker="OK", A=True, ab="good"),
    ])
    mask, rule = short_pool_mask(fade)
    assert rule == "fade"
    assert set(fade.loc[mask, "Ticker"]) == {"FD"}

    bad = pd.DataFrame([
        _row(Ticker="BD", ab="bad", A_bad=True, inv_score=2),
        _row(Ticker="OK", ab="good", A=True),
    ])
    mask, rule = short_pool_mask(bad)
    assert rule == "A_bad"
    assert set(bad.loc[mask, "Ticker"]) == {"BD"}

    empty = pd.DataFrame([_row(Ticker="OK")])
    mask, rule = short_pool_mask(empty)
    assert rule == "none"
    assert not bool(mask.any())


if __name__ == "__main__":
    test_defaults_are_25_by_6()
    test_join_band_when_no_hit_camera()
    test_hot_ab_peer_beats_loose_a()
    test_fill_walks_stacks_without_dupes()
    test_a_printed_when_cameras_on()
    test_sector_cap_and_score_order()
    test_pick_seats_fills_25_with_sector_cap_6()
    test_turnover_buy_hold_sell()
    test_short_pool_prefers_fade_then_bad_a()
    print("ok")
