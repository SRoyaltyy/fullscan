"""Unit tests for the boring-winner seater. No parquet required."""
from __future__ import annotations

import pandas as pd

from src.boring_winners_backtest import (
    SEATS,
    SECTOR_CAP,
    a_printed,
    annotate_actions,
    pick_seats,
    pool_mask,
    short_pool_mask,
)


def _row(**kw):
    base = dict(
        Ticker="X",
        ab="missing",
        peer="missing",
        ab_good=False,
        peer_good=False,
        blue=False,
        fade=False,
        first_crack=False,
        fade_x=False,
        short_b="low",
        sma20_b="above",
        B_and=False,
        A=False,
        A_bad=False,
        score=0,
        inv_score=0,
        points=10,
        n_red=0,
        relvol_rk=1,
        sector_name="Tech",
        ab_up=False,
    )
    base.update(kw)
    return base


def test_defaults_are_25_by_6():
    assert SEATS == 25
    assert SECTOR_CAP == 6


def test_fallback_band_when_no_a_no_blue():
    g = pd.DataFrame([
        _row(Ticker="KEEP", B_and=True, short_b="high", sma20_b="below", score=2),
        _row(Ticker="DROP", B_and=False, score=0),
        _row(Ticker="FADE", B_and=True, fade_x=True, score=2),
    ])
    mask, rule = pool_mask(g)
    assert rule == "Band"
    assert set(g.loc[mask, "Ticker"]) == {"KEEP"}


def test_a_beats_blue_when_cameras_printed():
    g = pd.DataFrame([
        _row(Ticker="AB", ab="good", ab_good=True, A=True, score=2),
        _row(Ticker="BL", blue=True, score=3),
    ])
    mask, rule = pool_mask(g)
    assert rule == "A"
    assert set(g.loc[mask, "Ticker"]) == {"AB"}
    assert a_printed(g) is True


def test_blue_fallback_needs_enough_names_for_seat_count():
    rows = [_row(Ticker=f"B{i}", blue=True, score=3) for i in range(15)]
    rows.append(_row(Ticker="BAND", B_and=True, score=2))
    g = pd.DataFrame(rows)
    _, rule25 = pool_mask(g, seats=25)
    assert rule25 == "Band"
    mask15, rule15 = pool_mask(g, seats=15)
    assert rule15 == "blue"
    assert set(g.loc[mask15, "Ticker"]) == {f"B{i}" for i in range(15)}


def test_sector_cap_and_score_order():
    rows = []
    for i in range(6):
        rows.append(_row(Ticker=f"H{i}", sector_name="Health", score=9 - i, points=20 - i, A=True, ab_good=True))
    rows.append(_row(Ticker="FIN", sector_name="Financial", score=8, points=15, A=True, ab_good=True))
    g = pd.DataFrame(rows)
    top = pick_seats(g, pd.Series([True] * len(g)), seats=5, cap=4)
    assert list(top["Ticker"]) == ["H0", "H1", "FIN", "H2", "H3"]


def test_pick_seats_fills_25_with_sector_cap_6():
    rows = []
    for sec, prefix in (("Health", "H"), ("Tech", "T"), ("Energy", "E"), ("Fin", "F"), ("Util", "U")):
        for i in range(8):
            rows.append(_row(Ticker=f"{prefix}{i}", sector_name=sec, score=20 - i, points=10))
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
    actions0, bought0, sold0, held0 = annotate_actions(None, ["AAA"])
    assert actions0 == ["buy"]
    assert bought0 == ["AAA"]
    assert sold0 == []
    assert held0 == []


def test_short_pool_prefers_fade_then_bad_a():
    fade = pd.DataFrame([
        _row(Ticker="FD", fade_x=True, inv_score=3),
        _row(Ticker="OK", A=True, ab="good", A_bad=False),
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
    test_fallback_band_when_no_a_no_blue()
    test_a_beats_blue_when_cameras_printed()
    test_blue_fallback_needs_enough_names_for_seat_count()
    test_sector_cap_and_score_order()
    test_pick_seats_fills_25_with_sector_cap_6()
    test_turnover_buy_hold_sell()
    test_short_pool_prefers_fade_then_bad_a()
    print("ok")
