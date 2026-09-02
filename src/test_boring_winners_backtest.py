"""Unit tests for the boring-winner seater. No parquet required."""
from __future__ import annotations

import pandas as pd

from src.boring_winners_backtest import a_printed, pick_seats, pool_mask


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
        score=0,
        points=10,
        n_red=0,
        relvol_rk=1,
        sector_name="Tech",
        ab_up=False,
    )
    base.update(kw)
    return base


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


def test_sector_cap_and_score_order():
    rows = []
    for i in range(6):
        rows.append(_row(Ticker=f"H{i}", sector_name="Health", score=9 - i, points=20 - i, A=True, ab_good=True))
    rows.append(_row(Ticker="FIN", sector_name="Financial", score=8, points=15, A=True, ab_good=True))
    g = pd.DataFrame(rows)
    top = pick_seats(g, pd.Series([True] * len(g)), seats=5, cap=4)
    assert list(top["Ticker"]) == ["H0", "H1", "FIN", "H2", "H3"]


if __name__ == "__main__":
    test_fallback_band_when_no_a_no_blue()
    test_a_beats_blue_when_cameras_printed()
    test_sector_cap_and_score_order()
    print("ok")
