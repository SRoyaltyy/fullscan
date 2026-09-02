"""Tiny unit tests for full_feature_mine helpers."""
from __future__ import annotations

import math

import pandas as pd

from src import full_feature_mine as ff


def test_bucket_rsi():
    assert ff._rsi_bucket(25) == "oversold"
    assert ff._rsi_bucket(55) == "mid"
    assert ff._rsi_bucket(78) == "overbought"
    assert ff._rsi_bucket(None) == "missing"


def test_relvol_bucket():
    assert ff._relvol_bucket(2.4) == "hot"
    assert ff._relvol_bucket(0.3) == "dead"
    assert ff._relvol_bucket(1.1) == "normal"


def test_flag_filters():
    rows = [
        {"blue": True, "white": False, "alarm": False, "relvol_b": "hot", "rsi_b": "mid"},
        {"blue": False, "white": True, "alarm": False, "relvol_b": "dead", "rsi_b": "overbought"},
        {"blue": True, "white": True, "alarm": False, "relvol_b": "hot", "rsi_b": "mid"},
    ]
    got = ff._where(rows, {"blue": True, "relvol_b": "hot"})
    assert len(got) == 2


def test_steady_fat_flags():
    row = {
        "white": True,
        "alarm": False,
        "fade": False,
        "n_red": 0,
        "rsi_b": "mid",
        "perf_w_b": "mid",
        "ins_sell": False,
        "peer_q": "q3",
        "relvol_b": "hot",
        "ab_good": True,
        "peer_good": True,
        "vol_good": True,
        "ins_buy": True,
        "catal": True,
        "short_b": "high",
    }
    assert ff._steady(row) is True
    assert ff._fat(row) is True


def test_summarize_empty():
    s = ff.summarize([], "1d")
    assert s["n"] == 0


def test_summarize_skips_nan():
    rows = [
        {"ret_1d": 2.0, "xs_1d": 1.0},
        {"ret_1d": float("nan"), "xs_1d": float("nan")},
        {"ret_1d": None, "xs_1d": None},
        {"ret_1d": -1.0, "xs_1d": -2.0},
    ]
    s = ff.summarize(rows, "1d")
    assert s["n"] == 2
    assert s["hit"] == 0.5
    assert s["mean"] == 0.5
    assert s["mean_xs"] == -0.5
    assert ff._nfmt(float("nan")) == "\u2014"


def test_panel_roundtrip_flags():
    df = pd.DataFrame([
        {"Ticker": "AAA", "date": "2026-08-20", "blue": True, "white": False,
         "alarm": False, "ret_1d": 1.2, "ret_2d": 2.0, "ret_3d": None,
         "ret_1w": None, "ret_2w": None},
        {"Ticker": "BBB", "date": "2026-08-20", "blue": False, "white": True,
         "alarm": True, "ret_1d": -0.4, "ret_2d": -1.0, "ret_3d": None,
         "ret_1w": None, "ret_2w": None},
    ])
    rows = df.to_dict("records")
    ff.attach_excess(rows)
    item = ff.pack_group("blue", "mark", [r for r in rows if r["blue"]], ff.summarize_all(rows), min_n=1)
    assert item is not None
    assert item["1d_n"] == 1


if __name__ == "__main__":
    test_bucket_rsi()
    test_relvol_bucket()
    test_flag_filters()
    test_steady_fat_flags()
    test_summarize_empty()
    test_summarize_skips_nan()
    test_panel_roundtrip_flags()
    print("ok")
