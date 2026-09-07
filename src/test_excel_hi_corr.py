"""Leak / formula checks for the Excel H/I correlation mine."""
from __future__ import annotations

import sys
from datetime import date
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "excel_bot" / "engine"))

from mine_hi_corr import (  # noqa: E402
    add_excel_features,
    add_labels,
    close_gates,
    open_gates,
)


def _toy():
    # 8 sessions, one ticker. Hand numbers so H/I/gap are obvious.
    rows = [
        # date, o, h, l, c, v
        (date(2026, 1, 2), 10.0, 10.5, 9.8, 10.2, 1_000_000),
        (date(2026, 1, 5), 10.3, 11.0, 10.0, 10.8, 1_200_000),
        (date(2026, 1, 6), 10.5, 10.6, 9.5, 9.6, 2_000_000),
        (date(2026, 1, 7), 9.2, 9.4, 8.8, 9.0, 3_000_000),
        (date(2026, 1, 8), 9.1, 9.8, 9.0, 9.7, 1_100_000),
        (date(2026, 1, 9), 9.8, 10.2, 9.6, 10.0, 1_000_000),
        (date(2026, 1, 12), 10.4, 10.5, 10.0, 10.1, 900_000),
        (date(2026, 1, 13), 10.0, 10.8, 9.9, 10.6, 1_500_000),
    ]
    df = pd.DataFrame({
        "ticker": ["TOY"] * len(rows),
        "date": [r[0] for r in rows],
        "open": [r[1] for r in rows],
        "high": [r[2] for r in rows],
        "low": [r[3] for r in rows],
        "close": [r[4] for r in rows],
        "volume": [r[5] for r in rows],
    })
    return add_labels(add_excel_features(df))


def test_h_i_match_excel_formulas():
    df = _toy()
    # H = (close-open)/open
    np.testing.assert_allclose(df["H"], (df["close"] - df["open"]) / df["open"])
    # I = (close-prev close)/prev close
    prev = df["close"].shift(1)
    np.testing.assert_allclose(df["I"].iloc[1:], (df["close"].iloc[1:] - prev.iloc[1:]) / prev.iloc[1:])


def test_i_equals_gap_plus_h_times_one_plus_gap():
    df = _toy().iloc[1:]  # need prior close
    ident = df["gap"] + df["H"] * (1 + df["gap"])
    np.testing.assert_allclose(df["I"], ident, rtol=1e-12)


def test_open_gates_never_use_same_row_h_or_i():
    df = _toy()
    # Corrupt today's H and I — open gates must not flip.
    g0 = {k: v.fillna(False).to_numpy().copy() for k, v in open_gates(df).items()}
    df2 = df.copy()
    df2["H"] = 99.0
    df2["I"] = -99.0
    g1 = {k: v.fillna(False).to_numpy() for k, v in open_gates(df2).items()}
    for k in g0:
        np.testing.assert_array_equal(g0[k], g1[k], err_msg=k)


def test_close_gates_may_use_today_h_i():
    df = _toy()
    g0 = close_gates(df)["H_ge_p2"].fillna(False).to_numpy()
    df2 = df.copy()
    df2["H"] = 0.05
    g1 = close_gates(df2)["H_ge_p2"].fillna(False).to_numpy()
    assert g1.sum() > g0.sum()


def test_labels_lead_do_not_include_today_i_in_stack():
    df = _toy()
    # y_i_stack_1d = close[t+1]/close[t] - 1 == I[t+1]
    nxt = df["I"].shift(-1)
    np.testing.assert_allclose(
        df["y_i_stack_1d"].iloc[:-1], nxt.iloc[:-1], rtol=1e-12)
