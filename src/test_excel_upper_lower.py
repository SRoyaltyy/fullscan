"""Past-row-only checks for the upper→lower mine."""
from __future__ import annotations

import sys
from datetime import date
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "excel_bot" / "engine"))

from mine_upper_lower import (  # noqa: E402
    add_future, add_past, build_past_gates, cheap_either_way,
    collapse_keeps, letter_gates, op_family, sleeve_key,
)


def _df():
    n = 10
    return pd.DataFrame({
        "ticker": ["AAA"] * n,
        "date": [date(2026, 3, i + 1) for i in range(n)],
        "open": np.full(n, 10.0),
        "close": 10 + np.linspace(-0.2, 0.3, n),
        "H": np.linspace(-0.02, 0.03, n),
        "CE_v": [0, 0, 1, 0, 0, 1, 0, 0, 0, 1],
        "O_f": ["red", "green"] * 5,
        "H_v": np.linspace(-0.02, 0.03, n),
    })


def test_future_labels_look_ahead_not_back():
    df = add_future(_df())
    # 2d uses close of next row vs today's open
    assert df.loc[0, "y_2d"] == df.loc[1, "close"] / df.loc[0, "open"] - 1


def test_past_gates_are_lag_only():
    df = add_past(add_future(_df()), ["CE", "O", "H"])
    assert "CE_v_l1" in df.columns and "O_f_l1" in df.columns
    inv = {
        "letters": ["CE", "O", "H"],
        "text_tokens": {},
    }
    disc = np.ones(len(df), dtype=bool)
    gates = build_past_gates(df, inv, disc)
    assert any(k.startswith("CE_l1_") for k in gates)
    assert any(k.startswith("O_l1_") for k in gates)
    # same-row CE / O must not be gated
    assert "CE_eq1" not in gates
    assert "O_green" not in gates
    # yesterday H is allowed (upper row)
    assert any(k.startswith("H_l1_") for k in gates)


def test_cheap_promotes_short_when_mean_down():
    y = np.array([-0.02] * 400 + [0.0] * 50)
    tick = np.array(["T"] * 50 + [f"X{i}" for i in range(400)])
    hold = np.ones(450, dtype=bool)
    mask = np.ones(450, dtype=bool)
    # 400 of one ticker + 50 others — need 50 tickers. fix:
    tick = np.array([f"T{i%60}" for i in range(450)])
    assert cheap_either_way(mask, y, tick, hold) == "short"
    assert cheap_either_way(mask, -y, tick, hold) == "long"


def test_letter_gates_are_lag_only():
    l1 = np.array([0.0, 1.0, 0.0, 1.0])
    l2 = np.array([np.nan, 0.0, 1.0, 0.0])
    f1 = np.array([0, 1, 2, 1], dtype=np.int8)
    f2 = np.array([0, 0, 1, 2], dtype=np.int8)
    disc = np.ones(4, dtype=bool)
    gates = letter_gates("CE", l1, l2, f1, f2, None, None, disc)
    names = [g[0] for g in gates]
    assert names
    assert all("_l1_" in n or "_l2_" in n or n.startswith("CE_l1")
               for n in names)
    assert "CE_eq1" not in names
    assert "CE_green" not in names
    assert op_family("CE_l1_eq1") == op_family("CE_l1_ge1") == "pos"


def test_collapse_keeps_twins():
    def row(gate, let, mean=0.0125, n=4000):
        return {
            "gate": gate, "letter": let, "plain": f"{let} yesterday",
            "label": "y_h1", "side": "long", "verdict": "KEEP",
            "hold_mean": mean, "hold_n": n, "why": [],
        }
    rows = [
        row("CE_l1_eq1", "CE"),
        row("CE_l1_ge1", "CE"),
        row("CE_l1_gt0", "CE"),
        row("S_l1_qlo", "S", mean=0.006, n=20000),
        row("DI_l1_qlo", "DI", mean=0.006, n=20000),
        row("CF_l1_eq1", "CF", mean=-0.002, n=8000),
    ]
    rows[-1]["verdict"] = "KILL"
    uniq = collapse_keeps(rows)
    assert len(uniq) == 2
    keys = {sleeve_key(r) for r in uniq}
    assert ("y_h1", "long", 4000, 0.0125) in keys
    assert ("y_h1", "long", 20000, 0.006) in keys


def run_all():
    test_future_labels_look_ahead_not_back()
    test_past_gates_are_lag_only()
    test_cheap_promotes_short_when_mean_down()
    test_letter_gates_are_lag_only()
    test_collapse_keeps_twins()
    print("test_excel_upper_lower: ok")


if __name__ == "__main__":
    run_all()
