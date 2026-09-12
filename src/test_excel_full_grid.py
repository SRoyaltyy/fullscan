"""Clock / coverage checks for the full A–JO workbook mine."""
from __future__ import annotations

import json
import sys
import tempfile
from datetime import date
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "excel_bot" / "engine"))

from mine_full_grid import (  # noqa: E402
    FILL_OPEN, NEVER_L0, VALUE_OPEN,
    build_gates, combo_specs, hyst_on, is_open_atom,
    load_dumps, s2d,
)


def test_lag1_always_open_even_for_close_letters():
    assert is_open_atom("T", 1, "v") is True
    assert is_open_atom("T", 1, "f") is True
    assert is_open_atom("H", 1, "v") is True


def test_same_row_h_i_never_open():
    assert is_open_atom("H", 0, "v") is False
    assert is_open_atom("I", 0, "f") is False
    assert "H" in NEVER_L0 and "I" in NEVER_L0


def test_fill_vs_value_clock():
    assert is_open_atom("O", 0, "f") is True
    assert is_open_atom("O", 0, "v") is False
    assert is_open_atom("B", 0, "f") is True
    assert is_open_atom("B", 0, "v") is False
    assert is_open_atom("AH", 0, "v") is True
    assert is_open_atom("AH", 0, "f") is False
    assert "O" in FILL_OPEN
    assert "AH" in VALUE_OPEN


def test_hysteresis_enter5_off2():
    scores = [0, 6, 4, 3, 1, 0, 7, 2]
    df = pd.DataFrame({
        "ticker": ["X"] * len(scores),
        "date": [date(2026, 1, d) for d in range(2, 2 + len(scores))],
        "A_fs": scores,
        "B_fs": [0] * len(scores),
        "C_fs": [0] * len(scores),
        "G_fs": [0] * len(scores),
        "J_fs": [0] * len(scores),
    })
    on = hyst_on(df, ("A", "B", "C", "G", "J"), enter=5.0, off=2.0)
    # off, on (6), stay (4), stay (3), off (1), off, on (7), off (2)
    np.testing.assert_array_equal(
        on, [False, True, True, True, False, False, True, False])


def test_build_gates_covers_every_letter_and_skips_same_row_hi():
    n = 12
    df = pd.DataFrame({
        "ticker": ["AAA"] * n,
        "date": [date(2026, 3, i + 1) for i in range(n)],
        "H": np.linspace(-0.02, 0.03, n),
        "AH_v": [0, 0, 1, 2, 0, 1, 0, 0, 3, 0, 1, 0],
        "T_v": [0, 1, 0, 0, 2, 0, 1, 0, 0, 1, 0, 0],
        "O_f": ["green", "none", "green", "red"] * 3,
        "A_f": ["green"] * n,
        "B_f": ["green"] * n,
        "C_f": ["green"] * n,
        "G_f": ["green"] * n,
        "J_f": ["green"] * n,
        "A_fs": [1.5] * n,
        "B_fs": [1.5] * n,
        "C_fs": [1.5] * n,
        "G_fs": [1.0] * n,
        "J_fs": [1.0] * n,
        "AH_v_l1": [0, 0, 0, 1, 2, 0, 1, 0, 0, 3, 0, 1],
        "T_v_l1": [0, 0, 1, 0, 0, 2, 0, 1, 0, 0, 1, 0],
        "O_f_l1": ["none", "green", "none", "green"] * 3,
        "H_v": np.linspace(-0.02, 0.03, n),
        "H_v_l1": np.linspace(-0.01, 0.02, n),
        "I_v": np.linspace(-0.03, 0.04, n),
    })
    inv = {
        "letters": ["A", "B", "C", "G", "J", "O", "AH", "T", "H", "I"],
        "text_tokens": {},
    }
    gates = build_gates(df, inv)
    letters_in_gates = {g[2] for g in gates.values()}
    for let in ("A", "B", "C", "G", "J", "O", "AH", "T"):
        assert let in letters_in_gates or any(
            let in name for name in gates), let
    assert "AH_ge1" in gates
    assert "T_l1_ge1" in gates
    assert "O_green" in gates
    assert "light5_O" in gates
    # same-row H/I values must not be features
    assert "H_eq1" not in gates and "H_gt0" not in gates
    assert "I_eq1" not in gates and "I_gt0" not in gates
    # yesterday H is fair
    assert "H_l1_gt0" in gates
    assert gates["light5_O"][1] == "open"
    assert gates["T_ge1"][1] == "close"  # T value lag0 is close
    assert gates["T_l1_ge1"][1] == "open"


def test_combo_specs_and_with_o_and_light():
    n = 8
    df = pd.DataFrame({
        "ticker": ["AAA"] * n,
        "date": [date(2026, 4, i + 1) for i in range(n)],
        "AH_v": [1, 0, 1, 1, 0, 1, 0, 1],
        "O_f": ["green", "green", "none", "green", "green", "none", "green", "green"],
        "A_fs": [2] * n, "B_fs": [2] * n, "C_fs": [1] * n,
        "G_fs": [1] * n, "J_fs": [1] * n,
        "A_f": ["green"] * n, "B_f": ["green"] * n, "C_f": ["green"] * n,
        "G_f": ["green"] * n, "J_f": ["green"] * n,
    })
    inv = {"letters": ["A", "B", "C", "G", "J", "O", "AH"], "text_tokens": {}}
    gates = build_gates(df, inv)
    specs = combo_specs(gates, n)
    names = {s[0] for s in specs}
    assert any(x.startswith("AH_ge1__and_O_green") for x in names)
    assert any(x.startswith("AH_ge1__and_light5") for x in names)
    assert any(x == "A_green__O_green" or x.startswith("A_green__") for x in names)


def test_load_dumps_dedups_same_ticker_date(tmp_path=None):
    td = Path(tempfile.mkdtemp())
    serial = 46000  # ~2025-12

    def dump(name, extra_days=0):
        days = []
        for i in range(40):
            days.append({
                "date": serial + i,
                "open": 10.0, "close": 10.2, "volume": 2_000_000,
                "cells": {
                    "A": {"v": serial + i, "f": "90EE90"},
                    "C": {"v": 10.0},
                    "B": {"v": 10.2},
                    "O": {"v": 1, "f": "90EE90"},
                    "AH": {"v": 1 if i % 3 == 0 else 0},
                },
            })
        (td / name).write_text(json.dumps({
            "ticker": "FAKE", "days": days, "all_cols": True,
        }))
    dump("FAKE.json")
    dump("FAKE__tprev.json")  # same dates — must collapse
    df, inv = load_dumps(td)
    assert inv["n_tickers"] == 1
    assert df.duplicated(["ticker", "date"]).sum() == 0
    assert inv["n_letters"] >= 4
    assert "O" in inv["fill"]


def test_s2d_roundtrip_serial():
    d = s2d(46074)
    assert d.year in (2025, 2026)


def run_all():
    test_lag1_always_open_even_for_close_letters()
    test_same_row_h_i_never_open()
    test_fill_vs_value_clock()
    test_hysteresis_enter5_off2()
    test_build_gates_covers_every_letter_and_skips_same_row_hi()
    test_combo_specs_and_with_o_and_light()
    test_load_dumps_dedups_same_ticker_date()
    test_s2d_roundtrip_serial()
    print("test_excel_full_grid: ok")


if __name__ == "__main__":
    run_all()
