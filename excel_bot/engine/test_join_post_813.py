"""Clock-gate + leak checks for the post-8-13 join miner."""
from __future__ import annotations

import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)

from excel_clock_gate import assert_excel_clock_gate, assert_feature_legal
from join_post_813 import EXCEL_ATOMS, HOLD_CUT, excel_features


def test_clock_gate_locked():
    clocks = assert_excel_clock_gate()
    assert len(clocks["groups"]["value_mine_open"]) == 44
    assert clocks["groups"]["fill_mine_open"] == [
        "A", "B", "C", "G", "J", "K", "L", "M", "O", "IR", "IS", "IT",
    ]


def test_atoms_are_legal():
    for col, lag, kind in EXCEL_ATOMS:
        assert_feature_legal(kind, col, lag)


def test_same_row_hi_rejected():
    try:
        assert_feature_legal("value", "H", 0)
    except ValueError as e:
        assert "OUT" in str(e) or "H" in str(e)
    else:
        raise AssertionError("same-row H must be illegal")
    try:
        assert_feature_legal("value", "M", 0)
    except ValueError:
        pass
    else:
        raise AssertionError("same-row M number must be illegal")


def test_holdout_is_after_813():
    assert HOLD_CUT == "2026-08-13"


def test_excel_features_ignore_same_row_h():
    hist = {
        "AAA": [
            ("2026-08-12", {"open": 10.0, "h": 0.06, "i": 0.04, "vol": 100}),
            ("2026-08-13", {"open": 10.5, "h": -0.02, "i": 0.01, "vol": 200}),
        ]
    }
    xl = excel_features(hist, "AAA", "2026-08-14", 10.0)
    assert xl["J"] is not None
    assert xl["H_l1"] == -0.02
    assert "h" not in xl  # same-row H is not a feature


if __name__ == "__main__":
    test_clock_gate_locked()
    test_atoms_are_legal()
    test_same_row_hi_rejected()
    test_holdout_is_after_813()
    test_excel_features_ignore_same_row_h()
    print("ok")
