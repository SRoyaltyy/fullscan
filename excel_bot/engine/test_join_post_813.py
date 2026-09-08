"""Clock-gate + leak checks for the post-8-13 join miner."""
from __future__ import annotations

import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)

from excel_clock_gate import assert_excel_clock_gate, assert_feature_legal
from join_post_813 import (
    DISCOVERY, EXCEL_ATOMS, HOLD_CUT, PROVE, excel_features, is_session,
    j_fresh, j_from_opens, prior_bars,
)


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
    assert xl["J_fresh"] is True
    assert xl["prior_open_date"] == "2026-08-13"


def test_sunday_is_not_a_session():
    assert is_session("2026-08-28") is True   # Friday
    assert is_session("2026-08-29") is False  # Saturday
    assert is_session("2026-08-30") is False  # Sunday
    assert is_session("2026-08-31") is True   # Monday


def test_j_skips_weekend_open():
    hist = {
        "AAA": [
            ("2026-08-28", {"open": 10.0, "h": 0.01, "i": 0.01, "vol": 100}),
            ("2026-08-29", {"open": 99.0, "h": 0.50, "i": 0.50, "vol": 100}),
            ("2026-08-30", {"open": 99.0, "h": 0.50, "i": 0.50, "vol": 100}),
        ]
    }
    prior = prior_bars(hist, "AAA", "2026-08-31")
    assert [d for d, _ in prior] == ["2026-08-28"]
    xl = excel_features(hist, "AAA", "2026-08-31", 10.2)
    assert abs(xl["J"] - 0.02) < 1e-9
    assert xl["prior_open_date"] == "2026-08-28"
    assert xl["J_fresh"] is True


def test_april_open_is_stale_j():
    # 2026-04-24 is Friday (session); 04-26 dump is Sunday and is skipped.
    hist = {
        "AAA": [
            ("2026-04-24", {"open": 10.0, "h": 0.01, "i": 0.0, "vol": 100}),
            ("2026-04-26", {"open": 10.0, "h": 0.01, "i": 0.0, "vol": 100}),
        ]
    }
    xl = excel_features(hist, "AAA", "2026-08-13", 12.0)
    assert xl["prior_open_date"] == "2026-04-24"
    assert xl["J"] is not None
    assert xl["J_fresh"] is False
    assert j_fresh(xl["prior_open_date"], "2026-08-13") is False
    # Sunday-only history is not a session Open — no J.
    xl2 = excel_features({"AAA": [hist["AAA"][1]]}, "AAA", "2026-08-13", 12.0)
    assert xl2["J"] is None
    assert xl2["J_fresh"] is False


def test_prove_is_after_discovery():
    assert DISCOVERY[1] < PROVE[0]
    assert HOLD_CUT < DISCOVERY[0]


def test_j_is_open_only_and_ignores_same_row_labels():
    """J must not move if same-row H/I/close/high/low are scrambled."""
    hist = {
        "AAA": [
            ("2026-08-25", {
                "open": 10.0, "h": -0.08, "i": -0.09,
                "close": 9.0, "high": 12.0, "low": 8.0, "vol": 100,
            }),
        ]
    }
    today_open = 10.4
    xl = excel_features(hist, "AAA", "2026-08-27", today_open)
    assert abs(xl["J"] - j_from_opens(10.4, 10.0)) < 1e-12
    hist2 = {
        "AAA": [
            ("2026-08-25", {
                "open": 10.0, "h": 0.99, "i": 0.99,
                "close": 99.0, "high": 99.0, "low": 1.0, "vol": 100,
            }),
        ]
    }
    xl2 = excel_features(hist2, "AAA", "2026-08-27", today_open)
    assert xl["J"] == xl2["J"]
    try:
        assert_feature_legal("value", "H", 0)
        raise AssertionError("H")
    except ValueError:
        pass
    try:
        assert_feature_legal("value", "core_score", 0)
        raise AssertionError("core_score")
    except ValueError:
        pass


if __name__ == "__main__":
    test_clock_gate_locked()
    test_atoms_are_legal()
    test_same_row_hi_rejected()
    test_holdout_is_after_813()
    test_excel_features_ignore_same_row_h()
    test_sunday_is_not_a_session()
    test_j_skips_weekend_open()
    test_april_open_is_stale_j()
    test_prove_is_after_discovery()
    test_j_is_open_only_and_ignores_same_row_labels()
    print("ok")
