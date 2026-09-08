"""Clock lock + candle/tally reconstruction for the beyond-J open-gates cut."""
from __future__ import annotations

import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)

from excel_clock_gate import (  # noqa: E402
    OPEN_44_TALLY_SUBS, SAME_ROW_LEAK_ABORT, assert_excel_clock_gate,
    assert_feature_legal,
)
from excel_open_features import (  # noqa: E402
    RECIPES, assert_lag_atoms, assert_recipes_legal, bq_of, bu_of,
    df_pattern, dg_pattern, feature_flags, open_features,
)
from j_winrate import MIN_FIRES, fire_winrate  # noqa: E402


def test_clock_and_leak_abort():
    clocks = assert_excel_clock_gate()
    assert_lag_atoms()
    assert_recipes_legal()
    by = {r["col"]: r for r in clocks["columns"]}
    for col in ("DF", "DG", "DH", "BB", "BQ", "BU"):
        assert by[col]["value_mine"] == "close"
    for col in SAME_ROW_LEAK_ABORT:
        try:
            assert_feature_legal("value", col, 0)
        except ValueError as e:
            assert "LEAK" in str(e) or "lag" in str(e).lower() or col in str(e)
        else:
            raise AssertionError(f"same-row {col} must abort")
        assert_feature_legal("value", col, 1)
    for col in OPEN_44_TALLY_SUBS:
        assert_feature_legal("value", col, 0)
    for rec in RECIPES:
        for col, lag in rec[6]:
            if col in SAME_ROW_LEAK_ABORT:
                assert lag >= 1, rec[0]


def test_doji_and_hammer():
    doji = df_pattern({"o": 10.0, "c": 10.02, "h": 11.0, "l": 9.0})
    assert "Doji" in doji
    hammer = df_pattern({"o": 10.0, "c": 10.2, "h": 10.25, "l": 9.0})
    assert hammer == "Bullish Hammer"


def test_engulf():
    prev = {"o": 10.5, "c": 10.0, "h": 10.6, "l": 9.9}
    last = {"o": 9.8, "c": 10.7, "h": 10.8, "l": 9.7}
    assert dg_pattern(prev, last) == "Bullish Engulfing"


def test_bq_bu_need_completed_bar():
    prev = {"o": 10, "h": 11, "l": 9, "c": 10.5, "v": 1e6}
    last = {"o": 10.2, "h": 10.1, "l": 9.5, "c": 9.6, "v": 2e6}
    assert bu_of(last, prev) == -1  # typical fell
    assert bq_of(last, prev) < 0


def test_open_features_ignore_today_hlc():
    prior = [
        {"o": 10.0, "h": 10.4, "l": 9.7, "c": 10.1, "v": 2e6},
        {"o": 10.1, "h": 10.2, "l": 9.5, "c": 9.6, "v": 2e6},
    ]
    xl = open_features(prior, 10.3)
    assert xl["J"] is not None
    assert abs(xl["J"] - (10.3 - 10.1) / 10.1) < 1e-12
    assert xl["same_row_df"] is False
    assert xl["same_row_bb"] is False
    assert xl["same_row_bq"] is False
    assert xl["BU_l1"] in (-1, 1)
    assert xl["AH"] is not None
    flags = feature_flags(xl)
    assert "J_ge0" in flags
    # today's open only — candles/BQ are from prior[-1], not a fake today bar
    xl2 = open_features(prior, 99.0)
    assert xl["df"] == xl2["df"]
    assert xl["BQ_l1"] == xl2["BQ_l1"]
    assert xl["BB_l1"] == xl2["BB_l1"]


def test_no_flatten_import():
    path = os.path.join(HERE, "excel_open_gates.py")
    text = open(path, encoding="utf-8").read()
    assert "flatten_robust" in text
    assert "from flatten" not in text
    assert "import flatten" not in text


def test_fire_floor_still_30():
    assert MIN_FIRES == 30
    base, rule = [], []
    for i in range(30):
        d = f"d{i:03d}"
        base.append({"date": d, "ticker": "A", "net": 0.0})
        rule.append({"date": d, "ticker": "B", "net": 0.01 if i < 17 else -0.01})
    wr = fire_winrate(base, rule)
    assert wr["verdict"] == "CLEAR"


if __name__ == "__main__":
    test_clock_and_leak_abort()
    test_doji_and_hammer()
    test_engulf()
    test_bq_bu_need_completed_bar()
    test_open_features_ignore_today_hlc()
    test_no_flatten_import()
    test_fire_floor_still_30()
    print("ok")
