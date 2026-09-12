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
from j_universe_prove import universe_verdict
from j_sleeve_prove import (
    elev_drop2, family_verdict, parse_factor_mine_md, recipe_verdict,
)
from j_winrate import MIN_FIRES, fire_winrate, hit_rate


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


def test_universe_verdict():
    keep = {"verdict": "KEEP"}
    kill = {"verdict": "KILL"}
    assert universe_verdict(keep) == "KEEP"
    assert universe_verdict(kill, keep) == "CONDITIONAL"
    assert universe_verdict(kill, kill) == "DEMOTE"


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


def test_parse_factor_mine_buys():
    import tempfile
    text = (
        "# Factor mine action — `flatten_h5`\n"
        "Side **long** · universe `flatten`\n"
        "| 2026-08-20 09:30 ET | **BUY** | `AG` | 66 | $20.55 |\n"
        "| 2026-08-20 09:30 ET | **SELL** | `INO` | 10 | $1.20 |\n"
        "| 2026-08-21 09:30 ET | **BUY** | `CYPH` | 20 | $1.32 |\n"
    )
    path = os.path.join(tempfile.gettempdir(), "fm_parse_test.md")
    open(path, "w", encoding="utf-8").write(text)
    parsed = parse_factor_mine_md(path)
    assert parsed["side"] == "long"
    assert [(b["date"], b["ticker"]) for b in parsed["buys"]] == [
        ("2026-08-20", "AG"),
        ("2026-08-21", "CYPH"),
    ]


def test_elev_drop2():
    rows = [
        {"date": "2026-08-20", "ticker": "A", "flags": {"J_ge0": True}, "J": 0.04},
        {"date": "2026-08-20", "ticker": "B", "flags": {"J_ge0": True}, "J": 0.02},
        {"date": "2026-08-20", "ticker": "C", "flags": {"J_ge0": True}, "J": 0.01},
        {"date": "2026-08-20", "ticker": "D", "flags": {"J_ge0": False}, "J": -0.02},
    ]
    kept = {(r["ticker"]) for r in elev_drop2(rows)}
    assert kept == {"C", "D"}  # drop two largest J≥0 (A, B)


def test_sleeve_recipe_verdict():
    assert recipe_verdict(0.40, 20, True, 0.01) == "KEEP"
    assert recipe_verdict(0.40, 20, False, 0.01) == "CONDITIONAL"
    assert recipe_verdict(0.05, 20, True, 0.01) == "KILL"
    assert recipe_verdict(0.40, 8, True, 0.01) == "null"
    keep = {"avoid_J_ge0": {"family_bar": "KEEP", "n": 20}}
    kill = {"avoid_J_ge0": {"family_bar": "KILL", "n": 20}}
    assert family_verdict(keep, keep) == "KEEP"
    assert family_verdict(keep, kill) == "CONDITIONAL"  # pooled only
    assert family_verdict(kill, kill) == "KILL"
    assert family_verdict(keep, keep, clock_ok=False) == "CONDITIONAL"


def test_fire_winrate_day_book():
    """Fire = ticker set changed. Win = rule day-mean H > baseline day-mean H."""
    base = [
        {"date": "2026-08-20", "ticker": "A", "net": 0.02},
        {"date": "2026-08-20", "ticker": "B", "net": -0.04},
        {"date": "2026-08-21", "ticker": "C", "net": 0.01},
        {"date": "2026-08-21", "ticker": "D", "net": 0.01},
        {"date": "2026-08-24", "ticker": "E", "net": 0.03},
        {"date": "2026-08-24", "ticker": "F", "net": 0.03},
    ]
    # 08-20: drop B (loser) → rule beats. 08-21: same set → not a fire.
    # 08-24: swap F for G worse → rule loses.
    rule = [
        {"date": "2026-08-20", "ticker": "A", "net": 0.02},
        {"date": "2026-08-21", "ticker": "C", "net": 0.01},
        {"date": "2026-08-21", "ticker": "D", "net": 0.01},
        {"date": "2026-08-24", "ticker": "E", "net": 0.03},
        {"date": "2026-08-24", "ticker": "G", "net": -0.05},
    ]
    wr = fire_winrate(base, rule)
    assert wr["n_fires"] == 2
    assert wr["n_wins"] == 1
    assert wr["n_losses"] == 1
    assert abs(wr["win_rate"] - 0.5) < 1e-12
    assert wr["clears_55"] is False
    assert wr["verdict"] == "FAIL"
    hits = hit_rate(
        [{"net": 0.02}, {"net": -0.01}, {"net": 0.00, "i_net": 0.01}],
        "net",
    )
    assert hits["n"] == 3
    assert hits["n_pos"] == 1


def _n_day_books(n_fires, n_wins):
    """n fire days; first n_wins beat the no-rule book."""
    base, rule = [], []
    for i in range(n_fires):
        d = f"d{i:03d}"
        base.append({"date": d, "ticker": "A", "net": 0.0})
        rule.append({"date": d, "ticker": "B",
                     "net": 0.01 if i < n_wins else -0.01})
    return fire_winrate(base, rule)


def test_fire_floor_30():
    """CLEAR needs ≥30 fires and >55%. n=8 and n=29 are PROVISIONAL."""
    assert MIN_FIRES == 30
    wr30 = _n_day_books(30, 17)  # 17/30 = 56.7%
    assert wr30["n_fires"] == 30
    assert wr30["verdict"] == "CLEAR"
    assert wr30["clears_55"] is True
    wr29 = _n_day_books(29, 29)  # 100% but n<30
    assert wr29["verdict"] == "PROVISIONAL"
    assert wr29["clears_55"] is False
    assert wr29["prints_55"] is True
    wr8 = _n_day_books(8, 6)  # prior 6/8 CLEAR — now demoted
    assert wr8["verdict"] == "PROVISIONAL"
    assert wr8["clears_55"] is False
    wr_fail = _n_day_books(30, 16)  # 16/30 = 53.3%
    assert wr_fail["verdict"] == "FAIL"


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
    test_universe_verdict()
    test_parse_factor_mine_buys()
    test_elev_drop2()
    test_sleeve_recipe_verdict()
    test_fire_winrate_day_book()
    test_fire_floor_30()
    print("ok")
