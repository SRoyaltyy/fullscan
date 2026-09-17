"""Leak + Cyrus fee-bar tests for Excel factor-mine Phase B."""
from __future__ import annotations

import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)

from excel_clock_gate import (  # noqa: E402
    SAME_ROW_LEAK_ABORT, assert_excel_clock_gate, assert_feature_legal,
)
from excel_factor_mine import (  # noqa: E402
    ATOM_SPEC, CORE_SEEDS, GRID_ONLY_ATOMS, OPEN_SEEDS,
    assert_atoms_legal, combo_atoms, combo_hits, combo_name, cutoff_from_dates,
    expand_combos, keep_verdict, score_hits, split_rows, time_slot,
    walk_folds, write_board,
)
from join_post_813 import FEE_RT  # noqa: E402
from j_winrate import MIN_FIRES, WIN_BAR  # noqa: E402


def test_clock_and_leak_abort():
    clocks = assert_excel_clock_gate()
    assert_atoms_legal()
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
    for name, (col, lag, kind) in ATOM_SPEC.items():
        assert col not in ("H", "I"), name
        assert_feature_legal(kind, col, lag)
        if col in SAME_ROW_LEAK_ABORT:
            assert lag >= 1, name


def test_fee_bar_keep_fail():
    """KEEP needs ≥30 prove fires and strictly >55% after-fee WR."""
    assert MIN_FIRES == 30
    assert WIN_BAR == 0.55
    assert FEE_RT == 0.0015
    v, why = keep_verdict(30, 17 / 30)  # 56.7%
    assert v == "KEEP"
    assert "56.7%" in why
    v29, why29 = keep_verdict(29, 1.0)
    assert v29 == "FAIL"
    assert "thin" in why29
    v55, _ = keep_verdict(30, 0.55)  # not strictly greater
    assert v55 == "FAIL"
    v_fail, why_fail = keep_verdict(371, 0.472)
    assert v_fail == "FAIL"
    assert "371" in why_fail
    assert "47.2%" in why_fail
    v0, _ = keep_verdict(0, None)
    assert v0 == "FAIL"


def test_lift_only_never_keep():
    """A 1.83 lift with sub-55% after-fee WR is FAIL."""
    v, why = keep_verdict(371, 0.48)
    assert v == "FAIL"
    assert "KEEP" not in v
    # keep_verdict does not take lift — calling site must not use it
    assert "lift" not in why.lower()


def test_after_fee_net_uses_futubull():
    """H = 20 bp is a win after 15 bp; H = 10 bp is a loss."""
    win = score_hits([{"net": 0.0020 - FEE_RT}])
    lose = score_hits([{"net": 0.0010 - FEE_RT}])
    assert win["n_pos"] == 1 and win["wr"] == 1.0
    assert lose["n_pos"] == 0 and lose["wr"] == 0.0
    assert win["mean_net"] > 0
    assert lose["mean_net"] < 0


def test_time_split_fail_closed():
    rows = [
        {"date": "2026-06-01", "net": 0.01, "flags": {"J_ge0": True}},
        {"date": "2026-07-06", "net": 0.01, "flags": {"J_ge0": True}},
        {"date": "2026-08-01", "net": -0.01, "flags": {"J_ge0": True}},
        {"date": "", "net": 0.99, "flags": {"J_ge0": True}},
        {"date": None, "net": 0.99, "flags": {"J_ge0": True}},
    ]
    disc, hold = split_rows(rows, "2026-07-06")
    assert [r["date"] for r in disc] == ["2026-06-01"]
    assert [r["date"] for r in hold] == ["2026-07-06", "2026-08-01"]
    assert time_slot("", "2026-07-06") is None
    assert time_slot(None, "2026-07-06") is None
    # empty dates never pollute discovery
    assert all(r.get("date") for r in disc)


def test_discovery_never_sees_holdout():
    dates = [f"2026-06-{d:02d}" for d in range(1, 21)] + [
        f"2026-08-{d:02d}" for d in range(1, 11)
    ]
    cutoff = cutoff_from_dates(dates, locked="2026-07-06")
    # locked date not on tape → 30% holdout, still a real cutoff
    assert cutoff
    rows = [{"date": d, "net": 0.01} for d in dates]
    disc, hold = split_rows(rows, cutoff)
    assert disc and hold
    assert max(r["date"] for r in disc) < cutoff
    assert min(r["date"] for r in hold) >= cutoff
    for lo, hi, chunk in walk_folds([r["date"] for r in disc]):
        assert hi < cutoff or hi < min(r["date"] for r in hold)
        assert all(d < cutoff for d in chunk)


def test_locked_cutoff_when_present():
    dates = ["2026-06-01", "2026-07-06", "2026-08-01"]
    assert cutoff_from_dates(dates, locked="2026-07-06") == "2026-07-06"


def test_combo_and_is_intersection():
    rows = [
        {"net": 0.01, "flags": {"J_ge0": True, "ER_m1": True}},
        {"net": 0.02, "flags": {"J_ge0": True, "ER_m1": False}},
        {"net": -0.01, "flags": {"J_ge0": False, "ER_m1": True}},
    ]
    hits = combo_hits(rows, ("J_ge0", "ER_m1"))
    assert len(hits) == 1
    assert hits[0]["net"] == 0.01
    assert combo_name(("ER_m1", "J_ge0")) == "J_ge0|ER_m1"


def test_expand_is_systematic():
    combos = expand_combos(OPEN_SEEDS, CORE_SEEDS)
    names = {combo_name(a) for a in combos}
    assert "J_ge0" in names
    assert "J_ge0|ER_m1" in names
    assert "J_ge0|prior_bear_engulf" in names
    assert "J_ge0|ER_m1|prior_bear_engulf" in names
    singles = [a for a in combos if len(a) == 1]
    pairs = [a for a in combos if len(a) == 2]
    trips = [a for a in combos if len(a) == 3]
    assert len(singles) == len(OPEN_SEEDS)
    assert len(pairs) == len(OPEN_SEEDS) * (len(OPEN_SEEDS) - 1) // 2
    assert len(trips) == len(CORE_SEEDS) * (len(CORE_SEEDS) - 1) * (len(CORE_SEEDS) - 2) // 6
    assert len(names) == len(combos)


def test_no_flatten_import():
    path = os.path.join(HERE, "excel_factor_mine.py")
    text = open(path, encoding="utf-8").read()
    assert "flatten_robust" in text
    assert "from flatten" not in text
    assert "import flatten" not in text
    assert "from flatten_robust" not in text


def test_grid_only_not_in_seeds():
    for a in GRID_ONLY_ATOMS:
        assert a not in ATOM_SPEC
        assert a not in OPEN_SEEDS


def test_fail_headline_prefers_material_n():
    """Thin 100% must not be the FAIL card when a ≥30 near-miss exists."""
    from excel_factor_mine import headline_from
    scored = [
        {
            "rule": "J_ge0|ER_m1|prior_hanging",
            "hold": {"n": 3, "wr": 1.0, "why": "thin n=3, after-fee WR 100.0%"},
            "verdict": "FAIL",
        },
        {
            "rule": "FQ|J_lt0",
            "hold": {"n": 432, "wr": 0.549, "why": "n=432, after-fee WR 54.9% ≤ 55%"},
            "verdict": "FAIL",
        },
        {
            "rule": "J_ge0|ER_m1",
            "hold": {"n": 602, "wr": 0.452, "why": "n=602, after-fee WR 45.2% ≤ 55%"},
            "verdict": "FAIL",
        },
    ]
    hl = headline_from(scored, "2026-07-06")
    assert hl["verdict"] == "FAIL"
    assert hl["best"]["rule"] == "FQ|J_lt0"
    assert hl["best"]["n"] == 432
    assert "54.9%" in hl["text"]
    assert "J_ge0|ER_m1" in hl["text"]
    assert "n=602" in hl["text"]


def test_board_says_fail_plainly():
    scored = [{
        "rule": "J_ge0|ER_m1", "kind": "combo2", "atoms": ["J_ge0", "ER_m1"],
        "disc": {"n": 551, "wr": 0.49, "mean_net": 0.01, "verdict": "FAIL",
                 "why": "n=551, after-fee WR 49.0% ≤ 55%"},
        "hold": {"n": 371, "wr": 0.472, "mean_net": 0.02, "verdict": "FAIL",
                 "why": "n=371, after-fee WR 47.2% ≤ 55%"},
        "verdict": "FAIL",
        "why": "n=371, after-fee WR 47.2% ≤ 55%",
        "walk": [], "walk_folds_wr_gt_50": 0, "n_walk_folds": 3,
    }]
    from excel_clock_gate import gate_payload
    from excel_factor_mine import headline_from
    import tempfile
    hl = headline_from(scored, "2026-07-06")
    assert hl["verdict"] == "FAIL"
    assert "371" in hl["text"]
    assert "47.2%" in hl["text"]
    payload = {
        "status": "DONE", "cutoff": "2026-07-06", "n_tickers": 10,
        "n_rows": 100, "headline": hl, "scored": scored, "hypothesis": scored,
        "gate": gate_payload(), "leak": "PASS",
    }
    with tempfile.TemporaryDirectory() as td:
        path = os.path.join(td, "EXCEL_FACTOR_MINE.md")
        write_board(payload, path)
        md = open(path, encoding="utf-8").read()
    assert "FAIL" in md
    assert "KEEP bar" in md
    assert "flatten_robust" in md
    assert "Lift-only is never KEEP" in md or "lift-only is never KEEP" in md.lower()


def test_open_features_ignore_today_hlc():
    from excel_open_features import feature_flags, open_features
    prior = [
        {"o": 10.0, "h": 10.4, "l": 9.7, "c": 10.1, "v": 2e6},
        {"o": 10.1, "h": 10.2, "l": 9.5, "c": 9.6, "v": 2e6},
    ]
    for _ in range(8):
        prior.insert(0, {"o": 10.0, "h": 10.2, "l": 9.8, "c": 10.0, "v": 1e6})
    xl = open_features(prior, 10.3)
    xl2 = open_features(prior, 99.0)
    assert xl["df"] == xl2["df"]
    assert xl["BQ_l1"] == xl2["BQ_l1"]
    flags = feature_flags(xl)
    assert "J_ge0" in flags
    assert flags["J_ge0"] is True  # 10.3 > 10.1
    assert xl.get("same_row_df") is False


if __name__ == "__main__":
    test_clock_and_leak_abort()
    test_fee_bar_keep_fail()
    test_lift_only_never_keep()
    test_after_fee_net_uses_futubull()
    test_time_split_fail_closed()
    test_discovery_never_sees_holdout()
    test_locked_cutoff_when_present()
    test_combo_and_is_intersection()
    test_expand_is_systematic()
    test_no_flatten_import()
    test_grid_only_not_in_seeds()
    test_fail_headline_prefers_material_n()
    test_board_says_fail_plainly()
    test_open_features_ignore_today_hlc()
    print("ok")
