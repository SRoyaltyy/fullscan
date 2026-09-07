"""Clock-aware excel_bot miner: leak-free clocks, ship bar, no live wire."""
from __future__ import annotations

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
ENG = ROOT / "excel_bot" / "engine"
sys.path.insert(0, str(ENG))

from clock import (  # noqa: E402
    CLOSE_IDX, OPEN_IDX, SHIP, annotate_days, assert_clock_legal,
    feature_clock, futu_cost, hold_exit_idx, mcap_cost, simulate_clock,
)
from mine_clock import (  # noqa: E402
    apply_hold1_sibling, apply_horizon_sibling, lottery, pack_cell,
)
from patterns import detect_pattern, new_lag_defs, pattern_matrix  # noqa: E402


def test_open_and_close_partition():
    assert set(OPEN_IDX).isdisjoint(set(CLOSE_IDX))
    assert len(OPEN_IDX) + len(CLOSE_IDX) == 15
    # A is open; D/H/N are close
    assert 0 in OPEN_IDX
    assert 3 in CLOSE_IDX and 7 in CLOSE_IDX and 13 in CLOSE_IDX


def test_core_score_cannot_enter_open():
    assert feature_clock(range(10)) == "close"
    try:
        assert_clock_legal(range(10), "open")
        raise AssertionError("should have refused")
    except ValueError as e:
        assert "open entry illegal" in str(e)


def test_a_keyed_may_enter_open():
    assert feature_clock([0]) == "open"
    assert_clock_legal([0], "open")
    assert_clock_legal([0], "close")  # conservative twin is legal


def test_open_score_cols_are_open_clock():
    assert feature_clock(OPEN_IDX) == "open"


def test_hold_indices_are_sleeve_native():
    # open hold1 = same day; close hold1 = next day
    assert hold_exit_idx(10, 1, "open", 50) == 10
    assert hold_exit_idx(10, 1, "close", 50) == 11
    assert hold_exit_idx(10, 3, "open", 50) == 12
    assert hold_exit_idx(10, 3, "close", 50) == 13


def _days(n=12, green_a=True):
    rows = []
    px = 10.0
    for i in range(n):
        o, c = px, px + 0.2
        fills = ["00AA00" if green_a else "CC0000"] + ["FFFFFF"] * 14
        rows.append({
            "date": 46000 + i, "open": o, "close": c,
            "high": c + 0.1, "low": o - 0.1, "volume": 1e6,
            "fills": fills,
        })
        px = c
    return annotate_days(rows)


def test_simulate_open_hold1_is_same_day_oc():
    days = _days()
    cluster = {"side": 1, "entry_idx": 3, "exit_idx": 8}
    r = simulate_clock(days, cluster, "open", "hold1")
    exp = (days[3]["close"] - days[3]["open"]) / days[3]["open"]
    assert abs(r - exp) < 1e-12


def test_simulate_close_hold1_is_next_close():
    days = _days()
    cluster = {"side": 1, "entry_idx": 3, "exit_idx": 8}
    r = simulate_clock(days, cluster, "close", "hold1")
    exp = (days[4]["close"] - days[3]["close"]) / days[3]["close"]
    assert abs(r - exp) < 1e-12


def test_costs_are_labeled_and_distinct():
    assert mcap_cost(100) == 0.003
    assert mcap_cost(1000) == 0.001
    assert futu_cost(1) == 0.0015
    assert futu_cost(-1) == 0.0020


def test_lottery_flags_one_huge_winner():
    vals = [0.01] * 40 + [8.0]
    bad, frac, trimmed = lottery(vals)
    assert bad is True
    assert frac > SHIP["max_trade_frac"]
    assert trimmed < 1.0


def test_hold1_without_hold2_is_fail():
    def fake(exit_rule, verdict):
        return {
            "def": "combo_A_green_ml1", "clock": "open", "side": "long",
            "exit": exit_rule, "cohort": "ALL", "cost_model": "futubull",
            "verdict": verdict, "fail_reasons": [],
        }
    rows = apply_hold1_sibling([fake("hold1", "PASS"), fake("hold2", "FAIL")])
    assert rows[0]["verdict"] == "FAIL"
    assert "hold1_without_hold2" in rows[0]["fail_reasons"]


def test_lag_combo_uses_prior_fill_only():
    days = _days(8, green_a=True)
    # paint yesterday H red, today A green
    days[2]["fams"][7] = "red"
    days[3]["fams"][0] = "green"
    pat = [p for p in new_lag_defs() if p["name"] == "lag_Hred_Agreen"][0]
    assert pat["clock"] == "open"
    cl = detect_pattern(days, pat)
    starts = [c["start"] for c in cl]
    assert 3 in starts
    assert 2 not in starts


def test_pattern_matrix_clocks_are_legal():
    for p in pattern_matrix():
        assert p["clock"] in ("open", "close")
        assert_clock_legal(p.get("feature_cols") or (), p["clock"])
        assert "name" in p


def test_inventory_full_vs_stored():
    from inventory_cols import FULL_COLS, STORED_FILL_COLS, build
    inv = build()
    assert inv["full_cols"] == FULL_COLS == 275
    assert inv["stored"]["fill_cols"] == STORED_FILL_COLS == 15
    assert inv["n_formulas"] > 30000
    assert inv["n_cf_columns"] > 15
    assert inv["all_cols_mode"]["used_in_daily"] is False
    assert inv["all_cols_mode"].get("phase") == 2
    assert inv["stored"]["missing_vs_full"]["formula_values_G_to_O"] is True
    assert inv["stored"]["signal_colors"].startswith("A-O")
    assert inv["stored"]["fill_letters"] == "A..O"
    assert inv["stored"]["done_grids_approx"] == 3445


def test_does_not_import_flatten_live():
    src = (ENG / "mine_clock.py").read_text(encoding="utf-8")
    assert "flatten_robust" in src  # labeled untouched
    assert "sleeve_merge_live" not in src
    assert "LIVE_POLICY" not in src


def test_pack_cell_thin_fails_ship_bar():
    cell = {
        "raw": [0.01] * 20, "tickers": set(f"T{i}" for i in range(10)),
        "dates": set(f"2026-01-{i:02d}" for i in range(1, 11)),
        "early": [0.01] * 10, "late": [0.01] * 10,
        "spy_up": [0.01] * 10, "spy_dn": [0.01] * 10,
        "disc": [0.01] * 20, "hold": [0.01] * 10,
    }
    row = pack_cell(
        ("toy", "open", "long", "hold2", "ALL", "futubull"), cell)
    assert row["verdict"] == "THIN"
    assert "thin_disc" in row["fail_reasons"]
    assert row["live_untouched"] == "flatten_robust"
    assert row["clock"] == "open"
    assert row["cost_model"] == "futubull"


def test_notes_and_scoreboard_exist():
    notes = (ENG / "NOTES.md").read_text(encoding="utf-8")
    assert "A, B, C, G, J, K, L, M, O" in notes
    assert "D, E, F, H, I, N" in notes


def test_all_cols_patterns_clocks_are_legal():
    from mine_all_cols import PATS, verdict
    for name, _side, clock in PATS:
        if name.startswith("lag_"):
            assert clock == "open", name
        else:
            assert clock == "close", name
    assert verdict(n=200, n_tickers=25, t=4.0, avg=0.01, early_s=0.01, late_s=0.01) == "THIN"
    assert verdict(n=300, n_tickers=50, t=3.2, avg=0.01, early_s=0.01, late_s=0.01) == "PASS"
    assert verdict(n=300, n_tickers=50, t=3.2, avg=-0.01, early_s=0.01, late_s=0.01) == "FAIL"


def test_all_cols_miners_do_not_wire_live():
    for fn in ("mine_all_cols.py", "capture_all_cols.py", "mine_clock.py",
               "mine_first.py"):
        src = (ENG / fn).read_text(encoding="utf-8")
        assert "sleeve_merge_live" not in src
        assert "LIVE_POLICY" not in src
        if fn.startswith("mine"):
            assert "flatten_robust" in src


def test_all_cols_sample_size_is_thin_by_design():
    """N=20–50 discovery cannot clear the 50-ticker ship bar."""
    from mine_all_cols import SHIP
    assert SHIP["n_tickers"] == 50


def test_all_cols_mine_report_is_committed():
    md = (ROOT / "excel_bot" / "research" / "ALL_COLS_MINE.md").read_text()
    assert "THIN 90" in md
    assert "flatten_robust" in md
    assert "| THIN |" in md
    payload = json.loads((ROOT / "excel_bot" / "research" / "all_cols_mine.json").read_text())
    assert payload["n_thin"] == 90
    assert payload["n_pass"] == 0
    assert payload["n_tickers"] >= 20
    assert payload["live_untouched"] == "flatten_robust"


def test_pack_cell_quality_fail_is_fail_not_thin():
    cell = {
        "raw": [-0.02] * 400, "tickers": set(f"T{i}" for i in range(60)),
        "dates": set(f"2026-01-{i:02d}" for i in range(1, 22)),
        "early": [-0.02] * 200, "late": [-0.02] * 200,
        "spy_up": [-0.02] * 200, "spy_dn": [-0.02] * 200,
        "disc": [-0.02] * 300, "hold": [-0.02] * 100,
    }
    row = pack_cell(
        ("toy", "close", "long", "hold2", "ALL", "mcap_bps"), cell)
    assert row["verdict"] == "FAIL"
    assert "disc_sign" in row["fail_reasons"]


def test_hold8_without_hold2_edge_is_fail():
    def fake(exit_rule, verdict, reasons=None, hold_avg=0.05):
        return {
            "def": "strict_A_ml1", "clock": "open", "side": "long",
            "exit": exit_rule, "cohort": "ALL", "cost_model": "futubull",
            "verdict": verdict, "fail_reasons": list(reasons or []),
            "holdout": {"n": 200, "avg_net": hold_avg, "t": 3.0},
        }
    rows = apply_horizon_sibling([
        fake("hold8", "PASS"),
        fake("hold2", "FAIL", ["no_edge_vs_uncond"], hold_avg=0.001),
    ])
    assert rows[0]["verdict"] == "FAIL"
    assert "long_hold_without_hold2" in rows[0]["fail_reasons"]


def test_first_mine_never_opens_core_score():
    from mine_first import A_DEFS, A_CLOSE, CLOSE_DEFS, first_mine_pats
    for name, defn, clock, _sides in A_DEFS:
        assert clock == "open"
        assert defn.get("key") == "a"
    for name, defn, clock, _sides in A_CLOSE:
        assert clock == "close"
        assert defn.get("key") == "a"
    for name, defn, clock, _sides in CLOSE_DEFS:
        assert clock == "close"
        if "core" in name:
            assert defn.get("key") == "core_score"
    pats = first_mine_pats()
    assert len(pats) >= 20
    for p in pats:
        if p["clock"] == "open":
            assert p["defn"].get("key") != "core_score", p["name"]
            cols = p["defn"].get("feature_cols")
            if cols:
                assert_clock_legal(cols, "open")


if __name__ == "__main__":
    tests = [v for k, v in list(globals().items()) if k.startswith("test_")]
    for fn in tests:
        fn()
        print("ok", fn.__name__)
    print(f"{len(tests)} excel_bot mine tests passed")
