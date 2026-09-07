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
    assert inv["all_cols_mode"].get("phase") == 1
    assert inv["all_cols_mode"].get("priority") == "whole_excel_sample"
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
        if name.startswith("lag_") or name in ("A_green", "A_red"):
            assert clock == "open", name
        else:
            assert clock == "close", name
        if "core_score" in name:
            assert clock == "close", name
    assert verdict(n=200, n_tickers=25, t=4.0, avg=0.01, early_s=0.01, late_s=0.01) == "THIN"
    assert verdict(n=300, n_tickers=50, t=3.2, avg=0.01, early_s=0.01, late_s=0.01) == "PASS"
    assert verdict(n=300, n_tickers=50, t=3.2, avg=-0.01, early_s=0.01, late_s=0.01) == "FAIL"


def test_clock_map_locks_measured_ao():
    from classify_clocks import build, feature_clock, MEAS_FILL_OPEN, MEAS_FILL_CLOSE
    inv = build()
    by = {r["col"]: r for r in inv["columns"]}
    for c in MEAS_FILL_OPEN:
        assert by[c]["fill"] == "open", c
        assert by[c]["fill_mine"] == "open", c
    for c in MEAS_FILL_CLOSE:
        assert by[c]["fill"] == "close", c
        assert by[c]["fill_mine"] == "close", c
    assert by["B"]["value_mine"] == "close"  # close price
    assert by["G"]["value_mine"] == "close"  # vol ratio
    assert feature_clock({"D": "close"}, {"D": "close"}, "D", "fill") == "close"
    assert feature_clock({"GU": "open"}, {"GU": "open"}, "GU", "fill") == "close"
    assert inv["unknown_mined_as"] == "close"
    assert inv["core_score_entry"] == "close"
    md = (ROOT / "excel_bot" / "research" / "CLOCK_MAP.md").read_text()
    assert "open / close / unknown" in md
    assert "D,E,F,H,I" in md


def test_all_cols_miners_do_not_wire_live():
    for fn in ("mine_all_cols.py", "capture_all_cols.py", "mine_clock.py",
               "mine_first.py", "mine_formula_cut.py", "classify_clocks.py",
               "harden_hyst_open.py", "mine_color_join.py", "pit_joins.py"):
        src = (ENG / fn).read_text(encoding="utf-8")
        assert "sleeve_merge_live" not in src
        assert "LIVE_POLICY" not in src
        if fn.startswith("mine") or fn.startswith("harden") or fn == "pit_joins.py":
            assert "flatten_robust" in src


def test_all_cols_sample_size_is_thin_by_design():
    """Ship ticker bar stays 50 — sample N is chosen to be able to clear it."""
    from clock import SHIP as CLOCK_SHIP
    from mine_all_cols import SHIP
    assert SHIP["n_tickers"] == CLOCK_SHIP["n_tickers"] == 50


def test_all_cols_mine_report_is_committed():
    md = (ROOT / "excel_bot" / "research" / "ALL_COLS_MINE.md").read_text()
    assert "flatten_robust" in md
    assert "whole Excel" in md or "A–JL" in md
    assert "core_score" in md
    payload = json.loads((ROOT / "excel_bot" / "research" / "all_cols_mine.json").read_text())
    assert payload["live_untouched"] == "flatten_robust"
    assert payload["n_tickers"] >= 50
    assert payload.get("surface") == "A-JL"
    assert payload.get("ao_is_not_whole_excel") is True
    assert payload.get("n_pass", 0) + payload.get("n_fail", 0) + payload.get("n_thin", 0) >= 1
    for r in payload.get("cells") or []:
        if "core_score" in r.get("def", ""):
            assert r["clock"] == "close"


def test_scoreboard_leads_with_first_cut():
    """Manager critical path: first PASS/FAIL/THIN with n, effect, tape."""
    for rel in ("03_scoreboard/EXCEL_BOT_MINE.md",
                "excel_bot/research/MINE_CYCLE.md"):
        md = (ROOT / rel).read_text()
        assert "first A–JL cut" in md
        assert "PASS 0" in md
        assert "tape early" in md and "tape late" in md
        assert "flatten_robust" in md


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


def test_formula_cut_report_is_committed():
    md = (ROOT / "excel_bot" / "research" / "FORMULA_CUT.md").read_text()
    assert "futubull" in md
    assert "flatten_robust" in md
    payload = json.loads((ROOT / "excel_bot" / "research" / "formula_cut.json").read_text())
    assert payload["live_untouched"] == "flatten_robust"
    assert payload["cost_model"] == "futubull"
    assert payload["core_score_entry"] == "close"
    assert payload["holds"] == [1, 2, 3]
    for r in payload.get("keepers") or []:
        if "core_score" in r["def"]:
            assert r["clock"] == "close"
        if r["clock"] == "open" and r["def"].endswith("_fill_green"):
            assert r["def"].split("_")[0] in list("ABCGJKLMO") or r["def"].startswith("IR")


def test_ao_first_mine_report_is_committed():
    md = (ROOT / "excel_bot" / "research" / "AO_FIRST_MINE.md").read_text()
    assert "VISIBLE_COLS A..O" in md
    assert "core_score` needs **CLOSE**" in md or "CLOSE" in md
    assert "FAIL" in md and "tol2_core_score_ml3" in md
    assert "flatten_robust" in md
    payload = json.loads((ROOT / "excel_bot" / "research" / "ao_first_mine.json").read_text())
    assert payload["n_grids"] >= 3000
    assert payload["n_pass"] >= 0
    assert payload["live_untouched"] == "flatten_robust"
    assert payload.get("n_demoted_long_hold", 0) >= 0
    for r in payload.get("focus") or []:
        assert r["verdict"] != "PASS"
    for r in payload.get("primary") or []:
        assert r["cohort"] == "ALL"
        assert r["exit"] in ("hold1", "hold2")
        assert r["clock"] == "open"
        assert "core_score" not in r["def"]


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


def test_load_spy_tape_accepts_list_json():
    import tempfile
    from mine_clock import load_spy_tape
    import mine_clock as mc
    with tempfile.TemporaryDirectory() as td:
        tape = Path(td) / "spy_tape.json"
        tape.write_text(json.dumps([
            {"date": "2026-01-02", "close": 100.0},
            {"date": "2026-01-05", "close": 101.0},
            {"date": "2026-01-06", "close": 99.0},
        ]), encoding="utf-8")
        old_rows, old_grids, old_research = mc.ROWS_DIR, mc.GRIDS_DIR, mc.RESEARCH
        mc.ROWS_DIR = str(Path(td) / "no_rows")
        mc.GRIDS_DIR = str(Path(td) / "no_grids")
        mc.RESEARCH = td
        try:
            out = load_spy_tape()
        finally:
            mc.ROWS_DIR, mc.GRIDS_DIR, mc.RESEARCH = old_rows, old_grids, old_research
    assert out["2026-01-05"] == 1
    assert out["2026-01-06"] == -1


def test_lottery_day_flags_one_huge_day():
    from harden_hyst_open import lottery_day
    trades = [{"date": f"2026-01-{(i % 28) + 1:02d}", "net": 0.01}
              for i in range(40)]
    trades.append({"date": "2026-03-04", "net": 8.0})
    bad, frac, top_d, top_v, n_days = lottery_day(trades)
    assert bad is True
    assert top_d == "2026-03-04"
    assert frac > 0.25
    assert n_days >= 2
    assert top_v == 8.0


def test_lottery_day_ok_when_spread_across_days():
    from harden_hyst_open import lottery_day
    trades = []
    for i in range(1, 21):
        trades.append({"date": f"2026-01-{i:02d}", "net": 0.02})
        trades.append({"date": f"2026-01-{i:02d}", "net": 0.01})
    bad, frac, _d, _v, n_days = lottery_day(trades)
    assert bad is False
    assert frac < 0.25
    assert n_days == 20


def test_harden_candidates_are_the_six():
    from harden_hyst_open import CANDIDATE_KEYS, CANDIDATE_NAMES, candidate_pats
    assert len(CANDIDATE_KEYS) == 6
    assert CANDIDATE_NAMES == (
        "hyst_open_core_e5_x0",
        "hyst_open_core_e5_x2",
        "hyst_open_score_e5_x2",
    )
    holds = sorted({h for _n, h in CANDIDATE_KEYS})
    assert holds == ["hold1", "hold2"]
    pats = candidate_pats()
    assert {p["name"] for p in pats} == set(CANDIDATE_NAMES)
    for p in pats:
        assert p["clock"] == "open"
        assert p["kind"] == "hyst"
        assert p["key"] in ("open_core", "open_score")
        assert p["enter"] == 5


def _toy_trades(n=400, half="late", tape=1, split="holdout", net=0.015):
    out = []
    for i in range(n):
        month = "03" if half == "early" else "07"
        day = (i % 28) + 1
        out.append({
            "def": "hyst_open_core_e5_x2",
            "hold": 1,
            "ticker": f"T{i % 80}",
            "date": f"2026-{month}-{day:02d}",
            "split": split,
            "half": half,
            "tape": tape,
            "raw": net + 0.0015,
            "net": net,
        })
    return out


def test_harden_score_cell_kills_late_red():
    from harden_hyst_open import score_cell
    base = {"n": 1000, "avg_net": -0.0007, "t": -1.7, "win": 0.45}
    trades = (_toy_trades(400, "early", 1, "discovery", 0.02)
              + _toy_trades(400, "early", -1, "discovery", 0.02)
              + _toy_trades(200, "late", 1, "holdout", -0.01)
              + _toy_trades(200, "late", -1, "holdout", -0.01))
    row = score_cell("hyst_open_core_e5_x2", 1, trades, base)
    assert row["verdict"] == "KILL"
    assert "tape_split" in row["fail_reasons"] or "hold_sign" in row["fail_reasons"]
    assert row["cost_model"] == "futubull"
    assert row["live_untouched"] == "flatten_robust"


def test_harden_score_cell_kills_spy_down():
    from harden_hyst_open import score_cell
    base = {"n": 1000, "avg_net": -0.0007, "t": -1.7, "win": 0.45}
    trades = (_toy_trades(300, "early", 1, "discovery", 0.02)
              + _toy_trades(300, "early", -1, "discovery", -0.02)
              + _toy_trades(200, "late", 1, "holdout", 0.02)
              + _toy_trades(200, "late", -1, "holdout", -0.02))
    row = score_cell("hyst_open_core_e5_x2", 1, trades, base)
    assert row["verdict"] == "KILL"
    assert "spy_regime" in row["fail_reasons"]


def test_harden_score_cell_keeps_balanced_book():
    from harden_hyst_open import apply_hold1_keep, score_cell
    base = {"n": 1000, "avg_net": -0.0007, "t": -1.7, "win": 0.45}
    def book(hold):
        return (
            _toy_trades(300, "early", 1, "discovery", 0.018)
            + _toy_trades(300, "early", -1, "discovery", 0.016)
            + _toy_trades(200, "late", 1, "holdout", 0.017)
            + _toy_trades(200, "late", -1, "holdout", 0.015)
        )
    r1 = score_cell("hyst_open_core_e5_x2", 1, book(1), base)
    r2 = score_cell("hyst_open_core_e5_x2", 2, book(2),
                    {"n": 1000, "avg_net": 0.0041, "t": 4.6, "win": 0.46})
    apply_hold1_keep([r1, r2])
    assert r1["verdict"] == "KEEP"
    assert r2["verdict"] == "KEEP"
    assert r1["lottery_day_frac"] < 0.25


def test_harden_hold1_without_hold2_is_kill():
    from harden_hyst_open import apply_hold1_keep
    rows = [
        {"def": "hyst_open_core_e5_x2", "exit": "hold1",
         "verdict": "KEEP", "fail_reasons": []},
        {"def": "hyst_open_core_e5_x2", "exit": "hold2",
         "verdict": "KILL", "fail_reasons": ["tape_split"]},
    ]
    apply_hold1_keep(rows)
    assert rows[0]["verdict"] == "KILL"
    assert "hold1_without_hold2" in rows[0]["fail_reasons"]


def test_hyst_open_core_fires_and_hold1_is_same_day():
    from harden_hyst_open import candidate_pats
    from clock import COST_FUTU_LONG
    days = _days(16)
    # Paint A,B,C,G,J deep green on days 4-6 so open_core = +10.
    for i in (4, 5, 6):
        fills = list(days[i]["fills"])
        for col in (0, 1, 2, 6, 9):
            fills[col] = "00AA00"
        days[i]["fills"] = fills
    days = annotate_days(days)
    pat = [p for p in candidate_pats() if p["name"] == "hyst_open_core_e5_x2"][0]
    cl = detect_pattern(days, pat)
    longs = [c for c in cl if c["side"] == 1]
    assert longs
    c = longs[0]
    assert days[c["entry_idx"]]["open_core"] >= 5
    raw = simulate_clock(days, c, "open", "hold1")
    ei = c["entry_idx"]
    exp = (days[ei]["close"] - days[ei]["open"]) / days[ei]["open"]
    assert abs(raw - exp) < 1e-12
    net = raw - COST_FUTU_LONG
    assert net == raw - 0.0015


def test_harden_plain_english_before_code_names():
    from harden_hyst_open import PLAIN, render_plain
    md = render_plain([], 3603)
    assert md.index("What the cell means") < md.index("`hyst_") if "`hyst_" in md else True
    assert "buy at that open" in next(iter(PLAIN.values())) or "9:30" in next(iter(PLAIN.values()))
    assert "Futubull" in md
    assert "same day's close" in md
    assert "SPY-up" in md and "SPY-down" in md
    assert "fattest" in md.lower() or "25%" in md
    assert "flatten_robust" in md


def test_harden_report_is_committed():
    md = (ROOT / "excel_bot" / "research" / "AO_FIRST_MINE.md").read_text()
    assert "VISIBLE_COLS A..O" in md
    harden = md.split("## Harden: morning hysteresis light", 1)[1]
    assert "What the cell means" in harden
    assert harden.index("What the cell means") < harden.index("`hyst_open_core_e5_x2`")
    assert "same day's close" in md
    assert "SPY-up" in md and "SPY-down" in md
    assert "KEEP 6" in md or "**KEEP**" in md
    sb = (ROOT / "03_scoreboard" / "EXCEL_BOT_MINE.md").read_text()
    assert "first A–JL cut" in sb
    assert "Harden: morning hysteresis light" in sb
    assert "flatten_robust" in sb
    payload = json.loads(
        (ROOT / "excel_bot" / "research" / "hyst_open_harden.json").read_text())
    assert payload["live_untouched"] == "flatten_robust"
    assert payload["cost_model"] == "futubull"
    assert payload["n_keep"] + payload["n_kill"] == 6
    assert payload["grids"] >= 3000
    names = {(r["def"], r["exit"]) for r in payload["candidates"]}
    assert names == {
        ("hyst_open_core_e5_x2", "hold1"),
        ("hyst_open_score_e5_x2", "hold1"),
        ("hyst_open_core_e5_x0", "hold1"),
        ("hyst_open_core_e5_x2", "hold2"),
        ("hyst_open_core_e5_x0", "hold2"),
        ("hyst_open_score_e5_x2", "hold2"),
    }
    for r in payload["candidates"]:
        assert r["clock"] == "open"
        assert r["cost_model"] == "futubull"
        assert r["early"] and r["late"]
        assert r["spy_up"] and r["spy_dn"]
        assert r["lottery_day_frac"] <= 0.25


def test_harden_does_not_import_flatten_live():
    src = (ENG / "harden_hyst_open.py").read_text(encoding="utf-8")
    assert "import flatten" not in src
    assert "sleeve_merge_live" not in src
    assert "LIVE_POLICY" not in src
    assert "flatten_robust" in src


def test_harden_splice_keeps_first_cut(tmp_path=None):
    import tempfile
    from harden_hyst_open import splice_md
    with tempfile.TemporaryDirectory() as td:
        p = Path(td) / "board.md"
        p.write_text("# Excel emulator mine — first A–JL cut\n\n"
                     "PASS 0 · tape early · tape late\n", encoding="utf-8")
        out = splice_md(str(p), "## Harden: morning hysteresis light",
                        "## Harden: morning hysteresis light\n\nKILL 6.\n",
                        require="first A–JL cut")
        assert out.startswith("# Excel emulator mine — first A–JL cut")
        assert "PASS 0" in out and "tape early" in out
        assert out.count("## Harden: morning hysteresis light") == 1
        p.write_text(out, encoding="utf-8")
        out2 = splice_md(str(p), "## Harden: morning hysteresis light",
                         "## Harden: morning hysteresis light\n\nKEEP 1.\n",
                         require="first A–JL cut")
        assert "KILL 6" not in out2
        assert "KEEP 1" in out2
        assert "first A–JL cut" in out2
        try:
            splice_md(str(p), "## Harden: morning hysteresis light",
                      "## Harden: morning hysteresis light\n\nx\n",
                      require="VISIBLE_COLS A..O")
            raise AssertionError("should have refused")
        except ValueError as e:
            assert "required lead-in" in str(e)


def test_pit_joins_use_prior_date_only():
    from pit_joins import prior_lookup
    ab = {"2026-09-03": {"AAA": "good"}, "2026-09-04": {"AAA": "bad"}}
    asof, tone = prior_lookup(ab, "2026-09-04", "AAA")
    assert asof == "2026-09-03" and tone == "good"
    asof, tone = prior_lookup(ab, "2026-09-03", "AAA")
    assert asof is None and tone is None
    asof, tone = prior_lookup(ab, "2026-09-05", "AAA")
    assert asof == "2026-09-04" and tone == "bad"
    book = {"2026-09-03": {"HOOD", "AAPL"}}
    asof, hit = prior_lookup(book, "2026-09-04", "hood")
    assert asof == "2026-09-03" and hit is True
    asof, hit = prior_lookup(book, "2026-09-04", "ZZZ")
    assert hit is False


def test_color_mine_open_letters_only():
    from clock import CLOSE_LETTERS, OPEN_LETTERS
    from mine_color_join import OPEN_IDX
    assert set(OPEN_IDX) == set(OPEN_LETTERS)
    assert set(OPEN_IDX).isdisjoint(set(CLOSE_LETTERS))
    src = (ENG / "mine_color_join.py").read_text(encoding="utf-8")
    assert "CLOSE_LETTERS" in src
    assert "never enter open" in src or "never_open" in src or "Never" in src or "never start" in src


def test_color_plain_english_before_code():
    from mine_color_join import color_plain, fold_plain, render
    text = color_plain("A", "green", 1)
    assert "highlighted green" in text
    assert "same day's close" in text
    assert "hyst_" not in text
    fold = fold_plain("hyst_open_core_e5_x2", "yesterday's AB tape already liked the name")
    assert "yesterday's AB" in fold
    md = render([], 3603, {
        "ab": {"n_dates": 2, "first": "2026-08-18", "last": "2026-09-06"},
        "book": {"n_dates": 2, "first": "2026-08-18", "last": "2026-09-06"},
        "wx": {"n_dates": 2, "first": "2026-08-12", "last": "2026-09-06"},
    })
    assert md.index("What a color means") < md.index("`color_") if "`color_" in md else True
    assert "A, B, C, G, J, K, L, M, O" in md
    assert "D, E, F, H, I, N" in md
    assert "dated before" in md
    assert "flatten_robust" in md


def test_color_join_does_not_import_live():
    for fn in ("mine_color_join.py", "pit_joins.py"):
        src = (ENG / fn).read_text(encoding="utf-8")
        assert "from flatten" not in src
        assert "sleeve_merge_live" not in src
        assert "LIVE_POLICY" not in src
        assert "flatten_robust" in src


if __name__ == "__main__":
    tests = [v for k, v in list(globals().items()) if k.startswith("test_")]
    for fn in tests:
        fn()
        print("ok", fn.__name__)
    print(f"{len(tests)} excel_bot mine tests passed")
