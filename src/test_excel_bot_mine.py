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
    assert row["def"] == "toy"
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
    assert "Same-row open-knowable formulas" in md
    assert "Fair inputs at the 9:30 open" in md
    assert inv.get("same_row_open")
    assert "AA" not in inv["same_row_open"]["letters"]
    assert by["AA"]["value_mine"] == "close"
    assert by["O"]["value_mine"] == "close"


def test_all_cols_miners_do_not_wire_live():
    for fn in ("mine_all_cols.py", "capture_all_cols.py", "mine_clock.py",
               "mine_first.py", "mine_formula_cut.py", "classify_clocks.py",
               "harden_hyst_open.py", "mine_color_join.py", "pit_joins.py",
               "mine_unmined.py", "harden_unmined.py", "harden_open_stack.py",
               "harden_close_cluster.py", "harden_close_peers.py",
               "mine_next_region.py", "mine_same_day.py", "mine_pair_lag.py",
               "mine_shade_open.py"):
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


def test_scoreboard_leads_with_ao_clock_mine():
    """Standing cycle: A–O mine_clock keepers + scaled all-cols, n/effect/tape."""
    for rel in ("03_scoreboard/EXCEL_BOT_MINE.md",
                "excel_bot/research/MINE_CYCLE.md"):
        md = (ROOT / rel).read_text()
        assert "A–O" in md
        assert "PASS 376" in md
        assert "FAIL 1160" in md
        assert "THIN 18" in md
        assert "tape early" in md and "tape late" in md
        assert "flatten_robust" in md
        assert "499" in md
        assert "PASS 0" in md  # scaled all-cols null
    payload = json.loads(
        (ROOT / "excel_bot" / "research" / "mine_clock_summary.json").read_text())
    assert payload["live_untouched"] == "flatten_robust"
    assert payload["grids"] >= 3000
    assert payload["n_pass"] >= 1
    for r in payload.get("keepers_hold12_futubull") or []:
        assert r["verdict"] == "PASS"
        assert r["exit"] in ("hold1", "hold2")
        assert r["cohort"] == "ALL"


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


def test_parent_of_matches_same_hold():
    from mine_color_join import parent_of, rejudge
    assert parent_of("hyst_open_core_e5_x2__O_green", 2) == (
        "hyst_open_core_e5_x2", "hold2")
    assert parent_of("hyst_open_core_e5_x2__O_green", 1) == (
        "hyst_open_core_e5_x2", "hold1")
    rows = [{
        "def": "hyst_open_core_e5_x2__A_green", "hold": 2, "exit": "hold2",
        "holdout": {"n": 100, "avg_net": 0.0162, "t": 5},
        "verdict": "KEEP", "fail_reasons": [],
    }]
    parents = {("hyst_open_core_e5_x2", "hold2"):
               {"n": 100, "avg_net": 0.0162, "t": 5}}
    rejudge(rows, parents)
    assert rows[0]["verdict"] == "KILL"
    assert "no_edge_vs_parent" in rows[0]["fail_reasons"]


def test_color_join_report_is_committed():
    md = (ROOT / "excel_bot" / "research" / "COLOR_JOIN_MINE.md").read_text()
    assert "What a color means" in md
    assert "A, B, C, G, J, K, L, M, O" in md
    assert "D, E, F, H, I, N" in md
    assert "dated before" in md
    assert "flatten_robust" in md
    assert "BLOCKED" in md
    assert "2026-07-01" in md
    assert "does Q3 still pay" in md or "Q3" in md
    sb = (ROOT / "03_scoreboard" / "EXCEL_BOT_MINE.md").read_text()
    assert "first A–JL cut" in sb
    assert "Color + join mine" in sb
    assert "Join verdict" in sb
    assert "BLOCKED" in sb
    assert "2026-07-01" in sb
    ao = (ROOT / "excel_bot" / "research" / "AO_FIRST_MINE.md").read_text()
    assert "VISIBLE_COLS A..O" in ao
    assert "Color + join mine" in ao
    payload = json.loads(
        (ROOT / "excel_bot" / "research" / "color_join_mine.json").read_text())
    assert payload["live_untouched"] == "flatten_robust"
    assert payload["cost_model"] == "futubull"
    assert payload["open_letters"] == "ABCGJKLMO"
    assert payload["n_keep"] + payload["n_thin"] + payload["n_kill"] >= 1
    for r in payload.get("cells") or []:
        if r["def"].startswith("color_"):
            letter = r["def"].split("_")[1]
            assert letter in "ABCGJKLMO"


def test_color_join_does_not_import_live():
    for fn in ("mine_color_join.py", "pit_joins.py"):
        src = (ENG / fn).read_text(encoding="utf-8")
        assert "from flatten" not in src
        assert "sleeve_merge_live" not in src
        assert "LIVE_POLICY" not in src
        assert "flatten_robust" in src


def test_seed_anchor_drops_bars_after_anchor():
    from datetime import date
    from backtest import stockhistory_from_rows
    rows = [
        {"date": date(2026, 1, 2), "open": 1, "high": 1, "low": 1,
         "close": 1, "volume": 10},
        {"date": date(2026, 1, 5), "open": 2, "high": 2, "low": 2,
         "close": 2, "volume": 20},
        {"date": date(2026, 1, 6), "open": 99, "high": 99, "low": 99,
         "close": 99, "volume": 99},
    ]
    grid = stockhistory_from_rows(rows, date(2026, 1, 1), date(2026, 1, 5), 0)
    dates = [r[0] for r in grid[1:]]
    from stockhistory import serial
    assert serial(date(2026, 1, 5)) in dates
    assert serial(date(2026, 1, 6)) not in dates
    assert serial(date(2026, 1, 2)) in dates


def test_a_f_are_stockhistory_aliases_not_cached_literals():
    from audit_af_seed import inspect_model
    meta = inspect_model()
    assert meta["ir1_is_stockhistory"]
    assert meta["a_f_are_formulas"]
    assert meta["aliases"]["A"]["row1"] == "=IR1"
    assert meta["aliases"]["C"]["row1"] == "=IT1"
    assert meta["aliases"]["F"]["row1"] == "=IW1"
    assert "P1" in meta["ir1"]  # STOCKHISTORY end = TODAY()


def test_harden_loaders_refuse_excel_cached_af():
    from audit_af_seed import loader_src_ok, load_mine_grid, is_rows_cache_grid
    assert loader_src_ok() == []
    assert is_rows_cache_grid({"source": "rows_cache", "days": []})
    assert load_mine_grid is not None
    for fn in ("harden_hyst_open.py", "mine_color_join.py", "rebuild_grids.py"):
        src = (ENG / fn).read_text(encoding="utf-8")
        assert "build_seeds" not in src
        assert "from_cache" not in src
        assert "--from-cache" not in src
    harden = (ENG / "harden_hyst_open.py").read_text(encoding="utf-8")
    color = (ENG / "mine_color_join.py").read_text(encoding="utf-8")
    assert "load_mine_grid" in harden and "load_mine_grid" in color


def test_harden_color_is_color_only_no_joins():
    from harden_color import is_color_fold, recipe_rows, load_folds, OPEN_LETTERS
    assert OPEN_LETTERS == "ABCGJKLMO"
    assert is_color_fold({"family": "hyst_color", "def": "hyst_open_core_e5_x2__O_green"})
    assert not is_color_fold({"family": "join", "def": "hyst_open_core_e5_x2__fz_volM"})
    assert not is_color_fold({"def": "hyst_open_core_e5_x2__ab_good"})
    src = (ENG / "harden_color.py").read_text(encoding="utf-8")
    assert "from flatten" not in src
    assert "flatten_robust" in src
    assert "Finviz" in src  # named only to say it is out of scope
    raw, parents, folds = load_folds()
    assert folds
    assert all("fz_" not in c["def"] and "ab_" not in c["def"] for c in folds)
    recipes = recipe_rows(folds, parents)
    assert len(recipes) == 6
    keeps = [r for r in recipes if r["verdict"] == "KEEP"]
    assert keeps
    assert all(r["best_letter"] == "O" for r in keeps)


def test_color_harden_report_is_committed():
    md = (ROOT / "excel_bot" / "research" / "COLOR_HARDEN.md").read_text()
    assert "light + open fill vs light alone" in md
    assert "What a color means" not in md or "Question" in md
    assert md.index("Question") < md.index("`hyst_")
    assert "green O" in md or "O" in md
    assert "flatten_robust" in md
    assert "no Finviz" in md or "out of scope" in md
    sb = (ROOT / "03_scoreboard" / "EXCEL_BOT_MINE.md").read_text()
    assert "Color harden" in sb
    assert "A–O clock cycle" in sb or "first A–JL cut" in sb
    payload = json.loads(
        (ROOT / "excel_bot" / "research" / "color_harden.json").read_text())
    assert payload["live_untouched"] == "flatten_robust"
    assert payload["excel_cache_used"] is False
    assert payload["n_keep_recipes"] + payload["n_kill_recipes"] == 6
    assert payload["open_letters"] == "ABCGJKLMO"
    for r in payload["recipes"]:
        if r["verdict"] == "KEEP":
            assert r["best_vs_parent_pp"] >= 0.20
            assert r["best_letter"] == "O"


def test_finviz_asof_is_prior_date_only():
    from pit_joins import load_finviz_asof_highvol, prior_lookup
    by = load_finviz_asof_highvol()
    assert by, "dated Elite exports should exist in data/exports"
    assert "latest" not in by
    dates = sorted(by)
    # same-day file is not knowable at 9:30
    asof, _hit = prior_lookup(by, dates[0], "AAPL")
    assert asof is None
    if len(dates) >= 2:
        asof, _hit = prior_lookup(by, dates[1], "ZZZZNOPE")
        assert asof == dates[0]


def test_join_verdict_script_no_live():
    src = (ENG / "harden_joins.py").read_text(encoding="utf-8")
    assert "from flatten" not in src
    assert "flatten_robust" in src
    assert "REGIME_CUT" in src
    assert "2026-07-01" in src


def test_join_verdict_report_is_committed():
    md = (ROOT / "excel_bot" / "research" / "JOIN_VERDICT.md").read_text()
    assert "Incremental layers" in md
    assert md.index("### Incremental layers") < md.index("### Second regime")
    assert "BLOCKED" in md
    assert "Q3" in md
    assert "AB" in md and "weather" in md
    assert "flatten_robust" in md
    sb = (ROOT / "03_scoreboard" / "EXCEL_BOT_MINE.md").read_text()
    assert "Join verdict" in sb
    cj = (ROOT / "excel_bot" / "research" / "COLOR_JOIN_MINE.md").read_text()
    assert "BLOCKED" in cj
    assert "Q3" in cj or "2026-07-01" in cj
    assert "cannot ship" in cj or "as-of" in cj.lower()
    payload = json.loads(
        (ROOT / "excel_bot" / "research" / "join_verdict.json").read_text())
    assert payload["live_untouched"] == "flatten_robust"
    assert payload["regime_cut"] == "2026-07-01"
    assert payload["excel_cache_used"] is False
    o_keeps = [r for r in payload["scored"]
               if r["def"].endswith("__O_green") and "fz_" not in r["def"]
               and r["verdict"] == "KEEP"]
    assert o_keeps, "light+O should still KEEP on at least one recipe after Q3"
    for r in payload["scored"]:
        if "fz_volM_asof" in r["def"]:
            assert r["verdict"] != "KEEP"


def test_af_seed_audit_report_is_committed():
    md = (ROOT / "excel_bot" / "research" / "AF_SEED_AUDIT.md").read_text()
    assert "STOCKHISTORY" in md
    assert "Yahoo" in md or "rows" in md
    assert "flatten_robust" in md
    assert "tile" in md.lower() or "TODAY" in md
    assert "Excel-cache" in md or "cached" in md
    sb = (ROOT / "03_scoreboard" / "EXCEL_BOT_MINE.md").read_text()
    assert "A–F seed" in sb
    assert "first A–JL cut" in sb or "A–O clock cycle" in sb
    ao = (ROOT / "excel_bot" / "research" / "AO_FIRST_MINE.md").read_text()
    assert "VISIBLE_COLS A..O" in ao
    assert "A–F seed" in ao
    payload = json.loads(
        (ROOT / "excel_bot" / "research" / "af_seed_audit.json").read_text())
    assert payload["live_untouched"] == "flatten_robust"
    assert payload["excel_cache_used_by_harden"] is False
    assert payload["grids_source"] == "rows_cache"
    assert payload["tile_today_is_anchor"] is True
    assert payload["corrupt_future_open_fills_changed"] is False


def test_unmined_inventory_is_committed():
    md = (ROOT / "excel_bot" / "research" / "UNMINED_INVENTORY.md").read_text()
    assert "Plain English" in md
    assert "Yahoo" in md or "rows" in md
    assert "flatten_robust" in md
    assert "BLOCKED" in md or "Finviz" in md
    payload = json.loads(
        (ROOT / "excel_bot" / "research" / "unmined_inventory.json").read_text())
    assert payload["live_untouched"] == "flatten_robust"
    assert payload["excel_cache_used"] is False
    fams = payload["families"]
    assert fams["val_open"]["letters"]
    assert fams["val_close"]["letters"]
    assert fams["fill_new"]["letters"]
    assert "core_score" in fams["score_killed"]["letters"]
    # leftover letters are past the first all-cols hand list
    from mine_all_cols import FILL_CLOSE, VALUE_CLOSE
    old_val = {row[1] for row in VALUE_CLOSE}
    assert not set(fams["val_close"]["letters"]) & old_val


def test_unmined_specs_clocks_are_legal():
    from mine_unmined import build_specs, inventory, load_clocks
    clocks = load_clocks()
    specs = build_specs(inventory(), clocks)
    assert len(specs) >= 100
    for name, clock, _side, kind, col, _extra in specs:
        if clock == "open":
            if kind == "fill":
                assert col in clocks["fill_open"], name
            else:
                assert col in clocks["value_open"], name
        if "core_score" in name:
            assert clock == "close", name
        assert not name.startswith("valopen_") or clock == "open", name


def test_unmined_sweep_report_is_committed():
    md = (ROOT / "excel_bot" / "research" / "UNMINED_SWEEP.md").read_text()
    assert "Plain English" in md
    assert md.index("Plain English") < md.index("`val") if "`val" in md else True
    assert "KEEP" in md and "KILL" in md and "THIN" in md
    assert "2026-07-01" in md or "Q3" in md
    assert "flatten_robust" in md
    assert "Yahoo" in md or "rows" in md
    sb = (ROOT / "03_scoreboard" / "EXCEL_BOT_MINE.md").read_text()
    assert "PASS 376" in sb
    assert "FAIL 1160" in sb
    assert "Remaining A–JL families" in sb
    assert "first A–JL cut" in sb or "A–O clock cycle" in sb
    payload = json.loads(
        (ROOT / "excel_bot" / "research" / "unmined_sweep.json").read_text())
    assert payload["live_untouched"] == "flatten_robust"
    assert payload["excel_cache_used"] is False
    assert payload["n_tickers"] >= 3000
    assert payload.get("scale") == "full_rows_cache"
    assert payload["finviz"] == "BLOCKED"
    assert payload["n_keep"] + payload["n_kill"] + payload["n_thin"] >= 1
    if payload["n_keep"]:
        assert payload.get("n_keep_unique", 0) >= 1
        assert payload.get("keep_letters")
    for r in payload.get("keepers") or []:
        assert r["clock"] in ("open", "close")
        if r["clock"] == "open":
            assert "core_score" not in r["def"]
        assert r.get("q3") and r["q3"].get("n", 0) >= 40
        assert (r.get("lottery_day_frac") or 0) <= 0.25
    for r in payload.get("cells") or []:
        if r.get("def") == "fill_DE_red":
            assert r.get("keep") != "KEEP"
            assert "q3_missing" in (r.get("fail_reasons") or []) or r.get("keep") == "KILL"


def test_unmined_harden_collapses_twins():
    from harden_unmined import FEATURED, collapse, rejudge
    twins = [
        {"def": "valclose_BA_eq1", "family": "val_close", "clock": "close",
         "side": "long", "exit": "hold2",
         "discovery": {"n": 1000, "avg_net": 0.02, "t": 5},
         "holdout": {"n": 400, "avg_net": 0.02, "t": 4},
         "keep": "KEEP"},
        {"def": "valclose_BA_ge1", "family": "val_close", "clock": "close",
         "side": "long", "exit": "hold2",
         "discovery": {"n": 1000, "avg_net": 0.02, "t": 5},
         "holdout": {"n": 400, "avg_net": 0.02, "t": 4},
         "keep": "KEEP"},
        {"def": "valclose_BA_gt0", "family": "val_close", "clock": "close",
         "side": "long", "exit": "hold2",
         "discovery": {"n": 1000, "avg_net": 0.02, "t": 4.5},
         "holdout": {"n": 400, "avg_net": 0.019, "t": 3.5},
         "keep": "KEEP"},
    ]
    surv, killed = collapse(twins)
    assert len(surv) == 1
    assert surv[0]["def"] == "valclose_BA_eq1"
    assert len(killed) == 2
    assert all("twin" in k["fail_reasons"][0] for k in killed)
    bad = {
        "discovery": {"n": 400, "avg_net": 0.02, "t": 5},
        "holdout": {"n": 200, "avg_net": 0.02, "t": 4},
        "early": {"n": 300, "avg_net": 0.02}, "late": {"n": 300, "avg_net": 0.02},
        "spy_up": {"n": 200, "avg_net": 0.02}, "spy_dn": {"n": 200, "avg_net": 0.02},
        "q12": {"n": 300, "avg_net": 0.02}, "q3": {"n": 200, "avg_net": 0.02},
        "baseline": {"n": 1000, "avg_net": 0.001},
        "lottery_day_frac": 0.1, "n_tickers": 100, "n_dates": 40,
    }
    assert rejudge(bad)[0] == "KEEP"
    late_red = dict(bad, late={"n": 300, "avg_net": -0.01})
    assert rejudge(late_red)[0] == "KILL"
    assert FEATURED == ("T", "BA", "AH", "CZ", "EH", "IB")


def test_unmined_harden_report_is_committed():
    md = (ROOT / "excel_bot" / "research" / "UNMINED_HARDEN.md").read_text()
    assert "Plain English" in md
    assert md.index("Plain English") < md.index("`val") if "`val" in md else True
    assert "Killed twins" in md
    assert "Shortboard" in md
    assert "light + green O" in md or "light+green O" in md
    assert "flatten_robust" in md
    assert "2026-07-01" in md or "Q3" in md
    sb = (ROOT / "03_scoreboard" / "EXCEL_BOT_MINE.md").read_text()
    assert "PASS 376" in sb
    assert "Leftover KEEP harden" in sb
    assert "Color harden" in sb or "light + green O" in sb or "green O" in sb
    payload = json.loads(
        (ROOT / "excel_bot" / "research" / "unmined_harden.json").read_text())
    assert payload["live_untouched"] == "flatten_robust"
    assert payload["excel_cache_used"] is False
    assert payload["finviz"] == "BLOCKED"
    assert payload["n_tickers"] >= 3000
    assert payload["n_twins_killed"] >= 1
    letters = payload["keep_letters"]
    assert len(letters) == len(set(letters))
    seen = set()
    for r in payload["survivors"]:
        if r["keep"] != "KEEP":
            continue
        key = (r["letter"], r["side"], r["exit"])
        assert key not in seen
        seen.add(key)
        assert r["exit"] in ("hold1", "hold2")
        assert (r.get("lottery_day_frac") or 0) <= 0.25
        assert r.get("q3") and r["q3"].get("n", 0) >= 40


def test_open_stack_collapses_to_standing_recipes():
    from harden_open_stack import STANDING, STACKS, _word, ge1
    assert STANDING == (
        ("hyst_open_core_e5_x2", 1),
        ("hyst_open_core_e5_x2", 2),
        ("hyst_open_score_e5_x2", 2),
    )
    assert [s[0] for s in STACKS] == ["AH", "FR"]
    assert ge1(1) and ge1(2.0) and not ge1(0) and not ge1(None)
    assert _word(0.24) == "stronger"
    assert _word(0.04) == "no better"
    assert _word(-0.30) == "weaker"


def test_open_stack_report_is_committed():
    md = (ROOT / "excel_bot" / "research" / "OPEN_STACK.md").read_text()
    assert "Plain English" in md
    assert md.index("Plain English") < md.index("`hyst_")
    assert "light+O" in md or "light + green O" in md
    assert "AH" in md and "FR" in md
    assert "KEEP" in md and ("KILL" in md or "THIN" in md)
    assert "2026-07-01" in md or "Q3" in md
    assert "flatten_robust" in md
    assert "T, BA" in md or "out of scope" in md
    assert "light + green O" in md or "light+green O" in md
    sb = (ROOT / "03_scoreboard" / "EXCEL_BOT_MINE.md").read_text()
    assert "PASS 376" in sb
    assert "Open-stack verdict" in sb
    assert "Color harden" in sb or "green O" in sb
    payload = json.loads(
        (ROOT / "excel_bot" / "research" / "open_stack.json").read_text())
    assert payload["live_untouched"] == "flatten_robust"
    assert payload["excel_cache_used"] is False
    assert payload["finviz"] == "BLOCKED"
    assert payload["standing_ao_keeps"].startswith("light+green O")
    assert payload["q3_cut"] == "2026-07-01"
    letters = {r.get("letter") for r in payload["rows"]
               if str(r.get("layer", "")).startswith("light+O ∧")}
    assert letters <= {"AH", "FR"}
    for r in payload["rows"]:
        if r.get("layer") == "light+O":
            assert r["verdict"] in ("KEEP", "KILL", "THIN")
        if r.get("layer", "").startswith("light+O ∧") and r["verdict"] == "KEEP":
            assert (r.get("vs_parent_pp") or 0) >= 0.20
            assert r.get("q3") and r["q3"].get("n", 0) >= 40
            assert (r.get("lottery_day_frac") or 0) <= 0.25


def test_close_cluster_is_close_only():
    from harden_close_cluster import HOLDS, LAYERS, ba_eq1, t_green
    assert HOLDS == (1, 2)
    assert [x[0] for x in LAYERS] == ["T", "BA", "T∧BA"]
    assert t_green({"T": {"f": "C6EFCE", "v": 1}})
    assert not t_green({"T": {"f": None, "v": 1}})
    assert ba_eq1({"BA": {"v": 1}})
    assert not ba_eq1({"BA": {"v": 0}})
    src = (ENG / "harden_close_cluster.py").read_text(encoding="utf-8")
    assert "clock\": \"close\"" in src or 'clock": "close"' in src
    assert "from flatten" not in src
    assert "flatten_robust" in src
    assert "AH/FR" in src or "open recipes" in src.lower()


def test_close_cluster_report_is_committed():
    md = (ROOT / "excel_bot" / "research" / "CLOSE_CLUSTER.md").read_text()
    assert "Plain English" in md
    assert md.index("Plain English") < md.index("`fill_T_green`")
    assert "T∧BA" in md or "T and BA" in md
    assert "KEEP" in md
    assert "2026-07-01" in md or "Q3" in md
    assert "flatten_robust" in md
    assert "light + green O" in md or "light+green O" in md
    sb = (ROOT / "03_scoreboard" / "EXCEL_BOT_MINE.md").read_text()
    assert "PASS 376" in sb
    assert "Close-cluster harden" in sb
    assert "Open-stack verdict" in sb
    payload = json.loads(
        (ROOT / "excel_bot" / "research" / "close_cluster.json").read_text())
    assert payload["live_untouched"] == "flatten_robust"
    assert payload["excel_cache_used"] is False
    assert payload["finviz"] == "BLOCKED"
    assert payload["open_stack"] == "untouched"
    assert payload["q3_cut"] == "2026-07-01"
    letters = {r["letter"] for r in payload["rows"]}
    assert letters == {"T", "BA", "T∧BA"}
    for r in payload["rows"]:
        assert r["clock"] == "close"
        assert r["exit"] in ("hold1", "hold2")
        if r["verdict"] == "KEEP":
            assert (r.get("lottery_day_frac") or 0) <= 0.25
            assert r.get("q3") and r["q3"].get("n", 0) >= 40


def test_close_peers_fire_leftover_defs():
    from harden_close_peers import PEERS, fire
    assert [p[0] for p in PEERS] == ["CZ", "EH", "IB", "HO", "IL", "GV"]
    assert fire({"CZ": {"v": 2}}, "CZ", "ge2")
    assert not fire({"CZ": {"v": 1}}, "CZ", "ge2")
    assert fire({"EH": {"v": 4}}, "EH", "ge1")
    assert fire({"IB": {"v": 1}}, "IB", "eq1")
    assert not fire({"IB": {"v": 3}}, "IB", "eq1")
    src = (ENG / "harden_close_peers.py").read_text(encoding="utf-8")
    assert "from flatten" not in src
    assert "flatten_robust" in src
    assert "T/BA" in src or "T / BA" in src


def test_close_peers_report_is_committed():
    md = (ROOT / "excel_bot" / "research" / "CLOSE_CLUSTER.md").read_text()
    assert "Close-cluster peers" in md
    assert md.index("T alone is KILL") < md.index("Close-cluster peers")
    assert "CZ" in md and "EH" in md and "IB" in md
    assert "HO" in md and "IL" in md and "GV" in md
    assert "Peers table" in md
    assert "Q1" in md
    sb = (ROOT / "03_scoreboard" / "EXCEL_BOT_MINE.md").read_text()
    assert "PASS 376" in sb
    assert "Close-cluster peers" in sb
    payload = json.loads(
        (ROOT / "excel_bot" / "research" / "close_cluster_peers.json").read_text())
    assert payload["live_untouched"] == "flatten_robust"
    assert payload["excel_cache_used"] is False
    assert payload["tba_cluster"].startswith("KILL")
    assert payload["open_stack"] == "untouched"
    letters = {r["letter"] for r in payload["rows"]}
    assert letters == {"CZ", "EH", "IB", "HO", "IL", "GV"}
    for r in payload["rows"]:
        assert r["clock"] == "close"
        if r["verdict"] == "KEEP":
            q1 = r.get("q1") or {}
            assert q1.get("avg_net", 0) > 0
            assert (r.get("top5_share") or 0) <= 0.25


def test_next_region_inventory_skips_tba_and_open_stack():
    from mine_next_region import SKIP_LAG, WEEKLY, remaining_inventory
    assert tuple(WEEKLY) == ("AP", "AQ", "AR", "AS", "AT", "AU")
    assert SKIP_LAG == {"AH", "FR"}
    meta = remaining_inventory()
    assert meta["letter_space"] == "exhausted"
    assert "AH" not in meta["val_open_lag_letters"]
    assert "FR" not in meta["val_open_lag_letters"]
    src = (ENG / "mine_next_region.py").read_text(encoding="utf-8")
    assert "from flatten" not in src
    assert "flatten_robust" in src


def test_next_region_report_is_committed():
    inv = (ROOT / "excel_bot" / "research" / "NEXT_REGION_INVENTORY.md").read_text()
    assert "Plain English" in inv
    assert "exhausted" in inv
    assert "AP" in inv and "AU" in inv
    md = (ROOT / "excel_bot" / "research" / "NEXT_REGION.md").read_text()
    assert "Plain English" in md
    assert md.index("Plain English") < md.index("`weekly_") if "`weekly_" in md else True
    assert "KEEP" in md and ("KILL" in md or "THIN" in md)
    assert "light + green O" in md or "light+O" in md
    assert "T / BA" in md or "T/BA" in md
    sb = (ROOT / "03_scoreboard" / "EXCEL_BOT_MINE.md").read_text()
    assert "PASS 376" in sb
    assert "Next A–JL region" in sb
    payload = json.loads(
        (ROOT / "excel_bot" / "research" / "next_region.json").read_text())
    assert payload["live_untouched"] == "flatten_robust"
    assert payload["excel_cache_used"] is False
    assert payload["letter_space"] == "exhausted"
    assert payload["close_shortboard"].startswith("KILL")
    assert "AH/FR" in payload["standing_open"]
    for r in payload["rows"]:
        if r.get("keep") == "KEEP":
            q1 = r.get("q1") or {}
            assert q1.get("avg_net", 0) > 0
            assert (r.get("top5_share") or 0) <= 0.25
            assert r["clock"] in ("open", "close")
            assert r.get("def", "").split("_")[0] != "T"


def test_same_day_skips_tba_weekly_and_open_stack():
    from mine_same_day import (
        AND_PAIRS, SHORTBOARD, SKIP_TBA, STANDING_OPEN, remaining_inventory,
    )
    assert SKIP_TBA == {"T", "BA"}
    assert "T" in SHORTBOARD and "BA" in SHORTBOARD
    assert STANDING_OPEN == {"AH", "FR"}
    meta = remaining_inventory()
    assert "T" not in meta["fresh_letters"]
    assert "BA" not in meta["fresh_letters"]
    assert "AH" not in meta["fresh_letters"]
    assert "FR" not in meta["fresh_letters"]
    for a, b in AND_PAIRS:
        assert a not in SKIP_TBA and b not in SKIP_TBA
        assert a not in STANDING_OPEN and b not in STANDING_OPEN
    src = (ENG / "mine_same_day.py").read_text(encoding="utf-8")
    assert "from flatten" not in src
    assert "flatten_robust" in src
    assert "weekly" in src.lower() or "AP" in src


def test_same_day_report_is_committed():
    inv = (ROOT / "excel_bot" / "research" / "SAME_DAY_INVENTORY.md").read_text()
    assert "Plain English" in inv
    assert "T/BA" in inv or "T / BA" in inv
    md = (ROOT / "excel_bot" / "research" / "SAME_DAY.md").read_text()
    assert "Plain English" in md
    assert md.index("Plain English") < md.index("`close_") if "`close_" in md else True
    assert "KEEP" in md and ("KILL" in md or "THIN" in md)
    assert "exhausted" in md.lower()
    assert "light + green O" in md or "light+O" in md
    sb = (ROOT / "03_scoreboard" / "EXCEL_BOT_MINE.md").read_text()
    assert "PASS 376" in sb
    assert "Same-day multi-letter" in sb
    payload = json.loads(
        (ROOT / "excel_bot" / "research" / "same_day.json").read_text())
    assert payload["live_untouched"] == "flatten_robust"
    assert payload["excel_cache_used"] is False
    assert payload["ajl_surface"] in ("exhausted", "not_exhausted")
    assert payload["close_shortboard"].startswith("KILL")
    assert "AH/FR" in payload["standing_open"]
    assert payload["n_keep"] == 0 or payload["ajl_surface"] == "not_exhausted"
    for r in payload["rows"]:
        if r.get("keep") == "KEEP":
            q1 = r.get("q1") or {}
            assert q1.get("avg_net", 0) > 0
            assert (r.get("top5_share") or 0) <= 0.25
            assert (r.get("july_share") or 0) <= 0.40
            name = r.get("def", "")
            assert not name.startswith("weekly_")
            assert "lag_" not in name
            assert "T_green" not in name
            assert "BA_" not in name


def test_pair_lag_pilot_is_open_and_bounded():
    from mine_pair_lag import (
        FILL_OPEN, LAG_EXTRA, LANDMINE_VALUE, N_LAG, VALUE_OPEN_44,
        assert_locked_gate, build_atoms, build_pairs, collapse_twins,
    )
    from classify_clocks import build
    clocks = build()
    assert_locked_gate(clocks)
    assert len(VALUE_OPEN_44) == 44
    assert VALUE_OPEN_44 == tuple(clocks["groups"]["value_mine_open"])
    assert FILL_OPEN == tuple(clocks["groups"]["fill_mine_open"])
    assert "AH" in VALUE_OPEN_44 and "FR" in VALUE_OPEN_44
    assert "O" in FILL_OPEN and "O" not in VALUE_OPEN_44
    by = {r["col"]: r for r in clocks["columns"]}
    atoms = build_atoms(by)
    assert N_LAG == 5
    names = {a["name"] for a in atoms}
    assert "AA_l0_eq1" not in names
    assert "O_l0_eq1" not in names
    assert "O_l0_green" in names
    assert "O_l2_lt1" in names
    for a in atoms:
        if a["lag"] == 0 and a["kind"] == "num":
            assert a["col"] in VALUE_OPEN_44
            assert a["col"] not in LANDMINE_VALUE
        if a["lag"] == 0 and a["kind"] == "fill":
            assert a["col"] in FILL_OPEN
        if a["lag"] >= 1:
            assert a["col"] in VALUE_OPEN_44 or a["col"] in LAG_EXTRA
    pairs = build_pairs(collapse_twins(atoms)[:80])
    assert not any("AA_l0_" in p["name"] for p in pairs)
    labels = (ROOT / "excel_bot" / "research" / "OPEN_SAME_ROW_LABELS.md").read_text()
    assert "value_mine_open" in labels or "44" in labels
    src = (ENG / "mine_pair_lag.py").read_text(encoding="utf-8")
    assert "from flatten" not in src
    assert "flatten_robust" in src


def test_pair_lag_report_is_committed():
    plan = (ROOT / "excel_bot" / "research" / "PAIR_LAG_PLAN.md").read_text()
    assert "Plain English" in plan
    assert "09:30" in plan or "open" in plan.lower()
    inv = (ROOT / "excel_bot" / "research" / "PAIR_LAG_INVENTORY.md").read_text()
    assert "Plain English" in inv
    md = (ROOT / "excel_bot" / "research" / "PAIR_LAG.md").read_text()
    assert "Plain English" in md
    assert "Code names (after the English)" in md
    assert md.index("Plain English") < md.index("Code names (after the English)")
    assert "KEEP" in md and ("KILL" in md or "THIN" in md)
    assert "light + green O" in md or "light+O" in md
    sb = (ROOT / "03_scoreboard" / "EXCEL_BOT_MINE.md").read_text()
    assert "PASS 376" in sb
    assert "Pair+lag mine" in sb
    payload = json.loads(
        (ROOT / "excel_bot" / "research" / "pair_lag.json").read_text())
    assert payload["live_untouched"] == "flatten_robust"
    assert payload["excel_cache_used"] is False
    assert payload["entry"] == "open"
    assert payload.get("gate", "").startswith("Excel-locked") or "44" in (payload.get("gate") or "")
    assert "AH/FR" in payload["standing_open"]
    for r in payload["rows"]:
        assert r["clock"] == "open"
        if r.get("keep") == "KEEP":
            q1 = r.get("q1") or {}
            assert q1.get("avg_net", 0) > 0
            assert (r.get("top5_share") or 0) <= 0.25
            assert (r.get("july_share") or 0) <= 0.40
            assert "T_green" not in r.get("def", "")
            assert "BA_" not in r.get("def", "")


def test_pair_lag_close_gate_does_not_treat_open_as_close():
    from mine_pair_lag_close import (
        FILL_CLOSE_TODAY, HIGHLIGHT_GHOSTS, VALUE_CLOSE_CORE,
        VALUE_CLOSE_EXPAND, assert_close_gate, build_atoms, build_pairs,
        value_close_today,
    )
    from mine_pair_lag import FILL_OPEN, VALUE_OPEN_44
    from classify_clocks import build
    clocks = build()
    today = value_close_today(bound=False)
    assert_close_gate(clocks, today)
    assert "AA" in today and "O" in today
    assert "AA" in VALUE_CLOSE_CORE
    for col in VALUE_OPEN_44:
        assert col not in today
    for col in HIGHLIGHT_GHOSTS:
        assert col not in today
        assert col not in VALUE_CLOSE_CORE
        assert col not in VALUE_CLOSE_EXPAND
    for col in FILL_CLOSE_TODAY:
        assert col not in FILL_OPEN
    by = {r["col"]: r for r in clocks["columns"]}
    atoms = build_atoms(by, today)
    names = {a["name"] for a in atoms}
    assert "AA_l0_eq1" in names
    assert "O_l0_eq1" in names
    assert "O_l2_lt1" in names
    assert "O_l0_green" not in names
    assert "AA_l0_green" not in names
    for ghost in ("T_l0_eq1", "BA_l0_eq1", "T_l0_green", "BA_l0_green"):
        assert ghost not in names
    for a in atoms:
        if a["lag"] == 0 and a["kind"] == "num":
            assert a["col"] in today
            assert a["col"] not in VALUE_OPEN_44
            assert a["col"] not in HIGHLIGHT_GHOSTS
        if a["lag"] == 0 and a["kind"] == "fill":
            assert a["col"] in FILL_CLOSE_TODAY
            assert a["col"] not in FILL_OPEN
    pairs = build_pairs(atoms, today)
    assert any(p["name"] == "O_l2_lt1__and__AA_l0_eq1" for p in pairs)
    assert not any(
        tok.startswith("T_l0_") or tok.startswith("BA_l0_")
        for p in pairs for tok in p["name"].split("__and__")
    )
    src = (ENG / "mine_pair_lag_close.py").read_text(encoding="utf-8")
    assert "from flatten" not in src
    assert "flatten_robust" in src
    assert "COST_FUTU_LONG" in src


def test_pair_lag_close_report_is_committed():
    plan = (ROOT / "excel_bot" / "research" / "PAIR_LAG_CLOSE_PLAN.md").read_text()
    assert "Plain English" in plan
    assert "close" in plan.lower()
    inv = (ROOT / "excel_bot" / "research" / "PAIR_LAG_CLOSE_INVENTORY.md").read_text()
    assert "Plain English" in inv
    md = (ROOT / "excel_bot" / "research" / "PAIR_LAG_CLOSE.md").read_text()
    assert "Plain English" in md
    assert "Code names (after the English)" in md
    assert md.index("Plain English") < md.index("Code names (after the English)")
    assert "KEEP" in md and ("KILL" in md or "THIN" in md)
    assert "light + green O" in md or "light+O" in md
    sb = (ROOT / "03_scoreboard" / "EXCEL_BOT_MINE.md").read_text()
    assert "PASS 376" in sb
    assert "Pair+lag mine (close-entry)" in sb
    payload = json.loads(
        (ROOT / "excel_bot" / "research" / "pair_lag_close.json").read_text())
    assert payload["live_untouched"] == "flatten_robust"
    assert payload["excel_cache_used"] is False
    assert payload["entry"] == "close"
    assert "AH/FR" in payload["standing_open"]
    for r in payload["rows"]:
        assert r["clock"] == "close"
        if r.get("keep") == "KEEP":
            q1 = r.get("q1") or {}
            assert q1.get("avg_net", 0) > 0
            assert (r.get("top5_share") or 0) <= 0.25
            assert (r.get("july_share") or 0) <= 0.40
            assert "T_green" not in r.get("def", "")
            assert "BA_" not in r.get("def", "")
            assert "T_l0_" not in r.get("def", "")


def test_hi_horizon_gate_excludes_same_row_h_i():
    from mine_hi_horizon import (
        CLOSE_TODAY, FILL_CLOSE_TODAY, HORIZONS, LABELS, STANDING,
        assert_hi_gate, build_close_atoms, build_pairs,
    )
    from mine_pair_lag import load_clocks
    clocks, by = load_clocks()
    if "same_row_open" not in clocks:
        from classify_clocks import build
        clocks = build()
        by = {r["col"]: r for r in clocks["columns"]}
    assert_hi_gate(clocks, CLOSE_TODAY)
    assert "H" not in CLOSE_TODAY and "I" not in CLOSE_TODAY
    assert "H" not in FILL_CLOSE_TODAY and "I" not in FILL_CLOSE_TODAY
    assert "AA" in CLOSE_TODAY and "O" in CLOSE_TODAY
    atoms = build_close_atoms(by, CLOSE_TODAY)
    names = {a["name"] for a in atoms}
    assert "AA_l0_eq1" in names
    assert "O_l2_lt1" in names
    assert "H_l1_lt1" in names  # lag OK
    assert "I_l1_lt1" in names
    assert "H_l0_eq1" not in names
    assert "I_l0_eq1" not in names
    assert "H_l0_green" not in names
    pairs = build_pairs(atoms, CLOSE_TODAY)
    assert any(p["name"] == "O_l2_lt1__and__AA_l0_eq1" for p in pairs)
    assert HORIZONS == (1, 2, 3, 5, 10)
    assert "H" in LABELS and "I" in LABELS
    assert "light_O" in STANDING
    src = (ENG / "mine_hi_horizon.py").read_text(encoding="utf-8")
    assert "from flatten" not in src
    assert "flatten_robust" in src
    plan = (ROOT / "excel_bot" / "research" / "HI_HORIZON_PLAN.md").read_text()
    assert "Plain English" in plan
    assert "column H" in plan and "column I" in plan


def test_hi_horizon_report_is_committed():
    plan = (ROOT / "excel_bot" / "research" / "HI_HORIZON_PLAN.md").read_text()
    assert "Plain English" in plan
    md = (ROOT / "excel_bot" / "research" / "HI_HORIZON.md").read_text()
    assert "Plain English" in md
    assert md.index("Plain English") < md.index("Code names (after the English)")
    assert "1d" in md and "2w" in md
    assert "light" in md.lower() and "green O" in md
    sb = (ROOT / "03_scoreboard" / "EXCEL_BOT_MINE.md").read_text()
    assert "PASS 376" in sb
    assert "H/I multi-horizon" in sb
    payload = json.loads(
        (ROOT / "excel_bot" / "research" / "hi_horizon.json").read_text())
    assert payload["live_untouched"] == "flatten_robust"
    assert payload["excel_cache_used"] is False
    assert "H" in payload["labels"] and "I" in payload["labels"]
    for r in payload["rows"]:
        if r.get("family") != "standing":
            name = r.get("def") or ""
            assert "H_l0_" not in name
            assert "I_l0_" not in name
        if r.get("keep") == "KEEP":
            assert (r.get("top5_share") or 0) <= 0.25
            assert (r.get("july_share") or 0) <= 0.40
            assert "T_green" not in (r.get("def") or "")
            assert "BA_" not in (r.get("def") or "")


def test_unmined_miner_does_not_wire_live():
    for fn in ("mine_unmined.py", "harden_unmined.py", "harden_open_stack.py",
               "harden_close_cluster.py", "harden_close_peers.py",
               "mine_next_region.py", "mine_same_day.py", "mine_pair_lag.py",
               "mine_pair_lag_close.py", "mine_hi_horizon.py",
               "mine_shade_open.py"):
        src = (ENG / fn).read_text(encoding="utf-8")
        assert "from flatten" not in src
        assert "sleeve_merge_live" not in src
        assert "LIVE_POLICY" not in src
        assert "flatten_robust" in src
    src = (ENG / "mine_unmined.py").read_text(encoding="utf-8")
    assert "Never Excel" in src or "never" in src.lower()
    cap = (ENG / "capture_all_cols.py").read_text(encoding="utf-8")
    assert "yahoo_rows_cache" in cap
    assert "excel_stockhistory_cache" in cap
    assert "--all-rows" in cap
    assert "tickers_all_rows" in cap


def test_shade_open_cf_inventory_and_clocks():
    from mine_shade_open import (
        CLOSE_FILL, MULTI_SHADE, OPEN_ALIASES, OPEN_FILL, assert_open_gate,
        cf_inventory, meaning_of, parent_of,
    )
    assert_open_gate()
    assert OPEN_FILL == tuple("ABCGJKLMO")
    assert set(CLOSE_FILL) == set("DEFHIN")
    assert OPEN_ALIASES == ("IR", "IS", "IT")
    inv = cf_inventory()
    assert inv["live_untouched"] == "flatten_robust"
    assert set(inv["multi_shade_letters"]) == set(MULTI_SHADE) == {"A", "G", "K", "L"}
    assert "O" in inv["single_green_letters"]
    assert "C" in inv["no_green_cf"]
    assert "IR" in inv["no_green_cf"]
    by = {r["col"]: r for r in inv["letters"]}
    assert by["A"]["green_hexes"] == ["3B7D23", "B8DCAB"]
    assert by["O"]["green_hexes"] == ["C6EFCE"]
    assert by["IR"]["n_rules"] == 0
    assert parent_of("A_hex_3B7D23") == "A_green"
    assert parent_of("light__A_hex_3B7D23") == "light__A_green"
    assert parent_of("light__A_green") == "light_on"
    assert parent_of("A_onset_green") == "A_green"
    assert "deep green" in meaning_of("A_ge20")
    assert "flips from red" in meaning_of("A_onset_red2green")
    assert "pale" in meaning_of("M_hex_DCEDD5")
    src = (ENG / "mine_shade_open.py").read_text(encoding="utf-8")
    assert "from flatten" not in src
    assert "flatten_robust" in src
    assert "H_l0_" not in src or "never" in src.lower()


def test_shade_open_report_is_committed():
    md = (ROOT / "excel_bot" / "research" / "SHADE_OPEN.md").read_text()
    assert md.index("Plain English") < md.index("Inventory")
    assert "flatten_robust" in md
    assert "same-day H" in md.lower() or "Same-day H" in md
    assert "3B7D23" in md and "C6EFCE" in md
    assert "IR" in md and "IT" in md
    assert "Family verdict: KEEP" in md
    assert "Per-recipe majority" in md
    assert "#95CA82" in md
    sb = (ROOT / "03_scoreboard" / "EXCEL_BOT_MINE.md").read_text()
    assert "PASS 376" in sb
    assert "Shade hex + onset" in sb
    assert "Family **KEEP**" in sb
    payload = json.loads(
        (ROOT / "excel_bot" / "research" / "shade_open.json").read_text())
    assert payload["live_untouched"] == "flatten_robust"
    assert payload["excel_cache_used"] is False
    assert payload["entry"] == "open"
    assert payload["cost_model"] == "futubull"
    assert payload["family_verdict"] == "KEEP"
    assert "A" in payload["multi_shade_letters"]
    for r in payload.get("rows") or []:
        assert r.get("clock") == "open"
        name = r.get("def") or ""
        assert "H_l0_" not in name and "I_l0_" not in name
        if r.get("keep") == "KEEP":
            assert (r.get("top5_share") or 0) <= 0.25
            reasons = set(r.get("fail_reasons") or [])
            assert not reasons.intersection(
                {"thin_disc", "thin_hold", "ticker_bar", "date_bar"})


if __name__ == "__main__":
    tests = [v for k, v in list(globals().items()) if k.startswith("test_")]
    for fn in tests:
        fn()
        print("ok", fn.__name__)
    print(f"{len(tests)} excel_bot mine tests passed")
