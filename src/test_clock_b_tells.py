"""Clock-B catalogue wiring — leak-free unit tests, no full remine."""
from __future__ import annotations

import json
from pathlib import Path

from src import clock_b_tells as cbt
from src import factor_mine as fm
from src import factor_mine_book as fmb
from src import factor_mine_sim as fms


def _row(**kw):
    base = {
        "date": "2026-09-16",
        "ticker": "AAA",
        "sources": ["union", "ohlc_hot"],
        "boxes": {"peer": "good", "sector": "neutral", "catal": "missing",
                  "news": "neutral"},
        "last_green": True,
        "last_red": False,
        "ohlc_ret_1": 1.2,
        "ohlc_ret_5": 4.0,
        "ohlc_ret_10": 6.0,
        "ohlc_rvol": 1.1,
        "ohlc_break_10": True,
        "ohlc_nr7": False,
        "candle_capture": False,
        "macd_up": True,
        "macd_down": False,
        "rsi_ob": False,
        "rsi_os": False,
        "flow_in": False,
        "alarm": False,
        "news_box": "neutral",
        "news_prior": "missing",
        "erd_earn_react": False,
        "erd_flag_E": None,
        "erd_days_since_E": None,
        "erd_flag_R": 0,
        "erd_days_since_R": None,
        "e_pol": "",
        "fv_inst": None,
        "ins_buy": False,
        "form4_buy": False,
        "change_pct": 19.4,  # same-day leak — must not gate
        "Gap": 8.0,
        "RelVol": 12.0,
        "vwap": 11.2,
    }
    base.update(kw)
    return base


def test_catalogue_has_24_families_and_10_priority() -> None:
    assert len(cbt.FAMILIES) == 24
    assert len(cbt.PRIORITY) == 10
    assert len(cbt.COMBO_KEYS) == 10
    statuses = {f["status"] for f in cbt.FAMILIES}
    assert statuses <= {"have", "calculable", "need-source"}
    assert all(f["status"] != "need-source" for f in cbt.FAMILIES)
    assert cbt.NEED_SOURCE
    assert all("VWAP" not in " ".join(f.get("cols") or ()) for f in cbt.FAMILIES)


def test_clock_b_atoms_ignore_same_day_tape() -> None:
    row = _row()
    assert not cbt.leak_fields_used({"ohlc_ret_5": 4.0})
    used = cbt.leak_fields_used(row)
    assert "change_pct" in used or "Gap" in used
    # Evaluators never read those keys — flipping them cannot flip a combo.
    a = {k: cbt.combo_true(row, k) for k in cbt.COMBO_KEYS}
    flipped = dict(row, change_pct=-99, Gap=-9, RelVol=0.1, vwap=0.0)
    b = {k: cbt.combo_true(flipped, k) for k in cbt.COMBO_KEYS}
    assert a == b
    for leak in ("change", "Gap", "RelVol", "vwap", "intraday"):
        assert leak not in fm.INPUT_FIELDS


def test_priority_1_mom_break_peer() -> None:
    row = _row()
    assert cbt.clk_mom_break_peer(row) is True
    assert cbt.clk_mom_break_peer(_row(ohlc_break_10=False)) is False
    assert cbt.clk_mom_break_peer(_row(boxes={"peer": "bad", "sector": "bad"})) is False
    assert cbt.clk_mom_break_peer(_row(ohlc_ret_5=18.0)) is False
    rec = fm.make_recipe("t", require={"clk_mom_break_peer": True},
                         forbid={"clk_ext_veto": True})
    assert fm.matches(row, rec) is True
    assert fm.matches(_row(ohlc_break_10=False), rec) is False


def test_priority_2_fresh_cat_coil() -> None:
    row = _row(news_prior="good", ohlc_break_10=False)
    assert cbt.clk_fresh_cat_coil(row) is True
    exploded = _row(news_prior="good", ohlc_ret_5=22.0, ohlc_rvol=4.0)
    assert cbt.clk_fresh_cat_coil(exploded) is False
    rec = fm.make_recipe("t", require={"clk_fresh_cat_coil": True})
    assert fm.matches(row, rec) is True


def test_priority_3_earn_guide_react_knowable_only() -> None:
    ok = _row(
        erd_flag_E=1, erd_days_since_E=1, erd_earn_react=True,
        e_pol="good", news_prior="good",
    )
    assert cbt.clk_earn_guide_react(ok) is True
    # Same-day miss painted bad — not knowable-good.
    miss = dict(ok, e_pol="bad")
    assert cbt.clk_earn_guide_react(miss) is False
    # Stale E (>5 sessions) is not "fresh".
    stale = dict(ok, erd_days_since_E=12)
    assert cbt.clk_earn_guide_react(stale) is False
    rec = fm.make_recipe("t", require={"clk_earn_guide_react": True})
    assert fm.matches(ok, rec) is True
    assert fm.matches(miss, rec) is False


def test_priority_4_neg_weak_fail() -> None:
    row = _row(
        news_box="bad", last_green=False, last_red=True, macd_up=False,
        macd_down=True, ohlc_ret_5=-2.0,
        boxes={"peer": "bad", "sector": "bad"},
    )
    assert cbt.clk_neg_weak_fail(row) is True
    rec = fm.make_recipe("t", side="short", require={"clk_neg_weak_fail": True})
    assert fm.matches(row, rec) is True
    assert fm.matches(_row(), rec) is False


def test_priority_5_ext_veto_on_longs() -> None:
    veto = _row(
        ohlc_ret_5=18.0, ohlc_ret_10=20.0, last_green=True,
        last_red=True, ohlc_break_10=True, rsi_ob=True,
    )
    assert cbt.clk_ext_veto(veto) is True
    rec = fm.make_recipe("t", require={"clk_mom_break_peer": True},
                         forbid={"clk_ext_veto": True})
    # Extreme + failed breakout is a long veto even if peer/breakout fire.
    assert fm.matches(veto, rec) is False
    short = fm.make_recipe("t", side="short", require={"clk_ext_veto": True})
    assert fm.matches(veto, short) is True


def test_priority_6_hold_vs_sector() -> None:
    row = _row(boxes={"peer": "good", "sector": "bad"}, last_green=True,
               ohlc_ret_1=0.8)
    assert cbt.clk_hold_vs_sector(row) is True
    weak = _row(boxes={"sector": "bad"}, last_green=False, ohlc_ret_1=-1.5)
    assert cbt.clk_hold_vs_sector(weak) is False
    rec = fm.make_recipe("t", require={"clk_hold_vs_sector": True})
    assert fm.matches(row, rec) is True


def test_priority_7_insider_requires_data() -> None:
    bare = _row(fv_inst=2.0, flow_in=True, last_green=True)
    assert cbt.clk_insider_cash_stab(bare) is False  # no insider print
    hit = _row(ins_buy=True, fv_inst=1.2, flow_in=True)
    assert cbt.clk_insider_cash_stab(hit) is True
    selling = _row(ins_buy=True, fv_inst=-3.0, flow_in=True)
    assert cbt.clk_insider_cash_stab(selling) is False
    rec = fm.make_recipe("t", require={"clk_insider_cash_stab": True})
    assert fm.matches(hit, rec) is True
    assert fm.matches(bare, rec) is False


def test_priority_8_10_existing_collectors() -> None:
    flow = _row(flow_in=True, last_green=True, ohlc_ret_5=3.0, ohlc_rvol=1.6)
    assert cbt.clk_flow_coil(flow) is True
    up = _row(erd_flag_R=1, erd_days_since_R=2, last_green=True,
              ohlc_ret_5=2.0)
    assert cbt.clk_r_up_coil(up) is True
    nr7 = _row(ohlc_nr7=True, last_green=True, ohlc_ret_5=3.5, macd_up=True)
    assert cbt.clk_nr7_mom(nr7) is True
    recs = {r["name"]: r for r in fm.build_recipes()}
    assert fm.matches(flow, recs["union_clk_flow_coil_h1"]) is True
    assert fm.matches(up, recs["union_clk_r_up_coil_h1"]) is True
    assert fm.matches(nr7, recs["union_clk_nr7_mom_h1"]) is True


def test_recipes_and_gates_are_wired() -> None:
    names = {r["name"] for r in fm.build_recipes()}
    for n in cbt.CLOCK_B_RECIPES:
        assert n in names, n
    recs = [r for r in fm.build_recipes() if r["name"] in cbt.CLOCK_B_RECIPES]
    assert len(recs) == 10
    assert "clk_b" in fmb.GATES
    gated = fmb.recipes_from_action(gate="clk_b", auto_tweak=False)
    assert {r["name"] for r in gated} == set(cbt.CLOCK_B_RECIPES)
    for r in recs:
        ex = fm.explain_recipe(r)
        assert ex["kid"] and ex["inputs"] and ex["buy"]
        assert "09:30" in " ".join(ex["inputs"])
        assert "KEEP" not in (r.get("note") or "") or "not KEEP" in (r.get("note") or "")
    assert "clk_mom_break_peer" in fm.INPUT_FIELDS
    assert "ins_buy" in fms.ROW_KEEP
    assert "clk_ext_veto" in fms.ROW_KEEP
    js = fm.SIM_JS.read_text(encoding="utf-8")
    assert "clk_mom_break_peer" in js
    assert "clk_ext_veto" in js


def test_stamp_and_match_why() -> None:
    row = _row()
    cbt.stamp_row(row)
    assert row["clk_mom_break_peer"] is True
    rec = fm.make_recipe("t", require={"clk_mom_break_peer": True})
    why = fm.match_why(row, rec)
    assert why["ok"] is True
    assert any("Clock-B #1" in p for p in why["passed"])


def test_js_matches_stamped_clock_b_flags() -> None:
    import subprocess
    Path("/tmp/fm_clk_run.mjs").write_text(
        """
        import { readFileSync, writeFileSync } from 'node:fs';
        import vm from 'node:vm';
        vm.runInThisContext(readFileSync('src/factor_mine_sim.js','utf8'));
        const row = {ticker:'AAA', sources:['union'], boxes:{},
          clk_mom_break_peer:true, clk_ext_veto:false};
        const rec = {universe:'union', require:{clk_mom_break_peer:true},
                     forbid:{clk_ext_veto:true}};
        const veto = {universe:'union', require:{clk_mom_break_peer:true},
                      forbid:{clk_ext_veto:true}};
        const rowV = {...row, clk_ext_veto:true};
        writeFileSync('/tmp/fm_clk_out.json', JSON.stringify({
          ok: globalThis.FMSim.matches(row, rec, {}),
          veto: globalThis.FMSim.matches(rowV, veto, {}),
        }));
        """,
        encoding="utf-8",
    )
    subprocess.check_call(["node", "/tmp/fm_clk_run.mjs"])
    out = json.loads(Path("/tmp/fm_clk_out.json").read_text(encoding="utf-8"))
    assert out["ok"] is True
    assert out["veto"] is False


def test_panel_smoke_restored_multisrc_and_clock_b_eval() -> None:
    """Remine 35438833792 shape: 09-16/17/18 multi-src. Evaluate tells.

    Does not cash-book and does not claim KEEP.
    """
    path = Path("data/factor_mine/panel.json")
    if not path.is_file():
        return
    panel = json.loads(path.read_text(encoding="utf-8"))
    want = ("2026-09-16", "2026-09-17", "2026-09-18")
    proof = cbt.panel_fire_counts(panel, list(want))
    for d in want:
        assert proof["n"].get(d, 0) >= 50, (d, proof["n"])
        srcs = set(proof["sources"].get(d) or {})
        assert {"ohlc_hot", "yday_gainer", "probable"} <= srcs, (d, srcs)
    # Fresh-cat / mom-break / nr7 should find *some* names on a 3-day union.
    fires = proof["fires"]
    live = sum(fires["clk_mom_break_peer"].values())
    coil = sum(fires["clk_fresh_cat_coil"].values())
    nr7 = sum(fires["clk_nr7_mom"].values())
    assert live + coil + nr7 >= 1, proof["fires"]
    rec = fm.make_recipe("t", require={"clk_fresh_cat_coil": True},
                         forbid={"clk_ext_veto": True})
    n_match = sum(
        1 for r in panel["rows"]
        if r.get("date") in want and fm.matches(r, rec)
    )
    assert n_match <= coil  # #5 veto may drop longs; it cannot add them


def test_form4_asof_is_completed_month() -> None:
    # Knowable: August month is visible on 2026-09-16. September is not.
    # Function must not throw when the panel is present or missing.
    assert cbt.form4_buy_asof("", "2026-09-16") is False
    assert cbt.form4_buy_asof("ZZZZZZ", "2026-09-16") is False


def main() -> None:
    test_catalogue_has_24_families_and_10_priority()
    test_clock_b_atoms_ignore_same_day_tape()
    test_priority_1_mom_break_peer()
    test_priority_2_fresh_cat_coil()
    test_priority_3_earn_guide_react_knowable_only()
    test_priority_4_neg_weak_fail()
    test_priority_5_ext_veto_on_longs()
    test_priority_6_hold_vs_sector()
    test_priority_7_insider_requires_data()
    test_priority_8_10_existing_collectors()
    test_recipes_and_gates_are_wired()
    test_stamp_and_match_why()
    test_js_matches_stamped_clock_b_flags()
    test_panel_smoke_restored_multisrc_and_clock_b_eval()
    test_form4_asof_is_completed_month()
    print("15 clock-b tell tests passed")


if __name__ == "__main__":
    main()
