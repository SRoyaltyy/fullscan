"""Tests for the v2 prediction engines and the numeric learning loop:
tape_anchor, engine_policy, compute_scores / compute_sector_scores v2,
lesson_select, lesson_retire, improvement_tracker, etf_premarket fetch."""
from __future__ import annotations

import json
import sys
import types
from pathlib import Path

import pytest

from src import (compute_scores, compute_sector_scores, engine_policy,
                 fetch_channel1, improvement_tracker, lesson_retire,
                 lesson_select, tape_anchor)


# ------------------------------------------------------------ fixtures
def _ch1(es=0.8, nq=1.0, eu=0.5, vix=-0.6, cl=None, pm=None):
    d = {
        "fetched_at": "2026-09-10T05:55:00-04:00",
        "futures": {"ES=F": {"available": True, "pct_1d": es},
                    "NQ=F": {"available": True, "pct_1d": nq}},
        "global_sessions": {"europe": {"composite_avg": eu, "per_index": {}},
                            "asia": {"composite_avg": 0.2, "per_index": {}}},
        "vix": {"vix": {"delta_1d": vix}},
        "commodities_fx": {"CL=F": {"available": cl is not None, "pct_1d": cl}},
    }
    if pm is not None:
        d["etf_premarket"] = {"XLE": {"available": True, "pct_vs_prev_close": pm}}
    return d


def _scores(**over):
    base = {k: 0.0 for k in engine_policy.GENERAL_KEYS}
    base.update({"MULTIPLIER": 1.0, "CONFIDENCE": 0.6})
    base.update(over)
    return base


def _sector_scores(**over):
    base = {k: 0.0 for k in engine_policy.SECTOR_KEYS}
    base.update({"MULTIPLIER": 1.0, "CONFIDENCE": 0.6})
    base.update(over)
    return base


# --------------------------------------------------------- tape_anchor
def test_general_anchor_weighted_mean_and_vix_sign():
    a = tape_anchor.general_anchor(_ch1(es=1.0, nq=1.0, eu=1.0, vix=0.0))
    assert a["available"]
    # ES 1.0 + NQ 0.6 + EU 0.8 + VIX -0.15*0 over |w| sum 2.55
    assert a["pct"] == pytest.approx((1.0 + 0.6 + 0.8) / 2.55, abs=1e-4)
    assert a["score"] == pytest.approx(min(tape_anchor.ANCHOR_CLIP,
                                           a["pct"] * tape_anchor.SCORE_PER_PCT), rel=1e-3)
    legs = {l["leg"] for l in a["legs"]}
    assert {"ES", "NQ", "EUROPE", "VIX_D1"} <= legs
    # a VIX jump pulls the anchor down
    b = tape_anchor.general_anchor(_ch1(es=0.0, nq=0.0, eu=0.0, vix=5.0))
    assert b["available"] and b["pct"] < 0


def test_general_anchor_unavailable_without_tape():
    assert tape_anchor.general_anchor(None)["available"] is False
    assert tape_anchor.general_anchor({})["available"] is False
    assert tape_anchor.general_anchor({"futures": {"ES=F": {"pct_1d": "nan"}}})["available"] is False


def test_general_anchor_clips():
    a = tape_anchor.general_anchor(_ch1(es=9.0, nq=9.0, eu=9.0, vix=-20))
    assert a["score"] == tape_anchor.ANCHOR_CLIP


def test_sector_anchor_beta_sum_and_premarket_blend():
    ch1 = _ch1(es=1.0, nq=0.0, eu=0.0, vix=0.0, cl=2.0)
    a = tape_anchor.sector_anchor("Energy", "XLE", ch1)
    assert a["available"]
    # Energy: CL .35 + ES .6  (QA absent)
    assert a["pct"] == pytest.approx(0.35 * 2.0 + 0.6 * 1.0, abs=1e-4)
    b = tape_anchor.sector_anchor("Energy", "XLE", _ch1(es=1.0, nq=0.0, eu=0.0, vix=0.0, cl=2.0, pm=-1.0))
    share = tape_anchor.ETF_PREMARKET_SHARE
    assert b["pct"] == pytest.approx(share * -1.0 + (1 - share) * a["pct"], abs=1e-3)
    assert any(l["leg"] == "PM:XLE" for l in b["legs"])


def test_sector_anchor_needs_an_equity_leg():
    ch1 = {"commodities_fx": {"CL=F": {"pct_1d": 3.0}}}
    assert tape_anchor.sector_anchor("Energy", "XLE", ch1)["available"] is False
    # but the own-ETF gap alone is enough
    ch1["etf_premarket"] = {"XLE": {"pct_vs_prev_close": 0.9}}
    a = tape_anchor.sector_anchor("Energy", "XLE", ch1)
    assert a["available"] and a["pct"] == pytest.approx(0.9)


# -------------------------------------------------------- engine_policy
def test_multiplier_ladder():
    assert engine_policy.multiplier_for(engine_policy.MIN_N - 1, 0.1) == 1.0
    assert engine_policy.multiplier_for(20, 0.30) == 0.0
    assert engine_policy.multiplier_for(20, 0.50) == 0.5
    assert engine_policy.multiplier_for(20, 0.60) == 1.0
    assert engine_policy.multiplier_for(20, 0.70) == engine_policy.BONUS_MULT


def _board(n_good=12, n_bad=12):
    runs = []
    for i in range(n_good):
        d = f"2026-07-{i + 1:02d}"
        runs.append({"date": d, "topic": "general", "actual_pct_change": 0.8,
                     "components": {"B1_CATALYSTS": 2.0, "B2_BONDS": -2.0}})
    for i in range(n_bad):
        d = f"2026-08-{i + 1:02d}"
        runs.append({"date": d, "topic": "sector:Energy", "sector": "Energy", "etf": "XLE",
                     "actual_pct_change": -0.8,
                     "components": {"S0_SHARED_MACRO": -1.0, "S3_FLOWS_POSITIONING": 1.0}})
    return {"runs": runs}


def test_factor_skill_walk_forward_and_policy_shape():
    board = _board()
    pol = engine_policy.build_policy(board)
    assert pol["general"]["B1_CATALYSTS"]["mult"] == engine_policy.BONUS_MULT
    assert pol["general"]["B2_BONDS"]["mult"] == 0.0
    assert pol["general"]["B4_VIX"]["n"] == 0 and pol["general"]["B4_VIX"]["mult"] == 1.0
    assert pol["sectors"]["Energy"]["S3_FLOWS_POSITIONING"]["mult"] == 0.0
    # walk-forward: before the first date nothing is known -> design weights
    early = engine_policy.build_policy(board, before_date="2026-07-01")
    assert all(v["mult"] == 1.0 for v in early["general"].values())
    # thin sector falls back to pooled and says so
    board["runs"].append({"date": "2026-08-20", "topic": "sector:Utilities", "sector": "Utilities",
                          "actual_pct_change": 0.5, "components": {"S0_SHARED_MACRO": 1.0}})
    pol = engine_policy.build_policy(board)
    assert pol["sectors"]["Utilities"]["S0_SHARED_MACRO"].get("pooled") is True


def test_update_policy_file_ledger(tmp_path, monkeypatch):
    monkeypatch.setattr(engine_policy, "POLICY_PATH", str(tmp_path / "engine_policy.json"))
    p1 = engine_policy.update_policy_file(_board())
    assert p1["version"] == 1
    assert any("B2_BONDS: 1.0 -> 0.0" in c for c in p1["history"][-1]["changes"])
    p2 = engine_policy.update_policy_file(_board())
    assert p2["version"] == 2 and p2["history"][-1]["changes"] == ["hold"]
    assert engine_policy.general_multipliers(engine_policy.load_policy())["B2_BONDS"] == 0.0


# ------------------------------------------------- compute_scores (v2)
def test_v2_anchor_dominates_muted_llm_and_flat_zone():
    pol = {"general": {k: {"mult": 0.0} for k in engine_policy.GENERAL_KEYS}}
    # LLM screams down, every component muted, tape says up -> up
    d = compute_scores.compute(_scores(B2_BONDS=-5, B5_SENTIMENT=-5), ch1=_ch1(), policy=pol)
    assert d["engine"] == "v2" and d["predicted_direction"] == "up"
    assert d["overlay_score"] == 0.0 and d["anchor"]["available"]
    # nothing at all -> flat, not a coin flip
    z = compute_scores.compute(_scores(), ch1=None, policy={})
    assert z["predicted_direction"] == "flat" and z["total_score"] == 0.0


def test_v2_overlay_is_capped_and_duplicates_dropped():
    pol = {"general": {k: {"mult": 1.0} for k in engine_policy.GENERAL_KEYS}}
    loud = _scores(**{k: 5.0 for k in engine_policy.GENERAL_KEYS})
    d = compute_scores.compute(loud, ch1=_ch1(es=-0.1, nq=-0.1, eu=-0.1, vix=0.0), policy=pol)
    assert d["overlay_score"] == compute_scores.OVERLAY_CAP
    assert d["overlay_raw"] > d["overlay_score"]
    # without tape the same components are used uncapped, and the tape
    # duplicates (B0/B6) are back in
    e = compute_scores.compute(loud, ch1=None, policy=pol)
    assert e["overlay_score"] > d["overlay_raw"]
    assert e["predicted_direction"] == "up"


def test_v2_magnitude_from_anchor_and_legacy_still_available():
    d = compute_scores.compute(_scores(), ch1=_ch1(es=2.5, nq=2.5, eu=2.5, vix=-3), policy={})
    assert d["predicted_direction"] == "up" and d["predicted_magnitude_band"] == "severe"
    m = compute_scores.compute(_scores(), ch1=_ch1(es=0.5, nq=0.5, eu=0.5, vix=0), policy={})
    assert m["predicted_magnitude_band"] == "mild"
    leg = compute_scores.compute_legacy(_scores(B1_CATALYSTS=2.0, B6_FUTURES=1.0))
    assert leg["engine"] == "legacy" and "total_score" in leg


def test_v2_sector_index_carry_and_flat_inherit():
    pol = {"sector_pooled": {k: {"mult": 1.0} for k in engine_policy.SECTOR_KEYS}, "sectors": {}}
    d = compute_sector_scores.compute(_sector_scores(), sector="Technology", etf="XLK",
                                      ch1=None, policy=pol, general_total=4.0)
    assert d["index_carry"] == pytest.approx(compute_sector_scores.INDEX_CARRY * 4.0)
    assert d["predicted_direction"] == "up" and d["predicted_magnitude_band"] == "mild"
    # own tape overrides index
    e = compute_sector_scores.compute(_sector_scores(), sector="Energy", etf="XLE",
                                      ch1=_ch1(es=-1.0, nq=-1.0, eu=-1.0, vix=2, cl=-3.0),
                                      policy=pol, general_total=1.0)
    assert e["predicted_direction"] == "down" and e["anchor"]["available"]
    assert e["engine"] == "v2"
    assert compute_sector_scores.compute_legacy(_sector_scores(S1_SECTOR_FACTORS=2.0))["engine"] == "legacy"


# --------------------------------------------------------- lesson_select
def _lesson(path: Path, scope: str, sources: str, since: str, cat: str = "B", body: str = "## RULE\nx"):
    path.write_text(
        f'---\nscope: "{scope}"\nerror_category: "{cat}"\nstatus: "active"\n'
        f'promoted_on: "{since}"\nsources: "{sources}"\n---\n\n{body}\n', encoding="utf-8")


def test_lesson_select_filters_ranks_and_caps(tmp_path, monkeypatch):
    active = tmp_path / "active"
    active.mkdir()
    monkeypatch.setattr(lesson_select.config, "LESSONS_ACTIVE", str(active))
    monkeypatch.setattr(lesson_select, "EFFICACY_JSON", str(tmp_path / "eff.json"))
    _lesson(active / "gen_old.md", "general", "['2026-08-01_lesson.md']", "2026-08-01")
    _lesson(active / "gen_new.md", "general", "['2026-08-20_lesson.md']", "2026-08-20")
    _lesson(active / "energy.md", "general", "['2026-08-10_sector_energy_lesson.md']", "2026-08-10")
    _lesson(active / "book.md", "book", "['2026-08-10_book_lesson.md']", "2026-08-10")
    _lesson(active / "ops.md", "ops", "['2026-08-05_lesson.md']", "2026-08-05", cat="D")
    _lesson(active / "gen_bad.md", "general", "['2026-08-15_lesson.md']", "2026-08-15")
    (tmp_path / "eff.json").write_text(json.dumps({"lessons": [
        {"lesson": "gen_bad.md", "verdict": "WORSE"},
        {"lesson": "gen_old.md", "verdict": "improved"},
    ]}), encoding="utf-8")
    names = [n for n, _ in lesson_select.select_active("general")]
    assert "gen_bad.md" not in names and "book.md" not in names and "energy.md" not in names
    assert names[0] == "gen_old.md"              # improved outranks newer/unjudged
    assert names[1] == "gen_new.md"              # then newest first
    assert "ops.md" in names
    energy = [n for n, _ in lesson_select.select_active("sector:Energy")]
    assert energy == ["energy.md", "ops.md"]
    assert len(lesson_select.select_active("general", limit=1)) == 1
    # compact drops the front matter
    text = dict(lesson_select.select_active("general"))["gen_new.md"]
    assert text.startswith("## RULE") and "promoted_on" not in text


# --------------------------------------------------------- lesson_retire
def test_retire_moves_worst_first_and_caps(tmp_path, monkeypatch):
    active, retired = tmp_path / "active", tmp_path / "retired"
    active.mkdir()
    monkeypatch.setattr(lesson_retire, "ACTIVE_DIR", active)
    monkeypatch.setattr(lesson_retire, "RETIRED_DIR", retired)
    monkeypatch.setattr(lesson_retire, "LEDGER_MD", tmp_path / "L.md")
    monkeypatch.setattr(lesson_retire, "LEDGER_JSON", tmp_path / "L.json")
    for n in ("a.md", "b.md", "c.md", "keep.md"):
        _lesson(active / n, "general", "[]", "2026-08-10")
    eff = {"lessons": [
        {"lesson": "a.md", "topic": "general", "verdict": "WORSE", "delta": -0.2,
         "before": {"hit": 0.7, "n": 5}, "after": {"hit": 0.5, "n": 5}, "active_since": "2026-08-10"},
        {"lesson": "b.md", "topic": "general", "verdict": "WORSE", "delta": -0.6,
         "before": {"hit": 0.8, "n": 5}, "after": {"hit": 0.2, "n": 5}, "active_since": "2026-08-10"},
        {"lesson": "c.md", "topic": "general", "verdict": "WORSE", "delta": -0.3,
         "before": {"hit": 0.6, "n": 5}, "after": {"hit": 0.3, "n": 5}, "active_since": "2026-08-10"},
        {"lesson": "keep.md", "topic": "general", "verdict": "improved", "delta": 0.3,
         "before": {"hit": 0.4, "n": 5}, "after": {"hit": 0.7, "n": 5}},
    ]}
    dry = lesson_retire.retire(eff, dry_run=True, max_per_cycle=2)
    assert [r["lesson"] for r in dry] == ["b.md", "c.md"] and (active / "b.md").exists()
    done = lesson_retire.retire(eff, max_per_cycle=2)
    assert [r["lesson"] for r in done] == ["b.md", "c.md"]
    assert not (active / "b.md").exists() and (retired / "b.md").exists()
    assert (active / "a.md").exists() and (active / "keep.md").exists()
    moved = (retired / "b.md").read_text(encoding="utf-8")
    assert 'status: "retired"' in moved and "retire_reason" in moved and 'status: "active"' not in moved
    ledger = json.loads((tmp_path / "L.json").read_text(encoding="utf-8"))
    assert len(ledger) == 2 and (tmp_path / "L.md").exists()
    # second night: a.md goes, and the ledger appends
    again = lesson_retire.retire(eff, max_per_cycle=2)
    assert [r["lesson"] for r in again] == ["a.md"]
    assert len(json.loads((tmp_path / "L.json").read_text(encoding="utf-8"))) == 3


# --------------------------------------------------- improvement_tracker
def test_tracker_baselines_eras_and_curve(tmp_path, monkeypatch):
    runs = []
    seq = [("up", 0.5, True), ("up", 0.6, True), ("down", -0.7, True), ("up", -0.5, False),
           ("up", 0.9, True), ("down", 0.4, False)]
    for i, (pred, pct, hit) in enumerate(seq):
        runs.append({"date": f"2026-09-{i + 1:02d}", "topic": "general", "predicted_direction": pred,
                     "predicted_magnitude_band": "mild", "actual_pct_change": pct,
                     "direction_hit": hit, "magnitude_hit": True,
                     "engine": "v2" if i >= 4 else None})
    monkeypatch.setattr(improvement_tracker.scoreboard, "load", lambda: {"runs": runs})
    monkeypatch.setattr(improvement_tracker, "OUT_MD", str(tmp_path / "T.md"))
    monkeypatch.setattr(improvement_tracker, "OUT_JSON", str(tmp_path / "t.json"))
    monkeypatch.setattr(improvement_tracker, "REPLAY_JSON", str(tmp_path / "missing.json"))
    res = improvement_tracker.run()
    g = res["scopes"]["general"]
    assert g["n_runs"] == 6 and g["n_sessions"] == 6
    assert g["all"]["dir"] == pytest.approx(4 / 6, abs=1e-3)
    assert g["all"]["always_up"] == pytest.approx(4 / 6, abs=1e-3)
    assert g["all"]["always_down"] == pytest.approx(2 / 6, abs=1e-3)
    # same-as-yesterday: actual dirs up,up,down,down,up,up -> matches on 2,4,6 = 3/5
    assert g["all"]["same_as_yesterday"] == pytest.approx(3 / 5, abs=1e-3)
    assert g["legacy_era"]["n"] == 4 and g["since_v2"]["live"] and g["since_v2"]["n"] == 2
    assert g["since_v2"]["since"] == "2026-09-05"
    assert g["since_v2"]["cumulative"][-1]["cum_dir"] == 0.5
    assert g["curve"][-1]["dir_10"] == pytest.approx(4 / 6, abs=1e-3)
    assert "edge_10" in g["curve"][-1]
    md = (tmp_path / "T.md").read_text(encoding="utf-8")
    assert "v2 engine live (since 2026-09-05)" in md and "Session curve" in md
    assert res["scopes"]["sectors"]["n_runs"] == 0


# ------------------------------------------------- fetch_channel1 premarket
def test_premarket_pct_uses_prior_regular_close_and_latest_preopen_bar(monkeypatch):
    import pandas as pd

    idx = pd.to_datetime([
        "2026-09-09 15:55", "2026-09-09 17:00",      # last regular bar, then after-hours (ignored)
        "2026-09-10 04:05", "2026-09-10 05:50",      # pre-market today
        "2026-09-10 09:35",                          # regular session (ignored)
    ]).tz_localize("America/New_York")
    bars = pd.DataFrame({"Close": [100.0, 101.0, 99.0, 98.0, 97.0]}, index=idx)

    class _T:
        def __init__(self, sym):
            self.sym = sym

        def history(self, **kw):
            assert kw.get("prepost") is True
            return bars

    monkeypatch.setitem(sys.modules, "yfinance", types.SimpleNamespace(Ticker=_T))
    out = fetch_channel1._premarket_pct("XLE", "2026-09-10")
    assert out["available"] and out["prev_close"] == 100.0 and out["last"] == 98.0
    assert out["pct_vs_prev_close"] == pytest.approx(-2.0)
    # no pre-market prints today -> unavailable, never a fake number
    none = fetch_channel1._premarket_pct("XLE", "2026-09-11")
    assert none["available"] is False


def test_replay_harness_smoke():
    """Every policy scores, v2 beats every naive baseline on both boards,
    and no future information leaks in (pre-open snapshots only)."""
    from src import replay_harness as rh
    snaps = rh.preopen_snapshots()
    assert all(rh._fetched_hhmm(s) < rh.PREOPEN_CUTOFF for s in snaps.values())
    out = rh.run(write=False)
    v2 = "v2 engine (anchor + skill-weighted LLM)"
    for scope in ("general", "sectors"):
        pols = out[scope]["policies"]
        assert pols[v2]["n"] > 0
        v2_rate = pols[v2]["dir"] / pols[v2]["n"]
        for name, p in pols.items():
            if name.startswith("baseline") and p["n"]:
                assert v2_rate > p["dir"] / p["n"], (scope, name)
        leg = pols["legacy engine (as shipped)"]
        assert v2_rate > leg["dir"] / leg["n"]
