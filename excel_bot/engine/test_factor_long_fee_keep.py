"""Leak + Cyrus fee-bar tests for factor-mine long fee KEEP."""
from __future__ import annotations

import os
import sys
import tempfile

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(os.path.dirname(HERE))
sys.path.insert(0, HERE)
sys.path.insert(0, REPO)

from excel_clock_gate import SAME_ROW_LEAK_ABORT, assert_feature_legal  # noqa: E402
from excel_factor_mine import keep_verdict, score_hits, split_rows, time_slot  # noqa: E402
from join_post_813 import FEE_RT  # noqa: E402
from j_winrate import MIN_FIRES, WIN_BAR  # noqa: E402
from src import clock_b_tells as cbt  # noqa: E402
from factor_long_fee_keep import (  # noqa: E402
    FEATURE_KEYS, RECIPES, assert_atoms_legal, board_verdict,
    combo_claims, feature_row, fire_e_fresh, fire_join_vol_green,
    fire_short_news_r, headline_from, name_days_from_aisle, on_oppset,
    recipe_defs_from_wire, recipe_fires, recipe_hits, score_recipe,
    write_board,
)
from catalogue_combo_keep import (  # noqa: E402
    FORBIDDEN_FEATURE_FIELDS, aisle_rows, leak_check,
)


def _feat(**kw):
    base = {
        "last_green": True, "last_red": False, "ohlc_break_10": True,
        "ohlc_nr7": False, "ohlc_ret_5": 4.0, "ohlc_ret_1": 1.2,
        "ohlc_rvol": 1.1, "macd_up": True, "macd_down": False,
        "candle_capture": False, "rsi_ob": False, "rsi_os": False,
        "alarm": False, "boxes": {"peer": "good", "sector": "neutral",
                                  "join": "missing", "vol": "missing",
                                  "news": "missing"},
        "sources": ["yday_gainer", "ohlc_hot"],
        "erd_days_since_E": None, "erd_flag_E": None,
    }
    base.update(kw)
    return base


def _row(net=0.01, *, oppset=False, alarm=False, feats=None, **kw):
    rec = {
        "date": kw.pop("date", "2026-09-11"),
        "ticker": kw.pop("ticker", "AAA"),
        "net": net,
        "net_short": kw.pop("net_short", -net - 2 * FEE_RT if net else None),
        "oppset": oppset,
        "alarm": alarm,
        "feats": feats if feats is not None else _feat(),
    }
    rec.update(kw)
    return rec


def _spec(name):
    return next(r for r in RECIPES if r["name"] == name)


def test_clock_and_leak_abort():
    clocks, leak = leak_check()
    assert leak == "PASS"
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


def test_feature_keys_exclude_close_and_same_day_tape():
    assert not (FEATURE_KEYS & FORBIDDEN_FEATURE_FIELDS)
    assert not (FEATURE_KEYS & cbt.LEAK_FIELDS)
    for name in ("H", "I", "Gap", "Change", "RelVol", "close",
                 "Performance (1 Minute)", "Performance (5 Minutes)"):
        assert name in FORBIDDEN_FEATURE_FIELDS
        assert name not in FEATURE_KEYS


def test_feature_row_strips_leaks_and_stamps():
    raw = {
        "last_green": True, "ohlc_ret_5": 4.0, "ohlc_break_10": True,
        "boxes": {"peer": "good"}, "open": 10.0, "close": 12.0,
        "H": 0.2, "Gap": 8.0, "Change": 19.0, "RelVol": 12.0,
        "change_pct": 19.4, "vwap": 11.2,
        "clk_mom_break_peer": True, "_clock_b": True,
        "erd_days_since_E": 0, "erd_flag_E": 1,
    }
    feats = feature_row(raw)
    assert "H" not in feats and "Gap" not in feats
    assert "close" not in feats and "open" not in feats
    assert "change_pct" not in feats and "vwap" not in feats
    assert "clk_mom_break_peer" not in feats
    assert "_clock_b" not in feats
    assert feats["ohlc_ret_5"] == 4.0
    assert feats["erd_days_since_E"] == 0
    raw2 = dict(raw, Gap=80.0, Change=-40.0, RelVol=50.0, close=1.0)
    assert feature_row(raw2) == feats


def test_combo_true_ignores_planted_same_day_tape():
    row = _feat()
    a = {k: cbt.combo_true(row, k) for k in (
        "clk_mom_break_peer", "clk_nr7_mom", "clk_hold_vs_sector",
        "clk_ext_veto",
    )}
    flipped = dict(row, Gap=-9, Change=-40, RelVol=0.1, change_pct=-99,
                   vwap=0.0, close=1.0, H=0.9)
    b = {k: cbt.combo_true(flipped, k) for k in a}
    assert a == b
    assert a["clk_mom_break_peer"] is True


def test_fee_bar_keep_fail_thin():
    assert MIN_FIRES == 30
    assert WIN_BAR == 0.55
    assert FEE_RT == 0.0015
    v, why = board_verdict(30, 17 / 30)
    assert v == "KEEP" and "56.7%" in why
    v29, why29 = board_verdict(29, 1.0)
    assert v29 == "thin-n" and "thin" in why29
    kv, _ = keep_verdict(29, 1.0)
    assert kv == "FAIL"
    v55, _ = board_verdict(30, 0.55)
    assert v55 == "FAIL"
    v0, _ = board_verdict(0, None)
    assert v0 == "FAIL"


def test_lift_only_never_keep():
    v, why = board_verdict(371, 0.48)
    assert v == "FAIL"
    assert "lift" not in why.lower()


def test_after_fee_long_and_short_pay_fee():
    win = score_hits([{"net": 0.0020 - FEE_RT}])
    lose = score_hits([{"net": 0.0010 - FEE_RT}])
    assert win["n_pos"] == 1 and lose["n_pos"] == 0
    short_lose = score_hits([{"net": -0.0020 - FEE_RT}])
    short_win = score_hits([{"net": -(-0.0100) - FEE_RT}])
    assert short_lose["n_pos"] == 0
    assert short_win["n_pos"] == 1


def test_time_split_fail_closed():
    rows = [
        {"date": "2026-08-14", "net": 0.01},
        {"date": "2026-09-08", "net": 0.01},
        {"date": "2026-09-18", "net": -0.01},
        {"date": "", "net": 0.99},
        {"date": None, "net": 0.99},
    ]
    disc, hold = split_rows(rows, "2026-09-08")
    assert [r["date"] for r in disc] == ["2026-08-14"]
    assert [r["date"] for r in hold] == ["2026-09-08", "2026-09-18"]
    assert time_slot("", "2026-09-08") is None
    assert time_slot(None, "2026-09-08") is None


def test_recipe_wire_matches_set():
    wire = recipe_defs_from_wire()
    assert set(wire) == {r["name"] for r in RECIPES}
    assert len(RECIPES) == 6
    assert [r["name"] for r in RECIPES] == [
        "union_e_fresh_h3",
        "combo_se_5050_skip",
        "combo_ej_5050_shared",
        "union_clk_mom_break_peer_h1",
        "union_clk_hold_vs_sector_h1",
        "union_clk_nr7_mom_opp_h1",
    ]
    e = wire["union_e_fresh_h3"]
    assert e["require"] == {"days_since_E_max": 1, "flag_E_min": 0}
    assert e["forbid"]["alarm"] is True
    se = wire["combo_se_5050_skip"]
    assert se["members"] == ["short_news_r_h3", "union_e_fresh_h3"]
    assert se["net"] == "skip"
    assert se["side"] == "mix"
    ej = wire["combo_ej_5050_shared"]
    assert ej["members"] == ["union_e_fresh_h3", "union_join_vol_green_h1"]
    assert ej["side"] == "long"
    a = wire["union_clk_mom_break_peer_h1"]
    assert a["require"] == {"clk_mom_break_peer": True}
    assert "oppset" not in a["require"]
    assert a["forbid"]["clk_ext_veto"] is True
    b = wire["union_clk_hold_vs_sector_h1"]
    assert b["require"] == {"clk_hold_vs_sector": True}
    assert "oppset" not in b["require"]
    c = wire["union_clk_nr7_mom_opp_h1"]
    assert c["require"] == {"clk_nr7_mom": True, "oppset": True}


def test_e_fresh_and_cash_gates():
    hit = _row(feats=_feat(erd_days_since_E=0, erd_flag_E=1))
    assert fire_e_fresh(hit) is True
    assert recipe_fires(hit, _spec("union_e_fresh_h3")) is True
    assert fire_e_fresh(_row(feats=_feat(erd_days_since_E=2, erd_flag_E=1))) is False
    assert fire_e_fresh(_row(feats=_feat(erd_days_since_E=0, erd_flag_E=-1))) is False
    assert fire_e_fresh(_row(alarm=True, feats=_feat(
        erd_days_since_E=0, erd_flag_E=1, alarm=True))) is False
    assert fire_e_fresh(_row(feats=_feat())) is False

    jvg = _feat(boxes={"join": "good", "vol": "good", "news": "missing"},
                last_green=True)
    assert fire_join_vol_green(_row(feats=jvg)) is True
    bad_news = _feat(boxes={"join": "good", "vol": "good", "news": "bad"},
                     last_green=True)
    assert fire_join_vol_green(_row(feats=bad_news)) is False
    assert fire_short_news_r(_row(feats=_feat(boxes={"news": "bad"}))) is True
    assert fire_short_news_r(_row(feats=_feat(boxes={"news": "good"}))) is False


def test_combo_skip_and_claim():
    se = _spec("combo_se_5050_skip")
    ej = _spec("combo_ej_5050_shared")
    both = _row(feats=_feat(
        erd_days_since_E=0, erd_flag_E=1,
        boxes={"news": "bad", "join": "good", "vol": "good"},
        last_green=True,
    ))
    assert combo_claims(both, se) == []
    assert recipe_fires(both, se) is False
    only_e = _row(feats=_feat(erd_days_since_E=0, erd_flag_E=1,
                              boxes={"news": "missing"}))
    assert combo_claims(only_e, se) == [("union_e_fresh_h3", "long")]
    only_s = _row(feats=_feat(boxes={"news": "bad"}))
    assert combo_claims(only_s, se) == [("short_news_r_h3", "short")]
    # Both longs fire: e_fresh wins claim.
    both_long = _row(feats=_feat(
        erd_days_since_E=0, erd_flag_E=1, last_green=True,
        boxes={"join": "good", "vol": "good", "news": "missing"},
    ))
    assert combo_claims(both_long, ej) == [("union_e_fresh_h3", "long")]
    only_j = _row(feats=_feat(
        last_green=True,
        boxes={"join": "good", "vol": "good", "news": "missing"},
    ))
    assert combo_claims(only_j, ej) == [("union_join_vol_green_h1", "long")]


def test_clock_b_need_opp_and_forbid_veto():
    spec4 = _spec("union_clk_mom_break_peer_h1")
    assert recipe_fires(_row(oppset=False, feats=_feat()), spec4) is True
    veto = _feat(ohlc_ret_5=16.0, last_green=True, ohlc_break_10=True,
                 last_red=True, rsi_ob=True)
    assert cbt.clk_ext_veto(veto) is True
    assert recipe_fires(_row(feats=veto), spec4) is False
    assert recipe_fires(_row(alarm=True, feats=_feat()), spec4) is False

    spec5 = _spec("union_clk_hold_vs_sector_h1")
    hold = _feat(boxes={"sector": "bad"}, last_green=True, ohlc_break_10=False)
    assert recipe_fires(_row(oppset=False, feats=hold), spec5) is True

    spec6 = _spec("union_clk_nr7_mom_opp_h1")
    nr7 = _feat(ohlc_nr7=True, ohlc_break_10=False)
    assert recipe_fires(_row(oppset=True, feats=nr7), spec6) is True
    assert recipe_fires(_row(oppset=False, feats=nr7), spec6) is False


def test_on_oppset_accepts_aisle_source():
    assert on_oppset({"oppset": True}) is True
    assert on_oppset({"sources": ["oppset"]}) is True
    assert on_oppset({"sources": ["oppset_clock_b"]}) is True
    assert on_oppset({"sources": ["yday_gainer"]}) is False


def test_aisle_drops_flatten_only_days():
    panel = {"rows": [
        {"date": "2026-08-13", "ticker": "AAA", "sources": ["flatten"],
         "open": 10, "close": 11},
        {"date": "2026-08-14", "ticker": "BBB",
         "sources": ["flatten", "yday_gainer"], "open": 10, "close": 11},
    ]}
    rows, dates, skipped, stats = aisle_rows(panel, oppset_by_date={})
    assert dates == ["2026-08-14"]
    assert "2026-08-13" in skipped
    assert stats["n_oppset_only"] == 0


def test_name_days_abort_same_row_finviz():
    panel = {"rows": [
        {"date": "2026-08-14", "ticker": "BBB",
         "sources": ["yday_gainer"], "open": 10, "close": 11,
         "finviz_asof": "2026-08-14"},
    ]}
    try:
        name_days_from_aisle(panel, oppset_by_date={})
    except ValueError as e:
        assert "LEAK" in str(e)
    else:
        raise AssertionError("same-row finviz_asof must abort")


def test_recipe_hits_short_pays_fee():
    se = _spec("combo_se_5050_skip")
    rows = [
        _row(0.02, feats=_feat(erd_days_since_E=0, erd_flag_E=1),
             ticker="EEE", net_short=-0.02 - FEE_RT),
        _row(0.02, feats=_feat(boxes={"news": "bad"}),
             ticker="SSS", net_short=-0.02 - FEE_RT),
        _row(0.02, feats=_feat(erd_days_since_E=0, erd_flag_E=1,
                               boxes={"news": "bad"}),
             ticker="XX", net_short=-0.02 - FEE_RT),
    ]
    hits = recipe_hits(rows, se)
    assert [h["ticker"] for h in hits] == ["EEE", "SSS"]
    assert hits[0]["side"] == "long"
    assert hits[1]["side"] == "short"
    assert hits[1]["net"] == -0.02 - FEE_RT
    scored = score_recipe(rows, se)
    assert scored["n"] == 2
    assert scored["n_long"] == 1 and scored["n_short"] == 1


def test_no_flatten_or_factor_mine_import():
    path = os.path.join(HERE, "factor_long_fee_keep.py")
    text = open(path, encoding="utf-8").read()
    assert "flatten_robust" in text
    assert "from flatten" not in text
    assert "import flatten" not in text
    assert "from flatten_robust" not in text
    assert "import factor_mine" not in text.replace("not import factor_mine", "")
    assert "from factor_mine" not in text
    assert "from src import factor_mine" not in text
    assert "from src.factor_mine" not in text
    assert "\nimport factor_mine" not in text
    assert "\nfrom src import factor_mine" not in text
    assert "from src.factor_mine_combo" not in text


def test_fail_headline_says_goal_b():
    scored = [{
        "name": "union_e_fresh_h3", "verdict": "FAIL", "side": "long",
        "hold": {"n": 40, "wr": 0.45},
    }, {
        "name": "combo_se_5050_skip", "verdict": "KEEP", "side": "mix",
        "hold": {"n": 80, "wr": 0.60}, "why": "n=80, after-fee WR 60.0% (>55%)",
    }, {
        "name": "union_clk_nr7_mom_opp_h1", "verdict": "thin-n",
        "side": "long", "hold": {"n": 8, "wr": 0.75},
    }, {
        "name": "union_clk_mom_break_peer_h1", "verdict": "FAIL",
        "side": "long", "hold": {"n": 90, "wr": 0.50},
    }, {
        "name": "union_clk_hold_vs_sector_h1", "verdict": "FAIL",
        "side": "long", "hold": {"n": 200, "wr": 0.46},
    }, {
        "name": "combo_ej_5050_shared", "verdict": "FAIL", "side": "long",
        "hold": {"n": 120, "wr": 0.44},
    }]
    hl = headline_from(scored, "2026-09-10")
    assert hl["verdict"] == "FAIL"
    assert hl["n_keep"] == 0
    assert "FAIL vs goal (b)" in hl["text"]
    assert "Cash Book%" in hl["text"]
    assert "mix KEEP" in hl["text"].lower() or "Mix KEEP" in hl["text"]
    assert "union_clk_mom_break_peer_h1" in hl["text"] or "Best near-miss" in hl["text"]


def test_keep_headline_requires_long():
    scored = [{
        "name": "union_e_fresh_h3", "verdict": "KEEP", "side": "long",
        "hold": {"n": 40, "wr": 0.60}, "why": "n=40, after-fee WR 60.0% (>55%)",
    }, {
        "name": "combo_se_5050_skip", "verdict": "FAIL", "side": "mix",
        "hold": {"n": 80, "wr": 0.40},
    }]
    hl = headline_from(scored, "2026-09-10")
    assert hl["verdict"] == "KEEP"
    assert hl["n_keep"] == 1
    assert "KEEP vs goal (b)" in hl["text"]
    assert "`union_e_fresh_h3`" in hl["text"]


def test_board_says_fail_plainly():
    scored = [{
        "id": 1, "group": "A", "name": "union_e_fresh_h3",
        "kind": "cash", "clk": None, "need_opp": False,
        "title": "e_fresh", "thesis": "earn", "have": "ERD", "note": "",
        "side": "long", "min_hold": 3, "members": [], "net": None,
        "cash_book_pct": 38.46, "cash_starts": "25/26",
        "goal_b_eligible": True,
        "disc": {"n": 80, "wr": 0.44, "mean_net": 0.0, "verdict": "FAIL",
                 "why": "n=80, after-fee WR 44.0% ≤ 55%"},
        "hold": {"n": 40, "wr": 0.45, "mean_net": 0.0, "n_pos": 18,
                 "verdict": "FAIL",
                 "why": "n=40, after-fee WR 45.0% ≤ 55%"},
        "verdict": "FAIL", "why": "n=40, after-fee WR 45.0% ≤ 55%",
        "walk": [],
    }]
    from excel_clock_gate import gate_payload
    hl = headline_from(scored, "2026-09-10")
    payload = {
        "status": "DONE", "cutoff": "2026-09-10", "n_aisle_dates": 10,
        "n_rows": 100, "headline": hl, "scored": scored,
        "gate": gate_payload(), "leak": "PASS", "lookback": "full_session_cal",
        "baseline_hold": {"n": 200, "wr": 0.44, "mean_net": -0.002},
        "n_thin": 0, "n_fail": 1, "panel_to": "2026-09-18", "panel_n": 2542,
        "aisle_dates": ["2026-08-14"], "skipped_days": {},
    }
    with tempfile.TemporaryDirectory() as td:
        path = os.path.join(td, "FACTOR_LONG_FEE_KEEP.md")
        write_board(payload, path)
        md = open(path, encoding="utf-8").read()
    assert "FAIL vs goal (b)" in md
    assert "KEEP bar" in md
    assert "flatten_robust" in md
    assert "Cash Book%" in md
    assert "thin-n" in md
    assert "Goal (b)" in md
    assert "Hold 3." in md
    assert "Hold {'n'" not in md


if __name__ == "__main__":
    test_clock_and_leak_abort()
    test_feature_keys_exclude_close_and_same_day_tape()
    test_feature_row_strips_leaks_and_stamps()
    test_combo_true_ignores_planted_same_day_tape()
    test_fee_bar_keep_fail_thin()
    test_lift_only_never_keep()
    test_after_fee_long_and_short_pay_fee()
    test_time_split_fail_closed()
    test_recipe_wire_matches_set()
    test_e_fresh_and_cash_gates()
    test_combo_skip_and_claim()
    test_clock_b_need_opp_and_forbid_veto()
    test_on_oppset_accepts_aisle_source()
    test_aisle_drops_flatten_only_days()
    test_name_days_abort_same_row_finviz()
    test_recipe_hits_short_pays_fee()
    test_no_flatten_or_factor_mine_import()
    test_fail_headline_says_goal_b()
    test_keep_headline_requires_long()
    test_board_says_fail_plainly()
    print("ok")
