"""Leak + Cyrus fee-bar tests for catalogue combo KEEP."""
from __future__ import annotations

import os
import sys
import tempfile

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)

from excel_clock_gate import (  # noqa: E402
    SAME_ROW_LEAK_ABORT, assert_feature_legal, gate_payload,
)
from catalogue_combo_keep import (  # noqa: E402
    COMBOS, FEATURE_KEYS, FORBIDDEN_FEATURE_FIELDS,
    aisle_rows, assert_atoms_legal, assert_flags_legal, build_flags,
    clock_b_asof, combo_hits, headline_from, leak_check, load_finviz_index,
    load_finviz_labels, load_oppset_flagged, name_days_from_panel,
    prior_finviz_date, score_veto, synth_oppset_row, write_board,
)
from excel_factor_mine import keep_verdict, score_hits, split_rows, time_slot  # noqa: E402
from join_post_813 import FEE_RT  # noqa: E402
from j_winrate import MIN_FIRES, WIN_BAR  # noqa: E402


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
    for col in ("H", "I"):
        try:
            assert_feature_legal("value", col, 0)
        except ValueError:
            pass
        else:
            raise AssertionError("same-row H/I must abort as features")


def test_feature_keys_exclude_close_and_same_day_tape():
    assert not (FEATURE_KEYS & FORBIDDEN_FEATURE_FIELDS)
    for name in ("H", "I", "Gap", "Change", "RelVol", "close",
                 "Performance (1 Minute)", "Performance (5 Minutes)"):
        assert name in FORBIDDEN_FEATURE_FIELDS
        assert name not in FEATURE_KEYS
    assert_flags_legal({k: False for k in FEATURE_KEYS})
    try:
        assert_flags_legal({"Gap": True})
    except ValueError as e:
        assert "LEAK" in str(e)
    else:
        raise AssertionError("Gap flag must abort")
    try:
        assert_flags_legal({"H": 0.01})
    except ValueError as e:
        assert "LEAK" in str(e)
    else:
        raise AssertionError("H flag must abort")


def test_build_flags_ignore_planted_same_day_tape():
    row = {
        "last_green": True, "last_red": False, "ohlc_break_10": True,
        "ohlc_ret_5": 6.0, "ohlc_ret_1": 1.0, "ohlc_rvol": 1.1,
        "rsi": 58.0, "fv_sma20": 3.0, "rsi_os": False, "rsi_ob": False,
        "ohlc_nr7": False, "news_box": "good", "news_prior": "good",
        "erd_earn_react": True, "erd_days_since_E": 2,
        "boxes": {"sector": "good", "peer": "good", "catal": "good"},
        "sources": ["yday_gainer", "ohlc_hot"],
        "open": 10.0, "close": 12.0,  # labels — must not enter flags
        "H": 0.2, "I": 0.3, "Gap": 8.0, "Change": 19.0, "RelVol": 12.0,
    }
    fv = {
        "sector": "Technology", "perf_week": 4.0, "sma20": 3.0, "rsi": 58.0,
        "rvol": 1.1, "eps_surp": 8.0, "rev_surp": 2.0, "insider_txn": 1.2,
        "cash_sh": 3.0, "current_ratio": 1.8, "de": 0.4,
        "short_float": 4.0, "shortable": True,
        "news_title": "Acme raises full-year EPS guidance after beat",
    }
    flags = build_flags(row, fv, {"cash_sh": 2.0}, {"Technology": 1.5})
    assert "H" not in flags and "I" not in flags
    assert "Gap" not in flags and "RelVol" not in flags
    assert flags["fresh_pos_catalyst"] is True
    assert flags["raised_guidance"] is True
    assert flags["completed_breakout"] is True
    # Plant a huge same-day gap/change on the row — flags must not flip
    # because build_flags never reads those keys.
    row2 = dict(row, Gap=80.0, Change=-40.0, RelVol=50.0, close=1.0)
    flags2 = build_flags(row2, fv, {"cash_sh": 2.0}, {"Technology": 1.5})
    assert flags2 == flags


def test_fee_bar_keep_fail():
    assert MIN_FIRES == 30
    assert WIN_BAR == 0.55
    assert FEE_RT == 0.0015
    v, why = keep_verdict(30, 17 / 30)
    assert v == "KEEP" and "56.7%" in why
    v29, why29 = keep_verdict(29, 1.0)
    assert v29 == "FAIL" and "thin" in why29
    v55, _ = keep_verdict(30, 0.55)
    assert v55 == "FAIL"
    v0, _ = keep_verdict(0, None)
    assert v0 == "FAIL"


def test_lift_only_never_keep():
    v, why = keep_verdict(371, 0.48)
    assert v == "FAIL"
    assert "lift" not in why.lower()


def test_after_fee_long_and_short_pay_fee():
    win = score_hits([{"net": 0.0020 - FEE_RT}])
    lose = score_hits([{"net": 0.0010 - FEE_RT}])
    assert win["n_pos"] == 1 and lose["n_pos"] == 0
    # Short a +20 bp up-day is a loss after the fee.
    short_lose = score_hits([{"net": -0.0020 - FEE_RT}])
    short_win = score_hits([{"net": -(-0.0100) - FEE_RT}])
    assert short_lose["n_pos"] == 0
    assert short_win["n_pos"] == 1


def test_time_split_fail_closed():
    rows = [
        {"date": "2026-08-14", "net": 0.01, "flags": {"mod_mom": True}},
        {"date": "2026-09-08", "net": 0.01, "flags": {"mod_mom": True}},
        {"date": "2026-09-18", "net": -0.01, "flags": {"mod_mom": True}},
        {"date": "", "net": 0.99, "flags": {"mod_mom": True}},
        {"date": None, "net": 0.99, "flags": {"mod_mom": True}},
    ]
    disc, hold = split_rows(rows, "2026-09-08")
    assert [r["date"] for r in disc] == ["2026-08-14"]
    assert [r["date"] for r in hold] == ["2026-09-08", "2026-09-18"]
    assert time_slot("", "2026-09-08") is None
    assert time_slot(None, "2026-09-08") is None
    assert all(r.get("date") for r in disc)


def test_combo_and_is_intersection():
    rows = [
        {"net": 0.01, "net_short": -0.01 - FEE_RT,
         "flags": {"fresh_pos_catalyst": True, "limited_extension": True}},
        {"net": 0.02, "net_short": -0.02 - FEE_RT,
         "flags": {"fresh_pos_catalyst": True, "limited_extension": False}},
        {"net": -0.01, "net_short": 0.01 - FEE_RT,
         "flags": {"fresh_pos_catalyst": False, "limited_extension": True}},
    ]
    hits = combo_hits(rows, ("fresh_pos_catalyst", "limited_extension"))
    assert len(hits) == 1
    assert hits[0]["net"] == 0.01


def test_aisle_drops_flatten_only_days():
    panel = {"rows": [
        {"date": "2026-08-13", "ticker": "AAA", "sources": ["flatten"],
         "open": 10, "close": 11},
        {"date": "2026-08-14", "ticker": "BBB",
         "sources": ["flatten", "yday_gainer"], "open": 10, "close": 11},
        {"date": "2026-08-27", "ticker": "CCC",
         "sources": ["flatten", "mover_buy"], "open": 10, "close": 11},
    ]}
    rows, dates, skipped, stats = aisle_rows(panel, oppset_by_date={})
    assert dates == ["2026-08-14"]
    assert "2026-08-13" in skipped and "2026-08-27" in skipped
    assert [r["ticker"] for r in rows] == ["BBB"]
    assert stats["n_oppset_only"] == 0


def test_aisle_unions_oppset_not_flatten_only():
    """Oppset ∪ multi-src panel. Flatten-only day without oppset stays out."""
    panel = {"rows": [
        {"date": "2026-08-13", "ticker": "HOT4", "sources": ["flatten"],
         "open": 10, "close": 11},
        {"date": "2026-08-14", "ticker": "BBB",
         "sources": ["yday_gainer"], "open": 10, "close": 11},
    ]}
    oppset = {
        "2026-08-14": [{
            "join_morning": "2026-08-14", "finviz_asof": "2026-08-13",
            "ticker": "NEW", "any_opp": "1", "change_pct": "6.0",
            "pweek": "4.0", "rvol": "2.4",
        }],
    }
    rows, dates, skipped, stats = aisle_rows(panel, oppset_by_date=oppset)
    assert dates == ["2026-08-14"]
    assert "2026-08-13" in skipped
    ticks = {r["ticker"] for r in rows}
    assert ticks == {"BBB", "NEW"}
    assert stats["n_oppset_only"] == 1
    new = next(r for r in rows if r["ticker"] == "NEW")
    assert new["sources"] == ["oppset_clock_b"]
    assert new["finviz_asof"] == "2026-08-13"
    assert new["news_export_date"] == "2026-08-13"
    assert "yday_gainer" in next(r for r in rows if r["ticker"] == "BBB")["sources"]


def test_oppset_asof_must_be_tminus1():
    try:
        synth_oppset_row({
            "join_morning": "2026-09-18", "finviz_asof": "2026-09-18",
            "ticker": "LEAK", "any_opp": "1",
        })
    except ValueError as e:
        assert "LEAK" in str(e)
    else:
        raise AssertionError("same-row oppset asof must abort")
    with tempfile.TemporaryDirectory() as td:
        path = os.path.join(td, "oppset_flagged.csv")
        open(path, "w", encoding="utf-8").write(
            "join_morning,finviz_asof,ticker,any_opp\n"
            "2026-09-18,2026-09-18,LEAK,1\n"
        )
        try:
            load_oppset_flagged(path)
        except ValueError as e:
            assert "LEAK" in str(e)
        else:
            raise AssertionError("flagged CSV same-row asof must abort")


def test_oppset_tminus1_not_same_day_gap():
    """Oppset change/gap/rvol are T−1 aisle context, not same-day flags."""
    rec = {
        "join_morning": "2026-09-18", "finviz_asof": "2026-09-17",
        "ticker": "AAA", "any_opp": "1", "change_pct": "8.0",
        "pweek": "5.0", "rvol": "3.2", "gap_pct": "4.0",
    }
    row = synth_oppset_row(rec)
    assert row["ohlc_ret_1"] == 8.0
    assert row["last_green"] is True
    flags = build_flags(row, {
        "sector": "Tech", "perf_week": 5.0, "sma20": 2.0, "rsi": 55.0,
        "rvol": 3.2, "shortable": True, "news_title": "",
    }, {}, {"Tech": -1.0})
    assert "Gap" not in flags and "RelVol" not in flags and "Change" not in flags
    assert flags["mod_mom"] is True  # pweek 5 in 2–15, rsi 55


def test_finviz_labels_use_price_as_close():
    """Elite dumps have Open + Price, not Close. Labels only."""
    with tempfile.TemporaryDirectory() as td:
        open(os.path.join(td, "finviz_2026-09-18.csv"), "w", encoding="utf-8").write(
            "Ticker,Open,Price,Gap,Change,Relative Volume\n"
            "AAA,10.00,10.20,8.0,19.0,12.0\n"
        )
        lab = load_finviz_labels("2026-09-18", td)
        assert lab["AAA"]["open"] == 10.0
        assert lab["AAA"]["close"] == 10.20
        assert "Gap" not in lab["AAA"]


def test_prior_finviz_never_same_session():
    assert prior_finviz_date("2026-09-18", ["2026-09-17", "2026-09-18"]) == "2026-09-17"
    assert clock_b_asof("2026-09-18", "2026-09-17") == "2026-09-17"
    try:
        clock_b_asof("2026-09-18", "2026-09-18")
    except ValueError as e:
        assert "LEAK" in str(e)
    else:
        raise AssertionError("same-day finviz_asof must abort")
    # stamped same-day is a leak, not a walk-back
    try:
        prior_finviz_date(
            "2026-09-18", ["2026-09-16", "2026-09-17"],
            {"news_export_date": "2026-09-18", "prior_date": "2026-09-17"},
        )
    except ValueError as e:
        assert "LEAK" in str(e)
    else:
        raise AssertionError("same-day news_export_date must abort")
    got = prior_finviz_date(
        "2026-09-18", ["2026-09-16", "2026-09-17"],
        {"finviz_asof": "2026-09-17", "join_morning": "2026-09-18"},
    )
    assert got == "2026-09-17"


def test_join_uses_asof_not_neighbor_or_same_day_gap():
    """Features come from finviz_asof (T−1). T Gap / minute Performance stay out."""
    with tempfile.TemporaryDirectory() as td:
        open(os.path.join(td, "finviz_2026-09-16.csv"), "w", encoding="utf-8").write(
            "Ticker,Sector,Relative Strength Index (14),News Title,"
            "Gap,Performance (5 Minutes)\n"
            "AAA,Tech,30.0,OLD TITLE,1.0,9.0\n"
        )
        open(os.path.join(td, "finviz_2026-09-17.csv"), "w", encoding="utf-8").write(
            "Ticker,Sector,Relative Strength Index (14),News Title,"
            "Gap,Performance (5 Minutes)\n"
            "AAA,Tech,58.0,Acme raises full-year EPS guidance after beat,2.0,8.0\n"
        )
        open(os.path.join(td, "finviz_2026-09-18.csv"), "w", encoding="utf-8").write(
            "Ticker,Open,Price,Gap,Change,Relative Volume,Performance (5 Minutes)\n"
            "AAA,10.00,10.50,80.0,19.0,12.0,7.0\n"
        )
        snap, _ = load_finviz_index("2026-09-17", td)
        assert "Gap" not in snap["AAA"]
        assert snap["AAA"]["rsi"] == 58.0
        panel = {"rows": [{
            "date": "2026-09-18", "ticker": "AAA",
            "sources": ["yday_gainer"], "open": 10, "close": 10.5,
            "news_export_date": "2026-09-16", "prior_date": "2026-09-16",
            "last_green": True, "rsi": 58.0, "fv_sma20": 2.0,
            "ohlc_ret_5": 5.0, "ohlc_break_10": False,
        }]}
        oppset = {"2026-09-18": [{
            "join_morning": "2026-09-18", "finviz_asof": "2026-09-17",
            "ticker": "AAA", "any_opp": "1", "change_pct": "3.0",
            "pweek": "5.0", "rvol": "1.2",
        }]}
        rows, _, _, _ = name_days_from_panel(
            panel, export_dir=td, oppset_by_date=oppset,
        )
        assert len(rows) == 1
        fl = rows[0]["flags"]
        assert "Gap" not in fl
        assert fl["raised_guidance"] is True  # 09-17 title, not 09-16
        assert fl["mod_mom"] is True


def test_name_days_abort_same_row_finviz(tmp_path=None):
    """Same-session Finviz date must abort (would leak Change/Gap/RelVol)."""
    panel = {
        "lookback": "full_session_cal",
        "rows": [{
            "date": "2026-09-18", "ticker": "AAA",
            "sources": ["yday_gainer"], "open": 10, "close": 11,
            "news_export_date": "2026-09-18", "prior_date": "2026-09-18",
            "last_green": True,
        }],
    }
    try:
        name_days_from_panel(panel, export_dir="/tmp/no_such_exports")
    except ValueError as e:
        # no same-row date available in empty export list → prior is None
        # (no abort). Plant fv_dates via a real same-row file:
        pass
    with tempfile.TemporaryDirectory() as td:
        path = os.path.join(td, "finviz_2026-09-18.csv")
        open(path, "w", encoding="utf-8").write("Ticker,Sector\nAAA,Tech\n")
        panel["rows"][0]["news_export_date"] = "2026-09-18"
        panel["rows"][0]["prior_date"] = "2026-09-18"
        try:
            name_days_from_panel(panel, export_dir=td, oppset_by_date={})
        except ValueError as e:
            assert "LEAK" in str(e)
        else:
            raise AssertionError("same-row Finviz must abort")


def test_veto_keep_requires_bar_and_lift():
    hold = [{"net": 0.01, "flags": {"extreme_ext": False, "diminishing": False,
                                    "failed_breakout": False}}] * 20
    hold += [{"net": -0.02, "flags": {"extreme_ext": True, "diminishing": True,
                                      "failed_breakout": True}}] * 20
    # complement n=20 thin even if WR=100%
    base = score_hits(hold)
    veto = score_veto(hold, ("extreme_ext", "diminishing", "failed_breakout"), base)
    assert veto["verdict"] == "FAIL"
    assert "thin" in veto["why"] or veto["complement"]["n"] < 30

    keepers = [{"net": 0.01, "flags": {"extreme_ext": False, "diminishing": False,
                                       "failed_breakout": False}}] * 30
    dead = [{"net": -0.02, "flags": {"extreme_ext": True, "diminishing": True,
                                     "failed_breakout": True}}] * 10
    hold2 = keepers + dead
    base2 = score_hits(hold2)
    veto2 = score_veto(hold2, ("extreme_ext", "diminishing", "failed_breakout"), base2)
    assert veto2["verdict"] == "KEEP"
    assert veto2["lifts_baseline"] is True

    # lift without clearing 55% is FAIL
    mixed = [{"net": 0.01, "flags": {"extreme_ext": False, "diminishing": False,
                                     "failed_breakout": False}}] * 16
    mixed += [{"net": -0.01, "flags": {"extreme_ext": False, "diminishing": False,
                                       "failed_breakout": False}}] * 14
    mixed += [{"net": -0.05, "flags": {"extreme_ext": True, "diminishing": True,
                                       "failed_breakout": True}}] * 10
    base3 = score_hits(mixed)
    veto3 = score_veto(mixed, ("extreme_ext", "diminishing", "failed_breakout"), base3)
    assert veto3["verdict"] == "FAIL"
    assert "lift" in veto3["why"].lower() or veto3["complement"]["wr"] <= WIN_BAR


def test_ten_combos_declared():
    assert len(COMBOS) == 10
    assert [c["id"] for c in COMBOS] == list(range(1, 11))
    assert COMBOS[7]["key"] == "c8_squeeze" and COMBOS[7]["role"] == "need"
    assert COMBOS[9]["key"] == "c10_kronos" and COMBOS[9]["role"] == "need"
    assert COMBOS[4]["role"] == "veto"


def test_no_flatten_import():
    path = os.path.join(HERE, "catalogue_combo_keep.py")
    text = open(path, encoding="utf-8").read()
    assert "flatten_robust" in text
    assert "from flatten" not in text
    assert "import flatten" not in text
    assert "from flatten_robust" not in text
    assert "import factor_mine" not in text
    assert "from factor_mine" not in text


def test_fail_headline_prefers_material_n():
    scored = [
        {"key": "c7_insider_recovery", "verdict": "FAIL",
         "hold": {"n": 3, "wr": 1.0}},
        {"key": "c2_room", "verdict": "FAIL",
         "hold": {"n": 80, "wr": 0.512}},
        {"key": "c8_squeeze", "verdict": "NEED",
         "hold": {"n": 0, "wr": None}},
    ]
    hl = headline_from(scored, "2026-09-08")
    assert hl["verdict"] == "FAIL"
    assert hl["best"]["key"] == "c2_room"
    assert "51.2%" in hl["text"]


def test_board_says_fail_plainly():
    scored = [{
        "id": 2, "key": "c2_room", "title": "room", "kind": "combo",
        "atoms": ["fresh_pos_catalyst", "limited_extension"],
        "thesis": "room", "side": "long", "role": "direction",
        "status": "have", "have": "news", "need": "",
        "disc": {"n": 100, "wr": 0.49, "mean_net": 0.0, "verdict": "FAIL",
                 "why": "n=100, after-fee WR 49.0% ≤ 55%"},
        "hold": {"n": 80, "wr": 0.512, "mean_net": 0.0, "verdict": "FAIL",
                 "why": "n=80, after-fee WR 51.2% ≤ 55%"},
        "verdict": "FAIL", "why": "n=80, after-fee WR 51.2% ≤ 55%",
        "walk": [], "note": "",
    }]
    hl = headline_from(scored, "2026-09-08")
    assert hl["verdict"] == "FAIL"
    payload = {
        "status": "DONE", "cutoff": "2026-09-08", "n_aisle_dates": 10,
        "n_rows": 100, "headline": hl, "scored": scored,
        "gate": gate_payload(), "leak": "PASS", "lookback": "full_session_cal",
        "baseline_hold": {"n": 200, "wr": 0.44, "mean_net": -0.002},
        "n_need": 2, "n_fail": 8, "panel_to": "2026-09-18", "panel_n": 1969,
        "aisle_dates": ["2026-08-14"], "skipped_days": {},
    }
    with tempfile.TemporaryDirectory() as td:
        path = os.path.join(td, "CATALOGUE_COMBO_KEEP.md")
        write_board(payload, path)
        md = open(path, encoding="utf-8").read()
    assert "FAIL" in md
    assert "KEEP bar" in md
    assert "flatten_robust" in md
    assert "Lift-only is never KEEP" in md or "lift-only is never KEEP" in md.lower()


if __name__ == "__main__":
    test_clock_and_leak_abort()
    test_feature_keys_exclude_close_and_same_day_tape()
    test_build_flags_ignore_planted_same_day_tape()
    test_fee_bar_keep_fail()
    test_lift_only_never_keep()
    test_after_fee_long_and_short_pay_fee()
    test_time_split_fail_closed()
    test_combo_and_is_intersection()
    test_aisle_drops_flatten_only_days()
    test_aisle_unions_oppset_not_flatten_only()
    test_oppset_asof_must_be_tminus1()
    test_oppset_tminus1_not_same_day_gap()
    test_finviz_labels_use_price_as_close()
    test_prior_finviz_never_same_session()
    test_join_uses_asof_not_neighbor_or_same_day_gap()
    test_name_days_abort_same_row_finviz()
    test_veto_keep_requires_bar_and_lift()
    test_ten_combos_declared()
    test_no_flatten_import()
    test_fail_headline_prefers_material_n()
    test_board_says_fail_plainly()
    print("ok")
