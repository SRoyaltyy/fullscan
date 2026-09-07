"""Rank residual mine: leak-free flags, avoid/elevate bars, patched books."""
from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd

from src import rank_residual as rr


def _join_frame() -> pd.DataFrame:
    """Two sessions. Day 1 has Elite labels; day 2 is weather-only."""
    rows = []
    # 2026-08-20 — Elite day. Ranks 1–3 are junk; HC sits at 20–22.
    for i, (tkr, sec, ext, surp, rsi, oc, ctry) in enumerate([
        ("JUNK1", "Consumer Defensive", "neutral", "beat", "mid", -0.03, "China"),
        ("JUNK2", "Technology", None, None, None, -0.02, "USA"),
        ("JUNK3", "Financial", "neutral", "beat", "mid", -0.015, "China"),
        ("OK1", "Energy", "neutral", "beat", "mid", 0.004, "USA"),
        ("OK2", "Technology", "neutral", "beat", "mid", 0.002, "USA"),
        ("OK3", "Industrials", "washed", "big_beat", "mid", 0.006, "USA"),
        ("OK4", "Financial", "neutral", "beat", "mid", 0.001, "USA"),
        ("OK5", "Energy", "neutral", "inline", "mid", -0.001, "USA"),
        ("OK6", "Technology", "neutral", "beat", "overbought", 0.000, "USA"),
        ("OK7", "Utilities", "neutral", "miss", "mid", 0.003, "USA"),
        ("OK8", "Real Estate", "neutral", "beat", "mid", 0.002, "USA"),
        ("OK9", "Communication Services", "neutral", "beat", "mid", 0.001, "USA"),
        ("OK10", "Industrials", "neutral", "beat", "mid", 0.002, "USA"),
        ("OK11", "Energy", "extended", "beat", "mid", -0.004, "USA"),
        ("OK12", "Technology", "neutral", "beat", "mid", 0.001, "USA"),
        ("MID1", "Healthcare", "washed", "big_beat", "mid", 0.018, "USA"),
        ("MID2", "Healthcare", "neutral", "beat", "mid", 0.014, "USA"),
        ("MID3", "Healthcare", "neutral", "beat", "oversold", 0.012, "USA"),
        ("MID4", "Energy", "neutral", "beat", "mid", 0.005, "USA"),
        ("MID5", "Technology", "neutral", "inline", "mid", 0.004, "USA"),
    ], start=1):
        rows.append({
            "Ticker": tkr, "sector": sec, "industry": "x",
            "total_score": 5.0 - i * 0.1, "families_known": 12,
            "detail": "geo:ADR-China=bull" if ctry == "China" else "sector:Energy=bull",
            "ext": ext, "earnsurp": surp, "rsi": rsi,
            "session_date": "2026-08-20", "rank": i,
            "oc": oc, "px_open": 10.0, "px_close": 10.0 * (1 + oc),
            "country": ctry, "news_tone": "missing", "news_preopen": False,
            "last_green": tkr.startswith("MID"), "last_red": tkr.startswith("JUNK"),
            "spy_cc_down": False, "prior_spy_red": True,
        })
    # 2026-08-17 — no Elite columns. CD China ADRs occupy the top.
    for i, (tkr, sec, oc, ctry) in enumerate([
        ("EDU", "Consumer Defensive", -0.04, "China"),
        ("DAO", "Consumer Defensive", -0.03, "China"),
        ("GOTU", "Consumer Defensive", -0.02, "China"),
        ("RLX", "Consumer Defensive", -0.025, "China"),
        ("AAA", "Energy", 0.01, "USA"),
        ("BBB", "Healthcare", 0.02, "USA"),
        ("CCC", "Technology", 0.008, "USA"),
        ("DDD", "Industrials", 0.006, "USA"),
        ("EEE", "Financial", 0.004, "USA"),
        ("FFF", "Energy", 0.003, "USA"),
        ("GGG", "Healthcare", 0.015, "USA"),
        ("HHH", "Healthcare", 0.011, "USA"),
        ("III", "Technology", 0.002, "USA"),
        ("JJJ", "Utilities", 0.001, "USA"),
        ("KKK", "Real Estate", 0.000, "USA"),
        ("LLL", "Healthcare", 0.013, "USA"),
        ("MMM", "Energy", 0.005, "USA"),
        ("NNN", "Technology", 0.003, "USA"),
        ("OOO", "Industrials", 0.002, "USA"),
        ("PPP", "Financial", 0.001, "USA"),
    ], start=1):
        rows.append({
            "Ticker": tkr, "sector": sec, "industry": "x",
            "total_score": 3.0, "families_known": 13,
            "detail": "geo:ADR-China=bull" if ctry == "China" else "",
            "ext": None, "earnsurp": None, "rsi": None,
            "session_date": "2026-08-17", "rank": i,
            "oc": oc, "px_open": 8.0, "px_close": 8.0 * (1 + oc),
            "country": ctry, "news_tone": "missing", "news_preopen": False,
            "last_green": False, "last_red": True,
            "spy_cc_down": True, "prior_spy_red": False,
        })
    return pd.DataFrame(rows)


def test_incomplete_detects_missing_elite() -> None:
    df = _join_frame()
    mask = rr.incomplete_mask(df)
    d1 = df[df.session_date == "2026-08-20"]
    d2 = df[df.session_date == "2026-08-17"]
    assert bool(mask[d1[d1.Ticker == "JUNK2"].index[0]]) is True
    assert bool(mask[d1[d1.Ticker == "OK1"].index[0]]) is False
    assert bool(mask.loc[d2.index].all()) is True


def test_china_adr_from_detail_and_country() -> None:
    df = _join_frame()
    hit = rr.china_adr_mask(df)
    assert bool(hit[df.Ticker == "JUNK1"].iloc[0]) is True
    assert bool(hit[df.Ticker == "OK1"].iloc[0]) is False
    assert bool(hit[df.Ticker == "EDU"].iloc[0]) is True


def test_banned_features_never_keep() -> None:
    assert rr.is_banned("gap") is True
    assert rr.is_banned("Change from Open") is True
    assert rr.is_banned("relvol") is True
    assert rr.is_banned("healthcare") is False
    rows = [{
        "feature": "gap", "n": 900, "win": 0.1, "base_win": 0.5,
        "mean": -0.04, "base_mean": 0.002, "lift_pp": -40.0, "mean_edge": -0.042,
        "role": "avoid", "band": "1-15",
    }]
    assert rr.pick_avoid_keepers(rows) == []


def test_avoid_keeper_needs_worse_mean_and_lift() -> None:
    rows = [
        {"feature": "incomplete", "n": 50, "win": 0.40, "base_win": 0.52,
         "mean": -0.01, "base_mean": 0.002, "lift_pp": -12.0, "mean_edge": -0.012},
        {"feature": "last_red", "n": 50, "win": 0.40, "base_win": 0.52,
         "mean": 0.003, "base_mean": 0.002, "lift_pp": -12.0, "mean_edge": 0.001},
        {"feature": "last_green", "n": 57, "win": 0.47, "base_win": 0.52,
         "mean": 0.0017, "base_mean": 0.00172, "lift_pp": -4.5, "mean_edge": -0.00002},
        {"feature": "thin", "n": 12, "win": 0.10, "base_win": 0.52,
         "mean": -0.05, "base_mean": 0.002, "lift_pp": -42.0, "mean_edge": -0.052},
    ]
    keep = rr.pick_avoid_keepers(rows)
    assert [k["feature"] for k in keep] == ["incomplete"]


def test_elevate_keeper_needs_lift_and_positive_mean() -> None:
    rows = [
        {"feature": "healthcare", "n": 50, "win": 0.62, "base_win": 0.50,
         "mean": 0.008, "base_mean": 0.002, "lift_pp": 12.0, "mean_edge": 0.006},
        {"feature": "washed", "n": 50, "win": 0.60, "base_win": 0.50,
         "mean": -0.004, "base_mean": 0.002, "lift_pp": 10.0, "mean_edge": -0.006},
        {"feature": "tiny", "n": 10, "win": 0.90, "base_win": 0.50,
         "mean": 0.04, "base_mean": 0.002, "lift_pp": 40.0, "mean_edge": 0.038},
        {"feature": "consumer_defensive", "n": 77, "win": 0.60, "base_win": 0.50,
         "mean": 0.006, "base_mean": 0.003, "lift_pp": 8.0, "mean_edge": 0.003},
    ]
    keep = rr.pick_elevate_keepers(rows)
    assert [k["feature"] for k in keep] == ["healthcare"]


def test_elevate_blocklist_skips_cd() -> None:
    rows = [{
        "feature": "consumer_defensive", "n": 77, "win": 0.60, "base_win": 0.50,
        "mean": 0.006, "base_mean": 0.003, "lift_pp": 8.0, "mean_edge": 0.003,
        "role": "elevate", "band": "16-80",
    }]
    assert rr.pick_elevate_keepers(rows) == []


def test_patched_book_skips_junk_and_elevates_hc() -> None:
    df = rr.add_flags(_join_frame())
    d1 = df[df.session_date == "2026-08-20"]
    raw = rr.pick_book(d1, "raw8")
    assert list(raw.Ticker.head(3)) == ["JUNK1", "JUNK2", "JUNK3"]
    skip = rr.pick_book(d1, "skip_junk")
    assert "JUNK1" not in set(skip.Ticker)
    assert "JUNK2" not in set(skip.Ticker)
    assert "JUNK3" not in set(skip.Ticker)
    assert len(skip) == 8
    elev = rr.pick_book(d1, "elev_hc")
    assert list(elev.Ticker.head(3)) == ["MID1", "MID2", "MID3"]
    assert elev.oc.mean() > raw.oc.mean()
    red = rr.pick_book(d1, "elev_hc_red")
    assert list(red.Ticker.head(3)) == ["MID1", "MID2", "MID3"]


def test_pre_elite_day_skips_cd_without_requiring_complete() -> None:
    df = rr.add_flags(_join_frame())
    d2 = df[df.session_date == "2026-08-17"]
    skip = rr.pick_book(d2, "skip_junk")
    assert "EDU" not in set(skip.Ticker)
    assert "DAO" not in set(skip.Ticker)
    assert len(skip) == 8
    elev = rr.pick_book(d2, "elev_hc")
    assert set(elev.Ticker.head(3)) <= {"BBB", "GGG", "HHH", "LLL"}


def test_outcome_not_used_as_pick_feature() -> None:
    df = rr.add_flags(_join_frame())
    names = [n for n, _, _ in rr.feature_masks(df)]
    for banned in ("gap", "change", "relvol", "oc", "cfo"):
        assert banned not in names


def test_fee_pnl_whole_shares() -> None:
    fees = {
        "commission_per_share": 0.0049, "commission_min_per_order": 0.99,
        "commission_max_pct_of_amount": 0.005,
        "platform_per_share": 0.005, "platform_min_per_order": 1.00,
        "platform_max_pct_of_amount": 0.005,
        "settlement_per_share": 0.003,
        "regulatory_pct_of_amount_sell_only": 0.000008,
        "regulatory_min_per_order": 0.01,
        "taf_per_share_sell_only": 0.000166,
        "taf_min_per_order": 0.01, "taf_max_per_order": 8.30,
    }
    pnl = rr.fee_pnl_row(10.0, 11.0, 1250.0, fees)
    assert pnl is not None
    shares = int(1250 // 10.0)
    assert shares == 125
    # Gross 125 * $1 = $125 minus two-sided fees.
    assert pnl < 125
    assert pnl > 100


def test_template_mentions_leak_and_live() -> None:
    text = rr.TEMPLATE.read_text(encoding="utf-8")
    assert "__DATA__" in text
    assert "flatten_robust" in text
    assert "Change from Open" in text or "open→close" in text


def test_write_outputs_tmp(tmp_path: Path | None = None) -> None:
    payload = {
        "generated_at": "2026-09-06T00:00:00Z",
        "window": {"from": "2026-08-13", "to": "2026-09-04",
                   "n_sessions": 2, "n_name_days": 40},
        "clock": "1d open→close",
        "leak": "leak-free 09:30",
        "verdict": "test verdict",
        "bands": [{"band": "ranks_1_15", "n": 10, "win": 0.4, "mean": -0.01, "t": -1.0}],
        "keepers_avoid": [],
        "keepers_elevate": [],
        "books": {"raw8": {"n": 8, "days": 1, "win": 0.5, "mean": 0.001,
                           "t": 0.2, "fee": {"pnl": 1.0}, "tapes": {}}},
        "daily": [],
        "buried": [],
        "junk": [],
    }
    # Smoke the markdown builder against a tiny payload.
    lines = rr._md_table([], "avoid")
    assert "none cleared" in lines[0].lower()
    line = rr._book_line("raw8", payload["books"]["raw8"])
    assert "raw8" in line


if __name__ == "__main__":
    test_incomplete_detects_missing_elite()
    test_china_adr_from_detail_and_country()
    test_banned_features_never_keep()
    test_avoid_keeper_needs_worse_mean_and_lift()
    test_elevate_keeper_needs_lift_and_positive_mean()
    test_elevate_blocklist_skips_cd()
    test_patched_book_skips_junk_and_elevates_hc()
    test_pre_elite_day_skips_cd_without_requiring_complete()
    test_outcome_not_used_as_pick_feature()
    test_fee_pnl_whole_shares()
    test_template_mentions_leak_and_live()
    test_write_outputs_tmp()
    print("12 rank-residual tests passed")
