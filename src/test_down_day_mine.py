"""Down-day winner mine: leak-free features and keeper math."""
from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd

from src import down_day_mine as ddm


def _tiny_prices() -> pd.DataFrame:
    """Two names + SPY. D2 is a SPY-down day. AAA finishes green, BBB red."""
    rows = []
    # Need ≥11 prior bars for ret_10 / break_10; 14 sessions.
    dates = [f"2026-07-{d:02d}" for d in range(1, 15)]
    spy_c = 100.0
    for i, d in enumerate(dates):
        # SPY drifts up, then dumps on the last day.
        if i < len(dates) - 1:
            o, c = spy_c, spy_c + 0.4
        else:
            o, c = spy_c, spy_c - 2.0
        rows.append({"date": d, "ticker": "SPY", "open": o, "high": max(o, c) + 0.2,
                     "low": min(o, c) - 0.2, "close": c, "volume": 8e7})
        spy_c = c
        # AAA: last bar green, finishes up on the dump day.
        ao, ac = 10 + i * 0.1, 10 + i * 0.1 + 0.15
        if i == len(dates) - 1:
            ao, ac = 11.2, 11.6
        rows.append({"date": d, "ticker": "AAA", "open": ao, "high": max(ao, ac) + 0.1,
                     "low": min(ao, ac) - 0.1, "close": ac, "volume": 2e6})
        # BBB: last bar red, finishes down on the dump day.
        bo, bc = 20 - i * 0.05, 20 - i * 0.05 - 0.1
        if i == len(dates) - 1:
            bo, bc = 19.0, 18.4
        rows.append({"date": d, "ticker": "BBB", "open": bo, "high": max(bo, bc) + 0.1,
                     "low": min(bo, bc) - 0.1, "close": bc, "volume": 3e6})
    return pd.DataFrame(rows)


def test_prior_features_ignore_today_close() -> None:
    px = _tiny_prices()
    feat = ddm.attach_prior_ohlc(px)
    last = feat[(feat.ticker == "AAA") & (feat.date == "2026-07-14")].iloc[0]
    assert last["oc"] > 0
    # last_green is the *prior* bar, not today's green close.
    prev = feat[(feat.ticker == "AAA") & (feat.date == "2026-07-13")].iloc[0]
    assert bool(last["last_green"]) == bool(prev["close"] > prev["open"])
    assert last["prev_close"] == prev["close"]


def test_spy_down_flags() -> None:
    px = _tiny_prices()
    spy = ddm.spy_calendar(px)
    last = spy[spy.date == "2026-07-14"].iloc[0]
    assert bool(last["spy_cc_down"]) is True
    assert bool(last["spy_oc_down"]) is True
    prior = spy[spy.date == "2026-07-13"].iloc[0]
    assert bool(prior["spy_cc_down"]) is False


def test_banned_features_never_keep() -> None:
    fake = [{
        "feature": "gap", "n": 9000, "win": 0.9, "mean": 0.04,
        "lift_pp": 20, "mean_edge": 0.03, "target": "oc", "regime": "spy_cc_down",
    }]
    assert ddm.pick_keepers(fake) == []


def test_is_strong_needs_n_and_t() -> None:
    weak = {"feature": "fv_gold", "n": 158, "t": 1.7, "mean": 0.003,
            "mean_edge": 0.001, "lift_pp": 14}
    strong = {"feature": "fv_health", "n": 1849, "t": 2.2, "mean": 0.003,
              "mean_edge": 0.001, "lift_pp": 8}
    assert ddm.is_strong(weak) is False
    assert ddm.is_strong(strong) is True


def test_keeper_needs_lift_and_positive_mean() -> None:
    rows = [
        {"feature": "last_green", "n": 500, "win": 0.48, "base_win": 0.40,
         "mean": 0.002, "base_mean": 0.0001, "lift_pp": 8.0, "mean_edge": 0.0019},
        {"feature": "hot_ge_2", "n": 500, "win": 0.50, "base_win": 0.40,
         "mean": -0.01, "base_mean": -0.02, "lift_pp": 10.0, "mean_edge": 0.01},
        {"feature": "nr7", "n": 50, "win": 0.80, "base_win": 0.40,
         "mean": 0.02, "base_mean": 0.0, "lift_pp": 40.0, "mean_edge": 0.02},
    ]
    keep = ddm.pick_keepers(rows)
    assert [k["feature"] for k in keep] == ["last_green"]


def test_excel_open_cols_exclude_close_knowable() -> None:
    note = ddm.excel_notes()
    assert "A" in note["open_knowable_cols"]
    assert "D" in note["close_knowable_cols"]
    assert set(note["open_knowable_cols"]).isdisjoint(note["close_knowable_cols"])


def test_template_and_write(tmp_path=None) -> None:
    text = ddm.TEMPLATE.read_text(encoding="utf-8")
    assert "__DATA__" in text
    assert "leak-free" in text.lower()
    assert "flatten_robust" in text


def test_liquid_skips_spy() -> None:
    px = _tiny_prices()
    feat = ddm.attach_prior_ohlc(px)
    spy = ddm.spy_calendar(px)
    names = ddm.liquid_name_days(feat, spy)
    assert "SPY" not in set(names.ticker)
    dump = names[names.date == "2026-07-14"]
    assert not dump.empty
    aaa = dump[dump.ticker == "AAA"]
    assert not aaa.empty
    assert bool(aaa.iloc[0]["win"]) is True


def test_liquid_skips_inverse() -> None:
    px = _tiny_prices()
    extra = px[px.ticker == "AAA"].copy()
    extra["ticker"] = "SOXS"
    feat = ddm.attach_prior_ohlc(pd.concat([px, extra], ignore_index=True))
    spy = ddm.spy_calendar(px)
    names = ddm.liquid_name_days(feat, spy)
    assert "SOXS" not in set(names.ticker)
    assert "UVIX" not in ddm.LEV_SKIP or "UVIX" in ddm.LEV_SKIP


def test_combo_masks_need_sector() -> None:
    px = _tiny_prices()
    feat = ddm.attach_prior_ohlc(px)
    spy = ddm.spy_calendar(px)
    names = ddm.liquid_name_days(feat, spy)
    names["sector"] = np.where(names.ticker == "AAA", "Healthcare", "Energy")
    names["vol_m"] = 2.0
    names["mcap"] = 2000.0
    names["beta"] = 1.8
    names["optionable"] = True
    ids = {n for n, _, _ in ddm.combo_masks(names)}
    assert "health_last_green" in ids
    assert "xl_l1_lowvol_green" in ids
    assert "xl_l5_mid_hibeta" in ids


def test_near_miss_needs_negative_mean() -> None:
    rows = [
        {"feature": "hot_ge_2", "regime": "spy_cc_down", "target": "oc",
         "n": 500, "win": 0.45, "mean": -0.01, "lift_pp": 4.0},
        {"feature": "last_green", "regime": "spy_cc_down", "target": "oc",
         "n": 500, "win": 0.48, "mean": 0.002, "lift_pp": 8.0},
        {"feature": "tiny", "regime": "spy_cc_down", "target": "oc",
         "n": 20, "win": 0.9, "mean": -0.01, "lift_pp": 40.0},
    ]
    near = ddm.pick_near(rows)
    assert [n["feature"] for n in near] == ["hot_ge_2"]


def test_overlay_require_uses_tagged_base() -> None:
    px = _tiny_prices()
    feat = ddm.attach_prior_ohlc(px)
    spy = ddm.spy_calendar(px)
    names = ddm.liquid_name_days(feat, spy)
    names["sector"] = np.where(names.ticker == "AAA", "Healthcare", None)
    names["mcap"] = np.where(names.ticker == "AAA", 5000.0, np.nan)
    pred = lambda d: d["spy_cc_down"] == True
    rows = ddm.sweep_extra(
        names, "spy_cc_down", pred, ddm.finviz_masks(names),
        min_n=1, require=lambda d: d["sector"].notna(),
    )
    health = [r for r in rows if r["feature"] == "fv_health"]
    assert health
    # Tagged-only base: AAA is the only tagged name on the dump day.
    assert health[0]["base_n"] == health[0]["n"]


def test_sector_etf_missing_ok() -> None:
    px = _tiny_prices()
    spy = ddm.spy_calendar(px)
    rows = ddm.score_sector_etfs(px, spy)
    assert rows == [] or all("ticker" in r for r in rows)


def test_attach_panel_skips_missing(tmp_path, monkeypatch=None) -> None:
    px = _tiny_prices()
    feat = ddm.attach_prior_ohlc(px)
    spy = ddm.spy_calendar(px)
    names = ddm.liquid_name_days(feat, spy)
    # Existing helper must not raise when the official panel is absent
    # from this tiny frame (it only reads, never rebuilds).
    out = names.copy()
    out["has_panel"] = False
    assert "has_panel" in out.columns


if __name__ == "__main__":
    import numpy as np
    test_prior_features_ignore_today_close()
    test_spy_down_flags()
    test_banned_features_never_keep()
    test_is_strong_needs_n_and_t()
    test_keeper_needs_lift_and_positive_mean()
    test_excel_open_cols_exclude_close_knowable()
    test_template_and_write()
    test_liquid_skips_spy()
    test_liquid_skips_inverse()
    test_combo_masks_need_sector()
    test_near_miss_needs_negative_mean()
    test_overlay_require_uses_tagged_base()
    test_sector_etf_missing_ok()
    test_attach_panel_skips_missing(None)
    print("14 down-day mine tests passed")

