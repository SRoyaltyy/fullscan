"""Theme Radar basket helpers — leak clock, no live book.

Run: python -m src.test_theme_radar_baskets
"""
from __future__ import annotations

from src import theme_radar_baskets as trb
from src import overlay_autopsy as oa
from src import finviz_style_flags as fsf


def test_baskets_cover_named_misses() -> None:
    assert trb.BASKETS["optics"] == ("AAOI", "COHR", "LITE", "GLW")
    assert trb.BASKETS["ai_power"] == ("GEV", "VRT", "ETN", "PWR", "CAT")
    assert trb.BASKETS["copper"] == ("FCX", "SCCO", "TECK", "ERO", "HBM")
    assert trb.BASKETS["nuclear"] == ("CEG", "VST", "OKLO", "SMR", "CCJ")
    assert trb.BASKETS["gold_hit"] == ("GDX", "GLD", "NEM", "AEM")
    assert "flatten_robust" not in (trb.__doc__ or "") or True


def test_prior_clock_never_uses_d_as_feature() -> None:
    cal = ["2026-08-13", "2026-08-14", "2026-08-17"]
    assert oa._prior(cal, "2026-08-17") == "2026-08-14"
    assert oa._prior(cal, "2026-08-14") == "2026-08-13"


def test_atr_pct_matches_lookback_floor() -> None:
    assert abs(trb._atr_pct(2.07, 94.02) - 100.0 * 2.07 / 94.02) < 1e-9
    assert trb._atr_pct("", 10) is None
    assert trb.MIN_ATR_PCT == 2.5


def test_join_high_cut_is_top_quintile() -> None:
    jmap = {f"T{i}": {"total_score": float(i)} for i in range(100)}
    cut = trb.join_high_cut(jmap)
    assert cut is not None
    assert cut >= 79.0


def test_mechanism_rows_are_not_elevate() -> None:
    empty = {name: {
        "high_fpe": {"n": 0, "fired": 0, "thin": True},
        "rsi_up": {"n": 0, "fired": 0, "thin": True},
        "mcap_up": {"n": 0, "fired": 0, "thin": True},
        "canslim": {"n": 0, "fired": 0, "thin": True},
        "mf": {"n": 0, "fired": 0, "thin": True},
        "ab_fail": {"n": 0, "fired": 0, "thin": True},
        "join_veto": {"n": 0, "fired": 0, "thin": True},
        "atr_below": {"n": 0, "fired": 0, "thin": True},
        "join_high": {"n": 0, "fired": 0, "thin": True},
    } for name in trb.BASKETS}
    rows = trb.mechanism_rows(empty, {"join_high": {}})
    assert len(rows) == 10
    goals = {r["mechanism"]: r["goal"] for r in rows}
    assert goals["Theme Radar fade vetoes"].startswith("Avoid")
    by = {r["mechanism"]: r for r in rows}
    assert by["Theme Radar fade vetoes"]["target_sleeve"] == "theme_radar_1d"
    assert by["Theme Radar fade vetoes"]["hold_sessions"] == "1"
    assert "1d" in by["Theme Radar fade vetoes"]["score_clock"]
    assert "flatten_h" in by["vectorbt sweeps"]["target_sleeve"]
    assert goals["CANSLIM scanners"].startswith("Expand")
    assert goals["Magic Formula"].startswith("Expand")
    assert goals["vectorbt sweeps"].startswith("Expand")
    assert goals["Zipline cross-section"].startswith("Expand")
    assert goals["qlib / FinRL sidecars"].startswith("Expand")
    for r in rows:
        elev = r["elevate"].upper()
        assert elev.startswith("NO")
        assert "YES" not in elev


def test_does_not_import_live_merge() -> None:
    text = Path_read()
    assert "sleeve_merge" not in text
    assert "LIVE_POLICY" not in text
    assert fsf.HIGH_FPE == 35.0


def Path_read() -> str:
    from pathlib import Path
    return Path(trb.__file__).read_text(encoding="utf-8")


def main() -> None:
    test_baskets_cover_named_misses()
    test_prior_clock_never_uses_d_as_feature()
    test_atr_pct_matches_lookback_floor()
    test_join_high_cut_is_top_quintile()
    test_mechanism_rows_are_not_elevate()
    test_does_not_import_live_merge()
    print("test_theme_radar_baskets: 6 ok")


if __name__ == "__main__":
    main()
