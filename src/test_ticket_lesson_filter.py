"""Ticket lesson FILTER: hook + RWT fixture + missing-feature no-op.

Run: PYTHONPATH=. python3 -m src.test_ticket_lesson_filter
"""
from __future__ import annotations

from unittest import mock

from src import combo_broker as cb
from src import factor_mine as fm
from src import strategy_tickets as st
from src import ticket_lesson_filter as tlf


def _reg(**overrides) -> dict:
    base = {
        "filters": [
            {
                "id": "oversold_crash_pause",
                "lesson_id": "oversold_crash_pause",
                "side": "short",
                "action": "block",
                "enabled": True,
                "require": {"rsi_max": 25.0, "crash": True},
                "crash": {"ret_1_max": -8.0, "range_pct_min": 8.0, "gap_pct_max": -8.0},
                "audit": "rsi<=25 & crash",
            },
            {
                "id": "overbought_meltup_pause",
                "lesson_id": "overbought_meltup_pause",
                "side": "long",
                "action": "block",
                "enabled": True,
                "require": {"rsi_min": 75.0, "ret_1_min": 8.0, "no_camera_support": True},
                "audit": "rsi>=75 & 1d>=+8 & no support",
            },
        ]
    }
    base.update(overrides)
    return base


def test_never_raises_on_garbage() -> None:
    assert tlf.evaluate("short", None)["action"] == "pass"
    assert tlf.decide_row("nope", "2026-09-11")["action"] == "pass"
    kept, blocked = tlf.filter_rows(None, "2026-09-11")
    assert kept == [] and blocked == []
    assert tlf.stamp_strategy(None).get("buy") in (None, [])
    assert isinstance(tlf.apply_to_payload(None), dict)
    assert tlf.prior_features("", "")["rsi"] is None


def test_missing_features_are_pass_not_veto() -> None:
    dec = tlf.evaluate(
        "short",
        {"ticker": "XXXX", "date": "2026-09-11", "rsi": None, "ret_1": None},
        registry=_reg(),
    )
    assert dec["action"] == "pass"
    dec = tlf.evaluate(
        "short",
        {"ticker": "XXXX", "date": "2026-09-11", "rsi": 18.0, "ret_1": None,
         "range_pct": None, "gap_pct": None, "air_pocket": False},
        registry=_reg(),
    )
    assert dec["action"] == "pass", dec


def test_f1_needs_rsi_and_crash() -> None:
    crash = {"ticker": "FOO", "date": "2026-09-11", "rsi": 18.0, "ret_1": -16.7,
             "range_pct": 20.0, "air_pocket": True, "crash": True}
    dec = tlf.evaluate("short", crash, registry=_reg())
    assert dec["action"] == "block"
    assert dec["lesson_id"] == "oversold_crash_pause"
    assert "rsi<=25" in (dec.get("reason") or "")
    # long of the same tape is not F1
    assert tlf.evaluate("long", crash, registry=_reg())["action"] == "pass"
    # RSI-only, no crash
    mild = {"ticker": "FOO", "date": "2026-09-11", "rsi": 18.0, "ret_1": -2.0,
            "range_pct": 1.0, "gap_pct": 0.0, "air_pocket": False, "crash": False}
    assert tlf.evaluate("short", mild, registry=_reg())["action"] == "pass"
    # crash-only, RSI 40
    mid = {"ticker": "FOO", "date": "2026-09-11", "rsi": 40.0, "ret_1": -16.0,
           "air_pocket": True, "crash": True}
    assert tlf.evaluate("short", mid, registry=_reg())["action"] == "pass"
    # wide UP bar is not a crash even with RSI 18
    up_air = {"ticker": "FOO", "date": "2026-09-11", "rsi": 18.0, "ret_1": 14.9,
              "range_pct": 20.0, "gap_pct": 2.0, "air_pocket": True, "crash": True}
    assert tlf.evaluate("short", up_air, registry=_reg())["action"] == "pass"


def test_f2_meltup_without_support() -> None:
    blow = {"ticker": "BAR", "date": "2026-09-11", "rsi": 82.0, "ret_1": 11.0,
            "camera_support": False, "news": "bad"}
    dec = tlf.evaluate("long", blow, registry=_reg())
    assert dec["action"] == "block"
    assert dec["lesson_id"] == "overbought_meltup_pause"
    supported = dict(blow, camera_support=True, news="good")
    assert tlf.evaluate("long", supported, registry=_reg())["action"] == "pass"
    assert tlf.evaluate("short", blow, registry=_reg())["action"] == "pass"


def test_rwt_911_short_is_blocked_without_allowlist() -> None:
    """General predicate — ticker string must not appear in the filter module.

    Panel row is the 2026-09-11 archive print (rsi 18.36, 1d −14.8). Live
    OHLC is used when the parquet is on disk; overlay keeps CI leak-free
    without a ticker allowlist.
    """
    src = __import__("pathlib").Path(tlf.__file__).read_text(encoding="utf-8")
    assert "RWT" not in src
    row = {"rsi": 18.36, "ohlc_ret_1": -14.788734286201677, "ohlc_rvol": 12.57}
    feat = tlf.prior_features("RWT", "2026-09-11", row)
    assert feat.get("rsi") is not None and feat["rsi"] <= 25
    assert feat.get("ret_1") is not None and feat["ret_1"] <= -8
    dec = tlf.evaluate("short", feat, registry=tlf.load_registry())
    assert dec["action"] == "block", (dec, feat)
    assert dec["lesson_id"] == "oversold_crash_pause"
    assert "RWT" in (dec.get("reason") or "")
    assert "oversold_crash_pause" in (dec.get("reason") or "")
    live = tlf.prior_features("RWT", "2026-09-11")
    if live.get("rsi") is not None:
        assert live["rsi"] <= 25
        assert (live.get("ret_1") or 0) <= -8
        assert tlf.evaluate("short", live)["action"] == "block"


def test_entry_hook_blocks_short_and_tags_blotter() -> None:
    payload = {
        "date": "2026-09-11",
        "session_open": "2026-09-11",
        "strategies": {
            "stock_book_1d": {
                "name": "stock_book_1d",
                "family": "stock_book",
                "date": "2026-09-11",
                "side": "mixed",
                "buy": [{"ticker": "ORCL", "side": "long"}],
                "sell": [{"ticker": "RWT", "side": "short", "score": -0.47,
                           "rsi": 18.36, "ohlc_ret_1": -14.79}],
            },
            "flatten_robust": {
                "name": "flatten_robust",
                "family": "flatten",
                "date": "2026-09-11",
                "side": "long",
                "buy": [{"ticker": "ORCL"}],
                "sell": [{"ticker": "RWT", "side": "short"}],
            },
        },
    }
    out = tlf.apply_to_payload(payload, registry=tlf.load_registry())
    sb = out["strategies"]["stock_book_1d"]
    names = {x["ticker"] for x in sb.get("sell") or []}
    assert "RWT" not in names
    assert sb.get("blocked_n") == 1
    audit = " ".join(sb.get("blocked_audit") or [])
    assert "oversold_crash_pause" in audit
    assert "RWT" in audit
    # flatten sells are exits — do not eat a cover
    flat = out["strategies"]["flatten_robust"]
    assert any(x.get("ticker") == "RWT" for x in (flat.get("sell") or []))


def test_live_registry_keeps_f1() -> None:
    tlf.reset_caches()
    reg = tlf.load_registry()
    live = [f["id"] for f in (reg.get("filters") or []) if f.get("enabled") is not False]
    assert "oversold_crash_pause" in live, live
    assert "RWT" not in __import__("pathlib").Path(tlf.__file__).read_text(encoding="utf-8")


def test_c1_blocks_ob_recipe_with_low_rsi() -> None:
    spec = {
        "filters": [{
            "id": "short_vs_own_recipe",
            "lesson_id": "short_vs_own_recipe",
            "side": "short",
            "action": "block",
            "enabled": True,
            "require": {"recipe_ob_or_macd_dn": True, "rsi_max": 49.99},
            "audit": "recipe OB/MACD-dn gate & rsi<50",
        }]
    }
    feat = {"ticker": "FOO", "date": "2026-09-11", "rsi": 18.0, "ret_1": -2.0}
    # advertised gate + RSI 18 → block
    dec = tlf.evaluate("short", feat, registry=spec,
                       extra={"recipe_require": {"rsi_ob": True},
                              "strategy": "short_rsi_ob_h3"})
    assert dec["action"] == "block", dec
    # no recipe info → pass
    assert tlf.evaluate("short", feat, registry=spec, extra={})["action"] == "pass"
    # recipe without that gate → pass
    dec = tlf.evaluate("short", feat, registry=spec,
                       extra={"recipe_require": {"news": "bad"},
                              "strategy": "short_news_r_h3"})
    assert dec["action"] == "pass", dec
    # RSI 72 on an OB recipe → pass
    dec = tlf.evaluate("short", dict(feat, rsi=72.0), registry=spec,
                       extra={"recipe_require": {"macd_down": True},
                              "strategy": "short_macd_dn_h1"})
    assert dec["action"] == "pass", dec


def test_c2_c3_c4_missing_is_pass() -> None:
    specs = {
        "filters": [
            {
                "id": "short_vs_green_cameras",
                "side": "short", "action": "block", "enabled": True,
                "require": {"camera_net_min": 2, "unless_news_bad": True},
            },
            {
                "id": "long_vs_hard_red_news",
                "side": "long", "action": "block", "enabled": True,
                "require": {"news_bad": True, "no_camera_support": True},
            },
            {
                "id": "short_vs_sector_or_tape",
                "side": "short", "action": "block", "enabled": True,
                "require": {"crash": True, "sector_good": True},
                "crash": {"ret_1_max": -8.0, "gap_pct_max": -8.0},
            },
        ]
    }
    empty = {"ticker": "FOO", "date": "2026-09-11", "rsi": 18.0}
    assert tlf.evaluate("short", empty, registry=specs)["action"] == "pass"
    assert tlf.evaluate("long", empty, registry=specs)["action"] == "pass"
    green = dict(empty, camera_net=3, camera_support=True, news="neutral")
    assert tlf.evaluate("short", green, registry=specs)["action"] == "block"
    news_red = dict(empty, camera_net=3, news="bad")
    assert tlf.evaluate("short", news_red, registry=specs)["action"] == "pass"
    long_bad = {"ticker": "FOO", "date": "2026-09-11", "news": "bad",
                "camera_support": False}
    assert tlf.evaluate("long", long_bad, registry=specs)["action"] == "block"
    crash_good = {"ticker": "FOO", "date": "2026-09-11", "rsi": 18.0,
                  "ret_1": -16.0, "crash": True, "sector": "good"}
    assert tlf.evaluate("short", crash_good, registry=specs)["action"] == "block"
    crash_miss = {"ticker": "FOO", "date": "2026-09-11", "rsi": 18.0,
                  "ret_1": -16.0, "crash": True}
    assert tlf.evaluate("short", crash_miss, registry=specs)["action"] == "pass"


def test_strategy_tickets_build_calls_filter() -> None:
    src = __import__("pathlib").Path(st.__file__).read_text(encoding="utf-8")
    assert "ticket_lesson_filter" in src
    assert "apply_to_payload" in src


def test_combo_broker_skips_filtered_short() -> None:
    rec_by = {r["name"]: r for r in fm.build_recipes()}
    recs = [rec_by["short_news_r_macd_h3"], rec_by["union_hot_n4_h1"]]
    rows = [
        {"ticker": "HOT1", "sources": ["ohlc_hot"], "boxes": {"news": "missing"},
         "alarm": False, "macd_up": False, "ohlc_hot_score": 90},
        {"ticker": "SH1", "sources": ["yday_gainer"], "boxes": {"news": "bad"},
         "alarm": False, "macd_up": True, "ohlc_hot_score": 1,
         "rsi": 17.4, "ohlc_ret_1": -16.7},
    ]

    def _pick(panel_rows, rec):
        if rec["name"] == "union_hot_n4_h1":
            return [r for r in panel_rows if str(r["ticker"]).startswith("HOT")]
        return [r for r in panel_rows if str(r["ticker"]).startswith("SH")]

    with mock.patch.object(cb, "quote_px", side_effect=lambda t, d, **k: 10.0), \
            mock.patch.object(fm, "pick_day", side_effect=_pick):
        tickets, skips = cb.size_combo_tickets(
            rows, recs, [1, 1], cash=10_000, held=set(),
            date="2026-09-11", s=1.0, combo=cb.PAPER_COMBO,
        )
    by_t = {t["ticker"]: t for t in tickets}
    assert "HOT1" in by_t
    assert "SH1" not in by_t
    lesson = [s for s in skips if s.get("kind") == "lesson_filter"]
    assert lesson and lesson[0]["ticker"] == "SH1"
    assert lesson[0].get("lesson_id") == "oversold_crash_pause"


def main() -> None:
    tlf.reset_caches()
    test_never_raises_on_garbage()
    test_missing_features_are_pass_not_veto()
    test_f1_needs_rsi_and_crash()
    test_f2_meltup_without_support()
    test_rwt_911_short_is_blocked_without_allowlist()
    test_entry_hook_blocks_short_and_tags_blotter()
    test_live_registry_keeps_f1()
    test_c1_blocks_ob_recipe_with_low_rsi()
    test_c2_c3_c4_missing_is_pass()
    test_strategy_tickets_build_calls_filter()
    test_combo_broker_skips_filtered_short()
    print("ok")


if __name__ == "__main__":
    main()
