"""Hard-red sit research — unit tests, no live policy change.

Run: PYTHONPATH=. python3 -m src.test_hard_red_sit_research
"""
from __future__ import annotations

from src import combo_broker as cb
from src import factor_mine as fm
from src import factor_mine_combo as fmc
from src import hard_red_sit_research as hrs

DATES = [
    "2026-08-13", "2026-08-14", "2026-08-17",
    "2026-08-18", "2026-08-19",
]
ZERO_FEES = {
    "commission_per_share": 0.0,
    "commission_min_per_order": 0.0,
    "commission_max_pct_of_amount": 1.0,
    "platform_per_share": 0.0,
    "platform_min_per_order": 0.0,
    "platform_max_pct_of_amount": 1.0,
    "settlement_per_share": 0.0,
    "regulatory_pct_of_amount_sell_only": 0.0,
    "regulatory_min_per_order": 0.0,
    "taf_per_share_sell_only": 0.0,
    "taf_min_per_order": 0.0,
    "taf_max_per_order": 0.0,
}


def _boxes(news="missing"):
    return {k: "missing" for k in fm.CAMERAS} | {"news": news}


def _row(date, ticker, **kw):
    news = kw.pop("news", "missing")
    return {
        "date": date, "ticker": ticker,
        "sources": kw.pop("sources", ["union", "ohlc_hot", "yday_gainer"]),
        "boxes": kw.pop("boxes", None) or _boxes(news),
        "blue": False, "alarm": False, "zero_red": True,
        "last_green": True, "last_red": False,
        "ohlc_hot_score": kw.pop("ohlc_hot_score", 90.0),
        "src_rank": kw.pop("src_rank", 0),
        "macd_up": kw.pop("macd_up", True),
        **kw,
    }


def _panel(rows, dates=DATES):
    by = {}
    for r in rows:
        by.setdefault(r["date"], []).append(r)
    return {
        "session_dates": list(dates),
        "from_date": dates[0], "to_date": dates[-1],
        "n_sessions": len(dates), "n_rows": len(rows),
        "rows": rows, "by_date": by,
    }


def _recs():
    rec_by = {r["name"]: r for r in fm.build_recipes()}
    return [rec_by["short_news_r_macd_h3"], rec_by["union_hot_n4_h1"]]


def _combo_panel():
    rows = []
    bars = {}
    # Day 0 hard-red: long HOT + short SH. HOT dips 2%, SH fades.
    rows.append(_row(DATES[0], "HOT1", sources=["ohlc_hot"], news="missing",
                     ohlc_hot_score=99))
    rows.append(_row(DATES[0], "SH1", sources=["yday_gainer"], news="bad",
                     macd_up=True, ohlc_hot_score=1))
    bars[("HOT1", DATES[0])] = {"open": 10.0, "high": 10.1, "low": 9.80,
                                "close": 10.2}
    # SH1 also sits on the union hot-4 list. Keep its low shallow so a
    # 3% scoop cannot fill it as a long (only HOT1 can hit 1.5–2%).
    bars[("SH1", DATES[0])] = {"open": 8.0, "high": 8.1, "low": 7.90,
                               "close": 7.5}
    # Later green days so the book can still trade.
    for d, px in ((DATES[1], 10.3), (DATES[2], 10.4),
                  (DATES[3], 10.5), (DATES[4], 10.6)):
        rows.append(_row(d, "HOT1", sources=["ohlc_hot"], ohlc_hot_score=80))
        bars[("HOT1", d)] = {"open": px, "high": px + 0.2, "low": px - 0.1,
                             "close": px + 0.1}
        rows.append(_row(d, "SH1", sources=["yday_gainer"], news="bad",
                         macd_up=True, ohlc_hot_score=1))
        bars[("SH1", d)] = {"open": 7.4, "high": 7.5, "low": 7.2,
                            "close": 7.3}
    regime = {DATES[0]: {"predict_score": -5.0},
              DATES[1]: {"predict_score": 1.0}}
    return _panel(rows), bars, regime


def test_yahoo_overlay_does_not_clobber_official_open() -> None:
    # Parquet / injected official open wins; Yahoo is only a hole-filler.
    from unittest import mock
    fake = {("INDP", hrs.ASOF): {
        "open": 9.0, "low": 8.0, "close": 8.5, "src": "yahoo_session"}}
    bars = {("INDP", hrs.ASOF): {"open": 5.0, "low": 4.7, "close": None}}
    with mock.patch.object(hrs, "yahoo_session_overlay", return_value=fake):
        cf = hrs.counterfactual_0914(
            panel={"by_date": {}, "session_dates": [hrs.ASOF]},
            recs=_recs(), spec={"members": [], "weights": [1, 1]},
            bars=bars, fees=ZERO_FEES,
            regime={hrs.ASOF: {"predict_score": -11.0}},
            flatten_days=[], look_rows=[
                _row(hrs.ASOF, "INDP", sources=["ohlc_hot"]),
            ])
    indp = next(r for r in cf["webull_would"] if r["ticker"] == "INDP")
    assert abs(float(indp["open"]) - 5.0) < 1e-9
    assert indp.get("px_src") != "yahoo_session"


def test_dip_limit_clock_clean() -> None:
    fill, kind = fmc.dip_limit_px(100.0, 98.0, 1.5)
    assert kind == "scoop" and abs(fill - 98.5) < 1e-9
    fill, kind = fmc.dip_limit_px(100.0, 99.2, 1.5)
    assert fill is None and kind == "no_dip"
    fill, kind = fmc.dip_limit_px(None, 90.0, 1.0)
    assert fill is None and kind == "no_open"
    fill, kind = fmc.dip_limit_px(100.0, None, 1.0)
    assert fill is None and kind == "no_low"
    # Close must never be used as the open reference.
    fill, kind = fmc.dip_limit_px(100.0, 100.0, 0.5)
    assert fill is None and kind == "no_dip"


def test_hard_red_skip_modes() -> None:
    assert fmc.hard_red_skip_new("long", "sit") is True
    assert fmc.hard_red_skip_new("short", "sit") is True
    assert fmc.hard_red_skip_new("long", "short_only") is True
    assert fmc.hard_red_skip_new("short", "short_only") is False
    assert fmc.hard_red_skip_new("long", "dip_scoop") is False
    assert fmc.hard_red_skip_new("short", "dip_scoop") is True
    assert fmc.hard_red_skip_new("long", "short_and_scoop") is False
    assert fmc.hard_red_skip_new("short", "short_and_scoop") is False


def test_default_sit_still_blocks_long_and_short() -> None:
    panel, bars, regime = _combo_panel()
    recs = _recs()
    book = fmc.simulate_shared(
        panel, recs, [1, 1], bars=bars, fees=ZERO_FEES, regime=regime,
        name="t_sit")
    day0 = [t for t in book["trades"]
            if t["date"] == DATES[0] and t.get("side") in ("BUY", "SHORT")]
    assert day0 == []
    assert any(k.get("kind") == "hard_red" for k in book["skips"])
    # Default mode is sit even if someone passes nothing.
    assert book.get("hard_red_mode") in (None, fmc.HARD_RED_SIT, "sit")


def test_short_only_fires_short_sits_long() -> None:
    panel, bars, regime = _combo_panel()
    recs = _recs()
    book = fmc.simulate_shared(
        panel, recs, [1, 1], bars=bars, fees=ZERO_FEES, regime=regime,
        name="t_so", hard_red_mode=fmc.HARD_RED_SHORT_ONLY)
    day0 = [t for t in book["trades"]
            if t["date"] == DATES[0] and t.get("side") in ("BUY", "SHORT")]
    sides = {t["side"] for t in day0}
    assert "SHORT" in sides, day0
    assert "BUY" not in sides, day0
    assert any(k.get("kind") == "hard_red" and k.get("ticker") == "HOT1"
               for k in book["skips"])


def test_dip_scoop_fills_only_when_low_hits() -> None:
    panel, bars, regime = _combo_panel()
    recs = _recs()
    # HOT1 low = 9.80 = 2% below 10. Open−1.5% = 9.85 → fills.
    hit = fmc.simulate_shared(
        panel, recs, [1, 1], bars=bars, fees=ZERO_FEES, regime=regime,
        name="t_dip_hit", hard_red_mode=fmc.HARD_RED_DIP_SCOOP, dip_pct=1.5)
    buys = [t for t in hit["trades"]
            if t["date"] == DATES[0] and t.get("side") == "BUY"]
    assert buys and buys[0]["ticker"] == "HOT1"
    assert abs(float(buys[0]["price"]) - 9.85) < 1e-6
    shorts = [t for t in hit["trades"]
              if t["date"] == DATES[0] and t.get("side") == "SHORT"]
    assert shorts == [], "dip_scoop sits the short kid"

    # Open−3% = 9.70; low 9.80 misses.
    miss = fmc.simulate_shared(
        panel, recs, [1, 1], bars=bars, fees=ZERO_FEES, regime=regime,
        name="t_dip_miss", hard_red_mode=fmc.HARD_RED_DIP_SCOOP, dip_pct=3.0)
    buys = [t for t in miss["trades"]
            if t["date"] == DATES[0] and t.get("side") == "BUY"]
    assert buys == []
    assert any(k.get("kind") == "no_dip" for k in miss["skips"])


def test_combo_broker_modes() -> None:
    recs = _recs()
    rows = [
        _row("2026-09-14", "HOT1", sources=["ohlc_hot"]),
        _row("2026-09-14", "SH1", sources=["yday_gainer"], news="bad",
             macd_up=True, ohlc_hot_score=1),
    ]
    bars = {
        ("HOT1", "2026-09-14"): {"open": 10.0, "low": 9.7, "close": 10.1},
        ("SH1", "2026-09-14"): {"open": 8.0, "low": 7.5, "close": 7.6},
    }

    def _px(t, d, **k):
        return bars[(t, d)]["open"]

    from unittest import mock
    with mock.patch.object(cb, "quote_px", side_effect=_px):
        tickets, skips = cb.size_combo_tickets(
            rows, recs, [1, 1], cash=10_000, held=set(),
            date="2026-09-14", s=-11.0)
        assert tickets == []
        assert any(s["kind"] == "hard_red" for s in skips)

        tickets, skips = cb.size_combo_tickets(
            rows, recs, [1, 1], cash=10_000, held=set(),
            date="2026-09-14", s=-11.0,
            hard_red_mode=fmc.HARD_RED_SHORT_ONLY)
        assert tickets and all(t["kid_side"] == "short" for t in tickets)
        assert any(s["kind"] == "hard_red" and "HOT1" in str(s.get("ticker"))
                   for s in skips)

        tickets, skips = cb.size_combo_tickets(
            rows, recs, [1, 1], cash=10_000, held=set(),
            date="2026-09-14", s=-11.0,
            hard_red_mode=fmc.HARD_RED_DIP_SCOOP, dip_pct=2.0, bars=bars)
        assert tickets and all(t["kid_side"] == "long" for t in tickets)
        assert abs(tickets[0]["px"] - 9.8) < 1e-6


def test_after_fee_and_keep_kill() -> None:
    fees = {
        **ZERO_FEES,
        "commission_per_share": 0.005,
        "commission_min_per_order": 1.0,
    }
    # 1 share 10 → 11 is a win even after $1+$1 fees (pnl = 11-1 - (10+1) = -1)
    # Need a bigger move to clear $2 fees.
    pnl = hrs.after_fee_pnl(1, 10.0, 14.0, side="long", fees=fees)
    assert pnl > 0
    pnl_lose = hrs.after_fee_pnl(1, 10.0, 10.5, side="long", fees=fees)
    assert pnl_lose < 0
    short = hrs.after_fee_pnl(1, 10.0, 7.0, side="short", fees=fees)
    assert short > 0

    thin = hrs.decide_verdict(
        n_fires=8, win_rate=0.75,
        tapes_n={"webull": {"n_fires": 8, "win_rate": 0.75}},
        wf_ok=True, label="short-only")
    assert thin["label"] == "KILL" and thin["thin"] is True

    miss = hrs.decide_verdict(
        n_fires=40, win_rate=0.50,
        tapes_n={"webull": {"n_fires": 40, "win_rate": 0.50},
                 "flatten": {"n_fires": 12, "win_rate": 0.40}},
        wf_ok=True, label="scoop")
    assert miss["label"] == "KILL" and miss["thin"] is False

    keep = hrs.decide_verdict(
        n_fires=40, win_rate=0.60,
        tapes_n={"webull": {"n_fires": 22, "win_rate": 0.59},
                 "flatten": {"n_fires": 18, "win_rate": 0.61}},
        wf_ok=True, label="scoop")
    assert keep["label"] == "KEEP"


def test_run_synthetic_board() -> None:
    panel, bars, regime = _combo_panel()
    # Pad calendar so walk-forward folds can form (need ≥8 sessions
    # after 08-20). Extra days stay non-red and unused by picks.
    extra = ["2026-08-20", "2026-08-21", "2026-08-24", "2026-08-25",
             "2026-08-26", "2026-08-27"]
    for d in extra:
        panel["session_dates"].append(d)
        panel["by_date"][d] = []
    panel["to_date"] = extra[-1]
    panel["n_sessions"] = len(panel["session_dates"])
    flat = [{
        "date": DATES[0], "s": -5.0, "route": "hold",
        "tickers": ["CVE", "BG"], "io_picks": ["CVE", "BG"],
    }]
    bars[("CVE", DATES[0])] = {"open": 20.0, "low": 19.4, "close": 20.4}
    bars[("BG", DATES[0])] = {"open": 50.0, "low": 49.8, "close": 49.9}
    for d in panel["session_dates"][1:]:
        bars[("CVE", d)] = {"open": 20.4, "low": 20.2, "close": 20.5}
        bars[("BG", d)] = {"open": 49.9, "low": 49.7, "close": 50.0}
    look = [
        _row(hrs.ASOF, "INDP", sources=["ohlc_hot"]),
        _row(hrs.ASOF, "BKV", sources=["yday_gainer"], news="bad",
             macd_up=True, ohlc_hot_score=1),
    ]
    bars[("INDP", hrs.ASOF)] = {"open": 5.0, "low": 4.7, "close": None}
    bars[("BKV", hrs.ASOF)] = {"open": 25.0, "low": 24.0, "close": None}
    payload = hrs.run(
        panel=panel, bars=bars, fees=ZERO_FEES, regime=regime,
        flatten_days=flat, look_rows_0914=look, write=False,
        skip_look=True)
    assert payload["sit_hard_red_fires"] == 0
    assert payload["verdict_short"]["label"] in ("KEEP", "KILL")
    assert payload["verdict_x"]["label"] in ("KEEP", "KILL")
    md = hrs.render_md(payload)
    assert "Short-only:" in md
    assert "dip-scoop" in md.lower() or "scoop" in md.lower()
    assert "2026-09-14" in md
    assert "flatten_robust" in md
    assert hrs.LIVE_COMBO in md
    # Live sit must still be the (A) row.
    sit = next(r for r in payload["webull_rows"] if r["mode"] == "sit")
    assert sit["hard_red_fires"] == 0
    cf = payload["counterfactual_0914"]
    assert cf["hard_red"] is True
    assert any(r["ticker"] == "BKV" for r in cf["webull_would"])


def test_md_lists_the_gate() -> None:
    payload = {
        "verdict_short": {"label": "KILL", "why": "thin shorts"},
        "verdict_x": {"label": "KILL", "why": "thin scoops"},
        "best_x": {"dip_pct": 1.5},
        "from_date": "2026-08-13", "to_date": "2026-09-14",
        "n_sessions": 22,
        "hard_red_days": [{"date": "2026-09-14", "s": -11.0}],
        "webull_rows": [],
        "flatten_rows": [],
        "walkforward": {"n_folds": 0, "folds": [], "oos_short": {},
                        "oos_x": {}},
        "counterfactual_0914": {
            "date": "2026-09-14", "s": -11.002, "hard_red": True,
            "webull_would": [
                {"ticker": "INDP", "side": "long", "no_price": True},
                {"ticker": "BKV", "side": "short", "no_price": False,
                 "open": 25, "close": None, "same_day_oc_pnl_short": None},
            ],
            "flatten_would": [
                {"ticker": "CVE", "side": "long", "no_price": False,
                 "open": 33, "dip_from_open_pct": 1.2},
                {"ticker": "DK", "side": "long", "no_price": True},
            ],
            "note": "clock-clean",
        },
    }
    md = hrs.render_md(payload)
    assert "size_combo_tickets" in md
    assert "KILL" in md
    assert "DK" in md
    assert "Do not merge a live policy change" in md


def main() -> None:
    test_yahoo_overlay_does_not_clobber_official_open()
    test_dip_limit_clock_clean()
    test_hard_red_skip_modes()
    test_default_sit_still_blocks_long_and_short()
    test_short_only_fires_short_sits_long()
    test_dip_scoop_fills_only_when_low_hits()
    test_combo_broker_modes()
    test_after_fee_and_keep_kill()
    test_run_synthetic_board()
    test_md_lists_the_gate()
    print("test_hard_red_sit_research: 10 ok")


if __name__ == "__main__":
    main()
