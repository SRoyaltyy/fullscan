"""Hard-red strategy mine — unit tests, no live policy change.

Run: PYTHONPATH=. python3 -m src.test_hard_red_strategy_mine
"""
from __future__ import annotations

from src import factor_mine as fm
from src import factor_mine_book as fmb
from src import factor_mine_combo as fmc
from src import hard_red_strategy_mine as hrm

DATES = [
    "2026-08-13", "2026-08-14", "2026-08-17",
    "2026-08-18", "2026-08-19", "2026-08-20",
    "2026-08-21", "2026-08-24", "2026-08-25",
    "2026-08-26",
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


def test_time_split_cutoff() -> None:
    assert hrm.time_split_cutoff([]) is None
    assert hrm.time_split_cutoff(["2026-08-13"]) == "2026-08-13"
    # 10 dates, hold 30% → idx = round(7) = 7 → dates[7]
    cut = hrm.time_split_cutoff(DATES, hold_frac=0.30)
    assert cut == DATES[7], cut
    assert hrm.time_split_cutoff(DATES, locked="2026-08-20") == "2026-08-20"


def test_disc_label_ok_blocks_horizon_spill() -> None:
    cutoff = "2026-08-21"
    # Entry in disc, same-day close in disc — ok.
    assert hrm.disc_label_ok("2026-08-20", "2026-08-20", cutoff) is True
    # Entry in disc, hold-3 close on/after cutoff — leak, drop.
    assert hrm.disc_label_ok("2026-08-20", "2026-08-21", cutoff) is False
    assert hrm.disc_label_ok("2026-08-20", "2026-08-24", cutoff) is False
    # Holdout entry is not discovery.
    assert hrm.disc_label_ok("2026-08-21", "2026-08-25", cutoff) is False
    # Missing exit fails closed.
    assert hrm.disc_label_ok("2026-08-20", None, cutoff) is False
    assert hrm.disc_label_ok("2026-08-20", "2026-08-20", None) is False


def test_split_fires_hold_region() -> None:
    cutoff = "2026-08-21"
    fires = [
        {"date": "2026-08-18", "exit_date": "2026-08-19", "pnl": 1},
        {"date": "2026-08-20", "exit_date": "2026-08-24", "pnl": 9},  # spill
        {"date": "2026-08-21", "exit_date": "2026-08-25", "pnl": -1},
        {"date": "2026-08-24", "exit_date": "2026-08-25", "pnl": 2},
    ]
    parts = hrm.split_fires(fires, cutoff)
    assert [f["date"] for f in parts["disc"]] == ["2026-08-18"]
    assert [f["date"] for f in parts["hold"]] == ["2026-08-21", "2026-08-24"]


def test_horizons_use_different_exits() -> None:
    red = ["2026-08-18"]
    cal = DATES
    rec = fm.make_recipe("t_hot", universe="ohlc_hot", hold=1)
    rows = [_row("2026-08-18", "AAA", sources=["ohlc_hot"])]
    bars = {
        ("AAA", "2026-08-18"): {"open": 10.0, "close": 11.0},
        ("AAA", "2026-08-19"): {"open": 11.0, "close": 9.0},
        ("AAA", "2026-08-20"): {"open": 9.0, "close": 12.0},
        ("AAA", "2026-08-21"): {"open": 12.0, "close": 12.5},
        ("AAA", "2026-08-24"): {"open": 12.5, "close": 8.0},
    }
    panel = _panel(rows, dates=cal)
    h1 = hrm.name_day_fires(
        panel, rec, red, cal, bars=bars, fees=ZERO_FEES, hold=1)
    h3 = hrm.name_day_fires(
        panel, rec, red, cal, bars=bars, fees=ZERO_FEES, hold=3)
    assert len(h1) == 1 and len(h3) == 1
    assert h1[0]["exit_date"] == "2026-08-18"
    assert h3[0]["exit_date"] == "2026-08-20"
    assert h1[0]["pnl"] > 0
    assert h3[0]["pnl"] > 0
    # hold 5 is entry+4 sessions → 08-24 close 8, a loser.
    h5 = hrm.name_day_fires(
        panel, rec, red, cal, bars=bars, fees=ZERO_FEES, hold=5)
    assert h5[0]["exit_date"] == "2026-08-24"
    assert h5[0]["pnl"] < 0


def test_allow_mode_takes_open_on_red() -> None:
    rec = fm.make_recipe(
        "t_long", universe="ohlc_hot", hold=1)
    rec_s = fm.make_recipe(
        "t_short", universe="yday_gainer", hold=1, side="short",
        require={"news": "bad"})
    rows = [
        _row(DATES[0], "HOT1", sources=["ohlc_hot"]),
        _row(DATES[0], "SH1", sources=["yday_gainer"], news="bad"),
        _row(DATES[1], "HOT1", sources=["ohlc_hot"]),
    ]
    bars = {
        ("HOT1", DATES[0]): {"open": 10.0, "high": 10.2, "low": 9.9,
                             "close": 10.1},
        ("SH1", DATES[0]): {"open": 8.0, "high": 8.1, "low": 7.5,
                            "close": 7.4},
        ("HOT1", DATES[1]): {"open": 10.1, "high": 10.3, "low": 10.0,
                             "close": 10.2},
        ("SH1", DATES[1]): {"open": 7.4, "high": 7.5, "low": 7.2,
                            "close": 7.3},
    }
    panel = _panel(rows, dates=DATES[:2])
    regime = {DATES[0]: {"predict_score": -5.0},
              DATES[1]: {"predict_score": 1.0}}
    sit = fmc.simulate_shared(
        panel, [rec_s, rec], [1, 1], bars=bars, fees=ZERO_FEES,
        regime=regime, name="t_sit")
    day0 = [t for t in sit["trades"]
            if t["date"] == DATES[0] and t.get("side") in ("BUY", "SHORT")]
    assert day0 == []
    allow = fmc.simulate_shared(
        panel, [rec_s, rec], [1, 1], bars=bars, fees=ZERO_FEES,
        regime=regime, name="t_allow",
        hard_red_mode=fmc.HARD_RED_ALLOW)
    day0 = [t for t in allow["trades"]
            if t["date"] == DATES[0] and t.get("side") in ("BUY", "SHORT")]
    sides = {t["side"] for t in day0}
    assert "BUY" in sides and "SHORT" in sides, day0
    # simulate_book allow-through via rules override.
    red_book = fmb.simulate_book(
        panel, rec, bars=bars, fees=ZERO_FEES, regime=regime)
    assert not any(
        t.get("side") == "BUY" and t["date"] == DATES[0]
        for t in red_book["trades"])
    thru = fmb.simulate_book(
        panel, rec, bars=bars, fees=ZERO_FEES, regime=regime,
        rules={"hard_red_no_new": False})
    assert any(
        t.get("side") == "BUY" and t["date"] == DATES[0]
        for t in thru["trades"])


def test_research_survivor_and_live_keep() -> None:
    disc = {"n_graded": 10, "win_rate": 0.70, "n_fires": 10, "pnl": 4}
    hold = {"n_graded": 9, "win_rate": 0.66, "n_fires": 9, "pnl": 2}
    assert hrm.research_survivor(disc=disc, hold=hold) is True
    thin = {"n_graded": 3, "win_rate": 1.0, "n_fires": 3, "pnl": 1}
    assert hrm.research_survivor(disc=disc, hold=thin) is False
    miss = {"n_graded": 10, "win_rate": 0.40, "n_fires": 10, "pnl": -2}
    assert hrm.research_survivor(disc=disc, hold=miss) is False
    v = hrm.live_keep(n_fires=11, win_rate=0.80)
    assert v["label"] == "KILL" and v["thin"] is True
    fat = hrm.live_keep(n_fires=40, win_rate=0.60)
    assert fat["keep"] is True


def test_run_synthetic_picks_holdout_not_full_sample() -> None:
    """A sleeve that only wins before cutoff is not a survivor."""
    # 10 sessions; cutoff = dates[7] = 2026-08-24.
    # Hard-red: first 6 days + last 2. Long AAA wins only on early reds
    # (close > open) and loses on holdout reds.
    rec = fm.make_recipe("t_hot", universe="ohlc_hot", hold=1, top_n=1)
    rows, bars, regime = [], {}, {}
    red_early = DATES[:6]
    red_late = DATES[-2:]
    for d in DATES:
        rows.append(_row(d, "AAA", sources=["ohlc_hot"]))
        if d in red_early:
            regime[d] = {"predict_score": -5.0}
            bars[("AAA", d)] = {"open": 10.0, "high": 11.0, "low": 9.8,
                                "close": 11.0}
        elif d in red_late:
            regime[d] = {"predict_score": -5.0}
            bars[("AAA", d)] = {"open": 10.0, "high": 10.1, "low": 8.0,
                                "close": 8.0}
        else:
            regime[d] = {"predict_score": 1.0}
            bars[("AAA", d)] = {"open": 10.0, "high": 10.2, "low": 9.9,
                                "close": 10.1}
    panel = _panel(rows)
    payload = hrm.run(
        from_date=DATES[0], to_date=DATES[-1], write=False,
        panel=panel, bars=bars, fees=ZERO_FEES, regime=regime,
        recipes=[rec], combo_specs=[], cash=False)
    assert payload["cutoff"] == DATES[7]
    assert payload["hard_red_n"] == 8
    hot = [r for r in payload["survivors"] if r["name"] == "t_hot"]
    assert hot == [], payload.get("survivors")
    scored = next(r for r in payload["disc_leaders"] if r["name"] == "t_hot")
    assert scored["disc"]["win_rate"] == 1.0
    assert scored["holdout"]["win_rate"] == 0.0
    assert scored["research_ok"] is False


def test_default_callers_still_sit() -> None:
    import inspect
    sig = inspect.signature(fmc.simulate_shared)
    assert sig.parameters["hard_red_mode"].default == fmc.HARD_RED_SIT
    from src import combo_broker as cb
    sig = inspect.signature(cb.size_combo_tickets)
    assert sig.parameters["hard_red_mode"].default == fmc.HARD_RED_SIT
    assert fmb.BOOK_RULES["hard_red_no_new"] is True


def main() -> None:
    test_time_split_cutoff()
    test_disc_label_ok_blocks_horizon_spill()
    test_split_fires_hold_region()
    test_horizons_use_different_exits()
    test_allow_mode_takes_open_on_red()
    test_research_survivor_and_live_keep()
    test_run_synthetic_picks_holdout_not_full_sample()
    test_default_callers_still_sit()
    print("test_hard_red_strategy_mine: 8 ok")


if __name__ == "__main__":
    main()
