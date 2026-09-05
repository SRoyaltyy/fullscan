"""Leak-free factor strategy miner — unit tests, no full lookback scan."""
from __future__ import annotations

from src import factor_mine as fm
from src import ohlc_ripper as ohlc


CAL = [
    "2026-08-13", "2026-08-14", "2026-08-17", "2026-08-18",
    "2026-08-19", "2026-08-20", "2026-08-21",
]


def test_hold_window_includes_entry_day() -> None:
    # Buy 8-17 09:30, hold 3 → grade 8-17, 8-18, 8-19.
    assert fm.hold_window(CAL, "2026-08-17", 3) == [
        "2026-08-17", "2026-08-18", "2026-08-19",
    ]
    assert fm.hold_window(CAL, "2026-08-17", 1) == ["2026-08-17"]
    assert fm.hold_window(CAL, "2026-08-21", 3) == ["2026-08-21"]
    assert fm.hold_window(CAL, "2026-01-01", 3) == []


def test_feature_export_is_always_prior_session() -> None:
    for d in CAL[1:]:
        prior = fm.feature_export_date(CAL, d)
        assert prior is not None
        assert prior < d
        assert prior in CAL
    assert fm.feature_export_date(CAL, CAL[0]) is None


def test_prior_news_tone() -> None:
    assert fm.prior_news_tone("") == "missing"
    assert fm.prior_news_tone(None) == "missing"
    assert fm.prior_news_tone("FDA approves Phase 3 trial") == "good"
    assert fm.prior_news_tone("Analyst downgrade, cuts target") == "bad"
    assert fm.prior_news_tone("Company updates outlook") == "neutral"
    assert fm.prior_news_tone("Beat estimates after downgrade") == "neutral"


def test_input_news_prefers_morning_box_over_headline() -> None:
    assert fm.input_news_tone("good", "downgrade warning") == "good"
    assert fm.input_news_tone("missing", "FDA approves drug") == "good"
    assert fm.input_news_tone("missing", "") == "missing"
    assert fm.input_news_tone(None, "lawsuit and probe") == "bad"


def test_matches_ryg_presence_and_ignores_same_day_change() -> None:
    row = {
        "ticker": "AAA",
        "sources": ["union", "yday_gainer"],
        "boxes": {"vol": "good", "news": "good", "join": "neutral",
                  "ab": "missing", "catal": "missing"},
        "blue": True,
        "alarm": False,
        "zero_red": True,
        "last_green": True,
        "last_red": False,
        "ohlc_ret_5": 4.0,
        "ohlc_rvol": 1.1,
        "change_pct": 19.4,  # same-day outcome — must not be a gate
        "Gap": 8.0,
        "RelVol": 12.0,
    }
    rec = fm.make_recipe("t", universe="yday_gainer",
                         require={"vol": "good", "news_present": True, "blue": True},
                         forbid={"alarm": True})
    assert fm.matches(row, rec) is True
    rec_bad = fm.make_recipe("t", require={"vol": "bad"})
    assert fm.matches(row, rec_bad) is False
    rec_alarm = fm.make_recipe("t", universe="yday_gainer", forbid={"alarm": True})
    row_alarm = dict(row, alarm=True)
    assert fm.matches(row_alarm, rec_alarm) is False
    rec_present = fm.make_recipe("t", require={"join_present": True})
    assert fm.matches(row, rec_present) is True
    rec_catal = fm.make_recipe("t", require={"catal_present": True})
    assert fm.matches(row, rec_catal) is False
    # Planted same-day tape must not flip a match.
    rec_plain = fm.make_recipe("t", universe="yday_gainer")
    assert fm.matches(row, rec_plain) is True
    assert "change" not in fm.INPUT_FIELDS
    assert "Gap" not in fm.INPUT_FIELDS
    assert "RelVol" not in fm.INPUT_FIELDS


def test_matches_coil_and_short_alarm() -> None:
    row = {
        "ticker": "BBB",
        "sources": ["ohlc_hot"],
        "boxes": {"news": "missing"},
        "alarm": True,
        "ohlc_ret_5": 22.0,
        "ohlc_rvol": 3.4,
    }
    coil = fm.make_recipe("c", universe="ohlc_hot",
                          require={"ret_5_min": 0.0, "ret_5_max": 10.0})
    assert fm.matches(row, coil) is False
    short = fm.make_recipe("s", universe="union", side="short",
                           require={"alarm": True})
    assert fm.matches(row, short) is True


def test_ohlc_and_candles_are_strictly_prior() -> None:
    feat = ohlc.features("TLN", "2026-08-17")
    bars = ohlc.prior_bars("TLN", "2026-08-17")
    assert bars
    assert all(b["date"] < "2026-08-17" for b in bars)
    from src import candle_factor as cf
    cbars = cf.prior_bars("TLN", "2026-08-17")
    assert cbars
    assert all(b["date"] < "2026-08-17" for b in cbars)
    assert feat.get("asof") == "2026-08-17"


def test_score_recipe_six_metrics_and_start_dates() -> None:
    # Synthetic 4-session panel. AAA wins most days; BBB is the 8-13 pothole.
    cal = CAL[:4]
    def row(date, ticker, **kw):
        base = {
            "date": date, "ticker": ticker, "sources": ["union"],
            "boxes": {"vol": "good", "news": "missing"},
            "blue": False, "alarm": False, "zero_red": True,
            "cond_good": 1, "cond_bad": 0, "last_green": True, "last_red": False,
            "ohlc_ret_5": 3.0, "ohlc_rvol": 1.0, "ohlc_hot_score": 1.0,
            "src_rank": 0,
        }
        base.update(kw)
        return base
    rows = [
        row("2026-08-13", "AAA"),
        row("2026-08-13", "BBB"),
        row("2026-08-14", "AAA"),
        row("2026-08-17", "AAA"),
        row("2026-08-18", "AAA"),
    ]
    by_date = {}
    for r in rows:
        by_date.setdefault(r["date"], []).append(r)
    panel = {"session_dates": cal, "rows": rows, "by_date": by_date}
    # Prices: AAA +2% each close; BBB −8% on 8-13 then flat.
    bars = {
        ("AAA", "2026-08-13"): {"open": 100, "close": 102},
        ("AAA", "2026-08-14"): {"open": 102, "close": 104},
        ("AAA", "2026-08-17"): {"open": 104, "close": 106},
        ("AAA", "2026-08-18"): {"open": 106, "close": 108},
        ("BBB", "2026-08-13"): {"open": 100, "close": 92},
        ("BBB", "2026-08-14"): {"open": 92, "close": 92},
        ("BBB", "2026-08-17"): {"open": 92, "close": 92},
        ("BBB", "2026-08-18"): {"open": 92, "close": 92},
    }
    tapes = {
        "gainers": {
            "2026-08-13": {"AAA"},
            "2026-08-14": {"AAA"},
            "2026-08-17": set(),
            "2026-08-18": set(),
        },
        "losers": {
            "2026-08-13": {"BBB"},
            "2026-08-14": set(),
            "2026-08-17": set(),
            "2026-08-18": set(),
        },
    }
    rec = fm.make_recipe("union_h1", universe="union", hold=1, top_n=8)
    stats = fm.score_recipe(panel, rec, tapes, bars=bars)
    assert stats["n_picks"] == 5
    assert stats["n_graded"] == 5
    assert stats["win_rate"] is not None
    assert stats["win_rate"] > 0.5  # AAA wins 4/5; BBB loses 1
    assert stats["profitable_day_rate"] is not None
    assert 0 <= stats["profitable_day_rate"] <= 1
    assert stats["avg_win_pct"] is not None and stats["avg_win_pct"] > 0
    assert stats["avg_loss_pct"] is not None and stats["avg_loss_pct"] < 0
    assert stats["gainer_hits"] == 2  # AAA on 8-13 and 8-14 only
    assert stats["loser_hits"] == 1   # BBB on 8-13
    assert stats["start_n"] == 4
    assert stats["start_green"] >= 1
    assert stats["start_rate"] is not None
    assert len(stats["equity"]) == 5  # t0 + 4 days
    assert stats["effectiveness"] is not None
    assert "reliable" in stats
    assert "pothole_pct" in stats
    assert "median_start_pct" in stats


def test_short_flips_sign_and_early_exit_uses_open() -> None:
    cal = ["2026-08-17", "2026-08-18", "2026-08-19"]
    rows = [
        {"date": "2026-08-17", "ticker": "CCC", "sources": ["union"],
         "boxes": {"news": "good"}, "alarm": False, "last_red": False},
        {"date": "2026-08-18", "ticker": "CCC", "sources": ["union"],
         "boxes": {"news": "good"}, "alarm": True, "last_red": False},
        {"date": "2026-08-19", "ticker": "CCC", "sources": ["union"],
         "boxes": {"news": "good"}, "alarm": False, "last_red": False},
    ]
    idx = {(r["date"], r["ticker"]): r for r in rows}
    bars = {
        ("CCC", "2026-08-17"): {"open": 100, "close": 101},
        ("CCC", "2026-08-18"): {"open": 99, "close": 90},
        ("CCC", "2026-08-19"): {"open": 90, "close": 80},
    }
    # Hold 3, exit on alarm → sell at 8-18 09:30 open (99), not the 90 close.
    ret = fm.hold_return(
        "CCC", "2026-08-17", 3, cal, "long", {"alarm": True}, idx, bars=bars,
    )
    assert ret == round(100.0 * (99 / 100 - 1.0), 4)
    # No exit: 8-19 close 80.
    ret2 = fm.hold_return(
        "CCC", "2026-08-17", 3, cal, "long", {}, idx, bars=bars,
    )
    assert ret2 == round(100.0 * (80 / 100 - 1.0), 4)
    # Short the same hold-to-close path.
    ret3 = fm.hold_return(
        "CCC", "2026-08-17", 3, cal, "short", {}, idx, bars=bars,
    )
    assert ret3 == round(-100.0 * (80 / 100 - 1.0), 4)


def test_pothole_and_thin_sample_are_downranked() -> None:
    jackpot = fm._effectiveness(
        0.55, 0.5, 0.76, 0.1, 0.7, 3.0, 154.0, 3.0, 158.0, True)
    steady = fm._effectiveness(
        0.62, 0.80, 1.0, 0.05, 0.38, 2.0, 43.0, 16.0, 6.0, True)
    thin = fm._effectiveness(
        0.50, 1.0, 1.0, 0.0, 0.1, 2.0, 3.0, 3.0, 5.0, False)
    assert jackpot < steady
    assert thin < steady


def test_cash_book_whole_shares_fees_and_hard_red() -> None:
    from src import factor_mine_book as fmb
    from src import paper_trade as pt
    cal = ["2026-08-17", "2026-08-18", "2026-08-19"]
    def row(date, ticker, **kw):
        r = {
            "date": date, "ticker": ticker, "sources": ["union"],
            "boxes": {"vol": "good"}, "blue": False, "alarm": False,
            "zero_red": True, "last_green": True, "last_red": False,
            "ohlc_ret_5": 3.0, "ohlc_rvol": 1.0, "ohlc_hot_score": 1.0,
            "src_rank": 0, "cond_good": 1, "cond_bad": 0,
        }
        r.update(kw)
        return r
    rows = [
        row("2026-08-17", "CHEAP"),
        row("2026-08-17", "DEAR"),
        row("2026-08-18", "CHEAP"),
        row("2026-08-19", "CHEAP"),
    ]
    by_date = {}
    for r in rows:
        by_date.setdefault(r["date"], []).append(r)
    panel = {"session_dates": cal, "rows": rows, "by_date": by_date}
    bars = {
        ("CHEAP", "2026-08-17"): {"open": 10, "close": 11},
        ("CHEAP", "2026-08-18"): {"open": 11, "close": 12},
        ("CHEAP", "2026-08-19"): {"open": 12, "close": 12},
        ("DEAR", "2026-08-17"): {"open": 9000, "close": 9000},
        ("DEAR", "2026-08-18"): {"open": 9000, "close": 9000},
        ("DEAR", "2026-08-19"): {"open": 9000, "close": 9000},
    }
    rec = fm.make_recipe("union_h1", hold=1, top_n=8)
    fees = pt.load_fees()
    book = fmb.simulate_book(panel, rec, bars=bars, fees=fees, regime={})
    ticks_bought = [t["ticker"] for t in book["trades"] if t["side"] == "BUY"]
    assert "CHEAP" in ticks_bought
    assert "DEAR" not in ticks_bought  # leftover split cannot buy 1 share of $9000
    assert any(k["kind"] == "cash" and k["ticker"] == "DEAR" for k in book["skips"])
    cheap_buy = next(t for t in book["trades"] if t["ticker"] == "CHEAP" and t["side"] == "BUY")
    assert cheap_buy["shares"] >= 1
    assert cheap_buy["fees"] > 0
    assert cheap_buy.get("equity_after") is not None
    assert cheap_buy["equity_after"] == round(
        cheap_buy["cash_after"] + cheap_buy["shares"] * 10, 2)
    assert cheap_buy["equity_after"] < 10_000  # Futubull fees
    assert cheap_buy["equity_delta"] < 0
    # Hard-red sit: no new buys on 8-17.
    red = fmb.simulate_book(
        panel, rec, bars=bars, fees=fees,
        regime={"2026-08-17": {"predict_score": -6.2}},
    )
    assert all(t["date"] != "2026-08-17" or t["side"] != "BUY" for t in red["trades"])
    assert any(k["kind"] == "hard_red" for k in red["skips"])


def test_min_hold_blocks_sell_until_floor() -> None:
    from src import factor_mine_book as fmb
    from src import paper_trade as pt
    cal = ["2026-08-17", "2026-08-18", "2026-08-19"]
    rows = [
        {"date": "2026-08-17", "ticker": "AAA", "sources": ["union"],
         "boxes": {}, "alarm": False, "last_red": False, "src_rank": 0},
        {"date": "2026-08-18", "ticker": "BBB", "sources": ["union"],
         "boxes": {}, "alarm": False, "last_red": False, "src_rank": 0},
        {"date": "2026-08-19", "ticker": "BBB", "sources": ["union"],
         "boxes": {}, "alarm": False, "last_red": False, "src_rank": 0},
    ]
    by_date = {}
    for r in rows:
        by_date.setdefault(r["date"], []).append(r)
    panel = {"session_dates": cal, "rows": rows, "by_date": by_date}
    bars = {("AAA", d): {"open": 10, "close": 10} for d in cal}
    bars.update({("BBB", d): {"open": 10, "close": 10} for d in cal})
    rec = fm.make_recipe("union_h3", hold=3, top_n=1)
    book = fmb.simulate_book(panel, rec, bars=bars, fees=pt.load_fees(), regime={})
    sold = [t for t in book["trades"] if t["side"] == "SELL"]
    assert sold == []  # AAA still locked on 8-18/8-19 (held < 3)
    assert any(k["kind"] == "min_hold" for k in book["skips"])


def test_action_dropdown_auto_tweaks_neighbors() -> None:
    from src import factor_mine_book as fmb
    only = fmb.recipes_from_action(
        universe="flatten", hold="3", gate="none", rank="none",
        side="long", top_n="8", exit="none", auto_tweak=False)
    names = {r["name"] for r in only}
    assert "flatten_h3" in names
    tweaked = fmb.recipes_from_action(
        universe="flatten", hold="3", gate="none", rank="none",
        side="long", top_n="8", exit="none", auto_tweak=True)
    tnames = {r["name"] for r in tweaked}
    assert "flatten_h3" in tnames
    assert len(tnames) > len(names)  # nearby holds / gates
    live = fmb.recipes_from_action(
        universe="flatten", hold="auto", gate="auto", rank="auto",
        side="long", top_n="auto", exit="auto", entry="live",
        auto_tweak=False)
    assert all((r.get("require") or {}).get("live_entry") for r in live)
    assert any(r["name"].startswith("flatten_live") for r in live)


def test_src_rank_zero_is_first_not_last() -> None:
    rec = fm.make_recipe("flatten_h5", universe="flatten", hold=5, top_n=8)
    rows = [
        {"ticker": "BTSG", "sources": ["flatten"], "src_rank": 0, "boxes": {}},
        {"ticker": "IREN", "sources": ["flatten"], "src_rank": 1, "boxes": {}},
        {"ticker": "VOR", "sources": ["flatten"], "src_rank": 8, "boxes": {}},
    ]
    # nine names, top 8 must keep rank 0 and drop rank 8
    extra = [{"ticker": f"X{i}", "sources": ["flatten"], "src_rank": i,
              "boxes": {}} for i in range(2, 8)]
    picked = [r["ticker"] for r in fm.pick_day(rows + extra, rec)]
    assert picked[0] == "BTSG"
    assert "VOR" not in picked
    assert len(picked) == 8


def test_live_entry_skips_hold_mornings() -> None:
    rec_wish = fm.make_recipe("flatten_h1", universe="flatten", hold=1)
    rec_live = fm.make_recipe(
        "flatten_live_h1", universe="flatten", hold=1,
        require={"live_entry": True})
    hold_row = {
        "date": "2026-08-13", "ticker": "IREN", "sources": ["flatten"],
        "boxes": {}, "flatten_ok": False, "src_rank": 0,
    }
    fire_row = dict(hold_row, flatten_ok=True, date="2026-08-20")
    assert fm.matches(hold_row, rec_wish) is True
    assert fm.matches(hold_row, rec_live) is False
    assert fm.matches(fire_row, rec_live) is True


def test_short_book_marks_liability_and_cover() -> None:
    from src import factor_mine_book as fmb
    from src import paper_trade as pt
    cal = ["2026-08-17", "2026-08-18"]
    rows = [
        {"date": "2026-08-17", "ticker": "AAA", "sources": ["union"],
         "boxes": {}, "alarm": True, "src_rank": 0},
        {"date": "2026-08-18", "ticker": "AAA", "sources": ["union"],
         "boxes": {}, "alarm": True, "src_rank": 0},
    ]
    by_date = {}
    for r in rows:
        by_date.setdefault(r["date"], []).append(r)
    panel = {"session_dates": cal, "rows": rows, "by_date": by_date}
    bars = {
        ("AAA", "2026-08-17"): {"open": 100, "close": 100},
        ("AAA", "2026-08-18"): {"open": 90, "close": 90},
    }
    rec = fm.make_recipe("short_alarm_h1", hold=1, side="short",
                         require={"alarm": True}, top_n=1)
    book = fmb.simulate_book(panel, rec, bars=bars, fees=pt.load_fees(),
                             regime={})
    d0 = book["daily"][0]
    # Liability mark: stock is negative; equity stays near $10k, not $20k.
    assert d0["stock"] < 0
    assert 8_000 < d0["equity"] < 11_000
    assert book["total_ret_pct"] < 80  # no fantasy +300% from inverted mark
    shorts = [t for t in book["trades"] if t["side"] == "SHORT"]
    assert shorts
    # 2× cover: one name gets ≤ 50% of equity.
    assert shorts[0]["shares"] * shorts[0]["price"] <= 5_500


def test_recipes_cover_holds_shorts_and_exits() -> None:
    recs = fm.build_recipes()
    names = {r["name"] for r in recs}
    assert len(recs) >= 80
    assert any(r["hold"] == 1 for r in recs)
    assert any(r["hold"] == 3 for r in recs)
    assert any(r["hold"] == 5 for r in recs)
    assert any(r["side"] == "short" for r in recs)
    assert any(r.get("exit_when") for r in recs)
    assert any((r.get("require") or {}).get("news_present") for r in recs)
    assert "union_h3_exit_alarm" in names
    assert "short_alarm_h1" in names
    assert "union_w_hot_cond_h1" in names
    assert "flatten_live_h5" in names
    assert "flatten_h5_rankw" in names
    assert "flatten_h5_sboost" in names
    assert "union_h3_time" in names
    assert any((r.get("require") or {}).get("live_entry") for r in recs)
    assert any((r.get("size") or "leftover") == "rank_w" for r in recs)
    assert any((r.get("sell") or "list") == "time" for r in recs)
    assert any((r.get("s_boost") or "none") == "both" for r in recs)
    assert len(recs) >= 100


def test_template_has_data_slot() -> None:
    text = fm.TEMPLATE.read_text(encoding="utf-8")
    assert "__DATA__" in text
    assert "Win%" in text
    assert "Starts YES" in text
    assert "Blotter" in text
    assert "Open cash" in text
    assert "Audit" in text or "audit" in text
    assert "AvgW" in text
    assert "Equity" in text
    assert "selectSleeve" in text
    assert "this sleeve only" in text
    assert "Pick a sleeve" in text or "pick a sleeve" in text.lower()
    assert "leak-free" in text.lower() or "Leak-free" in text


def test_write_outputs_injects_payload(tmp_path=None) -> None:
    recs = [fm.make_recipe("demo_h1", hold=1)]
    panel = {
        "from_date": "2026-08-17", "to_date": "2026-08-17",
        "session_dates": ["2026-08-17"], "n_rows": 0, "n_sessions": 1,
        "rows": [], "by_date": {"2026-08-17": []},
    }
    payload = fm.run(
        "2026-08-17", "2026-08-17", write=False, recipes=recs, panel=panel,
        book=False,
    )
    assert payload["n_recipes"] == 1
    assert payload["live_untouched"] == "flatten_robust"
    assert "demo_h1" in payload["series"]
    assert payload["stats"][0]["name"] == "demo_h1"
    assert "win_rate" in payload["stats"][0]
    assert "profitable_day_rate" in payload["stats"][0]
    assert "start_rate" in payload["stats"][0]
    assert "gainer_hits" in payload["stats"][0]
    assert "loser_hits" in payload["stats"][0]
    assert "avg_win_pct" in payload["stats"][0]
    assert payload["stats"][0]["reliable"] is False  # empty book is thin


def test_dash_payload_ships_every_book_and_features_high_return() -> None:
    from src import paper_trade as pt
    cal = ["2026-08-17", "2026-08-18"]
    rows = [
        {"date": "2026-08-17", "ticker": "WIN", "sources": ["union"],
         "boxes": {}, "alarm": False, "src_rank": 0, "last_green": True},
        {"date": "2026-08-18", "ticker": "WIN", "sources": ["union"],
         "boxes": {}, "alarm": False, "src_rank": 0, "last_green": True},
    ]
    by_date = {}
    for r in rows:
        by_date.setdefault(r["date"], []).append(r)
    panel = {
        "from_date": "2026-08-17", "to_date": "2026-08-18",
        "session_dates": cal, "n_rows": 2, "n_sessions": 2,
        "rows": rows, "by_date": by_date,
    }
    bars = {
        ("WIN", "2026-08-17"): {"open": 10, "close": 12},
        ("WIN", "2026-08-18"): {"open": 12, "close": 13},
    }
    recs = [
        fm.make_recipe("high_ret_h1", hold=1, top_n=1),
        fm.make_recipe("also_h1", hold=1, top_n=1),
    ]
    payload = fm.run(
        "2026-08-17", "2026-08-18", write=False, recipes=recs, panel=panel,
        book=True, bars=bars,
    )
    assert set(payload["books"]) == {"high_ret_h1", "also_h1"}
    t0 = (payload["books"]["high_ret_h1"].get("trades") or [None])[0]
    assert t0 and t0.get("equity_after") is not None
    assert "cameras" not in t0
    assert "daily" not in payload["books"]["high_ret_h1"]
    assert payload["daily"]["high_ret_h1"]


def _panel(cal, rows, extra=None):
    by_date = {}
    for r in rows:
        by_date.setdefault(r["date"], []).append(r)
    out = {"session_dates": cal, "rows": rows, "by_date": by_date}
    if extra:
        out.update(extra)
    return out


def _row(date, ticker, **kw):
    r = {
        "date": date, "ticker": ticker, "sources": ["union"],
        "boxes": {"vol": "good"}, "blue": False, "alarm": False,
        "zero_red": True, "last_green": True, "last_red": False,
        "ohlc_ret_5": 3.0, "ohlc_rvol": 1.0, "ohlc_hot_score": 1.0,
        "src_rank": 0, "cond_good": 1, "cond_bad": 0,
    }
    r.update(kw)
    return r


def test_butterfly_day2_opens_at_day1_leftover() -> None:
    from src import factor_mine_book as fmb
    from src import paper_trade as pt
    cal = ["2026-08-17", "2026-08-18", "2026-08-19"]
    rows = [
        _row("2026-08-17", "AAA", src_rank=0),
        _row("2026-08-18", "AAA", src_rank=0),
        _row("2026-08-19", "AAA", src_rank=0),
    ]
    bars = {("AAA", d): {"open": 10, "close": 11} for d in cal}
    rec = fm.make_recipe("union_h3", hold=3, top_n=1)
    book = fmb.simulate_book(
        _panel(cal, rows), rec, bars=bars, fees=pt.load_fees(), regime={})
    d0, d1 = book["daily"][0], book["daily"][1]
    assert d0["open_cash"] == 10_000
    assert d0["open_held"] == []
    assert d1["open_cash"] == d0["cash"]
    assert any(h.startswith("AAA×") for h in d1["open_held"])
    assert d1["open_cash"] == book["daily"][0]["cash"]
    assert book["audit"]["ok"] is True
    # Cannot invent shares: every SELL is a name we opened.
    held_ever = set()
    for t in book["trades"]:
        if t["side"] == "BUY":
            held_ever.add(t["ticker"])
        if t["side"] == "SELL":
            assert t["ticker"] in held_ever


def test_audit_fails_on_unheld_sell_and_overspend() -> None:
    from src import factor_mine_book as fmb
    from src import paper_trade as pt
    cal = ["2026-08-17"]
    rows = [_row("2026-08-17", "AAA")]
    bars = {("AAA", "2026-08-17"): {"open": 10, "close": 10}}
    rec = fm.make_recipe("union_h1", hold=1, top_n=1)
    book = fmb.simulate_book(
        _panel(cal, rows), rec, bars=bars, fees=pt.load_fees(), regime={})
    assert book["audit"]["ok"] is True
    bad = dict(book)
    bad["trades"] = list(book["trades"]) + [{
        "date": "2026-08-17", "ticker": "ZZZ", "side": "SELL",
        "shares": 10, "price": 10, "fees": 0, "cash_after": 99_999,
    }]
    aud = fmb.audit_book(bad, capital=10_000, side="long")
    assert aud["ok"] is False
    assert any("ZZZ" in f for f in aud["fails"])
    spend = dict(book)
    spend["trades"] = list(book["trades"]) + [{
        "date": "2026-08-17", "ticker": "QQQ", "side": "BUY",
        "shares": 99999, "price": 100, "fees": 0, "cash_after": -1,
    }]
    aud2 = fmb.audit_book(spend, capital=10_000, side="long")
    assert aud2["ok"] is False
    assert any("cash" in f.lower() or "buy" in f.lower() for f in aud2["fails"])


def test_time_sell_exits_at_min_hold_even_if_listed() -> None:
    from src import factor_mine_book as fmb
    from src import paper_trade as pt
    cal = ["2026-08-17", "2026-08-18"]
    rows = [_row(d, "AAA") for d in cal]
    bars = {("AAA", d): {"open": 10, "close": 10} for d in cal}
    keep = fm.make_recipe("union_h1", hold=1, top_n=1, sell="list")
    timed = fm.make_recipe("union_h1_time", hold=1, top_n=1, sell="time")
    fees = pt.load_fees()
    b_keep = fmb.simulate_book(_panel(cal, rows), keep, bars=bars, fees=fees, regime={})
    b_time = fmb.simulate_book(_panel(cal, rows), timed, bars=bars, fees=fees, regime={})
    # list + still listed: no SELL on 8-18 (name never dropped).
    assert not any(t["side"] == "SELL" and t["date"] == "2026-08-18"
                   for t in b_keep["trades"])
    # time-stop: sell at min-hold even though AAA is still on the list.
    sold = [t for t in b_time["trades"] if t["side"] == "SELL"]
    assert sold and sold[0]["date"] == "2026-08-18"
    assert "time-stop" in (sold[0].get("reason") or "")
    assert b_time["audit"]["ok"] is True


def test_rank_w_gives_more_shares_to_first() -> None:
    from src import factor_mine_book as fmb
    from src import paper_trade as pt
    cal = ["2026-08-17"]
    rows = [
        _row("2026-08-17", "AAA", src_rank=0),
        _row("2026-08-17", "BBB", src_rank=1),
    ]
    bars = {
        ("AAA", "2026-08-17"): {"open": 10, "close": 10},
        ("BBB", "2026-08-17"): {"open": 10, "close": 10},
    }
    rec = fm.make_recipe("union_h1_rankw", hold=1, top_n=2, size="rank_w")
    book = fmb.simulate_book(
        _panel(cal, rows), rec, bars=bars, fees=pt.load_fees(), regime={})
    buys = {t["ticker"]: t["shares"] for t in book["trades"] if t["side"] == "BUY"}
    assert buys["AAA"] > buys["BBB"]
    leftover = fmb.split_budgets([{}, {}], 100.0, "leftover")
    rankw = fmb.split_budgets([{}, {}], 100.0, "rank_w")
    assert leftover[0] == leftover[1]
    assert rankw[0] > rankw[1]
    assert abs(sum(rankw) - 100.0) < 1e-9
    half = fmb.split_budgets([{}, {}], 100.0, "half")
    assert abs(sum(half) - 50.0) < 1e-9


def test_sboost_more_names_on_good_s_still_cash_capped() -> None:
    from src import factor_mine_book as fmb
    from src import paper_trade as pt
    cal = ["2026-08-17"]
    rows = [_row("2026-08-17", f"T{i}", src_rank=i) for i in range(12)]
    rows.append(_row("2026-08-17", "DEAR", src_rank=99))
    bars = {(f"T{i}", "2026-08-17"): {"open": 10, "close": 10} for i in range(12)}
    bars[("DEAR", "2026-08-17")] = {"open": 9000, "close": 9000}
    rec = fm.make_recipe(
        "union_h1_sboost", hold=1, top_n=4, s_boost="more_names")
    fees = pt.load_fees()
    good = fmb.simulate_book(
        _panel(cal, rows), rec, bars=bars, fees=fees,
        regime={"2026-08-17": {"predict_score": 8.5}})
    n_try = len([t for t in good["trades"] if t["side"] == "BUY"]) + len([
        k for k in good["skips"] if k["date"] == "2026-08-17" and k["kind"] == "cash"])
    assert n_try > 4  # top_n raised by +4
    spent = sum(
        t["shares"] * t["price"] + t["fees"]
        for t in good["trades"] if t["side"] == "BUY")
    assert spent <= 10_000 + 0.05
    assert all(t["ticker"] != "DEAR" or t["side"] != "BUY" for t in good["trades"])
    red = fmb.simulate_book(
        _panel(cal, rows), rec, bars=bars, fees=fees,
        regime={"2026-08-17": {"predict_score": -6.2}})
    assert all(t["side"] != "BUY" for t in red["trades"])
    assert any(k["kind"] == "hard_red" for k in red["skips"])
    assert good["audit"]["ok"] is True


def test_action_filters_size_sell_boost() -> None:
    from src import factor_mine_book as fmb
    only = fmb.recipes_from_action(
        universe="flatten", hold="5", gate="none", rank="none",
        side="long", top_n="8", exit="none", size="rank_w",
        sell="auto", s_boost="auto", auto_tweak=False)
    assert only and all((r.get("size") or "leftover") == "rank_w" for r in only)
    assert any(r["name"] == "flatten_h5_rankw" for r in only)


if __name__ == "__main__":
    test_hold_window_includes_entry_day()
    test_feature_export_is_always_prior_session()
    test_prior_news_tone()
    test_input_news_prefers_morning_box_over_headline()
    test_matches_ryg_presence_and_ignores_same_day_change()
    test_matches_coil_and_short_alarm()
    test_ohlc_and_candles_are_strictly_prior()
    test_score_recipe_six_metrics_and_start_dates()
    test_short_flips_sign_and_early_exit_uses_open()
    test_pothole_and_thin_sample_are_downranked()
    test_cash_book_whole_shares_fees_and_hard_red()
    test_min_hold_blocks_sell_until_floor()
    test_action_dropdown_auto_tweaks_neighbors()
    test_src_rank_zero_is_first_not_last()
    test_live_entry_skips_hold_mornings()
    test_short_book_marks_liability_and_cover()
    test_recipes_cover_holds_shorts_and_exits()
    test_template_has_data_slot()
    test_write_outputs_injects_payload()
    test_butterfly_day2_opens_at_day1_leftover()
    test_audit_fails_on_unheld_sell_and_overspend()
    test_time_sell_exits_at_min_hold_even_if_listed()
    test_rank_w_gives_more_shares_to_first()
    test_sboost_more_names_on_good_s_still_cash_capped()
    test_action_filters_size_sell_boost()
    test_dash_payload_ships_every_book_and_features_high_return()
    print("26 factor-mine tests passed")
