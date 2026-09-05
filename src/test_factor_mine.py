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


def test_template_has_data_slot() -> None:
    text = fm.TEMPLATE.read_text(encoding="utf-8")
    assert "__DATA__" in text
    assert "Win%" in text
    assert "Starts YES" in text
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
    test_recipes_cover_holds_shorts_and_exits()
    test_template_has_data_slot()
    test_write_outputs_injects_payload()
    print("12 factor-mine tests passed")
