"""Quarantined sessions keep their dates in Factor Mine and walk-forward.

On those days the news packet, catalyst, judge, and map-heat are blank.
Price and Finviz tape stay, so hot4 and holdup still see the session.
Lane and the news-impact backtest still skip the whole date.

Run: python -m src.test_quarantine_sessions
"""
from __future__ import annotations

from src.quarantine_sessions import (
    dates,
    drop_sessions,
    filter_dates,
    is_quarantined,
    lane_artifact_blocked,
    lane_runs,
    reason,
)
from src.factor_mine import slice_panel as fm_slice
from src.walkforward_factor_mine import slice_panel as wf_slice


def _panel() -> dict:
    rows = [
        {"date": "2026-08-27", "ticker": "AAA"},
        {"date": "2026-09-24", "ticker": "BBB"},
        {"date": "2026-09-25", "ticker": "CCC"},
    ]
    return {
        "session_dates": ["2026-08-27", "2026-09-24", "2026-09-25"],
        "rows": rows,
        "by_date": {
            "2026-08-27": [rows[0]],
            "2026-09-24": [rows[1]],
            "2026-09-25": [rows[2]],
        },
        "n_sessions": 3,
        "n_rows": 3,
    }


def test_list_marks_2026_09_24_stale_news() -> None:
    assert "2026-09-24" in dates()
    assert is_quarantined("2026-09-24")
    assert not is_quarantined("2026-08-27")
    assert reason("2026-09-24") == "stale_dated"
    assert filter_dates(["2026-08-27", "2026-09-24", "2026-09-25"]) == [
        "2026-08-27", "2026-09-25",
    ]


def test_drop_sessions_keeps_neighbors() -> None:
    out, dropped = drop_sessions(_panel())
    assert dropped == ["2026-09-24"]
    assert out["session_dates"] == ["2026-08-27", "2026-09-25"]
    assert [r["ticker"] for r in out["rows"]] == ["AAA", "CCC"]
    assert "2026-09-24" not in out["by_date"]
    assert out["n_sessions"] == 2
    # A panel with no quarantined day is returned unchanged.
    other = {"session_dates": ["2026-08-27"], "rows": [{"date": "2026-08-27"}]}
    same, none = drop_sessions(other)
    assert same is other and none == []


def test_factor_mine_and_walkforward_slices_keep_the_day() -> None:
    """A shared slice does not strip the date before a price-only score."""
    fm = fm_slice(_panel(), "2026-08-27", "2026-09-25")
    wf = wf_slice(_panel(), "2026-08-27", "2026-09-25")
    assert "2026-09-24" in fm["session_dates"]
    assert "2026-09-24" in wf["session_dates"]
    assert fm["n_rows"] == 3 and wf["n_rows"] == 3


def _ready(panel: dict) -> dict:
    panel = dict(panel)
    panel["_ohlc_filled"] = True
    panel["_tape_filled"] = True
    panel["_clock_b"] = True
    panel["_oppset"] = True
    return panel


def _eighteen_panel() -> dict:
    days = ["2026-08-27", *sorted(dates())]
    rows = []
    for d in days:
        news = "bad" if d == "2026-08-27" else "good"
        rows.append({
            "date": d,
            "ticker": "AAA",
            "sources": ["union", "ohlc_hot"],
            "src_rank": 1,
            "boxes": {
                "join": "good", "sector": "neutral", "gen": "neutral",
                "news": news, "digest": news, "judge": "good",
                "ab": "neutral", "peer": "good", "heat": "good",
                "vol": "good", "catal": "good", "buy": "neutral",
            },
            "blue": True,
            "alarm": False,
            "zero_red": False,
            "cond_good": 6,
            "cond_bad": 0,
            "news_prior": news,
            "news_box": news,
            "news_export_date": "2026-08-28",
            "ohlc_hot_score": 9.5,
            "ohlc_ret_5": 4.0,
            "last_green": True,
            "ins_buy": True,
            "form4_buy": False,
        })
    return _ready({
        "session_dates": days,
        "rows": rows,
        "by_date": {r["date"]: [r] for r in rows},
        "n_sessions": len(days),
        "n_rows": len(rows),
    })


def test_quarantine_nulls_packet_and_keeps_price_tape() -> None:
    """hot4 and holdup still score every quarantined day.

    News, catalyst, judge, and map-heat on those days are missing.
    A news-red short finds no pick there. Lane and news-impact still
    skip the whole date.
    """
    from src.factor_mine import build_recipes, score_recipe, scrub_quarantine_inputs
    from src.factor_mine_book import simulate_book
    from src.lane_news_scan import harvest
    from src.news_impact.backtest import load_corpus

    bad = sorted(dates())
    assert len(bad) == 18
    panel = _eighteen_panel()
    raw = next(r for r in panel["rows"] if r["date"] == "2026-09-24")
    scrubbed = scrub_quarantine_inputs(panel)
    assert scrubbed["session_dates"] == panel["session_dates"]
    assert scrubbed["n_rows"] == panel["n_rows"]
    # The source row is left intact.
    assert raw["boxes"]["news"] == "good"
    assert raw["ohlc_hot_score"] == 9.5
    clean = next(r for r in scrubbed["rows"] if r["date"] == "2026-08-27")
    hit = next(r for r in scrubbed["rows"] if r["date"] == "2026-09-24")
    assert clean["boxes"]["news"] == "bad"
    assert clean["news_box"] == "bad"
    for cam in ("news", "digest", "judge", "heat", "catal"):
        assert hit["boxes"][cam] == "missing", cam
    assert hit["boxes"]["vol"] == "good"
    assert hit["news_box"] == "missing"
    assert hit["news_prior"] == "missing"
    assert hit["news_export_date"] is None
    assert hit["ins_buy"] is False
    assert hit["ohlc_hot_score"] == 9.5
    assert hit["ohlc_ret_5"] == 4.0
    assert hit["last_green"] is True
    # Book tally no longer counts the blanked cameras. vol/join/peer stay green.
    assert hit["cond_good"] == 3
    assert hit["cond_bad"] == 0

    recs = {r["name"]: r for r in build_recipes()}
    hot4 = recs["union_hot_n4_h1"]
    holdup = recs["union_hot_n4_holdup"]
    news = recs["short_news_r_h3"]
    tapes = {"gainers": {}, "losers": {}}
    regime = {d: {"predict_score": 0.0} for d in panel["session_dates"]}
    hot_daily = {d["date"]: d for d in score_recipe(panel, hot4, tapes, bars={})["daily"]}
    hold_daily = {d["date"]: d for d in score_recipe(panel, holdup, tapes, bars={})["daily"]}
    news_daily = {d["date"]: d for d in score_recipe(panel, news, tapes, bars={})["daily"]}
    import src.factor_mine as fm_mod
    real_closed = fm_mod.session_has_closed
    fm_mod.session_has_closed = lambda date, now=None: bool(date)
    try:
        hot_book = {
            d["date"]: d for d in simulate_book(
                panel, hot4, bars={}, fees={}, regime=regime)["daily"]
        }
        wf_book = {
            d["date"]: d for d in simulate_book(
                wf_slice(panel, "2026-08-27", "2026-09-24"),
                holdup, bars={}, fees={}, regime=regime)["daily"]
        }
    finally:
        fm_mod.session_has_closed = real_closed
    for day in bad:
        assert day in hot_daily and hot_daily[day]["n"] == 1, day
        assert hot_daily[day]["tickers"] == ["AAA"]
        assert day in hold_daily and hold_daily[day]["n"] == 1, day
        assert day in hot_book and hot_book[day]["n"] == 1, day
        assert day in wf_book and wf_book[day]["n"] == 1, day
        assert day in news_daily and news_daily[day]["n"] == 0, day
    assert news_daily["2026-08-27"]["n"] == 1
    assert news_daily["2026-08-27"]["tickers"] == ["AAA"]

    assert harvest("2026-09-24") == []
    assert load_corpus("2026-09-24") == []


def test_lane_run_35823365502_and_the_tails_it_read() -> None:
    """The only Lane harvest that read the carry-forward parses."""
    runs = lane_runs()
    assert [r["run_id"] for r in runs] == ["35823365502"]
    assert lane_artifact_blocked("03_scoreboard/LANE_ONE_SHOT_100.md")
    assert lane_artifact_blocked("02_lessons/lane/one_shot_100/a16b5b75a698e098.json")
    assert not lane_artifact_blocked("03_scoreboard/LANE_ONE_SHOT_GOLD.md")
    stale_dated = {
        "2026-08-31", "2026-09-01", "2026-09-02", "2026-09-03",
        "2026-09-04", "2026-09-08", "2026-09-24",
    }
    undated_finviz = {
        "2026-09-09", "2026-09-10", "2026-09-11", "2026-09-14",
        "2026-09-15", "2026-09-16", "2026-09-17", "2026-09-18",
        "2026-09-21", "2026-09-22", "2026-09-23",
    }
    assert dates() == stale_dated | undated_finviz
    for day in stale_dated:
        assert is_quarantined(day), day
        assert reason(day) == "stale_dated"
    for day in undated_finviz:
        assert is_quarantined(day), day
        assert reason(day) == "undated_finviz"
    # August cluster's own files stay. No parsed file in the scan was CLEAN.
    assert not is_quarantined("2026-08-27")
    assert not is_quarantined("2026-08-28")
    assert not is_quarantined("2026-09-25")


def test_lane_harvest_and_news_readers_skip_the_day() -> None:
    from src.lane_news_scan import harvest, list_session_dates
    from src.news_grade import _signal_dates
    from src.news_impact.backtest import load_corpus

    assert "2026-09-24" not in list_session_dates()
    assert "2026-09-23" not in list_session_dates()
    assert "2026-08-27" in list_session_dates()
    assert harvest("2026-09-24") == []
    assert harvest("2026-09-08") == []
    assert harvest("2026-08-31") == []
    assert harvest("2026-09-17") == []
    assert "2026-09-24" not in _signal_dates(None)
    assert "2026-09-23" not in _signal_dates(None)
    assert load_corpus("2026-09-24") == []
    assert load_corpus("2026-09-23") == []
    # 2026-08-27 is outside the stale window and still loads.
    assert load_corpus("2026-08-27")
    from src.news_impact.corpus import load_all_sources
    raw, meta = load_all_sources("2026-09-24")
    assert raw == [] and meta.get("quarantined") == "2026-09-24"


if __name__ == "__main__":
    test_list_marks_2026_09_24_stale_news()
    test_lane_run_35823365502_and_the_tails_it_read()
    test_drop_sessions_keeps_neighbors()
    test_factor_mine_and_walkforward_slices_keep_the_day()
    test_quarantine_nulls_packet_and_keeps_price_tape()
    test_lane_harvest_and_news_readers_skip_the_day()
    print("6 tests passed")
