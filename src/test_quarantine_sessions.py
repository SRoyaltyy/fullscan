"""Quarantined sessions stay out of news evidence.

Price-only factor-mine and walk-forward recipes keep every date.
A news recipe drops all of them. Lane and the news-impact backtest
still skip the whole date.

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
    rows = [{"date": d, "ticker": "AAA", "boxes": {}} for d in days]
    return _ready({
        "session_dates": days,
        "rows": rows,
        "by_date": {d: [r] for d, r in zip(days, rows)},
        "n_sessions": len(days),
        "n_rows": len(rows),
    })


def test_price_recipe_keeps_all_18_dates_news_recipe_drops_them() -> None:
    """union_hot_n4_h1 keeps the 18 sessions; short_news_r_h3 drops them.

    Lane harvest and the news-impact loader still skip the whole date.
    A shared book that includes a news member drops the dates too.
    """
    from src.factor_mine import build_recipes, recipe_uses_news, score_recipe
    from src.factor_mine_book import simulate_book
    from src.factor_mine_combo import simulate_shared
    from src.lane_news_scan import harvest
    from src.news_impact.backtest import load_corpus

    bad = sorted(dates())
    assert len(bad) == 18
    panel = _eighteen_panel()
    recs = {r["name"]: r for r in build_recipes()}
    price = recs["union_hot_n4_h1"]
    news = recs["short_news_r_h3"]
    assert not recipe_uses_news(price)
    assert not recipe_uses_news(recs["union_vol_g_h1"])
    assert recipe_uses_news(news)
    assert recipe_uses_news(recs["union_h3_exit_news_r"])
    # Forbidding news=bad still reads the news camera.
    assert recipe_uses_news(recs["union_join_vol_green_h1"])

    fm = fm_slice(panel, "2026-08-27", "2026-09-24")
    wf = wf_slice(panel, "2026-08-27", "2026-09-24")
    for day in bad:
        assert day in fm["session_dates"], day
        assert day in wf["session_dates"], day

    tapes = {"gainers": {}, "losers": {}}
    regime = {d: {"predict_score": 0.0} for d in panel["session_dates"]}
    price_scored = [d["date"] for d in score_recipe(panel, price, tapes, bars={})["daily"]]
    news_scored = [d["date"] for d in score_recipe(panel, news, tapes, bars={})["daily"]]
    import src.factor_mine as fm_mod
    real_closed = fm_mod.session_has_closed
    fm_mod.session_has_closed = lambda date, now=None: bool(date)
    try:
        price_book = [
            d["date"] for d in simulate_book(
                panel, price, bars={}, fees={}, regime=regime)["daily"]
        ]
        news_book = [
            d["date"] for d in simulate_book(
                wf, news, bars={}, fees={}, regime=regime)["daily"]
        ]
    finally:
        fm_mod.session_has_closed = real_closed
    for day in bad:
        assert day in price_scored, day
        assert day in price_book, day
        assert day not in news_scored, day
        assert day not in news_book, day
    assert "2026-08-27" in news_scored
    assert "2026-08-27" in news_book

    mixed = simulate_shared(
        panel, [news, price], [1, 1], bars={}, fees={}, regime=regime)
    price_mix = simulate_shared(
        panel, [price, recs["flatten_h5"]], [1, 1],
        bars={}, fees={}, regime=regime)
    mixed_days = {d["date"] for d in mixed["daily"]}
    price_mix_days = {d["date"] for d in price_mix["daily"]}
    for day in bad:
        assert day not in mixed_days, day
        assert day in price_mix_days, day
    assert "2026-08-27" in mixed_days

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
    test_price_recipe_keeps_all_18_dates_news_recipe_drops_them()
    test_lane_harvest_and_news_readers_skip_the_day()
    print("6 tests passed")
