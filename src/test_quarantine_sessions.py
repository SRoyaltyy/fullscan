"""2026-09-24 stale-news session stays out of research evidence.

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


def test_factor_mine_and_walkforward_slices_skip_the_day() -> None:
    fm = fm_slice(_panel(), "2026-08-27", "2026-09-25")
    wf = wf_slice(_panel(), "2026-08-27", "2026-09-25")
    assert "2026-09-24" not in fm["session_dates"]
    assert "2026-09-24" not in wf["session_dates"]
    assert fm["n_rows"] == 2 and wf["n_rows"] == 2


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
        "2026-09-09", "2026-09-11", "2026-09-15",
    }
    finviz_scrape_dated = {
        "2026-09-10", "2026-09-14", "2026-09-16", "2026-09-17",
        "2026-09-18", "2026-09-21", "2026-09-22", "2026-09-23",
    }
    assert dates() == stale_dated | undated_finviz | finviz_scrape_dated
    for day in stale_dated:
        assert is_quarantined(day), day
        assert reason(day) == "stale_dated"
    for day in undated_finviz:
        assert is_quarantined(day), day
        assert reason(day) == "undated_finviz"
    for day in finviz_scrape_dated:
        assert is_quarantined(day), day
        assert reason(day) == "finviz_scrape_dated"
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
    test_factor_mine_and_walkforward_slices_skip_the_day()
    test_lane_harvest_and_news_readers_skip_the_day()
    print("5 tests passed")
