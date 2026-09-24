"""2026-09-24 stale-news session stays out of research evidence.

Run: python -m src.test_quarantine_sessions
"""
from __future__ import annotations

from src.quarantine_sessions import (
    dates,
    drop_sessions,
    filter_dates,
    is_quarantined,
    reason,
)
from src.factor_mine import slice_panel as fm_slice
from src.walkforward_factor_mine import slice_panel as wf_slice


def _panel() -> dict:
    rows = [
        {"date": "2026-09-23", "ticker": "AAA"},
        {"date": "2026-09-24", "ticker": "BBB"},
        {"date": "2026-09-25", "ticker": "CCC"},
    ]
    return {
        "session_dates": ["2026-09-23", "2026-09-24", "2026-09-25"],
        "rows": rows,
        "by_date": {
            "2026-09-23": [rows[0]],
            "2026-09-24": [rows[1]],
            "2026-09-25": [rows[2]],
        },
        "n_sessions": 3,
        "n_rows": 3,
    }


def test_list_marks_2026_09_24_stale_news() -> None:
    assert "2026-09-24" in dates()
    assert is_quarantined("2026-09-24")
    assert not is_quarantined("2026-09-23")
    assert reason("2026-09-24") == "stale_news"
    assert filter_dates(["2026-09-23", "2026-09-24", "2026-09-25"]) == [
        "2026-09-23", "2026-09-25",
    ]


def test_drop_sessions_keeps_neighbors() -> None:
    out, dropped = drop_sessions(_panel())
    assert dropped == ["2026-09-24"]
    assert out["session_dates"] == ["2026-09-23", "2026-09-25"]
    assert [r["ticker"] for r in out["rows"]] == ["AAA", "CCC"]
    assert "2026-09-24" not in out["by_date"]
    assert out["n_sessions"] == 2
    # A panel with no quarantined day is returned unchanged.
    other = {"session_dates": ["2026-09-23"], "rows": [{"date": "2026-09-23"}]}
    same, none = drop_sessions(other)
    assert same is other and none == []


def test_factor_mine_and_walkforward_slices_skip_the_day() -> None:
    fm = fm_slice(_panel(), "2026-09-23", "2026-09-25")
    wf = wf_slice(_panel(), "2026-09-23", "2026-09-25")
    assert "2026-09-24" not in fm["session_dates"]
    assert "2026-09-24" not in wf["session_dates"]
    assert fm["n_rows"] == 2 and wf["n_rows"] == 2


def test_lane_harvest_and_news_readers_skip_the_day() -> None:
    from src.lane_news_scan import harvest, list_session_dates
    from src.news_grade import _signal_dates
    from src.news_impact.backtest import load_corpus

    assert "2026-09-24" not in list_session_dates()
    assert "2026-09-23" in list_session_dates()
    assert harvest("2026-09-24") == []
    assert "2026-09-24" not in _signal_dates(None)
    assert load_corpus("2026-09-24") == []
    # Neighbor still loads.
    assert load_corpus("2026-09-23")
    from src.news_impact.corpus import load_all_sources
    raw, meta = load_all_sources("2026-09-24")
    assert raw == [] and meta.get("quarantined") == "2026-09-24"


if __name__ == "__main__":
    test_list_marks_2026_09_24_stale_news()
    test_drop_sessions_keeps_neighbors()
    test_factor_mine_and_walkforward_slices_skip_the_day()
    test_lane_harvest_and_news_readers_skip_the_day()
    print("4 tests passed")
