"""Coverage list the staleness canary can load after PR #743 merges."""
import json
from pathlib import Path

from src.staleness_coverage import (
    newest_updown_day, pages, sources_for_canary,
)

ROOT = Path(__file__).resolve().parent.parent


def test_canary_sources_include_the_daily_pages():
    from src import staleness_canary as canary
    paths = [row[0] for row in canary.SOURCES]
    assert "dashboard/updown/data/index.json" in paths
    assert "dashboard/hard-red-exceptions/latest.json" in paths
    assert "dashboard/day-movers/days.json" in paths
    assert paths.count("dashboard/jev-train/days.json") == 1
    assert "dashboard/scanner-backtest/index.html" not in paths


def test_daily_pages_are_the_canary_rows():
    rows = sources_for_canary()
    paths = [row[0] for row in rows]
    assert paths == [
        "dashboard/updown/data/index.json",
        "dashboard/hard-red-exceptions/latest.json",
        "dashboard/day-movers/days.json",
    ]
    assert rows[0][1] == "updown_days"
    assert all(row[2] == 0 for row in rows)


def test_scanner_is_frozen_and_jev_has_a_daily_writer():
    by_path = {page["path"]: page for page in pages()}
    scanner = by_path["dashboard/scanner-backtest/index.html"]
    jev = by_path["dashboard/jev-train/days.json"]
    assert scanner["manual"] is True and scanner["frozen"] is True
    assert scanner["writer"] is None
    assert jev["manual"] is False and jev["frozen"] is False
    assert jev["already_in_canary"] is True
    assert jev["lag_limit"] == 1
    assert "jev_train.yml" in jev["writer"]
    covered = {row[0] for row in sources_for_canary()}
    assert scanner["path"] not in covered
    assert jev["path"] not in covered


def test_coverage_names_pr_743_dependency():
    raw = json.loads((ROOT / "src" / "staleness_coverage.json").read_text())
    assert raw["depends_on_pr"] == 743
    assert "743" in raw["note"]
    assert "staleness_canary.py" in raw["note"]


def test_updown_day_reader_uses_last_day_slot():
    text = json.dumps({"days": [["2026-10-02", "h"], ["2026-10-05", "h", 1]]})
    assert newest_updown_day(text) == "2026-10-05"
    assert newest_updown_day("") == ""
    assert newest_updown_day("{") == ""
