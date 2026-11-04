"""Coverage list the staleness canary can load after PR #743 merges."""
import json
from pathlib import Path

from src.staleness_coverage import (
    newest_updown_day, pages, sources_for_canary,
)

ROOT = Path(__file__).resolve().parent.parent


def test_canary_module_is_not_on_this_branch():
    assert not (ROOT / "src" / "staleness_canary.py").is_file()


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


def test_scanner_and_jev_are_manual_frozen():
    by_path = {page["path"]: page for page in pages()}
    scanner = by_path["dashboard/scanner-backtest/index.html"]
    jev = by_path["dashboard/jev-train/days.json"]
    assert scanner["manual"] is True and scanner["frozen"] is True
    assert scanner["writer"] is None
    assert jev["manual"] is True and jev["frozen"] is True
    assert jev["writer"] is None
    assert "743" in json.dumps(pages())
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
