"""Staleness canary. No network.

Run: PYTHONPATH=. python3 -m src.test_staleness_canary
"""
from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

from src import staleness_canary as canary

ET = ZoneInfo("America/New_York")
# Wednesday after the close. The latest completed session is this day.
NOW = datetime(2026, 11, 4, 18, 0, tzinfo=ET)


def _write(root: Path, rel: str, text: str) -> None:
    path = root / rel
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def _fresh_tree(root: Path, *, html: str = "2026-11-04",
                webull: str = "2026-11-03", jev: str = "2026-11-03",
                curve: str = "2026-11-04") -> None:
    _write(root, "dashboard/index.html",
           f'<html><head><meta charset="utf-8">\n'
           f'<meta name="data-asof" content="{html}"></head></html>')
    _write(root, "data/webull_sim/days.jsonl",
           '{"date":"2026-10-01"}\n'
           f'{{"date":"{webull}","equity":"1"}}\n')
    _write(root, "dashboard/h1/closes.json",
           json.dumps({"2026-10-30": {"AAA": 1}, curve: {"AAA": 2}}))
    _write(root, "data/book_paper/equity_curve.csv",
           f"date,equity\n2026-10-01,1\n{curve},2\n")
    _write(root, "data/paper/equity_curve.csv",
           f"date,sleeve,equity\n{curve},1d_top,1\n")
    _write(root, "dashboard/factor-mine/preopen.json", json.dumps({
        "start": "2026-10-08",
        "sleeves": {"h1": {"days": [
            {"date": "2026-10-30"},
            {"date": curve},
        ]}},
    }))
    _write(root, "data/day_board/latest.json", json.dumps({"date": curve}))
    _write(root, "dashboard/jev-train/days.json", json.dumps({
        "schema": "jev-train-days-1",
        "days": [{"date": "2026-09-29"}, {"date": jev}],
    }))
    _write(root, "dashboard/factor-mine/strategy_tickets.json",
           json.dumps({"date": curve, "strategies": {"a": {"date": "2026-10-01"}}}))


def test_session_lag_counts_nyse_days() -> None:
    assert canary.sessions_behind("2026-11-04", "2026-11-04") == 0
    assert canary.sessions_behind("2026-11-03", "2026-11-04") == 1
    assert canary.sessions_behind("2026-11-06", "2026-11-09") == 1
    assert canary.sessions_behind("", "2026-11-04") > 1


def test_parsers_use_the_newest_data_date() -> None:
    assert canary.newest_date(
        '<meta name="data-asof" content="2026-11-03">', "html") == "2026-11-03"
    assert canary.newest_date(
        '{"generated":"t","dates":["2026-08-13","2026-10-07"]}', "html",
    ) == "2026-10-07"
    assert canary.newest_date(
        "date,equity\n2026-10-01,1\n2026-11-03,2\n", "csv") == "2026-11-03"
    assert canary.newest_date(
        '{"date":"2026-10-01"}\n{"date":"2026-10-28"}\n', "jsonl") == "2026-10-28"
    assert canary.newest_date(
        '{"2026-10-30":{},"2026-11-03":{}}', "closes") == "2026-11-03"
    assert canary.newest_date(json.dumps({
        "start": "2026-10-08",
        "sleeves": {"h": {"days": [{"date": "2026-11-02"}]}},
    }), "json") == "2026-11-02"
    assert canary.newest_date(json.dumps({
        "days": [{"date": "2026-09-23"}, {"date": "2026-09-29"}],
    }), "jev_days") == "2026-09-29"


def test_report_fails_when_a_source_is_over_its_lag(tmp_path: Path) -> None:
    _fresh_tree(tmp_path)
    texts = {}
    for rel, _kind, _limit in canary.SOURCES:
        texts[rel] = (tmp_path / rel).read_text(encoding="utf-8")

    def fetch(url: str) -> str:
        rel = url.split("/fullscan/", 1)[1]
        return texts[rel]

    fresh = canary.evaluate(root=tmp_path, now=NOW, fetch=fetch)
    assert fresh["session"] == "2026-11-04"
    assert fresh["ok"] is True
    report = tmp_path / "data" / "health" / "staleness.json"
    assert json.loads(report.read_text(encoding="utf-8"))["ok"] is True

    # Main page one session behind is over the 0-session limit.
    # webull and jev at one session are still inside their limit.
    _fresh_tree(tmp_path, html="2026-11-03")
    texts["dashboard/index.html"] = (
        tmp_path / "dashboard/index.html").read_text(encoding="utf-8")
    stale = canary.evaluate(root=tmp_path, now=NOW, fetch=fetch)
    assert stale["ok"] is False
    page = next(row for row in stale["sources"] if row["path"] == "dashboard/index.html")
    assert page["main_lag"] == 1
    assert page["ok"] is False
    webull = next(row for row in stale["sources"] if row["path"].endswith("days.jsonl"))
    assert webull["ok"] is True
    jev = next(row for row in stale["sources"] if row["path"].endswith("jev-train/days.json"))
    assert jev["ok"] is True

    def missing(_url: str) -> str:
        raise OSError("404")

    gone = canary.evaluate(root=tmp_path, now=NOW, fetch=missing)
    assert gone["ok"] is False
    assert all(row["live_date"] == "" for row in gone["sources"])
    assert "404" in gone["sources"][0]["live_error"]


def test_scanner_backtest_is_manual() -> None:
    root = Path(__file__).resolve().parents[1]
    workflow = (root / ".github/workflows/scanner-backtest-publish.yml").read_text(encoding="utf-8")
    assert "cron:" not in workflow
    coverage = json.loads((root / "src/staleness_coverage.json").read_text(encoding="utf-8"))
    row = next(item for item in coverage["sources"] if item.get("id") == "scanner-backtest")
    assert row["mode"] == "manual"
    assert row["last_run"] == "2026-10-25"
    assert row["fill_forward"] is False
    assert row["page"] == "dashboard/scanner-backtest/index.html"
    page_path = root / row["page"]
    reported = next(
        item for item in canary.manual_rows(root) if item["page"] == row["page"])
    assert reported["ok"] is True
    assert reported["mode"] == "manual"
    assert reported["main_date"] == row["last_run"]
    # Opening execution omits this page from its sparse checkout. The
    # committed page is copied into that job when the blob exists. A
    # tree without the file keeps the coverage date.
    if page_path.is_file():
        assert reported["page_present"] is True
        text = page_path.read_text(encoding="utf-8")
        assert f"updated by hand, last run {row['last_run']}" in text
    else:
        assert reported["page_present"] is False
        assert row["last_run"] in reported["note"]


def test_missing_scanner_page_keeps_the_documented_day(tmp_path: Path) -> None:
    reported = next(
        item for item in canary.manual_rows(tmp_path)
        if item["page"] == "dashboard/scanner-backtest/index.html")
    assert reported["page_present"] is False
    assert reported["ok"] is True
    assert reported["mode"] == "manual"
    assert reported["main_date"] == "2026-10-25"
    assert "2026-10-25" in reported["note"]
    assert not (tmp_path / "dashboard/scanner-backtest/index.html").exists()


def test_manual_source_does_not_fail_the_canary(tmp_path: Path) -> None:
    _fresh_tree(tmp_path, curve="2026-11-02")
    texts = {}
    for rel, _kind, _limit in canary.SOURCES:
        texts[rel] = (tmp_path / rel).read_text(encoding="utf-8")

    def fetch(url: str) -> str:
        rel = url.split("/fullscan/", 1)[1]
        return texts[rel]

    report = canary.evaluate(root=tmp_path, now=NOW, fetch=fetch)
    manual = next(row for row in report["sources"] if row.get("mode") == "manual")
    assert manual["path"] == "dashboard/scanner-backtest/meta.json"
    assert manual["main_date"] == "2026-10-25"
    assert manual["page_present"] is False
    assert manual["ok"] is True
    assert report["ok"] is False
    preopen = next(row for row in report["sources"]
                   if row["path"] == "dashboard/factor-mine/preopen.json")
    assert preopen["ok"] is False
    assert preopen.get("mode") != "manual"


def main() -> None:
    import tempfile
    test_session_lag_counts_nyse_days()
    test_parsers_use_the_newest_data_date()
    test_scanner_backtest_is_manual()
    test_missing_scanner_page_keeps_the_documented_day(Path(tempfile.mkdtemp()))
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        test_report_fails_when_a_source_is_over_its_lag(root)
        test_manual_source_does_not_fail_the_canary(root)
    print("staleness canary ok")


if __name__ == "__main__":
    main()
