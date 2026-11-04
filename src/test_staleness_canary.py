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
    _write(root, "dashboard/updown/data/index.json",
           json.dumps({"model": "updown_o2c_v2", "days": [
               ["2026-10-05", "h"], [curve, "h", 1],
           ]}))
    _write(root, "dashboard/hard-red-exceptions/latest.json",
           json.dumps({"to_date": curve, "days": [{"date": curve}]}))
    _write(root, "dashboard/day-movers/days.json",
           json.dumps({"to_date": curve, "days": [{"date": curve}]}))


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
    assert canary.newest_date(json.dumps({
        "days": [["2026-10-02", "h"], ["2026-10-05", "h"]],
    }), "updown_days") == "2026-10-05"


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


def _tape(marker: bytes) -> bytes:
    return b"Ticker,Change\nAAA,1.0\n" + marker + b"\n" + (b"x" * 220)


def test_writer_refuses_a_byte_copy_and_keeps_a_fresh_tape(tmp_path: Path) -> None:
    from src.finviz_export_guard import publish_bytes, refuse_copied_export

    exports = tmp_path / "data" / "exports"
    exports.mkdir(parents=True, exist_ok=True)
    stale = _tape(b"STALE")
    (exports / "finviz_2026-11-03.csv").write_bytes(stale)
    dest = exports / "finviz_2026-11-04.csv"
    try:
        publish_bytes("2026-11-04", stale, dest, root=tmp_path)
    except SystemExit as exc:
        text = str(exc)
        assert "FAIL" in text
        assert "byte-identical" in text
        assert "2026-11-03" in text
        assert "sha256=" in text
    else:
        raise AssertionError("a copied export was published")
    assert not dest.exists()
    fresh = _tape(b"FRESH")
    publish_bytes("2026-11-04", fresh, dest, root=tmp_path)
    assert dest.read_bytes() == fresh
    refuse_copied_export("2026-11-05", root=tmp_path)
    dest.write_bytes(stale)
    try:
        refuse_copied_export("2026-11-04", root=tmp_path)
    except SystemExit as exc:
        assert "byte-identical to 2026-11-03" in str(exc)
    else:
        raise AssertionError("morning guard continued on a copied tape")


def test_staleness_alarm_stops_on_a_copied_finviz_export(tmp_path: Path) -> None:
    _fresh_tree(tmp_path)
    exports = tmp_path / "data" / "exports"
    exports.mkdir(parents=True, exist_ok=True)
    body = _tape(b"SAME")
    (exports / "finviz_2026-11-03.csv").write_bytes(body)
    (exports / "finviz_2026-11-04.csv").write_bytes(body)
    texts = {}
    for rel, _kind, _limit in canary.SOURCES:
        texts[rel] = (tmp_path / rel).read_text(encoding="utf-8")

    def fetch(url: str) -> str:
        rel = url.split("/fullscan/", 1)[1]
        return texts[rel]

    copied = canary.evaluate(root=tmp_path, now=NOW, fetch=fetch)
    assert copied["ok"] is False
    row = next(item for item in copied["sources"] if item["path"].endswith("finviz_2026-11-04.csv"))
    assert row["ok"] is False
    assert "byte-identical to 2026-11-03" in row["live_error"]
    (exports / "finviz_2026-11-04.csv").write_bytes(_tape(b"OTHER"))
    fresh = canary.evaluate(root=tmp_path, now=NOW, fetch=fetch)
    assert fresh["ok"] is True


def main() -> None:
    test_session_lag_counts_nyse_days()
    test_parsers_use_the_newest_data_date()
    import tempfile
    with tempfile.TemporaryDirectory() as tmp:
        test_report_fails_when_a_source_is_over_its_lag(Path(tmp))
        test_writer_refuses_a_byte_copy_and_keeps_a_fresh_tape(Path(tmp))
        test_staleness_alarm_stops_on_a_copied_finviz_export(Path(tmp))
    print("staleness canary ok")


if __name__ == "__main__":
    main()
