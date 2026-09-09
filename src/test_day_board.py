"""Incremental land + day board. No network, no git push.

Run: PYTHONPATH=. python3 -m src.test_day_board
"""
from __future__ import annotations

import json
import os
import tempfile
from pathlib import Path
from unittest import mock

from src import day_board, land_file, output_qc


def test_preview_news_parse_uses_titles() -> None:
    data = {
        "raw_count": 12, "usable_count": 3, "single_name_count": 1,
        "usable_top": [
            {"title": "Fed holds rates"},
            {"title": "Oil jumps on supply"},
        ],
    }
    text = land_file._preview_json(data, "2026-09-08_parsed.json")
    assert "raw=12" in text
    assert "Fed holds rates" in text


def test_preview_book_lists_1d_names() -> None:
    data = {
        "books": {
            "1d": {
                "buy": [{"ticker": "AAA"}, {"ticker": "BBB"}],
                "sell": [{"ticker": "CCC"}],
            }
        }
    }
    text = land_file._preview_json(data, "2026-09-08_stock_book.json")
    assert "AAA" in text and "BBB" in text and "CCC" in text


def test_qc_rejects_db_timeout_parse() -> None:
    with tempfile.TemporaryDirectory() as d:
        p = Path(d) / "2026-09-08_parsed.json"
        p.write_text(json.dumps({
            "error": "db_timeout", "raw_count": 0,
            "usable_top": [], "all_items": [],
        }), encoding="utf-8")
        r = land_file._qc_one(p, "2026-09-08")
        assert r.ok is False
        assert r.reason == "db_timeout"


def test_land_does_not_push_when_qc_fails() -> None:
    os.environ["FULLSCAN_LAND_NOPUSH"] = "1"
    date = "2026-09-09"
    with tempfile.TemporaryDirectory() as d:
        tmp_path = Path(d)
        news = tmp_path / "01_daily" / "news"
        news.mkdir(parents=True)
        bad = news / f"{date}_parsed.json"
        bad.write_text(json.dumps({
            "error": "db_timeout", "raw_count": 0,
            "usable_top": [], "all_items": [],
        }), encoding="utf-8")
        with mock.patch.object(land_file, "ROOT", tmp_path), \
                mock.patch.object(day_board, "ROOT", tmp_path), \
                mock.patch.object(day_board, "BOARD_DIR",
                                  tmp_path / "data" / "day_board"), \
                mock.patch.object(land_file, "step_paths",
                                  return_value=[bad]), \
                mock.patch.object(day_board, "build", return_value={
                    "date": date, "generated_at": "t", "overall": "FAIL",
                    "ranker_ready": False, "blockers": [], "counts": {},
                    "processes": [], "selections": {}, "lands": [], "href": {},
                }):
            rec = land_file.land(date, "news_parse", title="News parse")
    os.environ.pop("FULLSCAN_LAND_NOPUSH", None)
    assert rec["pushed"] is False
    assert rec["ok"] is False
    assert "db_timeout" in (rec.get("preview") or "")


def test_land_pushes_qc_ok_file() -> None:
    date = "2026-09-09"
    with tempfile.TemporaryDirectory() as d:
        tmp_path = Path(d)
        news = tmp_path / "01_daily" / "news"
        news.mkdir(parents=True)
        good = news / f"{date}_parsed.json"
        good.write_text(json.dumps({
            "raw_count": 40, "usable_count": 8,
            "usable_top": [{"title": "Payrolls beat"}],
            "all_items": [{"title": "x"}],
        }), encoding="utf-8")
        pushed: list[list[str]] = []

        def fake_push(msg, paths):
            pushed.append([str(p) for p in paths])
            return True

        with mock.patch.object(land_file, "ROOT", tmp_path), \
                mock.patch.object(land_file, "_push", side_effect=fake_push), \
                mock.patch.object(land_file, "step_paths", return_value=[good]), \
                mock.patch.object(day_board, "note_land",
                                  return_value=[tmp_path / "data" / "day_board" / f"{date}.json"]):
            rec = land_file.land(date, "news_parse", title="News parse")
    assert rec["ok"] is True
    assert rec["pushed"] is True
    assert pushed and any("parsed.json" in x for x in pushed[0])


def test_merge_boards_unions_lands() -> None:
    theirs = {
        "date": "2026-09-09",
        "generated_at": "2026-09-09T06:10:00-04:00",
        "ranker_ready": True,
        "selections": {"buy_1d": [{"ticker": "AAA"}], "sell_1d": []},
        "lands": [{"key": "join", "at": "2026-09-09T06:05:00-04:00"}],
    }
    ours = {
        "date": "2026-09-09",
        "generated_at": "2026-09-09T06:00:00-04:00",
        "ranker_ready": False,
        "selections": {"buy_1d": [], "sell_1d": []},
        "lands": [{"key": "weather", "at": "2026-09-09T05:58:00-04:00"}],
    }
    merged = day_board.merge_boards(theirs, ours)
    keys = {row["key"] for row in merged["lands"]}
    assert keys == {"join", "weather"}
    assert merged["ranker_ready"] is True
    assert merged["selections"]["buy_1d"][0]["ticker"] == "AAA"


def test_day_board_html_has_raw_poll() -> None:
    html = day_board.DASH_HTML
    assert "raw.githubusercontent.com/SRoyaltyy/fullscan/main/data/day_board" in html
    assert "Stock Book readiness" in html
    assert "What was just pushed" in html
    assert "factor-mine" in html


def test_should_not_push_locally() -> None:
    os.environ.pop("GITHUB_ACTIONS", None)
    os.environ.pop("FULLSCAN_LAND", None)
    os.environ["FULLSCAN_LAND_NOPUSH"] = "1"
    try:
        assert land_file._should_push() is False
    finally:
        os.environ.pop("FULLSCAN_LAND_NOPUSH", None)


def test_land_never_raises() -> None:
    os.environ["FULLSCAN_LAND_NOPUSH"] = "1"
    try:
        with mock.patch.object(
                land_file, "step_paths", side_effect=RuntimeError("boom")):
            rec = land_file.land("2026-09-09", "news_parse", title="News parse")
        assert rec["ok"] is False
        assert rec["pushed"] is False
        assert "boom" in (rec.get("preview") or "")
    finally:
        os.environ.pop("FULLSCAN_LAND_NOPUSH", None)


def test_qc_news_parse_still_loud() -> None:
    r = output_qc.qc_news_parse("/no/such/parsed.json")
    assert r.ok is False
    assert r.reason == "missing"


def main() -> None:
    tests = [
        test_preview_news_parse_uses_titles,
        test_preview_book_lists_1d_names,
        test_qc_rejects_db_timeout_parse,
        test_land_does_not_push_when_qc_fails,
        test_land_pushes_qc_ok_file,
        test_merge_boards_unions_lands,
        test_day_board_html_has_raw_poll,
        test_should_not_push_locally,
        test_land_never_raises,
        test_qc_news_parse_still_loud,
    ]
    failed = 0
    for fn in tests:
        try:
            fn()
            print(f"ok  {fn.__name__}")
        except Exception as e:  # noqa: BLE001
            failed += 1
            print(f"FAIL {fn.__name__}: {e}")
    if failed:
        raise SystemExit(f"{failed} test(s) failed")
    print(f"{len(tests)} tests passed")


if __name__ == "__main__":
    main()
