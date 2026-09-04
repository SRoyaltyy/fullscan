"""Skip-if-good must not treat a stale board as today's pack.

Run: PYTHONPATH=. python3 -m src.test_skip_if_good
"""
from __future__ import annotations

import json
import tempfile
from pathlib import Path

from src import skip_if_good


def test_missing_date_is_run() -> None:
    assert skip_if_good.check_learn_cycle("1999-01-01") is False
    assert skip_if_good.check_stock_book_all("1999-01-01") is False
    assert skip_if_good.check_preopen_full("1999-01-01") is False
    assert skip_if_good.check_postclose_all("1999-01-01") is False
    assert skip_if_good.check_label_weather("1999-01-01") is False


def test_learn_requires_dated_file_not_stale_board() -> None:
    # 2026-09-03 never wrote 01_daily/2026-09-03_learnings.md.
    # 03_scoreboard/LEARNINGS.md is always large — that used to skip learn.
    assert skip_if_good.check_learn_cycle("2026-09-03") is False
    assert skip_if_good.check_learn_cycle("2026-09-01") is True


def test_stock_book_requires_green_and_ranker_inputs() -> None:
    # 2026-09-03 1d BUY still has printed dead relvol (VFF/VEEV/WAY).
    # That must not skip the ubuntu heal.
    assert skip_if_good.check_stock_book_all("2026-09-03") is False
    assert skip_if_good.check_label_weather("2026-09-03") is True
    assert skip_if_good.check_ab_checklist("2026-09-03") is True


def test_dead_relvol_1d_buy_is_not_good() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        js = Path(tmp) / "book.json"
        js.write_text(json.dumps({
            "meta": {"n": 400},
            "books": {"1d": {"buy": [
                {"ticker": "WAY", "relvol": 0.54},
                {"ticker": "LIVE", "relvol": 1.2},
            ]}},
        }), encoding="utf-8")
        assert skip_if_good.book_1d_has_dead_relvol(js) is True
        js.write_text(json.dumps({
            "meta": {"n": 400},
            "books": {"1d": {"buy": [
                {"ticker": "LIVE", "relvol": 1.2},
                {"ticker": "ZERO", "relvol": 0},
            ]}},
        }), encoding="utf-8")
        assert skip_if_good.book_1d_has_dead_relvol(js) is False


def test_postclose_all_needs_learn_not_just_outcome() -> None:
    # 09-03 has an outcome + next-session baseline, but no dated learnings.
    assert skip_if_good.check_daily_pipeline_outcome("2026-09-03") is True
    assert skip_if_good.check_learn_cycle("2026-09-03") is False
    assert skip_if_good.check_postclose_all("2026-09-03") is False


def test_finviz_scrape_requires_elite_export() -> None:
    assert skip_if_good.check_finviz_scrape("1999-01-01") is False


def test_jobs_include_label_weather() -> None:
    assert "label_weather" in skip_if_good.JOBS
    assert "stock_book_all" in skip_if_good.JOBS
    assert "postclose_all" in skip_if_good.JOBS


def test_degraded_book_is_not_good() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        js = Path(tmp) / "book.json"
        green = Path(tmp) / "green.json"
        js.write_text(json.dumps({
            "meta": {"degraded": True, "error": "boom"},
            "universe": ["AAA"] * 400,
            "rows": [{"ticker": "AAA", "s_join": 0.0}] * 400,
        }), encoding="utf-8")
        green.write_text(json.dumps({
            "date": "1999-01-01",
            "degraded": True,
            "n_buy": 0,
            "n_sell": 400,
        }), encoding="utf-8")
        assert skip_if_good.book_files_are_degraded(js, green) is True
        js.write_text(json.dumps({
            "meta": {"n": 400},
            "books": {"1d": {"buy": [{"ticker": "AAA"}]}},
        }), encoding="utf-8")
        green.write_text(json.dumps({"date": "1999-01-01", "n_buy": 1}), encoding="utf-8")
        assert skip_if_good.book_files_are_degraded(js, green) is False
        js.write_text(json.dumps({
            "meta": {"date": "1999-01-01"},
            "books": {},
        }), encoding="utf-8")
        assert skip_if_good.book_files_are_degraded(js, green) is True


if __name__ == "__main__":
    test_missing_date_is_run()
    test_learn_requires_dated_file_not_stale_board()
    test_stock_book_requires_green_and_ranker_inputs()
    test_dead_relvol_1d_buy_is_not_good()
    test_postclose_all_needs_learn_not_just_outcome()
    test_finviz_scrape_requires_elite_export()
    test_jobs_include_label_weather()
    test_degraded_book_is_not_good()
    print("ok")
