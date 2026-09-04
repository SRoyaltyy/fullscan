"""Skip-if-good must not treat a stale board as today's pack.

Run: PYTHONPATH=. python3 -m src.test_skip_if_good
"""
from __future__ import annotations

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
    assert skip_if_good.check_stock_book_all("2026-09-03") is True
    assert skip_if_good.check_label_weather("2026-09-03") is True
    assert skip_if_good.check_ab_checklist("2026-09-03") is True


def test_postclose_all_needs_learn_not_just_outcome() -> None:
    # 09-03 has an outcome + next-session baseline, but no dated learnings.
    assert skip_if_good.check_daily_pipeline_outcome("2026-09-03") is True
    assert skip_if_good.check_learn_cycle("2026-09-03") is False
    assert skip_if_good.check_postclose_all("2026-09-03") is False


def test_jobs_include_label_weather() -> None:
    assert "label_weather" in skip_if_good.JOBS
    assert "stock_book_all" in skip_if_good.JOBS
    assert "postclose_all" in skip_if_good.JOBS


if __name__ == "__main__":
    test_missing_date_is_run()
    test_learn_requires_dated_file_not_stale_board()
    test_stock_book_requires_green_and_ranker_inputs()
    test_postclose_all_needs_learn_not_just_outcome()
    test_jobs_include_label_weather()
    print("ok")
