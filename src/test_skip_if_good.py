"""Skip-if-good must not treat a stale board as today's pack.

Run: PYTHONPATH=. python3 -m src.test_skip_if_good
"""
from __future__ import annotations

import json
import tempfile
from pathlib import Path

from src import green_pile, skip_if_good


def test_skip_constants_match_pile_and_avoid_pandas() -> None:
    src = Path(skip_if_good.__file__).read_text(encoding="utf-8")
    assert "from . import green_pile" not in src
    assert skip_if_good.EPS == green_pile.EPS
    assert skip_if_good.RELVOL_DEAD == green_pile.RELVOL_DEAD


def test_missing_date_is_run() -> None:
    assert skip_if_good.check_learn_cycle("1999-01-01") is False
    assert skip_if_good.check_stock_book_all("1999-01-01") is False
    assert skip_if_good.check_preopen_full("1999-01-01") is False
    assert skip_if_good.check_postclose_all("1999-01-01") is False
    assert skip_if_good.check_label_weather("1999-01-01") is False


def test_learn_requires_dated_file_not_stale_board() -> None:
    # 2026-09-04 is still the live session — no dated learnings yet.
    # 03_scoreboard/LEARNINGS.md is always large — that used to skip learn.
    assert skip_if_good.check_learn_cycle("2026-09-04") is False
    assert skip_if_good.check_learn_cycle("2026-09-03") is True
    assert skip_if_good.check_learn_cycle("2026-09-01") is True


def test_stock_book_requires_green_and_ranker_inputs() -> None:
    # 2026-09-03 1d BUY still has printed dead relvol (VFF/VEEV/WAY).
    # That must not skip the ubuntu heal.
    assert skip_if_good.check_stock_book_all("2026-09-03") is False
    assert skip_if_good.check_label_weather("2026-09-03") is True
    assert skip_if_good.check_ab_checklist("2026-09-03") is True


def test_book_without_essays_is_not_good() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        js = Path(tmp) / "book.json"
        js.write_text(json.dumps({
            "meta": {"same_day_general": False, "same_day_sectors": 0},
            "books": {"1d": {"buy": [{"ticker": "LIVE", "relvol": 1.2}]}},
        }), encoding="utf-8")
        assert skip_if_good.book_missing_same_day_essays(js) is True
        js.write_text(json.dumps({
            "meta": {"same_day_general": True, "same_day_sectors": 3},
            "books": {"1d": {"buy": [{"ticker": "LIVE", "relvol": 1.2}]}},
        }), encoding="utf-8")
        assert skip_if_good.book_missing_same_day_essays(js) is True
        js.write_text(json.dumps({
            "meta": {"same_day_general": True, "same_day_sectors": 11},
            "books": {"1d": {"buy": [{"ticker": "LIVE", "relvol": 1.2}]}},
        }), encoding="utf-8")
        assert skip_if_good.book_missing_same_day_essays(js) is False


def test_1d_buy_not_all_green_is_not_good() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        js = Path(tmp) / "book.json"
        js.write_text(json.dumps({
            "meta": {"same_day_general": True, "same_day_sectors": 11},
            "books": {"1d": {"buy": [
                {"ticker": "HTFL", "green": False, "relvol": 0.81,
                 "s_join": 0.2, "s_general": 0.4, "s_ab": 0.8, "s_peer": 0.0,
                 "s_sector": 0.5, "s_news": 0.0},
                {"ticker": "CNH", "green": False, "relvol": 2.47,
                 "s_join": 0.2, "s_general": 0.2, "s_ab": 0.9, "s_peer": 0.9,
                 "s_sector": -0.45, "s_news": 0.0},
            ]}},
        }), encoding="utf-8")
        assert skip_if_good.book_1d_breaks_all_green(js) is True
        js.write_text(json.dumps({
            "meta": {"same_day_general": True, "same_day_sectors": 11},
            "books": {"1d": {"buy": [
                {"ticker": "NU", "green": True, "relvol": 1.08,
                 "s_join": 0.9, "s_general": 0.2, "s_ab": 0.7, "s_peer": 0.7,
                 "s_sector": 0.05, "s_news": 0.3},
            ]}},
        }), encoding="utf-8")
        assert skip_if_good.book_1d_breaks_all_green(js) is False


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


def test_night_pack_dates_heals_prior_session_after_bell() -> None:
    from datetime import datetime
    from zoneinfo import ZoneInfo
    et = ZoneInfo("America/New_York")
    after_bell = datetime(2026, 9, 4, 16, 10, tzinfo=et)
    dates = skip_if_good.night_pack_dates(after_bell)
    assert dates[-1] == "2026-09-04"
    assert "2026-09-03" in dates
    before_bell = datetime(2026, 9, 4, 8, 40, tzinfo=et)
    assert skip_if_good.last_closed_session(before_bell) == "2026-09-03"
    assert skip_if_good._prev_weekday("2026-09-04") == "2026-09-03"
    assert skip_if_good._prev_weekday("2026-09-07") == "2026-09-04"


def test_postclose_all_needs_learn_not_just_outcome() -> None:
    # 09-03 now has dated learnings, but tool-dump sector outcomes
    # (XLB/XLE/XLV) must still fail the pack.
    assert skip_if_good.check_daily_pipeline_outcome("2026-09-03") is True
    assert skip_if_good.check_learn_cycle("2026-09-03") is True
    assert skip_if_good.check_postclose_all("2026-09-03") is False


def test_postclose_all_needs_reflect_and_sector_outcomes() -> None:
    # 09-03 reflect.md + 11 sector reflects landed. Three outcome.md
    # files are leaked DeepSeek tool-call XML and must not skip the heal.
    assert skip_if_good.check_general_reflect("2026-09-03") is True
    assert skip_if_good.check_sector_outcomes("2026-09-03") is False
    assert skip_if_good.check_sector_reflects("2026-09-03") is True
    assert skip_if_good.check_postclose_all("2026-09-03") is False
    energy = Path("01_daily/sectors/2026-09-03/energy_outcome.md")
    assert energy.is_file()
    assert skip_if_good.is_tool_dump(energy.read_text(encoding="utf-8"))


def test_sector_md_counts_only_quality_files() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        d = root / "01_daily" / "sectors" / "1999-01-01"
        d.mkdir(parents=True)
        (d / "technology_outcome.md").write_text("x" * 50, encoding="utf-8")
        (d / "healthcare_outcome.md").write_text("y" * 250, encoding="utf-8")
        (d / "energy_outcome.md").write_text(
            'Actuals\n<｜｜DSML｜｜tool_calls>\n'
            '<｜｜DSML｜｜invoke name="web_search">\n' + ("q" * 200),
            encoding="utf-8")
        (d / "energy_reflect.md").write_text("z" * 50, encoding="utf-8")
        (d / "financial_reflect.md").write_text("w" * 250, encoding="utf-8")
        old_root = skip_if_good.ROOT
        skip_if_good.ROOT = root
        try:
            assert skip_if_good._count_sector_md("1999-01-01", "_outcome.md") == 1
            assert skip_if_good._count_sector_dumps("1999-01-01", "_outcome.md") == 1
            assert skip_if_good.check_sector_outcomes("1999-01-01") is False
            assert skip_if_good._count_sector_md("1999-01-01", "_reflect.md") == 1
        finally:
            skip_if_good.ROOT = old_root


def test_is_tool_dump_detects_dsml_and_web_search() -> None:
    assert skip_if_good.is_tool_dump("") is False
    assert skip_if_good.is_tool_dump("# Sector Outcome\nXLE sold off.") is False
    assert skip_if_good.is_tool_dump(
        'invoke name="web_search"\nquery=oil') is True
    assert skip_if_good.is_tool_dump("<x>tool_calls></x>") is True


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
    test_skip_constants_match_pile_and_avoid_pandas()
    test_missing_date_is_run()
    test_learn_requires_dated_file_not_stale_board()
    test_stock_book_requires_green_and_ranker_inputs()
    test_book_without_essays_is_not_good()
    test_1d_buy_not_all_green_is_not_good()
    test_dead_relvol_1d_buy_is_not_good()
    test_night_pack_dates_heals_prior_session_after_bell()
    test_postclose_all_needs_learn_not_just_outcome()
    test_postclose_all_needs_reflect_and_sector_outcomes()
    test_sector_md_counts_only_quality_files()
    test_is_tool_dump_detects_dsml_and_web_search()
    test_finviz_scrape_requires_elite_export()
    test_jobs_include_label_weather()
    test_degraded_book_is_not_good()
    print("ok")
