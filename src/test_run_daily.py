"""Daily / generate spine — clock, registry, collectors.

Run: PYTHONPATH=. python3 -m src.test_run_daily
"""
from __future__ import annotations

import json
import tempfile
from datetime import datetime
from pathlib import Path
from unittest import mock
from zoneinfo import ZoneInfo

from src import run_daily, run_generate, skip_if_good

ET = ZoneInfo("America/New_York")


def test_labor_day_is_not_a_session() -> None:
    assert run_daily.is_session("2026-09-07") is False
    assert run_daily.is_session("2026-09-08") is True
    assert run_daily.is_session("2026-09-04") is True


def test_schedule_job_maps_crons() -> None:
    assert run_daily.schedule_job("40 9 * * 1-5", "auto") == "scrape"
    assert run_daily.schedule_job("10 10 * * 1-5", "auto") == "generate"
    assert run_daily.schedule_job("10 20 * * 1-5", "auto") == "night"
    assert run_daily.schedule_job("", "morning") == "morning"


def test_auto_lane_before_and_after_bell() -> None:
    morning = datetime(2026, 9, 8, 6, 10, tzinfo=ET)
    night = datetime(2026, 9, 8, 16, 10, tzinfo=ET)
    assert run_daily.resolve_lane("auto", morning) == "morning"
    assert run_daily.resolve_lane("auto", night) == "night"
    clk = run_daily.clock("auto", "2026-09-08", now=morning)
    assert clk["phase"] == "morning"
    assert clk["session"] is True
    hol = run_daily.clock("morning", "2026-09-07", now=morning)
    assert hol["skip_session"] is True
    forced = run_daily.clock("morning", "2026-09-07", now=morning, force=True)
    assert forced["skip_session"] is False


def test_holiday_morning_run_is_noop() -> None:
    morning = datetime(2026, 9, 7, 6, 10, tzinfo=ET)
    code = run_daily.run(phase="morning", date="2026-09-07", now=morning)
    assert code == 0


def test_generate_registry_covers_live_and_research() -> None:
    ids = [s["id"] for s in run_generate.STRATEGIES]
    for need in ("stock_book", "flatten_robust", "paper_io",
                 "sleeve_combine", "factor_mine", "excel", "strategy_board"):
        assert need in ids
    live = [s for s in run_generate.STRATEGIES if s.get("live")]
    assert live and live[0]["id"] == "flatten_robust"


def test_collect_stock_book_from_fixture() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        book_dir = root / "data" / "stock_book"
        book_dir.mkdir(parents=True)
        (book_dir / "2026-09-08_stock_book.json").write_text(json.dumps({
            "meta": {"same_day_general": True, "same_day_sectors": 11},
            "books": {
                "1d": {"buy": [{"ticker": "AAA"}], "sell": [{"ticker": "BBB"}]},
                "3d": {"buy": [{"ticker": "CCC"}], "sell": []},
            },
        }), encoding="utf-8")
        with mock.patch.object(run_generate, "ROOT", root):
            got = run_generate.collect_stock_book("2026-09-08")
        assert got["ok"] is True
        assert got["buy"] == ["AAA"]
        assert got["sell"] == ["BBB"]
        assert got["horizons"]["3d"]["buy"] == ["CCC"]


def test_collect_flatten_from_today_json() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        p = root / "data" / "sleeve_merge" / "today.json"
        p.parent.mkdir(parents=True)
        p.write_text(json.dumps({
            "policy": "flatten_robust",
            "route": "io",
            "why": "test",
            "n_holds_open": 1,
            "tickets": [
                {"side": "BUY", "ticker": "XYZ"},
                {"side": "SELL", "ticker": "OLD"},
            ],
            "would_buy": {"rows": [{"ticker": "WSH"}]},
        }), encoding="utf-8")
        with mock.patch.object(run_generate, "ROOT", root):
            with mock.patch.object(run_generate, "TODAY_JSON", p):
                got = run_generate.collect_flatten("2026-09-08")
        assert got["ok"] is True
        assert got["buy"] == ["XYZ"]
        assert got["sell"] == ["OLD"]
        assert got["would_buy"] == ["WSH"]


def test_skip_jobs_include_daily_and_generate() -> None:
    assert "daily" in skip_if_good.JOBS
    assert "generate" in skip_if_good.JOBS
    assert skip_if_good.check_generate("1999-01-01") is False


def test_retired_spine_has_no_live_schedule() -> None:
    root = Path(__file__).resolve().parent.parent
    retired = (
        "postclose_all.yml",
        "postclose_last_closed.yml",
        "finviz_preopen_scrape.yml",
        "stock_book_all.yml",
        "sleeve_merge_live.yml",
        "daily_orchestrator.yml",
        "stock_book_diag.yml",
    )
    for name in retired:
        text = (root / ".github" / "workflows" / name).read_text(encoding="utf-8")
        live = [ln for ln in text.splitlines()
                if ln.startswith("  schedule:") or ln.startswith("schedule:")]
        assert not live, f"{name} still has a live schedule"


if __name__ == "__main__":
    test_labor_day_is_not_a_session()
    test_schedule_job_maps_crons()
    test_auto_lane_before_and_after_bell()
    test_holiday_morning_run_is_noop()
    test_generate_registry_covers_live_and_research()
    test_collect_stock_book_from_fixture()
    test_collect_flatten_from_today_json()
    test_skip_jobs_include_daily_and_generate()
    test_retired_spine_has_no_live_schedule()
    print("ok")
