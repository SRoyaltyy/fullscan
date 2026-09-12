"""Contracts for the 2026-09-08 Pre-Open harden (fixes #1–#5).

Run: PYTHONPATH=. python3 -m src.test_preopen_harden
"""
from __future__ import annotations

import json
import os
import tempfile
from pathlib import Path
from unittest import mock

from src import db, output_qc, preopen
from src import map_heat_refresh as mr


ROOT = Path(__file__).resolve().parent.parent


def test_news_parse_falls_back_to_digest() -> None:
    from src import news_parse

    with tempfile.TemporaryDirectory() as d:
        root = Path(d)
        news = root / "01_daily" / "news"
        news.mkdir(parents=True)
        (news / "2026-09-09_finviz_digest.json").write_text(json.dumps({
            "index_digests": [{
                "digest": "S&P 500 slips as Middle East tensions lift oil "
                          "ahead of CPI",
                "source": "finviz_elite_news",
            }],
            "top_signal": [{
                "news_title": "Fed officials signal patience on rate cuts",
                "digest": "Bernstein upgrades semis on AI capex",
                "source": "finviz_export",
            }],
        }), encoding="utf-8")
        orig_dir = news_parse.NEWS_DIR
        news_parse.NEWS_DIR = str(news)
        try:
            with mock.patch.object(
                    news_parse.db, "recent_news",
                    side_effect=news_parse.db.NewsDbError(
                        "db_timeout", "statement timeout")):
                cwd = os.getcwd()
                os.chdir(root)
                try:
                    report = news_parse.build_report(
                        hours=48, limit=40, date_str="2026-09-09")
                finally:
                    os.chdir(cwd)
        finally:
            news_parse.NEWS_DIR = orig_dir
    assert report.get("error") in (None, "")
    assert report["raw_count"] >= 2


def test_qc_news_parse_db_timeout_is_actionable() -> None:
    with tempfile.TemporaryDirectory() as d:
        p = Path(d) / "parsed.json"
        p.write_text(json.dumps({
            "error": "db_timeout",
            "error_detail": "statement timeout",
            "raw_count": 0,
            "usable_top": [],
            "all_items": [],
        }), encoding="utf-8")
        r = output_qc.qc_news_parse(p)
        assert not r.ok
        assert r.reason == "db_timeout"
        assert "empty_parse" not in r.reason


def test_recent_news_raises_after_timeout_retries() -> None:
    class _Cur:
        def execute(self, q, params=None):
            raise RuntimeError("canceling statement due to statement timeout")

        def fetchall(self):
            return []

        def close(self):
            return None

    class _Conn:
        def cursor(self):
            return _Cur()

        def rollback(self):
            return None

        def close(self):
            return None

    orig = db._conn
    db._conn = lambda: _Conn()  # type: ignore[method-assign]
    sleeps: list[float] = []
    try:
        with mock.patch.object(db.time, "sleep", side_effect=sleeps.append):
            try:
                db.recent_news(hours=48, limit=10)
                raise AssertionError("expected NewsDbError")
            except db.NewsDbError as e:
                assert e.reason == "db_timeout"
        assert len(sleeps) == 1
    finally:
        db._conn = orig


def test_bypass_cutoff_skips_refuse() -> None:
    os.environ.pop("PREOPEN_BYPASS_CUTOFF", None)
    assert preopen.bypass_cutoff() is False
    os.environ["PREOPEN_BYPASS_CUTOFF"] = "1"
    try:
        assert preopen.bypass_cutoff() is True
        with mock.patch.object(preopen, "past_predict_cutoff", return_value=True):
            preopen.refuse_if_late("news_parse", force=False)
    finally:
        os.environ.pop("PREOPEN_BYPASS_CUTOFF", None)


def test_ubuntu_late_heal_after_0925() -> None:
    """#199 clock + orch dispatch must late-heal; ECS must not."""
    assert preopen.ubuntu_late_heal("push", hm=925) is True
    assert preopen.ubuntu_late_heal("schedule", hm=930) is True
    assert preopen.ubuntu_late_heal("workflow_dispatch", "ubuntu", hm=935) is True
    assert preopen.ubuntu_late_heal("workflow_dispatch", "ecs", hm=935) is False
    assert preopen.ubuntu_late_heal("schedule", hm=924) is False
    assert preopen.ubuntu_late_heal("push", hm=554) is False
    yml = (ROOT / ".github" / "workflows" / "preopen_all.yml").read_text(
        encoding="utf-8")
    assert "ubuntu late-heal --bypass-cutoff" in yml
    assert '[ "$EVENT" = "schedule" ]' in yml
    assert '[ "$RUNNER" = "ubuntu" ]' in yml
    assert 'event_name }}" = "push" ] && [ "$ET_HM" -ge 925 ]' not in yml
    assert "weekend push poke swallowed" in yml
    orch = (ROOT / ".github" / "workflows" / "daily_orchestrator.yml").read_text(
        encoding="utf-8")
    assert "ubuntu Pre-Open late heal (--bypass-cutoff)" in orch
    assert "maybe preopen_all.yml" in orch
    # Midday late heal must not rewrite quality-ok files.
    late = orch.split("past 09:25 ET — ubuntu Pre-Open late heal")[1]
    assert "maybe preopen_all.yml" in late.split("MISSING news parse")[0]
    assert "inputs[force]=true" not in late.split("MISSING news parse")[0]


def test_run_preopen_cli_has_bypass_not_permanent_force() -> None:
    src = (ROOT / "src" / "run_preopen_all.py").read_text(encoding="utf-8")
    assert "--bypass-cutoff" in src
    assert "bypass_cutoff=args.bypass_cutoff" in src
    assert "PREOPEN_BYPASS_CUTOFF" in src
    yml = (ROOT / ".github" / "workflows" / "preopen_all.yml").read_text(
        encoding="utf-8")
    assert "ARGS+=(--bypass-cutoff)" in yml
    assert "ARGS=(--llm-backend \"$BACKEND\" --force)" not in yml


def test_incremental_land_hooks() -> None:
    pre = (ROOT / "src" / "run_preopen_all.py").read_text(encoding="utf-8")
    book = (ROOT / "src" / "run_stock_book_all.py").read_text(encoding="utf-8")
    yml = (ROOT / ".github" / "workflows" / "preopen_all.yml").read_text(
        encoding="utf-8")
    assert "from . import land_file" in pre
    assert "_land(date, key, title)" in pre
    assert "land_file.land" in book
    assert '_land(date, "universe"' in book
    assert '_land(date, "finviz"' in book
    assert "leftover sweep" in yml
    assert "FULLSCAN_LAND" in yml
    assert "before 03:55 ET" in yml
    assert "weekend push poke swallowed" in yml
    assert "go=no" in yml
    book_yml = (ROOT / ".github" / "workflows" / "stock_book_all.yml").read_text(
        encoding="utf-8")
    assert "before 05:35 ET" in book_yml
    assert "go=no" in book_yml
    assert "needs: gate" in book_yml
    assert 'github.event_name }}" != "workflow_dispatch"' in book_yml
    orch = (ROOT / ".github" / "workflows" / "daily_orchestrator.yml").read_text(
        encoding="utf-8")
    assert "news_judge.yml" in orch
    assert 'inputs[force]=true' in orch
    assert "35 13" in orch
    dash = (ROOT / "src" / "day_board.py").read_text(encoding="utf-8")
    assert "raw.githubusercontent.com" in dash
    assert "dashboard/day-board" in dash
    scrape = (ROOT / ".github" / "workflows" / "finviz_preopen_scrape.yml").read_text(
        encoding="utf-8")
    assert "FULLSCAN_LAND" in scrape
    assert "src.finviz_digest --date $DATE --force" in scrape
    assert "collectors.finviz_financials" in scrape
    digest = (ROOT / "src" / "finviz_digest.py").read_text(encoding="utf-8")
    assert "land_file.land" in digest
    assert "finviz_{d}.csv" in digest or "finviz_{" in digest
    assert "existing_digest_is_morning_ok" in digest
    assert "weather_stamped_before_open" in digest
    fin_all = (ROOT / ".github" / "workflows" / "finviz_all.yml").read_text(
        encoding="utf-8")
    assert "last_closed_session" in fin_all
    assert "delayed schedule before 05:35 ET" in fin_all
    heat = (ROOT / "src" / "map_heat.py").read_text(encoding="utf-8")
    assert "land_file.land" in heat
    sleeve = (ROOT / ".github" / "workflows" / "sleeve_merge_live.yml").read_text(
        encoding="utf-8")
    assert "no ${DATE} book — skip hollow flatten card" in sleeve
    assert "before 09:00 ET" in sleeve
    assert "no checkout" in sleeve
    ecs = (ROOT / "scripts" / "ecs_preopen.sh").read_text(encoding="utf-8")
    assert "FULLSCAN_LAND=1" in ecs
    assert "overlay_at" in ecs


def test_preopen_unattended_clock_is_ubuntu() -> None:
    """Laptop off: weekday cron + orch heal must not queue on ECS."""
    yml = (ROOT / ".github" / "workflows" / "preopen_all.yml").read_text(
        encoding="utf-8")
    orch = (ROOT / ".github" / "workflows" / "daily_orchestrator.yml").read_text(
        encoding="utf-8")
    assert 'cron: "55 9 * * 1-5"' in yml
    assert "github.event_name == 'schedule'" in yml
    assert "dispatch_preopen_ubuntu" in orch
    assert "inputs[llm_backend]=deepseek" in orch
    assert '-f "inputs[runner]=ubuntu"' in orch
    # Bare dispatch would default runner=ecs and sit on an offline box.
    assert "maybe preopen_all.yml" in orch


def test_holiday_overlay_uses_last_session() -> None:
    text = (ROOT / "src" / "map_heat.py").read_text(encoding="utf-8")
    assert "last_closed_session" in text
    assert "copied" in text and "last session" in text


def test_weather_step_rejects_pre_0535_stamp() -> None:
    pre = (ROOT / "src" / "run_preopen_all.py").read_text(encoding="utf-8")
    book = (ROOT / "src" / "run_stock_book_all.py").read_text(encoding="utf-8")
    assert "skip_if_good.check_label_weather" in pre
    assert "skip_if_good.check_label_weather" in book
    assert "weather_stamped_before_open" in (
        ROOT / "src" / "skip_if_good.py").read_text(encoding="utf-8")


def test_deepseek_preflight_is_wired() -> None:
    pre = (ROOT / "src" / "run_preopen_all.py").read_text(encoding="utf-8")
    assert "_deepseek_credits_ok" in pre
    assert "credits_preflight" in pre
    ds = (ROOT / "src" / "deepseek_client.py").read_text(encoding="utf-8")
    assert "def credits_preflight" in ds
    assert "402" in ds


def test_news_parse_honors_bypass_when_missing() -> None:
    """09-09: orchestrator called parse after 09:25; module returned no file."""
    parse_src = (ROOT / "src" / "news_parse.py").read_text(encoding="utf-8")
    assert "preopen.bypass_cutoff()" in parse_src
    assert "not writing a late" in parse_src
    assert "PREOPEN_BYPASS_CUTOFF" in parse_src
    actions = (ROOT / "src" / "news_actions.py").read_text(encoding="utf-8")
    assert "preopen.bypass_cutoff()" in actions
    judge = (ROOT / "src" / "run_news_judge.py").read_text(encoding="utf-8")
    assert "preopen.bypass_cutoff()" in judge


def test_parse_runs_when_credits_fail_or_past_cutoff() -> None:
    """09-09 hole: skip_writes ate parse on 402 / 09:25. Parse is file/DB."""
    pre = (ROOT / "src" / "run_preopen_all.py").read_text(encoding="utf-8")
    parse_at = pre.index('step("news_parse"')
    essays_at = pre.index('step("events"')
    credits_at = pre.index("skip LLM essays (DeepSeek credits)")
    assert parse_at < essays_at
    assert "skip_essays" in pre
    assert '"news_parse"' not in pre[pre.index("llm_steps"):pre.index("def step")]
    # 402 / late must not set skip_writes (that skipped parse).
    assert "skip LLM packet (DeepSeek credits)" not in pre
    assert "parse still runs if missing" in pre
    assert credits_at < essays_at
    assert "timeout_s=45 if clock_late" in pre


def test_map_heat_passthrough_flag_skips_llm(tmp_path: Path | None = None) -> None:
    orig = mr.OUT
    with tempfile.TemporaryDirectory() as d:
        out = Path(d)
        mr.OUT = out
        date = "2026-09-09"
        (out / f"{date}_map_heat.json").write_text(json.dumps({
            "macro_gate": False, "size_gate": False, "earnings_gate": False,
            "tape": [{"ticker": "ES"}], "econ": [], "earnings": [],
        }), encoding="utf-8")
        cards = [{
            "industry": f"I{i}", "sector": "X", "action": "HEAT",
            "subsector_dir": "flat", "conviction": "low",
            "captains": [{"ticker": "AAA", "sent": "none",
                          "search_note": "n/a", "evidence": []}],
        } for i in range(22)]
        (out / f"{date}_research_baseline.json").write_text(json.dumps({
            "generated_at": "2026-09-08T22:00:00",
            "n_targets": 22,
            "cards": cards,
            "opportunities": [],
            "parent_splits": [],
        }), encoding="utf-8")
        payload = mr.run(date, passthrough=True)
        assert payload["passthrough"] is True
        qc = output_qc.qc_map_heat_research(out / f"{date}_research.md")
        assert qc.ok, qc.reason
    mr.OUT = orig


def main() -> None:
    tests = [
        test_news_parse_falls_back_to_digest,
        test_qc_news_parse_db_timeout_is_actionable,
        test_recent_news_raises_after_timeout_retries,
        test_bypass_cutoff_skips_refuse,
        test_ubuntu_late_heal_after_0925,
        test_run_preopen_cli_has_bypass_not_permanent_force,
        test_map_heat_passthrough_flag_skips_llm,
        test_incremental_land_hooks,
        test_preopen_unattended_clock_is_ubuntu,
        test_holiday_overlay_uses_last_session,
        test_weather_step_rejects_pre_0535_stamp,
        test_deepseek_preflight_is_wired,
        test_parse_runs_when_credits_fail_or_past_cutoff,
        test_news_parse_honors_bypass_when_missing,
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
