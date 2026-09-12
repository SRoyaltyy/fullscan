"""Contracts for the 2026-09-04 job-hardening pass. No network.

Run: PYTHONPATH=. python3 -m src.test_job_hardening
"""
from __future__ import annotations

import copy
import os
import time
from pathlib import Path
from unittest import mock

from src import config, deepseek_client as dc
from src.test_llm_routing import _fake_response, _reset

ROOT = Path(__file__).resolve().parent.parent
WF = ROOT / ".github" / "workflows"


def test_grok_only_default_is_off() -> None:
    os.environ.pop("GROK_ONLY", None)
    os.environ.pop("LLM_BACKEND", None)
    os.environ.pop("FORCE_DEEPSEEK", None)
    config.OPENCLAW_GATEWAY_URL = "http://gw:18789"
    assert config.grok_only() is False
    config.apply_llm_backend("auto")
    assert config.grok_only() is False


def test_http_500_does_not_trip_breaker() -> None:
    _reset(openclaw_url="http://gw:18789", deepseek_key="ds-key", grok_only=False)
    urls: list[str] = []

    def fake_post(url, headers=None, json=None, timeout=None):
        urls.append(url)
        if "gw:18789" in url:
            return _fake_response(500)
        return _fake_response(200, "DEEPSEEK ANSWER")

    with mock.patch.object(dc.requests, "post", side_effect=fake_post), \
            mock.patch.object(dc.time, "sleep"):
        text = dc.chat([{"role": "user", "content": "hi"}],
                       model="deepseek-chat", tools=False)
    assert text == "DEEPSEEK ANSWER"
    assert not dc._OPENCLAW_STATE["down"]


def test_401_does_trip_breaker_then_deepseek() -> None:
    _reset(openclaw_url="http://gw:18789", deepseek_key="ds-key", grok_only=False)
    urls: list[str] = []

    def fake_post(url, headers=None, json=None, timeout=None):
        urls.append(url)
        if "gw:18789" in url:
            r = _fake_response(401)
            err = dc.requests.HTTPError("401 Client Error")
            err.response = r
            r.raise_for_status.side_effect = err
            return r
        return _fake_response(200, "DEEPSEEK ANSWER")

    with mock.patch.object(dc.requests, "post", side_effect=fake_post), \
            mock.patch.object(dc.time, "sleep"):
        text = dc.chat([{"role": "user", "content": "hi"}],
                       model="deepseek-chat", tools=False)
    assert text == "DEEPSEEK ANSWER"
    assert dc._OPENCLAW_STATE["down"]


def test_deepseek_402_returns_empty() -> None:
    _reset(openclaw_url="", deepseek_key="ds-key", grok_only=False)

    def fake_post(url, headers=None, json=None, timeout=None):
        r = _fake_response(402)
        r.text = "Payment Required"
        err = dc.requests.HTTPError("402")
        err.response = r
        r.raise_for_status.side_effect = err
        return r

    with mock.patch.object(dc.requests, "post", side_effect=fake_post), \
            mock.patch.object(dc.time, "sleep"):
        text = dc.chat([{"role": "user", "content": "hi"}],
                       model="deepseek-chat", tools=False)
    assert text == ""


def test_db_optional_when_url_missing() -> None:
    os.environ["FULLSCAN_DB_OPTIONAL"] = "1"
    os.environ.pop("DATABASE_URL", None)
    from db.connection import get_connection
    assert get_connection() is None
    os.environ.pop("FULLSCAN_DB_OPTIONAL", None)


def test_cancel_in_progress_off_on_grok_jobs() -> None:
    for name in (
        "postclose_all.yml",
        "postclose_last_closed.yml",
        "map_heat_postclose.yml",
        "daily_pipeline.yml",
        "learn_cycle.yml",
        "xai_reauth.yml",
    ):
        text = (WF / name).read_text(encoding="utf-8")
        assert "cancel-in-progress: true" not in text, name
        assert "cancel-in-progress: false" in text, name
    book = (WF / "stock_book_all.yml").read_text(encoding="utf-8")
    assert "cancel-in-progress: ${{ github.event_name == 'schedule'" in book
    # Fix #1: ubuntu Pre-Open must cancel twins. ECS Grok stays uncanceled.
    pre = (WF / "preopen_all.yml").read_text(encoding="utf-8")
    assert "&& 'ubuntu' || 'ecs'" in pre
    group_line = next(ln for ln in pre.splitlines() if ln.strip().startswith("group: preopen-all-"))
    assert "ubuntu-0" not in group_line and "ubuntu-stop" not in group_line
    assert "&& 'ubuntu' || 'ecs'" in group_line
    assert "cancel-in-progress: ${{ github.event_name == 'push' || github.event.inputs.runner == 'ubuntu' }}" in pre


def test_safe_git_push_used_by_failing_commit_jobs() -> None:
    for name in (
        "preopen_all.yml",
        "postclose_all.yml",
        "postclose_last_closed.yml",
        "stock_book_all.yml",
        "learn_cycle.yml",
        "label_weather.yml",
        "ab_checklist.yml",
        "news_grade.yml",
        "hit_board.yml",
    ):
        text = (WF / name).read_text(encoding="utf-8")
        assert "scripts/safe_git_push.sh" in text, name
        assert "git pull --rebase origin main" not in text, name


def test_deploy_dashboard_follows_preopen_and_book() -> None:
    text = (WF / "deploy-dashboard.yml").read_text(encoding="utf-8")
    assert "Pre-Open ALL (predictive one-shot)" in text
    assert "Stock Book ALL (one-shot)" in text
    assert "dashboard/**" in text
    assert "github.event_name == 'push'" in text


def test_jobs_publish_dashboard_in_place() -> None:
    script = ROOT / "scripts" / "publish_dashboard.sh"
    assert script.is_file()
    body = script.read_text(encoding="utf-8")
    assert "gh-pages" in body
    assert "dashboard/index.html" in body
    pre = (WF / "preopen_all.yml").read_text(encoding="utf-8")
    book = (WF / "stock_book_all.yml").read_text(encoding="utf-8")
    assert "scripts/publish_dashboard.sh" in pre
    assert "scripts/publish_dashboard.sh" in book
    assert "name: dashboard" in pre


def test_all_jobs_degrade_instead_of_failing() -> None:
    pre = (ROOT / "src" / "run_preopen_all.py").read_text(encoding="utf-8")
    post = (ROOT / "src" / "run_postclose_all.py").read_text(encoding="utf-8")
    book = (ROOT / "src" / "run_stock_book_all.py").read_text(encoding="utf-8")
    assert "Not committing as success" not in pre
    assert "[preopen-all] FAIL" not in pre
    assert "[postclose-all] FAIL" not in post
    assert "DEGRADED" in pre
    assert "DEGRADED" in post
    assert "[all] FATAL: no membership" not in book
    assert "[all] FATAL: weather" not in book
    assert "FATAL: no join ranked" not in book
    assert "src.stock_book" in book
    assert "check=False" in book


def test_label_weather_yaml_inputs_not_under_permissions() -> None:
    text = (WF / "label_weather.yml").read_text(encoding="utf-8")
    # inputs must live under workflow_dispatch, not permissions
    perms = text.split("permissions:")[1].split("jobs:")[0]
    assert "run_date:" not in perms
    assert "workflow_dispatch:" in text
    assert "run_date:" in text.split("workflow_dispatch:")[1].split("permissions:")[0]


def test_price_checklist_skips_empty_store() -> None:
    text = (WF / "price_checklist.yml").read_text(encoding="utf-8")
    assert "Price store empty." not in text or "Skipping" in text
    assert "scripts/safe_git_push.sh" in text
    assert "git push origin HEAD:main" not in text


def test_db_optional_without_flag() -> None:
    os.environ.pop("FULLSCAN_DB_OPTIONAL", None)
    os.environ.pop("DATABASE_URL", None)
    from db.connection import get_connection
    assert get_connection() is None


def test_health_does_not_pin_grok_only() -> None:
    text = (ROOT / "src" / "pipeline_health.py").read_text(encoding="utf-8")
    assert 'os.environ["GROK_ONLY"] = "1"' not in text
    assert '"GROK_ONLY": "1"' not in text


def test_ecs_jobs_skip_live_finviz() -> None:
    for name in (
        "preopen_all.yml",
        "postclose_all.yml",
        "postclose_last_closed.yml",
        "map_heat_postclose.yml",
        "stock_book_all.yml",
        "catalyst_daily.yml",
    ):
        text = (WF / name).read_text(encoding="utf-8")
        assert 'FINVIZ_SKIP_LIVE: "1"' in text or "FINVIZ_SKIP_LIVE=1" in text, name


def test_ticker_lookback_defaults_random() -> None:
    text = (WF / "ticker_lookback.yml").read_text(encoding="utf-8")
    assert "defaulting to 50 random" in text
    assert "Provide tickers or check Random" not in text


def test_pipeline_health_audit_default() -> None:
    text = (WF / "pipeline_health.yml").read_text(encoding="utf-8")
    assert "--no-fix" in text
    assert 'GROK_ONLY: "1"' not in text
    assert 'default: "true"' in text  # no_fix default


def test_hit_and_news_grade_still_commit() -> None:
    hit = (WF / "hit_board.yml").read_text(encoding="utf-8")
    news = (WF / "news_grade.yml").read_text(encoding="utf-8")
    assert "cat 03_scoreboard/HIT_BOARD.md || true" in hit
    assert "if: always()" in hit
    assert "if: always()" in news
    assert "src.news_grade" in news and "|| true" in news


def test_diag_exits_zero() -> None:
    text = (WF / "stock_book_diag.yml").read_text(encoding="utf-8")
    assert "exit 0" in text
    assert "|| true" in text
    assert "13:00 ET cron" not in text


def test_preopen_lock_matches_postclose() -> None:
    pre = (ROOT / "scripts" / "ecs_preopen.sh").read_text(encoding="utf-8")
    post = (ROOT / "scripts" / "ecs_map_postclose.sh").read_text(encoding="utf-8")
    yml = (WF / "preopen_all.yml").read_text(encoding="utf-8")
    post_yml = (WF / "postclose_all.yml").read_text(encoding="utf-8")
    assert "locks/preopen.lock" in pre
    assert "locks/preopen.lock" in post
    assert "/tmp/fullscan-preopen.lock" not in pre
    assert "fullscan-persist/locks/preopen.lock" in yml
    assert "locks/map-postclose.lock" in post
    assert "fullscan-persist/locks/map-postclose.lock" in post_yml


def test_heal_targets_all_jobs() -> None:
    text = (ROOT / "src" / "pipeline_health.py").read_text(encoding="utf-8")
    assert '("postclose.", "postclose_all.yml")' in text
    assert '("book.weather", "stock_book_all.yml")' in text
    assert '("book.ab", "stock_book_all.yml")' in text
    assert '("outcome.", "postclose_all.yml")' in text
    assert '("learn.", "postclose_all.yml")' in text
    assert '("book.weather", "label_weather.yml")' not in text


def test_preopen_does_not_skip_python_after_cutoff() -> None:
    yml = (WF / "preopen_all.yml").read_text(encoding="utf-8")
    ecs = (ROOT / "scripts" / "ecs_preopen.sh").read_text(encoding="utf-8")
    orch = (WF / "daily_orchestrator.yml").read_text(encoding="utf-8")
    assert "past 09:25 ET — skip python" not in yml
    assert "not running python" not in ecs
    assert "still land weather/join/AB/book" in ecs
    assert "src.skip_if_good" in orch
    assert "stock_book_all.yml" in orch
    assert "inputs[runner]=ubuntu" in orch
    assert "inputs[skip_llm]=true" in orch
    assert "inputs[skip_extras]=true" in orch
    assert "past 09:00 ET — heal ranker on ubuntu" in orch
    assert 'already_running "preopen_all.yml"' in orch
    assert "book heal stays on ubuntu" in orch
    # 16:10 postclose cron is new and may skip day 1. 16:20 + 17:15 orch fire.
    assert 'cron: "20 20 * * 1-5"' in orch
    assert 'cron: "15 21 * * 1-5"' in orch
    assert "maybe postclose_all.yml" in orch
    assert "skip postclose_all.yml second writer" in orch
    sig = (ROOT / "src" / "skip_if_good.py").read_text(encoding="utf-8")
    assert "sidecar already writing last-closed" in sig
    assert "POSTCLOSE_ALL_WORKFLOW_NAME" in sig
    assert "--job postclose_all || return 1" in orch
    assert "skip Post-Close ALL until 16:00 ET" in orch
    assert "18h Post-Close ALL spans midnight" in orch
    assert 'WF" != "postclose_all.yml"' in orch
    # 17:15 is the existing cron. New 16:10/23:30 may skip day 1;
    # dispatch ubuntu/DeepSeek so a dead ECS runner cannot stall the pack.
    assert "dispatch_postclose_ubuntu" in orch
    assert "MISSING night pack → postclose_all.yml ubuntu/DeepSeek" in orch
    assert "inputs[llm_backend]=deepseek" in orch
    assert "active run is push last-closed heal — queue ubuntu night_pack" in orch
    assert "dispatch_last_closed_sidecar" in orch
    assert "postclose_last_closed.yml/dispatches" in orch
    assert "MISSING last-closed pack → postclose_last_closed.yml" in orch


def test_ranker_inputs_before_llm_packet() -> None:
    pre = (ROOT / "src" / "run_preopen_all.py").read_text(encoding="utf-8")
    book = (ROOT / "src" / "run_stock_book_all.py").read_text(encoding="utf-8")
    assert "wait_for_night_baseline" in pre
    wx = pre.find('step("weather", "Weather / regime"')
    pred = pre.find('step("general_predict"')
    assert 0 <= wx < pred
    wxb = book.find("Weather / regime (before LLM heals")
    ev = book.find("Event scanner (primary)")
    assert 0 <= wxb < ev
    land = pre.find("Stock book + paper dashboard")
    cat = pre.find('step("catalyst"')
    grok = pre.find("grok_review.review_preopen")
    assert 0 <= land < cat
    assert 0 <= land < grok
    assert "past 09:25 ET — book still runs" in pre
    assert "skip_extras=True" in pre
    assert "refresh_ranker=True" in pre
    assert "refresh_ranker" in book
    assert "land_file.land" in pre
    assert "safe_git_push.sh" in (ROOT / "src" / "land_file.py").read_text(encoding="utf-8")
    assert "timeout_s=45 if clock_late" in pre
    assert "passthrough after timeout" in pre
    assert "MAP_HEAT_REFRESH_TIMEOUT" in pre
    assert "--passthrough" in pre
    assert 'PREOPEN_LLM_TIMEOUT", "420"' in pre
    assert "10800s ate 2026-09-04" in pre
    assert "subprocess {llm_sub_t}s" in pre or "llm_sub_t" in pre
    assert 'timeout_s=llm_sub_t' in pre
    assert "timeout_s=2400" in pre
    assert "TimeoutExpired" in pre
    assert "weather missing/thin — retry --offline" in pre
    assert "timeout_s=1500" in pre
    assert "timeout_s=180" in pre
    assert "timeout_s=50" in pre
    assert "parse_t = 120" in pre
    assert "retry --limit 80" in pre
    assert "rebuild after essays" in pre
    gen = pre.find('step("general_predict"')
    heat = pre.find('step(\n                    "map_heat_research"')
    if heat < 0:
        heat = pre.find('"Map heat morning delta refresh"')
    assert 0 <= gen < heat
    assert "_exists_gt" in pre
    assert "skip_extras" in book
    extras_gate = book.find("skip extras before book")
    news_parse = book.find("[all] → News parse")
    assert 0 <= extras_gate < news_parse
    assert "TimeoutExpired" in book
    assert "ab_t = 1500" in book
    assert "wx_t = 50" in book
    assert "parse_t = 120" in book
    assert "PREOPEN_LLM_TIMEOUT" in book
    assert "hung Grok must not block the book" in book
    assert "--offline" in book
    assert "weather missing/thin — retry --offline" in book
    post = (ROOT / "src" / "run_postclose_all.py").read_text(encoding="utf-8")
    assert "TimeoutExpired" in post
    assert "POSTCLOSE_LLM_TIMEOUT" in post
    assert "llm_timeout_s=llm_to" in post
    assert "timeout_s=300" in post  # news_grade yfinance bound
    assert "src.news_grade" in post
    assert "sector_wall = max(5400, 11 * (llm_to + 90))" in post
    post_yml = (WF / "postclose_all.yml").read_text(encoding="utf-8")
    assert 'OPENCLAW_TIMEOUT: "900"' in post_yml
    assert 'OPENCLAW_TIMEOUT: "10800"' not in post_yml
    assert "timeout-minutes: 1080" in post_yml
    assert 'cron: "10 20 * * 1-5"' in post_yml
    assert 'cron: "30 3 * * 2-6"' in post_yml
    assert "postclose-all-${{" in post_yml
    assert "github.event_name == 'schedule'" in post_yml
    # ubuntu/DeepSeek must not inherit ECS HOME or try Grok first.
    assert "HOME: \"/home/gha\"" not in post_yml
    assert "FULLSCAN_HOME: \"/home/gha\"" not in post_yml
    assert "'/home/runner'" in post_yml
    assert "&& 'deepseek'" in post_yml
    assert 'export HOME="${FULLSCAN_HOME:-/home/gha}"' not in post_yml
    assert "MAP_POSTCLOSE_LOCK" in post_yml
    assert "leftover ECS files must not fake SKIP" in post_yml
    assert "git reset --hard origin/main" in post_yml
    assert "git clean -fd -- 01_daily/general" in post_yml
    assert "01_daily/_transcripts" in post_yml
    assert "git clean -fd -- 01_daily 02_lessons" not in post_yml
    assert "ECS systemd job holds the lock" in post_yml
    assert "ECS systemd job grabbed the lock" in post_yml
    assert "def _push_pack" in post
    unit = (ROOT / "scripts" / "systemd" / "fullscan-map-postclose.service").read_text(
        encoding="utf-8")
    assert "TimeoutStartSec=18h" in unit
    news_py = (ROOT / "src" / "news_grade.py").read_text(encoding="utf-8")
    assert "threads=False" in news_py
    assert "setdefaulttimeout(30)" in news_py
    wx_yml = (WF / "label_weather.yml").read_text(encoding="utf-8")
    assert "weather missing/thin — retry --offline" in wx_yml
    learn_yml = (WF / "learn_cycle.yml").read_text(encoding="utf-8")
    assert 'OPENCLAW_TIMEOUT: "900"' in learn_yml
    sb = (ROOT / "src" / "stock_book.py").read_text(encoding="utf-8")
    assert "input_health.check(date)" in sb
    assert "predict.md if ingest lagged" in sb
    assert "weather.load_runs(asof)" in sb
    assert "BUY is the green pile when it is thick enough" in sb
    assert "def _horizon_pick" in sb
    assert "input_health.load(date) or input_health.check" not in sb
    pre_yml = (WF / "preopen_all.yml").read_text(encoding="utf-8")
    assert "Land book + green (ubuntu — no Grok, no ECS)" in pre_yml
    assert "--skip-llm --skip-extras" in pre_yml
    assert "Pull scrape + export from main" in pre_yml
    assert "git checkout origin/main --" in pre_yml
    assert 'force=true — rank even if a book is on disk' in pre_yml
    assert "collectors.finviz_financials" in pre_yml
    assert "data/exports/" in pre_yml
    scrape_yml = (WF / "finviz_preopen_scrape.yml").read_text(encoding="utf-8")
    assert "collectors.finviz_financials" in scrape_yml
    assert "data/exports/" in scrape_yml
    assert "src.finviz_digest --date $DATE --force" in scrape_yml
    skip = (ROOT / "src" / "skip_if_good.py").read_text(encoding="utf-8")
    assert "elite export missing/thin" in skip
    assert "1d BUY has printed dead relvol" in skip
    assert "book_1d_has_dead_relvol" in skip
    assert "1d BUY is not all-green" in skip
    assert "book_1d_breaks_all_green" in skip
    assert "sector outcomes missing" in skip
    assert "sector reflects missing" in skip
    assert "reflect missing" in skip
    assert "check_general_reflect" in skip
    assert "check_sector_reflects" in skip
    assert "def check_sector_reflects" in skip
    assert "book ranked without same-day essays" in skip
    assert "book_missing_same_day_essays" in skip
    fin = (ROOT / "collectors" / "finviz_financials.py").read_text(encoding="utf-8")
    assert "America/New_York" in fin
    ch1 = (ROOT / "src" / "fetch_channel1.py").read_text(encoding="utf-8")
    assert "setdefaulttimeout(min(20, _YF_TIMEOUT))" in ch1
    assert "_YF_TIMEOUT = 20" in ch1
    assert "skip remaining FRED" in ch1
    book_yml = (WF / "stock_book_all.yml").read_text(encoding="utf-8")
    assert "skip_extras:" in book_yml
    assert "past 09:25 ET — skip LLM + extras" in book_yml
    assert 'cron: "10 10 * * 1-5"' in book_yml
    assert 'cron: "15 13 * * 1-5"' in book_yml
    assert "ubuntu land-book" in book_yml
    assert 'github.event_name == \'schedule\'' in book_yml
    assert 'github.event_name == \'push\'' in book_yml
    assert 'github.event_name == \'workflow_run\'' in book_yml
    assert "stock-book-all-ubuntu" in book_yml
    assert "stock-book-all-ecs" in book_yml
    assert "HOME: \"/home/gha\"" not in book_yml
    assert "'/home/runner'" in book_yml
    assert 'branches: [main]' in book_yml
    assert "src/green_pile.py" in book_yml
    assert "src/stock_book.py" in book_yml
    assert "Skip if book already satisfies the gate" in book_yml
    assert "Book + green already all-green" in book_yml
    assert "Pre-Open ALL (predictive one-shot)" in book_yml
    assert "Do not add a cron back" not in book_yml
    health = (ROOT / "src" / "pipeline_health.py").read_text(encoding="utf-8")
    hold = health.split('wf == "stock_book_all.yml" and (')[1].split("):")[0]
    assert "preopen_all.yml" not in hold
    assert '_already_running("preopen_all.yml")' not in hold
    post = (ROOT / "scripts" / "ecs_map_postclose.sh").read_text(encoding="utf-8")
    assert "last_closed_session" in post
    assert "night pack still missing, running anyway" in post
    assert "preopen lock held — skip (exit 0" not in post
    assert "OPENCLAW_TIMEOUT=900" in post
    assert '[ "${OPENCLAW_TIMEOUT}" = "10800" ]' in post
    assert "exit 1" not in post
    assert "--job postclose_all" in post
    assert "night_pack_dates" in post
    assert "Keep 01_daily/_transcripts" in post
    assert "git clean -fd -- 01_daily/general" in post
    assert 'SKIP_ARGS=(--job postclose_all)' in post
    assert 'PC_ARGS=(--llm-backend "${LLM_BACKEND:-auto}")' in post
    assert 'dated=${SOURCE_DATE:-night_pack_dates}' in post
    learn = (ROOT / "src" / "run_postclose_all.py").read_text(encoding="utf-8")
    assert 'src.learn_cycle", "--date"' in learn or "--date\", date" in learn
    assert "night_pack_dates" in learn
    assert "def _run_one" in learn
    learn_yml = (WF / "learn_cycle.yml").read_text(encoding="utf-8")
    assert "last_closed_session" in learn_yml
    assert '--date "${closed_session}"' in learn_yml


def test_ecs_timers_stay_green_and_push() -> None:
    pre = (ROOT / "scripts" / "ecs_preopen.sh").read_text(encoding="utf-8")
    post = (ROOT / "scripts" / "ecs_map_postclose.sh").read_text(encoding="utf-8")
    assert "publish_dashboard.sh" in pre
    assert "dashboard/" in pre
    assert "exit 0" in pre
    assert "exit 0" in post
    assert "dispatch stock_book_all.yml ubuntu skip-llm" in pre
    assert "inputs[skip_extras]=true" in pre


def test_empty_futures_tape_not_ready() -> None:
    from src.output_qc import qc_map_heat
    import json
    import tempfile
    with tempfile.TemporaryDirectory() as td:
        p = Path(td) / "heat.json"
        p.write_text(json.dumps({
            "industries": [{"spx_leaders": ["A"]}] * 60,
            "sectors": list(range(11)),
            "tape": [],
        }), encoding="utf-8")
        r = qc_map_heat(p)
        assert not r.ok
        assert r.reason == "empty_futures_tape"


def test_sector_outcome_skips_existing_and_times_out_yf() -> None:
    """Resume leftover 09-03 sectors without re-calling the LLM."""
    src = (ROOT / "src" / "run_sector_outcome.py").read_text(encoding="utf-8")
    assert "setdefaulttimeout(30)" in src
    assert "threads=False" in src
    assert "outcome already on disk" in src
    assert "reuse transcript" in src
    assert "last_assistant" in src
    assert "_persist" in src
    assert "try Ticker.history" in src
    assert "_fill_from_history" in src
    assert "empty/thin/tool-dump LLM" in src
    assert "not writing a stub" in src
    assert "disk file is a tool-dump" in src
    assert "is_tool_dump" in src
    # One failure must not abort the remaining 10.
    assert 'print(f"[sector-outcome] WARN {sector}: {e}")' in src
    assert "SECTOR_ONE_TIMEOUT" in src
    assert "SECTOR_GRADE_CHILD" in src
    assert "TimeoutExpired" in src
    assert "killed after" in src


def test_sector_parent_continues_after_one_timeout() -> None:
    """A hung first-sector chat() must not eat the other 10."""
    import subprocess
    from src import run_sector_outcome as so

    os.environ.pop("SECTOR_GRADE_CHILD", None)
    os.environ["SECTOR_ONE_TIMEOUT"] = "1"
    with mock.patch.object(so.subprocess, "run",
                           side_effect=subprocess.TimeoutExpired(["x"], 1)):
        so._run_one_bounded("Technology", "2026-09-03")
    os.environ.pop("SECTOR_ONE_TIMEOUT", None)

    called: list[tuple[str, str]] = []
    os.environ["SECTOR_GRADE_CHILD"] = "1"
    with mock.patch.object(so, "run_one",
                           side_effect=lambda s, d: called.append((s, d))):
        so._run_one_bounded("Healthcare", "2026-09-03")
    os.environ.pop("SECTOR_GRADE_CHILD", None)
    assert called == [("Healthcare", "2026-09-03")]


def test_sector_reflect_skips_existing() -> None:
    src = (ROOT / "src" / "run_sector_reflect.py").read_text(encoding="utf-8")
    assert "reflect already on disk" in src
    assert "reuse transcript" in src
    assert "last_assistant" in src
    assert "_persist" in src
    assert "actuals from outcome.md" in src
    assert 'print(f"[sector-reflect] WARN {sector}: {e}")' in src
    assert "SECTOR_ONE_TIMEOUT" in src
    assert "TimeoutExpired" in src
    assert "killed after" in src
    assert "disk file is a tool-dump" in src
    assert "is_tool_dump" in src
    from src.run_sector_reflect import _pct_from_outcome_md
    text = "# Sector Outcome\n\nActuals: {'etf': 'XLK', 'pct': 1.25, 'spy_pct': 0.4}\n\nbody"
    assert _pct_from_outcome_md(text) == 1.25
    assert _pct_from_outcome_md("no actuals") is None


def test_etf_actual_falls_back_to_history() -> None:
    from src import run_sector_outcome as so

    bars = {
        "XLK": [
            {"date": "2026-09-02", "open": 100.0, "close": 100.0},
            {"date": "2026-09-03", "open": 101.0, "close": 102.0},
        ],
        "SPY": [
            {"date": "2026-09-02", "open": 200.0, "close": 200.0},
            {"date": "2026-09-03", "open": 201.0, "close": 202.0},
        ],
    }

    def fake_hist(symbol, days=15):
        return bars[symbol]

    with mock.patch.object(so, "_bars_via_history",
                           side_effect=lambda sym, d: (
                               bars[sym][1]["open"], bars[sym][1]["close"],
                               bars[sym][0]["close"])):
        out = so._fill_from_history(
            {"etf": "XLK", "pct": None, "spy_pct": None, "rel": None,
             "open": None, "close": None},
            "XLK", "2026-09-03")
    assert abs(out["pct"] - 2.0) < 1e-9
    assert abs(out["spy_pct"] - 1.0) < 1e-9
    assert out["source"] == "yf_history"


def test_general_outcome_skips_existing_and_reuses_transcript() -> None:
    src = (ROOT / "src" / "run_outcome.py").read_text(encoding="utf-8")
    assert "outcome already on disk" in src
    assert "reuse transcript" in src
    assert "last_assistant" in src
    assert "disk file is a tool-dump" in src
    assert "is_tool_dump" in src
    assert "empty/thin/tool-dump LLM" in src
    assert "not writing a stub" in src
    # LLM only after a miss — a hung 09-04 retry must not require_llm first.
    assert src.index("last_assistant") < src.index("config.require_llm()")


def test_general_reflect_writes_gate_file_and_reuses_transcript() -> None:
    """Live main has 09-03 reflect_trace + 6k transcript and ZERO *_reflect.md."""
    import json
    import tempfile
    from src.run_reflect import last_assistant

    src = (ROOT / "src" / "run_reflect.py").read_text(encoding="utf-8")
    assert 'f"{date_str}_reflect.md"' in src
    assert "reuse transcript" in src
    assert "reflect already on disk" in src
    assert "disk file is a tool-dump" in src
    assert "is_tool_dump" in src
    with tempfile.TemporaryDirectory() as td:
        p = Path(td) / "tx.json"
        p.write_text(json.dumps({
            "provider": "openclaw",
            "messages": [
                {"role": "user", "content": "hi"},
                {"role": "assistant", "content": "TRIAGE " + ("x" * 250)},
            ],
        }), encoding="utf-8")
        text = last_assistant(str(p))
        assert text.startswith("TRIAGE")
        assert len(text) >= 200
        assert last_assistant(str(Path(td) / "missing.json")) == ""
        dump = Path(td) / "dump.json"
        dump.write_text(json.dumps({
            "messages": [
                {"role": "assistant", "content": (
                    'TRIAGE\n<｜DSML｜tool_calls>\n'
                    '<invoke name="web_search">' + ("q" * 200)
                )},
            ],
        }), encoding="utf-8")
        assert last_assistant(str(dump)) == ""


def test_search_and_sector_rounds_are_bounded() -> None:
    """Dead SearXNG / hung ddgs must not eat the first sector before persist.

    These files are intentionally NOT on postclose_all.yml push: — merging
    them must not queue a twin behind the live ubuntu heal.
    """
    from src import websearch as ws

    push_block = (WF / "postclose_all.yml").read_text(encoding="utf-8")
    push_paths = push_block.split("push:")[1].split("workflow_dispatch:")[0]
    assert "src/websearch.py" not in push_paths
    assert "src/deepseek_client.py" not in push_paths
    assert "src/config.py" not in push_paths

    assert ws.SEARXNG_TIMEOUT <= 10
    assert ws.SEARXNG_ATTEMPTS == 1
    assert ws.DDG_TIMEOUT <= 15

    def hang(_q, _n):
        time.sleep(30)
        return [{"title": "late", "url": "http://x", "snippet": "no"}]

    t0 = time.monotonic()
    try:
        ws._run_bounded(hang, 0.2, "q", 3)
        raise AssertionError("hung search must raise")
    except TimeoutError as e:
        assert "exceeded" in str(e)
    assert time.monotonic() - t0 < 2.0

    assert dc._effective_tool_rounds("SECTOR OUTCOME Technology 2026-09-03",
                                     None) == 2
    assert dc._effective_tool_rounds("SECTOR REFLECT Healthcare 2026-09-03",
                                     None) == 2
    assert dc._effective_tool_rounds(
        "MAP POSTCLOSE captains_technology 2026-09-07", None) == 2
    assert dc._effective_tool_rounds("GENERAL OUTCOME 2026-09-03", None) == 10
    assert dc._effective_tool_rounds("SECTOR OUTCOME X", 1) == 1
    assert getattr(config, "SECTOR_TOOL_ROUNDS", 0) == 2
    assert getattr(config, "MODEL_REFLECT", "") == "deepseek-chat"
    assert getattr(config, "SECTOR_MAX_SEARCHES", 0) == 2
    assert getattr(config, "SECTOR_CHAT_BUDGET_S", 0) == 420
    ds_src = (ROOT / "src" / "deepseek_client.py").read_text(encoding="utf-8")
    assert "is_tool_dump" in ds_src
    assert "ignoring tool-dump content" in ds_src
    assert "from tool-dump content" in ds_src
    assert "force no-tool close" in ds_src
    mh = (ROOT / "src" / "map_heat_postclose.py").read_text(encoding="utf-8")
    assert "JSON-only close" in mh
    assert "tools=False" in mh
    assert "SECTOR_CHUNK" in mh
    assert "honest_none_cards" in mh
    assert "honest-none" in mh
    assert "synthesis tickers dropped" in mh
    assert 'raise SystemExit("; ".join(opp_errors' not in mh
    rs = (ROOT / "src" / "map_heat_research.py").read_text(encoding="utf-8")
    assert "def salvage_cards" in rs


def test_ubuntu_postclose_skips_grok_and_keeps_runner_home() -> None:
    """16:10/23:30 + orch 17:15 must run DeepSeek under the runner HOME."""
    post_yml = (WF / "postclose_all.yml").read_text(encoding="utf-8")
    orch = (WF / "daily_orchestrator.yml").read_text(encoding="utf-8")
    book_yml = (WF / "stock_book_all.yml").read_text(encoding="utf-8")
    assert "HOME: \"/home/gha\"" not in post_yml
    assert "FULLSCAN_HOME: \"/home/gha\"" not in post_yml
    assert "'/home/runner'" in post_yml
    assert "&& 'deepseek'" in post_yml
    assert 'export HOME="${FULLSCAN_HOME:-/home/gha}"' not in post_yml
    assert "dispatch_postclose_ubuntu" in orch
    assert "inputs[llm_backend]=deepseek" in orch
    assert "HOME: \"/home/gha\"" not in book_yml
    assert "'/home/runner'" in book_yml
    ds = (ROOT / "src" / "deepseek_client.py").read_text(encoding="utf-8")
    assert "timeout=(15, config.OPENCLAW_TIMEOUT)" in ds
    assert '"connect timeout"' in ds
    # Merge of healer code must start ubuntu last-closed (dispatch is 403).
    assert "github.event_name == 'push'" in post_yml
    assert "push heal — last_closed=" in post_yml
    assert "src/skip_if_good.py" not in post_yml
    assert "01_daily/" not in post_yml.split("push:")[1].split("workflow_dispatch:")[0]
    assert "postclose_last_closed.yml" not in post_yml.split("push:")[1].split("workflow_dispatch:")[0]


def test_persist_dir_falls_back_when_gha_unwritable() -> None:
    """GH-hosted ubuntu cannot mkdir /home/gha — snapshot must still land."""
    import tempfile

    from src import run_preopen_all as rpa

    home = tempfile.mkdtemp(prefix="fs-persist-home-")
    prev = {k: os.environ.get(k) for k in (
        "FULLSCAN_PERSIST", "FULLSCAN_HOME", "HOME")}
    try:
        os.environ["FULLSCAN_PERSIST"] = "/root/fullscan-persist-denied"
        os.environ["FULLSCAN_HOME"] = home
        os.environ["HOME"] = home
        got = rpa.persist_dir()
        assert got == Path(home) / "fullscan-persist"
        assert got.is_dir()
        probe = got / "ok.txt"
        probe.write_text("ok", encoding="utf-8")
        assert probe.read_text(encoding="utf-8") == "ok"
    finally:
        for k, v in prev.items():
            if v is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = v


def test_safe_git_push_keeps_dated_ranker_on_conflict() -> None:
    """Land-book vs stock-book race must not abort the essay/book persist."""
    text = (ROOT / "scripts" / "safe_git_push.sh").read_text(encoding="utf-8")
    # 2026-09-10: plumbing land. The work tree, HEAD and the real index
    # are never rebased/stashed/reset under the python writer.
    code = "\n".join(
        ln for ln in text.splitlines() if not ln.lstrip().startswith("#"))
    for banned in ("git stash", "git rebase", "git merge ", "git reset --hard",
                   "git pull", "git checkout origin/main --",
                   'git checkout "$LOCAL"', "git commit -m"):
        assert banned not in code, banned
    assert "git write-tree" in text
    assert "git commit-tree" in text
    assert 'git push -q origin "$new:refs/heads/main"' in text
    assert "git update-index -z --add --index-info" in text
    assert "GIT_INDEX_FILE=\"$TMP_INDEX\"" in text
    # Three-way on blobs: skip if already on main, ours if only we changed.
    assert '[ "$o" = "$u" ]' in text
    assert '[ "$u" = "$b" ]' in text
    # Conflict policy: dashboard → main (sleeve-merge / Pages), day-board and
    # dated packet/ranker → ours, scoreboard + day_board → union.
    assert "keeping origin/main (sleeve-merge / Pages)" in text
    assert "dashboard/day-board/*)        echo ours" in text
    assert "03_scoreboard/scoreboard.json) echo union_sb" in text
    assert "data/day_board/*)             echo union_db" in text
    assert "src.scoreboard --merge-ours" in text
    assert "src.day_board --merge-ours" in text
    # 2026-09-08 finish-holes: one missing note pathspec made
    # `git add a b missing` stage nothing, so dashboard never reached main.
    assert "skip missing" in text
    assert 'git add "$@"' not in text
    # Never delete on main; never push ignored files; retry on rejection.
    assert 'never land deletions' in text
    assert "--others --exclude-standard" in text
    assert "push rejected (main moved)" in text
    assert 'ATTEMPTS="${SAFE_PUSH_ATTEMPTS:-6}"' in text
    # 2026-09-09: leftover UU must be resolved in the index only.
    assert "leftover unmerged from prior land" in text
    assert "clear_leftover_unmerged" in text
    assert 'git reset -q -- "$f"' in text
    assert "x-access-token" in text
    # 11MB Elite export (finviz_2026-09-09.csv) vanished with stash drop.
    assert "finviz_2026-09-09.csv" in text


def test_safe_git_push_refuses_conflict_marked_files() -> None:
    """01_daily/_ecs_clock.md reached main with `<<<<<<< Updated upstream`."""
    import subprocess
    import tempfile
    text = (ROOT / "scripts" / "safe_git_push.sh").read_text(encoding="utf-8")
    assert "has git conflict markers — not landing it" in text
    start = text.index("has_conflict_markers() {")
    fn = text[start: text.index("\n}\n", start) + 3]

    def check(name: str, body: str) -> bool:
        with tempfile.TemporaryDirectory() as td:
            p = os.path.join(td, name)
            with open(p, "w", encoding="utf-8") as fh:
                fh.write(body)
            r = subprocess.run(
                ["bash", "-c", fn + '\nhas_conflict_markers "$1"', "_", p],
                capture_output=True, text=True, timeout=20)
            return r.returncode == 0

    bad = ("# ECS clock status\n\n<<<<<<< Updated upstream\n- generated: a\n"
           "=======\n- generated: b\n>>>>>>> Stashed changes\n")
    assert check("_ecs_clock.md", bad)
    assert check("day.json", '{"a": 1}\n<<<<<<< HEAD\n{"a": 2}\n>>>>>>> x\n')
    # A markdown rule / setext heading is not a conflict.
    assert not check("note.md", "# Title\n=======\nbody\n")
    assert not check("essay.md", "quote: <<<<<<< not at col 0\n")
    # Binary-ish artefacts are never scanned (no false positives on .csv.gz).
    assert not check("dump.parquet", "<<<<<<< a\n>>>>>>> b\n")
    # The clock file on the branch itself is clean.
    clock = (ROOT / "01_daily" / "_ecs_clock.md").read_text(encoding="utf-8")
    assert "<<<<<<<" not in clock and ">>>>>>>" not in clock


def test_preopen_harden_halt_reverted() -> None:
    """2026-09-08 HALT must not stay on for tomorrow's unattended run."""
    orch = (WF / "daily_orchestrator.yml").read_text(encoding="utf-8")
    book = (WF / "stock_book_all.yml").read_text(encoding="utf-8")
    pre = (WF / "preopen_all.yml").read_text(encoding="utf-8")
    assert "if: false" not in orch
    assert "HALT 2026-09-08" not in orch
    assert "if: false" not in book
    assert "HALT 2026-09-08" not in book
    assert "if: false" not in pre
    assert "STOP all live writers" not in pre


def test_incremental_land_and_day_board() -> None:
    """Write A → QC A → push A. Day board is an .io page, not an Action."""
    pre = (ROOT / "src" / "run_preopen_all.py").read_text(encoding="utf-8")
    assert "land_file.land" in pre
    assert "src.publish_live_boards" in pre
    yml = (WF / "preopen_all.yml").read_text(encoding="utf-8")
    assert "leftover sweep" in yml
    assert "timeout-minutes: 180" in yml
    assert "timeout-minutes: 240" in yml
    assert "ubuntu-h0909c" not in yml
    gates = (ROOT / "src" / "packet_gates.py").read_text(encoding="utf-8")
    assert "MIN_JSON_BYTES = 80" in gates
    assert "MIN_WEATHER_BYTES = 800" in gates
    assert "min_bytes: int = MIN_WEATHER_BYTES" in gates
    assert "vix_unknown" in gates
    assert "yields_unknown" in gates
    land = (ROOT / "src" / "land_file.py").read_text(encoding="utf-8")
    assert land.index('endswith("_weather.json")') < land.index("json_too_small")
    assert "MIN_WEATHER_BYTES" in land
    assert '"live_boards"' in land
    assert "latest_suggestions.json" in land
    qc = (ROOT / "src" / "output_qc.py").read_text(encoding="utf-8")
    assert "packet_gates.json_too_small" in qc
    assert "Path(path).stat().st_size" in qc
    assert "leftover sweep" in (WF / "preopen_all.yml").read_text(encoding="utf-8")
    assert "leftover sweep" in (WF / "stock_book_all.yml").read_text(encoding="utf-8")
    pub = (ROOT / "scripts" / "publish_dashboard.sh").read_text(encoding="utf-8")
    assert "day-board" in pub
    dep = (WF / "deploy-dashboard.yml").read_text(encoding="utf-8")
    assert "id-token: write" in dep
    assert "day-board" in dep
    orch = (WF / "daily_orchestrator.yml").read_text(encoding="utf-8")
    assert "news_parse.yml" in orch
    assert "news_judge.yml" in orch
    assert 'inputs[force]=true' in orch
    book = (WF / "stock_book_all.yml").read_text(encoding="utf-8")
    assert "before 05:35 ET" in book
    assert "needs: gate" in book
    assert "workflow_dispatch" in book


def test_ubuntu_preopen_not_blocked_by_queued_ecs() -> None:
    """A queued ecs-openclaw job must not block the ubuntu/DeepSeek packet."""
    yml = (WF / "preopen_all.yml").read_text(encoding="utf-8")
    assert "group: preopen-all-${{" in yml
    assert "&& 'ubuntu' || 'ecs'" in yml
    assert "github.event_name == 'push'" in yml
    assert "&& 'deepseek'" in yml
    assert "'/home/runner'" in yml
    assert "no persist lock dir (ubuntu)" in yml
    assert 'export HOME="${FULLSCAN_HOME:-/home/gha}"' not in yml
    assert "HOME: \"/home/gha\"" not in yml
    # Fix #1 / #4: group line is stable ubuntu|ecs — no HHMM fork in the
    # expression. Comments may mention the 2026-09-08 hole.
    group_line = next(ln for ln in yml.splitlines() if ln.strip().startswith("group: preopen-all-"))
    assert "ubuntu-0" not in group_line
    assert "ubuntu-stop" not in group_line
    assert "&& 'ubuntu' || 'ecs'" in group_line
    assert "ARGS=(--llm-backend \"$BACKEND\" --force)" not in yml
    assert "--bypass-cutoff" in yml
    assert "if: false" not in yml
    assert "HALT 2026-09-08" not in yml
    # Packet commit must not include dashboard/ (fix #2 twin HTML race).
    commit = yml.split("Commit predictive artifacts")[1].split("Commit dashboard")[0]
    assert "dashboard/" not in commit
    assert "01_daily/general/" in commit


def test_factor_mine_lands_closed_after_postclose() -> None:
    """Dashboard must remine after the close, not only after the morning book."""
    yml = (WF / "factor_mine.yml").read_text(encoding="utf-8")
    assert "Post-Close ALL (grade + learn + next captains)" in yml
    assert "--land-closed" in yml
    assert 'cron: "25 20 * * 1-5"' in yml
    assert 'cron: "0 12 * * 6"' in yml
    assert "data/factor_mine/panel.json" in yml
    assert "Stock Book ALL (one-shot)" in yml
    assert "Pre-Open ALL (predictive one-shot)" in yml
    src = (ROOT / "src" / "factor_mine.py").read_text(encoding="utf-8")
    assert "def land_closed(" in src
    assert "def payload_covers_session(" in src


def test_last_closed_sidecar_does_not_share_ubuntu_concurrency() -> None:
    """A hung postclose-all-ubuntu push must not block yesterday's grade."""
    yml = (WF / "postclose_last_closed.yml").read_text(encoding="utf-8")
    assert "group: postclose-last-closed-ubuntu" in yml
    assert "group: postclose-all-ubuntu" not in yml
    assert "last_closed_session" in yml
    run_block = yml.split("Post-Close last-closed")[1]
    assert "night_pack_dates" not in run_block
    assert "LLM_BACKEND: deepseek" in yml
    assert "HOME: /home/runner" in yml
    assert "ubuntu-latest" in yml
    assert 'cron: "25 15 * * 1-5"' in yml
    assert 'cron: "15 20 * * 1-5"' in yml
    on_push = yml.split("\n  push:\n", 1)[1].split("\n  workflow_dispatch:", 1)[0]
    assert "postclose_last_closed.yml" in on_push
    assert "src/run_postclose_all.py" not in on_push


def test_postclose_pushes_after_each_llm_layer() -> None:
    """Kill after reflect / sectors / learn must still leave those files on main."""
    src = (ROOT / "src" / "run_postclose_all.py").read_text(encoding="utf-8")
    idx_gen = src.index('step("General outcome"')
    idx_push_gen = src.index("_push_pack(date)", idx_gen)
    idx_ref = src.index('step("General reflect"')
    assert "check_general_reflect(date)" in src[idx_ref:idx_ref + 280]
    assert '_exists_gt(f"01_daily/general/{date}_reflect.md"' not in src
    idx_push_ref = src.index("_push_pack(date)", idx_ref)
    idx_out = src.index('step("Sector outcomes"')
    idx_push_out = src.index("_push_pack(date)", idx_out)
    idx_sec_ref = src.index('step("Sector reflect"')
    idx_push_sec_ref = src.index("_push_pack(date)", idx_sec_ref)
    idx_learn = src.index('step("Learn cycle"')
    idx_push_learn = src.index("_push_pack(date)", idx_learn)
    idx_cap = src.index('step("Captain research')
    assert idx_push_gen < idx_ref, "must persist general outcome before reflect"
    assert idx_push_ref < idx_out, "must persist general reflect before sectors"
    assert idx_push_out < idx_sec_ref, "must persist sector outcomes before reflects"
    assert idx_push_sec_ref < idx_learn, "must persist sector reflects before learn"
    assert idx_push_learn < idx_cap, "must persist dated learnings before captains"
    # A thin dated file written before sector grades must not skip learn.
    learn_block = src[idx_learn:idx_cap]
    assert "check_learn_cycle" not in learn_block
    assert "timeout_s=180" in learn_block
    heat = (ROOT / "src" / "map_heat_postclose.py").read_text(encoding="utf-8")
    assert "setdefaulttimeout(30)" in heat
    assert "threads=False" in heat
    assert "from .run_reflect import last_assistant" in heat
    assert "_reuse_sector_cards" in heat
    assert "last_assistant(str(path))" in heat


def _py312_only_fstrings(path: Path) -> list[int]:
    """Lines where an f-string reuses its own quote character inside ``{}``.

    That is legal on Python 3.12+ (PEP 701) but a SyntaxError on the 3.10
    interpreter the ubuntu Pre-Open ALL job runs.  On 3.12 the tokenizer
    emits FSTRING_START/END tokens, so we can spot the offence without an
    older interpreter; on <3.12 the file would not even import, so we just
    compile it.
    """
    import io
    import tokenize

    src = path.read_text(encoding="utf-8", errors="replace")
    fs_start = getattr(tokenize, "FSTRING_START", None)
    if fs_start is None:
        compile(src, str(path), "exec")
        return []
    fs_end = tokenize.FSTRING_END
    bad: list[int] = []
    quotes: list[str] = []
    for tok in tokenize.generate_tokens(io.StringIO(src).readline):
        if tok.type == fs_start:
            q = tok.string.lstrip("rbfRBF")
            q = q[:3] if q[:3] in ('"""', "'''") else q[:1]
            if q in quotes and len(q) == 1:
                bad.append(tok.start[0])
            quotes.append(q)
        elif tok.type == fs_end:
            if quotes:
                quotes.pop()
        elif tok.type == tokenize.STRING and quotes:
            q = tok.string.lstrip("rbuRBU")
            q = q[:3] if q[:3] in ('"""', "'''") else q[:1]
            if len(q) == 1 and q in quotes:
                bad.append(tok.start[0])
    return sorted(set(bad))


def test_sources_parse_on_python_310() -> None:
    """The ubuntu Pre-Open runs Python 3.10; 09-10's sleeve card died on 3.12-only f-strings."""
    offenders: list[str] = []
    for sub in ("src", "collectors", "scripts", "dashboard"):
        d = ROOT / sub
        if not d.is_dir():
            continue
        for p in sorted(d.rglob("*.py")):
            lines = _py312_only_fstrings(p)
            if lines:
                offenders.append(f"{p.relative_to(ROOT)}:{','.join(map(str, lines))}")
    assert not offenders, "3.12-only nested-quote f-strings: " + "; ".join(offenders)
    live = (ROOT / "src" / "sleeve_merge_live.py").read_text(encoding="utf-8")
    assert "f'{h.get('pct')" not in live


def test_catalyst_close_asks_for_json_and_backoff_fits_deadline() -> None:
    """09-10 CENX/KSS: Step 1's forced close asked for a 'post-session essay'
    and then substituted a markdown review — neither parses as the events
    list, and 20/40/60s retry sleeps ran the ticker slice out."""
    assert "JSON" in dc._close_instruction("CATALYST STEP1 CENX")
    assert "essay" not in dc._close_instruction("CATALYST STEP1 CENX")
    assert "essay" in dc._close_instruction("SECTOR OUTCOME Energy 2026-09-10")
    assert dc._is_structured_stage("CATALYST VERDICT X")
    assert not dc._is_structured_stage("GENERAL OUTCOME 2026-09-10")

    _reset(openclaw_url="", deepseek_key="ds-key", grok_only=False)
    from src import step_deadline

    # Forced close on a catalyst stage: the close user turn asks for the
    # JSON, and when every close comes back empty chat() returns "" instead
    # of a '## Post-session review' the caller cannot parse.
    seen_payloads: list[dict] = []

    def fake_post(payload, retries=4):
        seen_payloads.append(copy.deepcopy(payload))
        return {"choices": [{"message": {"content": ""}}]}

    with mock.patch.object(dc, "_post", side_effect=fake_post), \
            mock.patch.object(dc.time, "sleep"):
        out = dc.chat([{"role": "system", "content": "Return JSON events."},
                       {"role": "user", "content": "Extract events for CENX."}],
                      model="deepseek-chat", tools=True, max_rounds=1,
                      stage_label="CATALYST STEP1 CENX")
    assert out == ""
    closes = [p for p in seen_payloads if "tools" not in p]
    assert closes, "no forced close was attempted"
    assert all("JSON" in p["messages"][-1]["content"] for p in closes)
    assert all("post-session essay" not in p["messages"][-1]["content"] for p in closes)

    # Retry backoff never sleeps past the step deadline: 70s left → 20s
    # sleep fits, 40s fits, 60s does not → give up after 3 dials.
    sleeps: list[float] = []
    posts = {"n": 0}

    def fake_requests_post(url, headers=None, json=None, timeout=None):
        posts["n"] += 1
        return _fake_response(429)

    with mock.patch.dict(os.environ, {step_deadline.ENV: f"{time.time() + 70:.0f}"}), \
            mock.patch.object(dc.requests, "post", side_effect=fake_requests_post), \
            mock.patch.object(dc.time, "sleep", side_effect=sleeps.append):
        try:
            dc._post({"model": "deepseek-chat", "messages": []})
            raise AssertionError("429 forever must raise")
        except RuntimeError as e:
            assert "HTTP 429" in str(e)
    assert posts["n"] == 3
    assert sleeps == [20, 40]


def test_catalyst_runtime_splits_ticker_slice_by_phase() -> None:
    """Research (verdict+Step1+Step2) gets PHASE_A_FRAC of the slice; Step 4
    + catcher keep the rest. 09-10: Step 4 started with a 30s tool budget."""
    from collectors import catalyst_grok_runtime as rt
    from src import step_deadline

    assert 0.5 <= rt.PHASE_A_FRAC <= 0.7
    src_txt = (ROOT / "collectors" / "catalyst_grok_runtime.py").read_text(encoding="utf-8")
    assert "with step_deadline.narrowed(phase_a_s):" in src_txt
    assert "verdict_task," in src_txt.split("await asyncio.gather(")[1]
    assert "ca.salvage_step4(final_raw)" in src_txt
    legacy = (ROOT / "collectors" / "catalyst_analysis.py").read_text(encoding="utf-8")
    assert "final_result = salvage_step4(final_raw)" in legacy
    # A 360s slice leaves the synthesis phase >= 130s after research.
    from src import catalyst_daily as cd
    assert cd.MIN_TICKER_S * (1 - rt.PHASE_A_FRAC) >= 130
    assert cd.MIN_TICKER_S * rt.PHASE_A_FRAC >= dc.CLOSE_RESERVE_S + 60
    with mock.patch.dict(os.environ, {step_deadline.ENV: f"{time.time() + 360:.0f}"}):
        rem = step_deadline.remaining_s()
        with step_deadline.narrowed(rem * rt.PHASE_A_FRAC):
            inner = step_deadline.remaining_s()
            assert 200 <= inner <= 225
        assert step_deadline.remaining_s() > 350


def main() -> None:
    tests = [
        test_sources_parse_on_python_310,
        test_catalyst_close_asks_for_json_and_backoff_fits_deadline,
        test_catalyst_runtime_splits_ticker_slice_by_phase,
        test_grok_only_default_is_off,
        test_http_500_does_not_trip_breaker,
        test_401_does_trip_breaker_then_deepseek,
        test_deepseek_402_returns_empty,
        test_db_optional_when_url_missing,
        test_cancel_in_progress_off_on_grok_jobs,
        test_safe_git_push_used_by_failing_commit_jobs,
        test_deploy_dashboard_follows_preopen_and_book,
        test_jobs_publish_dashboard_in_place,
        test_all_jobs_degrade_instead_of_failing,
        test_label_weather_yaml_inputs_not_under_permissions,
        test_price_checklist_skips_empty_store,
        test_db_optional_without_flag,
        test_health_does_not_pin_grok_only,
        test_ecs_jobs_skip_live_finviz,
        test_ticker_lookback_defaults_random,
        test_pipeline_health_audit_default,
        test_hit_and_news_grade_still_commit,
        test_diag_exits_zero,
        test_preopen_lock_matches_postclose,
        test_heal_targets_all_jobs,
        test_preopen_does_not_skip_python_after_cutoff,
        test_ranker_inputs_before_llm_packet,
        test_ecs_timers_stay_green_and_push,
        test_empty_futures_tape_not_ready,
        test_sector_outcome_skips_existing_and_times_out_yf,
        test_sector_parent_continues_after_one_timeout,
        test_sector_reflect_skips_existing,
        test_etf_actual_falls_back_to_history,
        test_general_outcome_skips_existing_and_reuses_transcript,
        test_general_reflect_writes_gate_file_and_reuses_transcript,
        test_postclose_pushes_after_each_llm_layer,
        test_ubuntu_postclose_skips_grok_and_keeps_runner_home,
        test_persist_dir_falls_back_when_gha_unwritable,
        test_safe_git_push_keeps_dated_ranker_on_conflict,
        test_safe_git_push_refuses_conflict_marked_files,
        test_ubuntu_preopen_not_blocked_by_queued_ecs,
        test_preopen_harden_halt_reverted,
        test_incremental_land_and_day_board,
        test_factor_mine_lands_closed_after_postclose,
        test_last_closed_sidecar_does_not_share_ubuntu_concurrency,
        test_search_and_sector_rounds_are_bounded,
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
