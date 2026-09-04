"""Contracts for the 2026-09-04 job-hardening pass. No network.

Run: PYTHONPATH=. python3 -m src.test_job_hardening
"""
from __future__ import annotations

import os
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
        "preopen_all.yml",
        "postclose_all.yml",
        "map_heat_postclose.yml",
        "daily_pipeline.yml",
        "learn_cycle.yml",
        "stock_book_all.yml",
        "xai_reauth.yml",
    ):
        text = (WF / name).read_text(encoding="utf-8")
        assert "cancel-in-progress: true" not in text, name
        assert "cancel-in-progress: false" in text, name


def test_safe_git_push_used_by_failing_commit_jobs() -> None:
    for name in (
        "preopen_all.yml",
        "postclose_all.yml",
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
    assert "locks/preopen.lock" in pre
    assert "locks/preopen.lock" in post
    assert "/tmp/fullscan-preopen.lock" not in pre
    assert "fullscan-persist/locks/preopen.lock" in yml


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
    assert "safe_git_push.sh" in pre
    assert "timeout_s=45 if late" in pre
    assert "No retry" in pre
    assert "_exists_gt" in pre
    assert "skip_extras" in book
    extras_gate = book.find("skip extras before book")
    news_parse = book.find("[all] → News parse")
    assert 0 <= extras_gate < news_parse
    assert "TimeoutExpired" in book
    assert "ab_t = 1500 if skip_extras" in book
    pre_yml = (WF / "preopen_all.yml").read_text(encoding="utf-8")
    assert "Land book + green (ubuntu — no Grok, no ECS)" in pre_yml
    assert "--skip-llm --skip-extras" in pre_yml
    assert "Pull scrape + export from main" in pre_yml
    assert "git checkout origin/main --" in pre_yml
    assert 'force=true — rank even if a book is on disk' in pre_yml
    book_yml = (WF / "stock_book_all.yml").read_text(encoding="utf-8")
    assert "skip_extras:" in book_yml
    assert "past 09:25 ET — skip LLM + extras" in book_yml
    health = (ROOT / "src" / "pipeline_health.py").read_text(encoding="utf-8")
    hold = health.split('wf == "stock_book_all.yml" and (')[1].split("):")[0]
    assert "preopen_all.yml" not in hold
    assert '_already_running("preopen_all.yml")' not in hold
    post = (ROOT / "scripts" / "ecs_map_postclose.sh").read_text(encoding="utf-8")
    assert "last_closed_session" in post
    assert "preopen lock held — skip (exit 0" in post
    assert "preopen service active — skip (exit 0" in post
    assert "exit 1" not in post
    learn = (ROOT / "src" / "run_postclose_all.py").read_text(encoding="utf-8")
    assert 'src.learn_cycle", "--date"' in learn or "--date\", date" in learn
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


def main() -> None:
    tests = [
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
