"""QC contract tests against real 2026-08-24 artifacts + timeout stubs.

Run: python -m src.test_output_qc
"""
from __future__ import annotations

from pathlib import Path

from src import deepseek_client as dc
from src import output_qc, scoreboard
from src.test_llm_routing import _fake_response, _reset, _SAVED
from unittest import mock


ROOT = Path(__file__).resolve().parent.parent
D24 = "2026-08-24"
SEC = ROOT / "01_daily" / "sectors" / D24
GEN = ROOT / "01_daily" / "general" / f"{D24}_predict.md"
EV = ROOT / "01_daily" / "events" / f"{D24}_events.json"
JUDGE = ROOT / "01_daily" / "news" / f"{D24}_judge.md"


def test_timeout_stub_rejected() -> None:
    r = output_qc.qc_sector_predict(SEC / "financial_predict.md")
    assert not r.ok, r
    assert r.timeout or "timeout" in r.reason, r
    r2 = output_qc.qc_sector_predict(SEC / "healthcare_predict.md")
    assert not r2.ok and (r2.timeout or "timeout" in r2.reason), r2


def test_gold_sector_accepted() -> None:
    r = output_qc.qc_sector_predict(SEC / "technology_predict.md")
    assert r.ok, r.explain()
    assert r.size > 2500


def test_carried_events_rejected() -> None:
    r = output_qc.qc_events_date(D24)
    assert not r.ok, r
    assert r.carried, r


def test_general_predict_accepted() -> None:
    r = output_qc.qc_general_predict(GEN)
    assert r.ok, r.explain()


def test_judge_accepted() -> None:
    r = output_qc.qc_news_judge(JUDGE)
    assert r.ok, r.explain()


def test_http_timeouts_trip_breaker_after_three() -> None:
    _reset(
        openclaw_url="http://gw:18789",
        deepseek_key="ds-key",
        grok_only=False,
    )
    import requests as req_mod

    def fake_post(url, headers=None, json=None, timeout=None):
        if "gw:18789" in url:
            raise req_mod.Timeout("hung")
        return _fake_response(200, "DEEPSEEK ANSWER")

    for n in range(1, 3):
        with mock.patch.object(dc.requests, "post", side_effect=fake_post):
            text = dc.chat([{"role": "user", "content": "hi"}],
                           model="deepseek-chat", tools=False)
        assert text == "DEEPSEEK ANSWER"
        assert not dc._OPENCLAW_STATE["down"]
        assert dc._OPENCLAW_STATE["timeouts"] == n

    with mock.patch.object(dc.requests, "post", side_effect=fake_post):
        text = dc.chat([{"role": "user", "content": "hi"}],
                       model="deepseek-chat", tools=False)
    assert text == "DEEPSEEK ANSWER"
    assert dc._OPENCLAW_STATE["down"]
    assert "HTTP timeouts" in dc._OPENCLAW_STATE["reason"]


def test_timeout_content_falls_back_to_deepseek() -> None:
    _reset(
        openclaw_url="http://gw:18789",
        deepseek_key="ds-key",
        grok_only=False,
    )
    stub = (
        "LLM request timed out.\n\n"
        "The model did not produce a response before the model idle timeout."
    )

    def fake_post(url, headers=None, json=None, timeout=None):
        if "gw:18789" in url:
            return _fake_response(200, stub)
        return _fake_response(200, "DEEPSEEK ANSWER")

    with mock.patch.object(dc.requests, "post", side_effect=fake_post):
        text = dc.chat([{"role": "user", "content": "hi"}],
                       model="deepseek-chat", tools=False)
    assert text == "DEEPSEEK ANSWER"
    # one timeout does not trip the circuit breaker
    assert not dc._OPENCLAW_STATE["down"]
    assert dc._OPENCLAW_STATE["timeouts"] == 1

    # idle-timeout stubs trip the breaker only after 5 in a row
    for n in range(2, 5):
        with mock.patch.object(dc.requests, "post", side_effect=fake_post):
            text = dc.chat([{"role": "user", "content": "hi"}],
                           model="deepseek-chat", tools=False)
        assert text == "DEEPSEEK ANSWER"
        assert not dc._OPENCLAW_STATE["down"]
        assert dc._OPENCLAW_STATE["timeouts"] == n

    with mock.patch.object(dc.requests, "post", side_effect=fake_post):
        text = dc.chat([{"role": "user", "content": "hi"}],
                       model="deepseek-chat", tools=False)
    assert text == "DEEPSEEK ANSWER"
    assert dc._OPENCLAW_STATE["down"]


def test_credit_exhaustion_content_falls_back_to_deepseek() -> None:
    _reset(
        openclaw_url="http://gw:18789",
        deepseek_key="ds-key",
        grok_only=False,
    )

    def fake_post(url, headers=None, json=None, timeout=None):
        if "gw:18789" in url:
            return _fake_response(
                200,
                "Usage limit reached: insufficient credits for this request.",
            )
        return _fake_response(200, "DEEPSEEK ANSWER")

    with mock.patch.object(dc.requests, "post", side_effect=fake_post):
        text = dc.chat(
            [{"role": "user", "content": "hi"}],
            model="deepseek-chat",
            tools=False,
        )
    assert text == "DEEPSEEK ANSWER"
    assert dc.last_provider() == "deepseek"


def test_real_essay_not_timeout() -> None:
    text = (SEC / "technology_predict.md").read_text(encoding="utf-8")
    assert not output_qc.looks_like_timeout(text)


def test_scoreboard_merge_unions_topics() -> None:
    primary = {"runs": [
        {"date": D24, "topic": "general", "predicted_direction": "down",
         "actual_pct_change": -0.5},
        {"date": D24, "topic": "sector:Energy", "predicted_direction": "up"},
    ]}
    extra = {"runs": [
        {"date": D24, "topic": "general", "predicted_direction": "flat"},
        {"date": D24, "topic": "sector:Technology", "predicted_direction": "down"},
    ]}
    merged = scoreboard.merge_boards(primary, extra)
    topics = {(r["date"], r["topic"]): r for r in merged["runs"]}
    assert ("2026-08-24", "sector:Technology") in topics
    assert ("2026-08-24", "sector:Energy") in topics
    # extra overlays non-empty predicted_direction, but does not erase actuals
    gen = topics[("2026-08-24", "general")]
    assert gen["predicted_direction"] == "flat"
    assert gen["actual_pct_change"] == -0.5


def test_preopen_report_flags_08_24() -> None:
    report = output_qc.preopen_report(D24)
    assert report["sector_n_ok"] >= 8, report["sector_n_ok"]
    assert report["sector_n_ok"] < 11  # financial + healthcare stubs
    kinds = {i["kind"]: i for i in report["items"]}
    assert not kinds["events"]["ok"]
    assert kinds["general_predict"]["ok"]
    assert kinds["news_judge"]["ok"]


def main() -> None:
    tests = [
        test_timeout_stub_rejected,
        test_gold_sector_accepted,
        test_carried_events_rejected,
        test_general_predict_accepted,
        test_judge_accepted,
        test_http_timeouts_trip_breaker_after_three,
        test_timeout_content_falls_back_to_deepseek,
        test_credit_exhaustion_content_falls_back_to_deepseek,
        test_real_essay_not_timeout,
        test_scoreboard_merge_unions_topics,
        test_preopen_report_flags_08_24,
    ]
    failed = 0
    for fn in tests:
        try:
            fn()
            print(f"ok  {fn.__name__}")
        except Exception as e:  # noqa: BLE001
            failed += 1
            print(f"FAIL {fn.__name__}: {e}")
    dc._OPENCLAW_STATE["down"] = False
    dc._OPENCLAW_STATE["timeouts"] = 0
    import src.config as config
    config.OPENCLAW_GATEWAY_URL, config.DEEPSEEK_API_KEY = _SAVED[0], _SAVED[1]
    if failed:
        raise SystemExit(f"{failed} test(s) failed")
    print(f"{len(tests)} tests passed")


if __name__ == "__main__":
    main()
