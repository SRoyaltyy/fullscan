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


def test_catalyst_fail_degraded_ok(tmp_path=None) -> None:
    """0/8 FAIL (no OK sentinel, exit 4). 1/8 DEGRADED. 2/8 OK."""
    import json
    import tempfile
    from unittest import mock

    from src import catalyst_daily as cd
    from src import land_file
    from src.run_preopen_all import catalyst_attempt_fields

    cases = (
        (0, 4, "FAIL", "CATALYST_DAILY_FAIL", "catalyst_fail n_ok=0/8"),
        (1, 3, "DEGRADED", "CATALYST_DAILY_DEGRADED", "catalyst_degraded n_ok=1/8"),
        (2, 0, "OK", "CATALYST_DAILY_OK", ""),
    )
    for n_ok, code, status, sentinel, qc_reason in cases:
        targets = [{"ticker": f"T{i}", "role": "x", "why": "y"} for i in range(8)]
        rows = []
        for i, spec in enumerate(targets):
            if i < n_ok:
                rows.append({
                    "ticker": spec["ticker"], "role": "x", "why": "y",
                    "net_signal": "Bullish", "search_backend": "grok_native",
                })
            else:
                rows.append({
                    "ticker": spec["ticker"], "role": "x", "why": "y",
                    "error": "timeout after 193s",
                })
        with tempfile.TemporaryDirectory() as tmp:
            out = Path(tmp)
            with mock.patch.object(cd, "OUT_DIR", out), \
                    mock.patch.object(cd, "already_good", return_value=False), \
                    mock.patch.object(cd, "select_targets", return_value=targets), \
                    mock.patch.object(cd, "run_dossiers", return_value=rows), \
                    mock.patch.object(cd, "apply_to_actions", return_value={}):
                if code:
                    try:
                        cd.run(date="2026-09-25", max_n=8)
                    except SystemExit as exc:
                        assert exc.code == code, (n_ok, exc.code)
                    else:
                        raise AssertionError(f"{n_ok}/8 should exit {code}")
                else:
                    payload = cd.run(date="2026-09-25", max_n=8)
                    assert payload["status"] == "OK"
                md = (out / "2026-09-25_dossiers.md").read_text(encoding="utf-8")
                js = json.loads((out / "2026-09-25_dossiers.json").read_text(encoding="utf-8"))
            assert js["status"] == status
            assert sentinel in md
            if status != "OK":
                assert "CATALYST_DAILY_OK" not in md
            qc = output_qc.qc_catalyst(out / "2026-09-25_dossiers.json")
            if status == "OK":
                assert qc.ok, qc.explain()
            else:
                assert not qc.ok
                assert qc.reason == qc_reason, qc.reason
                landed = land_file._qc_one(out / "2026-09-25_dossiers.json", "2026-09-25")
                assert landed.ok is False
                assert landed.reason == qc_reason
            if n_ok == 0:
                with mock.patch.object(cd, "OUT_DIR", out):
                    view = catalyst_attempt_fields("2026-09-25")
                assert view["status"] == "FAIL"
                assert view["detail"] == "0/8 dossiers (grok timeout x8, deepseek empty)"


def test_sector_unknown_remaining_blocks_deepseek() -> None:
    """Unknown SuperGrok remaining + timeout: no DeepSeek, sector DEGRADED."""
    import os
    from src import run_sector_predict as rsp
    from src.sector_board import provider_summary

    _reset(openclaw_url="http://gw:18789", deepseek_key="ds-key", grok_only=False)
    os.environ["SECTOR_GROK_RETRY_S"] = "0"
    os.environ.pop("SUPERGROK_REMAINING_PCT", None)
    if hasattr(output_qc.config.supergrok_remaining, "_gateway"):
        delattr(output_qc.config.supergrok_remaining, "_gateway")
    calls = []

    def fake_post(url, headers=None, json=None, timeout=None):
        calls.append(url)
        if "gw:18789" in url:
            raise dc.requests.Timeout("hung")
        return _fake_response(200, "DEEPSEEK ESSAY " + ("x" * 3000))

    import src.config as config
    with mock.patch.object(config, "supergrok_remaining", return_value=(None, "unknown")), \
            mock.patch.object(dc, "gateway_health_ping", return_value=False), \
            mock.patch.object(dc.time, "sleep"), \
            mock.patch.object(dc.requests, "post", side_effect=fake_post):
        text = dc.chat(
            [{"role": "user", "content": "hi"}],
            model="deepseek-chat", tools=False,
            stage_label="SECTOR PREDICT Technology 2026-09-25",
        )
    assert text == ""
    assert dc.last_sector_degraded()
    assert not any("deepseek.com" in url for url in calls)
    assert rsp.sector_llm_status(text, degraded=True, qc_ok=False) == "DEGRADED"
    os.environ.pop("SECTOR_GROK_RETRY_S", None)


def test_sector_remaining_85_labels_deepseek_fallback(tmp_path=None) -> None:
    import os
    import tempfile
    from src.sector_board import provider_summary

    _reset(openclaw_url="http://gw:18789", deepseek_key="ds-key", grok_only=False)
    os.environ["SECTOR_GROK_RETRY_S"] = "0"
    import src.config as config

    def fake_post(url, headers=None, json=None, timeout=None):
        if "gw:18789" in url:
            raise dc.requests.Timeout("hung")
        return _fake_response(200, "DEEPSEEK ESSAY")

    with tempfile.TemporaryDirectory() as tmp:
        trace = Path(tmp) / "trace.md"
        with mock.patch.object(config, "supergrok_remaining", return_value=(85.0, "env")), \
                mock.patch.object(dc.time, "sleep"), \
                mock.patch.object(dc.requests, "post", side_effect=fake_post):
            text = dc.chat(
                [{"role": "user", "content": "hi"}],
                model="deepseek-chat", tools=False,
                stage_label="SECTOR PREDICT Energy 2026-09-25",
                trace_path=str(trace),
            )
        body = trace.read_text(encoding="utf-8")
    assert text == "DEEPSEEK ESSAY"
    assert dc.last_provider() == "deepseek"
    assert not dc.last_sector_degraded()
    assert "provider: deepseek" in body
    assert "fallback_reason: openclaw_timeout" in body
    rows = [{"provider": "deepseek", "fallback_reason": "openclaw_timeout"} for _ in range(11)]
    assert provider_summary(rows) == "11/11 via deepseek (fallback: gateway timeout)"
    os.environ.pop("SECTOR_GROK_RETRY_S", None)


def test_sector_breaker_resets_after_health_ping() -> None:
    import os
    import src.config as config

    _reset(openclaw_url="http://gw:18789", deepseek_key="ds-key", grok_only=False)
    dc._OPENCLAW_STATE["down"] = True
    dc._OPENCLAW_STATE["reason"] = "3 consecutive HTTP timeouts"
    dc._OPENCLAW_STATE["timeouts"] = 3
    os.environ["SECTOR_GROK_RETRY_S"] = "0"

    def fake_get(url, headers=None, timeout=None):
        resp = mock.Mock()
        resp.status_code = 200
        return resp

    def fake_post(url, headers=None, json=None, timeout=None):
        if "deepseek.com" in url:
            raise AssertionError(url)
        return _fake_response(200, "GROK AGAIN")

    with mock.patch.object(config, "supergrok_remaining", return_value=(None, "unknown")), \
            mock.patch.object(dc.requests, "get", side_effect=fake_get), \
            mock.patch.object(dc.requests, "post", side_effect=fake_post), \
            mock.patch.object(dc.time, "sleep"):
        text = dc.chat(
            [{"role": "user", "content": "hi"}],
            model="deepseek-chat", tools=False,
            stage_label="SECTOR PREDICT Utilities 2026-09-25",
        )
    assert text == "GROK AGAIN"
    assert dc.last_provider() == "openclaw"
    assert dc._OPENCLAW_STATE["down"] is False
    os.environ.pop("SECTOR_GROK_RETRY_S", None)


def test_preopen_report_flags_08_24() -> None:
    report = output_qc.preopen_report(D24)
    assert report["sector_n_ok"] >= 8, report["sector_n_ok"]
    assert report["sector_n_ok"] < 11  # financial + healthcare stubs
    kinds = {i["kind"]: i for i in report["items"]}
    assert not kinds["events"]["ok"]
    assert kinds["general_predict"]["ok"]
    assert kinds["news_judge"]["ok"]
    paths = [i["path"] for i in report["items"]]
    assert any(p.endswith("_finviz_digest.json") or p.endswith("_finviz_digest.md")
               for p in paths)
    assert any(p.endswith("_finviz_market_digest.json") for p in paths)
    assert any(p.endswith("_finviz_market_digest_close.json") for p in paths)


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
        test_catalyst_fail_degraded_ok,
        test_sector_unknown_remaining_blocks_deepseek,
        test_sector_remaining_85_labels_deepseek_fallback,
        test_sector_breaker_resets_after_health_ping,
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
