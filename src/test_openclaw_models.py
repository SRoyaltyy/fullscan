"""Cheapest SuperGrok model above 30B + OpenClaw news hop (no network)."""
from __future__ import annotations

from unittest import mock

from src.openclaw_models import (
    DEFAULT_NEWS_MODEL,
    MIN_PARAMS_B,
    SECTOR_MODEL,
    above_30b,
    is_refused,
    pick_cheapest_above_30b,
    try_order,
)
from src.news_impact.openclaw_hop import hop as oc_hop
from src.news_impact.pipeline import analyze_article
import src.config as config
import src.deepseek_client as dc


def test_default_is_cheapest_general_above_30b() -> None:
    assert MIN_PARAMS_B == 30
    assert DEFAULT_NEWS_MODEL == "xai/grok-4.20-0309-non-reasoning"
    assert above_30b(DEFAULT_NEWS_MODEL)
    assert not is_refused(DEFAULT_NEWS_MODEL)
    assert pick_cheapest_above_30b() == DEFAULT_NEWS_MODEL
    assert SECTOR_MODEL == "xai/grok-4.6"
    assert DEFAULT_NEWS_MODEL != SECTOR_MODEL


def test_refuses_mini_and_coding() -> None:
    assert is_refused("xai/grok-3-mini")
    assert is_refused("grok-build-0.1")
    assert not above_30b("xai/grok-3-mini")


def test_live_list_prefers_legacy_fast_when_present() -> None:
    assert pick_cheapest_above_30b([
        "xai/grok-4.6", "xai/grok-4.1-fast", "xai/grok-4.7",
    ]) == "xai/grok-4.1-fast"
    assert pick_cheapest_above_30b([
        "grok-4.6", "grok-4.3",
    ]) == "xai/grok-4.3"


def test_try_order_includes_sector_fallback() -> None:
    order = try_order(None)
    assert order[0] == DEFAULT_NEWS_MODEL
    assert SECTOR_MODEL in order


def test_openclaw_complete_never_deepseek() -> None:
    config.OPENCLAW_GATEWAY_URL = "http://gw:18789"
    config.OPENCLAW_TOKEN = "tok"
    config._OPENCLAW_TOKEN_ALIGNED = True
    dc._OPENCLAW_STATE["down"] = False
    calls = []

    def fake_post(url, headers=None, json=None, timeout=None):
        calls.append({"url": url, "headers": headers, "body": json})
        r = mock.Mock()
        r.status_code = 200
        r.json.return_value = {"choices": [{"message": {"content": "PONG"}}]}
        r.text = "{}"
        r.raise_for_status = mock.Mock()
        return r

    with mock.patch.object(dc.requests, "post", side_effect=fake_post):
        text = dc.openclaw_complete(
            [{"role": "user", "content": "Reply with exactly the word PONG"}],
            max_tokens=16,
            backend_model=DEFAULT_NEWS_MODEL,
            stage_label="ping",
        )
    assert text == "PONG"
    assert calls[0]["url"] == "http://gw:18789/v1/chat/completions"
    assert calls[0]["headers"]["x-openclaw-model"] == DEFAULT_NEWS_MODEL
    assert calls[0]["body"]["model"] == config.OPENCLAW_AGENT


def test_news_hop_watermarks_openclaw() -> None:
    config.OPENCLAW_GATEWAY_URL = "http://gw:18789"
    config.OPENCLAW_TOKEN = "tok"
    config.OPENCLAW_NEWS_MODEL = DEFAULT_NEWS_MODEL
    config._OPENCLAW_TOKEN_ALIGNED = True

    def fake_complete(messages, max_tokens=64, temperature=0.0,
                      stage_label="", backend_model=None):
        assert backend_model == DEFAULT_NEWS_MODEL
        return '{"event_class":"factor_impulse","sign":"up","q5":"impulse","constraint":"hormuz","split":false,"split_facts":[],"why":"tanker"}'

    with mock.patch.object(dc, "openclaw_complete", side_effect=fake_complete):
        parsed, lane, model, log = oc_hop(
            "news_classify",
            {"title": "Brent jumps after Hormuz tanker attack"},
        )
    assert lane == "openclaw"
    assert model == DEFAULT_NEWS_MODEL
    assert parsed and parsed["event_class"] == "factor_impulse"
    assert log[0]["ok"] is True

    with mock.patch.object(dc, "openclaw_complete", side_effect=fake_complete):
        row = analyze_article(
            {"title": "Brent jumps after Hormuz tanker attack"},
            persist=False, use_openclaw=True,
        )
    assert row["lane"] == "openclaw"
    assert row["model"] == DEFAULT_NEWS_MODEL
    assert any(h.get("lane") == "openclaw" for h in row["hop_chain"])


def test_ping_helpers() -> None:
    import importlib.util
    from pathlib import Path
    path = Path(__file__).resolve().parent.parent / "scripts" / "openclaw_grok_ping.py"
    spec = importlib.util.spec_from_file_location("openclaw_grok_ping", path)
    mod = importlib.util.module_from_spec(spec)
    assert spec.loader
    spec.loader.exec_module(mod)
    assert mod.looks_like_pong("PONG")
    assert mod.looks_like_grok_reply('{"event_class":"factor_impulse"}')
    assert not mod.looks_like_grok_reply("LLM request timed out")
    assert not mod.looks_like_grok_reply("")


def test_openclaw_hop_fail_soft_without_gateway() -> None:
    config.OPENCLAW_GATEWAY_URL = ""
    config.OPENCLAW_TOKEN = ""
    config._OPENCLAW_TOKEN_ALIGNED = True
    parsed, lane, model, log = oc_hop("news_classify", {"title": "x"})
    assert parsed is None
    assert log[0]["skip"] == "no_gateway"
    row = analyze_article({"title": "Amgen gets FDA approval"}, persist=False)
    assert row["lane"] == "deterministic"


def main() -> None:
    tests = [
        test_default_is_cheapest_general_above_30b,
        test_refuses_mini_and_coding,
        test_live_list_prefers_legacy_fast_when_present,
        test_try_order_includes_sector_fallback,
        test_openclaw_complete_never_deepseek,
        test_news_hop_watermarks_openclaw,
        test_ping_helpers,
        test_openclaw_hop_fail_soft_without_gateway,
    ]
    failed = 0
    for fn in tests:
        try:
            fn()
            print(f"ok  {fn.__name__}")
        except Exception as exc:  # noqa: BLE001
            failed += 1
            print(f"FAIL {fn.__name__}: {exc}")
    if failed:
        raise SystemExit(f"{failed} test(s) failed")
    print(f"{len(tests)} tests passed")


if __name__ == "__main__":
    main()
