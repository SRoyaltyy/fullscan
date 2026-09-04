"""Unit tests for OpenClaw-primary / DeepSeek-fallback routing.

No network: requests.post is monkeypatched. Run:
    python -m src.test_llm_routing
"""
from __future__ import annotations

import json
import os
from unittest import mock

import src.config as config
import src.deepseek_client as dc


def _fake_response(status: int, content: str = "", tool_calls=None):
    r = mock.Mock()
    r.status_code = status
    msg = {"content": content}
    if tool_calls:
        msg["tool_calls"] = tool_calls
    r.json.return_value = {"choices": [{"message": msg}]}
    r.text = json.dumps(r.json.return_value)
    r.raise_for_status = mock.Mock()
    return r


def _reset(openclaw_url: str = "", deepseek_key: str = "",
           grok_only: bool | None = None):
    config.OPENCLAW_GATEWAY_URL = openclaw_url
    config.DEEPSEEK_API_KEY = deepseek_key
    config._OPENCLAW_TOKEN_ALIGNED = True  # tests supply the token themselves
    dc._OPENCLAW_STATE["down"] = False
    dc._OPENCLAW_STATE["reason"] = ""
    dc._OPENCLAW_STATE["timeouts"] = 0
    if grok_only is None:
        os.environ.pop("GROK_ONLY", None)
    else:
        os.environ["GROK_ONLY"] = "1" if grok_only else "0"


_SAVED = (config.OPENCLAW_GATEWAY_URL, config.DEEPSEEK_API_KEY,
          config.OPENCLAW_TOKEN, os.environ.get("GROK_ONLY"))


def test_gates() -> None:
    _reset()
    assert not config.has_llm()
    try:
        config.require_llm()
    except SystemExit as e:
        assert "OPENCLAW_GATEWAY_URL" in str(e)
    else:
        raise AssertionError("require_llm should exit with nothing set")
    _reset(openclaw_url="http://gw:18789")
    assert config.has_llm() and config.openclaw_enabled()
    config.require_llm()
    _reset(deepseek_key="ds-key")
    assert config.has_llm() and not config.openclaw_enabled()
    config.require_llm()


def test_openclaw_primary_wins() -> None:
    _reset(openclaw_url="http://gw:18789", deepseek_key="ds-key")
    calls = []

    def fake_post(url, headers=None, json=None, timeout=None):
        calls.append({"url": url, "headers": headers, "body": json,
                      "timeout": timeout})
        return _fake_response(200, "GROK ANSWER")

    with mock.patch.object(dc.requests, "post", side_effect=fake_post):
        text = dc.chat([{"role": "system", "content": "sys"},
                        {"role": "user", "content": "hi"}],
                       model="deepseek-chat", tools=False, max_tokens=100)
    assert text == "GROK ANSWER"
    assert dc.last_provider() == "openclaw"
    assert len(calls) == 1
    c = calls[0]
    assert c["url"] == "http://gw:18789/v1/chat/completions"
    assert c["body"]["model"] == config.OPENCLAW_AGENT
    assert c["headers"]["x-openclaw-model"] == config.OPENCLAW_BACKEND_MODEL
    assert c["headers"]["x-openclaw-session-key"].startswith("fullscan-")
    # connect timeout must be capped (tuple), not the full read timeout
    assert isinstance(c["timeout"], tuple) and c["timeout"][0] <= 30


def test_native_search_note_only_when_tools() -> None:
    _reset(openclaw_url="http://gw:18789")
    seen = {}

    def fake_post(url, headers=None, json=None, timeout=None):
        seen["body"] = json
        return _fake_response(200, "ok")

    with mock.patch.object(dc.requests, "post", side_effect=fake_post):
        dc.chat([{"role": "system", "content": "SYS"},
                 {"role": "user", "content": "u"}],
                model="deepseek-chat", tools=True)
    assert "RESEARCH MODE" in seen["body"]["messages"][0]["content"]
    assert seen["body"]["messages"][0]["content"].startswith("SYS")
    # no client-side function tools on the OpenClaw path
    assert "tools" not in seen["body"]

    with mock.patch.object(dc.requests, "post", side_effect=fake_post):
        dc.chat([{"role": "system", "content": "SYS"},
                 {"role": "user", "content": "u"}],
                model="deepseek-chat", tools=False)
    assert "RESEARCH MODE" not in seen["body"]["messages"][0]["content"]


def test_fallback_on_gateway_failure() -> None:
    _reset(openclaw_url="http://gw:18789", deepseek_key="ds-key",
           grok_only=False)
    urls = []

    def fake_post(url, headers=None, json=None, timeout=None):
        urls.append(url)
        if "gw:18789" in url:
            return _fake_response(503)
        return _fake_response(200, "DEEPSEEK ANSWER")

    with mock.patch.object(dc.requests, "post", side_effect=fake_post), \
            mock.patch.object(dc.time, "sleep"):
        text = dc.chat([{"role": "user", "content": "hi"}],
                       model="deepseek-chat", tools=False)
    assert text == "DEEPSEEK ANSWER"
    assert dc.last_provider() == "deepseek"
    assert any("deepseek" in u for u in urls)
    # circuit breaker: gateway now marked down for the rest of the process
    assert dc._OPENCLAW_STATE["down"]

    urls.clear()
    with mock.patch.object(dc.requests, "post", side_effect=fake_post):
        text = dc.chat([{"role": "user", "content": "hi"}],
                       model="deepseek-chat", tools=False)
    assert text == "DEEPSEEK ANSWER"
    assert not any("gw:18789" in u for u in urls), "down gateway re-tried"


def test_fallback_on_empty_answer() -> None:
    _reset(openclaw_url="http://gw:18789", deepseek_key="ds-key",
           grok_only=False)

    def fake_post(url, headers=None, json=None, timeout=None):
        if "gw:18789" in url:
            return _fake_response(200, "")          # empty from Grok
        return _fake_response(200, "DEEPSEEK ANSWER")

    with mock.patch.object(dc.requests, "post", side_effect=fake_post):
        text = dc.chat([{"role": "user", "content": "hi"}],
                       model="deepseek-chat", tools=False)
    assert text == "DEEPSEEK ANSWER"
    # empty is not an outage: the gateway must NOT be marked down
    assert not dc._OPENCLAW_STATE["down"]


def test_no_fallback_returns_empty() -> None:
    _reset(openclaw_url="http://gw:18789", deepseek_key="")

    def fake_post(url, headers=None, json=None, timeout=None):
        return _fake_response(503)

    with mock.patch.object(dc.requests, "post", side_effect=fake_post), \
            mock.patch.object(dc.time, "sleep"):
        text = dc.chat([{"role": "user", "content": "hi"}],
                       model="deepseek-chat", tools=False)
    assert text == ""


def test_deepseek_only_unchanged() -> None:
    _reset(deepseek_key="ds-key")
    urls = []

    def fake_post(url, headers=None, json=None, timeout=None):
        urls.append(url)
        return _fake_response(200, "DS")

    with mock.patch.object(dc.requests, "post", side_effect=fake_post):
        text = dc.chat([{"role": "user", "content": "hi"}],
                       model="deepseek-chat", tools=False)
    assert text == "DS"
    assert dc.last_provider() == "deepseek"
    assert urls == [f"{config.DEEPSEEK_BASE_URL}/chat/completions"]


def test_deepseek_chat_caps_grok_sized_output_budget() -> None:
    _reset(deepseek_key="ds-key")
    seen = {}

    def fake_post(url, headers=None, json=None, timeout=None):
        seen["body"] = json
        return _fake_response(200, "DS")

    with mock.patch.object(dc.requests, "post", side_effect=fake_post):
        text = dc.chat(
            [{"role": "user", "content": "hi"}],
            model="deepseek-chat",
            tools=False,
            max_tokens=40000,
        )
    assert text == "DS"
    assert seen["body"]["max_tokens"] == config.DEEPSEEK_CHAT_MAX_TOKENS


def test_describe_routing_no_secrets() -> None:
    _reset(openclaw_url="http://gw:18789", deepseek_key="super-secret-key")
    config.OPENCLAW_TOKEN = "another-secret"
    try:
        text = dc.describe_routing()
        assert "super-secret-key" not in text
        assert "another-secret" not in text
        assert "http://gw:18789" in text
    finally:
        config.OPENCLAW_TOKEN = _SAVED[2]


def test_timeout_content_is_empty() -> None:
    """Idle-timeout stub must NOT be returned as a successful answer."""
    _reset(openclaw_url="http://gw:18789", deepseek_key="ds-key",
           grok_only=False)
    stub = "LLM request timed out.\nThe model did not produce a response before the model idle timeout."

    def fake_post(url, headers=None, json=None, timeout=None):
        if "gw:18789" in url:
            return _fake_response(200, stub)
        return _fake_response(200, "DEEPSEEK ANSWER")

    with mock.patch.object(dc.requests, "post", side_effect=fake_post):
        text = dc.chat([{"role": "user", "content": "hi"}],
                       model="deepseek-chat", tools=False)
    assert text == "DEEPSEEK ANSWER"
    assert not dc._OPENCLAW_STATE["down"]


def test_grok_only_blocks_deepseek_and_force_flag() -> None:
    _reset(openclaw_url="http://gw:18789", deepseek_key="ds-key",
           grok_only=True)
    urls = []

    def fake_post(url, headers=None, json=None, timeout=None):
        urls.append(url)
        return _fake_response(200, "")

    with mock.patch.object(dc.requests, "post", side_effect=fake_post):
        text = dc.chat(
            [{"role": "user", "content": "hi"}],
            model="deepseek-chat", tools=False, force_deepseek=True)
    assert text == ""
    assert urls == ["http://gw:18789/v1/chat/completions"]


def test_grok_only_no_fallback_when_gateway_401() -> None:
    """GROK_ONLY must not DeepSeek even after OpenClaw 401 / circuit-breaker."""
    _reset(openclaw_url="http://gw:18789", deepseek_key="ds-key",
           grok_only=True)
    urls = []

    def fake_post(url, headers=None, json=None, timeout=None):
        urls.append(url)
        if "gw:18789" in url:
            r = _fake_response(401)
            err = dc.requests.HTTPError(
                "401 Client Error: Unauthorized for url: "
                "http://gw:18789/v1/chat/completions")
            err.response = r
            r.raise_for_status.side_effect = err
            return r
        return _fake_response(200, "DEEPSEEK MUST NOT RUN")

    with mock.patch.object(dc.requests, "post", side_effect=fake_post), \
            mock.patch.object(dc.time, "sleep"):
        text = dc.chat([{"role": "user", "content": "hi"}],
                       model="deepseek-chat", tools=False)
    assert text == ""
    assert dc._OPENCLAW_STATE["down"]
    assert all("deepseek" not in u for u in urls)
    assert urls  # did try OpenClaw

    urls.clear()
    with mock.patch.object(dc.requests, "post", side_effect=fake_post):
        text = dc.chat([{"role": "user", "content": "hi"}],
                       model="deepseek-chat", tools=False)
    assert text == ""
    assert urls == []  # breaker: do not re-hit gateway OR DeepSeek


def test_grok_only_no_fallback_when_already_down() -> None:
    _reset(openclaw_url="http://gw:18789", deepseek_key="ds-key",
           grok_only=True)
    dc._OPENCLAW_STATE["down"] = True
    dc._OPENCLAW_STATE["reason"] = "401 Unauthorized"
    urls = []

    def fake_post(url, headers=None, json=None, timeout=None):
        urls.append(url)
        return _fake_response(200, "DEEPSEEK MUST NOT RUN")

    with mock.patch.object(dc.requests, "post", side_effect=fake_post):
        text = dc.chat([{"role": "user", "content": "hi"}],
                       model="deepseek-chat", tools=False)
    assert text == ""
    assert urls == []


def test_pick_openclaw_token_prefers_live_48() -> None:
    secret64 = "s" * 64
    live48 = "l" * 48
    other32 = "x" * 32
    pick = config.pick_openclaw_token
    assert pick(live48, secret64) == live48
    assert pick(secret64, live48) == live48
    assert pick("", live48) == live48
    assert pick(live48, "") == live48
    assert pick(secret64, other32) == other32
    assert pick(other32, secret64) == other32
    assert pick(secret64, secret64) == secret64
    assert pick("", "") == ""
    assert pick(secret64, "") == secret64


def main() -> None:
    tests = [
        test_gates,
        test_openclaw_primary_wins,
        test_native_search_note_only_when_tools,
        test_fallback_on_gateway_failure,
        test_fallback_on_empty_answer,
        test_no_fallback_returns_empty,
        test_deepseek_only_unchanged,
        test_deepseek_chat_caps_grok_sized_output_budget,
        test_describe_routing_no_secrets,
        test_timeout_content_is_empty,
        test_grok_only_blocks_deepseek_and_force_flag,
        test_grok_only_no_fallback_when_gateway_401,
        test_grok_only_no_fallback_when_already_down,
        test_pick_openclaw_token_prefers_live_48,
    ]
    failed = 0
    for fn in tests:
        try:
            fn()
            print(f"ok  {fn.__name__}")
        except Exception as e:  # noqa: BLE001
            failed += 1
            print(f"FAIL {fn.__name__}: {e}")
    config.OPENCLAW_GATEWAY_URL, config.DEEPSEEK_API_KEY = _SAVED[0], _SAVED[1]
    if _SAVED[3] is None:
        os.environ.pop("GROK_ONLY", None)
    else:
        os.environ["GROK_ONLY"] = _SAVED[3]
    if failed:
        raise SystemExit(f"{failed} test(s) failed")
    print(f"{len(tests)} tests passed")


if __name__ == "__main__":
    main()
