"""Events JSON extract + OpenClaw-unusable → DeepSeek-this-stage.

No network. Run: python -m src.test_events_json
"""
from __future__ import annotations

from unittest import mock

import src.config as config
import src.deepseek_client as dc
from src.run_events import extract_json
from src.test_llm_routing import _fake_response, _reset, _SAVED


def test_extract_fenced() -> None:
    text = 'prose\n```json\n{"scan_date":"2026-08-25","events":[{"title":"A"}]}\n```\n'
    data = extract_json(text)
    assert data and data["events"][0]["title"] == "A"


def test_extract_trailing_comma() -> None:
    blob = '{"scan_date":"x","events":[{"title":"B",}],}'
    data = extract_json("```json\n" + blob + "\n```")
    assert data and data["events"][0]["title"] == "B"


def test_extract_unclosed_fence() -> None:
    text = '## TODAY\n```json\n{"scan_date":"x","events":[{"title":"C"}]}'
    data = extract_json(text)
    assert data and data["events"][0]["title"] == "C"


def test_extract_prose_without_json_is_none() -> None:
    text = "Eighteen minutes of Grok prose about Hormuz and the Fed. No block."
    assert extract_json(text) is None


def test_force_deepseek_skips_openclaw() -> None:
    _reset(openclaw_url="http://gw:18789", deepseek_key="ds-key")
    urls = []

    def fake_post(url, headers=None, json=None, timeout=None):
        urls.append(url)
        if "gw:18789" in url:
            return _fake_response(200, "GROK ESSAY WITHOUT JSON")
        return _fake_response(200, '{"events":[{"title":"DS"}]}')

    with mock.patch.object(dc.requests, "post", side_effect=fake_post):
        text = dc.chat([{"role": "user", "content": "hi"}],
                       model="deepseek-chat", tools=False,
                       force_deepseek=True)
    assert "DS" in text
    assert not any("gw:18789" in u for u in urls)
    assert not dc._OPENCLAW_STATE["down"]


def test_unusable_openclaw_does_not_mark_down() -> None:
    """Long unparseable Grok text is a success to chat(); caller decides."""
    _reset(openclaw_url="http://gw:18789", deepseek_key="ds-key")

    def fake_post(url, headers=None, json=None, timeout=None):
        if "gw:18789" in url:
            return _fake_response(200, "long grok essay " * 80)
        return _fake_response(200, "DEEPSEEK")

    with mock.patch.object(dc.requests, "post", side_effect=fake_post):
        text = dc.chat([{"role": "user", "content": "hi"}],
                       model="deepseek-chat", tools=False)
    assert text.startswith("long grok essay")
    assert not dc._OPENCLAW_STATE["down"]


def test_extract_json_first_then_prose() -> None:
    text = (
        '```json\n{"scan_date":"2026-08-25","events":[{"title":"FIRST"}]}\n```\n'
        + ("prose " * 200)
    )
    data = extract_json(text)
    assert data and data["events"][0]["title"] == "FIRST"


def main() -> None:
    tests = [
        test_extract_fenced,
        test_extract_trailing_comma,
        test_extract_unclosed_fence,
        test_extract_prose_without_json_is_none,
        test_extract_json_first_then_prose,
        test_force_deepseek_skips_openclaw,
        test_unusable_openclaw_does_not_mark_down,
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
    dc._OPENCLAW_STATE["down"] = False
    dc._OPENCLAW_STATE["timeouts"] = 0
    if failed:
        raise SystemExit(f"{failed} test(s) failed")
    print(f"{len(tests)} tests passed")


if __name__ == "__main__":
    main()
