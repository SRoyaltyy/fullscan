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
    assert c["timeout"] == (15, config.OPENCLAW_TIMEOUT)
    assert c["headers"]["x-openclaw-model"] == config.OPENCLAW_BACKEND_MODEL
    assert c["headers"]["x-openclaw-session-key"].startswith("fullscan-")


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
    # HTTP 503 is transient — do not blank the rest of the morning
    assert not dc._OPENCLAW_STATE["down"]

    urls.clear()
    with mock.patch.object(dc.requests, "post", side_effect=fake_post), \
            mock.patch.object(dc.time, "sleep"):
        text = dc.chat([{"role": "user", "content": "hi"}],
                       model="deepseek-chat", tools=False)
    assert text == "DEEPSEEK ANSWER"
    assert any("gw:18789" in u for u in urls), "transient 503 must retry Grok"


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

    timeouts = []

    def fake_post(url, headers=None, json=None, timeout=None):
        urls.append(url)
        timeouts.append(timeout)
        return _fake_response(200, "DS")

    with mock.patch.object(dc.requests, "post", side_effect=fake_post):
        text = dc.chat([{"role": "user", "content": "hi"}],
                       model="deepseek-chat", tools=False)
    assert text == "DS"
    assert dc.last_provider() == "deepseek"
    assert urls == [f"{config.DEEPSEEK_BASE_URL}/chat/completions"]
    assert timeouts == [(15, 120)]


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


def test_deepseek_read_timeout_does_not_retry() -> None:
    """A hung DeepSeek body must not burn 120s × 4 inside the sector wall."""
    _reset(deepseek_key="ds-key")
    n = {"calls": 0}

    def fake_post(url, headers=None, json=None, timeout=None):
        n["calls"] += 1
        raise dc.requests.ReadTimeout("read timed out")

    with mock.patch.object(dc.requests, "post", side_effect=fake_post), \
            mock.patch.object(dc.time, "sleep") as slept:
        text = dc.chat([{"role": "user", "content": "hi"}],
                       model="deepseek-chat", tools=False)
    assert text == ""
    assert n["calls"] == 1
    slept.assert_not_called()


def test_deepseek_connect_timeout_does_not_retry() -> None:
    """A firewalled api.deepseek.com must not burn 120s × 4 before empty."""
    _reset(deepseek_key="ds-key")
    n = {"calls": 0}

    def fake_post(url, headers=None, json=None, timeout=None):
        n["calls"] += 1
        raise dc.requests.ConnectTimeout("connect timed out")

    with mock.patch.object(dc.requests, "post", side_effect=fake_post), \
            mock.patch.object(dc.time, "sleep") as slept:
        text = dc.chat([{"role": "user", "content": "hi"}],
                       model="deepseek-chat", tools=False)
    assert text == ""
    assert n["calls"] == 1
    slept.assert_not_called()


def test_connect_timeout_marks_gateway_down() -> None:
    """Firewalled SYN-drop must not burn OPENCLAW_TIMEOUT before DeepSeek."""
    _reset(openclaw_url="http://gw:18789", deepseek_key="ds-key")
    timeouts = []

    def fake_post(url, headers=None, json=None, timeout=None):
        timeouts.append(timeout)
        if "gw:18789" in url:
            raise dc.requests.ConnectTimeout("connect timed out")
        return _fake_response(200, "DEEPSEEK ANSWER")

    with mock.patch.object(dc.requests, "post", side_effect=fake_post), \
            mock.patch.object(dc.time, "sleep"):
        text = dc.chat([{"role": "user", "content": "hi"}],
                       model="deepseek-chat", tools=False)
    assert text == "DEEPSEEK ANSWER"
    assert dc.last_provider() == "deepseek"
    assert timeouts[0] == (15, config.OPENCLAW_TIMEOUT)
    assert dc._OPENCLAW_STATE["down"] is True


def test_sector_outcome_caps_deepseek_tool_rounds() -> None:
    """11 sectors × 10 search rounds miss ≥8 files inside sector_wall."""
    _reset(openclaw_url="", deepseek_key="ds-key")
    posts: list[dict] = []

    def fake_post(url, headers=None, json=None, timeout=None):
        posts.append(json or {})
        return _fake_response(200, "", tool_calls=[{
            "id": f"c{len(posts)}",
            "function": {"name": "web_search",
                         "arguments": '{"query":"XLK close"}'},
        }])

    with mock.patch.object(dc.requests, "post", side_effect=fake_post), \
            mock.patch.object(dc, "web_search",
                              return_value='{"results":[]}'), \
            mock.patch.object(dc.time, "sleep"):
        dc.chat([{"role": "user", "content": "grade"}],
                model="deepseek-chat", tools=True,
                stage_label="SECTOR OUTCOME Technology 2026-09-03")
    # 2 capped tool rounds + up to 3 forced no-tool closes (dump/thin retry).
    # 4+1 at 120s/read fills the 600s sector child with 0 files written.
    assert len(posts) == 5


def test_sector_outcome_keeps_essay_instead_of_more_search() -> None:
    """A 200+ char essay plus tool_calls must not start another search round."""
    _reset(openclaw_url="", deepseek_key="ds-key")
    essay = "A" * 220
    searches = []

    def fake_post(url, headers=None, json=None, timeout=None):
        return _fake_response(200, essay, tool_calls=[{
            "id": "c1",
            "function": {"name": "web_search",
                         "arguments": '{"query":"XLK close"}'},
        }])

    with mock.patch.object(dc.requests, "post", side_effect=fake_post), \
            mock.patch.object(dc, "web_search",
                              side_effect=lambda q: searches.append(q) or
                              '{"results":[]}'), \
            mock.patch.object(dc.time, "sleep"):
        text = dc.chat([{"role": "user", "content": "grade"}],
                       model="deepseek-chat", tools=True,
                       stage_label="SECTOR OUTCOME Technology 2026-09-03")
    assert text == essay
    assert searches == []


def test_sector_outcome_caps_tool_calls_per_stage() -> None:
    """One DeepSeek message can emit many tool_calls; each search is ~65s."""
    _reset(openclaw_url="", deepseek_key="ds-key")
    searches = []
    posts = []

    def fake_post(url, headers=None, json=None, timeout=None):
        posts.append(json or {})
        if "tools" in (json or {}):
            return _fake_response(200, "", tool_calls=[
                {"id": f"c{i}",
                 "function": {"name": "web_search",
                              "arguments": '{"query":"q%d"}' % i}}
                for i in range(6)
            ])
        return _fake_response(200, "B" * 220)

    with mock.patch.object(dc.requests, "post", side_effect=fake_post), \
            mock.patch.object(dc, "web_search",
                              side_effect=lambda q: searches.append(q) or
                              '{"results":[]}'), \
            mock.patch.object(dc.time, "sleep"):
        text = dc.chat([{"role": "user", "content": "grade"}],
                       model="deepseek-chat", tools=True,
                       stage_label="SECTOR OUTCOME Technology 2026-09-03")
    assert len(searches) == 2
    assert len(posts) == 2  # one tool round + forced close
    assert text == "B" * 220


def test_extracts_queries_from_dsml_tool_dump() -> None:
    dump = (
        '<｜｜DSML｜｜tool_calls>\n'
        '<｜｜DSML｜｜invoke name="web_search">\n'
        '<｜｜DSML｜｜parameter name="query" string="true">'
        'XLE September 3 2026 oil</｜｜DSML｜｜parameter>\n'
        '<｜｜DSML｜｜invoke name="web_search">\n'
        '<｜｜DSML｜｜parameter name="query" string="true">'
        'WTI close</｜｜DSML｜｜parameter>\n'
    )
    assert dc._extract_dump_queries(dump) == [
        "XLE September 3 2026 oil", "WTI close"]
    assert dc._extract_dump_queries("# essay") == []


def test_tool_dump_followup_forces_no_tool_close() -> None:
    """Live 09-03 Energy: turn 1 real tool_calls, turn 2 DSML dump, no calls."""
    _reset(openclaw_url="", deepseek_key="ds-key")
    searches = []
    posts = []
    dump = (
        '<｜｜DSML｜｜tool_calls>\n'
        '<｜｜DSML｜｜invoke name="web_search">\n'
        '<｜｜DSML｜｜parameter name="query" string="true">'
        'more oil news</｜｜DSML｜｜parameter>\n'
    )

    def fake_post(url, headers=None, json=None, timeout=None):
        posts.append(json or {})
        if "tools" in (json or {}) and len(posts) == 1:
            return _fake_response(200, "", tool_calls=[{
                "id": "c1",
                "function": {"name": "web_search",
                             "arguments": '{"query":"XLE close"}'},
            }])
        if "tools" in (json or {}):
            return _fake_response(200, dump)
        return _fake_response(200, "E" * 220)

    with mock.patch.object(dc.requests, "post", side_effect=fake_post), \
            mock.patch.object(dc, "web_search",
                              side_effect=lambda q: searches.append(q) or
                              '{"results":[]}'), \
            mock.patch.object(dc.time, "sleep"):
        text = dc.chat([{"role": "user", "content": "grade"}],
                       model="deepseek-chat", tools=True,
                       stage_label="SECTOR OUTCOME Energy 2026-09-03")
    assert searches == ["XLE close", "more oil news"]
    assert text == "E" * 220
    from src.skip_if_good import is_tool_dump
    assert not is_tool_dump(text)
    close_posts = [p for p in posts if "tools" not in p]
    assert close_posts
    assert "Do not emit tool calls" in close_posts[0]["messages"][-1]["content"]


def test_forced_close_retries_when_first_answer_is_a_dump() -> None:
    """Live follow-up: the no-tool close can itself be DSML. Retry."""
    _reset(openclaw_url="", deepseek_key="ds-key")
    dump = (
        '<｜｜DSML｜｜tool_calls>\n'
        '<｜｜DSML｜｜invoke name="web_search">\n'
        '<｜｜DSML｜｜parameter name="query" string="true">'
        'still searching</｜｜DSML｜｜parameter>\n'
    )
    posts = []

    def fake_post(url, headers=None, json=None, timeout=None):
        posts.append(json or {})
        if "tools" in (json or {}):
            return _fake_response(200, dump)
        if sum(1 for p in posts if "tools" not in p) < 2:
            return _fake_response(200, dump)
        return _fake_response(200, "C" * 220)

    with mock.patch.object(dc.requests, "post", side_effect=fake_post), \
            mock.patch.object(dc, "web_search",
                              return_value='{"results":[]}'), \
            mock.patch.object(dc.time, "sleep"):
        text = dc.chat([{"role": "user", "content": "grade"}],
                       model="deepseek-chat", tools=True,
                       stage_label="SECTOR OUTCOME Energy 2026-09-03")
    assert text == "C" * 220
    assert sum(1 for p in posts if "tools" not in p) >= 2


def test_map_postclose_caps_deepseek_tool_rounds() -> None:
    """11 captain batches × 10 search rounds miss the 7200s captain wall."""
    _reset(openclaw_url="", deepseek_key="ds-key")
    posts: list[dict] = []

    def fake_post(url, headers=None, json=None, timeout=None):
        posts.append(json or {})
        return _fake_response(200, "", tool_calls=[{
            "id": f"c{len(posts)}",
            "function": {"name": "web_search",
                         "arguments": '{"query":"XLK close"}'},
        }])

    with mock.patch.object(dc.requests, "post", side_effect=fake_post), \
            mock.patch.object(dc, "web_search",
                              return_value='{"results":[]}'), \
            mock.patch.object(dc.time, "sleep"):
        dc.chat([{"role": "user", "content": "cards"}],
                model="deepseek-chat", tools=True,
                stage_label="MAP POSTCLOSE captains_technology 2026-09-07")
    assert len(posts) == 5


def test_chat_nonempty_skips_thin_reasoner_stub() -> None:
    """A 40-char reasoner stub must not become a skip-if-good reflect file."""
    _reset(openclaw_url="", deepseek_key="ds-key")
    n = {"i": 0}

    def fake_post(url, headers=None, json=None, timeout=None):
        n["i"] += 1
        if n["i"] == 1:
            return _fake_response(200, "short stub")
        return _fake_response(200, "C" * 220)

    with mock.patch.object(dc.requests, "post", side_effect=fake_post), \
            mock.patch.object(dc.time, "sleep"):
        text = dc.chat_nonempty(
            [{"role": "user", "content": "reflect"}],
            ladder=[("deepseek-reasoner", 12000),
                    ("deepseek-chat", 8000)],
            tools=False,
            stage_label="SECTOR REFLECT Technology 2026-09-03",
        )
    assert text == "C" * 220
    assert n["i"] == 2


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
        test_deepseek_connect_timeout_does_not_retry,
        test_deepseek_read_timeout_does_not_retry,
        test_deepseek_chat_caps_grok_sized_output_budget,
        test_describe_routing_no_secrets,
        test_timeout_content_is_empty,
        test_grok_only_blocks_deepseek_and_force_flag,
        test_grok_only_no_fallback_when_gateway_401,
        test_grok_only_no_fallback_when_already_down,
        test_connect_timeout_marks_gateway_down,
        test_pick_openclaw_token_prefers_live_48,
        test_sector_outcome_caps_deepseek_tool_rounds,
        test_sector_outcome_keeps_essay_instead_of_more_search,
        test_sector_outcome_caps_tool_calls_per_stage,
        test_extracts_queries_from_dsml_tool_dump,
        test_tool_dump_followup_forces_no_tool_close,
        test_forced_close_retries_when_first_answer_is_a_dump,
        test_map_postclose_caps_deepseek_tool_rounds,
        test_chat_nonempty_skips_thin_reasoner_stub,
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
