"""Unit tests for multi-provider LLM routing. No network, no real keys."""
from __future__ import annotations

import src.config as config
from src.deepseek_client import _prepare_payload, describe_routing


def test_is_xai_model() -> None:
    assert config.is_xai_model("grok-4.6")
    assert config.is_xai_model("grok-3-mini")
    assert config.is_xai_model("xai/grok-4.6")
    assert config.is_xai_model("x-ai/grok-4.6")
    assert not config.is_xai_model("deepseek-chat")
    assert not config.is_xai_model("deepseek-reasoner")
    assert not config.is_xai_model("")
    assert not config.is_xai_model("gpt-4o")


def test_provider_and_urls() -> None:
    assert config.provider_for("grok-4.6") == "xai"
    assert config.provider_for("deepseek-chat") == "deepseek"
    assert config.base_url_for("grok-4.6") == config.XAI_BASE_URL
    assert config.base_url_for("deepseek-reasoner") == config.DEEPSEEK_BASE_URL


def test_fallback_and_resolve() -> None:
    saved = {
        "XAI_API_KEY": config.XAI_API_KEY,
        "DEEPSEEK_API_KEY": config.DEEPSEEK_API_KEY,
    }
    try:
        config.XAI_API_KEY = "xai-test"
        config.DEEPSEEK_API_KEY = "ds-test"
        assert config.has_key_for("grok-4.6")
        assert config.has_key_for("deepseek-chat")
        assert config.fallback_model("grok-4.6") == "deepseek-reasoner"
        assert config.fallback_model("grok-4.6", tools=True) == "deepseek-chat"
        assert config.resolve_model("grok-4.6") == "grok-4.6"

        config.XAI_API_KEY = ""
        assert not config.has_key_for("grok-4.6")
        assert config.resolve_model("grok-4.6") == "deepseek-reasoner"
        assert config.resolve_model("grok-4.6", tools=True) == "deepseek-chat"

        config.DEEPSEEK_API_KEY = ""
        config.XAI_API_KEY = "xai-test"
        assert config.resolve_model("deepseek-reasoner") == config.MODEL_GROK
        assert config.has_any_llm_key()
    finally:
        config.XAI_API_KEY = saved["XAI_API_KEY"]
        config.DEEPSEEK_API_KEY = saved["DEEPSEEK_API_KEY"]


def test_require_llm_messages() -> None:
    saved_x, saved_d = config.XAI_API_KEY, config.DEEPSEEK_API_KEY
    try:
        config.XAI_API_KEY = ""
        config.DEEPSEEK_API_KEY = ""
        try:
            config.require_llm("grok-4.6")
        except SystemExit as e:
            assert "XAI_API_KEY" in str(e)
            assert "console.x.ai" in str(e)
        else:
            raise AssertionError("require_llm(grok) should have exited")
        try:
            config.require_llm("deepseek-chat")
        except SystemExit as e:
            assert "DEEPSEEK_API_KEY" in str(e)
        else:
            raise AssertionError("require_llm(deepseek) should have exited")
    finally:
        config.XAI_API_KEY, config.DEEPSEEK_API_KEY = saved_x, saved_d


def test_prepare_payload_xai() -> None:
    body = _prepare_payload({
        "model": "grok-4.6",
        "messages": [{"role": "user", "content": "hi"}],
        "max_tokens": 4000,
        "temperature": 0.2,
    })
    assert "max_tokens" not in body
    assert body["max_completion_tokens"] == 4000
    assert body["prompt_cache_key"] == "fullscan"


def test_prepare_payload_deepseek() -> None:
    body = _prepare_payload({
        "model": "deepseek-chat",
        "messages": [{"role": "user", "content": "hi"}],
        "max_tokens": 4000,
        "temperature": 0.2,
    })
    assert body["max_tokens"] == 4000
    assert "max_completion_tokens" not in body
    assert "prompt_cache_key" not in body


def test_reflect_ladder_shape() -> None:
    ladder = config.reflect_ladder()
    assert ladder[0][0] == config.MODEL_REFLECT
    assert ladder[0][1] == 12000
    assert ladder[1] == (config.MODEL_REFLECT, 16000)
    models = [m for m, _ in ladder]
    assert config.MODEL_PREDICT in models or config.MODEL_REFLECT == config.MODEL_PREDICT


def test_describe_routing_never_dumps_keys() -> None:
    text = describe_routing()
    assert "DEEPSEEK_API_KEY" in text
    assert "XAI_API_KEY" in text
    # never print the secret values themselves
    if config.XAI_API_KEY:
        assert config.XAI_API_KEY not in text
    if config.DEEPSEEK_API_KEY:
        assert config.DEEPSEEK_API_KEY not in text


def main() -> None:
    tests = [
        test_is_xai_model,
        test_provider_and_urls,
        test_fallback_and_resolve,
        test_require_llm_messages,
        test_prepare_payload_xai,
        test_prepare_payload_deepseek,
        test_reflect_ladder_shape,
        test_describe_routing_never_dumps_keys,
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
