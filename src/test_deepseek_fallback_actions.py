"""Emergency provider fallback wiring for the pre-market action chain."""
from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace
from unittest import mock

from collectors.catalyst_grok_runtime import install
from src import config, deepseek_client

ROOT = Path(__file__).resolve().parent.parent
WORKFLOWS = ROOT / ".github" / "workflows"

LLM_ACTIONS = (
    "map_heat_postclose.yml",
    "preopen_all.yml",
    "catalyst_daily.yml",
    "stock_book_all.yml",
)
NON_LLM_ACTIONS = (
    "finviz_preopen_scrape.yml",
    "label_weather.yml",
    "ab_checklist.yml",
)


def test_llm_actions_enable_deepseek_fallback() -> None:
    for name in LLM_ACTIONS:
        text = (WORKFLOWS / name).read_text(encoding="utf-8")
        assert 'GROK_ONLY: "0"' in text, name
        assert "secrets.DEEPSEEK_API_KEY" in text, name


def test_non_llm_actions_do_not_depend_on_grok() -> None:
    for name in NON_LLM_ACTIONS:
        text = (WORKFLOWS / name).read_text(encoding="utf-8")
        assert 'GROK_ONLY: "1"' not in text, name
        assert "OPENCLAW_GATEWAY_URL" not in text, name


def test_catalyst_overlay_uses_public_fallback_client() -> None:
    saved = (
        config.OPENCLAW_GATEWAY_URL,
        config.DEEPSEEK_API_KEY,
        config._OPENCLAW_TOKEN_ALIGNED,
    )
    config.OPENCLAW_GATEWAY_URL = "http://gw:18789"
    config.DEEPSEEK_API_KEY = "ds-key"
    config._OPENCLAW_TOKEN_ALIGNED = True
    fake_ca = SimpleNamespace()
    try:
        install(fake_ca)
        with mock.patch.object(
            deepseek_client, "chat", return_value="DEEPSEEK ANSWER"
        ) as routed, mock.patch.object(
            deepseek_client, "last_provider", return_value="deepseek"
        ):
            text = fake_ca.call_llm(
                "system", "user", tools=True, stage="CATALYST TEST"
            )
        assert text == "DEEPSEEK ANSWER"
        routed.assert_called_once()
        assert routed.call_args.kwargs["tools"] is True
        assert routed.call_args.kwargs["model"] == config.MODEL_PREDICT
    finally:
        (
            config.OPENCLAW_GATEWAY_URL,
            config.DEEPSEEK_API_KEY,
            config._OPENCLAW_TOKEN_ALIGNED,
        ) = saved


if __name__ == "__main__":
    test_llm_actions_enable_deepseek_fallback()
    test_non_llm_actions_do_not_depend_on_grok()
    test_catalyst_overlay_uses_public_fallback_client()
    print("3 DeepSeek fallback action tests passed")
