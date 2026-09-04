"""Two-job pipeline: skip-if-good keys + LLM backend toggle.

Run: PYTHONPATH=. python3 -m src.test_two_job_pipeline
"""
from __future__ import annotations

import os

from src import config, skip_if_good
from src.run_preopen_all import _packet_step_done


def test_llm_backend_auto_allows_deepseek() -> None:
    os.environ.pop("FORCE_DEEPSEEK", None)
    config.apply_llm_backend("auto")
    assert config.llm_backend() == "auto"
    assert config.grok_only() is False
    assert config.prefer_deepseek() is False


def test_llm_backend_grok_blocks_deepseek() -> None:
    config.apply_llm_backend("grok")
    assert config.grok_only() is True
    assert config.prefer_deepseek() is False


def test_llm_backend_deepseek_skips_gateway() -> None:
    config.apply_llm_backend("deepseek")
    assert config.prefer_deepseek() is True
    assert config.grok_only() is False


def test_skip_jobs_registered() -> None:
    for key in ("preopen_full", "postclose_all", "sector_outcomes"):
        assert key in skip_if_good.JOBS


def test_packet_step_done_missing_is_false() -> None:
    assert _packet_step_done("news_parse", "1999-01-01") is False
    assert _packet_step_done("events", "1999-01-01") is False
    assert _packet_step_done("general_predict", "1999-01-01") is False


def test_postclose_default_date_is_closed_session() -> None:
    d = skip_if_good.last_closed_session()
    assert len(d) == 10


if __name__ == "__main__":
    test_llm_backend_auto_allows_deepseek()
    test_llm_backend_grok_blocks_deepseek()
    test_llm_backend_deepseek_skips_gateway()
    test_skip_jobs_registered()
    test_packet_step_done_missing_is_false()
    test_postclose_default_date_is_closed_session()
    print("ok")
