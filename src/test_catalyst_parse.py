"""Catalyst JSON salvage + lookback window. No LLM.

Run: python -m src.test_catalyst_parse
"""
from __future__ import annotations

import os

os.environ.setdefault("DEEPSEEK_API_KEY", "test-dummy")
os.environ.setdefault("GROK_ONLY", "0")

from collectors import catalyst_analysis as ca


def test_parse_truncated_array() -> None:
    raw = """[
  {
    "event_date": "2026-08-07",
    "description": "Oracle expanded OpenAI on OCI.",
    "evidence_excerpt": "marketplace expansion",
    "source_urls": ["https://openai.com/x"],
    "confidence": 90
  },
  {
    "event_date": "2025-06-11",
    "description": "Oracle Corp (ORCL) reported FY2025 Q4 revenue of $15.9 billion, up 11%
"""
    parsed = ca.parse_json(raw)
    assert isinstance(parsed, list)
    assert len(parsed) == 1
    assert parsed[0]["event_date"] == "2026-08-07"


def test_parse_fenced_complete() -> None:
    raw = """```json
[{"event_date": "2026-09-01", "description": "ok"}]
```"""
    parsed = ca.parse_json(raw)
    assert parsed[0]["description"] == "ok"


def test_filter_drops_2025() -> None:
    events = [
        {"event_date": "2025-10-13", "description": "old AVGO/OpenAI"},
        {"event_date": "2026-08-07", "description": "fresh"},
        {"event_date": "2026-09-15", "description": "after as-of"},
    ]
    kept = ca.filter_events_to_window(
        events, lookback_start="2026-03-01", cutoff="2026-09-02")
    assert [e["description"] for e in kept] == ["fresh"]


def test_search_years_are_window_not_hardcoded_2025() -> None:
    orig = (ca.TODAY, ca.LOOKBACK_START, ca.CUTOFF_DATE)
    try:
        ca.TODAY = "2026-09-02"
        ca.LOOKBACK_START = "2026-03-01"
        ca.CUTOFF_DATE = None
        qs = ca._make_catalyst_templates("Broadcom Inc (AVGO)")
        assert all("2025" not in q for q in qs)
        assert any("2026" in q for q in qs)
        assert ca._search_year_span() == "2026"
    finally:
        ca.TODAY, ca.LOOKBACK_START, ca.CUTOFF_DATE = orig


def test_step1_prompt_names_the_window() -> None:
    prompt = ca._format_step1(
        "Broadcom Inc (AVGO)", "AVGO", "2026-09-02", "2026-03-01",
        "search", "[]")
    assert "2026-03-01" in prompt
    assert "2026-09-02" in prompt
    assert "LOOKBACK START" in prompt


def test_verdict_prompt_live_uses_today_not_none() -> None:
    orig = (ca.TODAY, ca.LOOKBACK_START)
    try:
        ca.TODAY = "2026-09-02"
        ca.LOOKBACK_START = "2026-03-01"
        text = ca.build_verdict_prompt("Oracle Corp (ORCL)", "ORCL", None)
        assert "AS-OF DATE: 2026-09-02" in text
        assert "CUTOFF DATE: None" not in text
        assert "2026-03-01" in text
    finally:
        ca.TODAY, ca.LOOKBACK_START = orig


if __name__ == "__main__":
    test_parse_truncated_array()
    test_parse_fenced_complete()
    test_filter_drops_2025()
    test_search_years_are_window_not_hardcoded_2025()
    test_step1_prompt_names_the_window()
    test_verdict_prompt_live_uses_today_not_none()
    print("6 tests passed")
