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


def test_object_followed_by_prose_is_a_dict() -> None:
    """09-02..09-04: 0/8 dossiers every morning — DeepSeek closed each
    Step 2/4 object with a sentence, the salvage returned [obj] and the
    caller's `.get` blew up on a list."""
    raw = ('Here is the profile:\n'
           '{"ticker": "NUE", "sensitivity_profile": '
           '{"Contract win/expansion": {"multiplier": 1.2, "rationale": "x"}}}'
           '\n\nNote: sources checked.')
    parsed = ca.parse_json(raw)
    assert isinstance(parsed, dict)
    assert parsed["sensitivity_profile"]["Contract win/expansion"]["multiplier"] == 1.2
    raw4 = ('{"ticker": "NUE", "catalyst_grid": [{"taxonomy": "Earnings beat", '
            '"status": "HIT"}], "net_signal": "Bullish", "conviction": 7}\nDone.')
    final = ca.parse_json(raw4)
    assert isinstance(final, dict) and final["net_signal"] == "Bullish"
    # Arrays stay arrays (events), truncated or not.
    assert isinstance(ca.parse_json('[{"a": 1}, {"b": 2}]  trailing'), list)


def test_as_object_picks_the_dict_a_step_expects() -> None:
    assert ca.as_object({"x": 1}) == {"x": 1}
    assert ca.as_object([{"noise": 1}, {"sensitivity_profile": {}}],
                        "sensitivity_profile") == {"sensitivity_profile": {}}
    assert ca.as_object([{"only": 1}], "catalyst_grid") == {"only": 1}
    assert ca.as_object("text", "catalyst_grid") == {}
    assert ca.as_object([1, 2], "catalyst_grid") == {}


_TRUNCATED_STEP4 = """{
  "ticker": "ORCL",
  "analysis_date": "2026-09-10",
  "current_price": "162.52",
  "net_signal": "Bullish",
  "conviction": 64,
  "catalyst_stack": "OCI demand rose on 2026-08-07; a $30bn contract landed 2026-09-02.",
  "key_assumption": "Cloud backlog converts \\"as guided\\".",
  "catalyst_grid": [
    {
      "taxonomy": "Contract win/expansion",
      "type": "positive",
      "category": "internal",
      "status": "HIT",
      "base_weight": 8,
      "adjusted_weight": 10,
      "event_ids": [0, 3],
      "event_date": "2026-09-02",
      "evidence_excerpt": "$30bn cloud contract",
      "source_urls": ["https://x"],
      "confidence": 90
    },
    {
      "taxonomy": "Insider selling (cluster)",
      "type": "negative",
      "status": "HIT",
      "adjusted_weight": 4,
      "event_date": "2026-08-20",
      "confidence": 70
    },
    {
      "taxonomy": "Earnings beat (revenue, EBITDA, EPS)",
      "type": "positive",
      "category": "i"""


def test_salvage_truncated_step4_keeps_complete_rows() -> None:
    """09-10 ORCL/SLVM: DeepSeek's 8192-token cap cut the grid mid-string and
    the whole ticker became 'Step 4 parse failure'. Complete rows + the
    summary fields in front of the grid are a usable dossier."""
    try:
        ca.parse_json(_TRUNCATED_STEP4)
        parsed_ok = True
    except ValueError:
        parsed_ok = False
    got = ca.salvage_step4(_TRUNCATED_STEP4)
    assert got and got["salvaged"] is True
    assert [r["taxonomy"] for r in got["catalyst_grid"]] == [
        "Contract win/expansion", "Insider selling (cluster)"]
    assert got["net_signal"] == "Bullish" and got["conviction"] == 64
    assert got["current_price"] == "162.52"
    assert got["key_assumption"] == 'Cloud backlog converts "as guided".'
    assert got["catalyst_stack"].startswith("OCI demand")
    # Without summary fields the signal is recomputed from the HIT rows.
    grid_only = '{"ticker": "X", "catalyst_grid": [' + \
        _TRUNCATED_STEP4.split('"catalyst_grid": [', 1)[1]
    got2 = ca.salvage_step4(grid_only)
    assert got2 and got2["net_signal"] in {"Neutral", "Bullish"}
    assert ca.as_object(got2, "catalyst_grid", "net_signal") is got2
    # Nothing to salvage: prose, empty, or no complete row.
    assert ca.salvage_step4("## Post-session review — CATALYST STEP4 ORCL") is None
    assert ca.salvage_step4("") is None
    assert ca.salvage_step4('{"ticker": "X", "catalyst_grid": [{"taxonomy": "cut') is None
    assert parsed_ok in (True, False)


def test_step4_prompt_is_compact_and_summary_first() -> None:
    """The full 66-row grid with excerpts blew DeepSeek's 8192-token cap."""
    prompt = ca._format_step4("Oracle (ORCL)", "ORCL", "2026-09-10", "[]", "{}", "{}")
    assert "OUTPUT ONLY the HIT rows" in prompt
    assert "at most 25 rows" in prompt
    assert "under 200 characters" in prompt
    assert prompt.index('"net_signal"') < prompt.index('"catalyst_grid"')
    assert prompt.index('"catalyst_stack"') < prompt.index('"catalyst_grid"')
    assert "Build the FULL catalyst grid (66 items). For each catalyst, set status" not in prompt


if __name__ == "__main__":
    test_parse_truncated_array()
    test_parse_fenced_complete()
    test_filter_drops_2025()
    test_search_years_are_window_not_hardcoded_2025()
    test_step1_prompt_names_the_window()
    test_verdict_prompt_live_uses_today_not_none()
    test_object_followed_by_prose_is_a_dict()
    test_as_object_picks_the_dict_a_step_expects()
    test_salvage_truncated_step4_keeps_complete_rows()
    test_step4_prompt_is_compact_and_summary_first()
    print("10 tests passed")
