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


def test_finviz_news_dates_are_real_calendar_days() -> None:
    """Quote-page stamps `Sep-03-26 04:32PM`; time-only rows inherit that day.

    The previous parser never matched that token (it tried `%b-%d-%y` on
    `Sep-03-26` via a digit-only regex, then `%b-%d-%Y` on `Sep-03-26-2026`),
    so every headline was labeled TODAY.
    """
    today = "2026-09-04"
    first = ca.parse_finviz_news_date("Sep-03-26 04:32PM", today=today)
    assert first == "2026-09-03"
    carried = ca.parse_finviz_news_date("07:15AM", last_date=first, today=today)
    assert carried == "2026-09-03"
    assert ca.parse_finviz_news_date("Today 08:01AM", today=today) == today
    assert ca.parse_finviz_news_date("Yesterday 09:00PM", today=today) == "2026-09-03"
    assert ca.parse_finviz_news_date("8 min", today=today) == today
    assert ca.parse_finviz_news_date("Sep-03", today=today) == "2026-09-03"
    assert ca.parse_finviz_news_date("Dec-28", today="2026-01-05") == "2025-12-28"
    assert ca.parse_finviz_news_date("2026-09-02 16:55:36", today=today) == "2026-09-02"
    from datetime import datetime
    assert ca.parse_finviz_news_date(datetime(2026, 8, 27, 8, 30), today=today) == "2026-08-27"


def test_old_finviz_date_formats_do_not_match() -> None:
    """Document the bug: these are the formats the old code tried."""
    raw = "Sep-03-26 04:32PM"
    token = raw.split()[0]
    from datetime import datetime
    failed = False
    try:
        datetime.strptime(token, "%I:%M %p %m/%d/%Y")
    except ValueError:
        failed = True
    assert failed
    failed = False
    try:
        datetime.strptime(f"{token}-2026", "%b-%d-%Y")
    except ValueError:
        failed = True
    assert failed
    # And the first-branch regex never saw Sep-03-26 as NN-NN-NN.
    import re
    assert not re.search(r"\d{2}-\d{2}-\d{2}", raw)


def test_parse_quote_news_html_carries_dates() -> None:
    from collectors.catalyst_grok_runtime import parse_quote_news_html
    html = """
    <table class="fullview-news-outer">
      <tr>
        <td>Sep-03-26 04:32PM</td>
        <td><a href="https://example.com/a" class="tab-link-news">AbbVie Phase 3 data</a>
            <span>(Reuters)</span></td>
      </tr>
      <tr>
        <td>07:15AM</td>
        <td><a href="https://example.com/b" class="tab-link-news">Same-day follow-up</a></td>
      </tr>
      <tr class="news_table-row">
        <td class="news_date-cell">Sep-02-26 11:00AM</td>
        <td><a class="nn-tab-link" href="https://example.com/c">Older item</a>
            <span class="news_date-cell">Bloomberg</span></td>
      </tr>
    </table>
    """
    orig = (ca.TODAY, ca.LOOKBACK_START, ca.CUTOFF_DATE)
    try:
        ca.TODAY = "2026-09-04"
        ca.LOOKBACK_START = "2026-03-01"
        ca.CUTOFF_DATE = None
        rows = parse_quote_news_html(html, "ABBV", ca)
    finally:
        ca.TODAY, ca.LOOKBACK_START, ca.CUTOFF_DATE = orig
    assert [r["event_date"] for r in rows] == ["2026-09-03", "2026-09-03", "2026-09-02"]
    assert rows[0]["headline"] == "AbbVie Phase 3 data"
    assert rows[0]["finviz_source"] == "Reuters"
    assert rows[1]["headline"] == "Same-day follow-up"
    assert rows[2]["source_urls"][0].startswith("https://example.com/c")


def test_coerce_step2_list_does_not_crash() -> None:
    salvaged = [{
        "ticker": "ORCL",
        "sensitivity_profile": {
            "Earnings beat (revenue, EBITDA, EPS)": {
                "multiplier": 1.2, "rationale": "high operating leverage"
            }
        },
    }]
    profile = ca.coerce_context_profile(salvaged)
    assert profile["ticker"] == "ORCL"
    assert "Earnings beat (revenue, EBITDA, EPS)" in profile["sensitivity_profile"]
    assert ca.coerce_context_profile(["not a dict"]).get("sensitivity_profile", {}) == {}


def test_coerce_step4_list_of_hits() -> None:
    parsed = [{"taxonomy": "Earnings beat (revenue, EBITDA, EPS)", "status": "HIT"}]
    result = ca.coerce_final_result(parsed)
    assert result["catalyst_grid"][0]["taxonomy"].startswith("Earnings")


if __name__ == "__main__":
    tests = [
        test_parse_truncated_array,
        test_parse_fenced_complete,
        test_filter_drops_2025,
        test_search_years_are_window_not_hardcoded_2025,
        test_step1_prompt_names_the_window,
        test_verdict_prompt_live_uses_today_not_none,
        test_finviz_news_dates_are_real_calendar_days,
        test_old_finviz_date_formats_do_not_match,
        test_parse_quote_news_html_carries_dates,
        test_coerce_step2_list_does_not_crash,
        test_coerce_step4_list_of_hits,
    ]
    for fn in tests:
        fn()
        print(f"ok  {fn.__name__}")
    print(f"{len(tests)} tests passed")
