"""Freshness gate for the 2026-09-24 stale Pre-Open news window.

Run: python -m src.test_news_freshness
"""
from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

from src.news_freshness import (
    NEWS_MODE_STALE,
    assess,
    banner,
    decision,
    grok_stale_reason,
)
from src.news_live import live_url

ROOT = Path(__file__).resolve().parent.parent
FIXTURE = ROOT / "src" / "fixtures" / "stale_news_2026-09-24.json"


def _items(stamp: str, n: int = 8) -> list[dict]:
    return [
        {"title": f"headline {i}", "published_at": stamp, "source": "rss"}
        for i in range(n)
    ]


def test_aug_26_29_fixture_is_stale_for_session_2026_09_24() -> None:
    blob = json.loads(FIXTURE.read_text(encoding="utf-8"))
    verdict = assess(blob["items"], "2026-09-24")
    assert verdict["ok"] is False
    assert verdict["news_mode"] == NEWS_MODE_STALE
    assert verdict["median_published"] == "2026-08-28"
    assert verdict["recent_share"] == 0.0
    assert verdict["median_floor"] == "2026-09-22"
    assert "2026-08-28" in verdict["reason"]


def test_on_disk_2026_09_24_parse_matches_the_fixture() -> None:
    """The real Pre-Open file, not just the reduced fixture."""
    path = ROOT / "01_daily" / "news" / "2026-09-24_parsed.json"
    data = json.loads(path.read_text(encoding="utf-8"))
    items = data.get("all_items") or []
    assert len(items) == 400
    verdict = assess(items, "2026-09-24")
    assert verdict["ok"] is False
    assert verdict["median_published"] == "2026-08-28"
    assert verdict["n_recent"] == 0
    dec = decision("2026-09-24", ROOT)
    assert dec["ok"] is False
    assert dec["news_mode"] == NEWS_MODE_STALE


def test_same_session_window_passes() -> None:
    verdict = assess(_items("2026-09-24T06:10:00-04:00"), "2026-09-24")
    assert verdict["ok"] is True
    assert verdict["news_mode"] == "on"
    assert verdict["recent_share"] == 1.0


def test_median_inside_two_trading_days_but_outside_36h_fails() -> None:
    # Tuesday 2026-09-22 is the median floor for Thursday 09-24, but
    # Tuesday morning is more than 36h before Thursday 09:30 ET.
    verdict = assess(_items("2026-09-22T08:00:00-04:00"), "2026-09-24")
    assert verdict["ok"] is False
    assert verdict["median_published"] == "2026-09-22"
    assert "36h" in verdict["reason"]


def test_undated_titles_cannot_prove_a_window() -> None:
    verdict = assess(
        [{"title": "Fed patience", "published_at": ""} for _ in range(12)],
        "2026-09-24")
    assert verdict["ok"] is False
    assert verdict["n_dated"] == 0


def test_grok_stale_news_is_a_hard_stop_map_heat_is_not() -> None:
    import tempfile
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        daily = root / "01_daily"
        daily.mkdir()
        (daily / "2026-09-24_grok_review.json").write_text(json.dumps({
            "ok": False,
            "fails": [
                {"path": "01_daily/news/2026-09-24_parsed.json",
                 "reason": "carry-forward of a stale 2026-08-27 news window"},
                {"path": "01_daily/map_heat/2026-09-24_map_heat.md",
                 "reason": "ES +0.20% vs -0.64% elsewhere"},
            ],
        }), encoding="utf-8")
        reason = grok_stale_reason("2026-09-24", root)
        assert "stale" in reason
        assert "ES" not in reason
        # Fresh parse on disk must still stop when grok named a stale window.
        news = daily / "news"
        news.mkdir()
        (news / "2026-09-24_parsed.json").write_text(json.dumps({
            "news_mode": "on",
            "freshness": {"ok": True},
            "all_items": _items("2026-09-24T06:00:00-04:00"),
        }), encoding="utf-8")
        dec = decision("2026-09-24", root)
        assert dec["ok"] is False
        assert dec["via"] == "grok"
        assert dec["news_mode"] == NEWS_MODE_STALE


def test_banner_names_none_stale() -> None:
    text = banner("median published 2026-08-28")
    assert "NEWS_MODE: none_stale" in text
    assert "2026-08-28" in text


def test_live_google_url_pins_when_2d() -> None:
    url = ("https://news.google.com/rss/search?q=federal+reserve+OR+inflation"
           "&hl=en-US&gl=US&ceid=US:en")
    got = live_url(url)
    assert "when:2d" in got
    assert live_url(got) == got
    assert live_url("https://feeds.bbci.co.uk/news/business/rss.xml") == (
        "https://feeds.bbci.co.uk/news/business/rss.xml")


def test_live_fetch_keeps_only_the_hours_window() -> None:
    from src import news_live

    class _E(dict):
        pass

    def parse_feed(_url):
        return type("F", (), {"entries": [
            _E(title="Fresh CPI", link="https://ex/a",
               published="Wed, 23 Sep 2026 22:00:00 GMT"),
            _E(title="Warsh August", link="https://ex/b",
               published="Fri, 28 Aug 2026 20:10:00 GMT"),
        ]})()

    now = datetime(2026, 9, 24, 10, 0, tzinfo=timezone.utc)
    rows = news_live.fetch(
        limit=10, hours=48, now=now, parse_feed=parse_feed,
        feeds=[("rss_google_macro", "https://example.test/rss")])
    titles = [r["title"] for r in rows]
    assert titles == ["Fresh CPI"]
    assert rows[0]["source"]


def test_catalyst_skips_news_actions_when_stale() -> None:
    from src import catalyst_daily

    def _load(path):
        name = str(path)
        if name.endswith("_actions.json"):
            return {"ticker_actions": [
                {"ticker": "XLE", "side": "buy", "net": 3.0},
            ]}
        return {}

    orig = catalyst_daily._load_json
    orig_dec = None
    import src.news_freshness as nf
    orig_dec = nf.decision
    catalyst_daily._load_json = _load  # type: ignore[assignment]
    nf.decision = lambda *_a, **_k: {  # type: ignore[assignment]
        "ok": False, "news_mode": "none_stale", "reason": "fixture",
    }
    try:
        picked = catalyst_daily.select_targets("2026-09-24", max_n=8)
    finally:
        catalyst_daily._load_json = orig  # type: ignore[assignment]
        nf.decision = orig_dec  # type: ignore[assignment]
    assert not any(p.get("role") == "action_top" for p in picked)


if __name__ == "__main__":
    test_aug_26_29_fixture_is_stale_for_session_2026_09_24()
    test_on_disk_2026_09_24_parse_matches_the_fixture()
    test_same_session_window_passes()
    test_median_inside_two_trading_days_but_outside_36h_fails()
    test_undated_titles_cannot_prove_a_window()
    test_grok_stale_news_is_a_hard_stop_map_heat_is_not()
    test_banner_names_none_stale()
    test_live_google_url_pins_when_2d()
    test_live_fetch_keeps_only_the_hours_window()
    test_catalyst_skips_news_actions_when_stale()
    print("10 tests passed")
