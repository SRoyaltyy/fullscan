"""Freshness gate for the 2026-09-24 stale Pre-Open news window.

Run: python -m src.test_news_freshness
"""
from __future__ import annotations

import json
import os
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from unittest import mock

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


def test_finviz_scraped_at_dates_a_same_morning_export() -> None:
    stamp = "2026-09-24T05:40:00-04:00"
    verdict = assess(
        [{"title": f"headline {i}", "published_at": "", "scraped_at": stamp}
         for i in range(8)],
        "2026-09-24")
    assert verdict["ok"] is True
    assert verdict["n_dated"] == 8
    assert verdict["recent_share"] == 1.0
    assert verdict["median_published"] == "2026-09-24"


def test_old_finviz_scrape_is_still_stale() -> None:
    verdict = assess(
        [{"title": f"headline {i}", "published_at": "",
          "scraped_at": "2026-08-28T05:40:00-04:00"}
         for i in range(8)],
        "2026-09-24")
    assert verdict["ok"] is False
    assert verdict["median_published"] == "2026-08-28"
    assert verdict["recent_share"] == 0.0


def test_published_at_wins_over_a_fresh_scrape_stamp() -> None:
    items = [{
        "title": f"headline {i}",
        "published_at": "2026-08-28T08:00:00-04:00",
        "scraped_at": "2026-09-24T05:40:00-04:00",
    } for i in range(8)]
    verdict = assess(items, "2026-09-24")
    assert verdict["ok"] is False
    assert verdict["median_published"] == "2026-08-28"
    assert verdict["n_recent"] == 0


def test_decision_assesses_scraped_at_when_published_at_is_blank() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        news = root / "01_daily" / "news"
        news.mkdir(parents=True)
        fresh = [{
            "title": f"headline {i}",
            "published_at": "",
            "scraped_at": "2026-09-24T05:40:00-04:00",
        } for i in range(8)]
        (news / "2026-09-24_parsed.json").write_text(json.dumps({
            "all_items": fresh,
        }), encoding="utf-8")
        dec = decision("2026-09-24", root)
        assert dec["ok"] is True
        assert dec["news_mode"] == "on"
        stale = [{
            "title": f"headline {i}",
            "published_at": "",
            "scraped_at": "2026-08-28T05:40:00-04:00",
        } for i in range(8)]
        (news / "2026-09-24_parsed.json").write_text(json.dumps({
            "all_items": stale,
        }), encoding="utf-8")
        dec = decision("2026-09-24", root)
        assert dec["ok"] is False
        assert dec["via"] == "assess"
        assert dec["news_mode"] == NEWS_MODE_STALE


def _finviz_digest(stamp_key: str | None, stamp: str) -> dict:
    titles = [f"Fed headline {i} on inflation and rates" for i in range(8)]
    body = {
        "top_signal": [
            {"news_title": title, "digest": f"digest {i}", "source": "finviz_export"}
            for i, title in enumerate(titles)
        ],
        "index_digests": [{
            "digest": "S&P 500 waits on CPI",
            "source": "finviz_elite_news",
        }],
    }
    if stamp_key:
        body[stamp_key] = stamp
    return body


def test_local_finviz_rows_carry_the_export_stamp() -> None:
    from src import news_parse
    from src.lane_one_shot import _published_span

    stamp = "2026-09-24T05:40:00-04:00"
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        news = root / "01_daily" / "news"
        events = root / "01_daily" / "events"
        ch1 = root / "01_daily" / "_channel1"
        news.mkdir(parents=True)
        events.mkdir(parents=True)
        ch1.mkdir(parents=True)
        (news / "2026-09-24_finviz_digest.json").write_text(
            json.dumps(_finviz_digest("generated_at", stamp)), encoding="utf-8")
        (events / "2026-09-24_events.json").write_text(json.dumps({
            "events": [{"title": "CPI print at 08:30", "source": "events"}],
        }), encoding="utf-8")
        (ch1 / "2026-09-24_predict.json").write_text(json.dumps({
            "news_24h": {"items": [{
                "title": "Channel one oil headline",
                "source": "channel1",
                "published_at": "2026-08-28T08:00:00-04:00",
            }]},
        }), encoding="utf-8")
        orig = news_parse.NEWS_DIR
        news_parse.NEWS_DIR = str(news)
        cwd = os.getcwd()
        try:
            os.chdir(root)
            rows = news_parse.rows_from_local_files("2026-09-24", limit=40)
        finally:
            os.chdir(cwd)
            news_parse.NEWS_DIR = orig
    finviz = [r for r in rows if "finviz" in str(r.get("source"))]
    assert len(finviz) >= 8
    assert all(r.get("scraped_at") == stamp for r in finviz)
    assert all(not r.get("published_at") for r in finviz)
    event = next(r for r in rows if r["source"] == "events")
    assert "scraped_at" not in event
    channel = next(r for r in rows if r["source"] == "channel1")
    assert "scraped_at" not in channel
    assert channel["published_at"] == "2026-08-28T08:00:00-04:00"
    span = _published_span(finviz)
    assert span["min"] == "2026-09-24"
    assert span["max"] == "2026-09-24"
    assert span["n_dated"] == len(finviz)


def test_sidecar_dates_a_digest_that_has_no_generated_at() -> None:
    from src import news_parse

    stamp = "2026-09-24T05:40:12-04:00"
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        news = root / "01_daily" / "news"
        exports = root / "data" / "exports"
        news.mkdir(parents=True)
        exports.mkdir(parents=True)
        (exports / "finviz_2026-09-24.scraped_at").write_text(
            stamp + "\n", encoding="utf-8")
        digest = _finviz_digest(None, "")
        digest["export_used"] = "data/exports/finviz_2026-09-24.csv"
        (news / "2026-09-24_finviz_digest.json").write_text(
            json.dumps(digest), encoding="utf-8")
        orig = news_parse.NEWS_DIR
        news_parse.NEWS_DIR = str(news)
        cwd = os.getcwd()
        try:
            os.chdir(root)
            with mock.patch.object(news_parse.db, "recent_news", return_value=[]), \
                    mock.patch.object(
                        news_parse.db, "_recent_news_once", return_value=[]), \
                    mock.patch("src.news_live.fetch", return_value=[]):
                rows, source, freshness = news_parse.load_headlines(
                    hours=48, limit=40, date_str="2026-09-24")
        finally:
            os.chdir(cwd)
            news_parse.NEWS_DIR = orig
    assert source == "local_files:empty"
    assert freshness["ok"] is True
    assert rows
    assert all(r.get("scraped_at") == stamp for r in rows)
    parsed = news_parse.parse_rows(rows)
    assert parsed
    assert all(p.get("scraped_at") == stamp for p in parsed)


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
    test_finviz_scraped_at_dates_a_same_morning_export()
    test_old_finviz_scrape_is_still_stale()
    test_published_at_wins_over_a_fresh_scrape_stamp()
    test_decision_assesses_scraped_at_when_published_at_is_blank()
    test_local_finviz_rows_carry_the_export_stamp()
    test_sidecar_dates_a_digest_that_has_no_generated_at()
    test_grok_stale_news_is_a_hard_stop_map_heat_is_not()
    test_banner_names_none_stale()
    test_live_google_url_pins_when_2d()
    test_live_fetch_keeps_only_the_hours_window()
    test_catalyst_skips_news_actions_when_stale()
    print("16 tests passed")
