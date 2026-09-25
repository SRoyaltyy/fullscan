"""Harvest + rank tests for grok_automations as a first-class source."""
from __future__ import annotations

import json
import tempfile
from pathlib import Path

from src.news_impact.classify import rank_articles
from src.news_impact.corpus import dedupe_titles, inventory, load_all_sources
from src.news_impact.grok_automations import (
    SOURCE,
    counts,
    is_macro_only,
    load_grok_dumps,
    parse_connector_results,
    parse_gmail_plaintext,
    write_dump,
)
from src.news_impact.mix import mix_book
from src.news_impact.pipeline import analyze_article


def test_load_reads_committed_json() -> None:
    rows = load_grok_dumps()
    assert len(rows) >= 157, "Gmail dump tree must be readable"
    assert all(r.get("harvest_source") == SOURCE for r in rows)
    assert all(r.get("source") == SOURCE for r in rows)
    slugs = {r.get("automation_slug") for r in rows}
    assert {"hype-factor", "google-news-prompt", "13-questions", "news-parsing"} <= slugs
    q13 = [r for r in rows if r.get("automation_slug") == "13-questions"]
    assert len(q13) >= 39
    assert q13[0]["macro_only"] is True
    assert is_macro_only(art=q13[0]) is True
    # Gmail schema: headline + optional published=null still harvests
    assert any(not r.get("published_at") for r in rows)


def test_empty_dir_does_not_crash() -> None:
    with tempfile.TemporaryDirectory() as td:
        rows = load_grok_dumps(Path(td))
        assert rows == []
    missing = Path(tempfile.gettempdir()) / "grok_automations_missing_dir_xyz"
    assert not missing.exists()
    assert load_grok_dumps(missing) == []


def test_inventory_lists_grok_after_wire() -> None:
    inv = inventory()
    names = {s["name"]: s for s in inv["sources"]}
    grok = names["grok_automations"]
    meta = counts()
    assert grok["n_files"] >= 157
    assert grok["status"] == "used"
    assert grok.get("n_items", 0) >= 157
    assert meta["n_items"] >= 157
    assert grok.get("earliest") == "2026-08-13"
    assert "Automations API" in (grok.get("note") or "")


def test_automations_headline_ranks_over_finviz_wrap() -> None:
    title = "Same fact: Tokenized Securities Venues get temporary exemptive relief"
    auto = {
        "title": title,
        "harvest_source": SOURCE,
        "source": SOURCE,
        "usable": True,
        "published_at": "2026-09-17T08:00:00-04:00",
        "classification": {"event_class": "market_structure", "q5": "impulse"},
        "entities": [{"ticker": "COIN", "direction": "up", "tradeable_expression": "direct"}],
        "performance": [{
            "ticker": "COIN", "kind": "ticker", "direction": "up",
            "tradeable_expression": "direct", "event_class": "market_structure",
            "q5": "impulse", "entry_date": "2026-09-17",
            "ret_1d": 2.0, "agree_1d": True,
        }],
    }
    wrap = {
        "title": title,
        "harvest_source": "finviz_export",
        "source": "finviz_export",
        "usable": True,
        "published_at": "2026-09-17T08:00:00-04:00",
        "classification": {"event_class": "market_structure", "q5": "impulse"},
        "entities": [{"ticker": "COIN", "direction": "down", "tradeable_expression": "direct"}],
        "performance": [{
            "ticker": "COIN", "kind": "ticker", "direction": "down",
            "tradeable_expression": "direct", "event_class": "market_structure",
            "q5": "impulse", "entry_date": "2026-09-17",
            "ret_1d": -1.0, "agree_1d": False,
        }],
    }
    ranked = rank_articles([wrap, auto])
    assert ranked[0]["harvest_source"] == SOURCE
    uniq = dedupe_titles([wrap, auto])
    assert len(uniq) == 1
    assert uniq[0]["harvest_source"] == SOURCE
    mix = mix_book([auto, wrap])
    assert mix["conflict_n"] == 0, mix
    rec = (mix["converge"] or mix["singleton"])[0]
    assert rec["direction"] == "up"


def test_thirteen_questions_is_macro_not_tickers() -> None:
    art = {
        "title": "What does a hotter-than-expected CPI print do to duration and the dollar?",
        "body": "Macro Q. NVDA and AAPL are named in the prompt only as examples.",
        "task_id": "7f250154-6401-4836-b5ff-5862211c5468",
        "automation_slug": "13-questions",
        "macro_only": True,
        "source": SOURCE,
        "harvest_source": SOURCE,
    }
    row = analyze_article(art, persist=False)
    ev = (row.get("classification") or {}).get("event_class")
    assert ev in {"factor_impulse", "regime_state", "discard", "rumor"}, row
    ticks = {e.get("ticker") for e in row.get("entities") or [] if e.get("ticker")}
    assert "NVDA" not in ticks
    assert "AAPL" not in ticks
    for e in row.get("entities") or []:
        if e.get("ticker"):
            assert e.get("tradeable_expression") == "proxy"


def test_ingest_gmail_and_connector_roundtrip() -> None:
    mail = (
        "Subject: Automation: hype-factor\n\n"
        "Headline: Widget Corp raises fiscal 2026 guidance\n"
        "Source: example.com\n"
        "Published: 2026-09-18T10:00:00-04:00\n"
        "raw_excerpt: guidance lift\n"
    )
    dump = parse_gmail_plaintext(
        mail, subject="Automation: hype-factor", retrieved="2026-09-21T16:00:00Z",
    )
    assert dump["slug"] == "hype-factor"
    assert dump["items"][0]["headline"].startswith("Widget Corp")
    conn = parse_connector_results({
        "task_id": "5b4f01c3-fe5b-463a-a270-8bc9def8e26f",
        "slug": "google-news-prompt",
        "retrieved": "2026-09-21T16:00:00Z",
        "items": [{
            "headline": "EIA crude inventory build surprises",
            "source": "eia.gov",
            "published": None,
            "retrieved": "2026-09-21T16:00:00Z",
            "task_id": "5b4f01c3-fe5b-463a-a270-8bc9def8e26f",
            "raw_excerpt": "build",
        }],
    })
    assert conn["slug"] == "google-news-prompt"
    assert len(conn["items"]) == 1
    with tempfile.TemporaryDirectory() as td:
        path = write_dump(conn, date="2026-09-21", root=Path(td))
        assert path.name == "2026-09-21_google-news-prompt.json"
        blob = json.loads(path.read_text(encoding="utf-8"))
        assert blob["items"][0]["published"] is None
        rows = load_grok_dumps(Path(td))
        assert len(rows) == 1
        assert rows[0]["title"].startswith("EIA crude")
        assert rows[0]["harvest_source"] == SOURCE


def test_load_all_sources_includes_grok() -> None:
    raw, meta = load_all_sources("2026-08-20")
    by = meta.get("by_harvest_source") or {}
    assert by.get(SOURCE, 0) >= 1
    assert any((a.get("harvest_source") == SOURCE) for a in raw)


def main() -> None:
    tests = [
        test_load_reads_committed_json,
        test_empty_dir_does_not_crash,
        test_inventory_lists_grok_after_wire,
        test_automations_headline_ranks_over_finviz_wrap,
        test_thirteen_questions_is_macro_not_tickers,
        test_ingest_gmail_and_connector_roundtrip,
        test_load_all_sources_includes_grok,
    ]
    failed = 0
    for fn in tests:
        try:
            fn()
            print(f"ok  {fn.__name__}")
        except Exception as exc:  # noqa: BLE001
            failed += 1
            print(f"FAIL {fn.__name__}: {exc}")
    if failed:
        raise SystemExit(f"{failed} test(s) failed")
    print(f"{len(tests)} tests passed")


if __name__ == "__main__":
    main()
