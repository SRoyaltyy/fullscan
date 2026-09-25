"""Live RSS read for Pre-Open when the news table window is empty or stale.

Does not insert into Postgres. The scheduled RSS / NewsAPI collectors have
been off since 2026-08-29, so a DB read cannot be the only way to see
this morning's headlines.

Google News search URLs get ``when:2d`` on this path only, so a stale
index page is not the whole window. Rows still have to pass
``news_freshness.assess`` before they become the packet.
"""
from __future__ import annotations

import socket
from datetime import datetime, timedelta, timezone
from typing import Callable

from .news_freshness import parse_published

# High-signal subset. rsshub / long-tail wires are skipped so a dead host
# cannot eat the 120s news_parse step.
LIVE_FEED_NAMES = (
    "rss_google_macro",
    "rss_google_markets",
    "rss_google_commodities",
    "rss_google_geopolitics",
    "rss_google_business",
    "rss_cnbc_finance",
    "rss_marketwatch_top",
    "rss_yahoo_finance",
    "rss_bbc_business",
    "rss_reuters_business",
)
_PER_FEED_S = 6


def live_url(url: str) -> str:
    """Bias Google News search RSS to the last 2 days."""
    if "news.google.com/rss/search?" not in url or "when:" in url:
        return url
    if "&hl=" in url:
        return url.replace("&hl=", "+when:2d&hl=", 1)
    return url + "+when:2d"


def _feeds() -> list[tuple[str, str]]:
    try:
        from collectors.rss_news import FEEDS
    except Exception as exc:  # noqa: BLE001
        print(f"[news_live] feed list unavailable: {exc}")
        return []
    out = []
    for name in LIVE_FEED_NAMES:
        url = FEEDS.get(name)
        if url:
            out.append((name, live_url(url)))
    return out


def fetch(limit: int = 400, hours: int = 48, *,
          now: datetime | None = None,
          parse_feed: Callable | None = None,
          feeds: list[tuple[str, str]] | None = None) -> list[dict]:
    """Headlines published inside ``hours``, newest first. No DB write."""
    parser = parse_feed
    if parser is None:
        import feedparser
        parser = feedparser.parse
    pairs = feeds if feeds is not None else _feeds()
    asof = now or datetime.now(timezone.utc)
    if asof.tzinfo is None:
        asof = asof.replace(tzinfo=timezone.utc)
    cutoff = asof.astimezone(timezone.utc) - timedelta(hours=max(1, int(hours)))
    rows: list[dict] = []
    seen: set[str] = set()
    prev = socket.getdefaulttimeout()
    socket.setdefaulttimeout(_PER_FEED_S)
    try:
        for name, url in pairs:
            if len(rows) >= limit:
                break
            try:
                feed = parser(url)
            except Exception as exc:  # noqa: BLE001
                print(f"[news_live] {name}: {exc}")
                continue
            entries = getattr(feed, "entries", None) or []
            kept = 0
            for entry in entries:
                if len(rows) >= limit:
                    break
                title = str(getattr(entry, "title", "") or "").strip()
                if hasattr(entry, "get"):
                    title = str(entry.get("title") or title).strip()
                    link = str(entry.get("link") or "").strip()
                    published = str(entry.get("published")
                                    or entry.get("updated") or "").strip()
                else:
                    link = str(getattr(entry, "link", "") or "").strip()
                    published = str(getattr(entry, "published", "")
                                    or getattr(entry, "updated", "") or "").strip()
                if not title:
                    continue
                dt = parse_published(published)
                if dt is None or dt < cutoff:
                    continue
                key = title.lower()
                if key in seen:
                    continue
                seen.add(key)
                rows.append({
                    "source": name,
                    "title": title,
                    "url": link,
                    "published_at": published,
                })
                kept += 1
            print(f"[news_live] {name}: {kept} inside {hours}h")
    finally:
        socket.setdefaulttimeout(prev)
    rows.sort(key=lambda r: parse_published(r.get("published_at"))
              or datetime.min.replace(tzinfo=timezone.utc), reverse=True)
    print(f"[news_live] {len(rows)} fresh headlines (no DB write)")
    return rows[:limit]
