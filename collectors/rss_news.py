"""
RSS News Collector — curated, high-signal feeds only.
Filters out local TV stations, fluff, and non-actionable noise.
"""

import feedparser
import sqlite3
import os
import re
from datetime import datetime

DB_PATH = os.path.join(os.path.dirname(__file__), "..", "data", "pipeline.db")

# ─────────────────────────────────────────────────────────
# 1.  FEED LIST — authoritative & actionable sources only
# ─────────────────────────────────────────────────────────
FEEDS = {
    # ── CNBC (keep economy + finance, drop "top news" lifestyle stuff) ──
    "rss_cnbc_economy":      "https://www.cnbc.com/id/20910258/device/rss/rss.html",
    "rss_cnbc_finance":      "https://www.cnbc.com/id/10000664/device/rss/rss.html",
    "rss_cnbc_markets":      "https://www.cnbc.com/id/20910258/device/rss/rss.html",

    # ── MarketWatch ──
    "rss_marketwatch_pulse":  "https://feeds.marketwatch.com/marketwatch/marketpulse/",
    "rss_marketwatch_top":    "https://feeds.marketwatch.com/marketwatch/topstories/",

    # ── Reuters ──
    "rss_reuters_business":   "https://feeds.reuters.com/reuters/businessNews",
    "rss_reuters_markets":    "https://feeds.reuters.com/reuters/marketsNews",
    "rss_reuters_world":      "https://feeds.reuters.com/Reuters/worldNews",

    # ── AP News ──
    "rss_ap_topnews":         "https://rsshub.app/apnews/topics/apf-topnews",
    "rss_ap_business":        "https://rsshub.app/apnews/topics/apf-business",

    # ── BBC ──
    "rss_bbc_world":          "https://feeds.bbci.co.uk/news/world/rss.xml",
    "rss_bbc_business":       "https://feeds.bbci.co.uk/news/business/rss.xml",

    # ── Al Jazeera ──
    "rss_aljazeera_economy":  "https://www.aljazeera.com/xml/rss/all.xml",

    # ── DW (Deutsche Welle) ──
    "rss_dw_world":           "https://rss.dw.com/rdf/rss-en-world",
    "rss_dw_business":        "https://rss.dw.com/rdf/rss-en-bus",

    # ── Yahoo Finance ──
    "rss_yahoo_finance":      "https://finance.yahoo.com/news/rssindex",

    # ── Financial Times (headlines) ──
    "rss_ft_world":           "https://www.ft.com/rss/home/uk",

    # ── Google News — curated topic pages ──
    "rss_google_business":    "https://news.google.com/rss/topics/CAAqJggKIiBDQkFTRWdvSUwyMHZNRGx6TVdZU0FtVnVHZ0pWVXlnQVAB",
    "rss_google_markets":     "https://news.google.com/rss/search?q=stock+market+OR+earnings+OR+IPO&hl=en-US&gl=US&ceid=US:en",
    "rss_google_macro":       "https://news.google.com/rss/search?q=federal+reserve+OR+interest+rate+OR+inflation+OR+GDP&hl=en-US&gl=US&ceid=US:en",
    "rss_google_commodities": "https://news.google.com/rss/search?q=oil+price+OR+gold+price+OR+commodities+market&hl=en-US&gl=US&ceid=US:en",
    "rss_google_geopolitics": "https://news.google.com/rss/search?q=trade+war+OR+tariffs+OR+sanctions+OR+geopolitical+risk&hl=en-US&gl=US&ceid=US:en",
    "rss_google_world":       "https://news.google.com/rss/topics/CAAqJggKIiBDQkFTRWdvSUwyMHZNRGx1YlY4U0FtVnVHZ0pWVXlnQVAB",
    "rss_google_us":          "https://news.google.com/rss/topics/CAAqIggKIhxDQkFTRHdvSkwyMHZNRGxqTjNjd0VnSmxiaWdBUAE",
}


# ─────────────────────────────────────────────────────────
# 2.  SOURCE QUALITY FILTER
#     Google News aggregates from thousands of outlets.
#     We keep only high-quality publishers.
# ─────────────────────────────────────────────────────────

# Trusted publishers — if the article source or title suffix matches any
# of these (case-insensitive), the article is KEPT.
TRUSTED_PUBLISHERS = {
    # Wire services & majors
    "reuters", "associated press", "ap news", "bloomberg",
    "financial times", "the wall street journal", "wsj",
    "the new york times", "nyt", "the washington post",
    "bbc", "bbc news", "the guardian",
    "al jazeera", "dw.com", "deutsche welle",
    "the economist", "fortune", "forbes",

    # Business / finance
    "cnbc", "marketwatch", "yahoo finance", "yahoo entertainment",
    "barron's", "barrons", "investor's business daily",
    "tipranks", "seeking alpha", "the motley fool",
    "stock titan", "benzinga", "zacks",
    "morningstar", "investopedia",

    # Tech & industry
    "wired", "the verge", "techcrunch", "ars technica",
    "about amazon",  # Amazon's official blog

    # Government / institutional
    "federal reserve", "sec.gov", "treasury.gov",
    "imf", "world bank",

    # Quality regionals / internationals
    "the independent", "euronews", "euronews.com",
    "politico", "axios", "the hill",
    "south china morning post", "nikkei", "haaretz",
    "times of india", "hindustan times",
    "cnn", "abc news", "nbc news", "cbs news",
    "anadolu ajansı", "anadolu agency",

    # Other useful
    "the street", "thestreet.com", "thestreet",
    "upi.com", "upi",
}

# Patterns that signal a LOCAL TV/radio station → always reject
# These are FCC call signs: 3-4 uppercase letters starting with K or W
LOCAL_CALLSIGN = re.compile(
    r"^[KW][A-Z]{2,4}(-[A-Z]{2,3})?"   # KGNS, WBNG, KOLD-TV, etc.
    r"(\.com)?$",                         # optional .com
    re.IGNORECASE,
)

# Additional explicit blocklist for known noise sources
BLOCKED_PUBLISHERS = {
    "fox news", "fox business",           # editorialised / low signal
    "new york post",                       # tabloid
    "daily mail", "the sun",              # tabloid
    "newsmax", "oann", "breitbart",       # opinion-heavy
    "the coloradoan", "kansas city star",
    "north dakota athletics",
}


def is_quality_source(article_source: str, title: str) -> bool:
    """
    Decide whether an article passes the quality filter.

    article_source: the <source> tag from the RSS item (Google News provides this)
    title:          full title string (often ends with " - Publisher Name")
    """
    # Extract publisher from title suffix as fallback
    publisher_from_title = ""
    if " - " in title:
        publisher_from_title = title.rsplit(" - ", 1)[-1].strip()

    candidates = [
        article_source.strip().lower(),
        publisher_from_title.lower(),
    ]

    for name in candidates:
        if not name:
            continue

        # Explicit block?
        if name in BLOCKED_PUBLISHERS:
            return False
        for blocked in BLOCKED_PUBLISHERS:
            if blocked in name:
                return False

        # Local call-sign pattern?
        if LOCAL_CALLSIGN.match(name):
            return False

    # Check if ANY candidate matches a trusted publisher
    for name in candidates:
        if not name:
            continue
        for trusted in TRUSTED_PUBLISHERS:
            if trusted in name or name in trusted:
                return True

    # If article came from a non-Google feed (CNBC, Reuters, etc.)
    # it's already curated — let it through
    # Google feeds start with "rss_google_" — those need publisher filtering
    return True  # will be narrowed in collect() per-feed


def collect():
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    total_new = 0
    total_skipped = 0

    for feed_name, url in FEEDS.items():
        is_google_feed = feed_name.startswith("rss_google_")

        try:
            feed = feedparser.parse(url)
        except Exception as e:
            print(f"  ✗ {feed_name}: {e}")
            continue

        new_count = 0
        skip_count = 0

        for entry in feed.entries:
            title = entry.get("title", "").strip()
            link = entry.get("link", "").strip()
            summary = entry.get("summary", entry.get("description", "")).strip()
            published = entry.get("published", entry.get("updated", "")).strip()

            # Google News provides a <source> tag
            article_source = ""
            if hasattr(entry, "source") and hasattr(entry.source, "title"):
                article_source = entry.source.title

            if not title or not link:
                continue

            # ── Quality gate (strict for Google feeds) ──
            if is_google_feed:
                if not is_quality_source(article_source, title):
                    skip_count += 1
                    continue

            # ── Insert (ignore duplicates via UNIQUE url) ──
            try:
                cur.execute(
                    """INSERT OR IGNORE INTO news
                       (source, title, url, summary, published_at, related_tickers, sentiment_score)
                       VALUES (?, ?, ?, ?, ?, NULL, NULL)""",
                    (feed_name, title, link, summary, published),
                )
                if cur.rowcount:
                    new_count += 1
            except Exception as e:
                print(f"    DB error: {e}")

        total_new += new_count
        total_skipped += skip_count
        status = f"{new_count} new"
        if skip_count:
            status += f", {skip_count} filtered out"
        print(f"  ✓ {feed_name}: {status}")

    conn.commit()
    conn.close()
    print(f"\nDone — {total_new} new articles, {total_skipped} filtered out total.")


if __name__ == "__main__":
    print("=" * 50)
    print("RSS News Collector (curated)")
    print("=" * 50)
    collect()
