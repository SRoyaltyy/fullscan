"""
RSS News Collector — curated, high-signal feeds only.
Filters out local TV stations, fluff, and non-actionable noise.
"""

import os
import sys
import re
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import feedparser
from db.connection import get_connection

# ─────────────────────────────────────────────────────────
# 1.  FEED LIST — authoritative & actionable sources only
# ─────────────────────────────────────────────────────────
FEEDS = {
    "rss_cnbc_economy":      "https://www.cnbc.com/id/20910258/device/rss/rss.html",
    "rss_cnbc_finance":      "https://www.cnbc.com/id/10000664/device/rss/rss.html",
    "rss_cnbc_markets":      "https://www.cnbc.com/id/20910258/device/rss/rss.html",
    "rss_marketwatch_pulse":  "https://feeds.marketwatch.com/marketwatch/marketpulse/",
    "rss_marketwatch_top":    "https://feeds.marketwatch.com/marketwatch/topstories/",
    "rss_reuters_business":   "https://feeds.reuters.com/reuters/businessNews",
    "rss_reuters_markets":    "https://feeds.reuters.com/reuters/marketsNews",
    "rss_reuters_world":      "https://feeds.reuters.com/Reuters/worldNews",
    "rss_ap_topnews":         "https://rsshub.app/apnews/topics/apf-topnews",
    "rss_ap_business":        "https://rsshub.app/apnews/topics/apf-business",
    "rss_bbc_world":          "https://feeds.bbci.co.uk/news/world/rss.xml",
    "rss_bbc_business":       "https://feeds.bbci.co.uk/news/business/rss.xml",
    "rss_aljazeera_economy":  "https://www.aljazeera.com/xml/rss/all.xml",
    "rss_dw_world":           "https://rss.dw.com/rdf/rss-en-world",
    "rss_dw_business":        "https://rss.dw.com/rdf/rss-en-bus",
    "rss_yahoo_finance":      "https://finance.yahoo.com/news/rssindex",
    "rss_ft_world":           "https://www.ft.com/rss/home/uk",
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
# ─────────────────────────────────────────────────────────
TRUSTED_PUBLISHERS = {
    "reuters", "associated press", "ap news", "bloomberg",
    "financial times", "the wall street journal", "wsj",
    "the new york times", "nyt", "the washington post",
    "bbc", "bbc news", "the guardian",
    "al jazeera", "dw.com", "deutsche welle",
    "the economist", "fortune", "forbes",
    "cnbc", "marketwatch", "yahoo finance", "yahoo entertainment",
    "barron's", "barrons", "investor's business daily",
    "tipranks", "seeking alpha", "the motley fool",
    "stock titan", "benzinga", "zacks",
    "morningstar", "investopedia",
    "wired", "the verge", "techcrunch", "ars technica",
    "about amazon",
    "federal reserve", "sec.gov", "treasury.gov",
    "imf", "world bank",
    "the independent", "euronews", "euronews.com",
    "politico", "axios", "the hill",
    "south china morning post", "nikkei", "haaretz",
    "times of india", "hindustan times",
    "cnn", "abc news", "nbc news", "cbs news",
    "anadolu ajansı", "anadolu agency",
    "the street", "thestreet.com", "thestreet",
    "upi.com", "upi",
}

LOCAL_CALLSIGN = re.compile(
    r"^[KW][A-Z]{2,4}(-[A-Z]{2,3})?"
    r"(\.com)?$",
    re.IGNORECASE,
)

BLOCKED_PUBLISHERS = {
    "fox news", "fox business",
    "new york post",
    "daily mail", "the sun",
    "newsmax", "oann", "breitbart",
    "the coloradoan", "kansas city star",
    "north dakota athletics",
}


def is_quality_source(article_source: str, title: str) -> bool:
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
        if name in BLOCKED_PUBLISHERS:
            return False
        for blocked in BLOCKED_PUBLISHERS:
            if blocked in name:
                return False
        if LOCAL_CALLSIGN.match(name):
            return False

    for name in candidates:
        if not name:
            continue
        for trusted in TRUSTED_PUBLISHERS:
            if trusted in name or name in trusted:
                return True

    return True


def collect():
    conn = get_connection()
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

            article_source = ""
            if hasattr(entry, "source") and hasattr(entry.source, "title"):
                article_source = entry.source.title

            if not title or not link:
                continue

            if is_google_feed:
                if not is_quality_source(article_source, title):
                    skip_count += 1
                    continue

            try:
                cur.execute(
                    """INSERT INTO news
                       (source, title, url, summary, published_at)
                       VALUES (%s, %s, %s, %s, %s)
                       ON CONFLICT (url) DO NOTHING""",
                    (feed_name, title, link, summary, published),
                )
                if cur.rowcount > 0:
                    new_count += 1
            except Exception as e:
                conn.rollback()
                print(f"    DB error: {e}")

        total_new += new_count
        total_skipped += skip_count
        status = f"{new_count} new"
        if skip_count:
            status += f", {skip_count} filtered out"
        print(f"  ✓ {feed_name}: {status}")

    conn.commit()
    cur.close()
    conn.close()
    print(f"\nDone — {total_new} new articles, {total_skipped} filtered out total.")


if __name__ == "__main__":
    print("=" * 50)
    print("RSS News Collector (curated)")
    print("=" * 50)
    collect()
