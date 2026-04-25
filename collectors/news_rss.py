"""
Collector: RSS News Feeds
Sources: Google News (business, markets, economy), CNBC, MarketWatch
Schedule: Every 30 minutes
Populates: news table

Uses feedparser to pull from multiple RSS feeds. Each feed is independent —
if one is down, the others still collect.
"""

import feedparser
from datetime import datetime
from db.db_utils import get_db, ensure_schema

# ── Feed registry ──────────────────────────────────────────────
# Add or remove feeds here. The key becomes the `source` column value.
FEEDS = {
    # Google News — business section
    "google_business": (
        "https://news.google.com/rss/topics/"
        "CAAqJggKIiBDQkFTRWdvSUwyMHZNRGx6TVdZU0FtVnVHZ0pWVXlnQVAB"
        "?hl=en-US&gl=US&ceid=US:en"
    ),
    # Google News — custom query: stock market, earnings, IPO
    "google_markets": (
        "https://news.google.com/rss/search"
        "?q=stock+market+OR+earnings+OR+IPO+OR+SEC+filing"
        "&hl=en-US&gl=US&ceid=US:en"
    ),
    # Google News — economy, Fed, inflation
    "google_macro": (
        "https://news.google.com/rss/search"
        "?q=Federal+Reserve+OR+inflation+OR+CPI+OR+interest+rate"
        "&hl=en-US&gl=US&ceid=US:en"
    ),
    # Google News — commodities
    "google_commodities": (
        "https://news.google.com/rss/search"
        "?q=crude+oil+price+OR+gold+price+OR+commodity+market"
        "&hl=en-US&gl=US&ceid=US:en"
    ),
    # Google News — geopolitics & world events
    "google_geopolitics": (
        "https://news.google.com/rss/search"
        "?q=sanctions+OR+tariffs+OR+geopolitical+OR+war+OR+summit"
        "&hl=en-US&gl=US&ceid=US:en"
    ),
    # CNBC Top News
    "cnbc_top": (
        "https://search.cnbc.com/rs/search/combinedcms/view.xml"
        "?partnerId=wrss01&id=100003114"
    ),
    # CNBC Finance
    "cnbc_finance": (
        "https://search.cnbc.com/rs/search/combinedcms/view.xml"
        "?partnerId=wrss01&id=10000664"
    ),
    # CNBC Economy
    "cnbc_economy": (
        "https://search.cnbc.com/rs/search/combinedcms/view.xml"
        "?partnerId=wrss01&id=20910258"
    ),
    # MarketWatch Top Stories
    "marketwatch_top": "http://feeds.marketwatch.com/marketwatch/topstories/",
    # MarketWatch Market Pulse
    "marketwatch_pulse": "http://feeds.marketwatch.com/marketwatch/marketpulse/",
}


def collect():
    print("[news_rss] Collecting from RSS feeds...")

    db = get_db()
    ensure_schema(db)
    cursor = db.cursor()

    total_new = 0
    total_errors = 0

    for source_name, feed_url in FEEDS.items():
        try:
            feed = feedparser.parse(feed_url)

            if feed.bozo and not feed.entries:
                print(f"  [WARN] {source_name}: feed parse error — {feed.bozo_exception}")
                total_errors += 1
                continue

            new_in_feed = 0
            for entry in feed.entries:
                title = (entry.get("title") or "").strip()
                link = (entry.get("link") or "").strip()
                summary = (entry.get("summary") or "")[:1000].strip()
                published = entry.get("published", "")

                if not title or not link:
                    continue

                cursor.execute("""
                    INSERT OR IGNORE INTO news
                        (source, title, url, summary, published_at)
                    VALUES (?, ?, ?, ?, ?)
                """, (f"rss_{source_name}", title, link, summary, published))

                if cursor.rowcount > 0:
                    new_in_feed += 1

            total_new += new_in_feed
            print(f"  {source_name}: {new_in_feed} new articles")

        except Exception as e:
            print(f"  [ERROR] {source_name}: {e}")
            total_errors += 1

    db.commit()
    db.close()
    print(f"[news_rss] Done. {total_new} new articles, {total_errors} feed errors.")


if __name__ == "__main__":
    collect()
