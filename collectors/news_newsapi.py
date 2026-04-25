"""
Collector: NewsAPI
Source: newsapi.org
Schedule: Every 6 hours (free tier = 100 requests/day, we use ~16)
Populates: news table

NewsAPI aggregates from 80,000+ sources. We run targeted queries to stay
within the free tier's 100 requests/day limit.
"""

import requests
import os
import sys
from db.db_utils import get_db, ensure_schema

API_KEY = os.environ.get("NEWSAPI_KEY", "")
BASE_URL = "https://newsapi.org/v2/everything"

# Each query uses 1 request. 4 queries × 4 runs/day = 16 requests/day.
# Leaves 84 requests/day of headroom.
QUERIES = [
    "stock market earnings revenue",
    "Federal Reserve interest rate inflation CPI",
    "merger acquisition IPO offering",
    "oil gold commodity tariff sanctions",
]


def collect():
    if not API_KEY:
        print("[newsapi] NEWSAPI_KEY not set — skipping.")
        return

    print("[newsapi] Collecting from NewsAPI...")

    db = get_db()
    ensure_schema(db)
    cursor = db.cursor()

    total_new = 0

    for query in QUERIES:
        try:
            resp = requests.get(BASE_URL, params={
                "q": query,
                "language": "en",
                "sortBy": "publishedAt",
                "pageSize": 100,          # max per request on free tier
                "apiKey": API_KEY,
            }, timeout=20)

            data = resp.json()

            if data.get("status") != "ok":
                print(f"  [WARN] Query '{query}': {data.get('message', 'unknown error')}")
                continue

            new_count = 0
            for article in data.get("articles", []):
                source_name = article.get("source", {}).get("name", "unknown")
                title = (article.get("title") or "").strip()
                url = (article.get("url") or "").strip()
                summary = (article.get("description") or "")[:1000].strip()
                published = article.get("publishedAt", "")

                if not title or not url or title == "[Removed]":
                    continue

                cursor.execute("""
                    INSERT OR IGNORE INTO news
                        (source, title, url, summary, published_at)
                    VALUES (?, ?, ?, ?, ?)
                """, (f"newsapi_{source_name}", title, url, summary, published))

                if cursor.rowcount > 0:
                    new_count += 1

            total_new += new_count
            print(f"  Query '{query[:40]}...': {new_count} new articles")

        except Exception as e:
            print(f"  [ERROR] Query '{query}': {e}")

    db.commit()
    db.close()
    print(f"[newsapi] Done. {total_new} new articles total.")


if __name__ == "__main__":
    collect()
