"""
Collector: NewsAPI
Source: newsapi.org
Schedule: Every 6 hours (free tier = 100 requests/day, we use ~16)
Populates: news table
"""

import os
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import requests
from db.connection import get_connection

API_KEY = os.environ.get("NEWSAPI_KEY", "")
BASE_URL = "https://newsapi.org/v2/everything"

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

    conn = get_connection()
    cur = conn.cursor()

    total_new = 0

    for query in QUERIES:
        try:
            resp = requests.get(BASE_URL, params={
                "q": query,
                "language": "en",
                "sortBy": "publishedAt",
                "pageSize": 100,
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

                cur.execute("""
                    INSERT INTO news (source, title, url, summary, published_at)
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT (url) DO NOTHING
                """, (f"newsapi_{source_name}", title, url, summary, published))

                if cur.rowcount > 0:
                    new_count += 1

            total_new += new_count
            print(f"  Query '{query[:40]}...': {new_count} new articles")

        except Exception as e:
            conn.rollback()
            print(f"  [ERROR] Query '{query}': {e}")

    conn.commit()

    start = time.time()
    cur.execute(
        "INSERT INTO collection_log(collector, status, records_added) VALUES(%s,%s,%s)",
        ("news_newsapi", "ok", total_new),
    )
    conn.commit()

    cur.close()
    conn.close()
    print(f"[newsapi] Done. {total_new} new articles total.")


if __name__ == "__main__":
    collect()
