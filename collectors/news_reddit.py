"""
Collector: Reddit
Sources: r/stocks, r/valueinvesting, r/investing, r/wallstreetbets, etc.
Schedule: Every hour
Populates: news table
"""

import os
import sys
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import praw
from db.connection import get_connection

CLIENT_ID = os.environ.get("REDDIT_CLIENT_ID", "")
CLIENT_SECRET = os.environ.get("REDDIT_CLIENT_SECRET", "")

SUBREDDITS = [
    ("stocks", 30),
    ("valueinvesting", 25),
    ("investing", 25),
    ("wallstreetbets", 30),
    ("stockmarket", 20),
    ("options", 15),
    ("SecurityAnalysis", 15),
    ("economics", 15),
    ("FluentInFinance", 15),
]


def collect():
    if not CLIENT_ID or not CLIENT_SECRET:
        print("[reddit] REDDIT_CLIENT_ID / REDDIT_CLIENT_SECRET not set — skipping.")
        return

    print("[reddit] Collecting from Reddit...")

    reddit = praw.Reddit(
        client_id=CLIENT_ID,
        client_secret=CLIENT_SECRET,
        user_agent="StockCatalystEngine/1.0 (by u/your_username)"
    )

    conn = get_connection()
    cur = conn.cursor()

    total_new = 0

    for sub_name, limit in SUBREDDITS:
        try:
            subreddit = reddit.subreddit(sub_name)

            new_count = 0
            for post in subreddit.hot(limit=limit):
                url = f"https://reddit.com{post.permalink}"
                title = (post.title or "").strip()

                selftext = (post.selftext or "")[:800]
                summary = (
                    f"[Score: {post.score} | Comments: {post.num_comments}] "
                    f"{selftext}"
                )[:1000]

                pub_dt = datetime.fromtimestamp(
                    post.created_utc, tz=timezone.utc
                ).isoformat()

                if not title:
                    continue

                cur.execute("""
                    INSERT INTO news (source, title, url, summary, published_at)
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT (url) DO NOTHING
                """, (f"reddit_r/{sub_name}", title, url, summary, pub_dt))

                if cur.rowcount > 0:
                    new_count += 1

            total_new += new_count
            print(f"  r/{sub_name}: {new_count} new posts")

        except Exception as e:
            conn.rollback()
            print(f"  [ERROR] r/{sub_name}: {e}")

    conn.commit()

    cur.execute(
        "INSERT INTO collection_log(collector, status, records_added) VALUES(%s,%s,%s)",
        ("news_reddit", "ok", total_new),
    )
    conn.commit()

    cur.close()
    conn.close()
    print(f"[reddit] Done. {total_new} new posts total.")


if __name__ == "__main__":
    collect()
