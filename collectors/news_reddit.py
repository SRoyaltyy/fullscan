"""
Collector: Reddit
Sources: r/stocks, r/valueinvesting, r/investing, r/wallstreetbets, etc.
Schedule: Every hour
Populates: news table

Uses PRAW (Python Reddit API Wrapper) to pull hot/top posts.
Reddit's API rate limit is 100 requests/minute with OAuth — we use far less.
"""

import praw
import os
from datetime import datetime, timezone
from db.db_utils import get_db, ensure_schema

CLIENT_ID = os.environ.get("REDDIT_CLIENT_ID", "")
CLIENT_SECRET = os.environ.get("REDDIT_CLIENT_SECRET", "")

SUBREDDITS = [
    ("stocks", 30),             # (subreddit_name, num_posts_to_fetch)
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

    db = get_db()
    ensure_schema(db)
    cursor = db.cursor()

    total_new = 0

    for sub_name, limit in SUBREDDITS:
        try:
            subreddit = reddit.subreddit(sub_name)

            new_count = 0
            for post in subreddit.hot(limit=limit):
                url = f"https://reddit.com{post.permalink}"
                title = (post.title or "").strip()

                # Build a useful summary: score, comment count, and post text
                selftext = (post.selftext or "")[:800]
                summary = (
                    f"[Score: {post.score} | Comments: {post.num_comments}] "
                    f"{selftext}"
                )[:1000]

                # Convert Unix timestamp to ISO format
                pub_dt = datetime.fromtimestamp(
                    post.created_utc, tz=timezone.utc
                ).isoformat()

                if not title:
                    continue

                cursor.execute("""
                    INSERT OR IGNORE INTO news
                        (source, title, url, summary, published_at)
                    VALUES (?, ?, ?, ?, ?)
                """, (f"reddit_r/{sub_name}", title, url, summary, pub_dt))

                if cursor.rowcount > 0:
                    new_count += 1

            total_new += new_count
            print(f"  r/{sub_name}: {new_count} new posts")

        except Exception as e:
            print(f"  [ERROR] r/{sub_name}: {e}")

    db.commit()
    db.close()
    print(f"[reddit] Done. {total_new} new posts total.")


if __name__ == "__main__":
    collect()
