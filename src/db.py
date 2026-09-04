"""Optional Supabase (Postgres) access. Degrades gracefully if unavailable."""
from __future__ import annotations

from . import config


def _conn():
    if not config.DATABASE_URL:
        return None
    last = None
    for attempt in range(3):
        try:
            import psycopg2
            return psycopg2.connect(config.DATABASE_URL, connect_timeout=10)
        except Exception as e:  # noqa: BLE001
            last = e
            print(f"[db] connect failed (try {attempt + 1}/3): {e}")
            if attempt < 2:
                import time
                time.sleep(2 * (attempt + 1))
    print(f"[db] giving up — morning/collectors continue without Postgres ({last})")
    return None


def recent_news(hours: int = 24, limit: int = 30) -> list[dict]:
    """Last-N-hours rows from the `news` table (rss/newsapi collectors).
    Market-relevant sources are ranked first. Tries `collected_at` first,
    falls back to `published_at` ordering — the table schema comes from
    migrations, so be liberal."""
    conn = _conn()
    if conn is None:
        return []
    queries = [
        """SELECT source, title, url, published_at
           FROM news
           WHERE collected_at >= NOW() - INTERVAL '%s hours'
           ORDER BY CASE WHEN lower(source) ~
                '(market|financ|cnbc|macro|business|bloomberg|reuters|wsj|stock|econom)'
                THEN 0 ELSE 1 END,
                collected_at DESC
           LIMIT %s""",
        """SELECT source, title, url, published_at
           FROM news
           ORDER BY CASE WHEN lower(source) ~
                '(market|financ|cnbc|macro|business|bloomberg|reuters|wsj|stock|econom)'
                THEN 0 ELSE 1 END,
                published_at DESC
           LIMIT %s""",
    ]
    try:
        cur = conn.cursor()
        try:
            for i, q in enumerate(queries):
                try:
                    params = (hours, limit) if i == 0 else (limit,)
                    cur.execute(q, params)
                    rows = [{"source": s, "title": t, "url": u,
                             "published_at": str(p)}
                            for s, t, u, p in cur.fetchall()]
                    if rows or i == 1:
                        return rows
                except Exception as e:  # noqa: BLE001
                    conn.rollback()
                    print(f"[db] news query variant {i} failed: {e}")
            return []
        finally:
            try:
                cur.close()
            except Exception:
                pass
    finally:
        conn.close()


def macro_series(series_id: str, limit: int = 45) -> list[tuple[str, float]]:
    """Fallback FRED source: macro_indicators table written by macro_fred
    collector. Returns [(date, value)] ascending."""
    conn = _conn()
    if conn is None:
        return []
    try:
        cur = conn.cursor()
        cur.execute(
            """SELECT date, value FROM macro_indicators
               WHERE indicator = %s ORDER BY date DESC LIMIT %s""",
            (series_id, limit),
        )
        rows = [(str(d), float(v)) for d, v in cur.fetchall()]
        cur.close()
        return sorted(rows)
    except Exception as e:  # noqa: BLE001
        print(f"[db] macro query failed for {series_id}: {e}")
        return []
    finally:
        conn.close()


def news_between(
    start: str,
    end: str,
    limit: int = 2000,
) -> list[dict]:
    """News with published_at in [start, end) (ISO dates YYYY-MM-DD).

    published_at is TEXT in schema; collected_at is timestamptz.
    """
    conn = _conn()
    if conn is None:
        return []
    queries = [
        """SELECT source, title, url, published_at, collected_at
           FROM news
           WHERE published_at IS NOT NULL AND published_at <> ''
             AND published_at::timestamp >= %s::timestamp
             AND published_at::timestamp < %s::timestamp
           ORDER BY published_at::timestamp DESC
           LIMIT %s""",
        """SELECT source, title, url, published_at, collected_at
           FROM news
           WHERE published_at IS NOT NULL AND published_at <> ''
             AND published_at >= %s
             AND published_at < %s
           ORDER BY published_at DESC
           LIMIT %s""",
        """SELECT source, title, url, published_at, collected_at
           FROM news
           WHERE collected_at >= %s::timestamptz
             AND collected_at < %s::timestamptz
           ORDER BY collected_at DESC
           LIMIT %s""",
    ]
    try:
        cur = conn.cursor()
        try:
            for i, q in enumerate(queries):
                try:
                    cur.execute(q, (start, end, limit))
                    rows = [
                        {
                            "source": s,
                            "title": t,
                            "url": u,
                            "published_at": str(p) if p is not None else "",
                            "collected_at": str(c) if c is not None else "",
                        }
                        for s, t, u, p, c in cur.fetchall()
                    ]
                    if rows:
                        return rows
                except Exception as e:  # noqa: BLE001
                    conn.rollback()
                    print(f"[db] news_between variant {i} failed: {e}")
            return []
        finally:
            try:
                cur.close()
            except Exception:
                pass
    finally:
        conn.close()
