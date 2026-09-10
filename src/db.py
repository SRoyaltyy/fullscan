"""Optional Supabase (Postgres) access. Degrades gracefully if unavailable."""
from __future__ import annotations

import os
import tempfile
import threading
import time

from . import config

# 2026-09-08: variant-1 full-table CASE regex scan hit statement_timeout
# (~5.5 min) and news_parse wrote empty → judge/actions cascade miss.
# 2026-09-09: 90s × 3 variants × 3 retries still ate the 480s parse slot
# (exit 124) so judge/actions never started. Morning default is 20s,
# jump to last-N LIMIT after the first timeout, retry once.
_DEFAULT_STATEMENT_TIMEOUT_MS = 20_000


class NewsDbError(RuntimeError):
    """News table read failed after retries. reason is a QC token."""

    def __init__(self, reason: str, detail: str = "") -> None:
        self.reason = reason
        super().__init__(detail or reason)


def _statement_timeout_ms() -> int:
    raw = (os.environ.get("FULLSCAN_DB_STATEMENT_TIMEOUT_MS") or "").strip()
    if raw.isdigit():
        return max(5_000, int(raw))
    return _DEFAULT_STATEMENT_TIMEOUT_MS


def _is_timeout(err: BaseException) -> bool:
    text = f"{type(err).__name__} {err}".lower()
    return ("statement timeout" in text or "querycanceled" in text
            or "due to user request" in text)


# 2026-09-09 17:13 News Actions: the Supabase transaction pooler ignores
# the startup `options=-c statement_timeout=...`, so the "20s" queries ran
# to the server default (~2.5 min each) and 4 of them ate 11 minutes before
# the job went red. Enforce the budget from our side of the wire too:
# SET LOCAL inside the query's own transaction (transaction pooling keeps
# the backend for the transaction) plus a client-side cancel a few seconds
# later in case even that is filtered.
_CANCEL_GRACE_S = 5.0


def _execute_bounded(conn, cur, sql: str, params=None) -> None:
    timeout_ms = _statement_timeout_ms()
    try:
        cur.execute(f"SET LOCAL statement_timeout = {int(timeout_ms)}")
    except Exception as e:  # noqa: BLE001
        # Not fatal — the cancel timer below is the real backstop.
        print(f"[db] SET LOCAL statement_timeout ignored: {str(e).splitlines()[0][:120]}")
        try:
            conn.rollback()
        except Exception:  # noqa: BLE001
            pass
    cancel = getattr(conn, "cancel", None)
    timer: threading.Timer | None = None
    if callable(cancel):
        def _fire() -> None:
            try:
                cancel()
            except Exception:  # noqa: BLE001
                pass
        timer = threading.Timer(timeout_ms / 1000.0 + _CANCEL_GRACE_S, _fire)
        timer.daemon = True
        timer.start()
    try:
        cur.execute(sql, params)
    finally:
        if timer is not None:
            timer.cancel()


# 2026-09-09 17:17 Stock Book ALL: the Supabase pooler resolved to two
# addresses and each connect_timeout expired on both → 2 tries × 2 hosts
# × 8s + sleep ≈ 44s inside a 50s weather budget, paid again by every
# subprocess of the run. Once the pooler is unreachable, remember it for
# the rest of this process AND (via a marker file) the rest of the run.
_DOWN_MARK = os.path.join(tempfile.gettempdir(), "fullscan_db_unreachable")
_down_in_proc: str = ""


def _down_ttl_s() -> int:
    raw = (os.environ.get("FULLSCAN_DB_DOWN_TTL_S") or "").strip()
    if raw.isdigit():
        return int(raw)
    return 600


def _is_unreachable(err: BaseException) -> bool:
    text = str(err).lower()
    return any(tok in text for tok in (
        "timeout expired", "could not connect", "connection refused",
        "could not translate host name", "network is unreachable",
        "no route to host", "name or service not known",
    ))


def _marked_down() -> str:
    """Reason string when the DB was recently unreachable, else ''."""
    if _down_in_proc:
        return _down_in_proc
    ttl = _down_ttl_s()
    if ttl <= 0:
        return ""
    try:
        st = os.stat(_DOWN_MARK)
        age = time.time() - st.st_mtime
        if 0 <= age < ttl:
            return f"marker {int(age)}s old"
    except OSError:
        pass
    return ""


def _mark_down(reason: str) -> None:
    global _down_in_proc
    _down_in_proc = reason or "unreachable"
    if _down_ttl_s() <= 0:
        return
    try:
        with open(_DOWN_MARK, "w", encoding="utf-8") as fh:
            fh.write(f"{time.time():.0f} {reason}\n")
    except OSError:
        pass


def _conn():
    if not config.DATABASE_URL:
        return None
    why = _marked_down()
    if why:
        print(f"[db] skipped — pooler unreachable earlier this run ({why}); "
              "continuing without Postgres")
        return None
    last = None
    timeout_ms = _statement_timeout_ms()
    for attempt in range(2):
        try:
            import psycopg2
            conn = psycopg2.connect(
                config.DATABASE_URL,
                connect_timeout=6,
                options=f"-c statement_timeout={timeout_ms}",
            )
            return conn
        except Exception as e:  # noqa: BLE001
            last = e
            print(f"[db] connect failed (try {attempt + 1}/2): {e}")
            if _is_unreachable(e):
                # A second dial into a dead pooler is another 12s for nothing.
                _mark_down(str(e).splitlines()[0][:160])
                break
            if attempt < 1:
                time.sleep(2)
    print(f"[db] giving up — morning/collectors continue without Postgres ({last})")
    return None


def _recent_news_once(hours: int, limit: int) -> list[dict]:
    """One connection, bounded variants. Raises NewsDbError on timeout."""
    conn = _conn()
    if conn is None:
        return []
    # Variant 0: collected_at window, no per-row regex (that scan timed out).
    # Variant 1: published_at window (text column; still bounded).
    # Variant 2: last-N rows only — last resort, still LIMIT, no full sort regex.
    queries = [
        ("""SELECT source, title, url, published_at
            FROM news
            WHERE collected_at >= NOW() - (%s * INTERVAL '1 hour')
            ORDER BY collected_at DESC
            LIMIT %s""", (hours, limit)),
        ("""SELECT source, title, url, published_at
            FROM news
            WHERE published_at IS NOT NULL AND published_at <> ''
              AND published_at::timestamptz >= NOW() - (%s * INTERVAL '1 hour')
            ORDER BY published_at::timestamptz DESC
            LIMIT %s""", (hours, limit)),
        ("""SELECT source, title, url, published_at
            FROM news
            ORDER BY collected_at DESC NULLS LAST
            LIMIT %s""", (limit,)),
    ]
    last_err: BaseException | None = None
    saw_timeout = False
    try:
        cur = conn.cursor()
        try:
            i = 0
            while i < len(queries):
                q, params = queries[i]
                try:
                    _execute_bounded(conn, cur, q, params)
                    rows = [{"source": s, "title": t, "url": u,
                             "published_at": str(p)}
                            for s, t, u, p in cur.fetchall()]
                    if rows:
                        return rows
                except Exception as e:  # noqa: BLE001
                    last_err = e
                    conn.rollback()
                    print(f"[db] news query variant {i} failed: {e}")
                    if _is_timeout(e):
                        saw_timeout = True
                        if i < len(queries) - 1:
                            print("[db] statement_timeout — jumping to last-N LIMIT")
                            i = len(queries) - 1
                            continue
                        break
                i += 1
            if saw_timeout:
                raise NewsDbError(
                    "db_timeout",
                    f"news query statement_timeout after variants ({last_err})")
            return []
        finally:
            try:
                cur.close()
            except Exception:
                pass
    finally:
        conn.close()


def recent_news(hours: int = 24, limit: int = 30) -> list[dict]:
    """Last-N-hours rows from the `news` table (rss/newsapi collectors).

    Bounded time window + session statement_timeout. Retries timeouts
    with backoff. Raises NewsDbError(db_timeout) if every attempt dies
    so news_parse can fail loud instead of writing empty_parse.
    """
    last: NewsDbError | None = None
    for attempt in range(2):
        try:
            return _recent_news_once(hours, limit)
        except NewsDbError as e:
            last = e
            print(f"[db] recent_news {e.reason} (try {attempt + 1}/2): {e}")
            if attempt < 1:
                time.sleep(2)
    if last is not None:
        raise last
    return []


def macro_series(series_id: str, limit: int = 45) -> list[tuple[str, float]]:
    """Fallback FRED source: macro_indicators table written by macro_fred
    collector. Returns [(date, value)] ascending."""
    conn = _conn()
    if conn is None:
        return []
    try:
        cur = conn.cursor()
        _execute_bounded(
            conn, cur,
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
                    _execute_bounded(conn, cur, q, (start, end, limit))
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
