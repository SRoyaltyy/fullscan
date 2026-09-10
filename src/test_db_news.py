"""News DB cursor must stay open across empty-then-fallback variants.

Run: python -m src.test_db_news
"""
from __future__ import annotations

from src import db


class _Cur:
    def __init__(self) -> None:
        self.closed = False
        self.q = ""

    def execute(self, q, params=None):
        if self.closed:
            raise RuntimeError("cursor already closed")
        self.q = q

    def fetchall(self):
        if "collected_at" in self.q:
            return []
        return [("CNBC", "Warsh", "https://example.com/n", "2026-09-02")]

    def close(self) -> None:
        self.closed = True


class _Conn:
    def __init__(self) -> None:
        self.cur = _Cur()

    def cursor(self) -> _Cur:
        return self.cur

    def rollback(self) -> None:
        return None

    def close(self) -> None:
        return None


def test_recent_news_empty_first_variant_still_reads_second(monkeypatch=None) -> None:
    orig = db._conn
    db._conn = lambda: _Conn()  # type: ignore[method-assign]
    try:
        rows = db.recent_news(hours=48, limit=10)
        assert rows
        assert rows[0]["title"] == "Warsh"
    finally:
        db._conn = orig


def test_recent_news_timeout_jumps_to_last_limit() -> None:
    n = {"n": 0}

    class _Cur:
        def execute(self, q, params=None):
            if q.startswith("SET LOCAL"):
                return
            n["n"] += 1
            raise RuntimeError("canceling statement due to statement timeout")

        def fetchall(self):
            return []

        def close(self) -> None:
            return None

    class _Conn:
        def cursor(self) -> _Cur:
            return _Cur()

        def rollback(self) -> None:
            return None

        def close(self) -> None:
            return None

    orig = db._conn
    orig_sleep = db.time.sleep
    db._conn = lambda: _Conn()  # type: ignore[method-assign]
    db.time.sleep = lambda *_a, **_k: None  # type: ignore[method-assign]
    try:
        try:
            db.recent_news(hours=48, limit=10)
            raise AssertionError("expected NewsDbError")
        except db.NewsDbError as e:
            assert e.reason == "db_timeout"
        # 2 attempts × (first variant + last-N LIMIT), not 3×3.
        assert n["n"] == 4
    finally:
        db._conn = orig
        db.time.sleep = orig_sleep


def test_unreachable_pooler_dials_once_then_is_remembered() -> None:
    """09-09: 2 tries × 2 hosts × 8s ate the 50s weather budget per process."""
    import os
    import sys
    import tempfile
    import types

    dials = {"n": 0}

    def _connect(*_a, **kw):
        dials["n"] += 1
        assert kw.get("connect_timeout", 99) <= 6
        raise RuntimeError(
            'connection to server at "pooler" (1.2.3.4), port 5432 failed: '
            "timeout expired")

    fake = types.ModuleType("psycopg2")
    fake.connect = _connect  # type: ignore[attr-defined]
    orig_mod = sys.modules.get("psycopg2")
    orig_url = db.config.DATABASE_URL
    orig_mark = db._DOWN_MARK
    orig_sleep = db.time.sleep
    orig_env = os.environ.get("FULLSCAN_DB_DOWN_TTL_S")
    sys.modules["psycopg2"] = fake
    db.time.sleep = lambda *_a, **_k: None
    db._down_in_proc = ""
    try:
        with tempfile.TemporaryDirectory() as tmp:
            db._DOWN_MARK = os.path.join(tmp, "mark")
            db.config.DATABASE_URL = "postgresql://x"
            os.environ.pop("FULLSCAN_DB_DOWN_TTL_S", None)
            assert db._conn() is None
            assert dials["n"] == 1          # no second dial into a dead pooler
            assert db._conn() is None
            assert dials["n"] == 1          # remembered in-process
            db._down_in_proc = ""           # a fresh subprocess ...
            assert db._conn() is None
            assert dials["n"] == 1          # ... reads the run marker
            os.environ["FULLSCAN_DB_DOWN_TTL_S"] = "0"
            db._down_in_proc = ""
            assert db._conn() is None
            assert dials["n"] == 2          # TTL 0 disables the memory
            # A non-network error still gets its one retry.
            def _connect_auth(*_a, **_kw):
                dials["n"] += 1
                raise RuntimeError("password authentication failed")
            fake.connect = _connect_auth  # type: ignore[attr-defined]
            db._down_in_proc = ""
            assert db._conn() is None
            assert dials["n"] == 4
    finally:
        db.config.DATABASE_URL = orig_url
        db._DOWN_MARK = orig_mark
        db.time.sleep = orig_sleep
        db._down_in_proc = ""
        if orig_mod is not None:
            sys.modules["psycopg2"] = orig_mod
        else:
            sys.modules.pop("psycopg2", None)
        if orig_env is None:
            os.environ.pop("FULLSCAN_DB_DOWN_TTL_S", None)
        else:
            os.environ["FULLSCAN_DB_DOWN_TTL_S"] = orig_env


def test_query_budget_is_enforced_client_side() -> None:
    """09-09 17:13 News Actions: the pooler ignored options=-c
    statement_timeout, 4 queries ran ~2.5 min each → 11 min red job.
    Every query now gets SET LOCAL in its own transaction plus a
    client-side cancel timer that fires shortly after the budget."""
    import os

    seen: list[str] = []
    cancelled = {"n": 0}

    class _Cur:
        def execute(self, q, params=None):
            seen.append(q)
            if q.startswith("SELECT"):
                # Simulate a server that never returns until cancelled.
                deadline = db.time.time() + 3.0
                while cancelled["n"] == 0 and db.time.time() < deadline:
                    db.time.sleep(0.01)
                if cancelled["n"]:
                    raise RuntimeError("canceling statement due to user request")
                raise AssertionError("cancel timer never fired")

        def fetchall(self):
            return []

        def close(self) -> None:
            return None

    class _Conn:
        def cursor(self) -> _Cur:
            return _Cur()

        def cancel(self) -> None:
            cancelled["n"] += 1

        def rollback(self) -> None:
            return None

        def close(self) -> None:
            return None

    orig_env = os.environ.get("FULLSCAN_DB_STATEMENT_TIMEOUT_MS")
    orig_grace = db._CANCEL_GRACE_S
    orig_conn = db._conn
    orig_sleep = db.time.sleep
    os.environ["FULLSCAN_DB_STATEMENT_TIMEOUT_MS"] = "5000"  # floor is 5s
    db._CANCEL_GRACE_S = -4.9                                  # fire at ~0.1s
    db._conn = lambda: _Conn()  # type: ignore[method-assign]
    try:
        t0 = db.time.time()
        try:
            db._recent_news_once(hours=48, limit=10)
            raise AssertionError("expected NewsDbError")
        except db.NewsDbError as e:
            assert e.reason == "db_timeout"
        assert db.time.time() - t0 < 2.5, "client-side cancel did not bound the query"
        assert cancelled["n"] >= 1
        assert seen[0] == "SET LOCAL statement_timeout = 5000"
        assert any(q.startswith("SELECT") for q in seen)
        assert db._is_timeout(RuntimeError("canceling statement due to user request"))
    finally:
        db._conn = orig_conn
        db._CANCEL_GRACE_S = orig_grace
        db.time.sleep = orig_sleep
        if orig_env is None:
            os.environ.pop("FULLSCAN_DB_STATEMENT_TIMEOUT_MS", None)
        else:
            os.environ["FULLSCAN_DB_STATEMENT_TIMEOUT_MS"] = orig_env


def test_news_actions_survives_db_timeout_with_local_headlines() -> None:
    """The dispatch-only News Actions job must not go red on a slow pooler."""
    from src import news_actions, news_parse

    orig_recent = db.recent_news
    orig_files = news_parse.rows_from_local_files

    def _boom(**_kw):
        raise db.NewsDbError("db_timeout", "statement timeout")

    db.recent_news = _boom  # type: ignore[assignment]
    news_parse.rows_from_local_files = lambda d, n: [  # type: ignore[assignment]
        {"source": "finviz_digest", "title": "Fed holds rates, signals cuts",
         "url": "", "published_at": ""}]
    try:
        rows, src = news_actions._load_rows(48, 300, "2026-09-09")
        assert rows and src == "local_files:db_timeout"
        rows, src = news_actions._load_rows(48, 300, None)
        assert rows == [] and src == "db_error:db_timeout"
    finally:
        db.recent_news = orig_recent  # type: ignore[assignment]
        news_parse.rows_from_local_files = orig_files  # type: ignore[assignment]


if __name__ == "__main__":
    test_recent_news_empty_first_variant_still_reads_second()
    test_recent_news_timeout_jumps_to_last_limit()
    test_unreachable_pooler_dials_once_then_is_remembered()
    test_query_budget_is_enforced_client_side()
    test_news_actions_survives_db_timeout_with_local_headlines()
    print("5 tests passed")
