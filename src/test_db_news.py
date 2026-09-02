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


if __name__ == "__main__":
    test_recent_news_empty_first_variant_still_reads_second()
    print("1 test passed")
