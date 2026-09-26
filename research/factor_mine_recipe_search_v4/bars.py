"""Clean-tape loader. Same duckdb read as PR #368's CleanStore."""
from __future__ import annotations

import hashlib
from bisect import bisect_left
from pathlib import Path

from research.factor_mine_recipe_search_v4.protocol import (
    CLEAN_PATH,
    CLEAN_SHA256,
    ROOT,
)


class CleanStore:
    """Pinned cleaned bars. The session open is a fill. The close is a mark."""

    def __init__(self, path: Path | None = None) -> None:
        import duckdb

        src = path or (ROOT / CLEAN_PATH)
        digest = hashlib.sha256(src.read_bytes()).hexdigest()
        if digest != CLEAN_SHA256:
            raise SystemExit(f"clean ohlc.parquet sha256 {digest} != {CLEAN_SHA256}")
        rows = duckdb.sql(
            "SELECT CAST(date AS VARCHAR) AS date, CAST(ticker AS VARCHAR) AS ticker, "
            "open, high, low, close, volume "
            f"FROM read_parquet('{src.as_posix()}') ORDER BY ticker, date"
        ).fetchall()
        tapes: dict[str, dict] = {}
        dates: set[str] = set()
        for date, ticker, open_, high, low, close, volume in rows:
            day = str(date)
            dates.add(day)
            tape = tapes.setdefault(str(ticker), {
                "date": [], "open": [], "high": [], "low": [], "close": [],
            })
            tape["date"].append(day)
            tape["open"].append(float(open_) if open_ is not None else float("nan"))
            tape["high"].append(float(high) if high is not None else float("nan"))
            tape["low"].append(float(low) if low is not None else float("nan"))
            tape["close"].append(float(close) if close is not None else float("nan"))
        self.dates = tuple(sorted(dates))
        self.tapes = tapes

    def _idx(self, ticker: str, session: str) -> int | None:
        tape = self.tapes.get(ticker)
        if not tape:
            return None
        idx = bisect_left(tape["date"], session)
        if idx >= len(tape["date"]) or tape["date"][idx] != session:
            return None
        return idx

    def session_open(self, ticker: str, session: str) -> float | None:
        idx = self._idx(ticker, session)
        if idx is None:
            return None
        value = self.tapes[ticker]["open"][idx]
        if value != value or value <= 0:
            return None
        return float(value)

    def session_close(self, ticker: str, session: str) -> float | None:
        idx = self._idx(ticker, session)
        if idx is None:
            return None
        value = self.tapes[ticker]["close"][idx]
        if value != value or value <= 0:
            return None
        return float(value)

    def prev_close(self, ticker: str, session: str) -> float | None:
        tape = self.tapes.get(ticker)
        if not tape:
            return None
        idx = bisect_left(tape["date"], session) - 1
        if idx < 0:
            return None
        value = tape["close"][idx]
        if value != value or value <= 0:
            return None
        return float(value)
