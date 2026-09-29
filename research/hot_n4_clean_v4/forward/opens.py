"""Official session open for the 09:35 ET open-fill. Nothing here is stored.

The open is the Yahoo split-adjusted daily Open: ``auto_adjust=False`` and
``actions=True``, the same call ``prices.fetch_yahoo`` and ``src/price_store.py``
lock. Dividends are not applied. Gap, last, and Finviz Price are not used.

At 09:35 ET the daily bar's close is not the official close, so the bar is
not written to ``prices.jsonl``. A later Yahoo print that disagrees with an
open a sealed record used fails the fill; it does not overwrite the fill.
If the live fetch has no positive open, an open already stored for that
session (a final bar from that same Yahoo call) is the only fallback. If
neither exists, the caller appends no fill.

A bar counts for the session only when its date is that session in
America/New_York. A date-only Yahoo label is already that session date.
A timezone-aware timestamp is converted before the calendar date is read.
"""
from __future__ import annotations

import math
from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo

OPEN_SOURCE = "yahoo_split_adjusted_daily_open"
ET = ZoneInfo("America/New_York")


def _positive(value) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(number) or number <= 0:
        return None
    return number


def ny_bar_date(value) -> str | None:
    """Calendar date of a Yahoo bar in America/New_York.

    A ``YYYY-MM-DD`` label is already the session date. A timezone-aware
    timestamp is converted first, so a UTC instant still on the previous
    New York date is not this session.
    """
    if value is None:
        return None
    if isinstance(value, datetime):
        if value.tzinfo is not None:
            value = value.astimezone(ET)
        return value.date().isoformat()
    if isinstance(value, date):
        return value.isoformat()
    text = str(value).strip()
    if not text or text.lower() in {"nat", "nan", "none"}:
        return None
    if len(text) >= 19 and text[4] == "-" and text[7] == "-" and text[10] in " T":
        try:
            parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
        except ValueError:
            parsed = None
        if parsed is not None:
            if parsed.tzinfo is not None:
                parsed = parsed.astimezone(ET)
            return parsed.date().isoformat()
    if len(text) >= 10 and text[4] == "-" and text[7] == "-":
        return text[:10]
    return None


def _prefer_bar(row: dict, old: dict, session: str) -> bool:
    if row["date"] == session and old["date"] != session:
        return True
    if old["date"] == session:
        return False
    if row["date"] and (not old["date"] or row["date"] > old["date"]):
        return True
    return False


def fetched_bars(tickers: list[str], session: str, fetch) -> dict[str, dict]:
    """One Yahoo bar per ticker, including a stale date. Writes nothing.

    The date is the America/New_York session date. A previous session's
    bar is kept so the open fill can refuse it instead of using its open.
    """
    end = (date.fromisoformat(session) + timedelta(days=1)).isoformat()
    fetched = fetch(list(tickers), session, end) or {}
    if fetched.get("error"):
        return {}
    found: dict[str, dict] = {}
    for bar in fetched.get("bars") or []:
        name = str(bar.get("ticker") or "").upper()
        if not name:
            continue
        row = {"date": ny_bar_date(bar.get("date")), "open": bar.get("open")}
        old = found.get(name)
        if old is None or _prefer_bar(row, old, session):
            found[name] = row
    return found


def live_opens(tickers: list[str], session: str, fetch) -> dict[str, float]:
    """Opens from one Yahoo download. Does not write the price store."""
    found: dict[str, float] = {}
    for ticker, bar in fetched_bars(tickers, session, fetch).items():
        if bar.get("date") != session:
            continue
        op = _positive(bar.get("open"))
        if op is None:
            continue
        found[ticker] = op
    return found


def session_open(ticker: str, session: str, live: dict, stored: dict) -> float | None:
    """Session open from the live bar, else a stored bar dated ``session``."""
    from research.hot_n4_clean_v4.run_study import open_px

    bar = live.get(ticker)
    if bar and bar.get("date") == session:
        op = _positive(bar.get("open"))
        if op is not None:
            return op
    return _positive(open_px(stored, ticker, session))


def session_opens(names: list[str], session: str, live: dict, stored: dict) -> dict[str, float]:
    """Opens whose bar is dated ``session``. A stale live bar is not used."""
    found: dict[str, float] = {}
    for ticker in names:
        op = session_open(ticker, session, live, stored)
        if op is not None:
            found[ticker] = op
    return found


def stored_opens(stored: dict, tickers: list[str], session: str) -> dict[str, float]:
    """Opens already in the pinned file or ``prices.jsonl`` for ``session``."""
    from research.hot_n4_clean_v4.run_study import open_px

    found: dict[str, float] = {}
    for ticker in tickers:
        name = str(ticker).upper()
        op = _positive(open_px(stored, name, session))
        if op is not None:
            found[name] = op
    return found


def collect_opens(tickers: list[str], session: str, stored: dict, fetch) -> dict[str, float]:
    """Live Yahoo open, else the stored Yahoo open. Empty when neither exists."""
    names = sorted({str(ticker).upper() for ticker in tickers if str(ticker).strip()})
    live = live_opens(names, session, fetch)
    if live:
        return live
    return stored_opens(stored, names, session)
