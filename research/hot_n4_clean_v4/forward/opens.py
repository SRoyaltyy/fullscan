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
"""
from __future__ import annotations

import math
from datetime import date, timedelta

OPEN_SOURCE = "yahoo_split_adjusted_daily_open"


def _positive(value) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(number) or number <= 0:
        return None
    return number


def live_opens(tickers: list[str], session: str, fetch) -> dict[str, float]:
    """Opens from one Yahoo download. Does not write the price store."""
    end = (date.fromisoformat(session) + timedelta(days=1)).isoformat()
    fetched = fetch(list(tickers), session, end)
    if fetched.get("error"):
        return {}
    found: dict[str, float] = {}
    for bar in fetched.get("bars") or []:
        if str(bar.get("date"))[:10] != session:
            continue
        op = _positive(bar.get("open"))
        if op is None:
            continue
        found[str(bar.get("ticker")).upper()] = op
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
