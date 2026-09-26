"""Proxy-panel membership for the long-history study.

Price, 20-day dollar volume, and relative volume use only bars with
date < D. Session D contributes its official open to the gap test and
nothing else. This is the check named in PREREG.md lines 96-101 and
133-137. It does not score a rule.
"""
from __future__ import annotations

import math

from src.ohlc_ripper import from_bars

PRIOR_MIN = 20
PRICE_MIN = 1.0
PRICE_MAX = 30.0
DOLLAR_MIN = 1_000_000.0
DOLLAR_MAX = 40_000_000.0
GAP_MIN = 0.04
RVOL_MIN = 1.5
CAP = 60


def _finite(value) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(number):
        return None
    return number


def _qualifies(prior: list[dict], today: dict) -> tuple[bool, float, float]:
    """Return (in raw pool, abs gap, rvol). Prior bars are date < D."""
    if len(prior) < PRIOR_MIN:
        return False, 0.0, 0.0
    close = _finite(prior[-1].get("close"))
    if close is None or close < PRICE_MIN or close > PRICE_MAX:
        return False, 0.0, 0.0
    last20 = prior[-PRIOR_MIN:]
    dollars = []
    for bar in last20:
        px = _finite(bar.get("close"))
        vol = _finite(bar.get("volume"))
        if px is None or vol is None:
            return False, 0.0, 0.0
        dollars.append(px * vol)
    dollar_mean = sum(dollars) / len(dollars)
    if dollar_mean < DOLLAR_MIN or dollar_mean > DOLLAR_MAX:
        return False, 0.0, 0.0
    opened = _finite(today.get("open"))
    if opened is None or opened <= 0 or close == 0:
        return False, 0.0, 0.0
    gap = abs(opened / close - 1.0)
    if gap < GAP_MIN:
        return False, 0.0, 0.0
    feat = from_bars(prior)
    rvol = float(feat.get("rvol") or 0.0)
    if not feat.get("ok") or rvol < RVOL_MIN:
        return False, 0.0, 0.0
    return True, gap, rvol


def proxy_members(bars_by_ticker: dict[str, list[dict]], session: str,
                  cap: int = CAP) -> list[str]:
    """Tickers in the capped proxy panel on ``session``.

    ``bars_by_ticker`` values are chronological bars. A bar dated
    ``session`` supplies the open only. Its high, low, close, and
    volume are not read.
    """
    day = str(session)[:10]
    pool: list[tuple[float, float, str]] = []
    for ticker, bars in bars_by_ticker.items():
        prior = [bar for bar in bars if str(bar.get("date"))[:10] < day]
        today = [bar for bar in bars if str(bar.get("date"))[:10] == day]
        if not today:
            continue
        ok, gap, rvol = _qualifies(prior, today[-1])
        if ok:
            pool.append((gap, rvol, str(ticker)))
    pool.sort(key=lambda row: (-row[0], -row[1], row[2]))
    return [ticker for _, _, ticker in pool[: int(cap)]]
