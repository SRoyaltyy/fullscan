"""Execution callbacks for the same stateful book; no daily-high/low latency claims."""
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from .research_validation import timestamp


def fixed_bps(bps=10.0, fraction=1.0):
    """Transparent sensitivity, NOT an empirically calibrated fill forecast."""
    if not 0 <= bps < 10000 or not 0 < fraction <= 1:
        raise ValueError("invalid execution scenario")

    def fill(*, side, action, official_px, intended_shares, **kw):
        buy = (side == "long") == (action == "entry")
        shares = int(intended_shares * fraction)
        return {"px": official_px * (1 + (1 if buy else -1) * bps / 10000),
                "shares": shares, "miss": shares < 1, "partial": shares < intended_shares,
                "how": f"sensitivity_{bps:g}bps_{fraction:g}",
                "reason": "constant adverse-cost scenario; execution unverified"}
    return fill


def timestamped_quotes(events, *, delay_seconds=5, expiry_seconds=60):
    """Conservative post-open quote replay with shared displayed-size depletion.

Quotes must supply timezone-aware timestamps, bid/ask AND displayed sizes.
Partial fills persist in the caller's portfolio. Daily OHLC cannot substitute.
Calls within a session never execute earlier than a prior fill in that session.
This is simulated execution; queue priority/impact still require calibration.
"""
    if delay_seconds < 0 or expiry_seconds < delay_seconds:
        raise ValueError("invalid execution window")
    used, last = {}, {}
    ordered = {}
    for ticker, quotes in events.items():
        valid = []
        for i, q in enumerate(quotes):
            try:
                valid.append((timestamp(q["timestamp"]), i, q))
            except (KeyError, TypeError, ValueError):
                continue
        ordered[ticker] = sorted(valid, key=lambda x: (x[0], x[1]))

    def fill(*, ticker, date, side, action, official_px, intended_shares, **kw):
        buy = (side == "long") == (action == "entry")
        bell = datetime.fromisoformat(date + "T09:30:00").replace(tzinfo=ZoneInfo("America/New_York"))
        start = max(bell + timedelta(seconds=delay_seconds), last.get(date, bell))
        end = bell + timedelta(seconds=expiry_seconds)
        for when, i, q in ordered.get(ticker, []):
            try:
                when = timestamp(q["timestamp"])
                if not start <= when <= end:
                    continue
                field = "ask" if buy else "bid"
                px = float(q[field])
                size = int(q[field + "_size"]) - used.get((ticker, i, field), 0)
                if not (0 < px < float("inf")) or size < 1:
                    continue
            except (KeyError, ValueError, TypeError):
                continue
            shares = min(intended_shares, size)
            used[ticker, i, field] = used.get((ticker, i, field), 0) + shares
            last[date] = when
            return {"px": px, "shares": shares, "miss": False,
                    "partial": shares < intended_shares, "how": "timestamped_quote",
                    "filled_at": when.isoformat(), "reason": "displayed quote-size simulation"}
        return {"px": None, "shares": 0, "miss": True,
                "how": "no_eligible_quote", "reason": "no post-submission executable quote"}
    return fill
