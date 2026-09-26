"""Information-only gross share. The binding line stays in protocol.py."""
from __future__ import annotations

from research.concentration_cap_v1.metrics import contributors


def gross_share(book: dict) -> float | None:
    """Best stock's net dollar profit divided by the sum of nets that made money."""
    ranked = contributors(book)
    if not ranked:
        return None
    totals: dict[str, float] = {}
    for day in book.get("daily") or []:
        bucket = book["pnl_by_day"].get(day["session"]) or {}
        for ticker, amount in bucket.items():
            totals[ticker] = totals.get(ticker, 0.0) + float(amount)
    made = sum(amount for amount in totals.values() if amount > 0.0)
    best = float(totals.get(ranked[0], 0.0))
    if made <= 0.0 or best <= 0.0:
        return None
    return best / made
