"""Window statistics. This module does not walk a book."""
from __future__ import annotations

from research.factor_mine_recipe_search_v4.protocol import CAPITAL, MIN_TRADES


def compound(returns: list[float]) -> float:
    acc = 1.0
    for value in returns:
        acc *= 1.0 + float(value)
    return acc - 1.0


def _drop_compound(book: dict, ticker: str) -> float:
    prev = CAPITAL
    kept = CAPITAL
    rets = []
    for day in book["daily"]:
        change = day["equity"] - prev
        cut = float((book["pnl_by_day"].get(day["session"]) or {}).get(ticker) or 0.0)
        nxt = kept + (change - cut)
        rets.append((nxt / kept - 1.0) if kept else 0.0)
        prev = day["equity"]
        kept = nxt
    return compound(rets)


def best_ticker(book: dict) -> str | None:
    totals: dict[str, float] = {}
    for session, bucket in book["pnl_by_day"].items():
        for ticker, amount in bucket.items():
            totals[ticker] = totals.get(ticker, 0.0) + float(amount)
    if not totals:
        return None
    first = book["first"]

    def key(ticker: str):
        return (-totals[ticker], first.get(ticker) or "9999-99-99", ticker)

    return min(totals, key=key)


def summarize(book: dict) -> dict:
    daily = book["daily"]
    closed = book["closed"]
    active = [day for day in daily if day["bought"] or day["sold"]]
    up = sum(1 for day in active if day["ret"] > 1e-12)
    down = sum(1 for day in active if day["ret"] < -1e-12)
    flat = len(active) - up - down
    n = len(closed)
    wins = sum(1 for trade in closed if trade["win"])
    win_rate = (wins / n) if n else None
    up_share = (up / len(active)) if active else None
    joint = None
    if n >= MIN_TRADES and win_rate is not None and up_share is not None:
        joint = min(win_rate, up_share)
    under = (sum(1 for trade in closed if trade["entry_px"] < 3.0) / n) if n else None
    best = best_ticker(book)
    named = {}
    for ticker in ("CYPH", "GLND", "INDP"):
        named[ticker] = _drop_compound(book, ticker)
    return {
        "compound": compound([day["ret"] for day in daily]) if daily else 0.0,
        "compound_15": compound([day["ret_15"] for day in daily]) if daily else 0.0,
        "down": down,
        "ex_best": None if best is None else _drop_compound(book, best),
        "ex_best_ticker": best,
        "flat": flat,
        "joint": joint,
        "n": n,
        "named": named,
        "under_3": under,
        "up": up,
        "up_share": up_share,
        "win_rate": win_rate,
    }
