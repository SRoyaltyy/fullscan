"""Period statistics. Dollar drops use the fixed single-name function."""
from __future__ import annotations

from research.concentration_screen_v1.protocol import MIN_TRADES, POSITIVE_EPS
from research.factor_mine_recipe_search_v4.metrics import (
    _drop_compound,
    compound,
    period_start,
)


def ticker_totals(book: dict) -> dict[str, float]:
    totals: dict[str, float] = {}
    for bucket in (book.get("pnl_by_day") or {}).values():
        for ticker, amount in bucket.items():
            totals[ticker] = totals.get(ticker, 0.0) + float(amount)
    return totals


def top_tickers(book: dict, count: int) -> list[str]:
    """Highest dollar contribution in this period. Ties take the earlier name, then the ticker."""
    totals = ticker_totals(book)
    if not totals or count < 1:
        return []
    first = book.get("first") or {}

    def key(ticker: str):
        return (-totals[ticker], first.get(ticker) or "9999-99-99", ticker)

    return sorted(totals, key=key)[:count]


def drop_compound(book: dict, tickers: tuple[str, ...] | list[str]) -> float:
    """Return with these names' dollars removed. One name calls the fixed function."""
    names = tuple(dict.fromkeys(tickers))
    if not names:
        daily = book.get("daily") or []
        return compound([day["ret"] for day in daily]) if daily else 0.0
    if len(names) == 1:
        return _drop_compound(book, names[0])
    daily = book.get("daily") or []
    if not daily:
        return 0.0
    prev = period_start(book)
    kept = prev
    rets = []
    ban = set(names)
    for day in daily:
        change = float(day["equity"]) - prev
        bucket = (book.get("pnl_by_day") or {}).get(day["session"]) or {}
        cut = sum(float(bucket.get(ticker) or 0.0) for ticker in ban)
        nxt = kept + (change - cut)
        rets.append((nxt / kept - 1.0) if kept else 0.0)
        prev = float(day["equity"])
        kept = nxt
    return compound(rets)


def slice_book(book: dict, sessions: tuple[str, ...] | list[str], start_equity: float) -> dict:
    """Keep one period. ``start_equity`` is the equity at the prior close, not a fresh $10,000."""
    wanted = set(sessions)
    daily = [day for day in book["daily"] if day["session"] in wanted]
    closed = [trade for trade in book["closed"] if trade["exit"] in wanted]
    pnl = {
        session: dict(bucket)
        for session, bucket in book["pnl_by_day"].items()
        if session in wanted
    }
    first: dict[str, str] = {}
    for trade in closed:
        first.setdefault(trade["ticker"], trade["entry"])
    for day in daily:
        for ticker in day.get("bought") or []:
            first.setdefault(ticker, day["session"])
    return {
        "closed": closed,
        "daily": daily,
        "first": first,
        "pnl_by_day": pnl,
        "start_equity": float(start_equity),
    }


def period_stats(book: dict) -> dict:
    daily = book.get("daily") or []
    closed = book.get("closed") or []
    n = len(closed)
    wins = sum(1 for trade in closed if trade.get("win"))
    start = period_start(book)
    end = float(daily[-1]["equity"]) if daily else start
    profit = end - start
    totals = ticker_totals(book)
    best_names = top_tickers(book, 1)
    best = best_names[0] if best_names else None
    best_pnl = float(totals.get(best) or 0.0) if best else 0.0
    share = (best_pnl / profit) if abs(profit) > 1e-9 else None
    tickers = set(totals)
    for day in daily:
        tickers.update(day.get("bought") or [])
        tickers.update(day.get("sold") or [])
    top3 = top_tickers(book, 3)
    top5 = top_tickers(book, 5)
    return {
        "best": best,
        "best_pnl": best_pnl,
        "best_share": share,
        "ex1": drop_compound(book, best_names) if best_names else (compound([day["ret"] for day in daily]) if daily else 0.0),
        "ex3": drop_compound(book, top3) if top3 else (compound([day["ret"] for day in daily]) if daily else 0.0),
        "ex5": drop_compound(book, top5) if top5 else (compound([day["ret"] for day in daily]) if daily else 0.0),
        "lt30": n < MIN_TRADES,
        "n_tickers": len(tickers),
        "n_trades": n,
        "profit": profit,
        "ret": compound([day["ret"] for day in daily]) if daily else 0.0,
        "ret_15": compound([day["ret_15"] for day in daily]) if daily else 0.0,
        "start_equity": start,
        "top1": best_names,
        "top3": top3,
        "top5": top5,
        "win_rate": (wins / n) if n else None,
    }


def is_positive(value) -> bool:
    return value is not None and float(value) > POSITIVE_EPS


def lists_from(rows: list[dict]) -> dict:
    """(A) positive without the top stock in both periods. (B) positive without the top 3."""
    both_a = []
    both_b = []
    for row in rows:
        p1 = row["p1"]
        p2 = row["p2"]
        if is_positive(p1["ex1"]) and is_positive(p2["ex1"]):
            both_a.append(row)
        if is_positive(p1["ex3"]) and is_positive(p2["ex3"]):
            both_b.append(row)

    def weaker(row: dict, key: str):
        return min(float(row["p1"][key]), float(row["p2"][key]))

    both_a.sort(key=lambda row: (-weaker(row, "ex1"), row["id"]))
    both_b.sort(key=lambda row: (-weaker(row, "ex3"), row["id"]))
    return {"A": both_a, "B": both_b}
