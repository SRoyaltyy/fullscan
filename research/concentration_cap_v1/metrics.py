"""Window statistics. Drop paths start from the equity the window already has."""
from __future__ import annotations

from research.factor_mine_recipe_search_v4.metrics import compound, period_start
from research.factor_mine_recipe_search_v4.protocol import MIN_TRADES


def drop_compound(book: dict, tickers: set[str] | list[str]) -> float:
    """Same arithmetic as v4 ``_drop_compound``, for one name or several."""
    daily = book.get("daily") or []
    if not daily:
        return 0.0
    want = set(tickers)
    prev = period_start(book)
    kept = prev
    rets = []
    for day in daily:
        change = float(day["equity"]) - prev
        bucket = book["pnl_by_day"].get(day["session"]) or {}
        cut = sum(float(bucket.get(ticker) or 0.0) for ticker in want)
        nxt = kept + (change - cut)
        rets.append((nxt / kept - 1.0) if kept else 0.0)
        prev = float(day["equity"])
        kept = nxt
    return compound(rets)


def contributors(book: dict) -> list[str]:
    """Largest summed pnl first. Ties: earlier first session, then ticker."""
    totals: dict[str, float] = {}
    first: dict[str, str] = {}
    for day in book.get("daily") or []:
        session = day["session"]
        bucket = book["pnl_by_day"].get(session) or {}
        for ticker, amount in bucket.items():
            totals[ticker] = totals.get(ticker, 0.0) + float(amount)
            first.setdefault(ticker, session)
    return sorted(totals, key=lambda ticker: (-totals[ticker], first[ticker], ticker))


def profit_share(book: dict) -> float | None:
    ranked = contributors(book)
    if not ranked:
        return None
    daily = book.get("daily") or []
    if not daily:
        return None
    start = period_start(book)
    end = float(daily[-1]["equity"])
    denom = end - start
    if denom == 0.0:
        return None
    best = ranked[0]
    total = 0.0
    for day in daily:
        total += float((book["pnl_by_day"].get(day["session"]) or {}).get(best) or 0.0)
    return total / denom


def distinct_tickers(book: dict) -> int:
    names: set[str] = set()
    for day in book.get("daily") or []:
        names.update(day.get("bought") or [])
        names.update(day.get("sold") or [])
        names.update(day.get("trimmed") or [])
        names.update((book["pnl_by_day"].get(day["session"]) or {}).keys())
    for trade in book.get("closed") or []:
        names.add(trade["ticker"])
    return len(names)


def _joint(book: dict) -> dict:
    daily = book.get("daily") or []
    closed = book.get("closed") or []
    active = [
        day for day in daily
        if day.get("bought") or day.get("sold") or day.get("trimmed")
    ]
    up = sum(1 for day in active if day["ret"] > 1e-12)
    down = sum(1 for day in active if day["ret"] < -1e-12)
    n = len(closed)
    wins = sum(1 for trade in closed if trade["win"])
    win_rate = (wins / n) if n else None
    up_share = (up / len(active)) if active else None
    joint = None
    if n >= MIN_TRADES and win_rate is not None and up_share is not None:
        joint = min(win_rate, up_share)
    return {
        "down": down,
        "joint": joint,
        "n": n,
        "up": up,
        "up_share": up_share,
        "win_rate": win_rate,
    }


def window_row(book: dict) -> dict:
    daily = book.get("daily") or []
    ranked = contributors(book)
    start = period_start(book) if daily else None
    end = float(daily[-1]["equity"]) if daily else None
    out = {
        "best": ranked[0] if ranked else None,
        "compound": compound([day["ret"] for day in daily]) if daily else 0.0,
        "compound_15": compound([day["ret_15"] for day in daily]) if daily else 0.0,
        "distinct": distinct_tickers(book),
        "end_equity": end,
        "profit_share": profit_share(book),
        "start_equity": start,
    }
    out.update(_joint(book))
    for k in (1, 3, 5):
        out[f"ex_top{k}"] = drop_compound(book, ranked[:k]) if ranked else out["compound"]
    return out


def slice_book(book: dict, sessions: tuple[str, ...] | list[str], start_equity: float) -> dict:
    wanted = set(sessions)
    daily = [day for day in book["daily"] if day["session"] in wanted]
    closed = [trade for trade in book["closed"] if trade["exit"] in wanted]
    pnl = {day["session"]: book["pnl_by_day"].get(day["session"]) or {} for day in daily}
    return {
        "closed": closed,
        "daily": daily,
        "first": {},
        "pnl_by_day": pnl,
        "start_equity": float(start_equity),
    }
