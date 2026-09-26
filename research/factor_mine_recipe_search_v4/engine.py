"""Factor Mine cash book for the recipe search.

Share counts, min-hold, list-drop, and holdup follow ``lot_should_sell``.
``fill="keep_held"`` is the live path: a name still selected and already
held is kept, with no trade and no fee. ``fill="renew"`` is the old Group 3
path: once the lot's min-hold is up it sells at the open and, if still
selected, buys again at that same open.
"""
from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from research.factor_mine_recipe_search_v4.protocol import (  # noqa: E402
    CAPITAL,
    HOLDUP_S,
    HOLDUP_SESS,
    hard_red,
)
from src.factor_mine import pick_day, should_exit  # noqa: E402
from src.factor_mine_book import lot_should_sell, split_budgets  # noqa: E402
from src.paper_trade import order_fees  # noqa: E402

BP_SIDE = 0.000075


def fee_15(shares: int, price: float) -> float:
    if shares <= 0 or price <= 0:
        return 0.0
    return round(shares * float(price) * BP_SIDE, 4)


def choose(rows: list[dict], recipe: dict) -> list[dict]:
    pool = rows
    if recipe.get("skip_first"):
        pool = [row for row in pool if int(row.get("days_on_list") or 1) > 1]
    if recipe.get("earn_news"):
        pool = [
            row for row in pool
            if row.get("erd_earn_react") or (row.get("boxes") or {}).get("news") == "good"
        ]
    return pick_day(pool, recipe)


def _stock(pos: dict, session: str, which: str, price) -> float:
    total = 0.0
    for lot in pos.values():
        px = price(lot["ticker"], session, which)
        if px is None or px <= 0:
            px = lot.get("close_px") or lot.get("last_px") or lot["entry_px"]
        total += lot["shares"] * float(px)
    return total


def walk(days: list[dict], recipe: dict, fees: dict, price, fill: str) -> dict:
    if fill not in ("keep_held", "renew"):
        raise SystemExit(f"fill {fill}")
    cash = CAPITAL
    cash_15 = CAPITAL
    pos: dict[str, dict] = {}
    side = "long"
    min_hold = int(recipe.get("hold") or 1)
    sell_mode = recipe.get("sell") or "list"
    boost = recipe.get("s_boost") or "none"
    weather = bool(recipe.get("weather", True))
    date_ix = {day["session"]: index for index, day in enumerate(days)}
    prev = CAPITAL
    prev_15 = CAPITAL
    daily = []
    closed = []
    pnl_by_day: dict[str, dict[str, float]] = {}
    first: dict[str, str] = {}

    def add_pnl(session: str, ticker: str, amount: float) -> None:
        bucket = pnl_by_day.setdefault(session, {})
        bucket[ticker] = bucket.get(ticker, 0.0) + float(amount)

    for day in days:
        session = day["session"]
        score = day.get("s")
        sit = weather and hard_red(score)
        holdup = (
            boost == "holdup" and score is not None and float(score) > HOLDUP_S and not sit
        )
        by_ticker = {row["ticker"]: row for row in day["rows"]}
        chosen = choose(day["rows"], recipe)
        tset = {row["ticker"] for row in chosen}
        bought = []
        sold = []
        for ticker, lot in list(pos.items()):
            opx = price(ticker, session, "open")
            if opx is None or opx <= 0:
                continue
            prior = float(lot.get("close_px") or lot["entry_px"])
            move = lot["shares"] * (float(opx) - prior)
            lot["marked"] = lot.get("marked", 0.0) + move
            add_pnl(session, ticker, move)
        for ticker in list(pos):
            lot = pos[ticker]
            held = date_ix[session] - date_ix[lot["entry"]]
            row = by_ticker.get(ticker) or {}
            early = should_exit(row, recipe.get("exit_when"))
            opx = price(ticker, session, "open")
            if opx is None or opx <= 0:
                lot["unpriced"] = True
                continue
            opx = float(opx)
            lot["peak_px"] = max(float(lot.get("peak_px") or lot["entry_px"]), opx)
            lot["last_px"] = opx
            lot_min = int(lot.get("min_hold") or min_hold)
            if fill == "renew":
                do_sell = bool(early or held >= lot_min)
            elif lot.get("unpriced"):
                do_sell = True
            else:
                do_sell, _kind = lot_should_sell(
                    lot, held=held, min_hold=lot_min, early=early,
                    dropped=ticker not in tset, sell_mode=sell_mode, px=opx,
                    side=side, take_pct=None, stop_pct=None,
                )
            if not do_sell:
                continue
            shares = lot["shares"]
            fee = float(order_fees(shares, opx, "sell", fees))
            fee_b = float(fee_15(shares, opx))
            cash += shares * opx - fee
            cash_15 += shares * opx - fee_b
            pnl = shares * opx - fee - lot["cost"]
            add_pnl(session, ticker, pnl - lot.get("marked", 0.0))
            closed.append({
                "entry": lot["entry"],
                "entry_px": lot["entry_px"],
                "exit": session,
                "pnl": pnl,
                "ticker": ticker,
                "win": pnl > 0,
            })
            sold.append(ticker)
            pos.pop(ticker)
        new = [row for row in chosen if row["ticker"] not in pos]
        if sit:
            new = []
        if new and cash > 0:
            budgets = split_budgets(new, cash, "leftover")
            for row, per in zip(new, budgets):
                ticker = row["ticker"]
                opx = price(ticker, session, "open")
                if opx is None or opx <= 0:
                    continue
                opx = float(opx)
                shares = int(per // opx)
                if shares < 1:
                    continue
                fee = float(order_fees(shares, opx, "buy", fees))
                cost = shares * opx + fee
                if cost > cash + 1e-6:
                    shares = int((cash - fee) // opx) if opx else 0
                    if shares < 1:
                        continue
                    fee = float(order_fees(shares, opx, "buy", fees))
                    cost = shares * opx + fee
                fee_b = float(fee_15(shares, opx))
                cash -= cost
                cash_15 -= shares * opx + fee_b
                pos[ticker] = {
                    "close_px": None,
                    "cost": cost,
                    "entry": session,
                    "entry_px": opx,
                    "last_px": opx,
                    "marked": -fee,
                    "min_hold": max(min_hold, HOLDUP_SESS) if holdup else min_hold,
                    "peak_px": opx,
                    "shares": shares,
                    "ticker": ticker,
                }
                add_pnl(session, ticker, -fee)
                first.setdefault(ticker, session)
                bought.append(ticker)
        for lot in pos.values():
            cpx = price(lot["ticker"], session, "close")
            opx = price(lot["ticker"], session, "open")
            if opx is None or opx <= 0:
                opx = lot["last_px"]
            if cpx is None or cpx <= 0:
                cpx = lot.get("close_px") or lot["last_px"]
            else:
                cpx = float(cpx)
            opx = float(opx)
            move = lot["shares"] * (cpx - opx)
            add_pnl(session, lot["ticker"], move)
            lot["marked"] = lot.get("marked", 0.0) + move
            lot["close_px"] = cpx
            lot["last_px"] = cpx
        equity = cash + _stock(pos, session, "close", price)
        equity_15 = cash_15 + _stock(pos, session, "close", price)
        ret = (equity / prev - 1.0) if prev else 0.0
        ret_15 = (equity_15 / prev_15 - 1.0) if prev_15 else 0.0
        attributed = sum((pnl_by_day.get(session) or {}).values())
        if abs(attributed - (equity - prev)) > 0.05:
            raise SystemExit(
                f"pnl drift {recipe.get('id') or recipe.get('name')} {session} "
                f"{attributed} vs {equity - prev}"
            )
        daily.append({
            "bought": bought,
            "equity": equity,
            "equity_15": equity_15,
            "ret": ret,
            "ret_15": ret_15,
            "session": session,
            "sold": sold,
        })
        prev = equity
        prev_15 = equity_15
    return {"closed": closed, "daily": daily, "first": first, "pnl_by_day": pnl_by_day}


def assert_fills() -> None:
    """Keep-held does not renew. Renew sells and buys the same open. Holdup still waits."""
    from src.paper_trade import load_fees

    fees = load_fees()
    days = []
    for session, score, ticker in (
        ("2026-09-02", 2.25, "AAA"),
        ("2026-09-03", -0.9, "AAA"),
        ("2026-09-04", 2.0, "BBB"),
    ):
        days.append({
            "session": session,
            "s": score,
            "rows": [{
                "ticker": ticker, "sources": ["union"], "src_rank": 0,
                "ohlc_hot_score": 9, "alarm": False, "boxes": {},
                "days_on_list": 2, "erd_earn_react": False,
            }],
        })
    bars = {
        ("AAA", "2026-09-02"): {"open": 10.0, "close": 10.0},
        ("AAA", "2026-09-03"): {"open": 12.0, "close": 12.0},
        ("AAA", "2026-09-04"): {"open": 11.0, "close": 11.0},
        ("BBB", "2026-09-02"): {"open": 20.0, "close": 20.0},
        ("BBB", "2026-09-03"): {"open": 20.0, "close": 20.0},
        ("BBB", "2026-09-04"): {"open": 19.0, "close": 19.0},
    }

    def price(ticker: str, session: str, which: str):
        bar = bars.get((ticker, session)) or {}
        value = bar.get(which)
        if value is None or float(value) <= 0:
            return None
        return float(value)

    recipe = {
        "name": "union_hot_n4_h1", "id": "union_hot_n4_h1__w1",
        "universe": "union", "hold": 1, "top_n": 1, "rank": "hot_score",
        "forbid": {"alarm": True}, "require": {}, "sell": "list",
        "s_boost": "none", "weather": False, "exit_when": {},
    }
    renewed = walk(days, recipe, fees, price, "renew")
    kept = walk(days, recipe, fees, price, "keep_held")
    if renewed["daily"][1]["sold"] != ["AAA"] or renewed["daily"][1]["bought"] != ["AAA"]:
        raise SystemExit(f"renew did not round-trip {renewed['daily'][1]}")
    if kept["daily"][1]["sold"] or kept["daily"][1]["bought"]:
        raise SystemExit(f"keep-held traded {kept['daily'][1]}")
    holdup = dict(recipe, s_boost="holdup", name="union_hot_n4_holdup")
    held = walk(days, holdup, fees, price, "keep_held")
    if held["daily"][1]["sold"]:
        raise SystemExit("holdup sold inside the extra session")
    if not held["closed"] or held["closed"][0]["exit"] != "2026-09-04":
        raise SystemExit(f"holdup exit {held['closed'][:1]}")
