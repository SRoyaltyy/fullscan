"""Keep-held v4 book plus a next-open weight cap.

``weight_cap`` absent or null skips the trim, and that book is the v4
keep-held walker at the candidate's width. Leftover cash from a trim is
not added to a name that is already held.
"""
from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from research.factor_mine_recipe_search_v4.engine import choose, fee_15  # noqa: E402
from research.factor_mine_recipe_search_v4.protocol import (  # noqa: E402
    CAPITAL,
    HOLDUP_S,
    HOLDUP_SESS,
    hard_red,
)
from src.factor_mine import should_exit  # noqa: E402
from src.factor_mine_book import lot_should_sell, split_budgets  # noqa: E402
from src.paper_trade import order_fees  # noqa: E402


def shares_to_keep(shares: int, price: float, equity: float, cap: float, fees: dict) -> int:
    """Largest whole-share remainder at or under the cap after the sell fee."""
    shares = int(shares)
    price = float(price)
    equity = float(equity)
    cap = float(cap)
    if shares < 1 or price <= 0 or equity <= 0 or not (0.0 < cap < 1.0):
        return max(shares, 0)
    if shares * price <= cap * equity + 1e-6:
        return shares
    keep = min(shares - 1, int((cap * equity) // price))
    while keep >= 0:
        sold = shares - keep
        fee = float(order_fees(sold, price, "sell", fees))
        after = equity - fee
        if after > 0.0 and keep * price <= cap * after + 1e-6:
            return keep
        keep -= 1
    return 0


def _stock(pos: dict, session: str, which: str, price) -> float:
    total = 0.0
    for lot in pos.values():
        px = price(lot["ticker"], session, which)
        if px is None or px <= 0:
            px = lot.get("close_px") or lot.get("last_px") or lot["entry_px"]
        total += lot["shares"] * float(px)
    return total


def _cap(recipe: dict):
    raw = recipe.get("weight_cap")
    if raw is None:
        return None
    cap = float(raw)
    if not (0.0 < cap < 1.0):
        return None
    return cap


def walk(days: list[dict], recipe: dict, fees: dict, price) -> dict:
    """Keep-held only. The v4 renew fill is not a second try in this study."""
    cash = CAPITAL
    cash_15 = CAPITAL
    pos: dict[str, dict] = {}
    side = "long"
    min_hold = int(recipe.get("hold") or 1)
    sell_mode = recipe.get("sell") or "list"
    boost = recipe.get("s_boost") or "none"
    weather = bool(recipe.get("weather", True))
    cap = _cap(recipe)
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
        trimmed = []
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
            if lot.get("unpriced"):
                do_sell = True
            else:
                do_sell, _kind = lot_should_sell(
                    lot, held=held, min_hold=lot_min, early=early,
                    dropped=ticker not in tset, sell_mode=sell_mode, px=opx,
                    side=side, take_pct=None, stop_pct=None,
                )
            if not do_sell:
                continue
            shares = int(lot["shares"])
            fee = float(order_fees(shares, opx, "sell", fees))
            fee_b = float(fee_15(shares, opx))
            cash += shares * opx - fee
            cash_15 += shares * opx - fee_b
            chunk = shares * opx - fee - lot["cost"]
            pnl = chunk + float(lot.get("realized") or 0.0)
            add_pnl(session, ticker, chunk - lot.get("marked", 0.0))
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
        if cap is not None:
            for ticker in sorted(pos):
                lot = pos.get(ticker)
                if lot is None:
                    continue
                opx = price(ticker, session, "open")
                if opx is None or opx <= 0:
                    continue
                opx = float(opx)
                equity = cash + _stock(pos, session, "open", price)
                shares = int(lot["shares"])
                keep = shares_to_keep(shares, opx, equity, cap, fees)
                if keep >= shares:
                    continue
                sold_n = shares - keep
                fee = float(order_fees(sold_n, opx, "sell", fees))
                fee_b = float(fee_15(sold_n, opx))
                frac = sold_n / shares
                sold_cost = float(lot["cost"]) * frac
                sold_marked = float(lot.get("marked") or 0.0) * frac
                cash += sold_n * opx - fee
                cash_15 += sold_n * opx - fee_b
                chunk = sold_n * opx - fee - sold_cost
                add_pnl(session, ticker, chunk - sold_marked)
                if keep < 1:
                    pnl = chunk + float(lot.get("realized") or 0.0)
                    closed.append({
                        "entry": lot["entry"],
                        "entry_px": lot["entry_px"],
                        "exit": session,
                        "pnl": pnl,
                        "ticker": ticker,
                        "win": pnl > 0,
                    })
                    pos.pop(ticker)
                else:
                    lot["cost"] = float(lot["cost"]) - sold_cost
                    lot["marked"] = float(lot.get("marked") or 0.0) - sold_marked
                    lot["realized"] = float(lot.get("realized") or 0.0) + chunk
                    lot["shares"] = keep
                    lot["last_px"] = opx
                trimmed.append(ticker)
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
                    "realized": 0.0,
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
                f"pnl drift {recipe.get('id')} {session} "
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
            "trimmed": trimmed,
        })
        prev = equity
        prev_15 = equity_15
    return {
        "closed": closed,
        "daily": daily,
        "first": first,
        "pnl_by_day": pnl_by_day,
        "start_equity": CAPITAL,
    }
