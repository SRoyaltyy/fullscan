"""Keep-held short book. Longs stay on the v4 walker.

A short still selected is kept: no cover and no new short. Borrow is the Group 3
daily rate, 1% a year over 252 sessions. Sizing uses half of open equity, the
Group 3 short room.
"""
from __future__ import annotations

from research.factor_mine_recipe_search_v4.engine import fee_15
from research.factor_mine_recipe_search_v4.protocol import CAPITAL
from src.factor_mine import pick_day, should_exit
from src.factor_mine_book import lot_should_sell
from src.paper_trade import order_fees

BORROW_DAY = 0.01 / 252.0


def _px(price, ticker: str, session: str, which: str, fallback: float) -> float:
    value = price(ticker, session, which)
    if value is None or value <= 0:
        return float(fallback)
    return float(value)


def walk_short(days: list[dict], recipe: dict, fees: dict, price) -> dict:
    cash = CAPITAL
    cash_15 = CAPITAL
    pos: dict[str, dict] = {}
    min_hold = int(recipe.get("hold") or 1)
    sell_mode = recipe.get("sell") or "list"
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

    def liability(session: str, which: str) -> float:
        total = 0.0
        for lot in pos.values():
            mark = _px(price, lot["ticker"], session, which, lot.get("last_px") or lot["entry_px"])
            total -= lot["shares"] * mark
        return total

    for day in days:
        session = day["session"]
        by_ticker = {row["ticker"]: row for row in day["rows"]}
        chosen = pick_day(day["rows"], recipe)
        tset = {row["ticker"] for row in chosen}
        bought = []
        sold = []
        for ticker, lot in list(pos.items()):
            opx = price(ticker, session, "open")
            if opx is None or opx <= 0:
                continue
            prior = float(lot.get("close_px") or lot["entry_px"])
            move = lot["shares"] * (prior - float(opx))
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
            lot["last_px"] = opx
            lot_min = int(lot.get("min_hold") or min_hold)
            do_sell, _kind = lot_should_sell(
                lot, held=held, min_hold=lot_min, early=early,
                dropped=ticker not in tset, sell_mode=sell_mode, px=opx,
                side="short", take_pct=None, stop_pct=None,
            )
            if not do_sell:
                continue
            shares = lot["shares"]
            fee = float(order_fees(shares, opx, "buy", fees))
            fee_b = float(fee_15(shares, opx))
            cash -= shares * opx + fee
            cash_15 -= shares * opx + fee_b
            add_pnl(session, ticker, -fee)
            lot["marked"] = lot.get("marked", 0.0) - fee
            closed.append({
                "entry": lot["entry"],
                "entry_px": lot["entry_px"],
                "exit": session,
                "pnl": lot["marked"],
                "ticker": ticker,
                "win": lot["marked"] > 0,
            })
            sold.append(ticker)
            pos.pop(ticker)
        new = [row for row in chosen if row["ticker"] not in pos]
        open_equity = cash + liability(session, "open")
        room = max(0.0, open_equity) * 0.5
        if new and room > 0:
            per = room / len(new)
            for row in new:
                ticker = row["ticker"]
                opx = price(ticker, session, "open")
                if opx is None or opx <= 0:
                    continue
                opx = float(opx)
                shares = int(per // opx)
                if shares < 1:
                    continue
                fee = float(order_fees(shares, opx, "sell", fees))
                fee_b = float(fee_15(shares, opx))
                cash += shares * opx - fee
                cash_15 += shares * opx - fee_b
                pos[ticker] = {
                    "close_px": None,
                    "entry": session,
                    "entry_px": opx,
                    "last_px": opx,
                    "marked": -fee,
                    "min_hold": min_hold,
                    "shares": shares,
                    "ticker": ticker,
                }
                add_pnl(session, ticker, -fee)
                first.setdefault(ticker, session)
                bought.append(ticker)
        for lot in pos.values():
            cpx = _px(price, lot["ticker"], session, "close", lot.get("last_px") or lot["entry_px"])
            opx = _px(price, lot["ticker"], session, "open", lot["last_px"])
            move = lot["shares"] * (opx - cpx)
            borrow = lot["shares"] * cpx * BORROW_DAY
            cash -= borrow
            cash_15 -= borrow
            add_pnl(session, lot["ticker"], move - borrow)
            lot["marked"] = lot.get("marked", 0.0) + move - borrow
            lot["close_px"] = cpx
            lot["last_px"] = cpx
        equity = cash + liability(session, "close")
        equity_15 = cash_15 + liability(session, "close")
        ret = (equity / prev - 1.0) if prev else 0.0
        ret_15 = (equity_15 / prev_15 - 1.0) if prev_15 else 0.0
        attributed = sum((pnl_by_day.get(session) or {}).values())
        if abs(attributed - (equity - prev)) > 0.05:
            raise SystemExit(
                f"short pnl drift {recipe.get('id')} {session} {attributed} vs {equity - prev}"
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
    return {
        "closed": closed,
        "daily": daily,
        "first": first,
        "pnl_by_day": pnl_by_day,
        "start_equity": CAPITAL,
    }
