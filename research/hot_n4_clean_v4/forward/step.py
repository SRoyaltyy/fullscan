"""One holdup session, using the locked v4 functions.

The stepper does not reimplement ``pick_day``, ``lot_should_sell``,
``split_budgets``, ``buy_shares``, or the fee schedule. A sell's realised
P&L is returned as a separate close body. It is not written onto the buy.
"""
from __future__ import annotations

from research.hot_n4_clean_v4.forward.book import current_book
from research.hot_n4_clean_v4.forward.ledger import RECIPE
from research.hot_n4_clean_v4.protocol import (
    CAPITAL,
    GAP_DAY,
    HOLD,
    HOLDUP_S,
    HOLDUP_SESS,
    SLIP_PRIMARY,
    VARIANTS,
)
from research.hot_n4_clean_v4.run_study import (
    buy_shares,
    mark_px,
    open_px,
    schedule_buy_cost,
    schedule_sell_proceeds,
    skip_reason_buy,
    skip_reason_sell,
)
from src.factor_mine import pick_day, should_exit
from src.factor_mine_book import lot_should_sell, split_budgets

VARIANT = next(row for row in VARIANTS if row["id"] == RECIPE)


def _recipe() -> str:
    return current_book().recipe


def _variant() -> dict:
    return next(row for row in VARIANTS if row["id"] == _recipe())


def sell_reason(min_hold: int, held_n: int, kind: str) -> str:
    """Map the engine kind onto the locked hold language.

    A list-drop inside the extra holdup session is a holdup extension.
    Every other filled list-drop is a hold that has expired.
    """
    if kind == "dropped" and int(min_hold) > HOLD and held_n <= int(min_hold):
        return "holdup extension"
    if kind == "dropped":
        return "hold-expired"
    return str(kind)


def fresh_state() -> dict:
    return {"cash": float(CAPITAL), "pos": {}}


def state_from_session(record: dict) -> dict:
    pos = {}
    for lot in record["holdings"]:
        pos[lot["ticker"]] = {
            "cost_primary": float(lot["cost_primary"]),
            "entry_date": lot["entry_date"],
            "entry_px": float(lot["entry_px"]),
            "last_px": float(lot["last_px"]),
            "min_hold": int(lot["min_hold"]),
            "peak_px": float(lot["peak_px"]),
            "shares": int(lot["shares"]),
            "ticker": lot["ticker"],
        }
    return {"cash": float(record["cash_primary"]), "pos": pos}


def step(payload: dict, state: dict, bars: dict, fees: dict, index: dict[str, int]) -> tuple[dict, list[dict], dict]:
    """Advance one session. ``payload`` is a committed day card or the same shape."""
    stored = bars["stored"]
    session = payload["session"]
    variant = _variant()
    rows = [row for row in payload["candidates"]]
    chosen = pick_day(rows, variant)
    chosen_names = {row["ticker"] for row in chosen}
    row_by = {row["ticker"]: row for row in rows}
    rank_of = {row["ticker"]: rank for rank, row in enumerate(chosen, start=1)}
    morning = payload.get("morning_s")
    holdup = (
        variant["s_boost"] == "holdup"
        and morning is not None
        and float(morning) > HOLDUP_S
    )
    cash = float(state["cash"])
    pos = {ticker: dict(lot) for ticker, lot in state["pos"].items()}
    sells = []
    closes = []
    unfilled = []
    for ticker in sorted(pos):
        lot = pos[ticker]
        op = open_px(stored, ticker, session)
        row = row_by.get(ticker) or {}
        early = should_exit(row, variant.get("exit_when") or {})
        held_n = index[session] - index[lot["entry_date"]]
        do_sell, kind = lot_should_sell(
            lot, held=held_n, min_hold=int(lot["min_hold"]), early=early,
            dropped=ticker not in chosen_names, sell_mode=variant["sell"],
            px=op, side="long", take_pct=None, stop_pct=None,
        )
        if op is None:
            if do_sell:
                unfilled.append({
                    "reason": skip_reason_sell(stored, ticker, session),
                    "side": "sell",
                    "ticker": ticker,
                })
            continue
        if not do_sell:
            lot["last_px"] = op
            lot["peak_px"] = max(float(lot.get("peak_px") or op), op)
            continue
        proceeds = schedule_sell_proceeds(lot["shares"], op, fees)
        pnl = proceeds[SLIP_PRIMARY] - float(lot["cost_primary"])
        cash += proceeds[SLIP_PRIMARY]
        reason = sell_reason(int(lot["min_hold"]), held_n, kind)
        sells.append({
            "fill": op,
            "reason": reason,
            "shares": int(lot["shares"]),
            "ticker": ticker,
        })
        closes.append({
            "cost_model": "futubull+0.5%/side",
            "date": session,
            "entry_date": lot["entry_date"],
            "entry_px": float(lot["entry_px"]),
            "fill": op,
            "kind": "close",
            "pnl_primary": pnl,
            "reason": reason,
            "recipe": _recipe(),
            "shares": int(lot["shares"]),
            "ticker": ticker,
        })
        del pos[ticker]
    new = [row for row in chosen if row["ticker"] not in pos]
    if session == GAP_DAY:
        new = []
    budgets = split_budgets(new, cash, "leftover") if new else []
    buys = []
    for row, budget in zip(new, budgets):
        ticker = row["ticker"]
        blocked = skip_reason_buy(stored, ticker, session)
        if blocked:
            unfilled.append({"reason": blocked, "side": "buy", "ticker": ticker})
            continue
        op = open_px(stored, ticker, session)
        shares, why = buy_shares(
            budget, cash, op, row.get("fv_price"), row.get("fv_avg_volume"), fees,
        )
        if why:
            unfilled.append({"reason": why, "side": "buy", "ticker": ticker})
            continue
        cost = schedule_buy_cost(shares, op, fees)
        cash -= cost[SLIP_PRIMARY]
        pos[ticker] = {
            "cost_primary": cost[SLIP_PRIMARY],
            "entry_date": session,
            "entry_px": op,
            "last_px": op,
            "min_hold": max(HOLD, HOLDUP_SESS) if holdup else HOLD,
            "peak_px": op,
            "shares": shares,
            "ticker": ticker,
        }
        buys.append({
            "fill": op,
            "fill_basis": "open",
            "rank": rank_of[ticker],
            "shares": int(shares),
            "sources": list(row.get("sources") or []),
            "ticker": ticker,
        })
    stock = 0.0
    for ticker, lot in pos.items():
        px = mark_px(stored, ticker, session)
        if px is None:
            px = float(lot.get("last_px") or lot["entry_px"])
        stock += lot["shares"] * px
    holdings = []
    for ticker in sorted(pos):
        lot = pos[ticker]
        holdings.append({
            "cost_primary": lot["cost_primary"],
            "entry_date": lot["entry_date"],
            "entry_px": lot["entry_px"],
            "last_px": lot["last_px"],
            "min_hold": int(lot["min_hold"]),
            "peak_px": lot["peak_px"],
            "shares": int(lot["shares"]),
            "ticker": ticker,
        })
    session_body = {
        "bar_cutoff": payload["bar_cutoff"],
        "buys": buys,
        "cash_primary": cash,
        "date": session,
        "day_card_sha256": payload.get("day_card_sha256"),
        "equity_primary": cash + stock,
        "excluded_unexplained_legs": payload.get("excluded_unexplained_legs") or [],
        "holdings": holdings,
        "holdup_on": holdup,
        "kind": "session",
        "morning_s": morning,
        "recipe": _recipe(),
        "sells": sells,
        "unfilled": unfilled,
    }
    return session_body, closes, {"cash": cash, "pos": pos}
