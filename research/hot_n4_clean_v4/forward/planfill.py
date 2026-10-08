"""Plan a session before the open, and fill it after the close.

A plan uses the locked rank and sell functions and no print from session
D. A fill is the plan's buys and sells at D's open. It does not edit the
plan. Fees and the liquidity cap are the same functions the v4 score uses.

From 2026-10-09 the h1 book's new-buy share count is the count locked at
send time (``forward.share_lock``). Price, cost, and fees still use the
session open. Earlier open fills, including 2026-10-08, stay sized from
that open.
"""
from __future__ import annotations

import bisect
import math

from research.hot_n4_clean_v4.forward.share_lock import AUTO, resolve_share_lock
from research.hot_n4_clean_v4.forward.step import _recipe, _variant, sell_reason, state_from_session
from research.hot_n4_clean_v4.protocol import (
    ADV_SHARE_SCALE,
    GAP_DAY,
    HOLD,
    HOLDUP_S,
    HOLDUP_SESS,
    LIQ_CAP_FRAC,
    SLIP_PRIMARY,
)
from research.hot_n4_clean_v4.run_study import (
    Halt,
    buy_shares,
    classify,
    halt_legs,
    mark_px,
    open_px,
    removal_legs,
    scan_legs,
    schedule_buy_cost,
    schedule_sell_proceeds,
    skip_reason_buy,
    skip_reason_sell,
)
from src.factor_mine import pick_day, should_exit
from src.factor_mine_book import lot_should_sell, split_budgets
from src.paper_trade import order_fees


def book_state(records: list[dict]) -> dict:
    """Cash and lots after the last seeded session or the last fill.

    An ``open_fill_correction`` replaces that date's open fill for cash and
    lots. A later close mark still wins, because the mark is the book after
    the close. The sealed open-fill line is not edited.
    """
    last = None
    for record in records:
        kind = record.get("kind")
        if kind in ("session", "fill", "open_fill", "mark"):
            last = record
        elif (
            kind == "open_fill_correction"
            and last is not None
            and last.get("kind") == "open_fill"
            and last.get("date") == record.get("date")
            and isinstance(record.get("corrected"), dict)
        ):
            last = dict(record["corrected"])
            if record.get("sha256"):
                last["sha256"] = record["sha256"]
    if last is None:
        from research.hot_n4_clean_v4.forward.step import fresh_state
        return fresh_state()
    return state_from_session(last)


def book_state_before(records: list[dict], day: str) -> dict:
    """Cash and lots before ``day`` is planned or filled."""
    trimmed = [
        row for row in records
        if not (
            row.get("date") == day
            and row.get("kind") in ("plan", "open_fill", "open_fill_correction", "fill", "mark", "close")
        )
    ]
    return book_state(trimmed)


def hide_session(bars: dict, session: str) -> dict:
    """Drop prints on or after ``session`` so a plan cannot see day D."""
    out = {}
    for side in ("feat", "stored"):
        copied = {}
        for ticker, blob in bars[side].items():
            dates = blob["date"]
            i = bisect.bisect_left(dates, session)
            if i == len(dates):
                copied[ticker] = blob
                continue
            sliced = {}
            for key, value in blob.items():
                if key == "adjusted":
                    sliced[key] = value
                else:
                    sliced[key] = value[:i]
            copied[ticker] = sliced
        out[side] = copied
    return out


def build_plan(payload: dict, state: dict, index: dict[str, int]) -> dict:
    """Ranked picks and planned sells. No open, fill, or equity from D."""
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
    pos = state["pos"]
    planned = []
    for ticker in sorted(pos):
        lot = pos[ticker]
        row = row_by.get(ticker) or {}
        early = should_exit(row, variant.get("exit_when") or {})
        held_n = index[session] - index[lot["entry_date"]]
        do_sell, kind = lot_should_sell(
            lot, held=held_n, min_hold=int(lot["min_hold"]), early=early,
            dropped=ticker not in chosen_names, sell_mode=variant["sell"],
            px=None, side="long", take_pct=None, stop_pct=None,
        )
        if not do_sell:
            continue
        planned.append({
            "reason": sell_reason(int(lot["min_hold"]), held_n, kind),
            "shares": int(lot["shares"]),
            "ticker": ticker,
        })
    picks = []
    for row in chosen:
        ticker = row["ticker"]
        picks.append({
            "fv_avg_volume": row.get("fv_avg_volume"),
            "fv_price": row.get("fv_price"),
            "rank": rank_of[ticker],
            "sources": list(row.get("sources") or []),
            "ticker": ticker,
        })
    picks.sort(key=lambda row: (row["rank"], row["ticker"]))
    planned.sort(key=lambda row: row["ticker"])
    return {
        "bar_cutoff": payload["bar_cutoff"],
        "cash_before": float(state["cash"]),
        "date": session,
        "excluded_unexplained_legs": list(payload.get("excluded_unexplained_legs") or []),
        "holdup_on": holdup,
        "kind": "plan",
        "morning_s": morning,
        "picks": picks,
        "planned_sells": planned,
        "recipe": _recipe(),
    }


def _cap_shares(op: float, fv_price, fv_adv) -> int | None:
    if fv_price is None or fv_adv is None or fv_price <= 0 or fv_adv <= 0 or op <= 0:
        return None
    cap_dollars = LIQ_CAP_FRAC * float(fv_price) * float(fv_adv) * ADV_SHARE_SCALE
    return int(math.floor(cap_dollars / op + 1e-12))


def _halt_on(stored: dict, session: str, held: set[str]) -> None:
    iwm = [leg for leg in scan_legs(stored, "IWM", session) if classify(leg) != "split"]
    if iwm:
        raise Halt(
            f"IWM {session} unexplained leg {iwm[0]['leg']} on {iwm[0]['bar_date']}"
        )
    for ticker in sorted(held):
        bad = halt_legs(stored, ticker, session)
        if bad:
            raise Halt(
                f"held {ticker} {session} unexplained {bad[0]['leg']} on {bad[0]['bar_date']}"
            )


def fill_book(plan: dict, state: dict, bars: dict, fees: dict, index: dict[str, int], *, share_lock=AUTO):
    """Fill ``plan`` at D's open. Raises Halt when a held name or IWM jumps.

    ``share_lock`` is the sent share count. When it is set, a different
    open changes the fill price, cost, and fee, not the share count.
    Carries are not in the lock and are not resized. ``AUTO`` reads the
    h1 send journal from 2026-10-09 on and leaves every earlier session
    on the open-price size.
    """
    if abs(float(state["cash"]) - float(plan["cash_before"])) > 1e-4:
        raise RuntimeError("cash does not match the plan")
    stored = bars["stored"]
    session = plan["date"]
    held_now = set(state["pos"])
    _halt_on(stored, session, held_now)
    picks = list(plan["picks"])
    planned = {row["ticker"]: row for row in plan["planned_sells"]}
    morning = plan.get("morning_s")
    holdup = bool(plan["holdup_on"])
    cash = float(state["cash"])
    pos = {ticker: dict(lot) for ticker, lot in state["pos"].items()}
    sells = []
    closes = []
    unfilled = []
    excluded_on_fill = []
    for ticker in sorted(planned):
        if ticker not in pos:
            raise RuntimeError(f"planned sell {ticker} is not held")
        lot = pos[ticker]
        row = planned[ticker]
        if int(row["shares"]) != int(lot["shares"]):
            raise RuntimeError(f"planned sell {ticker} shares do not match the lot")
        op = open_px(stored, ticker, session)
        if op is None:
            unfilled.append({
                "reason": skip_reason_sell(stored, ticker, session),
                "side": "sell",
                "ticker": ticker,
            })
            continue
        proceeds = schedule_sell_proceeds(lot["shares"], op, fees)
        pnl = proceeds[SLIP_PRIMARY] - float(lot["cost_primary"])
        cash += proceeds[SLIP_PRIMARY]
        fee = order_fees(int(lot["shares"]), op, "sell", fees)
        sells.append({
            "fee": fee,
            "fill": op,
            "reason": row["reason"],
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
            "reason": row["reason"],
            "recipe": _recipe(),
            "shares": int(lot["shares"]),
            "ticker": ticker,
        })
        del pos[ticker]
    for ticker, lot in pos.items():
        if ticker in planned:
            continue
        op = open_px(stored, ticker, session)
        if op is None:
            continue
        lot["last_px"] = op
        lot["peak_px"] = max(float(lot.get("peak_px") or op), op)
    new = []
    for pick in sorted(picks, key=lambda row: (row["rank"], row["ticker"])):
        ticker = pick["ticker"]
        if ticker in pos:
            continue
        legs = [
            leg for leg in removal_legs(stored, ticker, session)
            if leg["bar_date"] == session
        ]
        if legs:
            excluded_on_fill.extend(legs)
            unfilled.append({
                "reason": f"unexplained {legs[0]['leg']} on {session}",
                "side": "buy",
                "ticker": ticker,
            })
            continue
        new.append(pick)
    if session == GAP_DAY:
        for pick in new:
            unfilled.append({"reason": "gap day", "side": "buy", "ticker": pick["ticker"]})
        new = []
    lock = resolve_share_lock(session, share_lock)
    if lock is not None:
        carry = {pick["ticker"] for pick in picks} & set(pos)
        overlap = sorted(set(lock) & carry)
        if overlap:
            raise RuntimeError(
                "sent share lock includes carry "
                + ", ".join(overlap)
                + "; refusing to resize"
            )
    budgets = split_budgets(new, cash, "leftover") if new else []
    buys = []
    for pick, budget in zip(new, budgets):
        ticker = pick["ticker"]
        blocked = skip_reason_buy(stored, ticker, session)
        op = open_px(stored, ticker, session)
        cap = _cap_shares(op, pick.get("fv_price"), pick.get("fv_avg_volume")) if op else None
        if blocked:
            unfilled.append({"reason": blocked, "side": "buy", "ticker": ticker})
            continue
        if lock is None:
            shares, why = buy_shares(
                budget, cash, op, pick.get("fv_price"), pick.get("fv_avg_volume"), fees,
            )
            if why:
                row = {"reason": why, "side": "buy", "ticker": ticker}
                if cap is not None:
                    row["liquidity_cap_shares"] = cap
                unfilled.append(row)
                continue
        else:
            if ticker not in lock:
                raise RuntimeError(
                    f"sent share lock has no count for {ticker}; refusing to size from the open"
                )
            try:
                shares = int(lock[ticker])
            except (TypeError, ValueError) as exc:
                raise RuntimeError(
                    f"sent share lock for {ticker} is not a share count"
                ) from exc
            if shares < 1:
                raise RuntimeError(
                    f"sent share lock for {ticker} is {shares}; refusing to size from the open"
                )
        cost = schedule_buy_cost(shares, op, fees)
        if lock is not None and cost[SLIP_PRIMARY] > cash + 1e-6:
            raise RuntimeError(
                f"sent share lock {ticker} {shares} at open {op} costs "
                f"{cost[SLIP_PRIMARY]} and book cash is {cash}; refusing to resize"
            )
        cash -= cost[SLIP_PRIMARY]
        fee = order_fees(int(shares), op, "buy", fees)
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
            "fee": fee,
            "fill": op,
            "fill_basis": "open",
            "liquidity_cap_shares": cap,
            "rank": int(pick["rank"]),
            "shares": int(shares),
            "sources": list(pick.get("sources") or []),
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
    buys.sort(key=lambda row: (row["rank"], row["ticker"]))
    sells.sort(key=lambda row: row["ticker"])
    unfilled.sort(key=lambda row: (row["side"], row["ticker"]))
    excluded_on_fill.sort(key=lambda row: (row["ticker"], row["bar_date"], row["leg"]))
    fill = {
        "buys": buys,
        "cash_primary": cash,
        "cost_model": "futubull+0.5%/side",
        "date": session,
        "equity_primary": cash + stock,
        "excluded_on_fill": excluded_on_fill,
        "holdings": holdings,
        "holdup_on": holdup,
        "kind": "fill",
        "morning_s": morning,
        "plan_sha256": plan["sha256"],
        "recipe": _recipe(),
        "sells": sells,
        "unfilled": unfilled,
    }
    return fill, closes, {"cash": cash, "pos": pos}
