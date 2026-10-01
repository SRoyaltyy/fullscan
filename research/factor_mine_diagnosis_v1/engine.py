"""Factor Mine cash book for the diagnosis.

Share counts, min-hold, list-drop, holdup, and the red-day sit follow
``src.factor_mine_book``. A missing open keeps its slot and is not
replaced. Feature dates have to be before the session.
"""
from __future__ import annotations

import random
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from research.factor_mine_diagnosis_v1.protocol import (  # noqa: E402
    CAPITAL,
    HOLDUP_S,
    HOLDUP_SESS,
    RANDOM_N,
    RANDOM_SEED,
    SORT_SEED,
    hard_red,
    prior_feature_dates,
)
from src.factor_mine import matches, pick_day, rank_key, should_exit  # noqa: E402
from src.factor_mine_book import lot_should_sell, split_budgets  # noqa: E402
from src.lever_search_score import fee_15  # noqa: E402
from src.paper_trade import order_fees  # noqa: E402

BORROW_ANNUAL = 0.01


def choose(rows: list[dict], recipe: dict) -> list[dict]:
    mode = recipe.get("rank_mode") or "recipe"
    top_n = int(recipe.get("top_n") or 8)
    if mode == "random_sort":
        matched = [row for row in rows if matches(row, recipe)]
        matched.sort(key=lambda row: row["ticker"])
        random.Random(SORT_SEED).shuffle(matched)
        return matched[:top_n]
    if mode == "sample":
        pool = sorted(rows, key=lambda row: row["ticker"])
        draw = int(recipe.get("draw") or 0)
        rng = random.Random(RANDOM_SEED + draw)
        k = min(RANDOM_N, len(pool))
        return rng.sample(pool, k) if k else []
    spec = dict(recipe)
    if mode == "list":
        spec["rank"] = None
    return pick_day(rows, spec)


def _stock(pos: dict, session: str, which: str, side: str, price) -> float:
    total = 0.0
    for lot in pos.values():
        px = _mark_px(lot, session, which, price)
        notional = lot["shares"] * px
        total += notional if side == "long" else -notional
    return total


def _mark_px(lot: dict, session: str, which: str, price) -> float:
    px = price(lot["ticker"], session, which)
    if px is not None and px > 0:
        return float(px)
    if which == "close":
        return float(lot.get("close_px") or lot.get("last_px") or lot["entry_px"])
    return float(lot.get("last_px") or lot["entry_px"])


def walk(days: list[dict], recipe: dict, fees: dict, price) -> dict:
    """One continuous $10k book. ``days`` is the full session list in order."""
    cash = CAPITAL
    cash_15 = CAPITAL
    pos: dict[str, dict] = {}
    side = recipe.get("side") or "long"
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
    buys = []

    def add_pnl(session: str, ticker: str, amount: float) -> None:
        bucket = pnl_by_day.setdefault(session, {})
        bucket[ticker] = bucket.get(ticker, 0.0) + float(amount)

    for day in days:
        session = day["session"]
        score = day.get("s")
        sit = weather and hard_red(score)
        holdup = (
            boost == "holdup" and side == "long" and score is not None
            and float(score) > HOLDUP_S and not sit
        )
        by_ticker = {row["ticker"]: row for row in day["rows"]}
        chosen = choose(day["rows"], recipe)
        tset = {row["ticker"] for row in chosen}
        gap = 0.0
        session_dollars = 0.0
        first_dollars = 0.0
        later_dollars = 0.0
        bought = []
        sold = []
        for ticker, lot in list(pos.items()):
            opx = price(ticker, session, "open")
            if opx is None or opx <= 0:
                continue
            prior = float(lot.get("close_px") or lot["entry_px"])
            move = lot["shares"] * (float(opx) - prior)
            if side == "short":
                move = -move
            gap += move
            lot["gap_pnl"] = lot.get("gap_pnl", 0.0) + move
            lot["marked"] = lot.get("marked", 0.0) + move
            add_pnl(session, ticker, move)
            if lot["entry"] != session:
                lot["later_pnl"] = lot.get("later_pnl", 0.0) + move
                later_dollars += move
            else:
                first_dollars += move
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
            if side == "long":
                lot["peak_px"] = max(float(lot.get("peak_px") or lot["entry_px"]), opx)
            else:
                lot["peak_px"] = min(float(lot.get("peak_px") or lot["entry_px"]), opx)
            lot["last_px"] = opx
            lot_min = int(lot.get("min_hold") or min_hold)
            if lot.get("unpriced"):
                do_sell, kind = True, "unpriced"
            else:
                do_sell, kind = lot_should_sell(
                    lot, held=held, min_hold=lot_min, early=early,
                    dropped=ticker not in tset, sell_mode=sell_mode, px=opx,
                    side=side, take_pct=recipe.get("take_pct"), stop_pct=recipe.get("stop_pct"),
                )
            if not do_sell:
                continue
            shares = lot["shares"]
            fee = float(order_fees(shares, opx, "sell" if side == "long" else "buy", fees))
            fee_b = float(fee_15(shares, opx))
            if side == "long":
                cash += shares * opx - fee
                cash_15 += shares * opx - fee_b
                pnl = shares * opx - fee - lot["cost"]
            else:
                cover = shares * opx + fee
                cash -= cover
                cash_15 -= shares * opx + fee_b
                pnl = lot["notional"] - cover - lot["fee_in"]
            add_pnl(session, ticker, pnl - lot.get("marked", 0.0))
            session_dollars -= fee
            if lot["entry"] == session:
                first_dollars -= fee
            else:
                later_dollars -= fee
            trade = {
                "days_on_list": lot["days_on_list"],
                "entry": lot["entry"],
                "entry_px": lot["entry_px"],
                "exit": session,
                "exit_px": opx,
                "gap_pnl": lot.get("gap_pnl", 0.0),
                "held": held,
                "later_pnl": lot.get("later_pnl", 0.0),
                "pnl": pnl,
                "ret": pnl / lot["notional"] if lot["notional"] else 0.0,
                "row": lot["row"],
                "s": lot["s"],
                "session_pnl": lot.get("session_pnl", 0.0) - fee,
                "shares": shares,
                "side": side,
                "ticker": ticker,
                "win": pnl > 0,
            }
            trade["first_day_pnl"] = lot.get("first_day_pnl", 0.0)
            closed.append(trade)
            sold.append(ticker)
            pos.pop(ticker)
        new = [row for row in chosen if row["ticker"] not in pos]
        if sit:
            new = []
        if new and (cash > 0 or side == "short"):
            room = cash if side == "long" else max(0.0, (cash + _stock(pos, session, "open", side, price)) * 0.5)
            budgets = split_budgets(new, max(0.0, room), recipe.get("size") or "leftover")
            for row, per in zip(new, budgets):
                ticker = row["ticker"]
                opx = row.get("open")
                if opx is None or float(opx) <= 0:
                    continue
                opx = float(opx)
                shares = int(per // opx)
                if shares < 1:
                    continue
                if side == "long":
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
                    lot = {
                        "close_px": None,
                        "cost": cost,
                        "days_on_list": int(row.get("days_on_list") or 1),
                        "entry": session,
                        "entry_px": opx,
                        "fee_in": fee,
                        "first_day_pnl": -fee,
                        "gap_pnl": 0.0,
                        "last_px": opx,
                        "later_pnl": 0.0,
                        "marked": -fee,
                        "min_hold": max(min_hold, HOLDUP_SESS) if holdup else min_hold,
                        "notional": shares * opx,
                        "peak_px": opx,
                        "row": row["feat"],
                        "s": score,
                        "session_pnl": -fee,
                        "shares": shares,
                        "ticker": ticker,
                    }
                    add_pnl(session, ticker, -fee)
                    session_dollars -= fee
                    first_dollars -= fee
                else:
                    notional = shares * opx
                    equity_now = cash + _stock(pos, session, "open", side, price)
                    if equity_now < 2 * notional:
                        continue
                    borrow = notional * BORROW_ANNUAL / 365.0
                    fee = float(order_fees(shares, opx, "sell", fees)) + borrow
                    fee_b = float(fee_15(shares, opx))
                    cash += notional - fee
                    cash_15 += notional - fee_b
                    lot = {
                        "close_px": None,
                        "cost": fee,
                        "days_on_list": int(row.get("days_on_list") or 1),
                        "entry": session,
                        "entry_px": opx,
                        "fee_in": fee,
                        "first_day_pnl": -fee,
                        "gap_pnl": 0.0,
                        "last_px": opx,
                        "later_pnl": 0.0,
                        "marked": -fee,
                        "min_hold": min_hold,
                        "notional": notional,
                        "peak_px": opx,
                        "row": row["feat"],
                        "s": score,
                        "session_pnl": -fee,
                        "shares": shares,
                        "ticker": ticker,
                    }
                    add_pnl(session, ticker, -fee)
                    session_dollars -= fee
                    first_dollars -= fee
                pos[ticker] = lot
                first.setdefault(ticker, session)
                bought.append(ticker)
                buys.append({"session": session, "ticker": ticker})
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
            if side == "short":
                move = -move
            session_dollars += move
            lot["session_pnl"] = lot.get("session_pnl", 0.0) + move
            if lot["entry"] == session:
                lot["first_day_pnl"] = lot.get("first_day_pnl", 0.0) + move
                first_dollars += move
            else:
                lot["later_pnl"] = lot.get("later_pnl", 0.0) + move
                later_dollars += move
            add_pnl(session, lot["ticker"], move)
            lot["marked"] = lot.get("marked", 0.0) + move
            lot["close_px"] = cpx
            lot["last_px"] = cpx
        equity = cash + _stock(pos, session, "close", side, price)
        equity_15 = cash_15 + _stock(pos, session, "close", side, price)
        ret = (equity / prev - 1.0) if prev else 0.0
        ret_15 = (equity_15 / prev_15 - 1.0) if prev_15 else 0.0
        daily.append({
            "bought": bought,
            "equity": equity,
            "first_dollars": first_dollars,
            "gap": gap,
            "hard_red": sit,
            "later_dollars": later_dollars,
            "ret": ret,
            "ret_15": ret_15,
            "session": session,
            "session_dollars": session_dollars,
            "sold": sold,
        })
        prev = equity
        prev_15 = equity_15
    return {
        "buys": buys,
        "closed": closed,
        "daily": daily,
        "first": first,
        "pnl_by_day": pnl_by_day,
        "open": list(pos),
    }


def assert_engine_agreement() -> None:
    """The slim book sells the holdup lot on the same morning as simulate_book."""
    from src import factor_mine_book as fmb
    from src.factor_mine import make_recipe
    from src.paper_trade import load_fees

    cal = ["2026-09-02", "2026-09-03", "2026-09-04"]
    raw_rows = [
        {"date": "2026-09-02", "ticker": "AAA", "sources": ["union"], "src_rank": 0,
         "ohlc_hot_score": 9, "alarm": False, "boxes": {}},
        {"date": "2026-09-03", "ticker": "BBB", "sources": ["union"], "src_rank": 0,
         "ohlc_hot_score": 9, "alarm": False, "boxes": {}},
        {"date": "2026-09-04", "ticker": "BBB", "sources": ["union"], "src_rank": 0,
         "ohlc_hot_score": 9, "alarm": False, "boxes": {}},
    ]
    bars = {
        ("AAA", "2026-09-02"): {"open": 10, "close": 10},
        ("AAA", "2026-09-03"): {"open": 10.3, "close": 10.4},
        ("AAA", "2026-09-04"): {"open": 10.4, "close": 10.2},
        ("BBB", "2026-09-03"): {"open": 20, "close": 19},
        ("BBB", "2026-09-04"): {"open": 19, "close": 19},
    }
    panel = {
        "session_dates": cal,
        "rows": raw_rows,
        "by_date": {},
        "_ohlc_filled": True,
        "_tape_filled": True,
        "_clock_b": True,
        "_oppset": True,
    }
    for row in raw_rows:
        panel["by_date"].setdefault(row["date"], []).append(row)
    rec = make_recipe(
        "union_hot_n4_holdup", universe="union", hold=1, top_n=1, rank="hot_score",
        s_boost="holdup", forbid={"alarm": True},
    )
    fees = load_fees()
    book = fmb.simulate_book(
        panel, rec, bars=bars, fees=fees,
        regime={
            "2026-09-02": {"predict_score": 2.25},
            "2026-09-03": {"predict_score": -0.9},
            "2026-09-04": {"predict_score": 2.0},
        },
    )
    sells = [trade for trade in book["trades"] if trade.get("side") == "SELL"]
    if not sells or sells[0]["date"] != "2026-09-04" or sells[0]["ticker"] != "AAA":
        raise SystemExit(f"engine holdup sell moved: {sells[:1]}")
    days = []
    for session, score in (("2026-09-02", 2.25), ("2026-09-03", -0.9), ("2026-09-04", 2.0)):
        rows = []
        for row in panel["by_date"][session]:
            ticker = row["ticker"]
            px = {}
            for date in cal:
                bar = bars.get((ticker, date)) or {}
                if bar.get("open"):
                    px[(date, "open")] = bar["open"]
                if bar.get("close"):
                    px[(date, "close")] = bar["close"]
            rows.append({
                "ticker": ticker,
                "open": (bars.get((ticker, session)) or {}).get("open"),
                "feat": row,
                "px": px,
                "days_on_list": 1,
                "sources": row["sources"],
                "src_rank": row["src_rank"],
                "ohlc_hot_score": row["ohlc_hot_score"],
                "alarm": False,
                "boxes": {},
                **row,
            })
        days.append({"session": session, "s": score, "rows": rows})
    slim_rec = dict(rec)
    slim_rec["weather"] = True
    slim_rec["rank_mode"] = "recipe"
    def price(ticker: str, session: str, which: str):
        bar = bars.get((ticker, session)) or {}
        value = bar.get(which)
        if value is None or float(value) <= 0:
            return None
        return float(value)

    slim = walk(days, slim_rec, fees, price)
    if not slim["closed"] or slim["closed"][0]["exit"] != "2026-09-04":
        raise SystemExit(f"slim holdup sell {slim['closed'][:1]}")
    if abs(slim["closed"][0]["pnl"] - float(sells[0]["pnl"])) > 0.02:
        raise SystemExit(f"pnl {slim['closed'][0]['pnl']} != {sells[0]['pnl']}")
    prior_feature_dates(["2026-09-01"], "2026-09-02")
    try:
        prior_feature_dates(["2026-09-02"], "2026-09-02")
    except Exception as exc:  # noqa: BLE001 — the leak must raise
        if exc.__class__.__name__ != "SameDayLeak":
            raise
    else:
        raise SystemExit("same-day feature was accepted")


def ranked_order(rows: list[dict], recipe: dict) -> list[str]:
    """Exposed so a test can see that list order is src_rank, not a shuffle."""
    ordered = sorted(rows, key=lambda row: rank_key(row, recipe))
    return [row["ticker"] for row in ordered]
