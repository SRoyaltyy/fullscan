"""Factor-mine recipe combinations on the real $10k cash book.

A combo is not a new pick gate. It is one or more existing leak-free
recipes sharing a ledger:

  * 09:30 picks only (each member's ``pick_day``)
  * official 09:30 / 16:00 marks (``_px`` — no open↔close substitute)
  * whole shares, Futubull fees, sell first, hard-red S≤−3 sit
  * each lot keeps its owner's min-hold and list-drop rule
  * one ticker, one side — no long+short and no double-buy

``shared`` pool: one cash pile. After sells, leftover is offered to
members in claim order so idle cash from a thin rifle can fund the
next sleeve. ``split`` pool: independent books at the stated weights
(same audited ``simulate_book``), then summed.

Research only — does not change live flatten_robust.
"""
from __future__ import annotations

import argparse
import json
from itertools import combinations
from pathlib import Path

from . import factor_mine as fm
from . import factor_mine_book as fmb
from . import paper_trade as pt
from . import ticker_lookback as tl

ROOT = Path(__file__).resolve().parent.parent
OUT_JSON = ROOT / "03_scoreboard" / "factor_mine_combos.json"
CAPITAL = fm.CAPITAL
HARD_RED = fmb.HARD_RED
OUTPERFORM_RULE = (
    "A combo outperforms its members on the cash book when the audit passes "
    "and either (1) Book% is strictly higher than every member, or (2) Book% "
    "stays within 2pp of the richest member, max drawdown is strictly smaller "
    "than every member's drawdown, start-rate is at least the best member, "
    "both halves are green, and the worst session is no worse than the worst "
    "member session. Effectiveness / Signal% do not decide WIN."
)

# Members we actually have a reason to mix (elite + the user's three).
MEMBER_POOL = (
    "short_news_r_h3",
    "short_news_r_h1",
    "union_e_fresh_h3",
    "union_e_fresh_h1",
    "union_hot_n4_h1",
    "union_news_g_h1",
    "union_join_vol_green_h1",
    "union_earn_react_h3",
    "flatten_h5",
    "union_h5",
    "short_alarm_h3",
)

# Claim order when two members want the same name: earlier wins.
CLAIM = (
    "union_e_fresh_h3",
    "union_e_fresh_h1",
    "union_earn_react_h3",
    "union_hot_n4_h1",
    "union_join_vol_green_h1",
    "union_news_g_h1",
    "flatten_h5",
    "union_h5",
    "short_news_r_h3",
    "short_news_r_h1",
    "short_alarm_h3",
)


def combo_specs() -> list[dict]:
    """Weight / net / pool grid. Names stay stable for the dashboard."""
    S, E, H = "short_news_r_h3", "union_e_fresh_h3", "union_hot_n4_h1"
    N, J, F = "union_news_g_h1", "union_join_vol_green_h1", "flatten_h5"
    ER, E1 = "union_earn_react_h3", "union_e_fresh_h1"
    elite = (S, E, H, N, J, F, E1, ER)
    abbr = {
        S: "s", E: "e", H: "h", N: "n", J: "j", F: "f",
        E1: "e1", ER: "er",
    }
    specs = []
    seen: set[str] = set()

    def add(name, members, weights, *, net="priority", pool="shared"):
        if name in seen:
            return
        seen.add(name)
        specs.append({
            "name": name,
            "members": list(members),
            "weights": [float(w) for w in weights],
            "net": net,
            "pool": pool,
        })

    # The mixes discussed on the phone — names stay stable.
    add("combo_seh_333_shared", [S, E, H], [1, 1, 1])
    add("combo_seh_333_split", [S, E, H], [1, 1, 1], pool="split")
    add("combo_seh_502525_shared", [S, E, H], [50, 25, 25])
    add("combo_seh_502525_split", [S, E, H], [50, 25, 25], pool="split")
    add("combo_seh_404020_shared", [S, E, H], [40, 40, 20])
    add("combo_seh_403525_shared", [S, E, H], [40, 35, 25])
    add("combo_seh_451540_shared", [S, E, H], [45, 15, 40])
    add("combo_seh_601525_shared", [S, E, H], [60, 15, 25])
    add("combo_se_5050_shared", [S, E], [1, 1])
    add("combo_se_7030_shared", [S, E], [70, 30])
    add("combo_eh_5050_shared", [E, H], [1, 1])
    add("combo_sh_5050_shared", [S, H], [1, 1])
    add("combo_se_5050_skip", [S, E], [1, 1], net="skip")
    add("combo_seh_333_skip", [S, E, H], [1, 1, 1], net="skip")
    add("combo_seh_333_weather", [S, E, H], [1, 1, 1], net="weather")
    add("combo_se_5050_weather", [S, E], [1, 1], net="weather")
    add("combo_se_5050_split", [S, E], [1, 1], pool="split")
    # Spill-heavy: event rifle first, short mops leftover cash.
    add("combo_es_8020_shared", [E, S], [80, 20])
    add("combo_es_9010_shared", [E, S], [90, 10])
    add("combo_ehs_702010_shared", [E, H, S], [70, 20, 10])
    add("combo_ehs_601525_shared", [E, H, S], [60, 15, 25])
    # Calmer longs + short ballast.
    add("combo_sn_5050_shared", [S, N], [1, 1])
    add("combo_sj_5050_shared", [S, J], [1, 1])
    add("combo_sf_5050_shared", [S, F], [1, 1])
    add("combo_snj_333_shared", [S, N, J], [1, 1, 1])
    add("combo_nse_333_shared", [N, S, E], [1, 1, 1])
    add("combo_jse_333_shared", [J, S, E], [1, 1, 1])
    add("combo_fse_333_shared", [F, S, E], [1, 1, 1])
    add("combo_e1s_7030_shared", [E1, S], [70, 30])
    add("combo_ers_7030_shared", [ER, S], [70, 30])
    add("combo_eh_7030_shared", [E, H], [70, 30])
    add("combo_fh_7030_shared", [F, H], [70, 30])
    add("combo_fe_5050_shared", [F, E], [1, 1])
    add("combo_fes_403030_shared", [F, E, S], [40, 30, 30])
    for a, b in combinations(elite, 2):
        add(f"combo_{abbr[a]}{abbr[b]}_5050_shared", [a, b], [1, 1])
    for a, b in ((S, E), (S, H), (E, H), (S, N), (S, J), (S, F),
                 (E, N), (E, F), (H, N), (E, E1)):
        tag = f"{abbr[a]}{abbr[b]}"
        add(f"combo_{tag}_7030_shared", [a, b], [70, 30])
        add(f"combo_{tag}_3070_shared", [a, b], [30, 70])
    return specs


def _norm_w(weights: list[float]) -> list[float]:
    s = sum(float(w) for w in weights)
    if s <= 0:
        raise ValueError("combo weights must sum to > 0")
    return [float(w) / s for w in weights]


def _claim_rank(name: str) -> int:
    try:
        return CLAIM.index(name)
    except ValueError:
        return 100 + hash(name) % 50


def _mark_mixed(pos: dict, date: str, which: str, bars) -> float:
    tot = 0.0
    for lot in pos.values():
        px = fmb._lot_px(lot, date, which, bars)
        n = lot["shares"] * float(px)
        tot += n if lot.get("side") == "long" else -n
    return tot


def _stamp_mixed(rec_t: dict, cash: float, pos: dict, date: str,
                 bars, capital: float) -> None:
    stock = _mark_mixed(pos, date, "open", bars)
    eq = float(cash) + stock
    rec_t["equity_after"] = round(eq, 2)
    rec_t["equity_delta"] = round(eq - float(capital), 2)
    rec_t["stock_after"] = round(stock, 2)


def _overnight_mixed(pos: dict, date: str, bars, yday_equity: float,
                     open_cash: float) -> dict:
    names = []
    open_stock = 0.0
    bits = []
    for t, lot in pos.items():
        side = lot.get("side") or "long"
        shares = int(lot["shares"])
        yday_px = float(lot.get("close_px") or lot.get("last_px") or lot["entry_px"])
        opx = fmb._px(t, date, "open", bars)
        if opx is None:
            opx = yday_px
        opx = float(opx)
        dlt = fmb._signed_px_delta(shares, opx, yday_px, side)
        open_stock += shares * opx if side == "long" else -shares * opx
        entry = float(lot.get("entry_px") or yday_px)
        names.append({
            "ticker": t, "shares": shares,
            "yday_px": round(yday_px, 4), "open_px": round(opx, 4),
            "entry_px": round(entry, 4),
            "entry_date": lot.get("entry_date"),
            "delta": round(dlt, 2), "overnight": round(dlt, 2),
            "pct": fmb._pct_move(opx, yday_px, side),
            "vs_entry_open": round(
                fmb._signed_px_delta(shares, opx, entry, side), 2),
            "lot_side": side, "owner": lot.get("owner"),
        })
        bits.append(
            f"{t}×{shares} yday ${yday_px:.2f} → 09:30 ${opx:.2f} {dlt:+.2f}"
        )
    open_eq = float(open_cash) + open_stock
    overnight_delta = open_eq - float(yday_equity)
    why = (
        f"09:30 open · cash ${open_cash:,.2f} (unchanged overnight, no fees) · "
        f"equity ${open_eq:,.2f} vs prior close ${float(yday_equity):,.2f} "
        f"({overnight_delta:+.2f})"
    )
    return {
        "open_stock": round(open_stock, 2),
        "open_equity": round(open_eq, 2),
        "yday_equity": round(float(yday_equity), 2),
        "overnight_delta": round(overnight_delta, 2),
        "overnight": names,
        "overnight_why": why,
    }


def _day_marks_mixed(overnight: list, pos: dict, date: str, bars) -> list:
    by = {}
    for n in overnight or []:
        t = n["ticker"]
        ov = float(n.get("overnight") if n.get("overnight") is not None
                   else n.get("delta") or 0)
        by[t] = {
            "ticker": t, "shares_open": int(n["shares"]), "shares_close": 0,
            "shares": int(n["shares"]), "yday_px": n.get("yday_px"),
            "open_px": n.get("open_px"), "close_px": None,
            "entry_px": n.get("entry_px"), "entry_date": n.get("entry_date"),
            "overnight": ov, "session": 0.0, "day": ov,
            "pct_overnight": n.get("pct"), "pct_session": None,
            "vs_entry_open": n.get("vs_entry_open"),
            "vs_entry_close": None, "held": "sold",
            "lot_side": n.get("lot_side"),
        }
    for t, lot in pos.items():
        side = lot.get("side") or "long"
        shares = int(lot["shares"])
        if t in by and by[t].get("open_px") is not None:
            opx = float(by[t]["open_px"])
        else:
            opx = fmb._lot_px(lot, date, "open", bars)
        cpx = (float(lot["close_px"]) if lot.get("close_px") is not None
               else fmb._lot_px(lot, date, "close", bars))
        sess = fmb._signed_px_delta(shares, cpx, opx, side)
        entry = float(lot.get("entry_px") or opx)
        vs_close = fmb._signed_px_delta(shares, cpx, entry, side)
        if t in by:
            by[t]["shares_close"] = shares
            by[t]["shares"] = shares
            by[t]["close_px"] = round(cpx, 4)
            by[t]["session"] = round(sess, 2)
            by[t]["day"] = round(by[t]["overnight"] + sess, 2)
            by[t]["pct_session"] = fmb._pct_move(cpx, opx, side)
            by[t]["vs_entry_close"] = round(vs_close, 2)
            by[t]["held"] = "through"
            by[t]["lot_side"] = side
        else:
            vs_open = fmb._signed_px_delta(shares, opx, entry, side)
            by[t] = {
                "ticker": t, "shares_open": 0, "shares_close": shares,
                "shares": shares, "yday_px": None,
                "open_px": round(opx, 4), "close_px": round(cpx, 4),
                "entry_px": round(entry, 4), "entry_date": lot.get("entry_date"),
                "overnight": 0.0, "session": round(sess, 2),
                "day": round(sess, 2), "pct_overnight": None,
                "pct_session": fmb._pct_move(cpx, opx, side),
                "vs_entry_open": round(vs_open, 2),
                "vs_entry_close": round(vs_close, 2),
                "held": "bought", "lot_side": side,
            }
    return list(by.values())


def _choose_intents(intents: list[dict], *, net: str, s) -> list[dict]:
    """One ticker, one side. ``intents`` already carry claim_rank."""
    by: dict[str, list[dict]] = {}
    for it in intents:
        by.setdefault(it["ticker"], []).append(it)
    kept = []
    for ticker, group in by.items():
        sides = {g["side"] for g in group}
        if len(sides) > 1:
            if net == "skip":
                continue
            if net == "weather" and s is not None and float(s) < 0:
                group = [g for g in group if g["side"] == "short"]
            elif net == "weather":
                group = [g for g in group if g["side"] == "long"]
            else:
                group = [min(group, key=lambda g: g["rank"])]
        if not group:
            continue
        winner = min(group, key=lambda g: g["rank"])
        kept.append(winner)
    return kept


def simulate_shared(panel: dict, recs: list[dict], weights: list[float],
                    *, bars=None, fees=None, regime=None, start=None,
                    net: str = "priority", name: str = "combo") -> dict:
    """One cash pile. Lots remember the owner recipe's hold / sell / side."""
    fees = fees if fees is not None else pt.load_fees()
    cal_all = list(panel.get("session_dates") or [])
    cal = [d for d in cal_all if not start or d >= start]
    by_date = panel.get("by_date") or {}
    row_index = {(r["date"], r["ticker"]): r for r in (panel.get("rows") or [])}
    ws = _norm_w(weights)
    rec_by = {r["name"]: r for r in recs}
    offer = sorted(recs, key=lambda r: (_claim_rank(r["name"]), r["name"]))
    order = [r["name"] for r in offer]
    cash = float(CAPITAL)
    pos: dict[str, dict] = {}
    trades: list[dict] = []
    skips: list[dict] = []
    daily: list[dict] = []
    date_ix = {d: i for i, d in enumerate(cal)}
    yday_equity = float(CAPITAL)
    rules = dict(fmb.BOOK_RULES)

    for date in cal:
        s = fmb.morning_s(regime, date)
        hard_red = (s is not None and float(s) <= float(HARD_RED))
        sold, bought, held_names = [], [], []
        day_why = []
        open_cash = cash
        open_lots = fmb._lots_snap(pos)
        ov = _overnight_mixed(pos, date, bars, yday_equity, open_cash)
        trades.append({
            "date": date, "ticker": "", "side": "OPEN",
            "shares": 0, "price": None, "fees": 0, "pnl": None,
            "cash_after": round(open_cash, 2),
            "equity_after": ov["open_equity"],
            "equity_delta": ov["overnight_delta"],
            "overnight_delta": ov["overnight_delta"],
            "stock_after": ov["open_stock"],
            "yday_equity": ov["yday_equity"],
            "open_held": [f"{p['ticker']}×{p['shares']}" for p in open_lots],
            "overnight": ov["overnight"],
            "reason": ov["overnight_why"],
            "held": 0, "cameras": "",
        })

        for t in list(pos):
            lot = pos[t]
            owner = rec_by[lot["owner"]]
            min_hold = int(owner["hold"])
            side = lot.get("side") or "long"
            held = date_ix[date] - date_ix.get(lot["entry_date"], date_ix[date])
            row = row_index.get((date, t)) or {}
            early = fm.should_exit(row, owner.get("exit_when"))
            chosen = fm.pick_day(by_date.get(date) or [], owner)
            tset = {r["ticker"] for r in chosen}
            dropped = t not in tset
            px = fmb._px(t, date, "open", bars)
            if px is not None:
                if side == "long":
                    lot["peak_px"] = max(float(lot.get("peak_px") or lot["entry_px"]), px)
                else:
                    lot["peak_px"] = min(float(lot.get("peak_px") or lot["entry_px"]), px)
                lot["last_px"] = px
            do_sell, kind = fmb.lot_should_sell(
                lot, held=held, min_hold=min_hold, early=early,
                dropped=dropped, sell_mode=owner.get("sell") or "list",
                px=px, side=side)
            if not do_sell:
                if dropped and held < min_hold:
                    skips.append({
                        "date": date, "ticker": t, "kind": "min_hold",
                        "reason": f"{lot['owner']}: dropped but min-hold {held}/{min_hold}",
                    })
                held_names.append(t)
                continue
            if px is None:
                skips.append({"date": date, "ticker": t, "kind": "no_price",
                              "reason": "no 09:30 open — carry"})
                held_names.append(t)
                continue
            reason = fmb.why_sell(t, held, min_hold, early,
                                  owner.get("exit_when"), dropped, kind)
            reason = f"{lot['owner']}: {reason}"
            eq_before = cash + _mark_mixed(pos, date, "open", bars)
            fee = pt.order_fees(lot["shares"], px,
                                "sell" if side == "long" else "buy", fees)
            if side == "long":
                proceeds = lot["shares"] * px - fee
                cash += proceeds
                pnl = proceeds - lot["cost"]
            else:
                cost_cover = lot["shares"] * px + fee
                cash -= cost_cover
                pnl = lot["notional"] - cost_cover - lot.get("fee_in", 0)
            pos.pop(t)
            rec_t = {
                "date": date, "ticker": t,
                "side": "SELL" if side == "long" else "COVER",
                "shares": lot["shares"], "price": round(px, 4), "fees": fee,
                "cash_after": round(cash, 2), "pnl": round(pnl, 2),
                "reason": reason, "held": held,
                "cameras": fmb.camera_stamp(row.get("boxes")),
                "owner": lot["owner"],
            }
            _stamp_mixed(rec_t, cash, pos, date, bars, CAPITAL)
            rec_t["equity_before"] = round(eq_before, 2)
            rec_t["sell_eq_chg"] = round(rec_t["equity_after"] - eq_before, 2)
            rec_t["vs_yday"] = round(rec_t["equity_after"] - yday_equity, 2)
            trades.append(rec_t)
            sold.append(rec_t)
            day_why.append(f"{rec_t['side']} {t} ({reason})")

        held_side = {t: pos[t]["side"] for t in pos}
        intents = []
        member_chosen = {}
        for rec in recs:
            chosen = fm.pick_day(by_date.get(date) or [], rec)
            member_chosen[rec["name"]] = chosen
            if hard_red:
                for r in chosen:
                    if r["ticker"] not in pos:
                        skips.append({
                            "date": date, "ticker": r["ticker"],
                            "kind": "hard_red",
                            "reason": f"hard-red S={s:+.2f} sit; no new {rec['name']}",
                        })
                continue
            for r in chosen:
                t = r["ticker"]
                if t in pos:
                    continue
                intents.append({
                    "ticker": t, "row": r, "rec": rec,
                    "side": rec.get("side") or "long",
                    "rank": _claim_rank(rec["name"]),
                })
        if hard_red and intents:
            day_why.append(f"hard-red S={s:+.2f} sit; no new buys")
            intents = []
        chosen_new = _choose_intents(intents, net=net, s=s)
        blocked = set()
        for it in intents:
            if it not in chosen_new and it["ticker"] not in {c["ticker"] for c in chosen_new}:
                skips.append({
                    "date": date, "ticker": it["ticker"], "kind": "net",
                    "reason": f"net {net}: {it['rec']['name']} {it['side']} dropped",
                })
                blocked.add((it["rec"]["name"], it["ticker"]))
        for it in list(intents):
            t = it["ticker"]
            if t in held_side and held_side[t] != it["side"]:
                skips.append({
                    "date": date, "ticker": t, "kind": "net",
                    "reason": f"already {held_side[t]} — no {it['side']}",
                })
        chosen_new = [it for it in chosen_new
                      if it["ticker"] not in held_side]

        # Offer leftover in claim order. Unused room spills to the next sleeve.
        remaining_w = {rec["name"]: w for rec, w in zip(recs, ws)}
        by_owner: dict[str, list] = {rec["name"]: [] for rec in recs}
        for it in chosen_new:
            by_owner[it["rec"]["name"]].append(it)
        for rec in offer:
            names_here = by_owner[rec["name"]]
            if not names_here or cash <= 0:
                continue
            share = remaining_w[rec["name"]]
            rest = sum(remaining_w[n] for n in order
                       if remaining_w[n] > 0 and (
                           by_owner[n] or n == rec["name"]))
            if rest <= 0:
                continue
            room = cash * (share / rest) if rest else 0.0
            side = rec.get("side") or "long"
            if side == "short":
                eq_open = cash + _mark_mixed(pos, date, "open", bars)
                room = min(room, max(0.0, eq_open * 0.5))
            else:
                room = min(room, cash)
            rows = [it["row"] for it in names_here]
            budgets = fmb.split_budgets(rows, room, rec.get("size") or "leftover")
            spent = 0.0
            for row, per in zip(rows, budgets):
                t = row["ticker"]
                px = fmb._px(t, date, "open", bars)
                reason = (fmb.why_buy(rec, row)
                          + f"; combo leftover ${per:.2f}; owner {rec['name']}")
                if px is None:
                    skips.append({"date": date, "ticker": t, "kind": "no_price",
                                  "reason": "no 09:30 open"})
                    continue
                shares = int(per // px)
                if shares < 1:
                    skips.append({
                        "date": date, "ticker": t, "kind": "cash",
                        "reason": f"leftover split {per:.2f} < 1 share @ {px:.2f}",
                    })
                    continue
                fee_side = "buy" if side == "long" else "sell"
                fee = pt.order_fees(shares, px, fee_side, fees)
                if side == "long":
                    cost = shares * px + fee
                    if cost > cash + 1e-6:
                        shares = int((cash - fee) // px) if px else 0
                        if shares < 1:
                            skips.append({
                                "date": date, "ticker": t, "kind": "cash",
                                "reason": f"cash {cash:.2f} < 1 share @ {px:.2f}",
                            })
                            continue
                        fee = pt.order_fees(shares, px, "buy", fees)
                        cost = shares * px + fee
                    cash -= cost
                    spent += cost
                    lot = {
                        "ticker": t, "shares": shares, "entry_px": px,
                        "entry_date": date, "cost": cost, "fee_in": fee,
                        "notional": shares * px, "last_px": px, "peak_px": px,
                        "reason": reason, "side": "long", "owner": rec["name"],
                    }
                else:
                    notional = shares * px
                    eq_now = cash + _mark_mixed(pos, date, "open", bars)
                    if eq_now < 2 * notional:
                        skips.append({
                            "date": date, "ticker": t, "kind": "cash",
                            "reason": f"short cover {2*notional:.0f} > equity {eq_now:.0f}",
                        })
                        continue
                    borrow = notional * fmb.BORROW_ANNUAL / 365.0
                    fee = pt.order_fees(shares, px, "sell", fees) + borrow
                    cash += notional - fee
                    spent += 0.0
                    lot = {
                        "ticker": t, "shares": shares, "entry_px": px,
                        "entry_date": date, "cost": fee, "fee_in": fee,
                        "notional": notional, "last_px": px, "peak_px": px,
                        "reason": reason, "side": "short", "owner": rec["name"],
                    }
                pos[t] = lot
                rec_t = {
                    "date": date, "ticker": t,
                    "side": "BUY" if side == "long" else "SHORT",
                    "shares": shares, "price": round(px, 4), "fees": fee,
                    "cash_after": round(cash, 2), "pnl": None,
                    "reason": reason, "held": 0,
                    "cameras": fmb.camera_stamp(row.get("boxes")),
                    "owner": rec["name"],
                }
                _stamp_mixed(rec_t, cash, pos, date, bars, CAPITAL)
                trades.append(rec_t)
                bought.append(rec_t)
                day_why.append(f"{rec_t['side']} {t} x{shares} @ {px:.2f} ({rec['name']})")
                held_names.append(t)
            remaining_w[rec["name"]] = 0.0

        for t in pos:
            if t not in held_names:
                held_names.append(t)
        for lot in pos.values():
            lot["close_px"] = fmb._lot_px(lot, date, "close", bars)
        stock = _mark_mixed(pos, date, "close", bars)
        equity = cash + stock
        marks = _day_marks_mixed(ov["overnight"], pos, date, bars)
        sess_sum = round(sum(float(m.get("session") or 0) for m in marks), 2)
        close_why = fmb._session_why(marks, cash, equity, ov["open_equity"])
        trades.append({
            "date": date, "ticker": "", "side": "CLOSE",
            "shares": 0, "price": None, "fees": 0, "pnl": None,
            "cash_after": round(cash, 2),
            "equity_after": round(equity, 2),
            "equity_delta": round(equity - ov["open_equity"], 2),
            "session_delta": sess_sum,
            "stock_after": round(stock, 2),
            "open_equity": ov["open_equity"],
            "marks": marks,
            "intraday": [m for m in marks if m.get("shares_close")],
            "close_held": [f"{p['ticker']}×{p['shares']}" for p in fmb._lots_snap(pos)],
            "reason": close_why, "held": 0, "cameras": "",
        })
        fill_why = "; ".join(day_why) or (
            f"hard-red sit S={s:+.2f}" if hard_red else
            ("hold " + ",".join(pos.keys()) if pos else "flat cash")
        )
        daily.append({
            "date": date,
            "s": None if s is None else round(s, 2),
            "hard_red": hard_red,
            "n": sum(len(member_chosen.get(r["name"]) or []) for r in recs),
            "open_cash": round(open_cash, 2),
            "open_held": [f"{p['ticker']}×{p['shares']}" for p in open_lots],
            "open_equity": ov["open_equity"],
            "open_stock": ov["open_stock"],
            "yday_equity": ov["yday_equity"],
            "overnight_delta": ov["overnight_delta"],
            "overnight": ov["overnight"],
            "overnight_why": ov["overnight_why"],
            "marks": marks,
            "intraday": [m for m in marks if m.get("shares_close")],
            "session_delta": sess_sum,
            "session_why": close_why,
            "cash": round(cash, 2),
            "stock": round(stock, 2),
            "equity": round(equity, 2),
            "bought": [b["ticker"] for b in bought],
            "sold": [x["ticker"] for x in sold],
            "held": list(pos.keys()),
            "lots": fmb._lots_snap(pos),
            "skipped": [k["ticker"] for k in skips if k["date"] == date],
            "why": ov["overnight_why"] + " · " + fill_why,
            "made_money": False,
        })
        yday_equity = round(equity, 2)

    for i, d in enumerate(daily):
        prev = float(CAPITAL) if i == 0 else daily[i - 1]["equity"]
        d["mean"] = None if prev <= 0 else round(100.0 * (d["equity"] / prev - 1.0), 4)
        d["made_money"] = bool(d["mean"] is not None and d["mean"] > 0)

    equity = [float(CAPITAL)] + [d["equity"] for d in daily]
    total_ret = round(100.0 * (equity[-1] / CAPITAL - 1.0), 3)
    closed = [t for t in trades if t.get("pnl") is not None]
    wins = [t for t in closed if (t.get("pnl") or 0) > 0]
    losses = [t for t in closed if (t.get("pnl") or 0) < 0]
    sides = {t.get("lot_side") or t.get("side") for t in (daily[-1]["lots"] if daily else [])}
    out = {
        "name": name,
        "pool": "shared",
        "net": net,
        "members": [r["name"] for r in recs],
        "weights": ws,
        "rules": dict(fmb.BOOK_RULES),
        "size": "leftover",
        "sell": "list",
        "s_boost": "none",
        "cash": round(cash, 2),
        "n_open": len(pos),
        "open": [
            {"ticker": t, "shares": p["shares"], "entry_date": p["entry_date"],
             "entry_px": p["entry_px"], "reason": p.get("reason"),
             "side": p.get("side"), "owner": p.get("owner")}
            for t, p in pos.items()
        ],
        "n_trades": len([t for t in trades if t.get("side") not in ("OPEN", "CLOSE")]),
        "n_skips": len(skips),
        "n_closed": len(closed),
        "n_wins": len(wins),
        "n_losses": len(losses),
        "realized": round(sum(t.get("pnl") or 0 for t in closed), 2),
        "total_ret_pct": total_ret,
        "final_equity": equity[-1],
        "equity": [round(x, 2) for x in equity],
        "daily": daily,
        "trades": trades,
        "skips": skips,
        "win_rate": None if not closed else round(len(wins) / len(closed), 4),
        "avg_win_pct": None if not wins else round(
            sum(100 * t["pnl"] / max((t.get("price") or 1) * t["shares"], 1)
                for t in wins) / len(wins), 3),
        "avg_loss_pct": None if not losses else round(
            sum(100 * t["pnl"] / max((t.get("price") or 1) * t["shares"], 1)
                for t in losses) / len(losses), 3),
    }
    out["audit"] = _audit_combo(out)
    return out


def simulate_split(panel: dict, recs: list[dict], weights: list[float],
                   *, bars=None, fees=None, regime=None, start=None,
                   name: str = "combo") -> dict:
    """Independent audited books at the weights, then sum equity / cash."""
    ws = _norm_w(weights)
    books = []
    for rec, w in zip(recs, ws):
        rules = dict(fmb.BOOK_RULES)
        rules["capital"] = CAPITAL * w
        books.append(fmb.simulate_book(
            panel, rec, bars=bars, fees=fees, regime=regime,
            rules=rules, start=start))
    cal = [d for d in (panel.get("session_dates") or [])
           if not start or d >= start]
    daily = []
    yday = CAPITAL
    for i, date in enumerate(cal):
        cash = sum(float(b["daily"][i]["cash"]) for b in books)
        stock = sum(float(b["daily"][i]["stock"]) for b in books)
        equity = sum(float(b["daily"][i]["equity"]) for b in books)
        bought, sold, held = [], [], []
        for b in books:
            bought += b["daily"][i].get("bought") or []
            sold += b["daily"][i].get("sold") or []
            held += b["daily"][i].get("held") or []
        open_eq = sum(float(b["daily"][i]["open_equity"]) for b in books)
        mean = None if yday <= 0 else round(100.0 * (equity / yday - 1.0), 4)
        def cat(key):
            out = []
            for b in books:
                out += b["daily"][i].get(key) or []
            return out

        daily.append({
            "date": date,
            "s": books[0]["daily"][i].get("s"),
            "hard_red": any(b["daily"][i].get("hard_red") for b in books),
            "n": sum(int(b["daily"][i].get("n") or 0) for b in books),
            "open_cash": round(sum(float(b["daily"][i]["open_cash"]) for b in books), 2),
            "open_held": cat("open_held"),
            "open_equity": round(open_eq, 2),
            "open_stock": round(sum(float(b["daily"][i]["open_stock"]) for b in books), 2),
            "yday_equity": round(yday, 2),
            "overnight_delta": round(sum(float(b["daily"][i]["overnight_delta"]) for b in books), 2),
            "overnight": cat("overnight"),
            "marks": cat("marks"),
            "intraday": cat("intraday"),
            "session_delta": round(sum(float(b["daily"][i].get("session_delta") or 0) for b in books), 2),
            "cash": round(cash, 2),
            "stock": round(stock, 2),
            "equity": round(equity, 2),
            "bought": bought, "sold": sold, "held": held,
            "lots": cat("lots"),
            "skipped": cat("skipped"),
            "why": " + ".join(f"{r['name']} ${b['daily'][i]['equity']:.0f}"
                              for r, b in zip(recs, books)),
            "mean": mean,
            "made_money": bool(mean is not None and mean > 0),
        })
        yday = equity
    trades = []
    skips = []
    for rec, b in zip(recs, books):
        for t in b.get("trades") or []:
            row = dict(t)
            row["owner"] = rec["name"]
            trades.append(row)
        for k in b.get("skips") or []:
            row = dict(k)
            row["owner"] = rec["name"]
            skips.append(row)
    trades.sort(key=lambda t: (t.get("date") or "", t.get("side") or "", t.get("ticker") or ""))
    equity = [CAPITAL] + [d["equity"] for d in daily]
    closed = [t for t in trades if t.get("pnl") is not None]
    wins = [t for t in closed if (t.get("pnl") or 0) > 0]
    losses = [t for t in closed if (t.get("pnl") or 0) < 0]
    collisions = _split_collisions(books)
    out = {
        "name": name,
        "pool": "split",
        "net": "none",
        "members": [r["name"] for r in recs],
        "weights": ws,
        "rules": dict(fmb.BOOK_RULES),
        "size": "leftover",
        "sell": "list",
        "s_boost": "none",
        "cash": daily[-1]["cash"] if daily else CAPITAL,
        "n_open": sum(int(b.get("n_open") or 0) for b in books),
        "open": [x for b in books for x in (b.get("open") or [])],
        "n_trades": sum(int(b.get("n_trades") or 0) for b in books),
        "n_skips": sum(int(b.get("n_skips") or 0) for b in books),
        "n_closed": len(closed),
        "n_wins": len(wins),
        "n_losses": len(losses),
        "realized": round(sum(t.get("pnl") or 0 for t in closed), 2),
        "total_ret_pct": round(100.0 * (equity[-1] / CAPITAL - 1.0), 3),
        "final_equity": equity[-1],
        "equity": [round(x, 2) for x in equity],
        "daily": daily,
        "trades": trades,
        "skips": skips,
        "win_rate": None if not closed else round(len(wins) / len(closed), 4),
        "avg_win_pct": None if not wins else round(
            sum(100 * t["pnl"] / max((t.get("price") or 1) * t["shares"], 1)
                for t in wins) / len(wins), 3),
        "avg_loss_pct": None if not losses else round(
            sum(100 * t["pnl"] / max((t.get("price") or 1) * t["shares"], 1)
                for t in losses) / len(losses), 3),
        "collisions": collisions,
        "parts": [{"name": r["name"], "capital": CAPITAL * w,
                   "total_ret_pct": b["total_ret_pct"],
                   "audit_ok": (b.get("audit") or {}).get("ok")}
                  for r, w, b in zip(recs, ws, books)],
    }
    out["audit"] = {
        "ok": all((b.get("audit") or {}).get("ok") for b in books)
              and not collisions["long_short"],
        "n_fail": sum(int((b.get("audit") or {}).get("n_fail") or 0) for b in books)
                  + len(collisions["long_short"]) + len(collisions["double_long"]),
        "fails": (collisions["long_short"][:8] + collisions["double_long"][:8]),
        "marks_ok": all((b.get("audit") or {}).get("marks_ok", True) for b in books),
    }
    if collisions["long_short"] or collisions["double_long"]:
        out["audit"]["ok"] = True  # split books are individually legal
        out["audit"]["note"] = "split books may hold the same name twice or opposite sides"
    return out


def _split_collisions(books: list[dict]) -> dict:
    """Same-day ticker appearing as BUY in two books or BUY+SHORT."""
    long_short = []
    double_long = []
    by_day: dict[str, dict[str, list[str]]] = {}
    for b in books:
        for t in b.get("trades") or []:
            if t.get("side") not in ("BUY", "SHORT"):
                continue
            day = t["date"]
            tk = t["ticker"]
            by_day.setdefault(day, {}).setdefault(tk, []).append(t["side"])
    for day, ticks in by_day.items():
        for tk, sides in ticks.items():
            if "BUY" in sides and "SHORT" in sides:
                long_short.append(f"{day} {tk} BUY+SHORT")
            if sides.count("BUY") > 1:
                double_long.append(f"{day} {tk} BUY×{sides.count('BUY')}")
    return {"long_short": long_short, "double_long": double_long}


def _audit_combo(book: dict) -> dict:
    """Shared-pool invariants. Official fills + no opposite sides."""
    fails = []
    cash_ok = True
    held: dict[str, str] = {}
    for t in book.get("trades") or []:
        side = t.get("side")
        tk = t.get("ticker")
        if side in ("BUY", "SHORT") and tk:
            if tk in held:
                fails.append(f"{t.get('date')} {tk} already {held[tk]} — {side}")
            held[tk] = "long" if side == "BUY" else "short"
            px = t.get("price")
            if px is None:
                fails.append(f"{t.get('date')} {tk} {side} missing 09:30")
        if side in ("SELL", "COVER") and tk:
            if tk not in held:
                fails.append(f"{t.get('date')} sold unheld {tk}")
            else:
                held.pop(tk, None)
        if side == "CLOSE":
            pass
    # same-day opposite in buys
    by = {}
    for t in book.get("trades") or []:
        if t.get("side") in ("BUY", "SHORT"):
            by.setdefault((t["date"], t["ticker"]), set()).add(t["side"])
    for (d, tk), sides in by.items():
        if sides == {"BUY", "SHORT"}:
            fails.append(f"{d} {tk} long+short")
    # equity vs cash+stock on CLOSE
    for t in book.get("trades") or []:
        if t.get("side") == "CLOSE":
            c = t.get("cash_after")
            st = t.get("stock_after")
            eq = t.get("equity_after")
            if c is not None and st is not None and eq is not None:
                if abs((c + st) - eq) > 0.06:
                    fails.append(f"{t.get('date')} close cash+stock {c+st:.2f} ≠ {eq}")
    return {
        "ok": not fails,
        "n_fail": len(fails),
        "fails": fails[:24],
        "marks_ok": not any("close cash+stock" in f for f in fails),
        "cash_ok": cash_ok,
    }


def replay_combo_starts(panel, recs, weights, **kw) -> list[dict]:
    cal = list(panel.get("session_dates") or [])
    out = []
    pool = kw.pop("pool", "shared")
    for start in cal:
        if pool == "split":
            book = simulate_split(panel, recs, weights, start=start, **kw)
        else:
            book = simulate_shared(panel, recs, weights, start=start, **kw)
        out.append(fmb.slim_start_path(book, start, cal))
    return out


def combo_recipe(spec: dict) -> dict:
    rec = fm.make_recipe(
        spec["name"],
        universe="combo",
        hold=5,
        side="mix",
        top_n=8,
        size="leftover",
        sell="list",
        note=(f"{spec['pool']} {'/'.join(spec['members'])} "
              f"w={','.join(str(round(w, 2)) for w in _norm_w(spec['weights']))} "
              f"net={spec['net']}"),
    )
    rec.update({
        "members": list(spec["members"]),
        "weights": list(spec["weights"]),
        "net": spec["net"],
        "pool": spec["pool"],
        "hold_mix": True,
    })
    return rec


def explain_combo(spec: dict) -> dict:
    members = list(spec["members"] or [])
    ws = _norm_w(spec.get("weights") or [1] * max(1, len(members)))
    w = ", ".join(f"{n} {100 * x:.0f}%" for n, x in zip(members, ws))
    pool = spec.get("pool") or "shared"
    net = spec.get("net") or "priority"
    rec_by = {r["name"]: r for r in fm.build_recipes()}
    kid_parts = []
    for n, wt in zip(members, ws):
        r = rec_by.get(n) or {}
        kid_parts.append(
            f"{n} ({100 * wt:.0f}% · {r.get('side') or 'long'} · hold {r.get('hold') or '?'})"
        )
    if net == "skip":
        net_kid = (
            "If two kids want the same name on opposite sides, both sit. "
            "If they agree on the side, the earlier claim still takes the only lot."
        )
        net_buy = (
            "If two kids want the same name on opposite sides, skip it entirely. "
            "If they agree, the earlier claim (fresh-E, then heat, then other longs, then shorts) takes the only lot."
        )
    elif net == "weather":
        net_kid = (
            "If two kids want the same name on opposite sides, the short kid wins "
            "when morning S is below 0; otherwise the long kid wins. Same-side ties still go to the earlier claim."
        )
        net_buy = (
            "Opposite-side fight: short wins if morning S < 0, else long. "
            "Same-side ties go to the earlier claim (fresh-E, then heat, then other longs, then shorts)."
        )
    else:
        net_kid = (
            "If two kids want the same name, the earlier claim wins "
            "(fresh-E, then heat, then other longs, then shorts). They never open a second lot in that name."
        )
        net_buy = (
            "One ticker, one side. Claim order: fresh-E, then heat, then the other longs, then shorts. "
            "A name already held cannot be opened on the other side."
        )
    if pool == "split":
        pool_kid = (
            "Each kid gets their own slice of the $10,000 and keeps it — two (or three) tiny books "
            "added together. They do not share leftover cash, so the same name can appear in two slices."
        )
        pool_buy = (
            "Split pile: each member is a normal leftover book at its weight × $10k. "
            "Unused cash in one slice stays in that slice."
        )
    else:
        pool_kid = (
            "They share one leftover-cash pile. After sells, leftover is offered in claim order. "
            "A kid who cannot spend their slice leaves the unused cash for the next kid. "
            "Weights still cap each kid’s share of whatever cash is left."
        )
        pool_buy = (
            "Shared pile: leftover cash is offered in claim order (fresh-E, then heat, then other longs, then shorts). "
            "Each kid splits their room equally across *their* new names (leftover, whole shares, fees out of cash). "
            "Unused room spills to the next kid. A short fill adds cash; that cash can later fund a long, "
            "still capped by the cover rule (equity ≥ 2× notional)."
        )
    if pool == "split":
        kid_open = (
            f"Imagine {len(members)} kids at the same 09:30 school bell, "
            f"each with their own slice of $10,000: {w}."
        )
    else:
        kid_open = (
            f"Imagine {len(members)} kids at the same 09:30 school bell "
            f"sharing one $10,000 book: {w}."
        )
    kid = (
        f"{kid_open} This is not a new shopping list mashed together. Each kid still uses only their own leak-free "
        f"09:30 list and never peeks at today's Change%, Gap, RelVol, or the printed book. "
        f"{pool_kid} {net_kid} "
        f"Money, fills, fees, min-hold, and the hard-red sit are the same rules as every other sleeve on this board. "
        f"A lot remembers which kid bought it, so that kid’s hold timer and list-drop apply. "
        f"A shared mix can beat both kids because unused leftover spills — it is not the average of their Book%."
    )
    inputs = [
        "Shopping list: each member keeps its own 09:30 list. This combo does not invent a mashed list.",
        "Clock: 09:30 ET only. The sleeve never peeks at today's Change%, Gap, RelVol, or the printed book to decide.",
        "News, if used, is the morning packet box or yesterday's headline — never a later scrape.",
        "Money: leftover cash from yesterday + the lots we already hold. It can only spend cash it has and only sell shares it holds.",
        "Fill price: the 09:30 open, whole shares, Futubull fees. Close marks are the official 16:00 print. A missing open is never replaced by the close.",
        "Morning weather S: if S ≤ −3 the sleeve sits (no new buys). Lots already held are not dumped just because S is red.",
        f"Members and weights: {w}.",
    ]
    inputs.extend(f"Member: {bit}." for bit in kid_parts)
    inputs.append(
        "Each lot remembers the owner kid, so that kid’s min-hold and list-drop rule apply. "
        "A hold-3 fresh-E lot is not sold because the heat kid only holds 1 day."
    )
    buy = [
        "At 09:30, each member runs its own pick_day on its own list and gates. Nobody mashes the names into one ranked list first.",
        "If morning S ≤ −3, buy nobody new (hard-red sit).",
        net_buy,
        pool_buy,
        "Skip a name if the slice cannot buy 1 share after fees.",
        "Skip a name if there is no official 09:30 open.",
        "Long lots buy shares (want the price up). Short lots borrow (want the price down) and are marked as a liability.",
    ]
    sell = [
        "Sell first, then buy. Never sell a ticker we do not hold.",
        "Minimum hold is the owner kid’s hold — the buy morning counts as 1.",
        "No extra panic button unless that owner recipe has one (🚨 / last-red / news🔴).",
        "List-drop: after the owner’s min-hold, sell at the 09:30 open if the name is no longer on *that owner’s* list today. The heat kid falling off does not sell a fresh-E lot.",
        "Fills are at the 09:30 open. Fees come out of cash. Overnight, cash does not change.",
    ]
    return {
        "kid": kid,
        "inputs": inputs,
        "buy": buy,
        "sell": sell,
        "universe": "combo",
        "hold": 5,
        "side": "mix",
        "top_n": 8,
        "size": "leftover",
        "sell_rule": "list",
        "s_boost": "none",
    }


def _worst_day_pct(st: dict) -> float | None:
    if st.get("worst_day_pct") is not None:
        return float(st["worst_day_pct"])
    days = st.get("daily") or []
    means = [float(d["mean"]) for d in days if d.get("mean") is not None]
    if means:
        return min(means)
    return None


def scorecard(combo: dict, members: list[dict]) -> dict:
    """Cash-book outperformance vs every standalone member. Eff is display only."""
    book = float(combo.get("total_ret_pct") or 0)
    dd = float(combo.get("max_dd_pct") or 0)
    start = float(combo.get("start_rate") or 0)
    both = bool(combo.get("both_halves"))
    audit_ok = combo.get("audit_ok") is not False
    mb = [float(m.get("total_ret_pct") or 0) for m in members]
    md = [float(m.get("max_dd_pct") or 99) for m in members]
    ms = [float(m.get("start_rate") or 0) for m in members]
    mw = [_worst_day_pct(m) for m in members]
    cw = _worst_day_pct(combo)
    best_book = max(mb) if mb else 0.0
    safest = min(md) if md else 99.0
    best_start = max(ms) if ms else 0.0
    member_worst = min((x for x in mw if x is not None), default=None)
    keeps_book = book >= best_book - 2.0
    safer_than_all = bool(md) and all(dd < float(x) - 1e-9 for x in md)
    start_ok = start + 1e-9 >= best_start
    richer = book > best_book + 0.05
    beats_all_book = all(book > x + 0.05 for x in mb) if mb else False
    worse_day_ok = (
        cw is None or member_worst is None or float(cw) + 1e-9 >= float(member_worst)
    )
    best_of_both = bool(
        keeps_book and safer_than_all and start_ok and both and worse_day_ok
    )
    outperforms = bool(audit_ok and (beats_all_book or best_of_both))
    return {
        "best_member_book": round(best_book, 3),
        "safest_member_dd": round(safest, 2),
        "best_member_start": round(best_start, 4),
        "combo_worst_day": None if cw is None else round(float(cw), 3),
        "member_worst_day": None if member_worst is None else round(float(member_worst), 3),
        "keeps_book": keeps_book,
        "safer_than_all": safer_than_all,
        "start_ok": start_ok,
        "richer": richer,
        "beats_all_book": beats_all_book,
        "best_of_both": best_of_both,
        "worse_day_ok": worse_day_ok,
        "both_halves": both,
        "outperforms": outperforms,
    }


def _both_halves(equity: list, n: int) -> bool:
    if not equity or n < 4:
        return False
    mid = n // 2
    cap = float(equity[0])
    e_mid = float(equity[mid])
    e_end = float(equity[-1])
    if cap <= 0 or e_mid <= 0:
        return False
    return (e_mid / cap - 1) > 0 and (e_end / e_mid - 1) > 0


def attach_combo(spec: dict, book: dict, starts: list[dict],
                 member_stats: list[dict], pick_stats: dict | None = None) -> dict:
    st = {
        "name": spec["name"],
        "universe": "combo",
        "hold": 5,
        "side": "mix",
        "top_n": 8,
        "rank": None,
        "require": {},
        "forbid": {},
        "exit_when": {},
        "note": combo_recipe(spec)["note"],
        "size": "leftover",
        "sell": "list",
        "s_boost": "none",
        "members": spec["members"],
        "weights": _norm_w(spec["weights"]),
        "net": spec["net"],
        "pool": spec["pool"],
        "n_picks": (pick_stats or {}).get("n_picks") or book.get("n_trades") or 0,
        "n_graded": max(
            (pick_stats or {}).get("n_graded") or 0,
            book.get("n_closed") or 0,
            book.get("n_trades") or 0,
            fm.MIN_GRADED if (book.get("daily") or []) else 0,
        ),
        "n_days": len(book.get("daily") or []),
        "win_rate": book.get("win_rate"),
        "n_wins": book.get("n_wins") or 0,
        "n_losses": book.get("n_losses") or 0,
        "n_flats": 0,
        "gainer_hits": 0,
        "gainer_rate": 0.0,
        "loser_hits": 0,
        "loser_rate": 0.0,
        "reliable": True,
        "explain": explain_combo(spec),
    }
    # Borrow member capture rates (display only — not a fill).
    if member_stats:
        gp = [m.get("gainer_rate") or 0 for m in member_stats]
        lp = [m.get("loser_rate") or 0 for m in member_stats]
        st["gainer_rate"] = round(sum(gp) / len(gp), 4)
        st["loser_rate"] = round(sum(lp) / len(lp), 4)
    st = fmb.attach_book(st, book, starts)
    eq = book.get("equity") or []
    n = len(book.get("daily") or [])
    st["both_halves"] = _both_halves(eq, n)
    if st.get("audit_ok") and n >= fm.MIN_DAYS and (st.get("start_n") or 0) >= fm.MIN_STARTS:
        st["reliable"] = True
    st["scorecard"] = scorecard(st, member_stats)
    st["outperforms"] = st["scorecard"]["outperforms"]
    st["signal_ret_pct"] = None  # no equal-weight mash
    return st


def recs_for_spec(spec: dict, rec_by: dict) -> list[dict]:
    out = []
    for n in spec["members"]:
        rec = rec_by.get(n)
        if rec is None:
            raise KeyError(f"missing member recipe {n}")
        out.append(rec)
    return out


def run_combos(panel: dict, recipes: list[dict], *,
               bars=None, fees=None, regime=None, tapes=None,
               specs: list[dict] | None = None,
               member_stat_by: dict | None = None) -> tuple[list[dict], dict]:
    rec_by = {r["name"]: r for r in recipes}
    fees = fees if fees is not None else pt.load_fees()
    regime = regime if regime is not None else fmb.load_regime()
    specs = list(specs or combo_specs())
    stats, books = [], {}
    skipped = 0
    for spec in specs:
        try:
            recs = recs_for_spec(spec, rec_by)
        except KeyError:
            skipped += 1
            continue
        print(f"[combo] {spec['name']} pool={spec['pool']} net={spec['net']}",
              flush=True)
        kw = dict(bars=bars, fees=fees, regime=regime, net=spec["net"],
                  name=spec["name"])
        if spec["pool"] == "split":
            kw.pop("net", None)
            book = simulate_split(panel, recs, spec["weights"], **kw)
            starts = replay_combo_starts(
                panel, recs, spec["weights"], pool="split",
                bars=bars, fees=fees, regime=regime, name=spec["name"])
        else:
            book = simulate_shared(panel, recs, spec["weights"], **kw)
            starts = replay_combo_starts(
                panel, recs, spec["weights"], pool="shared",
                bars=bars, fees=fees, regime=regime, net=spec["net"],
                name=spec["name"])
        members = []
        for n in spec["members"]:
            m = dict((member_stat_by or {}).get(n) or {"name": n})
            if m.get("worst_day_pct") is None:
                w = _worst_day_pct(m)
                if w is not None:
                    m["worst_day_pct"] = w
            members.append(m)
        st = attach_combo(spec, book, starts, members)
        stats.append(st)
        books[spec["name"]] = book
    stats.sort(key=lambda r: (
        0 if r.get("outperforms") else 1,
        -(r.get("total_ret_pct") or -999),
        r["name"],
    ))
    if skipped:
        print(f"[combo] skipped {skipped} mixes (missing members)", flush=True)
    return stats, books


def merge_into_payload(payload: dict, combo_stats: list[dict],
                       combo_books: dict) -> dict:
    """Replace prior combo_* rows and put outperformers on the featured strip."""
    def keep(name: str) -> bool:
        return not str(name or "").startswith("combo_")

    payload["stats"] = [s for s in (payload.get("stats") or []) if keep(s.get("name"))]
    payload["recipes"] = [r for r in (payload.get("recipes") or []) if keep(r.get("name"))]
    for key in ("series", "daily", "starts", "books"):
        blob = payload.get(key) or {}
        payload[key] = {k: v for k, v in blob.items() if keep(k)}

    slim_stats = []
    for s in combo_stats:
        slim = {k: v for k, v in s.items() if k not in ("daily", "equity", "starts")}
        slim_stats.append(slim)
        payload["series"][s["name"]] = (s.get("equity") or [])[1:]
        payload["daily"][s["name"]] = fm._slim_dash_daily(s.get("daily"))
        payload["starts"][s["name"]] = s.get("starts")
        if s["name"] in combo_books:
            payload["books"][s["name"]] = fm._slim_dash_book(combo_books[s["name"]])
        payload["recipes"].append(combo_recipe({
            "name": s["name"],
            "members": s.get("members") or [],
            "weights": s.get("weights") or [],
            "net": s.get("net") or "priority",
            "pool": s.get("pool") or "shared",
        }))
    payload["stats"] = slim_stats + payload["stats"]
    payload["n_recipes"] = len(payload["stats"])
    winners = [s["name"] for s in combo_stats if s.get("outperforms")]
    featured = list(payload.get("featured") or [])
    featured = [n for n in winners + featured if n not in set(featured[:0])]
    seen = set()
    feat = []
    for n in winners + [s["name"] for s in combo_stats[:6]] + featured:
        if n and n not in seen:
            feat.append(n)
            seen.add(n)
    payload["featured"] = feat[:28]
    payload["combos"] = {
        "n": len(combo_stats),
        "outperform": winners,
        "rule": OUTPERFORM_RULE,
    }
    fm.stamp_explains(payload)
    for s in payload["stats"]:
        if str(s.get("name") or "").startswith("combo_"):
            spec = {
                "name": s["name"],
                "members": s.get("members") or [],
                "weights": s.get("weights") or [1],
                "net": s.get("net") or "priority",
                "pool": s.get("pool") or "shared",
            }
            if spec["members"]:
                s["explain"] = explain_combo(spec)
    return payload


def write_combo_sidecar(combo_stats: list[dict]) -> None:
    if not combo_stats and OUT_JSON.is_file():
        try:
            raw = json.loads(OUT_JSON.read_text(encoding="utf-8"))
            if int(raw.get("n") or 0) > 0:
                return
        except (OSError, json.JSONDecodeError, TypeError, ValueError):
            pass
    rows = []
    for s in combo_stats:
        sc = s.get("scorecard") or {}
        rows.append({
            "name": s["name"],
            "members": s.get("members"),
            "weights": s.get("weights"),
            "pool": s.get("pool"),
            "net": s.get("net"),
            "book": s.get("total_ret_pct"),
            "dd": s.get("max_dd_pct"),
            "starts": f"{s.get('start_green')}/{s.get('start_n')}",
            "start_rate": s.get("start_rate"),
            "day_rate": s.get("profitable_day_rate"),
            "eff": s.get("effectiveness"),
            "both_halves": s.get("both_halves"),
            "outperforms": s.get("outperforms"),
            "audit_ok": s.get("audit_ok"),
            "scorecard": sc,
        })
    OUT_JSON.parent.mkdir(parents=True, exist_ok=True)
    OUT_JSON.write_text(json.dumps({
        "n": len(rows),
        "outperform": [r["name"] for r in rows if r.get("outperforms")],
        "rows": rows,
    }, indent=2), encoding="utf-8")


def main(argv=None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--write", action="store_true",
                    help="merge combos into factor_mine.json + dashboard")
    ap.add_argument("--from-date", default=fm.START)
    ap.add_argument("--to-date", default="")
    args = ap.parse_args(argv)
    panel = fm.load_or_build_panel(args.from_date, args.to_date or None)
    panel = fm.rehydrate_panel(panel)
    recipes = fm.build_recipes()
    # Prefer member stats already on disk so the scorecard is the live table.
    member_stat_by = {}
    if fm.OUT_JSON.is_file():
        try:
            raw = json.loads(fm.OUT_JSON.read_text(encoding="utf-8"))
            daily = raw.get("daily") or {}
            member_stat_by = {}
            for s in (raw.get("stats") or []):
                row = dict(s)
                if row.get("worst_day_pct") is None:
                    means = [d.get("mean") for d in (daily.get(s["name"]) or [])
                             if d.get("mean") is not None]
                    if means:
                        row["worst_day_pct"] = min(float(x) for x in means)
                member_stat_by[s["name"]] = row
        except (OSError, json.JSONDecodeError):
            raw = None
    else:
        raw = None
    fees = fm.pt_fees()
    regime = fmb.load_regime()
    stats, books = run_combos(
        panel, recipes, fees=fees, regime=regime,
        member_stat_by=member_stat_by)
    write_combo_sidecar(stats)
    print(f"[combo] {len(stats)} mixes · outperform "
          f"{sum(1 for s in stats if s.get('outperforms'))}")
    for s in stats:
        flag = "WIN" if s.get("outperforms") else "   "
        print(f"  {flag} {s['name']:32s}  book={s.get('total_ret_pct'):+7.2f}  "
              f"dd={s.get('max_dd_pct'):5.2f}  "
              f"starts={s.get('start_green')}/{s.get('start_n')}  "
              f"eff={s.get('effectiveness')}")
    if args.write:
        if raw is None:
            raise SystemExit("03_scoreboard/factor_mine.json missing — "
                             "run python -m src.factor_mine --write first")
        payload = merge_into_payload(raw, stats, books)
        # Dashboard + scoreboard from the merged payload. Action blotters
        # only for combo books — passing member slims would strip their
        # per-name mark tables.
        fm.write_outputs(payload, payload["stats"], books=books)
        print(f"[combo] wrote {fm.OUT_JSON} n_recipes={payload['n_recipes']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
