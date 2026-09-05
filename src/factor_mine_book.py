"""Cash-accounted blotter for factor-mine recipes.

The first mine compounded equal-weight pick returns. That is **not**
the paper / live book:

  * $10k, whole shares, Futubull fees
  * leftover cash split equally among *new* names
  * sell first (cash freed), then buy
  * min-hold = recipe hold (trading sessions)
  * fill at the 09:30 open
  * hard-red morning S ≤ −3: sit, no new buys (same as flatten_robust)
  * skip if leftover cannot buy 1 share, or no open

This module is the book that the Action MD and dashboard blotter use.
It does not change live flatten_robust.
"""
from __future__ import annotations

import json
from pathlib import Path

from . import factor_mine as fm
from . import paper_trade as pt
from . import sleeve_merge as sm
from . import ticker_lookback as tl

ROOT = Path(__file__).resolve().parent.parent
OUT_DIR = ROOT / "03_scoreboard" / "factor_mine"
OUT_INDEX = ROOT / "03_scoreboard" / "FACTOR_MINE_ACTION.md"
DAILY_MD = ROOT / "01_daily" / "factor_mine_action.md"
HARD_RED = -3.0
UNIVERSES = ("auto", "union", "flatten", "probable", "yday_gainer", "ohlc_hot")
HOLDS = ("auto", "1", "3", "5")
GATES = (
    "auto", "none", "vol_g", "news_g", "white", "coil_off", "join_g",
    "last_green", "blue", "news_present", "join_present", "ab_g",
)
RANKS = ("auto", "none", "hot_score", "cond", "w_hot_cond", "w_hot_candle",
         "ret_5", "candle_score")
SIDES = ("auto", "long", "short")
TOP_NS = ("auto", "4", "8", "12")
EXITS = ("auto", "none", "alarm", "last_red", "news_bad")
ENTRIES = ("auto", "list", "live")
SIZES = ("auto", "leftover", "rank_w", "topheavy", "half")
SELLS = ("auto", "list", "time", "cut_loser", "trail")
S_BOOSTS = ("auto", "none", "sizeup", "more_names", "both")
BORROW_ANNUAL = 0.01
GOOD_S = 5.0
CUT_LOS = 0.03
TRAIL_OFF = 0.05
SIZEUP = 1.35
MORE_NAMES = 4
BOOK_RULES = {
    "capital": fm.CAPITAL,
    "day_cap": 1.0,
    "hard_red": HARD_RED,
    "hard_red_no_new": True,
    "sell_first": True,
    "fill": "open",
}


def morning_s(regime: dict | None, date: str):
    g = (regime or {}).get(date) or {}
    v = g.get("predict_score")
    try:
        return None if v is None else float(v)
    except (TypeError, ValueError):
        return None


def load_regime() -> dict:
    try:
        return (sm.load_payload() or {}).get("regime") or {}
    except Exception:
        return {}


def _px(ticker: str, date: str, which: str, bars: dict | None):
    bar = fm._bar(ticker, date, bars)
    return fm._finite(bar.get(which)) or fm._finite(bar.get("close"))


def _lot_px(lot: dict, date: str, which: str, bars) -> float:
    """Open/close for a held lot. A missing tape carries the last mark.

    Never fall back to a stale last_px from a prior session's *open* when
    today's bar is empty — that would replay yesterday's open→close as
    today's session.
    """
    px = _px(lot["ticker"], date, which, bars)
    if px is not None:
        return float(px)
    if which == "close":
        opx = _px(lot["ticker"], date, "open", bars)
        if opx is not None:
            return float(opx)
    return float(lot.get("close_px") or lot.get("last_px") or lot["entry_px"])


def _tone_glyph(v: str) -> str:
    return {"good": "🟢", "neutral": "🟡", "bad": "🔴"}.get(str(v), "⬛")


def camera_stamp(boxes: dict | None) -> str:
    bits = []
    for k in fm.CAMERAS:
        v = str((boxes or {}).get(k) or "missing")
        if v == "missing":
            continue
        bits.append(f"{k}{_tone_glyph(v)}")
    return " ".join(bits) or "—"


def why_buy(rec: dict, row: dict) -> str:
    bits = [rec.get("note") or rec["name"]]
    req = rec.get("require") or {}
    if req:
        shown = {k: v for k, v in req.items() if k != "live_entry"}
        if shown:
            bits.append("gate " + ",".join(f"{k}={v}" for k, v in shown.items()))
    if rec.get("rank"):
        bits.append(f"rank {rec['rank']}")
    src = ",".join(row.get("sources") or [])
    if src:
        bits.append(f"list {src}")
    plan = fm.flatten_plan(row.get("date") or "") if row.get("date") else {}
    if rec.get("universe") == "flatten" or req.get("live_entry"):
        if plan.get("flatten_ok"):
            bits.append(f"live flatten {plan.get('route') or 'mover'}")
        else:
            bits.append(
                f"wish-list (live {plan.get('route') or 'io'} HOLD — not a ticket)"
            )
    if row.get("blue"):
        bits.append("🔵")
    if row.get("zero_red"):
        bits.append("⚪")
    ret5 = row.get("ohlc_ret_5")
    if ret5 is not None:
        bits.append(f"ret5={float(ret5):+.1f}")
    return "; ".join(bits)


def why_sell(ticker: str, held: int, min_hold: int, early: bool,
             exit_when: dict | None, dropped: bool, kind: str = "") -> str:
    if early:
        if (exit_when or {}).get("alarm"):
            return f"exit 🚨 after {held} sess"
        if (exit_when or {}).get("last_red"):
            return f"exit last-red after {held} sess"
        if (exit_when or {}).get("news") == "bad":
            return f"exit news🔴 after {held} sess"
        return f"condition exit after {held} sess"
    if kind == "time":
        return f"time-stop after {held} sess (min {min_hold})"
    if kind == "cut_loser":
        return f"cut loser after {held} sess (−{CUT_LOS:.0%} vs entry)"
    if kind == "trail":
        return f"trail off peak after {held} sess (−{TRAIL_OFF:.0%})"
    if dropped:
        return f"dropped from list after {held} sess (min {min_hold})"
    return f"sold after {held} sess"


def lot_should_sell(lot: dict, *, held: int, min_hold: int, early: bool,
                    dropped: bool, sell_mode: str, px: float | None,
                    side: str) -> tuple[bool, str]:
    """Sell only lots we hold. Min-hold blocks everything except early exit."""
    if early:
        return True, "early"
    if held < min_hold:
        return False, "min_hold"
    mode = sell_mode or "list"
    entry = float(lot.get("entry_px") or 0) or 0.0
    peak = float(lot.get("peak_px") or entry) or entry
    if mode == "time":
        return True, "time"
    if mode == "cut_loser" and px and entry:
        if side == "long" and px < entry * (1.0 - CUT_LOS):
            return True, "cut_loser"
        if side == "short" and px > entry * (1.0 + CUT_LOS):
            return True, "cut_loser"
        if dropped:
            return True, "dropped"
        return False, "keep"
    if mode == "trail" and px and peak:
        if side == "long" and px < peak * (1.0 - TRAIL_OFF):
            return True, "trail"
        if side == "short" and px > peak * (1.0 + TRAIL_OFF):
            return True, "trail"
        if dropped:
            return True, "dropped"
        return False, "keep"
    if dropped:
        return True, "dropped"
    return False, "keep"


def split_budgets(new: list, room: float, mode: str) -> list[float]:
    """Split leftover cash. Never invent money — sum(budgets) ≤ room."""
    n = len(new)
    if n < 1 or room <= 0:
        return [0.0] * n
    mode = mode or "leftover"
    if mode == "half":
        room = room * 0.5
        return [room / n] * n
    if mode == "rank_w":
        weights = list(range(n, 0, -1))
        tot = float(sum(weights))
        return [room * w / tot for w in weights]
    if mode == "topheavy":
        if n == 1:
            return [room]
        first = room * 0.40
        rest = (room - first) / (n - 1)
        return [first] + [rest] * (n - 1)
    return [room / n] * n


def _stamp_equity(rec_t: dict, cash: float, pos: dict, date: str,
                  bars, side: str, rules: dict) -> None:
    """Cash + open mark of lots still held, as of this fill."""
    stock = 0.0
    for lot in pos.values():
        px = _lot_px(lot, date, "open", bars)
        notional = lot["shares"] * float(px)
        stock += notional if side == "long" else -notional
    eq = float(cash) + stock
    cap = float(rules.get("capital") or fm.CAPITAL)
    rec_t["equity_after"] = round(eq, 2)
    rec_t["equity_delta"] = round(eq - cap, 2)
    rec_t["stock_after"] = round(stock, 2)


def _signed_px_delta(shares: int, new_px: float, old_px: float,
                     side: str) -> float:
    raw = int(shares) * (float(new_px) - float(old_px))
    return raw if side == "long" else -raw


def _pct_move(new_px, old_px, side: str):
    try:
        old_px = float(old_px)
        new_px = float(new_px)
    except (TypeError, ValueError):
        return None
    if old_px == 0:
        return None
    pct = 100.0 * (new_px / old_px - 1.0)
    return round(pct if side == "long" else -pct, 3)


def _overnight_marks(pos: dict, date: str, bars, side: str,
                     yday_equity: float, open_cash: float) -> dict:
    """09:30 snapshot vs yesterday's close. Cash does not change overnight."""
    names = []
    open_stock = 0.0
    bits = []
    for t, lot in pos.items():
        shares = int(lot["shares"])
        yday_px = float(lot.get("close_px") or lot.get("last_px") or lot["entry_px"])
        opx = _px(t, date, "open", bars)
        if opx is None:
            opx = yday_px
        opx = float(opx)
        dlt = _signed_px_delta(shares, opx, yday_px, side)
        if side == "long":
            open_stock += shares * opx
        else:
            open_stock -= shares * opx
        entry = float(lot.get("entry_px") or yday_px)
        names.append({
            "ticker": t, "shares": shares,
            "yday_px": round(yday_px, 4), "open_px": round(opx, 4),
            "entry_px": round(entry, 4),
            "entry_date": lot.get("entry_date"),
            "delta": round(dlt, 2),
            "overnight": round(dlt, 2),
            "pct": _pct_move(opx, yday_px, side),
            "vs_entry_open": round(_signed_px_delta(shares, opx, entry, side), 2),
        })
        bits.append(
            f"{t}×{shares} yday ${yday_px:.2f} → 09:30 ${opx:.2f} {dlt:+.2f}"
        )
    open_eq = float(open_cash) + open_stock
    overnight_delta = open_eq - float(yday_equity)
    if not names:
        why = (
            f"09:30 open · cash ${open_cash:,.2f} · no holdings · "
            f"equity ${open_eq:,.2f} vs prior close ${float(yday_equity):,.2f} "
            f"({overnight_delta:+.2f}). Cash unchanged overnight; no fees."
        )
    else:
        why = (
            f"09:30 open · cash ${open_cash:,.2f} (unchanged overnight, no fees) · "
            f"equity ${open_eq:,.2f} vs prior close ${float(yday_equity):,.2f} "
            f"({overnight_delta:+.2f}) · {len(names)} name(s) re-marked at the "
            f"open (per-name table). "
            + "; ".join(bits)
        )
    return {
        "open_stock": round(open_stock, 2),
        "open_equity": round(open_eq, 2),
        "yday_equity": round(float(yday_equity), 2),
        "overnight_delta": round(overnight_delta, 2),
        "overnight": names,
        "overnight_why": why,
        "overnight_detail": "; ".join(bits),
    }


def _day_marks(overnight: list, pos: dict, date: str, bars,
               side: str) -> list[dict]:
    """Every lot this session: prior close → 09:30 open → close."""
    by: dict[str, dict] = {}
    for n in overnight or []:
        t = n["ticker"]
        ov = float(n["overnight"] if n.get("overnight") is not None
                   else n.get("delta") or 0)
        by[t] = {
            "ticker": t,
            "shares_open": int(n["shares"]),
            "shares_close": 0,
            "shares": int(n["shares"]),
            "yday_px": n.get("yday_px"),
            "open_px": n.get("open_px"),
            "close_px": None,
            "entry_px": n.get("entry_px"),
            "entry_date": n.get("entry_date"),
            "overnight": ov,
            "session": 0.0,
            "day": ov,
            "pct_overnight": n.get("pct"),
            "pct_session": None,
            "vs_entry_open": n.get("vs_entry_open"),
            "vs_entry_close": None,
            "held": "sold",
        }
    for t, lot in pos.items():
        shares = int(lot["shares"])
        if t in by and by[t].get("open_px") is not None:
            opx = float(by[t]["open_px"])
        else:
            opx = _lot_px(lot, date, "open", bars)
        cpx = float(lot["close_px"]) if lot.get("close_px") is not None else _lot_px(
            lot, date, "close", bars)
        sess = _signed_px_delta(shares, cpx, opx, side)
        entry = float(lot.get("entry_px") or opx)
        vs_close = _signed_px_delta(shares, cpx, entry, side)
        if t in by:
            by[t]["shares_close"] = shares
            by[t]["shares"] = shares
            by[t]["close_px"] = round(cpx, 4)
            by[t]["session"] = round(sess, 2)
            by[t]["day"] = round(by[t]["overnight"] + sess, 2)
            by[t]["pct_session"] = _pct_move(cpx, opx, side)
            by[t]["vs_entry_close"] = round(vs_close, 2)
            by[t]["held"] = "through"
            if by[t].get("entry_px") is None:
                by[t]["entry_px"] = round(entry, 4)
                by[t]["entry_date"] = lot.get("entry_date")
        else:
            vs_open = _signed_px_delta(shares, opx, entry, side)
            by[t] = {
                "ticker": t,
                "shares_open": 0,
                "shares_close": shares,
                "shares": shares,
                "yday_px": None,
                "open_px": round(opx, 4),
                "close_px": round(cpx, 4),
                "entry_px": round(entry, 4),
                "entry_date": lot.get("entry_date"),
                "overnight": 0.0,
                "session": round(sess, 2),
                "day": round(sess, 2),
                "pct_overnight": None,
                "pct_session": _pct_move(cpx, opx, side),
                "vs_entry_open": round(vs_open, 2),
                "vs_entry_close": round(vs_close, 2),
                "held": "bought",
            }
    return list(by.values())


def _session_why(marks: list, cash: float, close_eq: float,
                 open_eq: float) -> str:
    sess = round(sum(float(m.get("session") or 0) for m in marks), 2)
    gap = round(float(close_eq) - float(open_eq), 2)
    held = [m for m in marks if m.get("shares_close")]
    if not held:
        return (
            f"16:00 close · cash ${cash:,.2f} · no lots left · "
            f"equity ${float(close_eq):,.2f}."
        )
    bits = [
        f"{m['ticker']}×{m['shares_close']} "
        f"09:30 ${float(m['open_px']):.2f} → close ${float(m['close_px']):.2f} "
        f"{float(m['session']):+.2f}"
        for m in held if m.get("open_px") is not None and m.get("close_px") is not None
    ]
    return (
        f"16:00 close · cash ${cash:,.2f} · equity ${float(close_eq):,.2f} vs "
        f"09:30 ${float(open_eq):,.2f} ({gap:+.2f}; session marks {sess:+.2f}) · "
        f"{len(held)} name(s) marked open→close (per-name table). "
        + "; ".join(bits)
    )


MARK_TOL = 0.16


def reconcile_marks(book: dict) -> dict:
    """Prove per-name overnight + session $ sum to the printed equity path."""
    fails: list[str] = []
    daily = book.get("daily") or []
    trades = book.get("trades") or []
    opens = [t for t in trades if t.get("side") == "OPEN"]
    closes = [t for t in trades if t.get("side") == "CLOSE"]
    dates = [d["date"] for d in daily]
    if [t["date"] for t in opens] != dates:
        fails.append(f"OPEN rows {[t['date'] for t in opens]} ≠ sessions {dates}")
    if [t["date"] for t in closes] != dates:
        fails.append(f"CLOSE rows {[t['date'] for t in closes]} ≠ sessions {dates}")
    for d in daily:
        date = d["date"]
        marks = d.get("marks") or []
        ov_sum = round(sum(float(m.get("overnight") or 0) for m in marks), 2)
        printed_ov = float(d.get("overnight_delta") or 0)
        if abs(ov_sum - printed_ov) > MARK_TOL:
            fails.append(f"{date} overnight sum {ov_sum} ≠ {printed_ov}")
        for h in d.get("open_held") or []:
            tick = str(h).split("×")[0]
            m = next((x for x in marks if x.get("ticker") == tick), None)
            if not m:
                fails.append(f"{date} open held {tick} missing from marks")
                continue
            if m.get("open_px") is None:
                fails.append(f"{date} {tick} missing 09:30 open")
            if m.get("overnight") is None:
                fails.append(f"{date} {tick} missing overnight $")
        for lot in d.get("lots") or []:
            tick = lot["ticker"]
            m = next((x for x in marks if x.get("ticker") == tick), None)
            if not m or m.get("close_px") is None:
                fails.append(f"{date} close held {tick} missing close mark")
            elif m.get("session") is None:
                fails.append(f"{date} {tick} missing session $")
        if not (d.get("bought") or d.get("sold")):
            if abs(float(d["cash"]) - float(d["open_cash"])) > 0.05:
                fails.append(
                    f"{date} no-fill cash {d['cash']} ≠ open {d['open_cash']}")
            sess = round(sum(float(m.get("session") or 0) for m in marks), 2)
            gap = round(float(d["equity"]) - float(d["open_equity"]), 2)
            if abs(sess - gap) > MARK_TOL:
                fails.append(f"{date} no-fill session {sess} ≠ close−open {gap}")
        else:
            day_fills = [
                t for t in trades
                if t.get("date") == date
                and t.get("side") not in ("OPEN", "CLOSE")
                and t.get("equity_after") is not None
            ]
            if day_fills:
                last_eq = float(day_fills[-1]["equity_after"])
                sess = round(sum(
                    float(m.get("session") or 0) for m in marks
                    if m.get("shares_close")), 2)
                expect = round(last_eq + sess, 2)
                if abs(expect - float(d["equity"])) > MARK_TOL:
                    fails.append(
                        f"{date} last-fill {last_eq} + session {sess} = {expect} "
                        f"≠ close {d['equity']}")
    return {"ok": not fails, "n_fail": len(fails), "fails": fails[:24]}


def equity_walk(book: dict, from_date: str, to_date: str) -> dict:
    """Name-by-name path from the last fill on from_date to a later fill.

    Legs are from_date's open→close marks, each intervening overnight and
    session, to_date's overnight, then fills on to_date up to the first
    (or matching) fill. Sum of legs must equal end equity − start equity.
    """
    trades = book.get("trades") or []
    daily = {d["date"]: d for d in (book.get("daily") or [])}
    start_t = None
    for t in trades:
        if t.get("date") == from_date and t.get("side") not in ("OPEN", "CLOSE"):
            start_t = t
    if start_t is None:
        start_t = next(
            (t for t in reversed(trades)
             if t.get("date") == from_date and t.get("side") == "OPEN"),
            None)
    end_t = next(
        (t for t in trades
         if t.get("date") == to_date and t.get("side") not in ("OPEN", "CLOSE")),
        None)
    if end_t is None:
        end_t = next(
            (t for t in trades
             if t.get("date") == to_date and t.get("side") == "CLOSE"),
            None)
    start_eq = float((start_t or {}).get("equity_after")
                     or daily[from_date]["equity"])
    end_eq = float((end_t or {}).get("equity_after")
                   or daily[to_date]["equity"])
    dates = [d["date"] for d in (book.get("daily") or [])
             if from_date <= d["date"] <= to_date]
    legs = []
    d0 = daily[from_date]
    sess0 = [m for m in (d0.get("marks") or []) if m.get("shares_close")]
    s0 = round(sum(float(m.get("session") or 0) for m in sess0), 2)
    legs.append({
        "date": from_date, "kind": "session", "delta": s0,
        "names": sess0, "equity_after": round(start_eq + s0, 2),
    })
    for date in dates[1:]:
        d = daily[date]
        od = float(d.get("overnight_delta") or 0)
        legs.append({
            "date": date, "kind": "overnight", "delta": od,
            "names": d.get("marks") or d.get("overnight") or [],
            "equity_after": round(float(d["open_equity"]), 2),
        })
        if date < to_date:
            sess = [m for m in (d.get("marks") or []) if m.get("shares_close")]
            sd = round(sum(float(m.get("session") or 0) for m in sess), 2)
            legs.append({
                "date": date, "kind": "session", "delta": sd,
                "names": sess, "equity_after": round(float(d["equity"]), 2),
            })
        else:
            running = float(d["open_equity"])
            for t in trades:
                if t.get("date") != date or t.get("side") in ("OPEN", "CLOSE"):
                    continue
                nxt = float(t.get("equity_after") or running)
                legs.append({
                    "date": date, "kind": "fill",
                    "side": t.get("side"), "ticker": t.get("ticker"),
                    "delta": round(nxt - running, 2),
                    "pnl": t.get("pnl"), "fees": t.get("fees"),
                    "sell_eq_chg": t.get("sell_eq_chg"),
                    "equity_after": nxt,
                })
                running = nxt
                if t is end_t:
                    break
    sum_delta = round(sum(float(x["delta"]) for x in legs), 2)
    expect = round(end_eq - start_eq, 2)
    return {
        "from_date": from_date, "to_date": to_date,
        "start_equity": start_eq, "end_equity": end_eq,
        "sum_delta": sum_delta, "expect_delta": expect,
        "ok": abs(sum_delta - expect) <= MARK_TOL,
        "legs": legs,
    }


def _lots_snap(pos: dict) -> list[dict]:
    return [
        {"ticker": t, "shares": int(p["shares"]),
         "entry_date": p.get("entry_date"), "entry_px": p.get("entry_px")}
        for t, p in pos.items()
    ]


def audit_book(book: dict, *, capital: float | None = None,
               side: str = "long") -> dict:
    """Independent replay of fills. Proves the butterfly cash+holdings.

    Fails if a sell names a lot we do not hold, a buy spends past leftover
    cash, cash goes negative, or a printed cash_after disagrees.
    """
    cap = float(capital if capital is not None else (
        (book.get("rules") or {}).get("capital") or fm.CAPITAL))
    cash = cap
    hold: dict[str, int] = {}
    fails: list[str] = []
    side = side or "long"
    for t in book.get("trades") or []:
        ticker = t.get("ticker")
        shares = int(t.get("shares") or 0)
        px = float(t.get("price") or 0)
        fee = float(t.get("fees") or 0)
        date = t.get("date")
        kind = t.get("side")
        if kind in ("OPEN", "CLOSE"):
            continue
        if shares < 1 or not ticker:
            fails.append(f"{date} empty fill {kind} {ticker}")
            continue
        if kind in ("SELL", "COVER"):
            have = int(hold.get(ticker) or 0)
            if shares > have:
                fails.append(
                    f"{date} sold {ticker} x{shares} but held {have}")
            else:
                if side == "long" or kind == "SELL":
                    cash += shares * px - fee
                else:
                    cash -= shares * px + fee
                left = have - shares
                if left:
                    hold[ticker] = left
                else:
                    hold.pop(ticker, None)
        elif kind in ("BUY", "SHORT"):
            if kind == "BUY" or side == "long":
                cost = shares * px + fee
                if cost > cash + 0.05:
                    fails.append(
                        f"{date} buy {ticker} ${cost:.2f} > cash ${cash:.2f}")
                cash -= cost
                hold[ticker] = int(hold.get(ticker) or 0) + shares
            else:
                cash += shares * px - fee
                hold[ticker] = int(hold.get(ticker) or 0) + shares
        if cash < -0.05:
            fails.append(f"{date} cash negative ${cash:.2f} after {kind} {ticker}")
        printed = t.get("cash_after")
        if printed is not None and abs(float(printed) - cash) > 0.08:
            fails.append(
                f"{date} {kind} {ticker} cash_after ${printed} ≠ replay ${cash:.2f}")
    last_cash = book.get("cash")
    if last_cash is not None and abs(float(last_cash) - cash) > 0.08:
        fails.append(f"final cash ${last_cash} ≠ replay ${cash:.2f}")
    open_held = {p["ticker"]: int(p["shares"]) for p in (book.get("open") or [])}
    if open_held != hold:
        fails.append(f"open lots {open_held} ≠ replay {hold}")
    return {
        "ok": not fails,
        "n_fail": len(fails),
        "fails": fails[:24],
        "final_cash": round(cash, 2),
        "final_held": hold,
    }


def recipes_from_action(*, universe="auto", hold="auto", gate="auto",
                        rank="auto", side="auto", top_n="auto",
                        exit="auto", entry="auto", size="auto",
                        sell="auto", s_boost="auto",
                        auto_tweak=True) -> list[dict]:
    """Filter the systematic grid; auto dims stay swept.

    ``auto_tweak`` adds one-knob neighbors so a custom dropdown still
    explores nearby holds / gates / ranks / top-n / exits / universes
    / live-vs-list entry / size / sell / S-boost without a second click.
    """
    base = fm.build_recipes()

    def gate_name(rec: dict) -> str:
        req = rec.get("require") or {}
        req = {k: v for k, v in req.items() if k != "live_entry"}
        if not req:
            return "none"
        if req.get("vol") == "good" and len(req) == 1:
            return "vol_g"
        if req.get("news") == "good" and len(req) == 1:
            return "news_g"
        if req.get("zero_red"):
            return "white"
        if "ret_5_max" in req and "rvol_max" in req and not req.get("last_green"):
            return "coil_off"
        if req.get("join") == "good" and len(req) == 1:
            return "join_g"
        if req.get("last_green") and len(req) == 1:
            return "last_green"
        if req.get("blue") and len(req) == 1:
            return "blue"
        if req.get("news_present"):
            return "news_present"
        if req.get("join_present"):
            return "join_present"
        if req.get("ab") == "good" and len(req) == 1:
            return "ab_g"
        return "other"

    def exit_name(rec: dict) -> str:
        ex = rec.get("exit_when") or {}
        if ex.get("alarm"):
            return "alarm"
        if ex.get("last_red"):
            return "last_red"
        if ex.get("news") == "bad":
            return "news_bad"
        return "none"

    def entry_name(rec: dict) -> str:
        return "live" if (rec.get("require") or {}).get("live_entry") else "list"

    def keep(rec, *, uni, h, g, rk, sd, tn, ex, en, sz, sl, sb) -> bool:
        if uni != "auto" and rec["universe"] != uni:
            return False
        if h != "auto" and int(rec["hold"]) != int(h):
            return False
        if g != "auto" and gate_name(rec) != g:
            return False
        if rk != "auto":
            have = rec.get("rank") or "none"
            if have != rk:
                return False
        if sd != "auto" and rec["side"] != sd:
            return False
        if tn != "auto" and int(rec["top_n"]) != int(tn):
            return False
        if ex != "auto" and exit_name(rec) != ex:
            return False
        if en != "auto" and entry_name(rec) != en:
            return False
        if sz != "auto" and (rec.get("size") or "leftover") != sz:
            return False
        if sl != "auto" and (rec.get("sell") or "list") != sl:
            return False
        if sb != "auto" and (rec.get("s_boost") or "none") != sb:
            return False
        return True

    pinned = dict(uni=universe, h=hold, g=gate, rk=rank, sd=side,
                  tn=top_n, ex=exit, en=entry, sz=size, sl=sell, sb=s_boost)
    kept = [r for r in base if keep(r, **pinned)]
    if auto_tweak:
        extras = []
        dims = ("h", "g", "rk", "tn", "ex", "en", "uni", "sz", "sl", "sb")
        raw = dict(h=hold, g=gate, rk=rank, tn=top_n, ex=exit,
                   en=entry, uni=universe, sz=size, sl=sell, sb=s_boost)
        for dim in dims:
            if raw[dim] == "auto":
                continue
            neighbor = dict(pinned)
            neighbor[dim] = "auto"
            extras += [r for r in base if keep(r, **neighbor)]
        seen = {r["name"] for r in kept}
        for r in extras:
            if r["name"] not in seen:
                kept.append(r)
                seen.add(r["name"])
    return kept or list(base)


def simulate_book(panel: dict, rec: dict, *, bars=None, fees=None,
                  regime=None, rules=None, start: str | None = None) -> dict:
    """Walk one recipe as a $10k paper sleeve. Sell first, then buy."""
    rules = {**BOOK_RULES, **(rules or {})}
    fees = fees if fees is not None else pt.load_fees()
    cal_all = list(panel.get("session_dates") or [])
    cal = [d for d in cal_all if not start or d >= start]
    by_date = panel.get("by_date") or {}
    row_index = {(r["date"], r["ticker"]): r for r in (panel.get("rows") or [])}
    cash = float(rules["capital"])
    pos: dict[str, dict] = {}
    trades: list[dict] = []
    skips: list[dict] = []
    daily: list[dict] = []
    date_ix = {d: i for i, d in enumerate(cal)}
    min_hold = int(rec["hold"])
    side = rec.get("side") or "long"
    day_cap = float(rec.get("day_cap") or rules["day_cap"])
    size_mode = rec.get("size") or "leftover"
    sell_mode = rec.get("sell") or "list"
    s_boost = rec.get("s_boost") or "none"
    yday_equity = float(rules["capital"])

    def mark(date: str, which: str) -> float:
        tot = 0.0
        for lot in pos.values():
            px = _lot_px(lot, date, which, bars)
            notional = lot["shares"] * float(px)
            tot += notional if side == "long" else -notional
        return tot

    for date in cal:
        s = morning_s(regime, date)
        hard_red = (rules.get("hard_red_no_new")
                    and s is not None and float(s) <= float(rules["hard_red"]))
        good_s = (s is not None and float(s) >= GOOD_S and not hard_red)
        rec_day = rec
        if good_s and s_boost in ("more_names", "both"):
            rec_day = dict(rec, top_n=int(rec["top_n"]) + MORE_NAMES)
        chosen = fm.pick_day(by_date.get(date) or [], rec_day)
        tset = {r["ticker"] for r in chosen}
        sold, bought, held_names = [], [], []
        day_why = []
        open_cash = cash
        open_lots = _lots_snap(pos)
        ov = _overnight_marks(pos, date, bars, side, yday_equity, open_cash)
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
        if good_s and s_boost in ("more_names", "both"):
            day_why.append(f"S={s:+.2f} more_names top_n={rec_day['top_n']}")
        if good_s and s_boost in ("sizeup", "both"):
            day_why.append(f"S={s:+.2f} sizeup x{SIZEUP:g}")

        for t in list(pos):
            lot = pos[t]
            held = date_ix[date] - date_ix.get(lot["entry_date"], date_ix[date])
            row = row_index.get((date, t)) or {}
            early = fm.should_exit(row, rec.get("exit_when"))
            dropped = t not in tset
            px = _px(t, date, "open", bars)
            if px is not None:
                if side == "long":
                    lot["peak_px"] = max(float(lot.get("peak_px") or lot["entry_px"]), px)
                else:
                    lot["peak_px"] = min(float(lot.get("peak_px") or lot["entry_px"]), px)
                lot["last_px"] = px
            do_sell, kind = lot_should_sell(
                lot, held=held, min_hold=min_hold, early=early,
                dropped=dropped, sell_mode=sell_mode, px=px, side=side)
            if not do_sell:
                if dropped and held < min_hold:
                    skips.append({
                        "date": date, "ticker": t, "kind": "min_hold",
                        "reason": f"dropped but min-hold {held}/{min_hold} sess — no sell",
                    })
                held_names.append(t)
                continue
            if px is None:
                skips.append({"date": date, "ticker": t, "kind": "no_price",
                              "reason": "no 09:30 open — carry"})
                held_names.append(t)
                continue
            reason = why_sell(t, held, min_hold, early,
                              rec.get("exit_when"), dropped, kind)
            eq_before = cash + mark(date, "open")
            fee = pt.order_fees(lot["shares"], px, "sell" if side == "long" else "buy", fees)
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
                "date": date, "ticker": t, "side": "SELL" if side == "long" else "COVER",
                "shares": lot["shares"], "price": round(px, 4), "fees": fee,
                "cash_after": round(cash, 2),
                "pnl": round(pnl, 2),
                "reason": reason,
                "held": held,
                "cameras": camera_stamp(row.get("boxes")),
            }
            _stamp_equity(rec_t, cash, pos, date, bars, side, rules)
            rec_t["equity_before"] = round(eq_before, 2)
            rec_t["sell_eq_chg"] = round(rec_t["equity_after"] - eq_before, 2)
            rec_t["vs_yday"] = round(rec_t["equity_after"] - yday_equity, 2)
            trades.append(rec_t)
            sold.append(rec_t)
            day_why.append(f"SELL {t} ({reason})")

        new = [r for r in chosen if r["ticker"] not in pos]
        if hard_red:
            for r in new:
                skips.append({
                    "date": date, "ticker": r["ticker"], "kind": "hard_red",
                    "reason": f"hard-red S={s:+.2f} sit; no new buys",
                })
            if new:
                day_why.append(f"hard-red S={s:+.2f} sit; no new buys")
            new = []

        if new and (cash > 0 or side == "short"):
            eq_open = cash + mark(date, "open")
            if side == "short":
                room = max(0.0, eq_open * min(day_cap, 0.5))
            else:
                room = max(0.0, cash * day_cap)
            if good_s and s_boost in ("sizeup", "both"):
                room = min(room * SIZEUP, cash if side == "long" else room * SIZEUP)
                if side == "long":
                    room = min(room, cash)
            budgets = split_budgets(new, room, size_mode)
            for row, per in zip(new, budgets):
                t = row["ticker"]
                px = _px(t, date, "open", bars)
                reason = why_buy(rec, row) + f"; leftover ${per:.2f}"
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
                    lot = {
                        "ticker": t, "shares": shares, "entry_px": px,
                        "entry_date": date, "cost": cost, "fee_in": fee,
                        "notional": shares * px, "last_px": px, "peak_px": px,
                        "reason": reason,
                    }
                else:
                    notional = shares * px
                    eq_now = cash + mark(date, "open")
                    if eq_now < 2 * notional:
                        skips.append({
                            "date": date, "ticker": t, "kind": "cash",
                            "reason": f"short cover {2*notional:.0f} > equity {eq_now:.0f}",
                        })
                        continue
                    borrow = notional * BORROW_ANNUAL / 365.0
                    fee = pt.order_fees(shares, px, "sell", fees) + borrow
                    cash += notional - fee
                    lot = {
                        "ticker": t, "shares": shares, "entry_px": px,
                        "entry_date": date, "cost": fee, "fee_in": fee,
                        "notional": notional, "last_px": px, "peak_px": px,
                        "reason": reason,
                    }
                pos[t] = lot
                rec_t = {
                    "date": date, "ticker": t,
                    "side": "BUY" if side == "long" else "SHORT",
                    "shares": shares, "price": round(px, 4), "fees": fee,
                    "cash_after": round(cash, 2),
                    "pnl": None,
                    "reason": reason,
                    "held": 0,
                    "cameras": camera_stamp(row.get("boxes")),
                }
                _stamp_equity(rec_t, cash, pos, date, bars, side, rules)
                trades.append(rec_t)
                bought.append(rec_t)
                day_why.append(f"{rec_t['side']} {t} x{shares} @ {px:.2f}")
                held_names.append(t)
        for t in pos:
            if t not in held_names:
                held_names.append(t)

        for lot in pos.values():
            lot["close_px"] = _lot_px(lot, date, "close", bars)
        stock = mark(date, "close")
        equity = cash + stock
        plan = fm.flatten_plan(date)
        ov_why = ov["overnight_why"]
        marks = _day_marks(ov["overnight"], pos, date, bars, side)
        sess_sum = round(sum(float(m.get("session") or 0) for m in marks), 2)
        close_why = _session_why(marks, cash, equity, ov["open_equity"])
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
            "close_held": [f"{p['ticker']}×{p['shares']}" for p in _lots_snap(pos)],
            "reason": close_why,
            "held": 0, "cameras": "",
        })
        fill_why = "; ".join(day_why) or (
            f"hard-red sit S={s:+.2f}" if hard_red else
            ("hold " + ",".join(pos.keys()) if pos else "flat cash")
        )
        daily.append({
            "date": date,
            "s": None if s is None else round(s, 2),
            "hard_red": hard_red,
            "route": plan.get("route") or "",
            "flatten_ok": bool(plan.get("flatten_ok")),
            "n": len(chosen),
            "open_cash": round(open_cash, 2),
            "open_held": [f"{p['ticker']}×{p['shares']}" for p in open_lots],
            "open_equity": ov["open_equity"],
            "open_stock": ov["open_stock"],
            "yday_equity": ov["yday_equity"],
            "overnight_delta": ov["overnight_delta"],
            "overnight": ov["overnight"],
            "overnight_why": ov_why,
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
            "lots": _lots_snap(pos),
            "skipped": [k["ticker"] for k in skips if k["date"] == date],
            "why": ov_why + ((" · " + fill_why) if day_why else " · " + close_why),
            "made_money": False,
        })
        yday_equity = round(equity, 2)

    for i, d in enumerate(daily):
        prev = float(rules["capital"]) if i == 0 else daily[i - 1]["equity"]
        d["mean"] = None if prev <= 0 else round(100.0 * (d["equity"] / prev - 1.0), 4)
        d["made_money"] = bool(d["mean"] is not None and d["mean"] > 0)

    equity = [float(rules["capital"])] + [d["equity"] for d in daily]
    total_ret = round(100.0 * (equity[-1] / rules["capital"] - 1.0), 3) if equity else 0.0
    closed = [t for t in trades if t.get("pnl") is not None]
    wins = [t for t in closed if (t.get("pnl") or 0) > 0]
    losses = [t for t in closed if (t.get("pnl") or 0) < 0]
    out = {
        "name": rec["name"],
        "rules": {k: rules[k] for k in BOOK_RULES},
        "size": size_mode,
        "sell": sell_mode,
        "s_boost": s_boost,
        "cash": round(cash, 2),
        "n_open": len(pos),
        "open": [
            {"ticker": t, "shares": p["shares"], "entry_date": p["entry_date"],
             "entry_px": p["entry_px"], "reason": p.get("reason")}
            for t, p in pos.items()
        ],
        "n_trades": len([t for t in trades if t.get("side") not in ("OPEN", "CLOSE")]),
        "n_skips": len(skips),
        "n_closed": len(closed),
        "n_wins": len(wins),
        "n_losses": len(losses),
        "realized": round(sum(t.get("pnl") or 0 for t in closed), 2),
        "total_ret_pct": total_ret,
        "final_equity": equity[-1] if equity else rules["capital"],
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
    out["audit"] = audit_book(out, capital=rules["capital"], side=side)
    mark_aud = reconcile_marks(out)
    out["marks_audit"] = mark_aud
    if mark_aud["ok"]:
        out["audit"]["marks_ok"] = True
    else:
        fails = list(out["audit"].get("fails") or []) + list(mark_aud.get("fails") or [])
        out["audit"] = {
            **out["audit"],
            "ok": False,
            "n_fail": len(fails),
            "fails": fails[:24],
            "marks_ok": False,
        }
    return out


def slim_start_path(book: dict, start: str, cal: list[str]) -> dict:
    """Cash-start path: $10k, no lots, same rules, from ``start`` to the end."""
    daily = book.get("daily") or []
    d0 = next((d for d in daily if d.get("date") == start),
              daily[0] if daily else {})
    buys = []
    for t in book.get("trades") or []:
        if t.get("date") == start and t.get("side") in ("BUY", "SHORT"):
            buys.append({
                "ticker": t.get("ticker"),
                "shares": t.get("shares"),
                "price": t.get("price"),
                "fees": t.get("fees"),
                "reason": t.get("reason"),
            })
    skips = []
    for k in book.get("skips") or []:
        if k.get("date") != start:
            continue
        skips.append({
            "ticker": k.get("ticker"),
            "kind": k.get("kind"),
            "reason": k.get("reason"),
        })
        if len(skips) >= 16:
            break
    eq_by = {d["date"]: d.get("equity") for d in daily}
    days = []
    for d in daily:
        days.append({
            "date": d.get("date"),
            "s": d.get("s"),
            "hard_red": d.get("hard_red"),
            "bought": d.get("bought") or [],
            "sold": d.get("sold") or [],
            "cash": d.get("cash"),
            "equity": d.get("equity"),
            "open_cash": d.get("open_cash"),
            "made_money": d.get("made_money"),
        })
    ret = book.get("total_ret_pct")
    return {
        "start": start,
        "return_pct": ret,
        "made_money": (ret or 0) > 0,
        "n_sessions": len(daily),
        "final_equity": book.get("final_equity"),
        "s": d0.get("s"),
        "hard_red": bool(d0.get("hard_red")),
        "open_cash": d0.get("open_cash"),
        "cash": d0.get("cash"),
        "bought": d0.get("bought") or [],
        "buys": buys,
        "skips": skips,
        "n_up_days": sum(1 for d in days if d.get("made_money")),
        "equity": [eq_by.get(d) for d in cal],
        "days": days,
    }


def replay_starts(panel: dict, rec: dict, **kw) -> list[dict]:
    cal = list(panel.get("session_dates") or [])
    out = []
    for start in cal:
        book = simulate_book(panel, rec, start=start, **kw)
        out.append(slim_start_path(book, start, cal))
    return out


def attach_book(stats: dict, book: dict, starts: list[dict]) -> dict:
    """Overwrite money fields with the cash book; keep pick capture stats."""
    days = [d for d in book.get("daily") or [] if d.get("mean") is not None]
    n_green = sum(1 for s in starts if s["made_money"])
    start_rets = [s["return_pct"] for s in starts]
    median_start = (round(float(sorted(start_rets)[len(start_rets) // 2]), 3)
                    if start_rets else None)
    means = [d["mean"] for d in days]
    pothole_pct = max(means) if means else None
    pothole_date = None
    if means:
        pothole_date = next(d["date"] for d in days if d["mean"] == pothole_pct)
    reliable = (
        (stats.get("n_graded") or 0) >= fm.MIN_GRADED
        and len(starts) >= fm.MIN_STARTS
        and len(days) >= fm.MIN_DAYS
    )
    stats = dict(stats)
    stats["signal_ret_pct"] = stats.get("total_ret_pct")
    stats["book"] = True
    stats["total_ret_pct"] = book["total_ret_pct"]
    stats["final_equity"] = book["final_equity"]
    stats["equity"] = book["equity"]
    stats["daily"] = book["daily"]
    stats["starts"] = starts
    stats["start_n"] = len(starts)
    stats["start_green"] = n_green
    stats["start_rate"] = None if not starts else round(n_green / len(starts), 4)
    stats["median_start_pct"] = median_start
    stats["pothole_date"] = pothole_date
    stats["pothole_pct"] = None if pothole_pct is None else round(float(pothole_pct), 3)
    stats["profitable_day_rate"] = None if not days else round(
        sum(1 for d in days if d["made_money"]) / len(days), 4)
    stats["book_win_rate"] = book.get("win_rate")
    stats["book_n_trades"] = book.get("n_trades")
    stats["book_n_skips"] = book.get("n_skips")
    stats["book_realized"] = book.get("realized")
    aud = book.get("audit") or {}
    stats["audit_ok"] = bool(aud.get("ok"))
    stats["audit_n_fail"] = aud.get("n_fail") or 0
    stats["audit_fails"] = list(aud.get("fails") or [])[:8]
    stats["size"] = book.get("size")
    stats["sell"] = book.get("sell")
    stats["s_boost"] = book.get("s_boost")
    if not aud.get("ok"):
        reliable = False
    peak = None
    dd = 0.0
    for x in book.get("equity") or []:
        peak = x if peak is None else max(peak, x)
        if peak and peak > 0:
            dd = max(dd, (peak - x) / peak)
    stats["max_dd_pct"] = round(100.0 * dd, 2)
    if book.get("avg_win_pct") is not None:
        stats["avg_win_pct"] = book["avg_win_pct"]
    if book.get("avg_loss_pct") is not None:
        stats["avg_loss_pct"] = book["avg_loss_pct"]
    if stats.get("avg_win_pct") and stats.get("avg_loss_pct"):
        stats["payoff"] = round(
            abs(float(stats["avg_win_pct"]) / float(stats["avg_loss_pct"])), 3)
    stats["reliable"] = reliable
    stats["effectiveness"] = fm._effectiveness(
        stats.get("win_rate"), stats.get("profitable_day_rate"),
        stats.get("start_rate"), stats.get("gainer_rate"),
        stats.get("loser_rate"), stats.get("payoff"),
        stats.get("total_ret_pct"), median_start, pothole_pct, reliable,
    )
    return stats


def _gate_label(rec: dict) -> str:
    req = rec.get("require") or {}
    shown = {k: v for k, v in req.items() if k != "live_entry"}
    if not shown:
        return "none (list as ranked)"
    return ",".join(f"{k}={v}" for k, v in shown.items())


def _explain_md(rec: dict) -> list[str]:
    """Kid-plain inputs / buy / sell for the Action blotter."""
    ex = rec.get("explain") or fm.explain_recipe(rec)
    lines = [
        "## How this sleeve decides (like you are 10)",
        "",
        ex.get("kid") or "",
        "",
        "### What it looks at (inputs)",
        "",
    ]
    for x in ex.get("inputs") or []:
        lines.append(f"- {x}")
    lines += ["", "### When it buys", ""]
    for x in ex.get("buy") or []:
        lines.append(f"- {x}")
    lines += ["", "### When it sells", ""]
    for x in ex.get("sell") or []:
        lines.append(f"- {x}")
    lines += [""]
    return lines


def render_recipe_md(rec: dict, stats: dict, book: dict) -> str:
    live_gate = bool((rec.get("require") or {}).get("live_entry"))
    wish = rec.get("universe") == "flatten" and not live_gate
    entry_note = (
        "Buys the flatten **wish-list** even on io/HOLD mornings — live "
        "`flatten_robust` would not send 09:30 tickets those days. "
        "See `flatten_live_*` for the gated book."
        if wish else
        "New buys only when the live flatten gate fires (green S, ≥5 priced "
        "BUYs, prior book). io/HOLD mornings sit."
        if live_gate else
        "Research universe (not the live flatten gate). Cash/share/fee rules still apply."
    )
    lines = [
        f"# Factor mine action — `{rec['name']}`",
        "",
        f"_Book rules: $10k · whole shares · Futubull fees · leftover cash "
        f"split on new names · sell first · min-hold **{rec['hold']}** sessions · "
        f"fill 09:30 open · hard-red S≤{HARD_RED:g} sit · shorts marked as "
        f"liability (equity ≥ 2× notional). "
        f"Live `flatten_robust` is not changed._",
        "",
        entry_note,
        "",
        f"Side **{rec['side']}** · universe `{rec['universe']}` · "
        f"top {rec['top_n']} · rank `{rec.get('rank') or 'list'}` · "
        f"size `{rec.get('size') or book.get('size') or 'leftover'}` · "
        f"sell `{rec.get('sell') or book.get('sell') or 'list'}` · "
        f"S-boost `{rec.get('s_boost') or book.get('s_boost') or 'none'}` · "
        f"{rec.get('note') or ''}",
        "",
        f"Cash book **{stats.get('total_ret_pct'):+.2f}%** "
        f"(${stats.get('final_equity'):,.0f}) · "
        f"signal-only (no cash/fees) was "
        f"{stats.get('signal_ret_pct'):+.2f}%. "
        f"Starts YES **{stats.get('start_green')}/{stats.get('start_n')}**. "
        f"Fills {book.get('n_trades')} · skips {book.get('n_skips')} · "
        f"realized ${book.get('realized'):+.2f}.",
        "",
    ]
    lines += _explain_md(rec)
    lines += [
        "## Why these stocks",
        "",
        "Same shape as [FLATTEN_LOOKBACK_ACTION.md](../FLATTEN_LOOKBACK_ACTION.md): "
        "the 09:30 packet + leftover cash + lots on hand decide the ticket. "
        "Same-day Change% is outcome only.",
        "",
        f"- **Universe** `{rec['universe']}` — candidate list at 09:30 "
        f"(flatten wish-list, union, probable, yday gainer, or OHLC hot).",
        f"- **Gate** `{_gate_label(rec)}` · **rank** `{rec.get('rank') or 'list order'}` "
        f"· **top_n** {rec['top_n']}"
        + (f" (S≥+5 may raise this when S-boost is `{rec.get('s_boost')}`)"
           if (rec.get('s_boost') or 'none') != 'none' else "")
        + ".",
        f"- **Size** `{rec.get('size') or 'leftover'}` splits leftover cash among "
        f"*new* names only. Rank-weight / top-heavy still cannot invent money.",
        f"- **Sell** `{rec.get('sell') or 'list'}` after min-hold **{rec['hold']}**. "
        f"We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 "
        f"can still exit inside the floor.",
        f"- **Entry:** {entry_note}",
        "",
    ]
    aud = book.get("audit") or {}
    if aud.get("ok"):
        lines += [
            "## State audit",
            "",
            f"**PASS** · 0 violations. Independent replay of fills never sold "
            f"an unheld lot and never spent past leftover cash. Close cash "
            f"${aud.get('final_cash', book.get('cash')):,.2f}.",
            "",
        ]
    else:
        fails = aud.get("fails") or ["audit missing"]
        lines += [
            "## State audit",
            "",
            f"**FAIL** · {aud.get('n_fail', len(fails))} violations.",
            "",
        ]
        for f in fails[:8]:
            lines.append(f"- {f}")
        lines.append("")
    if (book.get("marks_audit") or {}).get("ok"):
        lines += [
            "Per-name 09:30 / close marks **PASS** — overnight $ sums to "
            "09:30 equity vs prior close, and on no-fill days intraday $ "
            "sums to close equity vs 09:30. No session is skipped.",
            "",
        ]
    lines += _render_marks_md(book.get("daily") or [])
    lines += [
        "## Each session (cash + holdings state)",
        "",
        "| Date | S | 09:30 cash | 09:30 held | 09:30 equity | Overnight $ | Intraday $ | Bought | Sold | Close cash | Close equity | Close held |",
        "|---|---:|---:|---|---:|---:|---:|---|---|---:|---:|---|",
    ]
    for d in book.get("daily") or []:
        s = d.get("s")
        lots = d.get("lots") or []
        close_held = ", ".join(f"{p['ticker']}×{p['shares']}" for p in lots) or "—"
        ov = d.get("overnight_delta")
        ov_s = "—" if ov is None else f"{ov:+,.2f}"
        sess = d.get("session_delta")
        sess_s = "—" if sess is None else f"{sess:+,.2f}"
        oeq = d.get("open_equity")
        lines.append(
            f"| {d['date']} | {('—' if s is None else f'{s:+.2f}')} | "
            f"${(d.get('open_cash') if d.get('open_cash') is not None else 0):,.2f} | "
            f"{', '.join(d.get('open_held') or []) or '—'} | "
            f"{'—' if oeq is None else f'${oeq:,.2f}'} | {ov_s} | {sess_s} | "
            f"{', '.join(d.get('bought') or []) or '—'} | "
            f"{', '.join(d.get('sold') or []) or '—'} | "
            f"${d['cash']:,.2f} | ${d['equity']:,.2f} | "
            f"{close_held} |"
        )
    lines += [
        "",
        "## Fills (09:30 open snapshot, then buys / sells, then 16:00 close)",
        "",
        "| Date | Side | Ticker | Shares | Px | Fees | P/L | Cash after | Equity change (sells only) | Why | Cameras |",
        "|---|---|---|---:|---:|---:|---:|---:|---:|---|---|",
    ]
    for t in book.get("trades") or []:
        kind = t.get("side")
        pnl = t.get("pnl")
        if kind == "OPEN":
            ov = t.get("equity_delta")
            mark = "▲" if (ov or 0) >= 0 else "▼"
            eq_s = (
                f"{mark} 09:30 equity ${t.get('equity_after'):,.2f} vs yday "
                f"${t.get('yday_equity'):,.2f} ({ov:+,.2f})"
                if t.get("equity_after") is not None and ov is not None
                else "—"
            )
            px_s = "—"
            fee_s = "—"
            share_s = "—"
            tick = "09:30 open"
        elif kind == "CLOSE":
            sess = t.get("session_delta")
            mark = "▲" if (sess or 0) >= 0 else "▼"
            eq_s = (
                f"{mark} close ${t.get('equity_after'):,.2f} vs 09:30 "
                f"${t.get('open_equity'):,.2f} (session {sess:+,.2f})"
                if t.get("equity_after") is not None and sess is not None
                else "—"
            )
            px_s = "—"
            fee_s = "—"
            share_s = "—"
            tick = "16:00 close"
        elif kind in ("SELL", "COVER"):
            chg = t.get("pnl")
            if chg is None:
                eq_s = "—"
            else:
                mark = "▲" if chg > 0 else "▼"
                fee_vs_mark = t.get("sell_eq_chg")
                extra = (
                    f"; vs 09:30 mark {fee_vs_mark:+,.2f}"
                    if fee_vs_mark is not None else ""
                )
                eq_s = (
                    f"{mark} {chg:+,.2f} after sell → book "
                    f"${t.get('equity_after'):,.2f}{extra}"
                )
            px_s = f"${t['price']:.2f}"
            fee_s = f"${t['fees']:.2f}"
            share_s = str(t["shares"])
            tick = f"`{t['ticker']}`"
        else:
            eq_s = "—"
            px_s = f"${t['price']:.2f}" if t.get("price") is not None else "—"
            fee_s = f"${t['fees']:.2f}" if t.get("fees") is not None else "—"
            share_s = str(t.get("shares") or "—")
            tick = f"`{t['ticker']}`"
        stamp = (
            f"{t['date']} 16:00 ET" if kind == "CLOSE"
            else f"{t['date']} 09:30 ET"
        )
        lines.append(
            f"| {stamp} | **{kind}** | {tick} | "
            f"{share_s} | {px_s} | {fee_s} | "
            f"{'—' if pnl is None else f'${pnl:+.2f}'} | "
            f"${t['cash_after']:,.2f} | {eq_s} | "
            f"{str(t.get('reason') or '—').replace('|', '/')} | "
            f"{t.get('cameras') or '—'} |"
        )
    skips = book.get("skips") or []
    if skips:
        lines += [
            "",
            "## Not taken",
            "",
            "| Date | Ticker | Kind | Why |",
            "|---|---|---|---|",
        ]
        for k in skips:
            lines.append(
                f"| {k['date']} | `{k['ticker']}` | {k['kind']} | "
                f"{str(k.get('reason') or '—').replace('|', '/')} |"
            )
    open_pos = book.get("open") or []
    if open_pos:
        lines += [
            "",
            "## Still open (marked at last close)",
            "",
            "| Ticker | Shares | Entry | Why |",
            "|---|---:|---|---|",
        ]
        for p in open_pos:
            lines.append(
                f"| `{p['ticker']}` | {p['shares']} | "
                f"{p['entry_date']} @ ${p['entry_px']:.2f} | "
                f"{str(p.get('reason') or '—').replace('|', '/')} |"
            )
    return "\n".join(lines) + "\n"


def _md_px(v) -> str:
    return "—" if v is None else f"${float(v):.2f}"


def _md_amt(v) -> str:
    return "—" if v is None else f"{float(v):+.2f}"


def _render_marks_md(daily: list) -> list[str]:
    lines = [
        "## Every lot, every session (09:30 mark and same-day change)",
        "",
        "Cash does not change overnight and no fees print until a fill. "
        "While a lot stays on the book, the 09:30 open vs the prior close is "
        "an unrealized overnight move; the close vs that 09:30 open is the "
        "same-day unrealized move. Sum of overnight $ = 09:30 equity − prior "
        "close equity. On a no-fill day, sum of intraday $ = close equity − "
        "09:30 equity. Bought-today names have overnight $ = 0 (they were "
        "not held at the prior close). Sold-at-open names have intraday $ = 0.",
        "",
        "| Date | Ticker | Shares | Prior close | 09:30 open | Overnight $ | Close | Intraday $ | Day $ | vs entry @ open | vs entry @ close |",
        "|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for d in daily:
        marks = d.get("marks") or []
        if not marks:
            lines.append(
                f"| {d['date']} | — | — | — | — | +0.00 | — | +0.00 | "
                f"+0.00 | — | — |"
            )
            continue
        for m in marks:
            sh = m.get("shares_close") or m.get("shares_open") or m.get("shares")
            lines.append(
                f"| {d['date']} | `{m['ticker']}` | {sh} | "
                f"{_md_px(m.get('yday_px'))} | {_md_px(m.get('open_px'))} | "
                f"{float(m.get('overnight') or 0):+.2f} | "
                f"{_md_px(m.get('close_px'))} | "
                f"{float(m.get('session') or 0):+.2f} | "
                f"{float(m.get('day') or 0):+.2f} | "
                f"{_md_amt(m.get('vs_entry_open'))} | "
                f"{_md_amt(m.get('vs_entry_close'))} |"
            )
    lines.append("")
    return lines


def write_action_mds(payload: dict, stats: list[dict], books: dict,
                     featured: list[str]) -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    recs = {r["name"]: r for r in (payload.get("recipes") or [])}
    by_stats = {s["name"]: s for s in stats}

    def rec_for(name: str, s: dict) -> dict:
        have = recs.get(name)
        if have:
            return have
        return {
            "name": name, "hold": s.get("hold"), "side": s.get("side"),
            "universe": s.get("universe"), "top_n": s.get("top_n"),
            "rank": s.get("rank"), "note": s.get("note"),
            "require": s.get("require") or {},
            "size": s.get("size") or "leftover",
            "sell": s.get("sell") or "list",
            "s_boost": s.get("s_boost") or "none",
        }

    for name, b in books.items():
        s = by_stats.get(name)
        if not s:
            continue
        (OUT_DIR / f"{name}.md").write_text(
            render_recipe_md(rec_for(name, s), s, b), encoding="utf-8")

    index = [
        f"# Factor mine action — {payload.get('from_date')} → {payload.get('to_date')}",
        "",
        "Cash-accounted blotters for the leak-free 09:30 recipes. "
        "Each recipe is a **daily cash + holdings state machine**: "
        "morning leftover cash and the lots we actually hold are the only "
        "inputs to that session's buys/sells. We can only sell shares on "
        "hand and only spend leftover cash (whole shares, Futubull fees). "
        "An independent fill-replay **audit** flags any violation.",
        "",
        "## Rule check (read this)",
        "",
        "- **Butterfly state:** day N open cash/held = day N−1 close after fills. "
        "A miss on 8-13 leftover changes every later ticket.",
        "- **Per-name marks:** every session (including no-fill days) lists "
        "each held ticker's prior close, 09:30 open, overnight $, close, and "
        "intraday $. That is the only reason equity moves when we do not trade.",
        "- **Cash / shares / fees:** leftover split (or rank-weight / top-heavy / half) "
        "among *new* names. Skip if the split cannot buy 1 share.",
        "- **Sell:** list-drop after min-hold, or time-stop / cut-loser / trail. "
        "Never sell a ticker we do not hold.",
        "- **S-boost:** on mornings with general S ≥ +5, optional sizeup (1.35×) "
        "and/or +4 names — still capped by leftover cash. Hard-red S ≤ −3 sits.",
        "- **Flatten wish-list ≠ live tickets.** `flatten_h*` buys the wish-list "
        "on io/HOLD mornings. `flatten_live_*` is the gated book.",
        "",
        f"Phone: `dashboard/factor-mine/index.html`. "
        f"Sister: [flatten lookback](../dashboard/flatten-lookback/) · "
        f"[sleeve merge](../dashboard/sleeve-merge/) · "
        f"[strategy board](../dashboard/strategy-board/).",
        "",
        "Live `flatten_robust` is not changed.",
        "",
        "## Featured books",
        "",
        "| Strategy | Size | Sell | Boost | Book % | Signal-only % | Starts YES | Fills | Skips | Audit | MD |",
        "|---|---|---|---|---:|---:|---:|---:|---:|---|---|",
    ]
    for name in featured:
        s = by_stats.get(name)
        b = books.get(name) or {}
        if not s:
            continue
        md_name = f"{name}.md"
        aud = "PASS" if s.get("audit_ok", True) else f"FAIL×{s.get('audit_n_fail') or '?'}"
        index.append(
            f"| `{name}` | {s.get('size') or 'leftover'} | "
            f"{s.get('sell') or 'list'} | {s.get('s_boost') or 'none'} | "
            f"{s.get('total_ret_pct'):+.2f} | "
            f"{s.get('signal_ret_pct'):+.2f} | "
            f"{s.get('start_green')}/{s.get('start_n')} | "
            f"{b.get('n_trades') or 0} | {b.get('n_skips') or 0} | "
            f"{aud} | [{md_name}](factor_mine/{md_name}) |"
        )
    others = [n for n in books if n not in set(featured)]
    if others:
        index += [
            "",
            "## All other blotters",
            "",
        ]
        for name in others:
            index.append(f"- [`{name}`](factor_mine/{name}.md)")
    OUT_INDEX.write_text("\n".join(index) + "\n", encoding="utf-8")
    DAILY_MD.write_text(
        OUT_INDEX.read_text(encoding="utf-8"), encoding="utf-8")
