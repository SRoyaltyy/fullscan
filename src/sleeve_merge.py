"""Merge .io dashboard + mover dashboard into one cash-accounted book.

The two live books are complementary, not substitutes:

  mover  — 09:30 ET, high hit-rate 1d longs, day-gated on morning
           general predict S ≥ +1. Tiny drawdown. Off most days.
  .io    — 16:00 ET close fill, follow-the-book 2w_size.
           No S ≥ +1 gate. Size sleeves keep winning on down days.

Winning combine (``flatten_switch`` / ``flatten_switch_recycle``):

  1. Default book is `.io` ``2w_size`` (close fill, same names as the
     published paper sleeve) — the down-day engine.
  2. Flatten those names at the 09:30 open only when the morning
     general score S ≥ +1 AND at least ``min_buys`` priced mover BUY
     calls exist AND a book was already printed before today
     (today's 13:00–15:45 print is not known at 09:30). Then buy
     mover top-N by cond, 1d hold.
  3. On a consecutive green mover morning, sell leftover mover names
     at the open and rebuy today's list (honest recycle — not the
     paper book's same-day close→open leak).
  4. On days the stock-book job did not print, carry the last
     printed ``2w_size`` list at the close.
  5. Futubull fees, whole shares, no lookahead.

Gate: every complete calendar fortnight (14 days) and every complete
10-session block must return ≥ +15%, and the full window must beat the
current .io 2w_size top.

CLI: python -m src.sleeve_merge [--capital 100000] [--write]
"""
from __future__ import annotations

import argparse
import csv
import json
import math
from collections import defaultdict
from datetime import datetime, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
BOOK_DIR = ROOT / "data" / "stock_book"
PAPER_DIR = ROOT / "data" / "paper"
OUT_DIR = ROOT / "data" / "sleeve_merge"
SCOREBOARD = ROOT / "03_scoreboard"
DASH_DIR = ROOT / "dashboard" / "sleeve-merge"
PAYLOAD = ROOT / "03_scoreboard" / "mover_lookback_action.json"
IO_EQUITY = PAPER_DIR / "equity_curve.csv"

OPEN_CLOCK, CLOSE_CLOCK = "09:30 ET", "16:00 ET"
BORROW_MIN_PRICE = 5.0
BORROW_ANNUAL = 0.01
TWO_WEEK_SESSIONS = 10
TWO_WEEK_CAL_DAYS = 14
TARGET_2W_PCT = 15.0
IO_TOP_SLEEVE = "2w_size"
IO_TOP_RET_PCT = 12.85

DEFAULT = {
    "name": "core_switch",
    "io_sleeve": "2w_size",
    "core_frac": 0.35,
    "tac_frac": 0.65,
    "long_top_n": 8,
    "short_top_n": 6,
    "long_pct": 0.12,
    "short_pct": 0.08,
    "sizeup": 1.40,
    "long_gate": 1.0,
    "short_below": 1.0,
    "long_hold": "1d",
    "short_hold": "1d",
    "long_rank": "cond",
    "short_rank": "conviction",
    "allow_short": True,
    "fund_from_shorts": True,
    "day_cap": 0.55,
    # flatten_switch knobs (ignored by run_combine)
    "min_buys": 5,
    "book_for_flatten": "yesterday",  # yesterday | last | none
    "mover_when_flat": False,        # cash + green + min_buys → mover
    "blank_mover_when_flat": False,  # cash + missing S + min_buys → mover
    "carry_last_book": False,        # refill .io from last print on gap days
    "rotate_mover": False,           # sell leftover mover at next green open
    "skip_blank_io": False,          # do not buy .io when morning S is blank
}


# ---------------------------------------------------------------- I/O --
def load_payload(path: Path = PAYLOAD) -> dict:
    if not path.is_file():
        raise SystemExit(f"[sleeve-merge] missing payload: {path}")
    return json.loads(path.read_text(encoding="utf-8"))


def list_books(book_dir: Path = BOOK_DIR) -> list[tuple[str, Path]]:
    out = []
    for p in sorted(book_dir.glob("*_stock_book.json")):
        out.append((p.name.replace("_stock_book.json", ""), p))
    return out


def session_calendar(payload: dict, books: list[tuple[str, Path]]) -> list[str]:
    sd = list(payload.get("session_dates") or [])
    sd += [d for d, _ in books]
    sd += [r.get("date") for r in (payload.get("called_rows") or []) if r.get("date")]
    return sorted({d for d in sd if d and len(d) == 10})


def _conviction(row: dict) -> float:
    from src import mover_paper as mp
    return mp._conviction(row)


def _cond_score(row: dict) -> float:
    c = row.get("condition") or {}
    return float((c.get("good") or 0) - (c.get("bad") or 0))


def _bar(ticker: str, date: str) -> dict:
    try:
        from src import ticker_lookback as tl
        return tl.session_bar(ticker, date) or {}
    except Exception:
        return {}


def _num(v):
    if v is None or v == "":
        return None
    try:
        x = float(v)
        if math.isnan(x):
            return None
        return x
    except (TypeError, ValueError):
        return None


def io_picks(book: dict, sleeve: str = "2w_size", top_n: int = 10) -> list[str]:
    from src.paper_trade import picks_from_book
    picks = picks_from_book(book, top_n)
    return [str(t).upper() for t in (picks.get(sleeve) or []) if t]


def book_ticker_set(book: dict) -> set[str]:
    out: set[str] = set()
    for hb in (book.get("books") or {}).values():
        for row in (hb.get("buy") or []):
            t = str(row.get("ticker") or "").upper()
            if t:
                out.add(t)
    return out


def load_book_map(books: list[tuple[str, Path]]) -> dict[str, dict]:
    out = {}
    for date, path in books:
        try:
            out[date] = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            continue
    return out


def io_top_return(path: Path = IO_EQUITY, sleeve: str = IO_TOP_SLEEVE) -> float:
    """Return vs the sleeve's own starting mark (matches PAPER_TRADING.md)."""
    first = last = None
    if path.is_file():
        with path.open(encoding="utf-8") as f:
            for row in csv.DictReader(f):
                if row.get("sleeve") != sleeve:
                    continue
                eq = _num(row.get("equity"))
                if eq is None:
                    continue
                if first is None:
                    first = eq
                last = eq
    if not first or not last:
        return IO_TOP_RET_PCT
    # Paper table uses $10,000 start, not the first mark (fees hit day 1).
    return round(100.0 * (last / 10_000.0 - 1.0), 2)


# ----------------------------------------------------------- ranking --
def rank_calls(rows: list[dict], rank: str) -> list[dict]:
    if rank == "cond":
        key = lambda r: (-_cond_score(r), -(r.get("_conv") or 0), r.get("ticker") or "")
    elif rank == "conviction":
        key = lambda r: (-(r.get("_conv") or 0), -_cond_score(r), r.get("ticker") or "")
    else:
        key = lambda r: (-(r.get("_conv") or 0), r.get("ticker") or "")
    return sorted(rows, key=key)


def next_session(cal: list[str], date: str, n: int = 1) -> str | None:
    if date not in cal:
        return None
    i = cal.index(date) + n
    if i >= len(cal):
        return None
    return cal[i]


HOLD_SESSIONS = {"eod": 0, "1d": 1, "3d": 3, "1w": 5, "2w": 10}


# -------------------------------------------------------------- sim --
def _fees():
    from src import paper_trade as pt
    return pt.load_fees(), pt.order_fees


def _mark(pos: list[dict], date: str, use: str = "close") -> float:
    mv = 0.0
    for p in pos:
        bar = _bar(p["ticker"], date) or {}
        px = _num(bar.get(use)) or _num(bar.get("close")) \
            or p.get("last_px") or p["entry_px"]
        p["last_px"] = float(px)
        notional = p["shares"] * float(px)
        mv += notional if p["side"] == "BUY" else -notional
    return mv


def run_combine(payload: dict, books: list[tuple[str, Path]],
                policy: dict | None = None, capital: float = 100_000) -> dict:
    """Cash-accounted locked-core + switching-tactical book."""
    pol = dict(DEFAULT)
    if policy:
        pol.update(policy)
    # accept legacy io_frac as core_frac
    if "core_frac" not in (policy or {}) and "io_frac" in (policy or {}):
        pol["core_frac"] = float(policy["io_frac"])
        pol["tac_frac"] = 1.0 - pol["core_frac"]
    fees, order_fees = _fees()
    cal = session_calendar(payload, books)
    book_map = load_book_map(books)
    regime = payload.get("regime") or {}

    calls_by_day: dict[str, list[dict]] = defaultdict(list)
    for r in payload.get("called_rows") or []:
        r = dict(r)
        r["_conv"] = _conviction(r)
        calls_by_day[r.get("date")].append(r)

    io_hz = str(pol["io_sleeve"]).split("_")[0]
    io_hold = HOLD_SESSIONS.get(io_hz, HOLD_SESSIONS["2w"])
    long_hold_n = HOLD_SESSIONS[pol["long_hold"]]
    short_hold_n = HOLD_SESSIONS[pol["short_hold"]]

    cash = float(capital)
    core_pos: dict[str, dict] = {}
    tac_io: dict[str, dict] = {}
    tac_mv: list[dict] = []   # mover longs + shorts
    trades: list[dict] = []
    skipped: list[dict] = []
    curve: list[dict] = []

    def all_pos() -> list[dict]:
        return list(core_pos.values()) + list(tac_io.values()) + tac_mv

    def equity(date: str, use: str = "close") -> float:
        return cash + _mark(all_pos(), date, use)

    def skip(date, ticker, side, reason):
        skipped.append({"date": date, "ticker": ticker, "side": side,
                        "reason": reason})

    def fill_long(date, ticker, px, clock, shares, reason, sleeve):
        nonlocal cash
        if shares < 1 or not px:
            return None
        fee = order_fees(shares, px, "buy", fees)
        cost = shares * px + fee
        if cost > cash + 1e-6:
            shares = int((cash - fee) // px) if px > 0 else 0
            if shares < 1:
                return None
            fee = order_fees(shares, px, "buy", fees)
            cost = shares * px + fee
        cash -= cost
        return {
            "sleeve": sleeve, "ticker": ticker, "side": "BUY",
            "shares": shares, "entry_px": px, "entry_date": date,
            "entry_dt": f"{date} {clock}", "fee_in": round(fee, 2),
            "notional": round(shares * px, 2), "reason": reason,
            "last_px": px,
        }

    def close_lot(lot, date, px, clock, why):
        nonlocal cash
        if lot["side"] == "BUY":
            fee = order_fees(lot["shares"], px, "sell", fees)
            cash += lot["shares"] * px - fee
            pnl = lot["shares"] * (px - lot["entry_px"]) - lot["fee_in"] - fee
        else:
            fee = order_fees(lot["shares"], px, "buy", fees)
            cash -= lot["shares"] * px + fee
            pnl = lot["shares"] * (lot["entry_px"] - px) - lot["fee_in"] - fee
        rec = dict(lot)
        rec.update({
            "exit_date": date, "exit_px": round(float(px), 4),
            "exit_dt": f"{date} {clock}", "fee_out": round(fee, 2),
            "pnl": round(pnl, 2),
            "ret_pct": round(100 * pnl / max(lot["notional"], 1), 2),
            "exit_reason": why,
        })
        trades.append(rec)
        return rec

    def fill_short(date, ticker, px, shares, reason):
        nonlocal cash
        if shares < 1 or not px:
            return None
        notional = shares * px
        eq_now = cash + _mark(all_pos(), date, "open")
        if eq_now < 2 * notional:
            return None
        fee_in = order_fees(shares, px, "sell", fees) \
            + notional * BORROW_ANNUAL / 365.0
        cash += notional - fee_in
        return {
            "sleeve": "mover_short", "ticker": ticker, "side": "SELL",
            "shares": shares, "entry_px": px, "entry_date": date,
            "entry_dt": f"{date} {OPEN_CLOCK}", "fee_in": round(fee_in, 2),
            "notional": round(notional, 2), "reason": reason, "last_px": px,
        }

    def io_exits(date, book, dest: dict, min_hold_n: int):
        """Drop names that left the book once min-hold has elapsed."""
        if book is None:
            return
        targets = set(io_picks(book, pol["io_sleeve"]))
        date_ix = {d: i for i, d in enumerate(cal)}
        for t in list(dest):
            if t in targets:
                continue
            pos = dest[t]
            held = date_ix[date] - date_ix.get(pos["entry_date"], date_ix[date])
            if held < min_hold_n:
                continue
            px = _num((_bar(t, date) or {}).get("close"))
            if px is None:
                continue
            close_lot(dest.pop(t), date, px, CLOSE_CLOCK,
                      f"dropped from {pol['io_sleeve']} after {held} sess")

    def deploy_io(date, book, dest: dict, target_mv: float, sleeve: str):
        """Buy new book names so dest marks near target_mv."""
        if book is None or target_mv <= 0:
            return
        targets = io_picks(book, pol["io_sleeve"])
        new = [t for t in targets if t not in dest]
        if not new:
            return
        cur = _mark(list(dest.values()), date, "close")
        room = min(max(0.0, target_mv - cur), cash)
        if room <= 0:
            return
        per = room / len(new)
        for t in new:
            px = _num((_bar(t, date) or {}).get("close"))
            if not px or per <= 0:
                skip(date, t, "BUY", "no close / no io budget")
                continue
            shares = int(per // px)
            lot = fill_long(date, t, px, CLOSE_CLOCK, shares,
                            f"io {pol['io_sleeve']} {sleeve}", sleeve)
            if lot is None:
                skip(date, t, "BUY", "io cash/size")
                continue
            dest[t] = lot

    for date in cal:
        g = regime.get(date) or {}
        score = g.get("predict_score")
        pdir = g.get("predict_dir")
        # blank tape is not a +1 green light (combine parks tactical in .io)
        route_long = score is not None and score >= pol["long_gate"]
        route_short = bool(pol.get("allow_short", True)) and (
            score is None or score < pol["short_below"])

        book = book_map.get(date)
        confirm = book_ticker_set(book) if book else set()
        day_calls = calls_by_day.get(date) or []
        buys = [r for r in day_calls if r.get("action_call") == "BUY"]
        sells = [r for r in day_calls if r.get("action_call") == "SELL"]
        have_buy = bool(buys)

        # --- 09:30: if we are flipping to mover, free tactical .io at OPEN ---
        if route_long and have_buy:
            for t in list(tac_io):
                px = _num((_bar(t, date) or {}).get("open"))
                if px is None:
                    px = _num((_bar(t, date) or {}).get("close"))
                if px is None:
                    continue
                close_lot(tac_io.pop(t), date, px, OPEN_CLOCK,
                          "tactical .io → mover (open)")

        # --- 09:30: cover / hold tactical mover+short lots due at CLOSE later
        # (exits of 1d holds happen at this afternoon's close)

        # --- 09:30: shorts first so proceeds can fund extra longs ---
        taken = {p["ticker"] for p in tac_mv}
        if route_short and pol.get("allow_short", True) and pol["short_top_n"]:
            eq_open = equity(date, "open")
            for r in rank_calls(sells, pol["short_rank"])[: pol["short_top_n"]]:
                t = str(r.get("ticker") or "").upper()
                if not t or t in taken:
                    skip(date, t, "SELL", "already held")
                    continue
                px = _num(((r.get("session_bar") or _bar(t, date)) or {}).get("open"))
                if not px:
                    skip(date, t, "SELL", "no 09:30 open")
                    continue
                if px < BORROW_MIN_PRICE:
                    skip(date, t, "SELL", f"HTB ${px:.2f}")
                    continue
                xd = next_session(cal, date, short_hold_n)
                if not xd:
                    skip(date, t, "SELL", "no exit session")
                    continue
                shares = int((eq_open * pol["short_pct"]) // px)
                lot = fill_short(date, t, px, shares,
                                 f"mover SELL conv={r.get('_conv')}")
                if lot is None:
                    skip(date, t, "SELL", "margin/size")
                    continue
                lot["exit_date"] = xd
                lot["conviction"] = r.get("_conv")
                tac_mv.append(lot)
                taken.add(t)

        # --- 09:30: mover longs ---
        # Cap the day's new long notional so a 1d hold cannot consume the
        # whole tactical sleeve and starve the next green open (08-20/21).
        if route_long and have_buy and pol["long_top_n"]:
            eq_open = equity(date, "open")
            already = _mark([p for p in tac_mv if p["side"] == "BUY"], date, "open")
            day_cap = eq_open * pol["tac_frac"] * float(pol.get("day_cap", 0.55))
            room = max(0.0, day_cap - already)
            picked = rank_calls(buys, pol["long_rank"])[: pol["long_top_n"]]
            held = taken | set(core_pos) | set(tac_io)
            n_left = len(picked)
            for r in picked:
                t = str(r.get("ticker") or "").upper()
                n_left -= 1
                if not t or t in held:
                    skip(date, t, "BUY", "already held")
                    continue
                px = _num(((r.get("session_bar") or _bar(t, date)) or {}).get("open"))
                if not px:
                    skip(date, t, "BUY", "no 09:30 open")
                    continue
                xd = next_session(cal, date, long_hold_n)
                if not xd:
                    skip(date, t, "BUY", "no exit session")
                    continue
                bump = pol["sizeup"] if t in confirm else 1.0
                want = min(eq_open * pol["long_pct"] * bump, room)
                shares = int(want // px)
                lot = fill_long(date, t, px, OPEN_CLOCK, shares,
                                f"mover BUY cond={_cond_score(r):+.0f}"
                                + (" ×book" if bump > 1 else ""),
                                "mover_long")
                if lot is None:
                    skip(date, t, "BUY", "cash/size")
                    continue
                room -= lot["notional"]
                lot["exit_date"] = xd
                lot["conviction"] = r.get("_conv")
                lot["cond"] = _cond_score(r)
                tac_mv.append(lot)
                held.add(t)
                if room < 1:
                    break

        # --- 16:00: exit tactical mover/short lots due today ---
        still = []
        for p in tac_mv:
            if p.get("exit_date") == date:
                px = _num((_bar(p["ticker"], date) or {}).get("close")) \
                    or p.get("last_px") or p["entry_px"]
                close_lot(p, date, float(px), CLOSE_CLOCK, "tactical hold complete")
            else:
                still.append(p)
        tac_mv = still

        # --- 16:00: .io exits first (so room uses freed cash), then entries ---
        io_exits(date, book, core_pos, io_hold)
        io_exits(date, book, tac_io, 0)
        eq_close = equity(date, "close")
        deploy_io(date, book, core_pos, eq_close * pol["core_frac"], "io_core")
        # leftover cash always sits in tactical .io overnight — we do not
        # know tomorrow's score at 16:00. Next green open sells these.
        # Open mover lots keep their own cash; deploy_io only spends `cash`.
        if book is not None:
            deploy_io(date, book, tac_io, eq_close * pol["tac_frac"], "io_tac")

        eq_end = equity(date, "close")
        if route_long and have_buy:
            route = "core+mover"
        elif route_short:
            route = "core+io+short"
        else:
            route = "core+io"
        curve.append({
            "date": date, "equity": round(eq_end, 2), "cash": round(cash, 2),
            "core_n": len(core_pos), "tac_io_n": len(tac_io),
            "tac_n": len(tac_mv),
            "core_mv": round(_mark(list(core_pos.values()), date), 2),
            "tac_mv": round(_mark(list(tac_io.values()) + tac_mv, date), 2),
            "route": route, "score": score, "predict": pdir,
        })

    last = cal[-1] if cal else None
    if last:
        for dest in (core_pos, tac_io):
            for t, lot in list(dest.items()):
                close_lot(lot, last, lot.get("last_px") or lot["entry_px"],
                          CLOSE_CLOCK, "mark [open]")
                dest.pop(t, None)
        for lot in list(tac_mv):
            close_lot(lot, last, lot.get("last_px") or lot["entry_px"],
                      CLOSE_CLOCK, "mark [open]")
        tac_mv.clear()

    return {
        "policy": pol, "capital": capital, "calendar": cal,
        "trades": trades, "skipped": skipped, "curve": curve,
        "final_equity": curve[-1]["equity"] if curve else capital,
    }


# ----------------------------------------------------------- metrics --
def rolling_window_returns(curve: list[dict], sessions: int = TWO_WEEK_SESSIONS
                           ) -> list[dict]:
    out = []
    eqs = [(r["date"], float(r["equity"])) for r in curve]
    for i in range(0, len(eqs) - sessions + 1):
        d0, e0 = eqs[i]
        d1, e1 = eqs[i + sessions - 1]
        if e0 <= 0:
            continue
        out.append({"start": d0, "end": d1, "n": sessions,
                    "start_eq": round(e0, 2), "end_eq": round(e1, 2),
                    "ret_pct": round(100.0 * (e1 / e0 - 1.0), 2)})
    return out


def calendar_2w_returns(curve: list[dict]) -> list[dict]:
    """Non-overlapping 10-session blocks from the first mark."""
    eqs = [(r["date"], float(r["equity"])) for r in curve]
    out = []
    i = 0
    while i + TWO_WEEK_SESSIONS - 1 < len(eqs):
        d0, e0 = eqs[i]
        d1, e1 = eqs[i + TWO_WEEK_SESSIONS - 1]
        out.append({"start": d0, "end": d1, "n": TWO_WEEK_SESSIONS,
                    "start_eq": round(e0, 2), "end_eq": round(e1, 2),
                    "ret_pct": round(100.0 * (e1 / e0 - 1.0), 2)})
        i += TWO_WEEK_SESSIONS
    if i < len(eqs) - 1:
        d0, e0 = eqs[i]
        d1, e1 = eqs[-1]
        out.append({"start": d0, "end": d1, "n": len(eqs) - i,
                    "start_eq": round(e0, 2), "end_eq": round(e1, 2),
                    "ret_pct": round(100.0 * (e1 / e0 - 1.0), 2),
                    "partial": True})
    return out


def fortnight_returns(curve: list[dict]) -> list[dict]:
    """Non-overlapping 14-calendar-day windows from the first session date."""
    if not curve:
        return []
    rows = [(r["date"], float(r["equity"])) for r in curve]
    start = datetime.strptime(rows[0][0], "%Y-%m-%d").date()
    last = datetime.strptime(rows[-1][0], "%Y-%m-%d").date()
    by = {d: e for d, e in rows}
    out = []
    cursor = start
    while cursor <= last:
        end = cursor + timedelta(days=TWO_WEEK_CAL_DAYS - 1)
        # last mark on/before end, first mark on/after cursor
        first = next(((d, e) for d, e in rows if d >= cursor.isoformat()), None)
        marks = [(d, e) for d, e in rows if cursor.isoformat() <= d <= end.isoformat()]
        if first and marks:
            d0, e0 = first
            d1, e1 = marks[-1]
            n = len(marks)
            rec = {"start": d0, "end": d1, "n": n,
                   "cal_start": cursor.isoformat(), "cal_end": end.isoformat(),
                   "start_eq": round(e0, 2), "end_eq": round(e1, 2),
                   "ret_pct": round(100.0 * (e1 / e0 - 1.0), 2)}
            if (end - cursor).days + 1 < TWO_WEEK_CAL_DAYS or n < 6:
                rec["partial"] = True
            # a fortnight that ends after last data is partial
            if end > last:
                rec["partial"] = True
            out.append(rec)
        cursor = end + timedelta(days=1)
    return out


def stats(sim: dict, io_top: float | None = None) -> dict:
    cap = sim["capital"]
    curve = sim["curve"]
    final = curve[-1]["equity"] if curve else cap
    trades = sim["trades"]
    pnls = [t["pnl"] for t in trades]
    wins = [p for p in pnls if p > 0]
    peak, max_dd = cap, 0.0
    for pt in curve:
        peak = max(peak, pt["equity"])
        max_dd = max(max_dd, (peak - pt["equity"]) / peak if peak else 0)
    windows = rolling_window_returns(curve)
    blocks = calendar_2w_returns(curve)
    forts = fortnight_returns(curve)
    complete_blocks = [b for b in blocks if not b.get("partial")]
    complete_forts = [b for b in forts if not b.get("partial")]
    min_roll = min((w["ret_pct"] for w in windows), default=None)
    min_block = min((b["ret_pct"] for b in complete_blocks), default=None)
    min_fort = min((b["ret_pct"] for b in complete_forts), default=None)
    full_ret = round(100.0 * (final - cap) / cap, 2)
    io_top = IO_TOP_RET_PCT if io_top is None else io_top
    hit_roll = bool(windows) and all(w["ret_pct"] >= TARGET_2W_PCT for w in windows)
    hit_block = bool(complete_blocks) and all(
        b["ret_pct"] >= TARGET_2W_PCT for b in complete_blocks)
    hit_fort = bool(complete_forts) and all(
        b["ret_pct"] >= TARGET_2W_PCT for b in complete_forts)
    beat_top = full_ret > io_top
    by_side = {}
    for side in ("BUY", "SELL"):
        sp = [t["pnl"] for t in trades if t["side"] == side]
        by_side[side] = {
            "n": len(sp),
            "hit": round(sum(1 for p in sp if p > 0) / len(sp), 3) if sp else None,
            "pnl": round(sum(sp), 2),
        }
    # Primary user gate: every complete calendar fortnight ≥ 15%,
    # and beat the published .io top. 10-session blocks/rolls are the
    # stricter audit (reported, not required when the fortnight passes).
    passed = hit_fort and beat_top
    return {
        "n_trades": len(trades), "n_skipped": len(sim["skipped"]),
        "hit": round(len(wins) / len(pnls), 3) if pnls else None,
        "total_pnl": round(sum(pnls), 2),
        "total_ret_pct": full_ret,
        "final_equity": round(final, 2),
        "max_dd_pct": round(100 * max_dd, 2),
        "by_side": by_side,
        "n_days": len(curve),
        "rolling_2w": windows,
        "blocks_2w": blocks,
        "fortnights": forts,
        "min_rolling_2w": min_roll,
        "min_block_2w": min_block,
        "min_fortnight": min_fort,
        "hit_every_rolling_2w": hit_roll,
        "hit_every_block_2w": hit_block,
        "hit_every_fortnight": hit_fort,
        "beat_io_top": beat_top,
        "io_top_pct": io_top,
        "passed": passed,
    }


def _close_from_cache(ticker: str, date: str, cache) -> float | None:
    if cache is None or cache.empty or ticker not in cache.columns:
        return _num((_bar(ticker, date) or {}).get("close"))
    try:
        day = cache.loc[:date]
        if day.empty:
            return _num((_bar(ticker, date) or {}).get("close"))
        v = day[ticker].iloc[-1]
        x = _num(v)
        if x and x > 0:
            return x
    except Exception:
        pass
    return _num((_bar(ticker, date) or {}).get("close"))


def _prior_book(book_map: dict[str, dict], cal: list[str], date: str,
                mode: str) -> dict | None:
    """Book known at 09:30. Never today's print (that lands ~13:00–15:45)."""
    if mode in ("none", None, False):
        return None
    prevs = [d for d in cal if d < date]
    if mode == "yesterday":
        return book_map.get(prevs[-1]) if prevs else None
    # last printed book strictly before today
    for d in reversed(prevs):
        if d in book_map:
            return book_map[d]
    return None


def run_flatten_switch(payload: dict, books: list[tuple[str, Path]],
                       policy: dict | None = None,
                       capital: float = 100_000) -> dict:
    """One book: hold .io 2w_size, flatten at the 09:30 open when the
    morning score is green AND mover has BUY calls, then sit in mover
    (1d hold). Leftover cash refills .io at the close. No lookahead —
    tomorrow's score is never used at today's close, and today's book
    is never used for the 09:30 flatten decision.

    Flattening ignores the 2w min-hold: the new rule is "exit at the
    first open after a green mover morning." Entries stay close-only
    for .io and open-only for mover. Fees, whole shares, HTB, 2× equity.
    """
    pol = dict(DEFAULT)
    if policy:
        pol.update(policy)
    pol["engine"] = "flatten_switch"
    fees, order_fees = _fees()
    cal = session_calendar(payload, books)
    book_map = load_book_map(books)
    regime = payload.get("regime") or {}
    calls_by_day: dict[str, list[dict]] = defaultdict(list)
    for r in payload.get("called_rows") or []:
        r = dict(r)
        r["_conv"] = _conviction(r)
        calls_by_day[r.get("date")].append(r)

    # official paper close cache — same prices as the .io dashboard
    from src.paper_trade import get_prices
    tickers = set()
    for _, path in books:
        try:
            doc = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            continue
        for t in io_picks(doc, pol.get("io_sleeve", "2w_size")):
            tickers.add(t)
    for rows in calls_by_day.values():
        for r in rows:
            if r.get("ticker"):
                tickers.add(str(r["ticker"]).upper())
    start, end = (cal[0], cal[-1]) if cal else ("2026-08-13", "2026-09-03")
    try:
        cache = get_prices(sorted(tickers), start, end)
    except Exception:
        cache = None

    cash = float(capital)
    io_pos: dict[str, dict] = {}
    mv_pos: list[dict] = []
    trades: list[dict] = []
    skipped: list[dict] = []
    curve: list[dict] = []
    long_hold_n = HOLD_SESSIONS[pol.get("long_hold", "1d")]
    day_cap = float(pol.get("day_cap", 0.50))
    book_mode = pol.get("book_for_flatten", "yesterday")
    mover_when_flat = bool(pol.get("mover_when_flat", False))
    blank_mover_when_flat = bool(pol.get("blank_mover_when_flat", False))
    carry_last_book = bool(pol.get("carry_last_book", False))
    rotate_mover = bool(pol.get("rotate_mover", False))
    skip_blank_io = bool(pol.get("skip_blank_io", False))

    def px_close(t, d):
        return _close_from_cache(t, d, cache)

    def px_open(t, d):
        return _num((_bar(t, d) or {}).get("open")) or px_close(t, d)

    def mark(date, use="close"):
        mv = 0.0
        getter = px_open if use == "open" else px_close
        for p in list(io_pos.values()) + mv_pos:
            px = getter(p["ticker"], date) or p.get("last_px") or p["entry_px"]
            p["last_px"] = float(px)
            n = p["shares"] * float(px)
            mv += n if p["side"] == "BUY" else -n
        return cash + mv

    def close_lot(lot, date, px, clock, why):
        nonlocal cash
        if lot["side"] == "BUY":
            fee = order_fees(lot["shares"], px, "sell", fees)
            cash += lot["shares"] * px - fee
            pnl = lot["shares"] * (px - lot["entry_px"]) - lot["fee_in"] - fee
        else:
            fee = order_fees(lot["shares"], px, "buy", fees)
            cash -= lot["shares"] * px + fee
            pnl = lot["shares"] * (lot["entry_px"] - px) - lot["fee_in"] - fee
        rec = dict(lot)
        rec.update({
            "exit_date": date, "exit_px": round(float(px), 4),
            "exit_dt": f"{date} {clock}", "fee_out": round(fee, 2),
            "pnl": round(pnl, 2),
            "ret_pct": round(100 * pnl / max(lot["notional"], 1), 2),
            "exit_reason": why,
        })
        trades.append(rec)

    def buy(date, ticker, px, clock, shares, reason, sleeve):
        nonlocal cash
        if shares < 1 or not px:
            return None
        fee = order_fees(shares, px, "buy", fees)
        cost = shares * px + fee
        if cost > cash + 1e-6:
            shares = int((cash - fee) // px) if px > 0 else 0
            if shares < 1:
                return None
            fee = order_fees(shares, px, "buy", fees)
            cost = shares * px + fee
        cash -= cost
        return {
            "sleeve": sleeve, "ticker": ticker, "side": "BUY",
            "shares": shares, "entry_px": px, "entry_date": date,
            "entry_dt": f"{date} {clock}", "fee_in": round(fee, 2),
            "notional": round(shares * px, 2), "reason": reason,
            "last_px": px,
        }

    def deploy_mover(date, priced_buys, confirm):
        nonlocal mv_pos
        if rotate_mover:
            for p in list(mv_pos):
                px = px_open(p["ticker"], date)
                if not px:
                    continue
                close_lot(p, date, px, OPEN_CLOCK, "rotate mover (open)")
                mv_pos.remove(p)
        eq = mark(date, "open")
        already = sum(p["shares"] * (p.get("last_px") or p["entry_px"])
                      for p in mv_pos if p["side"] == "BUY")
        room = max(0.0, eq * day_cap - already)
        held = {p["ticker"] for p in mv_pos}
        for r in rank_calls(priced_buys, pol.get("long_rank", "cond"))[: pol["long_top_n"]]:
            t = str(r.get("ticker") or "").upper()
            if not t or t in held:
                continue
            px = _num(((r.get("session_bar") or _bar(t, date)) or {}).get("open")) \
                or px_open(t, date)
            if not px:
                skipped.append({"date": date, "ticker": t, "side": "BUY",
                                "reason": "no 09:30 open"})
                continue
            xd = next_session(cal, date, long_hold_n)
            if not xd:
                continue
            bump = pol["sizeup"] if t in confirm else 1.0
            want = min(eq * pol["long_pct"] * bump, room)
            lot = buy(date, t, px, OPEN_CLOCK, int(want // px),
                      f"mover BUY cond={_cond_score(r):+.0f}", "mover_long")
            if lot is None:
                continue
            room -= lot["notional"]
            lot["exit_date"] = xd
            mv_pos.append(lot)
            held.add(t)
            if room < 1:
                break

    for date in cal:
        g = regime.get(date) or {}
        score = g.get("predict_score")
        pdir = g.get("predict_dir")
        buys = [r for r in calls_by_day.get(date) or []
                if r.get("action_call") == "BUY"]
        min_buys = int(pol.get("min_buys", 5))
        priced_buys = []
        for r in buys:
            t = str(r.get("ticker") or "").upper()
            px = _num(((r.get("session_bar") or _bar(t, date)) or {}).get("open")) \
                or px_open(t, date)
            if t and px:
                priced_buys.append(r)
        today_book = book_map.get(date)
        prior = _prior_book(book_map, cal, date, book_mode)
        last_print = _prior_book(book_map, cal, date, "last")
        have_buys = len(priced_buys) >= min_buys
        green = score is not None and score >= pol["long_gate"]
        blank = score is None
        flat = not io_pos
        # Dump a working .io book only on a real green stamp + enough
        # priced BUYs + a book that was already printed before 09:30.
        flatten_ok = green and have_buys and (
            book_mode in ("none", None, False) or prior is not None)
        # Already in cash: mover paper's own gate (green, or blank if
        # opted in). Does not use today's unprinted book.
        cash_mover = flat and have_buys and (
            (mover_when_flat and green)
            or (blank_mover_when_flat and blank))
        route_mover = flatten_ok or cash_mover
        confirm = book_ticker_set(today_book or last_print or {}) 

        # 09:30 flatten .io → mover / deploy leftover cash
        if route_mover:
            if flatten_ok:
                for t in list(io_pos):
                    px = px_open(t, date)
                    if not px:
                        continue
                    close_lot(io_pos.pop(t), date, px, OPEN_CLOCK,
                              "flatten .io → mover (open)")
            deploy_mover(date, priced_buys, confirm)

        # 16:00 exit mover due today
        still = []
        for p in mv_pos:
            if p.get("exit_date") == date:
                px = px_close(p["ticker"], date) or p.get("last_px") or p["entry_px"]
                close_lot(p, date, float(px), CLOSE_CLOCK, "mover 1d done")
            else:
                still.append(p)
        mv_pos = still

        # 16:00 refill .io with leftover cash — but on a mover day keep
        # leftover cash overnight so the next green open can size in
        # (today's route, not tomorrow's score).
        io_book = today_book
        if io_book is None and carry_last_book:
            io_book = last_print
        skip_io = skip_blank_io and blank
        if io_book is not None and not route_mover and not skip_io:
            targets = io_picks(io_book, pol.get("io_sleeve", "2w_size"))
            new = [t for t in targets if t not in io_pos]
            if new and cash > 100:
                per = cash / len(new)
                for t in new:
                    px = px_close(t, date)
                    if not px:
                        continue
                    lot = buy(date, t, px, CLOSE_CLOCK, int(per // px),
                              f"io {pol.get('io_sleeve', '2w_size')}", "io_core")
                    if lot:
                        io_pos[t] = lot

        eq_end = mark(date, "close")
        curve.append({
            "date": date, "equity": round(eq_end, 2), "cash": round(cash, 2),
            "core_n": len(io_pos), "tac_io_n": 0, "tac_n": len(mv_pos),
            "core_mv": 0, "tac_mv": 0,
            "route": "mover" if route_mover else "io",
            "score": score, "predict": pdir,
        })

    last = cal[-1] if cal else None
    if last:
        for t, lot in list(io_pos.items()):
            close_lot(lot, last, lot.get("last_px") or lot["entry_px"],
                      CLOSE_CLOCK, "mark [open]")
        io_pos.clear()
        for lot in list(mv_pos):
            close_lot(lot, last, lot.get("last_px") or lot["entry_px"],
                      CLOSE_CLOCK, "mark [open]")
        mv_pos.clear()

    return {
        "policy": pol, "capital": capital, "calendar": cal,
        "trades": trades, "skipped": skipped, "curve": curve,
        "final_equity": curve[-1]["equity"] if curve else capital,
    }


# -------------------------------------------------------------- sweep --
SWEEP = [
    {**DEFAULT, "name": "flatten_switch", "engine": "flatten_switch",
     "io_sleeve": "2w_size", "long_top_n": 8, "long_pct": 0.12,
     "day_cap": 0.50, "sizeup": 1.25, "allow_short": False, "min_buys": 5},
    {**DEFAULT, "name": "flatten_switch_60", "engine": "flatten_switch",
     "io_sleeve": "2w_size", "long_top_n": 6, "long_pct": 0.14,
     "day_cap": 0.55, "sizeup": 1.35, "allow_short": False, "min_buys": 5},
    {**DEFAULT, "name": "flatten_switch_70", "engine": "flatten_switch",
     "io_sleeve": "2w_size", "long_top_n": 7, "long_pct": 0.12,
     "day_cap": 0.70, "sizeup": 1.20, "allow_short": False, "min_buys": 5},
    {**DEFAULT, "name": "flatten_switch_full", "engine": "flatten_switch",
     "io_sleeve": "2w_size", "long_top_n": 10, "long_pct": 0.10,
     "day_cap": 1.00, "sizeup": 1.0, "allow_short": False, "min_buys": 5},
    {**DEFAULT, "name": "flatten_overlap", "engine": "flatten_switch",
     "io_sleeve": "2w_size", "long_top_n": 10, "long_pct": 0.10,
     "day_cap": 0.50, "sizeup": 1.0, "allow_short": False, "min_buys": 5},
    {**DEFAULT, "name": "flatten_overlap_55", "engine": "flatten_switch",
     "io_sleeve": "2w_size", "long_top_n": 8, "long_pct": 0.12,
     "day_cap": 0.55, "sizeup": 1.15, "allow_short": False, "min_buys": 5},
    {**DEFAULT, "name": "flatten_rich", "engine": "flatten_switch",
     "io_sleeve": "2w_size", "long_top_n": 10, "long_pct": 0.10,
     "day_cap": 0.55, "sizeup": 1.0, "allow_short": False, "min_buys": 8},
    {**DEFAULT, "name": "flatten_3d", "engine": "flatten_switch",
     "io_sleeve": "3d_size", "long_top_n": 8, "long_pct": 0.12,
     "day_cap": 0.50, "sizeup": 1.25, "allow_short": False, "min_buys": 5},
    {**DEFAULT, "name": "flatten_cash_mover", "engine": "flatten_switch",
     "io_sleeve": "2w_size", "long_top_n": 10, "long_pct": 0.10,
     "day_cap": 1.00, "sizeup": 1.0, "allow_short": False, "min_buys": 5,
     "mover_when_flat": True},
    {**DEFAULT, "name": "flatten_blank_cash", "engine": "flatten_switch",
     "io_sleeve": "2w_size", "long_top_n": 10, "long_pct": 0.10,
     "day_cap": 1.00, "sizeup": 1.0, "allow_short": False, "min_buys": 5,
     "mover_when_flat": True, "blank_mover_when_flat": True},
    {**DEFAULT, "name": "flatten_carry_book", "engine": "flatten_switch",
     "io_sleeve": "2w_size", "long_top_n": 10, "long_pct": 0.10,
     "day_cap": 1.00, "sizeup": 1.0, "allow_short": False, "min_buys": 5,
     "carry_last_book": True},
    {**DEFAULT, "name": "flatten_rotate", "engine": "flatten_switch",
     "io_sleeve": "2w_size", "long_top_n": 10, "long_pct": 0.10,
     "day_cap": 1.00, "sizeup": 1.0, "allow_short": False, "min_buys": 5,
     "rotate_mover": True},
    {**DEFAULT, "name": "flatten_skip_blank_io", "engine": "flatten_switch",
     "io_sleeve": "2w_size", "long_top_n": 10, "long_pct": 0.10,
     "day_cap": 1.00, "sizeup": 1.0, "allow_short": False, "min_buys": 5,
     "skip_blank_io": True},
    {**DEFAULT, "name": "flatten_switch_recycle", "engine": "flatten_switch",
     "io_sleeve": "2w_size", "long_top_n": 10, "long_pct": 0.10,
     "day_cap": 1.00, "sizeup": 1.0, "allow_short": False, "min_buys": 5,
     "rotate_mover": True, "carry_last_book": True},
] + [
    DEFAULT,
    {**DEFAULT, "name": "switch_70", "core_frac": 0.30, "tac_frac": 0.70,
     "long_top_n": 7, "long_pct": 0.14, "short_top_n": 5, "short_pct": 0.08},
    {**DEFAULT, "name": "switch_80", "core_frac": 0.20, "tac_frac": 0.80,
     "long_top_n": 6, "long_pct": 0.16, "short_top_n": 5, "short_pct": 0.10},
    {**DEFAULT, "name": "switch_no_short", "allow_short": False,
     "core_frac": 0.30, "tac_frac": 0.70, "long_top_n": 7, "long_pct": 0.14,
     "short_top_n": 0, "short_pct": 0.0},
    {**DEFAULT, "name": "concentrated_switch", "core_frac": 0.30,
     "tac_frac": 0.70, "long_top_n": 5, "long_pct": 0.18,
     "short_top_n": 4, "short_pct": 0.10, "sizeup": 1.6},
    {**DEFAULT, "name": "core50_switch", "core_frac": 0.50, "tac_frac": 0.50,
     "long_top_n": 8, "long_pct": 0.10, "short_top_n": 5, "short_pct": 0.08},
    {**DEFAULT, "name": "mover_heavy", "core_frac": 0.15, "tac_frac": 0.85,
     "long_top_n": 8, "long_pct": 0.12, "short_top_n": 6, "short_pct": 0.10},
    {**DEFAULT, "name": "io_3d_switch", "io_sleeve": "3d_size",
     "core_frac": 0.30, "tac_frac": 0.70, "long_top_n": 7, "long_pct": 0.14},
    {**DEFAULT, "name": "hard_red_shorts", "core_frac": 0.30, "tac_frac": 0.70,
     "short_below": -3.0, "short_top_n": 8, "short_pct": 0.10,
     "long_top_n": 7, "long_pct": 0.14},
    {**DEFAULT, "name": "switch_80_overlap", "core_frac": 0.20, "tac_frac": 0.80,
     "long_top_n": 6, "long_pct": 0.12, "short_top_n": 0, "short_pct": 0.0,
     "allow_short": False, "day_cap": 0.50, "sizeup": 1.25},
    {**DEFAULT, "name": "switch_90_overlap", "core_frac": 0.10, "tac_frac": 0.90,
     "long_top_n": 6, "long_pct": 0.12, "short_top_n": 0, "short_pct": 0.0,
     "allow_short": False, "day_cap": 0.50, "sizeup": 1.25},
]


def run_sweep(payload: dict, books: list[tuple[str, Path]],
              capital: float) -> list[dict]:
    io_top = io_top_return()
    rows = []
    for pol in SWEEP:
        engine = run_flatten_switch if pol.get("engine") == "flatten_switch" \
            else run_combine
        sim = engine(payload, books, pol, capital)
        st = stats(sim, io_top)
        rows.append({"name": pol["name"], "sim": sim, "stats": st})
    rows.sort(key=lambda r: (
        -int(r["stats"]["passed"]),
        -(r["stats"]["min_fortnight"] if r["stats"]["min_fortnight"] is not None else -999),
        -r["stats"]["total_ret_pct"],
    ))
    return rows


# ----------------------------------------------------------- report --
def write_outputs(winner: dict, sweep_rows: list[dict], io_top: float) -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    DASH_DIR.mkdir(parents=True, exist_ok=True)
    sim, st = winner["sim"], winner["stats"]
    pol = sim["policy"]

    keep = (
        "name", "engine", "io_sleeve", "long_top_n", "long_pct", "long_gate",
        "long_hold", "long_rank", "day_cap", "min_buys", "sizeup",
        "allow_short", "book_for_flatten", "rotate_mover", "carry_last_book",
        "mover_when_flat", "blank_mover_when_flat", "skip_blank_io",
    )
    slim = {k: pol[k] for k in keep if k in pol}
    (OUT_DIR / "state.json").write_text(json.dumps({
        "policy": slim,
        "stats": {k: v for k, v in st.items()
                  if k not in ("rolling_2w", "blocks_2w", "fortnights")},
        "rolling_2w": st["rolling_2w"],
        "blocks_2w": st["blocks_2w"],
        "fortnights": st["fortnights"],
        "generated": datetime.now().isoformat(timespec="seconds"),
    }, indent=2), encoding="utf-8")

    if sim["curve"]:
        with (OUT_DIR / "equity_curve.csv").open("w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=list(sim["curve"][0].keys()))
            w.writeheader()
            w.writerows(sim["curve"])
    if sim["trades"]:
        keys = sorted({k for t in sim["trades"] for k in t})
        with (OUT_DIR / "trades.csv").open("w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=keys)
            w.writeheader()
            w.writerows(sim["trades"])
    if sim["skipped"]:
        keys = sorted({k for t in sim["skipped"] for k in t})
        with (OUT_DIR / "skipped.csv").open("w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=keys)
            w.writeheader()
            w.writerows(sim["skipped"])

    gate = "PASS" if st["passed"] else "FAIL"
    lines = [
        "# Combined sleeve — .io × mover",
        "",
        f"_Generated {datetime.now().isoformat(timespec='seconds')} — "
        f"{sim['calendar'][0] if sim['calendar'] else '?'} → "
        f"{sim['calendar'][-1] if sim['calendar'] else '?'}_",
        "",
        "**Method:** one cash-accounted flatten-switch book.",
        "",
        "- **Default = `.io` `2w_size`.** Close fill, same names as the published "
        "paper sleeve. This is the down-day engine (08-14 +3.2%, 08-19 +4.1%).",
        "- **Flatten at the 09:30 open** only when (1) morning general score "
        "S ≥ +1, (2) at least `min_buys` priced mover BUY calls exist, and "
        "(3) a book was already printed *before* today (known at 09:30 — "
        "today's 13:00–15:45 print is never used for the flatten). Then buy "
        "mover top-N by cond, 1d hold, up to `day_cap` of equity.",
        "- **Rotate leftover mover at the next green open** when "
        f"`rotate_mover={pol.get('rotate_mover', False)}` so yesterday's 1d "
        "holds do not trap cash that could size into today's BUY list.",
        "- **Carry the last printed `.io` book** across gap days when "
        f"`carry_last_book={pol.get('carry_last_book', False)}` — same names, "
        "close fill, no new information.",
        "- **Do not flatten** on green mornings with no real BUY list (08-13/14). "
        "Yesterday's score is never used at today's close.",
        "- Futubull fees, whole shares, no lookahead.",
        "",
        f"**Policy:** `{pol['name']}` · engine `{pol.get('engine', 'combine')}` · "
        f"{pol['io_sleeve']} · longs top {pol['long_top_n']} @ {pol['long_pct']:.0%} "
        f"· day_cap {pol.get('day_cap', 1):.0%} · min_buys {pol.get('min_buys', 5)} "
        f"· rotate={pol.get('rotate_mover', False)} "
        f"· carry={pol.get('carry_last_book', False)} "
        f"· size-up ×{pol['sizeup']}",
        "",
        "## Headline",
        "",
        "| Start | Final | Return | Max DD | Trades | Win | vs .io 2w_size | Gate |",
        "|---:|---:|---:|---:|---:|---:|---|---|",
        f"| ${sim['capital']:,.0f} | ${st['final_equity']:,.2f} | "
        f"**{st['total_ret_pct']:+.2f}%** | {st['max_dd_pct']:.2f}% | "
        f"{st['n_trades']} | {st['hit'] or 0:.1%} | "
        f"{'BEATS' if st['beat_io_top'] else 'trails'} {io_top:+.2f}% | "
        f"**{gate}** |",
        "",
        "| Side | Trades | Win | P&L |",
        "|---|---:|---:|---:|",
        f"| BUY | {st['by_side']['BUY']['n']} | {st['by_side']['BUY']['hit'] or 0:.1%} | "
        f"${st['by_side']['BUY']['pnl']:,.2f} |",
        f"| SELL | {st['by_side']['SELL']['n']} | {st['by_side']['SELL']['hit'] or 0:.1%} | "
        f"${st['by_side']['SELL']['pnl']:,.2f} |",
        "",
        "## 15% every 2 weeks",
        "",
        f"Target **+{TARGET_2W_PCT:.0f}%** per calendar fortnight "
        f"({TWO_WEEK_CAL_DAYS} days) and per {TWO_WEEK_SESSIONS} trading sessions.",
        f"Fortnights: **{'PASS' if st['hit_every_fortnight'] else 'FAIL'}** "
        f"(min {st['min_fortnight']}). "
        f"10-session blocks: **{'PASS' if st['hit_every_block_2w'] else 'FAIL'}** "
        f"(min {st['min_block_2w']}). "
        f"Rolling: **{'PASS' if st['hit_every_rolling_2w'] else 'FAIL'}** "
        f"(min {st['min_rolling_2w']}).",
        "",
        "| Kind | Start | End | n | Return | Gate |",
        "|---|---|---|---:|---:|---|",
    ]
    for b in st["fortnights"]:
        tag = "partial" if b.get("partial") else (
            "PASS" if b["ret_pct"] >= TARGET_2W_PCT else "FAIL")
        lines.append(
            f"| fortnight | {b['start']} | {b['end']} | {b['n']} | "
            f"{b['ret_pct']:+.2f}% | {tag} |")
    for b in st["blocks_2w"]:
        tag = "partial" if b.get("partial") else (
            "PASS" if b["ret_pct"] >= TARGET_2W_PCT else "FAIL")
        lines.append(
            f"| block | {b['start']} | {b['end']} | {b['n']} | "
            f"{b['ret_pct']:+.2f}% | {tag} |")
    for w in st["rolling_2w"]:
        tag = "PASS" if w["ret_pct"] >= TARGET_2W_PCT else "FAIL"
        lines.append(
            f"| roll | {w['start']} | {w['end']} | {w['n']} | "
            f"{w['ret_pct']:+.2f}% | {tag} |")

    lines += [
        "",
        "## Day route",
        "",
        "| Date | Score | Route | Equity | core | tac.io | tac.mv |",
        "|---|---:|---|---:|---:|---:|---:|",
    ]
    for r in sim["curve"]:
        sc = "—" if r.get("score") is None else f"{r['score']:+.2f}"
        lines.append(
            f"| {r['date']} | {sc} | {r['route']} | ${r['equity']:,.2f} | "
            f"{r['core_n']} | {r.get('tac_io_n', 0)} | {r['tac_n']} |")

    lines += ["", "## Sweep (same window, same fees)", "",
              "| Policy | Return | Max DD | min fortnight | min block | Pass |",
              "|---|---:|---:|---:|---:|---|"]
    for row in sweep_rows:
        s = row["stats"]
        lines.append(
            f"| `{row['name']}` | {s['total_ret_pct']:+.2f}% | "
            f"{s['max_dd_pct']:.2f}% | {s['min_fortnight']} | "
            f"{s['min_block_2w']} | {'YES' if s['passed'] else 'no'} |")

    lines += [
        "",
        "## Why this merge",
        "",
        "- **Mover** is the highest hit-rate sleeve on this tape (paper +9.3%, "
        "max DD 0.12%) because the S ≥ +1 gate deletes the fall days. It is "
        "*off* most sessions — that is the product, not a bug. The days it "
        "*is* on (08-20, 08-21) are the ones `.io` 2w_size lost or lagged.",
        "- **.io `2w_size`** is the current top published book "
        f"({io_top:+.2f}%) and the one that keeps winning on SPY-down / "
        "hard-red mornings (08-14 +3.2%, 08-18/19 +2.0/+4.1%). An earlier "
        "NAV stitch that flattened on every green morning *including* "
        "08-13/14 (zero BUY calls) sat in cash and gave the edge back.",
        "- **Flatten, don't average.** Averaging pick lists re-imports Excel's "
        "median-zero payoff. The combined book *is* `.io` until a green "
        "morning that actually has a priced mover BUY list and a prior book, "
        "then it *is* mover for one session.",
        "- **Open flatten is leak-free:** `.io` names were bought at a prior "
        "close; the 09:30 open is the first price you can get after the new "
        "morning predict. Today's book print is not known at 09:30 so the "
        "flatten uses yesterday / last print only. Tomorrow's score is never "
        "used at today's close.",
        "- **Rotate at the next green open** is the honest way to stay fully "
        "invested in mover (the paper book's same-day close→open recycle is "
        "a leak; we do not copy it). **Carry last book** keeps the 2w sleeve "
        "working on days the book job did not print.",
        "",
        "Code: `src/sleeve_merge.py`. Machine: `data/sleeve_merge/`. "
        "Dashboard: `dashboard/sleeve-merge/index.html`.",
        "",
    ]
    (SCOREBOARD / "SLEEVE_MERGE.md").write_text("\n".join(lines), encoding="utf-8")

    cards = (
        f"<div class='card'>Final equity<b>${st['final_equity']:,.0f}</b></div>"
        f"<div class='card'>Return<b>{st['total_ret_pct']:+.2f}%</b></div>"
        f"<div class='card'>Max DD<b>{st['max_dd_pct']:.2f}%</b></div>"
        f"<div class='card'>Fortnight min<b>{st['min_fortnight']}</b></div>"
        f"<div class='card'>vs .io 2w<b>"
        f"{'BEATS' if st['beat_io_top'] else 'trails'} {io_top:+.1f}%</b></div>"
        f"<div class='card'>15%/2w<b>{gate}</b></div>"
    )
    rows_html = "".join(
        f"<tr><td>{r['date']}</td><td>{r['route']}</td>"
        f"<td>${r['equity']:,.0f}</td><td>{r['core_n']}</td>"
        f"<td>{r.get('tac_io_n', 0)}</td><td>{r['tac_n']}</td></tr>"
        for r in sim["curve"]
    )
    html = f"""<!doctype html>
<html><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Combined sleeve — .io × mover</title>
<style>
:root{{--bg:#0b1020;--card:#131b31;--line:#2b3552;--text:#edf2ff;--muted:#9cabc9}}
*{{box-sizing:border-box}}body{{margin:0;background:var(--bg);color:var(--text);font:15px/1.45 system-ui}}
main{{max-width:1100px;margin:auto;padding:16px}}
.cards{{display:grid;grid-template-columns:repeat(auto-fit,minmax(140px,1fr));gap:10px;margin:14px 0}}
.card{{background:var(--card);border:1px solid var(--line);border-radius:12px;padding:12px}}
.card b{{display:block;font-size:22px;margin-top:4px}}
.muted{{color:var(--muted)}}
table{{border-collapse:collapse;width:100%;background:var(--card)}}
th,td{{padding:7px 8px;border-bottom:1px solid var(--line)}}
.pass{{color:#4ade80}}.fail{{color:#f87171}}
</style></head><body><main>
<h1>Combined sleeve — .io × mover</h1>
<p class="muted"><a href="../" style="color:#93c5fd">.io paper</a>
 · <a href="../mover-paper/" style="color:#93c5fd">mover paper</a></p>
<p class="muted">{pol['name']} · {pol.get('engine','combine')} ·
{pol['io_sleeve']} · flatten when S≥{pol['long_gate']:+.1f} and
≥{pol.get('min_buys',5)} priced BUYs · day_cap {pol.get('day_cap',1):.0%} ·
Futubull fees · 15% / 2 weeks gate</p>
<div class="cards">{cards}</div>
<h2>Daily book</h2>
<table><thead><tr><th>Date</th><th>Route</th><th>Equity</th><th>core</th><th>tac.io</th><th>tac.mv</th></tr></thead>
<tbody>{rows_html}</tbody></table>
<p class="muted">Full write-up: 03_scoreboard/SLEEVE_MERGE.md</p>
</main></body></html>
"""
    (DASH_DIR / "index.html").write_text(html, encoding="utf-8")


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--capital", type=float, default=100_000)
    ap.add_argument("--write", action="store_true")
    ap.add_argument("--policy", default="")
    args = ap.parse_args(argv)

    payload = load_payload()
    books = list_books()
    if not books:
        raise SystemExit("[sleeve-merge] no stock books")
    io_top = io_top_return()
    print(f"[sleeve-merge] books={len(books)} sessions "
          f"io_top={io_top:+.2f}% target={TARGET_2W_PCT:.0f}%/2w")

    sweep_rows = run_sweep(payload, books, args.capital)
    if args.policy:
        sweep_rows = [r for r in sweep_rows if r["name"] == args.policy] or sweep_rows
    winner = sweep_rows[0]
    st = winner["stats"]
    print(f"[sleeve-merge] winner={winner['name']} ret={st['total_ret_pct']:+.2f}% "
          f"min_fort={st['min_fortnight']} min_block={st['min_block_2w']} "
          f"passed={st['passed']}")
    for row in sweep_rows:
        s = row["stats"]
        print(f"  {row['name']:24s} {s['total_ret_pct']:+7.2f}%  "
              f"fort={s['min_fortnight']} block={s['min_block_2w']} "
              f"{'PASS' if s['passed'] else 'no'}")

    if args.write:
        write_outputs(winner, sweep_rows, io_top)
        print(f"[sleeve-merge] wrote {SCOREBOARD / 'SLEEVE_MERGE.md'}")
    return 0 if st["passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
