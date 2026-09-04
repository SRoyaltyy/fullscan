"""Mover paper trading v2 — strategy-configured, day-gated, fully documented.

Trades the mover lookback calls as a cash-accounted paper book.

Sources (--source):
  mover  = mover-lookback BUY/SELL calls, knowable at 09:30 ET (default)
  book   = daily stock-book picks (data/stock_book/{date}_stock_book.json,
           --book-list 1d|3d|1w|2w|1m). The book prints ~13:00-15:45 ET,
           so book mode FORCES entry at the 16:00 ET close — a 09:30
           entry would be a forward leak. Ranks by book score.
Book mode writes its own outputs (data/book_paper/, BOOK_PAPER.md,
BOOK_STRATEGY_SWEEP.md, dashboard/book-paper/) and never clobbers mover mode.

Strategy levers (CLI):
  --side long|both        default long (SELL shorts optional)
  --entry open|close      default open  (09:30 ET vs 16:00 ET entry)
  --hold eod|1d|3d|1w     default 1d    (exit at 16:00 ET of that session)
  --top-n N               default 10    (per side, per day)
  --rank cond|conviction|dip   default cond
        cond       = condition boxes (good - bad), 09:30-knowable
        conviction = setup edge + lane bonus + condition, 09:30-knowable
        dip        = biggest same-day drop first — ONLY valid with
                     --entry close (day_change needs the 16:00 print)
  --gate-score X          default 1.0   (solo mover gate; ignored when
        --io-fallback is on)
  --io-fallback / --no-io-fallback
                          default ON for --source mover. Skip-day book:
                          live .io 2w_size daily mark when S < +1
                          (including hard-red); mover 1d at 09:30 when
                          S >= +1 or missing. S <= -3 blocks new 1d
                          tickets; it does not flatten 2w_size.
  --pct 0.10              per-trade notional as fraction of equity
  --capital 100000

Default mover-paper book (io-fallback): morning general predict S.
S >= +1 or missing → mover 1d at 09:30. Every mover-skip morning
(S < +1, including hard-red) takes that day's live .io 2w_size mark —
the book that was already on, not a new 1d ticket at 16:00. Hard-red
S <= -3 means no new 1d risk; 2w_size stays on. News-judge hawkish
items and high-uncertainty event binaries are advisory flags.
Backtest over 2026-08-13..09-03: gate score>=1 blocked all four bad BUY
days (08-24 -1.9%, 08-28 -9.1%, 08-31 -0.4%, 09-01 -1.2%) and kept all
three winners (+5.1 / +2.6 / +7.4).

Outputs:
  data/mover_paper/trades.csv        every fill: entry/exit date + ET clock,
                                     ticker, side, shares, prices, fees, P&L
  data/mover_paper/skipped.csv       calls not filled, with reason
  data/mover_paper/equity_curve.csv  daily mark-to-market
  data/mover_paper/state.json        headline stats
  03_scoreboard/MOVER_PAPER.md       human summary
  03_scoreboard/MOVER_STRATEGY_SWEEP.md  daily re-ranked lever sweep
                                         (trimmed compound = anti-lottery)
  dashboard/mover-paper/index.html   phone dashboard
"""
from __future__ import annotations

import argparse
import csv
import json
from collections import defaultdict
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
PAYLOAD = ROOT / "03_scoreboard" / "mover_lookback_action.json"
OUT_DIR = ROOT / "data" / "mover_paper"
MD_OUT = ROOT / "03_scoreboard" / "MOVER_PAPER.md"
SWEEP_MD = ROOT / "03_scoreboard" / "MOVER_STRATEGY_SWEEP.md"
HTML_OUT = ROOT / "dashboard" / "mover-paper" / "index.html"
NEWS_DIR = ROOT / "01_daily" / "news"
EVENTS_DIR = ROOT / "01_daily" / "events"

OPEN_CLOCK, CLOSE_CLOCK = "09:30 ET", "16:00 ET"
BORROW_MIN_PRICE = 5.0
BORROW_ANNUAL = 0.01
FEE_DRAG = 0.15  # % round-trip, sweep approximation

TITLE = "Mover paper trading"


# ------------------------------------------------------------ payload I/O --
def load_payload(path: Path = PAYLOAD) -> dict:
    if not path.is_file():
        raise SystemExit(f"[mover-paper] missing payload: {path}")
    return json.loads(path.read_text(encoding="utf-8"))


def _conviction(row: dict) -> float:
    c = row.get("conviction")
    if c is not None:
        return float(c)
    try:  # old payloads without the field — rebuild from setups/lane/cond
        from src import lookback_action as act
        packed = {"action": row.get("action_call")}
        return act.conviction(row, packed)
    except Exception:
        return 0.0


def tradeable_calls(payload: dict, side: str) -> list[dict]:
    rows = [r for r in payload.get("called_rows") or []
            if r.get("action_call") == "BUY" or
            (side == "both" and r.get("action_call") == "SELL")]
    for r in rows:
        r["_conv"] = _conviction(r)
    return rows


# ------------------------------------------------------------ book source --
BOOK_DIR = ROOT / "data" / "stock_book"
BOOK_LISTS = ("1d", "3d", "1w", "2w", "1m")
HOLD_SESSIONS = {"1d": 1, "3d": 3, "1w": 5}


def _session_calendar(payload: dict) -> list[str]:
    sd = sorted(payload.get("session_dates") or [])
    if sd:
        return sd
    return sorted(p.name[:10]
                  for p in BOOK_DIR.glob("????-??-??_stock_book.json"))


def book_calls(payload: dict, book_list: str, side: str) -> list[dict]:
    """Daily stock-book picks -> call rows for the sim.

    LEAK RULE: the book prints ~13:00-15:45 ET on the signal day, so the
    09:30 open of that day is NOT knowable. Book mode must run with
    --entry close (enforced in main). Exit dates come from the session
    calendar; prices from the shared OHLC store via _bar().
    """
    cal = _session_calendar(payload)
    rows: list[dict] = []
    for path in sorted(BOOK_DIR.glob("????-??-??_stock_book.json")):
        date = path.name[:10]
        try:
            doc = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            continue
        book = ((doc.get("books") or {}).get(book_list)) or {}
        if date in cal:
            i = cal.index(date)
            hz = {h: cal[i + n] for h, n in HOLD_SESSIONS.items()
                  if i + n < len(cal)}
        else:
            hz = {}
        for side_key, call in (("buy", "BUY"), ("sell", "SELL")):
            if call == "SELL" and side != "both":
                continue
            for p in book.get(side_key) or []:
                t = p.get("ticker")
                if not t:
                    continue
                score = float(p.get("score") or 0)
                rows.append({
                    "date": date, "ticker": t, "action_call": call,
                    "conviction": score, "_conv": score,
                    "cond_tally": p.get("size") or "—",
                    "action_reason": p.get("reasons"),
                    "horizon_dates": dict(hz),
                })
    return rows


# --------------------------------------------------------------- day gate --
def gate_table(payload: dict, gate_score: float | None,
               dates: list[str] | None = None) -> list[dict]:
    """Per-session gate decision + advisory flags (news judge / events)."""
    regime = payload.get("regime") or {}
    if dates is None:
        dates = sorted(set([r.get("date")
                            for r in payload.get("called_rows") or []]))
    table = []
    for d in sorted(dates):
        g = regime.get(d) or {}
        score, pdir = g.get("predict_score"), g.get("predict_dir")
        if gate_score is None:
            decision, why = "OPEN", "gate disabled"
        elif score is None:
            decision, why = "OPEN", "no predict on file — allowed"
        elif score >= gate_score:
            decision, why = "OPEN", f"predict score {score:+.2f} >= {gate_score:+.2f}"
        else:
            decision, why = "CLOSED", f"predict {pdir} score {score:+.2f} < {gate_score:+.2f}"
        advisory = []
        judge = NEWS_DIR / f"{d}_judge.md"
        if judge.is_file():
            try:
                t = judge.read_text(encoding="utf-8", errors="replace")[:4000]
                if "polarity: hawkish" in t or "polarity: bearish" in t:
                    advisory.append("news judge: hawkish/bearish top items")
            except OSError:
                pass
        ev = EVENTS_DIR / f"{d}_events.md"
        if ev.is_file():
            try:
                t = ev.read_text(encoding="utf-8", errors="replace")[:2000]
                if "uncertainty: **high**" in t:
                    advisory.append("events: high uncertainty")
            except OSError:
                pass
        table.append({"date": d, "predict_dir": pdir, "predict_score": score,
                      "spy_down_streak": g.get("spy_down_streak") or 0,
                      "decision": decision, "why": why,
                      "advisory": "; ".join(advisory)})
    return table


# --------------------------------------------------------------- pricing --
def _bar(ticker: str, date: str) -> dict:
    try:
        from src import ticker_lookback as tl
        return tl.session_bar(ticker, date) or {}
    except Exception:
        return {}


def _exit_for(row: dict, hold: str) -> tuple[str | None, float | None, str]:
    """(exit_date, exit_price, exit_clock). eod = same close; else close at
    horizon date via bar store, else derived close*(1+fwd)."""
    bar = row.get("session_bar") or {}
    c = bar.get("close")
    if hold == "eod":
        return (row.get("date"), float(c) if c else None, CLOSE_CLOCK)
    hz = row.get("horizon_dates") or {}
    nxt = hz.get(hold)
    if nxt:
        px = (_bar(row.get("ticker") or "", nxt) or {}).get("close")
        if px:
            return nxt, float(px), CLOSE_CLOCK
    fwd = (row.get("price_changes") or {}).get(hold)
    if fwd is not None and c:
        try:
            return nxt, float(c) * (1 + float(fwd) / 100.0), CLOSE_CLOCK
        except (TypeError, ValueError):
            pass
    return None, None, CLOSE_CLOCK


RANKS = {
    "cond": lambda r: -(((r.get("condition") or {}).get("good") or 0)
                        - ((r.get("condition") or {}).get("bad") or 0)),
    "conviction": lambda r: -(r.get("_conv") or 0),
    "dip": lambda r: (r.get("day_change") or 0),   # close-entry only
    # stock-book score: best BUY first, best SELL (most negative) first
    "book": lambda r: -(r.get("_conv") or 0)
    * (1 if r.get("action_call") == "BUY" else -1),
}


# --------------------------------------------------------------- the sim --
def run_sim(calls: list[dict], gates: list[dict], *, capital: float,
            top_n: int, pct: float, side: str, entry: str, hold: str,
            rank: str) -> dict:
    from src import paper_trade as pt

    fees = pt.load_fees()
    open_days = {g["date"] for g in gates if g["decision"] == "OPEN"}
    by_day: dict[str, list[dict]] = defaultdict(list)
    for r in calls:
        by_day[r.get("date")].append(r)

    cash = capital
    open_pos: list[dict] = []
    trades, skipped, curve = [], [], []
    rank_key = RANKS[rank]

    for date in sorted(by_day):
        def equity_now(px_date: str) -> float:
            eq = cash
            for p in open_pos:
                last = p.get("last_px") or p["entry_px"]
                c = (_bar(p["ticker"], px_date) or {}).get("close")
                if c:
                    last = p["last_px"] = float(c)
                mv = p["shares"] * last
                eq += mv if p["side"] == "BUY" else -mv
            return eq

        still_open = []
        for p in open_pos:
            if p["exit_date"] == date:
                px = float((_bar(p["ticker"], date) or {}).get("close")
                           or p.get("last_px") or p["entry_px"])
                if p["side"] == "BUY":
                    fee = pt.order_fees(p["shares"], px, "sell", fees)
                    cash += p["shares"] * px - fee
                    pnl = p["shares"] * (px - p["entry_px"]) - p["fee_in"] - fee
                else:
                    fee = pt.order_fees(p["shares"], px, "buy", fees)
                    cash -= p["shares"] * px + fee
                    pnl = p["shares"] * (p["entry_px"] - px) - p["fee_in"] - fee
                p.update({"exit_px": round(px, 4), "fee_out": round(fee, 2),
                          "pnl": round(pnl, 2),
                          "ret_pct": round(100 * pnl / max(p["notional"], 1), 2)})
                trades.append(p)
            else:
                still_open.append(p)
        open_pos = still_open

        if date not in open_days:
            g = next((g for g in gates if g["date"] == date), {})
            for r in by_day[date]:
                skipped.append({"date": date, "ticker": r.get("ticker"),
                                "side": r.get("action_call"),
                                "conviction": r.get("_conv"),
                                "reason": f"day gate CLOSED: {g.get('why', '')}"})
        else:
            taken = {"BUY": 0, "SELL": 0}
            for r in sorted(by_day[date], key=rank_key):
                s_ = r["action_call"]
                ticker = r.get("ticker") or ""
                if taken[s_] >= top_n:
                    skipped.append({"date": date, "ticker": ticker, "side": s_,
                                    "conviction": r.get("_conv"),
                                    "reason": "outside top-N conviction cut"})
                    continue
                bar = r.get("session_bar") or _bar(ticker, date)
                op, cl = (bar or {}).get("open"), (bar or {}).get("close")
                if not op or not cl:
                    skipped.append({"date": date, "ticker": ticker, "side": s_,
                                    "conviction": r.get("_conv"),
                                    "reason": "no session bar (price data missing)"})
                    continue
                op, cl = float(op), float(cl)
                entry_px = op if entry == "open" else cl
                entry_clock = OPEN_CLOCK if entry == "open" else CLOSE_CLOCK
                if s_ == "SELL" and entry_px < BORROW_MIN_PRICE:
                    skipped.append({"date": date, "ticker": ticker, "side": s_,
                                    "conviction": r.get("_conv"),
                                    "reason": f"open ${op:.2f} < ${BORROW_MIN_PRICE:.0f} "
                                              "hard-to-borrow screen"})
                    continue
                exit_date, exit_px, exit_clock = _exit_for(r, hold)
                if exit_px is None:
                    skipped.append({"date": date, "ticker": ticker, "side": s_,
                                    "conviction": r.get("_conv"),
                                    "reason": "no exit price (unsettleable)"})
                    continue
                eq = equity_now(date)
                notional = round(eq * pct, 2)
                shares = int(notional // entry_px)
                if shares <= 0:
                    skipped.append({"date": date, "ticker": ticker, "side": s_,
                                    "conviction": r.get("_conv"),
                                    "reason": "position size < 1 share"})
                    continue
                notional = shares * entry_px
                if s_ == "BUY":
                    fee_in = pt.order_fees(shares, entry_px, "buy", fees)
                    if notional + fee_in > cash:
                        skipped.append({"date": date, "ticker": ticker,
                                        "side": s_, "conviction": r.get("_conv"),
                                        "reason": f"insufficient cash "
                                                  f"(${notional + fee_in:,.0f} > ${cash:,.0f})"})
                        continue
                    cash -= notional + fee_in
                else:
                    if eq < 2 * notional:
                        skipped.append({"date": date, "ticker": ticker,
                                        "side": s_, "conviction": r.get("_conv"),
                                        "reason": "insufficient margin (equity < 2x notional)"})
                        continue
                    fee_in = pt.order_fees(shares, entry_px, "sell", fees) \
                        + notional * BORROW_ANNUAL / 365.0
                    cash += notional - fee_in
                taken[s_] += 1
                open_pos.append({
                    "entry_dt": f"{date} {entry_clock}", "date": date,
                    "ticker": ticker, "side": s_, "shares": shares,
                    "entry_px": entry_px, "notional": round(notional, 2),
                    "fee_in": round(fee_in, 2),
                    "exit_dt": f"{exit_date} {exit_clock}", "exit_date": exit_date,
                    "conviction": r.get("_conv"),
                    "cond_tally": r.get("cond_tally"),
                    "reason": r.get("action_reason"), "last_px": cl,
                })
        curve.append({"date": date, "cash": round(cash, 2),
                      "equity": round(equity_now(date), 2),
                      "open": len(open_pos)})

    for p in open_pos:
        px = float(p.get("last_px") or p["entry_px"])
        fee = pt.order_fees(p["shares"], px,
                            "sell" if p["side"] == "BUY" else "buy", fees)
        sign = 1 if p["side"] == "BUY" else -1
        pnl = sign * p["shares"] * (px - p["entry_px"]) - p["fee_in"] - fee
        cash += (p["shares"] * px - fee) if p["side"] == "BUY" \
            else -(p["shares"] * px + fee)
        p.update({"exit_px": px, "fee_out": round(fee, 2),
                  "pnl": round(pnl, 2),
                  "ret_pct": round(100 * pnl / max(p["notional"], 1), 2),
                  "reason": (p.get("reason") or "") + " [force-closed]"})
        trades.append(p)

    return {"capital": capital, "top_n": top_n, "pct": pct, "side": side,
            "entry": entry, "hold": hold, "rank": rank,
            "trades": trades, "skipped": skipped, "curve": curve,
            "final_equity": round(cash, 2)}


# ---------------------------------------------------------------- stats --
def stats(sim: dict) -> dict:
    trades, curve, cap = sim["trades"], sim["curve"], sim["capital"]
    final = curve[-1]["equity"] if curve else sim["final_equity"]
    pnls = [t["pnl"] for t in trades]
    wins = [p for p in pnls if p > 0]
    by_side = {}
    for s_ in ("BUY", "SELL"):
        sp = [t["pnl"] for t in trades if t["side"] == s_]
        by_side[s_] = {"n": len(sp),
                       "hit": round(sum(1 for p in sp if p > 0) / len(sp), 3) if sp else None,
                       "pnl": round(sum(sp), 2)}
    peak, max_dd = cap, 0.0
    for pt_ in curve:
        peak = max(peak, pt_["equity"])
        max_dd = max(max_dd, (peak - pt_["equity"]) / peak)
    return {"n_trades": len(trades), "n_skipped": len(sim["skipped"]),
            "hit": round(len(wins) / len(pnls), 3) if pnls else None,
            "total_pnl": round(sum(pnls), 2),
            "total_ret_pct": round(100 * (final - cap) / cap, 2),
            "final_equity": round(final, 2), "max_dd_pct": round(100 * max_dd, 2),
            "avg_win": round(sum(wins) / len(wins), 2) if wins else None,
            "avg_loss": (round(sum(p for p in pnls if p <= 0)
                               / max(len(pnls) - len(wins), 1), 2) if pnls else None),
            "by_side": by_side, "n_days": len(curve)}


PAPER_EQ = ROOT / "data" / "paper" / "equity_curve.csv"
IO_SKIP_SLEEVE = "2w_size"


def paper_sleeve_daily(sleeve: str = IO_SKIP_SLEEVE) -> dict[str, float]:
    """Live .io paper daily returns for one sleeve (already-on marks)."""
    eq: dict[str, float] = {}
    if not PAPER_EQ.is_file():
        return {}
    with PAPER_EQ.open(encoding="utf-8") as fh:
        for row in csv.DictReader(fh):
            if row.get("sleeve") != sleeve:
                continue
            try:
                eq[row["date"]] = float(row["equity"])
            except (TypeError, ValueError):
                pass
    out: dict[str, float] = {}
    dates = sorted(eq)
    for i, d in enumerate(dates):
        if i == 0:
            continue
        prev = eq[dates[i - 1]]
        if prev:
            out[d] = eq[d] / prev - 1.0
    return out


def _mover_day_rets(curve: list[dict]) -> dict[str, float]:
    eq = {c["date"]: float(c["equity"]) for c in curve if c.get("equity") is not None}
    dates = sorted(eq)
    out: dict[str, float] = {}
    for i, d in enumerate(dates):
        if i == 0:
            out[d] = 0.0
            continue
        prev = eq[dates[i - 1]]
        out[d] = (eq[d] / prev - 1.0) if prev else 0.0
    return out


def stitch_skip_io(raw: dict, payload: dict,
                   io_rets: dict[str, float] | None = None) -> tuple[dict, list[dict]]:
    """Mover 1d on S>=+1; live .io 2w_size mark on every mover-skip day.

    A new 1d .io ticket at yesterday's close cannot show yesterday's win —
    that win is the mark on 2w_size names that were already on. Hard-red
    does not flatten that book.
    """
    from src.sleeve_combine import route_fallback

    regime = payload.get("regime") or {}
    io_rets = paper_sleeve_daily() if io_rets is None else io_rets
    m_rets = _mover_day_rets(raw.get("curve") or [])
    score_by = {c["date"]: c.get("score") for c in raw.get("curve") or []}
    candidates = sorted(set(m_rets) | set(io_rets) | set(score_by) | set(regime))
    candidates = [d for d in candidates if d >= "2026-08-13"]

    capital = float(raw.get("capital") or 100_000)
    eq = capital
    curve, gates, io_trades = [], [], []
    for d in candidates:
        score = score_by.get(d)
        if score is None:
            score = (regime.get(d) or {}).get("predict_score")
        card = route_fallback(score)
        # Don't invent a flat mover day from a .io print (e.g. Sunday 08-30).
        if card["bucket"] == "mover" and d not in m_rets:
            continue
        try:
            weekday = datetime.strptime(d, "%Y-%m-%d").weekday()
        except ValueError:
            weekday = 0
        if weekday >= 5 and card["bucket"] == "mover":
            continue
        if card["bucket"] == "mover":
            r = m_rets.get(d, 0.0)
            adv = ""
        else:
            if d not in io_rets and d not in m_rets and d not in score_by:
                # regime-only skip with no mark and no mover row: still show
                r = 0.0
            else:
                r = io_rets.get(d, 0.0)
            pnl = round(eq * r, 2)
            gap = d not in io_rets
            io_trades.append({
                "entry_dt": f"{d} 16:00 ET", "date": d,
                "ticker": IO_SKIP_SLEEVE, "side": "BUY",
                "shares": 0, "entry_px": 0.0,
                "exit_dt": f"{d} 16:00 ET", "exit_px": 0.0,
                "notional": round(eq, 2), "fee_in": 0.0, "fee_out": 0.0,
                "pnl": pnl, "ret_pct": round(100 * r, 2),
                "conviction": 0, "cond_tally": "io",
                "reason": (
                    f"no live {IO_SKIP_SLEEVE} print (gap)" if gap
                    else f"live {IO_SKIP_SLEEVE} day mark (already on)"
                ),
                "source": "io",
            })
            adv = (f"no live {IO_SKIP_SLEEVE} print (gap)" if gap
                   else f"live {IO_SKIP_SLEEVE} {100 * r:+.2f}%")
        eq = eq * (1.0 + r)
        curve.append({"date": d, "cash": round(eq, 2),
                      "equity": round(eq, 2), "open": 0, "score": score})
        g = regime.get(d) or {}
        gates.append({
            "date": d,
            "predict_dir": g.get("predict_dir"),
            "predict_score": score,
            "spy_down_streak": g.get("spy_down_streak") or 0,
            "decision": "MOVER" if card["bucket"] == "mover" else "IO",
            "why": card["why"],
            "advisory": adv,
        })

    trades = []
    for t in raw.get("trades") or []:
        trades.append({
            "entry_dt": t.get("entry_dt"), "date": t.get("date"),
            "ticker": t.get("ticker"), "side": "BUY",
            "shares": t.get("shares"), "entry_px": t.get("entry_px"),
            "exit_dt": t.get("exit_dt"), "exit_px": t.get("exit_px"),
            "notional": t.get("notional"), "fee_in": t.get("fee_in"),
            "fee_out": t.get("fee_out"), "pnl": t.get("pnl"),
            "ret_pct": t.get("ret_pct"),
            "conviction": t.get("conviction") or 0,
            "cond_tally": "mover", "reason": "mover", "source": "mover",
        })
    trades.extend(io_trades)
    mv_pnl = round(sum(t["pnl"] or 0 for t in trades if t["source"] == "mover"), 2)
    io_pnl = round(sum(t["pnl"] or 0 for t in trades if t["source"] == "io"), 2)
    sim = {
        "capital": capital, "top_n": raw.get("top_n", 10),
        "pct": raw.get("pct", 0.10), "side": "long",
        "entry": "open+close", "hold": "1d / 2w_size mark",
        "rank": "cond / live 2w_size",
        "trades": trades, "skipped": [], "curve": curve,
        "final_equity": round(eq, 2),
        "source": "mover+io", "io_fallback": True,
        "book_list": "2w_size",
        "by_source": {
            "mover": {"n": sum(1 for t in trades if t["source"] == "mover"),
                      "pnl": mv_pnl},
            "io": {"n": sum(1 for t in trades if t["source"] == "io"),
                   "pnl": io_pnl},
        },
    }
    global TITLE
    TITLE = "Mover paper — skip days defer to live .io 2w_size"
    return sim, gates


def bt_to_mover_sim(raw: dict, payload: dict,
                    io_rets: dict[str, float] | None = None) -> tuple[dict, list[dict]]:
    """Skip-day stitch: mover 1d on S>=+1, live 2w_size mark otherwise."""
    return stitch_skip_io(raw, payload, io_rets=io_rets)


# ------------------------------------------------- strategy sweep (daily) --
def _row_ret(r: dict, entry: str, hold: str):
    bar = r.get("session_bar") or {}
    o, c = bar.get("open"), bar.get("close")
    if not o or not c:
        return None
    pc = r.get("price_changes") or {}
    if entry == "open":
        if hold == "eod":
            ret = (c - o) / o * 100
        else:
            fwd = pc.get(hold)
            if fwd is None:
                return None
            ret = (c * (1 + float(fwd) / 100) - o) / o * 100
    else:
        fwd = pc.get(hold)
        if fwd is None:
            return None
        ret = float(fwd)
    if r.get("action_call") == "SELL":
        ret = -ret
    return ret - FEE_DRAG


def _cond(r):
    c = r.get("condition") or {}
    return (c.get("good") or 0) - (c.get("bad") or 0)


SWEEP_FILTERS = {
    "all": lambda r: True,
    "nored": lambda r: ((r.get("condition") or {}).get("bad") or 0) == 0,
    "g3": lambda r: ((r.get("condition") or {}).get("good") or 0) >= 3,
    "tone_good": lambda r: (r.get("condition") or {}).get("tone") == "good",
}


def run_sweep(payload: dict, gate_score: float | None) -> list[dict]:
    """Re-rank every lever combo on current data. Ranked by TRIMMED compound
    (drop 2 best + 2 worst trades) — a single lottery winner cannot put a
    combo on top. dip rank only paired with close entry (leak rule)."""
    rows = payload.get("called_rows") or []
    regime = payload.get("regime") or {}

    def gate_ok(d, gate):
        g = regime.get(d) or {}
        s = g.get("predict_score")
        if gate == "none":
            return True
        return s is None or s >= (gate_score if gate_score is not None else 1.0)

    results = []
    for side in ("long", "both"):
        for fname, fn in SWEEP_FILTERS.items():
            for topn in (5, 10, 15):
                for entry, holds in (("open", ("eod", "1d", "3d")),
                                     ("close", ("1d", "3d"))):
                    ranks = ("cond", "conviction") if entry == "open" \
                        else ("cond", "dip")
                    for rank in ranks:
                        for hold in holds:
                            for gate in ("none", "score"):
                                by_day = defaultdict(list)
                                for r in rows:
                                    a = r.get("action_call")
                                    if side == "long" and a != "BUY":
                                        continue
                                    if side == "both" and a not in ("BUY", "SELL"):
                                        continue
                                    if not fn(r):
                                        continue
                                    r["_conv"] = _conviction(r)
                                    by_day[r["date"]].append(r)
                                key = RANKS[rank] if rank != "conviction" \
                                    else RANKS["conviction"]
                                daily, allr = [], []
                                for d in sorted(by_day):
                                    if not gate_ok(d, gate):
                                        continue
                                    picked = sorted(by_day[d], key=key)[:topn]
                                    vs = [x for x in (_row_ret(r, entry, hold)
                                                      for r in picked)
                                          if x is not None]
                                    if vs:
                                        daily.append((d, vs))
                                        allr += vs
                                if len(daily) < 3 or len(allr) < 20:
                                    continue

                                def compound(pairs):
                                    dd = defaultdict(list)
                                    for d, v in pairs:
                                        dd[d].append(v)
                                    eq = 1.0
                                    for d in sorted(dd):
                                        eq *= 1 + sum(dd[d]) / len(dd[d]) / 100
                                    return (eq - 1) * 100

                                flat_all = [(d, v) for d, vs in daily
                                            for v in vs]
                                raw = compound(flat_all)
                                kept = sorted(flat_all,
                                              key=lambda x: x[1])
                                kept = kept[2:-2] if len(kept) > 8 else kept
                                trim = compound(kept)
                                hit = sum(1 for x in allr if x > 0) / len(allr)
                                results.append({
                                    "side": side, "filter": fname,
                                    "rank": rank, "topn": topn,
                                    "entry": entry, "hold": hold,
                                    "gate": gate, "days": len(daily),
                                    "trades": len(allr),
                                    "raw_pct": round(raw, 1),
                                    "trim_pct": round(trim, 1),
                                    "hit": round(hit, 3)})
    results.sort(key=lambda x: -x["trim_pct"])
    return results


def _compound_trim(daily: list[tuple[str, list[float]]],
                   min_days: int, min_trades: int):
    """Shared anti-lottery scorer: raw vs trimmed compound (drop 2 best +
    2 worst trades) over per-day mean returns."""
    flat = [(d, v) for d, vs in daily for v in vs]
    allr = [v for _, vs in daily for v in vs]
    if len(daily) < min_days or len(allr) < min_trades:
        return None

    def compound(pairs):
        dd = defaultdict(list)
        for d, v in pairs:
            dd[d].append(v)
        eq = 1.0
        for d in sorted(dd):
            eq *= 1 + sum(dd[d]) / len(dd[d]) / 100
        return (eq - 1) * 100

    raw = compound(flat)
    kept = sorted(flat, key=lambda x: x[1])
    kept = kept[2:-2] if len(kept) > 8 else kept
    trim = compound(kept)
    hit = sum(1 for x in allr if x > 0) / len(allr)
    return {"days": len(daily), "trades": len(allr),
            "raw_pct": round(raw, 1), "trim_pct": round(trim, 1),
            "hit": round(hit, 3)}


def _book_row_ret(r: dict, hold: str):
    """close(signal day) -> close(exit session), from the OHLC store."""
    t = r.get("ticker") or ""
    c0 = (_bar(t, r.get("date") or "") or {}).get("close")
    if not c0:
        return None
    xd = (r.get("horizon_dates") or {}).get(hold)
    if not xd:
        return None
    c1 = (_bar(t, xd) or {}).get("close")
    if not c1:
        return None
    ret = (float(c1) - float(c0)) / float(c0) * 100
    if r.get("action_call") == "SELL":
        ret = -ret
    return ret - FEE_DRAG


def run_book_sweep(rows: list[dict], payload: dict,
                   gate_score: float | None) -> list[dict]:
    """Re-rank the book levers daily: top-N x hold x gate. Entry is always
    the 16:00 ET close (the book is not knowable at 09:30)."""
    regime = payload.get("regime") or {}

    def gate_ok(d, gate):
        if gate == "none":
            return True
        s = (regime.get(d) or {}).get("predict_score")
        return s is None or s >= (gate_score if gate_score is not None else 1.0)

    by_day: dict[str, list[dict]] = defaultdict(list)
    for r in rows:
        by_day[r["date"]].append(r)

    results = []
    for topn in (5, 10, 15):
        for hold in ("1d", "3d", "1w"):
            for gate in ("none", "score"):
                daily = []
                for d in sorted(by_day):
                    if not gate_ok(d, gate):
                        continue
                    picked = sorted(by_day[d], key=RANKS["book"])[:topn]
                    vs = [x for x in (_book_row_ret(r, hold)
                                      for r in picked)
                          if x is not None]
                    if vs:
                        daily.append((d, vs))
                sc = _compound_trim(daily, min_days=3, min_trades=10)
                if sc:
                    results.append({"side": "long", "filter": "book",
                                    "rank": "book", "topn": topn,
                                    "entry": "close", "hold": hold,
                                    "gate": gate, **sc})
    results.sort(key=lambda x: -x["trim_pct"])
    return results


# ---------------------------------------------------------------- output --
def _set_out_paths(source: str) -> None:
    """Book mode writes to its own files so it never clobbers mover mode."""
    global OUT_DIR, MD_OUT, SWEEP_MD, HTML_OUT, TITLE
    if source == "book":
        OUT_DIR = ROOT / "data" / "book_paper"
        MD_OUT = ROOT / "03_scoreboard" / "BOOK_PAPER.md"
        SWEEP_MD = ROOT / "03_scoreboard" / "BOOK_STRATEGY_SWEEP.md"
        HTML_OUT = ROOT / "dashboard" / "book-paper" / "index.html"
        TITLE = "Stock-book paper trading"


def write_outputs(sim, st, gates, sweep, payload, gate_score) -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    HTML_OUT.parent.mkdir(parents=True, exist_ok=True)
    cols = ["entry_dt", "ticker", "side", "shares", "entry_px",
            "exit_dt", "exit_px", "notional", "fee_in", "fee_out", "pnl",
            "ret_pct", "conviction", "cond_tally", "reason", "source"]
    with open(OUT_DIR / "trades.csv", "w", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(fh, fieldnames=cols, extrasaction="ignore")
        w.writeheader()
        w.writerows(sim["trades"])
    with open(OUT_DIR / "skipped.csv", "w", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(fh, fieldnames=["date", "ticker", "side",
                                           "conviction", "reason"],
                           extrasaction="ignore")
        w.writeheader()
        w.writerows(sim["skipped"])
    with open(OUT_DIR / "equity_curve.csv", "w", newline="",
              encoding="utf-8") as fh:
        w = csv.DictWriter(fh, fieldnames=["date", "cash", "equity", "open"],
                           extrasaction="ignore")
        w.writeheader()
        w.writerows(sim["curve"])
    state = {"generated_at": datetime.now().isoformat(timespec="seconds"),
             "asof": payload.get("to_date"),
             "params": {"capital": sim["capital"], "top_n": sim["top_n"],
                        "pct": sim["pct"], "side": sim["side"],
                        "entry": sim["entry"], "hold": sim["hold"],
                        "rank": sim["rank"], "gate_score": gate_score},
             **st}
    (OUT_DIR / "state.json").write_text(json.dumps(state, indent=2),
                                        encoding="utf-8")
    _write_md(sim, st, gates, payload, gate_score)
    _write_sweep_md(sweep, gate_score)
    _write_html(sim, st, gates, sweep, payload, gate_score)


def _write_md(sim, st, gates, payload, gate_score) -> None:
    bs = st["by_side"]
    src_note = ""
    if sim.get("source") == "book":
        src_note = (f" Selection: `{sim.get('book_list')}` stock-book buy "
                    "list (prints ~13:00-15:45 ET, hence close entry).")
    gate_line = (
        f"**Day gate:** trade only when the morning general predict score "
        f">= {gate_score if gate_score is not None else 'off'} "
        "(missing predict = allowed). News-judge hawkish items and "
        "high-uncertainty event binaries are advisory flags below."
    )
    if sim.get("io_fallback"):
        src_note = (" Mover 1d at 09:30 when S ≥ +1 or missing. Every "
                    "mover-skip morning (S < +1, including hard-red) "
                    "takes the live .io 2w_size daily mark — already-on "
                    "names, not a new 1d ticket.")
        gate_line = (
            "**Book:** skip days defer to live .io `2w_size` (same sleeve "
            "as the .io dashboard). Hard-red S ≤ −3 blocks new 1d risk; "
            "it does not flatten 2w_size. A same-close 1d .io fill cannot "
            "show yesterday’s win — that print is the mark on names that "
            "were already on."
        )
    L = [
        f"# {TITLE}", "",
        f"_Generated {datetime.now().isoformat(timespec='seconds')} — "
        f"calls {payload.get('from_date')} → {payload.get('to_date')}_", "",
        "**Strategy:** "
        f"{'LONG-only' if sim['side'] == 'long' else 'LONG+SHORT'} · "
        f"top {sim['top_n']}/day by {sim['rank']} · entry {sim['entry']} "
        f"({'09:30 ET' if sim['entry'] == 'open' else '16:00 ET'}) · "
        f"hold {sim['hold']} (exit 16:00 ET) · {sim['pct']:.0%} of equity "
        f"per trade · Futubull fees · cash-accounted (unfittable trades "
        f"skipped and logged).{src_note}",
        "",
        gate_line, "",
        "## Headline", "",
        "| Start capital | Final equity | Return | Max DD | Trades | "
        "Skipped | Win rate |",
        "|---:|---:|---:|---:|---:|---:|---:|",
        f"| ${sim['capital']:,.0f} | ${st['final_equity']:,.2f} | "
        f"**{st['total_ret_pct']}%** | {st['max_dd_pct']}% | "
        f"{st['n_trades']} | {st['n_skipped']} | "
        f"{round(100 * (st['hit'] or 0), 1)}% |", "",
        "| Side | Trades | Win rate | P&L |", "|---:|---:|---:|---:|",
        f"| BUY (long) | {bs['BUY']['n']} | "
        f"{round(100 * (bs['BUY']['hit'] or 0), 1)}% | ${bs['BUY']['pnl']:,.2f} |",
        f"| SELL (short) | {bs['SELL']['n']} | "
        f"{round(100 * (bs['SELL']['hit'] or 0), 1)}% | ${bs['SELL']['pnl']:,.2f} |",
        "", "## Day gate (per session)", "",
        "| Date | Predict | Score | SPY streak | Book | Advisory |",
        "|---|---|---:|---:|---|---|",
    ]
    for g in gates:
        L.append(f"| {g['date']} | {g.get('predict_dir') or '—'} | "
                 f"{g.get('predict_score') if g.get('predict_score') is not None else '—'} | "
                 f"{g.get('spy_down_streak')} | **{g['decision']}** — {g['why']} | "
                 f"{g.get('advisory') or '—'} |")
    L += ["", "## Last 25 filled trades", "",
          "| Entry (ET) | Ticker | Side | Shares | Entry px | Exit (ET) | "
          "Exit px | P&L | Ret | Cond |",
          "|---|---|---|---:|---:|---|---:|---:|---:|---|"]
    for t in sim["trades"][-25:]:
        L.append(f"| {t['entry_dt']} | `{t['ticker']}` | {t['side']} | "
                 f"{t['shares']} | ${t['entry_px']:.2f} | {t.get('exit_dt')} | "
                 f"${t.get('exit_px') or 0:.2f} | ${t.get('pnl') or 0:,.2f} | "
                 f"{t.get('ret_pct') or 0}% | {t.get('cond_tally') or '—'} |")
    L += ["", f"Full records: `{OUT_DIR.relative_to(ROOT)}/trades.csv` "
              "(every fill with ET timestamps, prices, fees), `skipped.csv`, "
              f"`equity_curve.csv`. Lever sweep: `{SWEEP_MD.name}`. "
              f"Dashboard: `{HTML_OUT.relative_to(ROOT)}`.", ""]
    MD_OUT.write_text("\n".join(L) + "\n", encoding="utf-8")


def _write_sweep_md(sweep: list[dict], gate_score) -> None:
    L = [
        f"# {TITLE} — strategy sweep (daily re-rank)", "",
        "Every lever combo re-scored on the latest payload. "
        "**Sorted by trimmed compound** — the 2 best and 2 worst trades are "
        "dropped before compounding, so one lottery winner cannot put a "
        "combo on top. `raw` is the untrimmed number (watch the gap: "
        "big gap = lottery-driven). `dip` rank pairs only with close entry "
        "(same-day change is only knowable at 16:00 ET). Gate = morning "
        f"predict score >= {gate_score if gate_score is not None else 'off'} "
        "(missing = allowed). 0.15% round-trip fee drag.", "",
        "| # | Side | Filter | Rank | N | Entry | Hold | Gate | "
        "Trimmed % | Raw % | Hit | Trades | Days |",
        "|---:|---|---|---|---:|---|---|---|---:|---:|---:|---:|---:|",
    ]
    for i, r in enumerate(sweep[:40], 1):
        L.append(f"| {i} | {r['side']} | {r['filter']} | {r['rank']} | "
                 f"{r['topn']} | {r['entry']} | {r['hold']} | {r['gate']} | "
                 f"**{r['trim_pct']}** | {r['raw_pct']} | {r['hit']} | "
                 f"{r['trades']} | {r['days']} |")
    L.append("")
    SWEEP_MD.write_text("\n".join(L) + "\n", encoding="utf-8")


def _write_html(sim, st, gates, sweep, payload, gate_score) -> None:
    import html as _html
    curve = sim["curve"]
    svg = ""
    if len(curve) > 1:
        W, H, P = 960, 260, 34
        ys = [c["equity"] for c in curve]
        lo, hi = min(ys + [sim["capital"]]), max(ys + [sim["capital"]])
        rng = (hi - lo) or 1.0
        X = lambda i: P + (W - 2 * P) * i / (len(curve) - 1)
        Y = lambda v: H - P - (H - 2 * P) * (v - lo) / rng
        pts = " ".join(f"{X(i):.1f},{Y(v):.1f}" for i, v in enumerate(ys))
        base = Y(sim["capital"])
        svg = (f"<svg viewBox='0 0 {W} {H}' width='100%' height='{H}'>"
               f"<line x1='{P}' y1='{base:.1f}' x2='{W - P}' y2='{base:.1f}' "
               f"stroke='#5b6b8c' stroke-dasharray='4 4'/>"
               f"<polyline points='{pts}' fill='none' stroke='#4ade80' "
               f"stroke-width='2'/>"
               f"<text x='{P}' y='{Y(hi) - 6:.1f}' fill='#9cabc9' "
               f"font-size='12'>${hi:,.0f}</text>"
               f"<text x='{P}' y='{Y(lo) + 14:.1f}' fill='#9cabc9' "
               f"font-size='12'>${lo:,.0f}</text>"
               f"<text x='{W - P}' y='{base - 6:.1f}' fill='#9cabc9' "
               f"font-size='12' text-anchor='end'>start "
               f"${sim['capital']:,.0f}</text></svg>")
    gate_rows = []
    pnl_by_day = {c["date"]: c for c in curve}
    prev_eq = None
    for g in gates:
        c = pnl_by_day.get(g["date"])
        day_pnl = ""
        if c and prev_eq:
            day_pnl = f"{100 * (c['equity'] - prev_eq) / prev_eq:+.2f}%"
        if c:
            prev_eq = c["equity"]
        dec = g["decision"]
        if dec in ("OPEN", "MOVER"):
            cls = "good"
        elif dec == "IO":
            cls = "io"
        else:
            cls = "bad"
        gate_rows.append(
            f"<tr><th>{g['date']}</th>"
            f"<td>{_html.escape(str(g.get('predict_dir') or '—'))}</td>"
            f"<td>{g.get('predict_score') if g.get('predict_score') is not None else '—'}</td>"
            f"<td>{g.get('spy_down_streak')}</td>"
            f"<td class='{cls}'>{g['decision']}</td>"
            f"<td class='why'>{_html.escape(g['why'])}</td>"
            f"<td class='why'>{_html.escape(g.get('advisory') or '—')}</td>"
            f"<td>{day_pnl}</td></tr>")
    rows = []
    for t in sim["trades"]:
        cls = "good" if (t.get("pnl") or 0) > 0 else "bad"
        book = t.get("source") or t.get("cond_tally") or ""
        rows.append(
            f"<tr><th>{_html.escape(t['entry_dt'])}</th>"
            f"<td>{_html.escape(str(book))}</td>"
            f"<td>{_html.escape(t['ticker'])}</td><td>{t['side']}</td>"
            f"<td>{t['shares']}</td><td>${t['entry_px']:.2f}</td>"
            f"<td>{_html.escape(str(t.get('exit_dt') or ''))}</td>"
            f"<td>${t.get('exit_px') or 0:.2f}</td>"
            f"<td class='{cls}'>${t.get('pnl') or 0:,.2f}</td>"
            f"<td class='{cls}'>{t.get('ret_pct') or 0}%</td>"
            f"<td>{(t.get('conviction') or 0):.1f}</td>"
            f"<td>{_html.escape(str(t.get('cond_tally') or '—'))}</td>"
            f"<td class='why'>{_html.escape(str(t.get('reason') or '—'))}</td></tr>")
    sk = []
    for s in sim["skipped"]:
        sk.append(f"<tr><th>{s['date']}</th>"
                  f"<td>{_html.escape(str(s['ticker']))}</td>"
                  f"<td>{s['side']}</td><td>{(s.get('conviction') or 0):.1f}</td>"
                  f"<td class='why'>{_html.escape(str(s.get('reason')))}</td></tr>")
    sw = []
    for i, r in enumerate(sweep[:20], 1):
        sw.append(f"<tr><td>{i}</td><td>{r['side']}</td><td>{r['filter']}</td>"
                  f"<td>{r['rank']}</td><td>{r['topn']}</td><td>{r['entry']}</td>"
                  f"<td>{r['hold']}</td><td>{r['gate']}</td>"
                  f"<td><b>{r['trim_pct']}</b></td><td>{r['raw_pct']}</td>"
                  f"<td>{r['hit']}</td><td>{r['trades']}</td></tr>")
    bs = st["by_side"]
    by_src = sim.get("by_source") or {}
    mv_pnl = (by_src.get("mover") or {}).get("pnl")
    io_pnl = (by_src.get("io") or {}).get("pnl")
    extra_cards = ""
    fallback_note = ""
    if sim.get("io_fallback"):
        extra_cards = (
            f"<div class='card'>Mover P&amp;L<b>"
            f"${0 if mv_pnl is None else mv_pnl:,.0f}</b></div>"
            f"<div class='card'>.io P&amp;L<b>"
            f"${0 if io_pnl is None else io_pnl:,.0f}</b></div>"
        )
        fallback_note = (
            "<p class='muted'>Rule: S ≥ +1 or missing → mover 1d at 09:30. "
            "S &lt; +1 (including hard-red) → that day’s live .io "
            "<code>2w_size</code> mark, the book that was already on. "
            "S ≤ −3 blocks new 1d tickets; it does not sit in cash while "
            "2w_size is green. A new 1d .io ticket at yesterday’s close "
            "marks ~0 and is not yesterday’s win.</p>"
        )
    gs = ("skip days → live .io 2w_size; mover the rest" if sim.get("io_fallback")
          else ("off" if gate_score is None else f"score ≥ {gate_score}"))
    title = TITLE
    if sim.get("io_fallback"):
        title = "Mover paper — skip days defer to live .io 2w_size"
    HTML_OUT.write_text(f"""<!doctype html>
<html><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>{title}</title>
<style>
:root{{--bg:#0b1020;--card:#131b31;--line:#2b3552;--text:#edf2ff;--muted:#9cabc9}}
*{{box-sizing:border-box}}body{{margin:0;background:var(--bg);color:var(--text);font:15px/1.45 system-ui}}
main{{max-width:1240px;margin:auto;padding:16px}}h1,h2{{margin:.4em 0}}
.muted{{color:var(--muted)}}
.cards{{display:grid;grid-template-columns:repeat(auto-fit,minmax(140px,1fr));gap:10px;margin:14px 0}}
.card{{background:var(--card);border:1px solid var(--line);border-radius:12px;padding:12px}}
.card b{{display:block;font-size:22px;margin-top:4px}}
.sheet{{overflow-x:auto;border:1px solid var(--line);border-radius:12px;margin:14px 0}}
table{{border-collapse:separate;border-spacing:0;width:100%;background:var(--card)}}
th,td{{padding:7px 8px;text-align:center;border-bottom:1px solid var(--line);white-space:nowrap}}
thead th{{position:sticky;top:0;background:#17213a}}
tbody th{{background:#17213a;text-align:left}}
td.good{{color:#4ade80}}td.bad{{color:#f87171}}td.io{{color:#60a5fa}}
td.why{{text-align:left;white-space:normal;max-width:320px;font-size:12px}}
</style></head><body><main>
<h1>{title}</h1>
<p class="muted">{'LONG-only' if sim['side'] == 'long' else 'LONG+SHORT'} ·
top {sim['top_n']}/day by {sim['rank']} · entry {sim['entry']} ·
hold {sim['hold']} · {sim['pct']:.0%} equity/trade · {gs} ·
Futubull fees · cash-accounted.
<a href="../sleeve-combine/" style="color:#93c5fd">sleeve combine</a> ·
<a href="../" style="color:#93c5fd">.io paper</a></p>
<div class="cards">
<div class="card">Final equity<b>${st['final_equity']:,.0f}</b></div>
<div class="card">Return<b>{st['total_ret_pct']}%</b></div>
<div class="card">Max drawdown<b>{st['max_dd_pct']}%</b></div>
<div class="card">Trades<b>{st['n_trades']}</b></div>
<div class="card">Skipped<b>{st['n_skipped']}</b></div>
<div class="card">Win rate<b>{round(100 * (st['hit'] or 0), 1)}%</b></div>
<div class="card">BUY P&amp;L<b>${bs['BUY']['pnl']:,.0f}</b></div>
<div class="card">SELL P&amp;L<b>${bs['SELL']['pnl']:,.0f}</b></div>
{extra_cards}
</div>
{fallback_note}
{svg}
<h2>Day book (mover / .io 2w_size)</h2>
<div class="sheet"><table>
<thead><tr><th>Date</th><th>Predict</th><th>Score</th><th>SPY streak</th>
<th>Book</th><th>Why</th><th>Advisory</th><th>Day P&amp;L</th></tr></thead>
<tbody>{''.join(gate_rows)}</tbody></table></div>
<h2>Filled trades (ET timestamps)</h2>
<div class="sheet"><table>
<thead><tr><th>Entry</th><th>Book</th><th>Ticker</th><th>Side</th><th>Shares</th>
<th>Entry px</th><th>Exit</th><th>Exit px</th><th>P&amp;L</th><th>Ret</th>
<th>Conviction</th><th>Cond</th><th>Why</th></tr></thead>
<tbody>{''.join(rows)}</tbody></table></div>
<h2>Strategy sweep (trimmed compound — anti-lottery)</h2>
<div class="sheet"><table>
<thead><tr><th>#</th><th>Side</th><th>Filter</th><th>Rank</th><th>N</th>
<th>Entry</th><th>Hold</th><th>Gate</th><th>Trim %</th><th>Raw %</th>
<th>Hit</th><th>Trades</th></tr></thead>
<tbody>{''.join(sw)}</tbody></table></div>
<h2>Skipped calls</h2>
<div class="sheet"><table>
<thead><tr><th>Date</th><th>Ticker</th><th>Side</th><th>Conviction</th>
<th>Why skipped</th></tr></thead>
<tbody>{''.join(sk)}</tbody></table></div>
</main></body></html>""", encoding="utf-8")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--payload", default=str(PAYLOAD))
    ap.add_argument("--source", choices=["mover", "book"], default="mover",
                    help="mover = lookback calls; book = daily stock-book "
                         "picks (entry forced to 16:00 ET close)")
    ap.add_argument("--book-list", choices=list(BOOK_LISTS), default="1d",
                    help="which stock-book horizon list to follow")
    ap.add_argument("--capital", type=float, default=100_000.0)
    ap.add_argument("--top-n", type=int, default=10)
    ap.add_argument("--pct", type=float, default=0.10)
    ap.add_argument("--side", choices=["long", "both"], default="long")
    ap.add_argument("--entry", choices=["open", "close"], default=None,
                    help="default: open for mover, close for book (leak "
                         "guard)")
    ap.add_argument("--hold", choices=["eod", "1d", "3d", "1w"], default=None,
                    help="default: 1d for mover, 1w for book (best "
                         "cash-accounted sim)")
    ap.add_argument("--rank", choices=["cond", "conviction", "dip", "book"],
                    default=None)
    ap.add_argument("--gate-score", default="1.0",
                    help="'none' disables the day gate")
    ap.add_argument("--io-fallback", dest="io_fallback", action="store_true",
                    default=True,
                    help="skip-day live .io 2w_size mark (default on)")
    ap.add_argument("--no-io-fallback", dest="io_fallback",
                    action="store_false")
    args = ap.parse_args()
    entry = args.entry or ("close" if args.source == "book" else "open")
    hold = args.hold or ("1w" if args.source == "book" else "1d")
    rank = args.rank or ("book" if args.source == "book" else "cond")
    if rank == "dip" and entry != "close":
        raise SystemExit("[mover-paper] dip rank needs the 16:00 print — "
                         "use --entry close (leak guard)")
    if args.source == "book" and entry != "close":
        raise SystemExit("[mover-paper] the stock book prints ~13:00-15:45 "
                         "ET — a 09:30 entry would be a forward leak. "
                         "Use --entry close.")
    gate_score = None if args.gate_score == "none" else float(args.gate_score)
    payload = load_payload(Path(args.payload))
    _set_out_paths(args.source)
    if args.source == "book":
        calls = book_calls(payload, args.book_list, args.side)
        gates = gate_table(payload, gate_score,
                           dates=sorted(set(r["date"] for r in calls)))
        sweep_fn = lambda: run_book_sweep(calls, payload, gate_score)
        sim = run_sim(calls, gates, capital=args.capital, top_n=args.top_n,
                      pct=args.pct, side=args.side, entry=entry,
                      hold=hold, rank=rank)
        sim["source"] = args.source
        sim["book_list"] = args.book_list
    elif args.io_fallback:
        from src.sleeve_combine_bt import run_one_live
        raw = run_one_live("1d", "mover_only", "size",
                           args.capital, args.top_n, args.pct)
        sim, gates = stitch_skip_io(raw, payload)
        sweep_fn = lambda: run_sweep(payload, 0.0)
        print(f"[mover-paper] skip-day .io {IO_SKIP_SLEEVE} stitch · "
              f"{sum(1 for g in gates if g['decision']=='MOVER')} mover / "
              f"{sum(1 for g in gates if g['decision']=='IO')} io")
    else:
        calls = tradeable_calls(payload, args.side)
        gates = gate_table(payload, gate_score)
        sweep_fn = lambda: run_sweep(payload, gate_score)
        print(f"[mover-paper] source={args.source} · {len(calls)} calls · "
              f"side={args.side} entry={entry} hold={hold} "
              f"rank={rank} gate={gate_score} · "
              f"{sum(1 for g in gates if g['decision'] == 'OPEN')}/{len(gates)} days open")
        sim = run_sim(calls, gates, capital=args.capital, top_n=args.top_n,
                      pct=args.pct, side=args.side, entry=entry,
                      hold=hold, rank=rank)
        sim["source"] = args.source
        sim["book_list"] = args.book_list
    st = stats(sim)
    try:
        sweep = sweep_fn()
    except Exception as e:  # noqa: BLE001 — page still writes without the sweep
        print(f"[mover-paper] sweep skipped: {e}")
        sweep = []
    write_outputs(sim, st, gates, sweep, payload, gate_score)
    print(f"[mover-paper] {st['n_trades']} trades, {st['n_skipped']} skipped, "
          f"equity ${st['final_equity']:,.0f} ({st['total_ret_pct']}%), "
          f"hit {st['hit']}, maxDD {st['max_dd_pct']}%")


if __name__ == "__main__":
    main()
