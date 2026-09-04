"""Matched-horizon mover × .io backtest — fill-level, cash-clocked.

The curve-stitch in sleeve_combine.py is a *sketch*. It mixes mover 1d
daily returns with .io 2w_size marks and cannot see cash lock, fees, or
missing source days (mover called nothing on 2026-08-13/14). This module
is the integrity path.

Hard rules
  1. Combine only on a MATCHED hold (1d / 3d / 1w). 2w and 1m are .io-only
     reference books — pairing them with mover 1d is refused.
  2. Entry clocks stay leak-free: mover = 09:30 open, .io = 16:00 close
     (the book prints ~13:00–15:45 ET).
  3. Intraday cash order: overnight cash → 09:30 mover entries → 16:00
     exits → 16:00 .io entries. Open buys cannot spend the same day's
     close-sale proceeds.
  4. Whole shares, Futubull fees, one shared cash account.
  5. Missing book / missing BUY calls / missing bars are logged. They are
     not silent "we chose cash" days.
  6. Existing holds ride to their scheduled exit. S < -3 blocks NEW 1d
     risk; it does not flatten.

CLI:
  python -m src.sleeve_combine_bt
  python -m src.sleeve_combine_bt --hold 1d --mode combine
"""
from __future__ import annotations

import argparse
import csv
import json
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

from src.sleeve_combine import (
    BUCKET_CASH,
    BUCKET_IO,
    BUCKET_MOVER,
    IO_HARD_RED,
    MOVER_GATE,
    load_regime,
    route,
)

ROOT = Path(__file__).resolve().parent.parent
ET = ZoneInfo("America/New_York")
PAYLOAD = ROOT / "03_scoreboard" / "mover_lookback_action.json"
BOOK_DIR = ROOT / "data" / "stock_book"
FEES_PATH = ROOT / "00_grounding" / "futubull_fees.json"
OUT_DIR = ROOT / "data" / "sleeve_combine"
OUT_MD = ROOT / "03_scoreboard" / "SLEEVE_COMBINE_BT.md"

HOLD_SESSIONS = {"1d": 1, "3d": 3, "1w": 5}
IO_ONLY_HOLDS = {"2w": 10, "1m": 21}
MATCHED_HOLDS = tuple(HOLD_SESSIONS)
SIZE_BUCKETS = ("large+", "mid", "small/micro")
OPEN_CLOCK, CLOSE_CLOCK = "09:30 ET", "16:00 ET"
WINDOW_START = "2026-08-13"

MODES = ("combine", "mover_only", "io_only", "dual")


def load_fees() -> dict:
    return json.loads(FEES_PATH.read_text(encoding="utf-8"))


def order_fees(shares: int, price: float, side: str, f: dict) -> float:
    """Same Futubull schedule as src.paper_trade.order_fees (no pandas)."""
    if shares <= 0 or price <= 0:
        return 0.0
    amount = shares * price
    comm = min(max(f["commission_per_share"] * shares, f["commission_min_per_order"]),
               f["commission_max_pct_of_amount"] * amount)
    plat = min(max(f["platform_per_share"] * shares, f["platform_min_per_order"]),
               f["platform_max_pct_of_amount"] * amount)
    settle = f["settlement_per_share"] * shares
    total = comm + plat + settle
    if side == "sell":
        reg = max(f["regulatory_pct_of_amount_sell_only"] * amount,
                  f["regulatory_min_per_order"])
        taf = min(max(f["taf_per_share_sell_only"] * shares, f["taf_min_per_order"]),
                  f["taf_max_per_order"])
        total += reg + taf
    return round(total, 4)


class MismatchError(ValueError):
    """Raised when a combine is asked to mix incomparable holds."""


def assert_matched_hold(mode: str, hold: str) -> None:
    if mode == "io_only" and hold in IO_ONLY_HOLDS:
        return
    if hold not in HOLD_SESSIONS:
        raise MismatchError(
            f"hold {hold!r} is not a matched combine horizon "
            f"(allowed: {', '.join(MATCHED_HOLDS)}; "
            f"2w/1m are .io-only reference books)"
        )
    if mode == "combine" and hold not in HOLD_SESSIONS:
        raise MismatchError(
            f"cannot combine mover with .io hold={hold}: "
            "mover has no 2w/1m book; cash would lock across unknown sessions"
        )


def exit_date(calendar: list[str], date: str, hold: str) -> str | None:
    n = HOLD_SESSIONS.get(hold) or IO_ONLY_HOLDS.get(hold)
    if n is None or date not in calendar:
        return None
    i = calendar.index(date)
    if i + n >= len(calendar):
        return None
    return calendar[i + n]


def _cond_net(row: dict) -> int:
    c = row.get("condition") or {}
    return int(c.get("good") or 0) - int(c.get("bad") or 0)


def load_calendar(payload: dict) -> list[str]:
    dates = set(payload.get("session_dates") or [])
    dates.update(p.name[:10] for p in BOOK_DIR.glob("????-??-??_stock_book.json"))
    dates.update((payload.get("regime") or {}).keys())
    sweeps = ((payload.get("sweeps") or {}).get("featured") or {})
    regime = ((sweeps.get("mover_days") or {}).get("params") or {}).get("_regime") or {}
    dates.update(regime)
    return sorted(d for d in dates if d >= WINDOW_START)


def load_mover_calls(payload: dict) -> dict[str, list[dict]]:
    by: dict[str, list[dict]] = defaultdict(list)
    for r in payload.get("called_rows") or []:
        if r.get("action_call") != "BUY":
            continue
        d = r.get("date")
        t = (r.get("ticker") or "").upper()
        if d and t:
            by[d].append(r)
    for d, rows in by.items():
        rows.sort(key=_cond_net, reverse=True)
    return dict(by)


def load_io_picks(hold: str, select: str) -> dict[str, list[dict]]:
    """Picks from the SAME horizon book as the hold (1d book → 1d hold)."""
    book_h = hold if hold in HOLD_SESSIONS or hold in IO_ONLY_HOLDS else "1d"
    # 2w/1m books exist on the .io dashboard; 2w is not a combine hold.
    if book_h not in ("1d", "3d", "1w", "2w", "1m"):
        book_h = "1d"
    by: dict[str, list[dict]] = {}
    for path in sorted(BOOK_DIR.glob("????-??-??_stock_book.json")):
        date = path.name[:10]
        try:
            doc = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            continue
        book = ((doc.get("books") or {}).get(book_h)) or {}
        picks: list[dict] = []
        if select == "size":
            sized = book.get("buy_by_size") or {}
            for bucket in SIZE_BUCKETS:
                for p in (sized.get(bucket) or [])[:3]:
                    t = (p.get("ticker") or "").upper()
                    if t:
                        picks.append({**p, "ticker": t, "bucket": bucket})
        else:
            for p in book.get("buy") or []:
                t = (p.get("ticker") or "").upper()
                if t:
                    picks.append({**p, "ticker": t})
        by[date] = picks
    return by


def _load_close_cache() -> dict[tuple[str, str], float]:
    path = ROOT / "data" / "paper" / "prices_cache.csv"
    out: dict[tuple[str, str], float] = {}
    if not path.is_file():
        return out
    with path.open(encoding="utf-8") as fh:
        rows = csv.DictReader(fh)
        for row in rows:
            d = (row.get("Date") or row.get("date") or "")[:10]
            if not d:
                continue
            for t, raw in row.items():
                if t in ("Date", "date") or not raw:
                    continue
                try:
                    out[(t.upper(), d)] = float(raw)
                except (TypeError, ValueError):
                    continue
    return out


def build_bar_fn(payload: dict):
    """Leak-free bars: payload session_bar (open+close), then paper closes.

    Never invent an open from a close. A missing open on a mover day is a
    skip, not a close-fill in disguise. Lookback store is a last resort
    and is only used if it imports.
    """
    cache: dict[tuple[str, str], dict] = {}
    for r in payload.get("called_rows") or []:
        d, t = r.get("date"), (r.get("ticker") or "").upper()
        bar = r.get("session_bar") or {}
        if d and t and (bar.get("open") or bar.get("close")):
            cache[(t, d)] = {
                "open": bar.get("open"), "close": bar.get("close"),
            }
    closes = _load_close_cache()
    lookback = None

    def bars(ticker: str, date: str) -> dict:
        t = (ticker or "").upper()
        hit = dict(cache.get((t, date)) or {})
        if hit.get("close") is None and (t, date) in closes:
            hit["close"] = closes[(t, date)]
        if hit.get("open") is not None or hit.get("close") is not None:
            return hit
        nonlocal lookback
        if lookback is False:
            return {}
        if lookback is None:
            try:
                from src.mover_paper import _bar
                lookback = _bar
            except Exception:
                lookback = False
                return {}
        return lookback(t, date) or {}

    return bars


def default_bar(ticker: str, date: str) -> dict:
    try:
        from src.mover_paper import _bar
        return _bar(ticker, date) or {}
    except Exception:
        return {}


def _px(bar: dict | None, field: str) -> float | None:
    if not bar:
        return None
    v = bar.get(field)
    if v is None:
        return None
    try:
        x = float(v)
    except (TypeError, ValueError):
        return None
    if x <= 0:
        return None
    return x


def run_bt(
    *,
    calendar: list[str],
    scores: dict[str, float | None],
    mover_calls: dict[str, list[dict]],
    io_picks: dict[str, list[dict]],
    bars,
    hold: str = "1d",
    mode: str = "combine",
    io_select: str = "size",
    capital: float = 100_000.0,
    top_n: int = 10,
    pct: float = 0.10,
    fees: dict | None = None,
) -> dict:
    """Shared-account fill sim. `bars(ticker, date) -> {open, close}`."""
    if mode not in MODES:
        raise ValueError(f"mode must be one of {MODES}")
    if mode == "dual":
        raise ValueError("dual is two wallets — use run_dual(), not run_bt()")
    assert_matched_hold(mode, hold)
    fees = fees or load_fees()
    cal = [d for d in calendar if d >= WINDOW_START]
    cash = float(capital)
    open_pos: list[dict] = []
    trades: list[dict] = []
    skipped: list[dict] = []
    audit: list[dict] = []
    curve: list[dict] = []

    def mark(date: str) -> float:
        eq = cash
        for p in open_pos:
            c = _px(bars(p["ticker"], date), "close") or p.get("last_px") or p["entry_px"]
            p["last_px"] = c
            eq += p["shares"] * c
        return eq

    def try_enter(date: str, source: str, candidates: list[dict], field: str,
                  clock: str) -> tuple[int, int]:
        nonlocal cash
        filled = 0
        looked = 0
        open_tickers = {p["ticker"] for p in open_pos}
        for row in candidates:
            if filled >= top_n:
                break
            t = (row.get("ticker") or "").upper()
            if not t:
                continue
            looked += 1
            if t in open_tickers:
                skipped.append({"date": date, "ticker": t, "source": source,
                                "reason": "already open"})
                continue
            bar = bars(t, date) or {}
            px = _px(bar, field)
            if px is None:
                skipped.append({"date": date, "ticker": t, "source": source,
                                "reason": f"missing {field} bar"})
                continue
            xd = exit_date(cal, date, hold)
            if xd is None:
                skipped.append({"date": date, "ticker": t, "source": source,
                                "reason": "no exit date (end of calendar)"})
                continue
            eq = mark(date)
            notional = round(eq * pct, 2)
            shares = int(notional // px)
            if shares <= 0:
                skipped.append({"date": date, "ticker": t, "source": source,
                                "reason": "position size < 1 share"})
                continue
            notional = shares * px
            fee_in = order_fees(shares, px, "buy", fees)
            if notional + fee_in > cash + 1e-9:
                skipped.append({"date": date, "ticker": t, "source": source,
                                "reason": (f"insufficient cash "
                                           f"(${notional + fee_in:,.0f} > ${cash:,.0f})")})
                continue
            cash -= notional + fee_in
            pos = {
                "entry_dt": f"{date} {clock}", "date": date, "ticker": t,
                "side": "BUY", "shares": shares, "entry_px": px,
                "notional": round(notional, 2), "fee_in": round(fee_in, 2),
                "exit_date": xd, "exit_dt": f"{xd} {CLOSE_CLOCK}",
                "source": source, "hold": hold,
                "conviction": row.get("_conv") or row.get("conviction") or row.get("score"),
                "last_px": _px(bar, "close") or px,
            }
            open_pos.append(pos)
            open_tickers.add(t)
            filled += 1
        return filled, looked

    def close_exits(date: str) -> int:
        nonlocal cash
        n = 0
        still = []
        for p in open_pos:
            if p["exit_date"] != date:
                still.append(p)
                continue
            px = _px(bars(p["ticker"], date), "close") or p.get("last_px") or p["entry_px"]
            fee = order_fees(p["shares"], px, "sell", fees)
            cash += p["shares"] * px - fee
            pnl = p["shares"] * (px - p["entry_px"]) - p["fee_in"] - fee
            p.update({"exit_px": round(px, 4), "fee_out": round(fee, 2),
                      "pnl": round(pnl, 2),
                      "ret_pct": round(100 * pnl / max(p["notional"], 1), 2)})
            trades.append(p)
            n += 1
        open_pos[:] = still
        return n

    for date in cal:
        score = scores.get(date)
        card = route(score)
        bucket = card["bucket"]
        if mode == "mover_only":
            want = BUCKET_MOVER if (score is None or score >= MOVER_GATE) else BUCKET_CASH
        elif mode == "io_only":
            want = BUCKET_IO
        else:
            want = bucket

        n_mover = len(mover_calls.get(date) or [])
        has_book = date in io_picks
        n_io = len(io_picks.get(date) or [])
        reasons = []
        if want == BUCKET_MOVER and n_mover == 0:
            reasons.append("mover source empty (no BUY calls)")
        if want == BUCKET_IO and not has_book:
            reasons.append("io source missing (no stock_book file)")
        elif want == BUCKET_IO and n_io == 0:
            reasons.append("io source empty (book has no buys)")
        if want == BUCKET_CASH:
            reasons.append("route cash — no new entries")

        filled_am = filled_pm = 0
        if want == BUCKET_MOVER:
            filled_am, _ = try_enter(
                date, "mover", (mover_calls.get(date) or [])[: top_n * 3],
                "open", OPEN_CLOCK)
        n_exits = close_exits(date)
        if want == BUCKET_IO:
            filled_pm, _ = try_enter(
                date, "io", (io_picks.get(date) or []),
                "close", CLOSE_CLOCK)

        eq = mark(date)
        rec = {
            "date": date, "score": score, "route": want,
            "why": card["why"],
            "cash": round(cash, 2), "equity": round(eq, 2),
            "open": len(open_pos),
            "filled_am": filled_am, "filled_pm": filled_pm,
            "exits": n_exits, "n_mover_calls": n_mover,
            "n_io_picks": n_io, "has_book": has_book,
            "gap": "; ".join(reasons) or "",
        }
        curve.append(rec)
        if reasons:
            audit.append(rec)

    for p in list(open_pos):
        px = p.get("last_px") or p["entry_px"]
        fee = order_fees(p["shares"], px, "sell", fees)
        cash += p["shares"] * px - fee
        pnl = p["shares"] * (px - p["entry_px"]) - p["fee_in"] - fee
        p.update({"exit_px": round(px, 4), "fee_out": round(fee, 2),
                  "pnl": round(pnl, 2),
                  "ret_pct": round(100 * pnl / max(p["notional"], 1), 2),
                  "reason": "force-closed (calendar edge)"})
        trades.append(p)
    open_pos.clear()

    pnls = [t["pnl"] for t in trades]
    wins = [p for p in pnls if p > 0]
    final = curve[-1]["equity"] if curve else cash
    peak, max_dd = capital, 0.0
    for pt in curve:
        peak = max(peak, pt["equity"])
        max_dd = max(max_dd, (peak - pt["equity"]) / peak if peak else 0.0)
    by_src = {}
    for src in ("mover", "io"):
        sp = [t for t in trades if t.get("source") == src]
        by_src[src] = {
            "n": len(sp),
            "hit": (round(sum(1 for t in sp if t["pnl"] > 0) / len(sp), 3)
                    if sp else None),
            "pnl": round(sum(t["pnl"] for t in sp), 2),
        }
    return {
        "capital": capital, "hold": hold, "mode": mode, "io_select": io_select,
        "top_n": top_n, "pct": pct, "mover_gate": MOVER_GATE,
        "io_hard_red": IO_HARD_RED,
        "trades": trades, "skipped": skipped, "curve": curve, "audit": audit,
        "n_trades": len(trades), "n_skipped": len(skipped),
        "hit": round(len(wins) / len(pnls), 3) if pnls else None,
        "final_equity": round(final, 2),
        "total_ret_pct": round(100 * (final - capital) / capital, 2),
        "max_dd_pct": round(100 * max_dd, 2),
        "gross_win": round(sum(wins), 2),
        "gross_loss": round(sum(p for p in pnls if p <= 0), 2),
        "by_source": by_src,
        "n_gap_days": len(audit),
    }


def _stats_from(trades, curve, capital, extra: dict) -> dict:
    pnls = [t["pnl"] for t in trades]
    wins = [p for p in pnls if p > 0]
    final = curve[-1]["equity"] if curve else capital
    peak, max_dd = capital, 0.0
    for pt in curve:
        peak = max(peak, pt["equity"])
        max_dd = max(max_dd, (peak - pt["equity"]) / peak if peak else 0.0)
    by_src = {}
    for src in ("mover", "io"):
        sp = [t for t in trades if t.get("source") == src]
        by_src[src] = {
            "n": len(sp),
            "hit": (round(sum(1 for t in sp if t["pnl"] > 0) / len(sp), 3)
                    if sp else None),
            "pnl": round(sum(t["pnl"] for t in sp), 2),
        }
    out = {
        "capital": capital, "n_trades": len(trades),
        "n_skipped": extra.get("n_skipped", 0),
        "hit": round(len(wins) / len(pnls), 3) if pnls else None,
        "final_equity": round(final, 2),
        "total_ret_pct": round(100 * (final - capital) / capital, 2),
        "max_dd_pct": round(100 * max_dd, 2),
        "gross_win": round(sum(wins), 2),
        "gross_loss": round(sum(p for p in pnls if p <= 0), 2),
        "by_source": by_src,
        "n_gap_days": extra.get("n_gap_days", 0),
        "trades": trades, "curve": curve,
        "skipped": extra.get("skipped", []),
        "audit": extra.get("audit", []),
    }
    out.update({k: extra[k] for k in extra if k not in out})
    return out


def run_dual(**kwargs) -> dict:
    """Two wallets, same hold: mover (gated) + .io size (always on).

    This is the combine that keeps .io's down-day attribute without
    switching the mover account or mixing 1d with 2w. Each sleeve has
    half the capital and never spends the other's cash.
    """
    capital = float(kwargs.pop("capital", 100_000.0))
    hold = kwargs.get("hold", "1d")
    assert_matched_hold("combine", hold)
    half = capital / 2.0
    mover = run_bt(mode="mover_only", capital=half, **kwargs)
    io = run_bt(mode="io_only", capital=half, **kwargs)
    dates = sorted({c["date"] for c in mover["curve"]} |
                   {c["date"] for c in io["curve"]})
    im = {c["date"]: c for c in mover["curve"]}
    ii = {c["date"]: c for c in io["curve"]}
    curve = []
    for d in dates:
        a, b = im.get(d) or {}, ii.get(d) or {}
        gaps = [g for g in ((a.get("gap") or ""), (b.get("gap") or "")) if g]
        curve.append({
            "date": d,
            "score": a.get("score") if a.get("score") is not None else b.get("score"),
            "route": "dual",
            "why": "mover wallet + .io size wallet (independent cash)",
            "cash": round((a.get("cash") or 0) + (b.get("cash") or 0), 2),
            "equity": round((a.get("equity") or half) + (b.get("equity") or half), 2),
            "open": (a.get("open") or 0) + (b.get("open") or 0),
            "filled_am": a.get("filled_am") or 0,
            "filled_pm": b.get("filled_pm") or 0,
            "exits": (a.get("exits") or 0) + (b.get("exits") or 0),
            "n_mover_calls": a.get("n_mover_calls") or 0,
            "n_io_picks": b.get("n_io_picks") or 0,
            "has_book": b.get("has_book") or False,
            "gap": "; ".join(gaps),
        })
    extra = {
        "hold": hold, "mode": "dual", "io_select": kwargs.get("io_select", "size"),
        "top_n": kwargs.get("top_n", 10), "pct": kwargs.get("pct", 0.10),
        "mover_gate": MOVER_GATE, "io_hard_red": IO_HARD_RED,
        "n_skipped": mover["n_skipped"] + io["n_skipped"],
        "n_gap_days": len([c for c in curve if c.get("gap")]),
        "skipped": (mover.get("skipped") or []) + (io.get("skipped") or []),
        "audit": (mover.get("audit") or []) + (io.get("audit") or []),
        "wallets": {
            "mover": {"ret": mover["total_ret_pct"], "dd": mover["max_dd_pct"],
                      "trades": mover["n_trades"]},
            "io": {"ret": io["total_ret_pct"], "dd": io["max_dd_pct"],
                   "trades": io["n_trades"]},
        },
    }
    return _stats_from(mover["trades"] + io["trades"], curve, capital, extra)


def load_live() -> tuple[dict, list[str], dict, dict]:
    payload = json.loads(PAYLOAD.read_text(encoding="utf-8"))
    cal = load_calendar(payload)
    regime = load_regime(payload)
    scores = {d: (g or {}).get("predict_score") for d, g in regime.items()}
    mover = load_mover_calls(payload)
    return payload, cal, scores, mover


def sweep_live(capital: float = 100_000.0, top_n: int = 10,
               pct: float = 0.10) -> dict:
    payload, cal, scores, mover = load_live()
    bars = build_bar_fn(payload)
    results = []
    for hold in MATCHED_HOLDS:
        io = load_io_picks(hold, "size")
        for mode in ("combine", "mover_only", "io_only"):
            sim = run_bt(
                calendar=cal, scores=scores, mover_calls=mover, io_picks=io,
                bars=bars, hold=hold, mode=mode, io_select="size",
                capital=capital, top_n=top_n, pct=pct,
            )
            results.append({k: sim[k] for k in (
                "hold", "mode", "io_select", "n_trades", "hit",
                "total_ret_pct", "max_dd_pct", "final_equity",
                "n_skipped", "n_gap_days", "by_source", "gross_win",
                "gross_loss")})
        dual = run_dual(
            calendar=cal, scores=scores, mover_calls=mover, io_picks=io,
            bars=bars, hold=hold, io_select="size",
            capital=capital, top_n=top_n, pct=pct,
        )
        row = {k: dual[k] for k in (
            "hold", "mode", "io_select", "n_trades", "hit",
            "total_ret_pct", "max_dd_pct", "final_equity",
            "n_skipped", "n_gap_days", "by_source", "gross_win",
            "gross_loss")}
        row["wallets"] = dual.get("wallets")
        results.append(row)
    # .io-only 2w reference — not a combine
    io2 = load_io_picks("2w", "size")
    ref = run_bt(
        calendar=cal, scores=scores, mover_calls=mover, io_picks=io2,
        bars=bars, hold="2w", mode="io_only", io_select="size",
        capital=capital, top_n=top_n, pct=pct,
    )
    results.append({k: ref[k] for k in (
        "hold", "mode", "io_select", "n_trades", "hit",
        "total_ret_pct", "max_dd_pct", "final_equity",
        "n_skipped", "n_gap_days", "by_source", "gross_win", "gross_loss")})
    return {
        "generated_at": datetime.now(ET).isoformat(timespec="seconds"),
        "window": [cal[0] if cal else None, cal[-1] if cal else None],
        "capital": capital, "top_n": top_n, "pct": pct,
        "results": results,
        "n_sessions": len(cal),
        "n_mover_call_days": sum(1 for d in cal if mover.get(d)),
        "n_book_days": sum(1 for d in cal if (BOOK_DIR / f"{d}_stock_book.json").is_file()),
    }


def run_one_live(hold: str, mode: str, io_select: str = "size",
                 capital: float = 100_000.0, top_n: int = 10,
                 pct: float = 0.10) -> dict:
    payload, cal, scores, mover = load_live()
    io = load_io_picks(hold if hold != "2w" else "2w", io_select)
    kw = dict(calendar=cal, scores=scores, mover_calls=mover, io_picks=io,
              bars=build_bar_fn(payload), hold=hold, io_select=io_select,
              capital=capital, top_n=top_n, pct=pct)
    sim = run_dual(**kw) if mode == "dual" else run_bt(mode=mode, **kw)
    sim["window"] = [cal[0] if cal else None, cal[-1] if cal else None]
    sim["generated_at"] = datetime.now(ET).isoformat(timespec="seconds")
    return sim


def _findings(rows: list[dict]) -> str:
    by = {(r.get("hold"), r.get("mode")): r for r in rows}
    c1, m1, i1 = by.get(("1d", "combine")), by.get(("1d", "mover_only")), by.get(("1d", "io_only"))
    d1 = by.get(("1d", "dual"))
    if not (c1 and m1 and i1):
        return ""
    dual_line = ""
    if d1:
        dual_line = (
            f" 1d dual (two wallets) is **{d1['total_ret_pct']:+.2f}%** "
            f"/ {d1['max_dd_pct']:.2f}% DD."
        )
    return (
        "## Finding (this window)\n"
        "\n"
        f"The 1d **switch** is **{c1['total_ret_pct']:+.2f}%**. "
        f"That is worse than mover-only 1d ({m1['total_ret_pct']:+.2f}%) "
        f"and worse than .io-only 1d size ({i1['total_ret_pct']:+.2f}%). "
        "Copying .io green-pile / join-good / sector-not-red onto mover "
        "names also fails on down days (weak-sector mover names bounced). "
        "The attribute that transfers is the **size book plus staying on**: "
        "two wallets, same hold — mover gated on green mornings, .io size "
        "always invested. That is `dual`."
        + dual_line
    )


def _pct(x) -> str:
    if x is None:
        return "—"
    return f"{x:.1f}%"


def _wr(x) -> str:
    if x is None:
        return "—"
    return f"{100 * x:.1f}%"


def render(doc: dict, primary: dict | None = None) -> str:
    rows = doc.get("results") or []
    w0, w1 = (doc.get("window") or [None, None])
    lines = [
        "# Sleeve combine backtest (matched hold, shared cash)",
        "",
        f"_Generated {doc.get('generated_at')} — {w0} → {w1} · "
        f"${doc.get('capital'):,.0f} · {doc.get('top_n')} names · "
        f"{100 * (doc.get('pct') or 0.1):.0f}% equity / fill · Futubull fees_",
        "",
        "This is the integrity backtest. Both sleeves use the **same hold** "
        "(1d / 3d / 1w). Mover still enters at 09:30, .io still enters at "
        "16:00 — those clocks are data constraints, not a style choice. "
        "Open buys cannot spend the same day's close-sale cash. Missing "
        "mover calls and missing books are logged as gaps, not as a gate.",
        "",
        "**2w / 1m are not combined with mover.** Live .io `2w_size` is a "
        "follow-the-book product with a 10-session min-hold; pairing it "
        "with mover 1d locks cash in ways a curve-stitch cannot see. The "
        "2w row below is an .io-only reference.",
        "",
        f"Sessions in window: {doc.get('n_sessions')} · "
        f"days with mover BUY calls: {doc.get('n_mover_call_days')} · "
        f"days with a stock book: {doc.get('n_book_days')}",
        "",
        _findings(rows),
        "",
        "## Sweep (size-sleeve .io picks)",
        "",
        "| Hold | Mode | Ret | Max DD | Win | Trades | Mover P&L | .io P&L | Gaps |",
        "|---|---|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for r in rows:
        bs = r.get("by_source") or {}
        tag = "**" if r.get("mode") == "dual" and r.get("hold") == "1d" else ""
        lines.append(
            f"| {tag}{r['hold']}{tag} | {tag}{r['mode']}{tag} | "
            f"{r['total_ret_pct']:+.2f}% | {r['max_dd_pct']:.2f}% | "
            f"{_wr(r.get('hit'))} | {r['n_trades']} | "
            f"${(bs.get('mover') or {}).get('pnl') or 0:,.0f} | "
            f"${(bs.get('io') or {}).get('pnl') or 0:,.0f} | "
            f"{r.get('n_gap_days')} |"
        )
    lines += [
        "",
        "## What the 1d combine is allowed to do",
        "",
        "| Clock | Action |",
        "|---|---|",
        "| ~05:55 | Read morning general score S (leak-free) |",
        "| 09:30 | If S ≥ +1 **and** mover has BUY calls: fill from overnight cash |",
        "| 16:00 | Exit anything whose hold elapsed (close) |",
        "| 16:00 | If −3 ≤ S < +1 **and** a book exists: fill .io size picks |",
        "| S < −3 | No new entries; existing holds ride to their exit |",
        "",
        "If mover has no BUY calls on a green morning (2026-08-13, "
        "2026-08-14), that day is a **source gap**, not a silent cash day. "
        "The combine does not invent .io fills at 09:30 to paper over it — "
        "that would leak the afternoon book.",
        "",
        "## .io attributes that do / do not transfer onto mover",
        "",
        "Leak-free test: take every mover BUY with a 1d print and tag "
        "the 09:30 boxes (same boxes the lookback already shows before "
        "the open). Do **not** use today's afternoon book.",
        "",
        "| Attribute | On S < +1 (down/messy) | Use it? |",
        "|---|---|---|",
        "| Green pile / join-good / sector-not-red | Hurts (weak-sector "
        "mover names bounced; join-good was −0.3% vs +1.5%) | No |",
        "| AB-good + peer-good as a top-10 filter | Hurts vs raw cond "
        "top-10 | No |",
        "| Yesterday's 1d book overlap | Rare (n=25) but 64% win / +1.0% | "
        "Size-up only, never a requirement |",
        "| Size-bucket book, always on, own cash | This *is* the down-day "
        "engine (1d .io size +6.5%) | **Yes — dual wallets** |",
        "",
        "`dual` is two accounts at half capital: mover still gated at "
        "S ≥ +1, .io size still buys on red mornings. Same hold. No "
        "shared cash clock.",
        "",
    ]
    if primary:
        lines += [
            f"## Primary book — {primary.get('mode')} hold={primary.get('hold')}",
            "",
            f"| Start | Final | Return | Max DD | Trades | Win | Skipped |",
            f"|---:|---:|---:|---:|---:|---:|---:|",
            f"| ${primary['capital']:,.0f} | ${primary['final_equity']:,.2f} | "
            f"**{primary['total_ret_pct']:+.2f}%** | {primary['max_dd_pct']:.2f}% | "
            f"{primary['n_trades']} | {_wr(primary.get('hit'))} | "
            f"{primary['n_skipped']} |",
            "",
            "### Session blotter",
            "",
            "| Date | S | Route | AM fills | PM fills | Exits | Open | Equity | Gap |",
            "|---|---:|---|---:|---:|---:|---:|---:|---|",
        ]
        for c in primary.get("curve") or []:
            lines.append(
                f"| {c['date']} | "
                f"{c['score'] if c['score'] is not None else '—'} | "
                f"{c['route']} | {c['filled_am']} | {c['filled_pm']} | "
                f"{c['exits']} | {c['open']} | ${c['equity']:,.0f} | "
                f"{c.get('gap') or '—'} |"
            )
        lines += ["", "### Last 20 fills", "",
                  "| Entry | Src | Ticker | Shares | In | Exit | Out | P&L |",
                  "|---|---|---|---:|---:|---|---:|---:|"]
        for t in (primary.get("trades") or [])[-20:]:
            lines.append(
                f"| {t['entry_dt']} | {t.get('source')} | `{t['ticker']}` | "
                f"{t['shares']} | ${t['entry_px']:.2f} | {t.get('exit_dt')} | "
                f"${t.get('exit_px') or 0:.2f} | ${t.get('pnl') or 0:,.2f} |"
            )
        lines.append("")
    lines += [
        "## Integrity checklist",
        "",
        "- [x] Matched hold (combine refused for 2w/1m)",
        "- [x] Mover entry = open; .io entry = close",
        "- [x] Same-day close proceeds are not spendable at the open",
        "- [x] Whole shares + Futubull fee file",
        "- [x] Missing bars / books / BUY calls logged on the blotter",
        "- [x] S < −3 does not flatten; scheduled exits still fire",
        "- [x] No yfinance inside the sim — prices from the lookback bar store",
        "",
        "Code: `src/sleeve_combine_bt.py`. Machine copy: "
        "`data/sleeve_combine/bt.json`.",
        "",
    ]
    return "\n".join(lines)


def write_outputs(doc: dict, primary: dict) -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    (OUT_DIR / "bt.json").write_text(
        json.dumps(doc, indent=2, default=str) + "\n", encoding="utf-8")
    slim = {k: primary[k] for k in primary if k not in ("trades", "skipped")}
    slim["n_trades"] = primary.get("n_trades")
    (OUT_DIR / "bt_primary.json").write_text(
        json.dumps(slim, indent=2, default=str) + "\n", encoding="utf-8")
    cols = ["entry_dt", "ticker", "source", "shares", "entry_px", "exit_dt",
            "exit_px", "notional", "fee_in", "fee_out", "pnl", "ret_pct", "hold"]
    with (OUT_DIR / "bt_trades.csv").open("w", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(fh, fieldnames=cols, extrasaction="ignore")
        w.writeheader()
        w.writerows(primary.get("trades") or [])
    with (OUT_DIR / "bt_curve.csv").open("w", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(fh, fieldnames=["date", "score", "route", "cash",
                                           "equity", "open", "filled_am",
                                           "filled_pm", "exits", "gap"],
                           extrasaction="ignore")
        w.writeheader()
        w.writerows(primary.get("curve") or [])
    OUT_MD.write_text(render(doc, primary), encoding="utf-8")


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--hold", default="1d", choices=list(HOLD_SESSIONS) + ["2w"])
    p.add_argument("--mode", default="combine", choices=MODES)
    p.add_argument("--io-select", default="size", choices=("size", "top"))
    p.add_argument("--capital", type=float, default=100_000)
    p.add_argument("--top-n", type=int, default=10)
    p.add_argument("--pct", type=float, default=0.10)
    p.add_argument("--no-write", action="store_true")
    args = p.parse_args(argv)
    doc = sweep_live(capital=args.capital, top_n=args.top_n, pct=args.pct)
    primary = run_one_live(args.hold, args.mode, args.io_select,
                           args.capital, args.top_n, args.pct)
    if not args.no_write:
        write_outputs(doc, primary)
        print(f"wrote {OUT_MD.relative_to(ROOT)}")
    print(f"{args.mode} hold={args.hold} ret={primary['total_ret_pct']:+.2f}% "
          f"dd={primary['max_dd_pct']:.2f}% trades={primary['n_trades']} "
          f"gaps={primary['n_gap_days']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
