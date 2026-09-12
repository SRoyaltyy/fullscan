"""Open-vs-close entry grading — research only, no live wire.

Same pick sets (stock-book 1d BUY + factor-mine ``union_hot_n4_h1``)
graded on the clocks the scoreboards actually mix:

  * c2c   close → next close     (STOCK_BOOK_BACKTEST convention)
  * o2nc  09:30 open → next close
  * o2c   09:30 open → same-day close  (hot-4 hold=1 signal clock)
  * o2o   09:30 open → next open       (hot-4 cash-book fill)

Fees: 15 bp round-trip (excel_bot FEE_RT) and ``paper_trade.order_fees``
on a $2,500 leftover slice. Official 09:30 / 16:00 from the local OHLC
store only — no yfinance, no live flatten.

CLI: python -m src.open_close_grade [--write]
"""
from __future__ import annotations

import argparse
import json
import math
import statistics
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

ROOT = Path(__file__).resolve().parent.parent
BOOK_DIR = ROOT / "data" / "stock_book"
PANEL_PATH = ROOT / "data" / "factor_mine" / "panel.json"
OHLC_PATH = ROOT / "data" / "prices" / "ohlc.parquet"
FEES_PATH = ROOT / "00_grounding" / "futubull_fees.json"
OUT_MD = ROOT / "03_scoreboard" / "OPEN_CLOSE_GRADE.md"
OUT_JSON = ROOT / "03_scoreboard" / "open_close_grade.json"

# Same 15 bp drag excel_bot/engine/join_post_813.py uses on name-day boards.
FEE_RT = 0.0015
SLICE_USD = 2500.0  # hot-4 leftover split on a $10k / 4-name book
TOP10 = 10
TZ = ZoneInfo("America/New_York")
CASE_DATE = "2026-09-02"
CASE_TICKER = "CVS"

CLOCKS = {
    "c2c": {
        "id": "c2c",
        "label": "close → next close",
        "entry": "close",
        "exit": "next_close",
        "note": "Current stock-book backtest convention.",
    },
    "o2nc": {
        "id": "o2nc",
        "label": "09:30 open → next close",
        "entry": "open",
        "exit": "next_close",
        "note": "Open fill, one session hop to the next 16:00.",
    },
    "o2c": {
        "id": "o2c",
        "label": "09:30 open → same-day close",
        "entry": "open",
        "exit": "same_close",
        "note": "Hot-4 hold=1 signal clock (open→close that morning).",
    },
    "o2o": {
        "id": "o2o",
        "label": "09:30 open → next 09:30 open",
        "entry": "open",
        "exit": "next_open",
        "note": "Hot-4 cash-book fill (list-drop sells the next open).",
    },
}
CLOCK_ORDER = ("c2c", "o2nc", "o2c", "o2o")

HOT4_REC = {
    "name": "union_hot_n4_h1",
    "universe": "union",
    "hold": 1,
    "top_n": 4,
    "rank": "hot_score",
    "forbid": {"alarm": True},
}


def _tick(v) -> str:
    return str(v or "").strip().upper()


def _finite(x):
    if x is None:
        return None
    try:
        v = float(x)
    except (TypeError, ValueError):
        return None
    if math.isnan(v) or math.isinf(v):
        return None
    return v


def next_session(cal: list[str], date: str) -> str | None:
    if date not in cal:
        return None
    i = cal.index(date)
    if i + 1 >= len(cal):
        return None
    return cal[i + 1]


def land_bucket(signal_date: str, generated_at: str | None) -> str:
    """When the printed book was knowable vs the 09:30 / 16:00 clocks."""
    if not generated_at:
        return "unknown"
    raw = str(generated_at).replace("Z", "+00:00")
    try:
        dt = datetime.fromisoformat(raw)
    except ValueError:
        return "unknown"
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=TZ)
    et = dt.astimezone(TZ)
    gdate = et.date().isoformat()
    minutes = et.hour * 60 + et.minute
    if gdate > signal_date:
        return "after_close"
    if gdate < signal_date:
        return "preopen"
    if minutes < 9 * 60 + 30:
        return "preopen"
    if minutes >= 16 * 60:
        return "after_close"
    return "session"


def load_fees(path: Path = FEES_PATH) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def order_fees(shares: int, price: float, side: str, fees: dict) -> float:
    """Delegate to paper_trade so the schedule stays in one place."""
    from .paper_trade import order_fees as _order_fees
    return _order_fees(shares, price, side, fees)


def fee_drag_order(entry: float, exit_px: float, fees: dict,
                   slice_usd: float = SLICE_USD) -> float | None:
    """Round-trip fee as a fraction of buy notional on a leftover slice."""
    if not entry or entry <= 0 or not exit_px or exit_px <= 0:
        return None
    shares = int(slice_usd // entry)
    if shares < 1:
        return None
    total = (order_fees(shares, entry, "buy", fees)
             + order_fees(shares, exit_px, "sell", fees))
    return total / (shares * entry)


def clock_legs(bar0: dict, bar1: dict | None, clock: str):
    """(entry_px, exit_px) for one clock. bar1 is the next session."""
    spec = CLOCKS[clock]
    entry_key = spec["entry"]
    exit_name = spec["exit"]
    entry = _finite((bar0 or {}).get(entry_key))
    if exit_name == "same_close":
        exit_px = _finite((bar0 or {}).get("close"))
    elif exit_name == "next_close":
        exit_px = _finite((bar1 or {}).get("close")) if bar1 else None
    elif exit_name == "next_open":
        exit_px = _finite((bar1 or {}).get("open")) if bar1 else None
    else:
        raise ValueError(clock)
    return entry, exit_px


def clock_return(bar0: dict, bar1: dict | None, clock: str) -> float | None:
    """Gross fraction return. None if a leg is missing."""
    entry, exit_px = clock_legs(bar0, bar1, clock)
    if entry is None or exit_px is None or entry == 0:
        return None
    return exit_px / entry - 1.0


def pick_hot4(rows: list[dict], rec: dict | None = None) -> list[dict]:
    """union_hot_n4_h1: union list, drop 🚨, top 4 by hot_score.

    Same gates as ``factor_mine.pick_day`` for this recipe — kept local so
    the research module does not import the full mine.
    """
    rec = rec or HOT4_REC
    uni = rec.get("universe") or "union"
    top_n = int(rec.get("top_n") or 4)
    kept = []
    for row in rows:
        srcs = set(row.get("sources") or [])
        if uni != "union" and uni not in srcs:
            continue
        if (rec.get("forbid") or {}).get("alarm") and row.get("alarm"):
            continue
        kept.append(row)
    kept.sort(key=lambda r: (
        -(_finite(r.get("ohlc_hot_score")) or 0.0),
        _tick(r.get("ticker")),
    ))
    return kept[:top_n]


def load_book_buys(path: Path, top_n: int | None = None) -> dict:
    data = json.loads(path.read_text(encoding="utf-8"))
    meta = data.get("meta") or {}
    signal = str(meta.get("date") or path.name.replace("_stock_book.json", ""))[:10]
    rows = list(((data.get("books") or {}).get("1d") or {}).get("buy") or [])
    if top_n is not None:
        rows = rows[:top_n]
    tickers = [_tick(r.get("ticker")) for r in rows if _tick(r.get("ticker"))]
    return {
        "date": signal,
        "generated_at": meta.get("generated_at"),
        "land": land_bucket(signal, meta.get("generated_at")),
        "general_bias": _finite(meta.get("general_bias")),
        "tickers": tickers,
        "rows": rows,
    }


def load_all_book_days(book_dir: Path = BOOK_DIR,
                       top_n: int | None = None) -> list[dict]:
    out = []
    for path in sorted(book_dir.glob("*_stock_book.json")):
        day = load_book_buys(path, top_n=top_n)
        if day["tickers"]:
            out.append(day)
    return out


def load_panel(path: Path = PANEL_PATH) -> dict:
    raw = json.loads(path.read_text(encoding="utf-8"))
    by_date: dict[str, list] = raw.get("by_date") or {}
    if not by_date:
        for row in raw.get("rows") or []:
            by_date.setdefault(row["date"], []).append(row)
        raw = dict(raw)
        raw["by_date"] = by_date
    return raw


def load_hot4_days(panel: dict | None = None) -> list[dict]:
    panel = panel or load_panel()
    by_date = panel.get("by_date") or {}
    days = []
    for date in panel.get("session_dates") or sorted(by_date):
        chosen = pick_hot4(by_date.get(date) or [])
        days.append({
            "date": date,
            "generated_at": "09:30 ET",
            "land": "preopen",
            "tickers": [_tick(r.get("ticker")) for r in chosen],
            "rows": chosen,
        })
    return days


def load_bar_store(path: Path = OHLC_PATH) -> dict:
    """(date, ticker) → {open, close} from the official tape. No network."""
    if not path.exists():
        return {}
    import pandas as pd
    df = pd.read_parquet(path)
    if df.empty:
        return {}
    out = {}
    for rec in df.to_dict("records"):
        d = str(rec.get("date") or "")[:10]
        t = _tick(rec.get("ticker"))
        o = _finite(rec.get("open"))
        c = _finite(rec.get("close"))
        if d and t:
            out[(d, t)] = {"open": o, "close": c}
    return out


def bar_from_store(store: dict, ticker: str, date: str) -> dict:
    return store.get((date, _tick(ticker))) or {"open": None, "close": None}


def grade_picks(days: list[dict], cal: list[str], store: dict,
                fees: dict, clocks: tuple[str, ...] = CLOCK_ORDER) -> dict:
    """Equal-weight name-day stats per clock, raw + after fees."""
    per_clock = {k: {"rets": [], "fee_rt": [], "fee_ord": [],
                     "rows": [], "day_means": []} for k in clocks}
    for day in days:
        date = day["date"]
        if date not in cal:
            continue
        nxt = next_session(cal, date)
        day_rets = {k: [] for k in clocks}
        for t in day["tickers"]:
            bar0 = bar_from_store(store, t, date)
            bar1 = bar_from_store(store, t, nxt) if nxt else None
            for clock in clocks:
                ret = clock_return(bar0, bar1, clock)
                if ret is None:
                    continue
                entry, exit_px = clock_legs(bar0, bar1, clock)
                drag = fee_drag_order(entry, exit_px, fees)
                row = {
                    "date": date,
                    "ticker": t,
                    "clock": clock,
                    "ret": round(ret, 6),
                    "hit": ret > 0,
                    "ret_fee_rt": round(ret - FEE_RT, 6),
                    "hit_fee_rt": (ret - FEE_RT) > 0,
                    "fee_order": None if drag is None else round(drag, 6),
                    "ret_fee_ord": None if drag is None else round(ret - drag, 6),
                    "hit_fee_ord": None if drag is None else (ret - drag) > 0,
                    "land": day.get("land"),
                }
                per_clock[clock]["rets"].append(ret)
                per_clock[clock]["fee_rt"].append(ret - FEE_RT)
                if drag is not None:
                    per_clock[clock]["fee_ord"].append(ret - drag)
                per_clock[clock]["rows"].append(row)
                day_rets[clock].append(ret)
        for clock in clocks:
            xs = day_rets[clock]
            if xs:
                per_clock[clock]["day_means"].append(
                    {"date": date, "mean": sum(xs) / len(xs),
                     "n": len(xs), "hits": sum(1 for r in xs if r > 0)}
                )
    return {k: _pack_clock(v) for k, v in per_clock.items()}


def _pack_clock(block: dict) -> dict:
    rets = block["rets"]
    fee_rt = block["fee_rt"]
    fee_ord = block["fee_ord"]
    days = block["day_means"]

    def _stats(xs):
        if not xs:
            return {"n": 0, "hits": 0, "hit_rate": None, "mean": None,
                    "median": None}
        hits = sum(1 for x in xs if x > 0)
        return {
            "n": len(xs),
            "hits": hits,
            "hit_rate": round(hits / len(xs), 4),
            "mean": round(sum(xs) / len(xs), 6),
            "median": round(statistics.median(xs), 6),
        }

    day_hits = sum(1 for d in days if d["mean"] > 0)
    return {
        "raw": _stats(rets),
        "fee_rt": _stats(fee_rt),
        "fee_order": _stats(fee_ord),
        "day_mean_hit_rate": None if not days else round(day_hits / len(days), 4),
        "n_days": len(days),
        "rows": block["rows"],
        "days": days,
    }


def grade_case(date: str, tickers: list[str], cal: list[str], store: dict,
               book_day: dict | None = None) -> dict:
    nxt = next_session(cal, date)
    rows = []
    for t in tickers:
        bar0 = bar_from_store(store, t, date)
        bar1 = bar_from_store(store, t, nxt) if nxt else None
        rec = {"ticker": t}
        for clock in CLOCK_ORDER:
            rec[clock] = clock_return(bar0, bar1, clock)
        rec["open"] = _finite((bar0 or {}).get("open"))
        rec["close"] = _finite((bar0 or {}).get("close"))
        rec["next_close"] = _finite((bar1 or {}).get("close")) if bar1 else None
        rec["next_open"] = _finite((bar1 or {}).get("open")) if bar1 else None
        if book_day:
            match = next(
                (r for r in book_day.get("rows") or [] if _tick(r.get("ticker")) == t),
                None,
            )
            if match:
                rec["change_pct_at_print"] = _finite(match.get("change_pct"))
                rec["gap_pct_at_print"] = _finite(match.get("gap_pct"))
        rows.append(rec)
    def _hits(key):
        xs = [r[key] for r in rows if r.get(key) is not None]
        return {
            "n": len(xs),
            "hits": sum(1 for x in xs if x > 0),
            "hit_rate": None if not xs else round(sum(1 for x in xs if x > 0) / len(xs), 4),
        }
    already_green = [
        r for r in rows
        if r.get("change_pct_at_print") is not None and r["change_pct_at_print"] > 0
    ]
    return {
        "date": date,
        "next": nxt,
        "n": len(rows),
        "rows": rows,
        "by_clock": {k: _hits(k) for k in CLOCK_ORDER},
        "already_green_at_print": len(already_green),
        "land": None if not book_day else book_day.get("land"),
        "generated_at": None if not book_day else book_day.get("generated_at"),
        "general_bias": None if not book_day else book_day.get("general_bias"),
    }


def flip_board(book: dict, hot4: dict) -> dict:
    """Does open-entry reverse the book-vs-hot-4 story?"""
    cells = {}
    for clock in CLOCK_ORDER:
        b = (book.get(clock) or {}).get("raw") or {}
        h = (hot4.get(clock) or {}).get("raw") or {}
        bhr = b.get("hit_rate")
        hhr = h.get("hit_rate")
        if bhr is None or hhr is None:
            winner = None
        elif hhr > bhr + 1e-9:
            winner = "hot4"
        elif bhr > hhr + 1e-9:
            winner = "book"
        else:
            winner = "tie"
        bm, hm = b.get("mean"), h.get("mean")
        if bm is None or hm is None:
            mean_winner = None
        elif hm > bm + 1e-12:
            mean_winner = "hot4"
        elif bm > hm + 1e-12:
            mean_winner = "book"
        else:
            mean_winner = "tie"
        cells[clock] = {
            "book_hit": bhr,
            "hot4_hit": hhr,
            "book_mean": bm,
            "hot4_mean": hm,
            "winner": winner,
            "mean_winner": mean_winner,
            "delta_pp": None if bhr is None or hhr is None
            else round(100.0 * (hhr - bhr), 2),
        }
    c2c_w = cells["c2c"]["winner"]
    open_w = cells["o2c"]["winner"]
    flipped = bool(c2c_w and open_w and c2c_w != open_w and "tie" not in (c2c_w, open_w))
    mean_flipped = bool(
        cells["c2c"]["mean_winner"] and cells["o2c"]["mean_winner"]
        and cells["c2c"]["mean_winner"] != cells["o2c"]["mean_winner"]
        and "tie" not in (cells["c2c"]["mean_winner"], cells["o2c"]["mean_winner"])
    )
    book_crosses_half = (
        cells["c2c"]["book_hit"] is not None
        and cells["o2c"]["book_hit"] is not None
        and (cells["c2c"]["book_hit"] - 0.5) * (cells["o2c"]["book_hit"] - 0.5) < 0
    )
    return {
        "cells": cells,
        "c2c_winner": c2c_w,
        "o2c_winner": open_w,
        "ranking_flips": flipped,
        "mean_flips": mean_flipped,
        "book_crosses_50": book_crosses_half,
        "verdict": _verdict(flipped, mean_flipped, book_crosses_half, cells),
    }


def _verdict(flipped: bool, mean_flipped: bool, book_crosses: bool,
             cells: dict) -> str:
    c2c = cells["c2c"]
    o2c = cells["o2c"]
    if flipped:
        return (
            f"YES — hit-rate ranking flips. On close→next close {c2c['winner']} "
            f"leads ({_pct(c2c['book_hit'])} book vs {_pct(c2c['hot4_hit'])} hot-4). "
            f"On open→same close {o2c['winner']} leads "
            f"({_pct(o2c['book_hit'])} book vs {_pct(o2c['hot4_hit'])} hot-4)."
        )
    extra = ""
    if book_crosses:
        extra = (
            f" Book hit-rate does cross 50% "
            f"({_pct(c2c['book_hit'])} c2c → {_pct(o2c['book_hit'])} o2c), "
            "but that is the afternoon-book leak, not a rescue of the 09:30 sleeve."
        )
    mean_line = (
        f" Mean ranking {'also flips' if mean_flipped else 'does not flip'} "
        f"(c2c {c2c['mean_winner']} wins, book {_mean_pct(c2c['book_mean'])} vs "
        f"hot-4 {_mean_pct(c2c['hot4_mean'])}; o2c {o2c['mean_winner']} wins, "
        f"book {_mean_pct(o2c['book_mean'])} vs hot-4 {_mean_pct(o2c['hot4_mean'])})."
    )
    return (
        f"NO — open-entry does not flip who wins on hit-rate. "
        f"c2c winner={c2c['winner']} ({_pct(c2c['book_hit'])} vs {_pct(c2c['hot4_hit'])}); "
        f"o2c winner={o2c['winner']} ({_pct(o2c['book_hit'])} vs {_pct(o2c['hot4_hit'])})."
        + extra + mean_line
    )


def _pct(x) -> str:
    if x is None:
        return "n/a"
    return f"{100.0 * x:.1f}%"


def _mean_pct(x) -> str:
    if x is None:
        return "n/a"
    return f"{100.0 * x:+.2f}%"


def recommend(book_days: list[dict], flip: dict) -> dict:
    lands = defaultdict(int)
    for d in book_days:
        lands[d.get("land") or "unknown"] += 1
    n = len(book_days) or 1
    session_share = lands.get("session", 0) / n
    preopen_share = lands.get("preopen", 0) / n
    return {
        "book_1d": {
            "default": "c2c",
            "why": (
                "Printed books land at mixed clocks "
                f"({lands.get('preopen', 0)} preopen / "
                f"{lands.get('session', 0)} session / "
                f"{lands.get('after_close', 0)} after close). "
                "Most 1d BUY lists are afternoon or overnight prints, so the "
                "honest 1d hop is close→next close (or next 09:30 if you "
                "refuse a same-day last). Same-day open→close is not a fill "
                "the afternoon book had — it grades a move already on the sheet."
            ),
            "not_default": "o2c",
            "land_counts": dict(lands),
            "session_share": round(session_share, 3),
            "preopen_share": round(preopen_share, 3),
        },
        "hot4": {
            "default": "o2c",
            "alt_fill": "o2o",
            "why": (
                "union_hot_n4_h1 is a 09:30 list: hold=1 signal-only grades "
                "open→same close; the cash book buys the open and sells the "
                "next open. close→next close bills it for an overnight gap "
                "it never paid and is the wrong scoreboard default."
            ),
        },
        "do_not": (
            "Do not put both sleeves on one c2c board and call that a "
            "fair hot-4 vs book bake-off."
        ),
        "flip": flip.get("verdict"),
    }


def _fmt_cell(stats: dict, key: str = "raw") -> str:
    block = (stats or {}).get(key) or {}
    if not block.get("n"):
        return "n/a"
    return (
        f"{block['hits']}/{block['n']} ({_pct(block['hit_rate'])}) "
        f"μ {_mean_pct(block['mean'])}"
    )


def render_md(payload: dict) -> str:
    book = payload["book_top10"]
    book_all = payload["book_all"]
    hot4 = payload["hot4"]
    flip = payload["flip"]
    rec = payload["recommend"]
    case = payload["case_9_2"]
    lands = rec["book_1d"]["land_counts"]
    L = [
        "# Open vs close entry grading",
        "",
        f"_Generated {payload['generated_at']} · research only · no live wire._",
        "",
        "Same pick sets, three (plus one) clocks, Futubull fees. "
        "Official 09:30 / 16:00 from `data/prices/ohlc.parquet` — not yfinance, "
        "not a Finviz last-trade.",
        "",
        "## Clocks",
        "",
        "| id | What | Who already uses it |",
        "|----|------|---------------------|",
        "| `c2c` | close → next close | `STOCK_BOOK_BACKTEST` / paper book |",
        "| `o2nc` | 09:30 open → next close | open fill, 1-session hop |",
        "| `o2c` | 09:30 open → same-day close | hot-4 hold=1 **signal** clock |",
        "| `o2o` | 09:30 open → next 09:30 open | hot-4 **cash-book** fill |",
        "",
        "After-fee columns: **15 bp** round-trip (`excel_bot` `FEE_RT`) and "
        "the real `paper_trade.order_fees` helper on a $2,500 leftover slice.",
        "",
        "## Plain board — does open-entry flip hot-4 vs book?",
        "",
        f"**{flip['verdict']}**",
        "",
        "| Clock | book 1d top-10 | book 1d all BUY | hot-4 `union_hot_n4_h1` | winner (top-10 vs hot-4) |",
        "|-------|----------------|-----------------|-------------------------|--------------------------|",
    ]
    for clock in CLOCK_ORDER:
        L.append(
            f"| {CLOCKS[clock]['label']} | {_fmt_cell(book[clock])} | "
            f"{_fmt_cell(book_all[clock])} | {_fmt_cell(hot4[clock])} | "
            f"{flip['cells'][clock]['winner'] or 'n/a'} "
            f"({flip['cells'][clock]['delta_pp']:+.1f} pp hot-4)"
            f" |"
        )
    L += [
        "",
        "### After 15 bp Futubull (name-day)",
        "",
        "| Clock | book top-10 | hot-4 |",
        "|-------|-------------|-------|",
    ]
    for clock in CLOCK_ORDER:
        L.append(
            f"| {CLOCKS[clock]['label']} | {_fmt_cell(book[clock], 'fee_rt')} | "
            f"{_fmt_cell(hot4[clock], 'fee_rt')} |"
        )
    L += [
        "",
        "### After `order_fees` on a $2,500 slice",
        "",
        "| Clock | book top-10 | hot-4 |",
        "|-------|-------------|-------|",
    ]
    for clock in CLOCK_ORDER:
        L.append(
            f"| {CLOCKS[clock]['label']} | {_fmt_cell(book[clock], 'fee_order')} | "
            f"{_fmt_cell(hot4[clock], 'fee_order')} |"
        )
    L += [
        "",
        f"Books graded: **{payload['n_book_days']}** · hot-4 sessions: "
        f"**{payload['n_hot4_days']}** · calendar: "
        f"{payload['cal'][0]} → {payload['cal'][-1]} ({len(payload['cal'])} sessions).",
        "",
        "Morning-landed books only (knowable before 09:30 — the only book "
        "slice that is allowed an open fill): "
        f"`o2c` {_fmt_cell((payload.get('book_preopen') or {}).get('o2c'))} · "
        f"`c2c` {_fmt_cell((payload.get('book_preopen') or {}).get('c2c'))} "
        f"({payload.get('n_book_preopen_days') or 0} days, small n).",
        "",
        "Book land times (when the JSON was printed vs that date's 09:30 / 16:00): "
        f"**{lands.get('preopen', 0)}** preopen · "
        f"**{lands.get('session', 0)}** during the session · "
        f"**{lands.get('after_close', 0)}** after close / next morning.",
        "",
        "## 9/2 CVS case",
        "",
    ]
    gen = case.get("generated_at") or "?"
    bias = case.get("general_bias")
    bias_s = "n/a" if bias is None else f"{bias:+.2f} (down)" if bias < 0 else f"{bias:+.2f}"
    c2c = case["by_clock"]["c2c"]
    o2c = case["by_clock"]["o2c"]
    L += [
        f"Book printed **{gen}** · land=`{case.get('land')}` · "
        f"general bias **{bias_s}** · CVS was #1 1d BUY on a HARD_RED lattice.",
        "",
        f"Top {case['n']} 1d BUY names:",
        "",
        f"- close→next close ({case.get('next')}): "
        f"**{c2c['hits']}/{c2c['n']}** green ({_pct(c2c['hit_rate'])})",
        f"- open→same close (09:30→16:00 on {CASE_DATE}): "
        f"**{o2c['hits']}/{o2c['n']}** green ({_pct(o2c['hit_rate'])})",
        f"- already green on the printed sheet (`change_pct` > 0): "
        f"**{case.get('already_green_at_print')}** / {case['n']}",
        "",
        "Same-day open→close is **not** a fill this book had. The file landed "
        "at 15:43 ET with Change% already on the row (CVS +3.93% / gap +1.15%). "
        "Every top-10 name was already green on the sheet.",
    ]
    cvs = next((r for r in case["rows"] if r.get("ticker") == CASE_TICKER), None)
    if cvs and cvs.get("o2c") is not None:
        L[-1] += (
            f" CVS itself was {'green' if cvs['o2c'] > 0 else 'red'} "
            f"open→close ({100.0 * cvs['o2c']:+.2f}%) — the "
            f"{o2c['hits']}/{o2c['n']} is the basket, not the #1 name."
        )
    L += [
        f"close→next close is the 1d hop after that print "
        f"({c2c['hits']}/{c2c['n']} green).",
        "",
        "| # | Ticker | o2c | c2c | change% at print | gap% |",
        "|---|--------|-----|-----|------------------|------|",
    ]
    for i, r in enumerate(case["rows"], 1):
        def cell(v):
            if v is None:
                return "—"
            return f"{100.0 * v:+.2f}%"
        chg = r.get("change_pct_at_print")
        gap = r.get("gap_pct_at_print")
        chg_s = "—" if chg is None else f"{chg:+.2f}%"
        gap_s = "—" if gap is None else f"{gap:+.2f}%"
        mark = " ← CVS" if r["ticker"] == CASE_TICKER else ""
        L.append(
            f"| {i} | `{r['ticker']}`{mark} | {cell(r.get('o2c'))} | "
            f"{cell(r.get('c2c'))} | {chg_s} | {gap_s} |"
        )
    L += [
        "",
        "## Scoreboard default (research recommendation)",
        "",
        f"- **Stock-book 1d BUY → `{rec['book_1d']['default']}`.** {rec['book_1d']['why']}",
        f"- **Hot-4 / `union_hot_n4_h1` → `{rec['hot4']['default']}`** "
        f"(cash blotter `{rec['hot4']['alt_fill']}`). {rec['hot4']['why']}",
        f"- {rec['do_not']}",
        "",
        "Live `flatten_robust` is not changed. This file is a clock audit, not a wire.",
        "",
        "## Method",
        "",
        "- Picks are frozen: book = printed `books.1d.buy`; hot-4 = panel rows "
        "that pass `union_hot_n4_h1` (union, no 🚨, top 4 by `ohlc_hot_score`).",
        "- Hot-4 **list** still prints on hard-red sit mornings (cash book buys nobody). "
        "Those names are graded so the clock comparison is the shopping list, not leftover cash.",
        "- Missing open/close or a missing next session drops that name-day for that clock.",
        "- Hit = return > 0. After-fee hit = return − drag > 0.",
        "- No network. Labor Day 2026-09-07 is not a session.",
    ]
    return "\n".join(L) + "\n"


def build(top_n: int = TOP10) -> dict:
    fees = load_fees()
    store = load_bar_store()
    panel = load_panel()
    cal = list(panel.get("session_dates") or [])
    if not cal:
        cal = sorted({d for d, _ in store})
    book_all_days = [d for d in load_all_book_days(top_n=None) if d["date"] in cal]
    book_top_days = [d for d in load_all_book_days(top_n=top_n) if d["date"] in cal]
    hot4_days = load_hot4_days(panel)
    book_all = grade_picks(book_all_days, cal, store, fees)
    book_top = grade_picks(book_top_days, cal, store, fees)
    book_am = grade_picks(
        [d for d in book_top_days if d.get("land") == "preopen"],
        cal, store, fees,
    )
    hot4 = grade_picks(hot4_days, cal, store, fees)
    case_book = next((d for d in book_top_days if d["date"] == CASE_DATE), None)
    case = grade_case(
        CASE_DATE,
        (case_book or {}).get("tickers") or [],
        cal, store, case_book,
    )
    flip = flip_board(book_top, hot4)
    rec = recommend(book_top_days, flip)
    now = datetime.now(TZ).isoformat()
    def _slim(block):
        return {k: {kk: vv for kk, vv in v.items() if kk != "rows"}
                for k, v in block.items()}
    return {
        "generated_at": now,
        "asof": "official_ohlc",
        "live_wire": False,
        "fee_rt": FEE_RT,
        "slice_usd": SLICE_USD,
        "cal": cal,
        "n_book_days": len(book_top_days),
        "n_hot4_days": sum(1 for d in hot4_days if d["tickers"]),
        "n_book_preopen_days": sum(1 for d in book_top_days if d.get("land") == "preopen"),
        "book_top10": _slim(book_top),
        "book_all": _slim(book_all),
        "book_preopen": _slim(book_am),
        "hot4": _slim(hot4),
        "flip": flip,
        "recommend": rec,
        "case_9_2": case,
        "book_lands": [
            {"date": d["date"], "generated_at": d.get("generated_at"),
             "land": d.get("land"), "n": len(d["tickers"])}
            for d in book_top_days
        ],
        "hot4_days": [
            {"date": d["date"], "tickers": d["tickers"]} for d in hot4_days
        ],
    }


def run(write: bool = True) -> dict:
    payload = build()
    text = render_md(payload)
    if write:
        OUT_MD.parent.mkdir(parents=True, exist_ok=True)
        OUT_MD.write_text(text, encoding="utf-8")
        OUT_JSON.write_text(json.dumps(payload, indent=2), encoding="utf-8")
        print(f"[open-close-grade] wrote {OUT_MD}")
        print(f"[open-close-grade] wrote {OUT_JSON}")
    else:
        print(text)
    return payload


def main(argv: list[str] | None = None) -> None:
    ap = argparse.ArgumentParser(description="Open vs close entry grading")
    ap.add_argument("--write", action="store_true", default=True)
    ap.add_argument("--no-write", action="store_false", dest="write")
    args = ap.parse_args(argv)
    run(write=args.write)


if __name__ == "__main__":
    main()
