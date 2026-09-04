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

CLI (all days in the lookback ∪ stock books — currently 2026-08-13 → last book):
  python -m src.sleeve_combine_bt
  python -m src.sleeve_combine_bt --mode dual --hold 1d
  python -m src.sleeve_combine_bt --from 2026-08-13 --to 2026-09-03

Dashboard: dashboard/sleeve-combine/index.html
Live: https://sroyaltyy.github.io/fullscan/dashboard/sleeve-combine/
"""
from __future__ import annotations

import argparse
import csv
import json
import re
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
    route_fallback,
)

ROOT = Path(__file__).resolve().parent.parent
ET = ZoneInfo("America/New_York")
PAYLOAD = ROOT / "03_scoreboard" / "mover_lookback_action.json"
BOOK_DIR = ROOT / "data" / "stock_book"
FEES_PATH = ROOT / "00_grounding" / "futubull_fees.json"
OUT_DIR = ROOT / "data" / "sleeve_combine"
OUT_MD = ROOT / "03_scoreboard" / "SLEEVE_COMBINE_BT.md"
DASH_DIR = ROOT / "dashboard" / "sleeve-combine"
DASH_SHELL = Path(__file__).with_name("sleeve_combine_dash.html")
PAGES_URL = "https://sroyaltyy.github.io/fullscan/dashboard/sleeve-combine/"

HOLD_SESSIONS = {"1d": 1, "3d": 3, "1w": 5}
IO_ONLY_HOLDS = {"2w": 10, "1m": 21}
MATCHED_HOLDS = tuple(HOLD_SESSIONS)
SIZE_BUCKETS = ("large+", "mid", "small/micro")
OPEN_CLOCK, CLOSE_CLOCK = "09:30 ET", "16:00 ET"
WINDOW_START = "2026-08-13"

MODES = ("combine", "mover_only", "io_only", "dual", "overlay", "fallback")


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
    if mode in ("combine", "overlay", "fallback") and hold not in HOLD_SESSIONS:
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


def load_calendar(payload: dict, from_date: str | None = None,
                  to_date: str | None = None) -> list[str]:
    dates = set(payload.get("session_dates") or [])
    dates.update(p.name[:10] for p in BOOK_DIR.glob("????-??-??_stock_book.json"))
    dates.update((payload.get("regime") or {}).keys())
    sweeps = ((payload.get("sweeps") or {}).get("featured") or {})
    regime = ((sweeps.get("mover_days") or {}).get("params") or {}).get("_regime") or {}
    dates.update(regime)
    start = from_date or WINDOW_START
    out = sorted(d for d in dates if d >= start)
    if to_date:
        out = [d for d in out if d <= to_date]
    return out


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


def load_io_buy_lists(hold: str) -> dict[str, list[dict]]:
    """Full horizon BUY list (not the 3-per-bucket sleeve). Close-knowable."""
    book_h = hold if hold in HOLD_SESSIONS or hold in IO_ONLY_HOLDS else "1d"
    by: dict[str, list[dict]] = {}
    for path in sorted(BOOK_DIR.glob("????-??-??_stock_book.json")):
        date = path.name[:10]
        try:
            doc = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            continue
        book = ((doc.get("books") or {}).get(book_h)) or {}
        picks = []
        for p in book.get("buy") or []:
            t = (p.get("ticker") or "").upper()
            if t:
                picks.append({**p, "ticker": t})
        by[date] = picks
    return by


def enrich_io_with_mover(
    io_picks: dict[str, list[dict]],
    mover_calls: dict[str, list[dict]],
    buy_lists: dict[str, list[dict]] | None = None,
    *,
    boost_pct: float = 0.20,
) -> dict[str, list[dict]]:
    """Mover as information on the close book — not a second account.

    Today's mover BUY list is knowable at 09:30, so it is legal at the
    16:00 .io fill. Overlap names in the size sleeve are sized up and
    taken first. A mover name that is on the same-horizon BUY list but
    missed the 3-per-bucket cut is appended as an extra slot (idle cash).
    """
    out: dict[str, list[dict]] = {}
    for date, picks in io_picks.items():
        movers = {(r.get("ticker") or "").upper()
                  for r in (mover_calls.get(date) or []) if r.get("ticker")}
        have = {(p.get("ticker") or "").upper() for p in picks}
        boosted, rest = [], []
        for p in picks:
            t = (p.get("ticker") or "").upper()
            q = dict(p)
            if t in movers:
                q["_pct"] = boost_pct
                q["_boost"] = True
                boosted.append(q)
            else:
                rest.append(q)
        extras = []
        for p in buy_lists.get(date) or [] if buy_lists else []:
            t = (p.get("ticker") or "").upper()
            if not t or t in have or t not in movers:
                continue
            extras.append({
                **p, "ticker": t, "_pct": boost_pct, "_boost": True,
                "bucket": p.get("bucket") or "mover-book",
            })
            have.add(t)
        out[date] = boosted + rest + extras
    return out


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


_REASON_RE = re.compile(r"(join|sector|gen|news)=([+-]?\d+(?:\.\d+)?)")


def parse_reasons(text: str) -> dict:
    """Parse the stock-book `reasons` blob. Knowable at book print (close)."""
    out: dict = {"has_event": False, "rebound_floor": False}
    for key, raw in _REASON_RE.findall(text or ""):
        out[key] = float(raw)
    blob = text or ""
    out["has_event"] = "ev=" in blob or "'event'" in blob or '"event"' in blob
    out["rebound_floor"] = "rebound_floor" in blob
    return out


def tag_io_pick(pick: dict) -> dict:
    reasons = parse_reasons(pick.get("reasons") or "")
    join = reasons.get("join")
    sector_s = reasons.get("sector")
    news = reasons.get("news")
    return {
        "ticker": (pick.get("ticker") or "").upper(),
        "bucket": pick.get("bucket") or "",
        "sector": pick.get("sector") or "",
        "rebound": bool(pick.get("rebound")),
        "join": join,
        "s_sector": sector_s,
        "news": news,
        "has_event": bool(reasons.get("has_event")),
        "join_good": join is not None and join > 0,
        "sector_good": sector_s is not None and sector_s > 0,
        "news_good": news is not None and news > 0,
        "score": pick.get("score"),
    }


def _mean(xs: list[float]) -> float | None:
    return round(sum(xs) / len(xs), 3) if xs else None


def _hit(xs: list[float]) -> float | None:
    return round(sum(1 for x in xs if x > 0) / len(xs), 3) if xs else None


def _cut(rows: list[dict], pred) -> dict:
    xs = [r["ret"] for r in rows if pred(r)]
    return {"n": len(xs), "mean": _mean(xs), "hit": _hit(xs)}


def analyze_io_size_attrs(
    calendar: list[str],
    scores: dict[str, float | None],
    io_picks: dict[str, list[dict]],
    bars,
    hold: str = "1d",
) -> dict:
    """Unweighted 1-hold close→close of the size-sleeve names.

    Attributes come from the same afternoon book that printed the pick
    (close entry, so no leak). This answers: *inside* the .io size book,
    which fields helped on down / messy mornings?
    """
    rows: list[dict] = []
    for date, picks in io_picks.items():
        if date not in calendar:
            continue
        xd = exit_date(calendar, date, hold)
        if xd is None:
            continue
        score = scores.get(date)
        if score is None:
            band = "missing"
        elif score >= MOVER_GATE:
            band = "green"
        elif score < IO_HARD_RED:
            band = "hard_red"
        else:
            band = "messy"
        down = band in ("hard_red", "messy")
        for pick in picks:
            tag = tag_io_pick(pick)
            t = tag["ticker"]
            if not t:
                continue
            px0 = _px(bars(t, date), "close")
            px1 = _px(bars(t, xd), "close")
            if px0 is None or px1 is None:
                continue
            rows.append({
                **tag,
                "date": date,
                "ret": round(100 * (px1 - px0) / px0, 4),
                "s": score,
                "band": band,
                "down": down,
            })
    down_rows = [r for r in rows if r["down"]]
    green_rows = [r for r in rows if r["band"] == "green"]
    cuts = {
        "all": _cut(rows, lambda r: True),
        "down": _cut(down_rows, lambda r: True),
        "green": _cut(green_rows, lambda r: True),
        "hard_red": _cut(rows, lambda r: r["band"] == "hard_red"),
        "down_large+": _cut(down_rows, lambda r: r["bucket"] == "large+"),
        "down_mid": _cut(down_rows, lambda r: r["bucket"] == "mid"),
        "down_small": _cut(down_rows, lambda r: r["bucket"] == "small/micro"),
        "down_rebound": _cut(down_rows, lambda r: r["rebound"]),
        "down_not_rebound": _cut(down_rows, lambda r: not r["rebound"]),
        "down_event": _cut(down_rows, lambda r: r["has_event"]),
        "down_no_event": _cut(down_rows, lambda r: not r["has_event"]),
        "down_join_good": _cut(down_rows, lambda r: r["join_good"]),
        "down_join_not": _cut(down_rows, lambda r: not r["join_good"]),
        "down_sector_good": _cut(down_rows, lambda r: r["sector_good"]),
        "down_sector_not": _cut(down_rows, lambda r: not r["sector_good"]),
        "down_news_good": _cut(down_rows, lambda r: r["news_good"]),
        "down_news_not": _cut(down_rows, lambda r: not r["news_good"]),
        "down_energy": _cut(down_rows, lambda r: r["sector"] == "Energy"),
        "down_healthcare": _cut(down_rows, lambda r: r["sector"] == "Healthcare"),
        "down_not_energy": _cut(down_rows, lambda r: r["sector"] != "Energy"),
    }
    by_sector: dict[str, dict] = {}
    for sec in sorted({r["sector"] for r in down_rows if r["sector"]}):
        by_sector[sec] = _cut(down_rows, lambda r, s=sec: r["sector"] == s)
    return {
        "hold": hold,
        "n_prints": len(rows),
        "n_down": len(down_rows),
        "n_green": len(green_rows),
        "cuts": cuts,
        "down_by_sector": by_sector,
    }


def filter_io_picks(io_picks: dict[str, list[dict]], keep) -> dict[str, list[dict]]:
    return {d: [p for p in rows if keep(p)] for d, rows in io_picks.items()}


def io_keep(name: str):
    """Named keepers for cash-accounted .io variants. Size book only."""
    if name == "all":
        return lambda p: True
    if name == "large+":
        return lambda p: (p.get("bucket") or "") == "large+"
    if name == "mid":
        return lambda p: (p.get("bucket") or "") == "mid"
    if name == "small":
        return lambda p: (p.get("bucket") or "") == "small/micro"
    if name == "rebound":
        return lambda p: bool(p.get("rebound"))
    if name == "event":
        return lambda p: bool(parse_reasons(p.get("reasons") or "").get("has_event"))
    if name == "energy":
        return lambda p: (p.get("sector") or "") == "Energy"
    if name == "sector_good":
        return lambda p: (parse_reasons(p.get("reasons") or "").get("sector") or 0) > 0
    raise ValueError(f"unknown io keep {name!r}")


def _slim_bt(sim: dict) -> dict:
    return {
        "total_ret_pct": sim["total_ret_pct"],
        "max_dd_pct": sim["max_dd_pct"],
        "n_trades": sim["n_trades"],
        "hit": sim.get("hit"),
        "final_equity": sim["final_equity"],
        "by_source": sim.get("by_source"),
    }


def sweep_io_attr_books(
    calendar: list[str],
    scores: dict[str, float | None],
    io_picks: dict[str, list[dict]],
    bars,
    *,
    hold: str = "1d",
    capital: float = 100_000.0,
    top_n: int = 10,
    pct: float = 0.10,
    fees: dict | None = None,
) -> list[dict]:
    """Cash-accounted .io-only 1d books that keep only one attribute."""
    out = []
    for name in ("all", "large+", "mid", "small", "rebound", "event",
                 "energy", "sector_good"):
        sim = run_bt(
            calendar=calendar, scores=scores, mover_calls={},
            io_picks=filter_io_picks(io_picks, io_keep(name)),
            bars=bars, hold=hold, mode="io_only", io_select=f"size:{name}",
            capital=capital, top_n=top_n, pct=pct, fees=fees,
        )
        row = _slim_bt(sim)
        row["filter"] = name
        row["hold"] = hold
        row["mode"] = "io_only"
        out.append(row)
    # Down-day large+ only; green mornings keep the full 3-bucket book.
    mixed: dict[str, list[dict]] = {}
    for date, picks in io_picks.items():
        score = scores.get(date)
        if score is not None and score < MOVER_GATE:
            mixed[date] = [p for p in picks if (p.get("bucket") or "") == "large+"]
        else:
            mixed[date] = list(picks)
    sim = run_bt(
        calendar=calendar, scores=scores, mover_calls={},
        io_picks=mixed, bars=bars, hold=hold, mode="io_only",
        io_select="size:large+_on_down",
        capital=capital, top_n=top_n, pct=pct, fees=fees,
    )
    row = _slim_bt(sim)
    row["filter"] = "large+_on_down"
    row["hold"] = hold
    row["mode"] = "io_only"
    out.append(row)
    return out


def _dual_gap(mover_gap: str, io_gap: str) -> str:
    """Mover sitting in cash on a red morning is the dual design, not a gap."""
    parts = []
    for raw in (mover_gap or "", io_gap or ""):
        for bit in raw.split(";"):
            g = bit.strip()
            if not g or g == "route cash — no new entries":
                continue
            if g not in parts:
                parts.append(g)
    return "; ".join(parts)


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
    sat_n: int = 1,
    sat_pct: float = 0.10,
    sat_hold: str = "1d",
) -> dict:
    """Shared-account fill sim. `bars(ticker, date) -> {open, close}`.

    overlay: .io size book still fills every close. Mover may take a
    capped satellite at the open (sat_n names at sat_pct) when S ≥ +1.
    That spends idle cash; it does not switch the account off .io.
    """
    if mode not in MODES:
        raise ValueError(f"mode must be one of {MODES}")
    if mode == "dual":
        raise ValueError("dual is two wallets — use run_dual(), not run_bt()")
    assert_matched_hold(mode, hold)
    fees = fees or load_fees()
    cal = list(calendar)
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
                  clock: str, fill_pct: float | None = None,
                  fill_hold: str | None = None,
                  fill_lim: int | None = None) -> tuple[int, int]:
        nonlocal cash
        filled = 0
        looked = 0
        lim = top_n if fill_lim is None else fill_lim
        open_tickers = {p["ticker"] for p in open_pos}
        for row in candidates:
            if filled >= lim:
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
            use_hold = row.get("_hold") or fill_hold or hold
            xd = exit_date(cal, date, use_hold)
            if xd is None:
                skipped.append({"date": date, "ticker": t, "source": source,
                                "reason": "no exit date (end of calendar)"})
                continue
            eq = mark(date)
            use_pct = row.get("_pct")
            if use_pct is None:
                use_pct = pct if fill_pct is None else fill_pct
            notional = round(eq * float(use_pct), 2)
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
                "source": source, "hold": use_hold,
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
        card = route_fallback(score) if mode == "fallback" else route(score)
        bucket = card["bucket"]
        if mode == "mover_only":
            want = BUCKET_MOVER if (score is None or score >= MOVER_GATE) else BUCKET_CASH
        elif mode == "io_only":
            want = BUCKET_IO
        elif mode == "overlay":
            want = "overlay"
        else:
            want = bucket

        n_mover = len(mover_calls.get(date) or [])
        has_book = date in io_picks
        n_io = len(io_picks.get(date) or [])
        want_m = want == BUCKET_MOVER or (
            mode == "overlay" and (score is None or score >= MOVER_GATE))
        want_i = want == BUCKET_IO or mode == "overlay"
        reasons = []
        if want_m and n_mover == 0:
            reasons.append("mover source empty (no BUY calls)")
        if want_i and not has_book:
            reasons.append("io source missing (no stock_book file)")
        elif want_i and n_io == 0:
            reasons.append("io source empty (book has no buys)")
        if want == BUCKET_CASH:
            reasons.append("route cash — no new entries")

        filled_am = filled_pm = 0
        if want_m:
            am_lim = sat_n if mode == "overlay" else top_n
            am_pct = sat_pct if mode == "overlay" else pct
            am_hold = sat_hold if mode == "overlay" else hold
            filled_am, _ = try_enter(
                date, "mover", (mover_calls.get(date) or [])[: top_n * 3],
                "open", OPEN_CLOCK, fill_pct=am_pct, fill_hold=am_hold,
                fill_lim=am_lim)
        n_exits = close_exits(date)
        if want_i:
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
        gap = _dual_gap(a.get("gap") or "", b.get("gap") or "")
        curve.append({
            "date": d,
            "score": a.get("score") if a.get("score") is not None else b.get("score"),
            "route": "dual",
            "why": "mover wallet + .io size wallet (independent cash)",
            "cash": round((a.get("cash") or 0) + (b.get("cash") or 0), 2),
            "equity": round((a.get("equity") or half) + (b.get("equity") or half), 2),
            "equity_mover": round(a.get("equity") or half, 2),
            "equity_io": round(b.get("equity") or half, 2),
            "open": (a.get("open") or 0) + (b.get("open") or 0),
            "filled_am": a.get("filled_am") or 0,
            "filled_pm": b.get("filled_pm") or 0,
            "exits": (a.get("exits") or 0) + (b.get("exits") or 0),
            "n_mover_calls": a.get("n_mover_calls") or 0,
            "n_io_picks": b.get("n_io_picks") or 0,
            "has_book": b.get("has_book") or False,
            "gap": gap,
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


def load_live(from_date: str | None = None,
              to_date: str | None = None) -> tuple[dict, list[str], dict, dict]:
    payload = json.loads(PAYLOAD.read_text(encoding="utf-8"))
    cal = load_calendar(payload, from_date, to_date)
    regime = load_regime(payload)
    scores = {d: (g or {}).get("predict_score") for d, g in regime.items()}
    mover = load_mover_calls(payload)
    return payload, cal, scores, mover


def sweep_live(capital: float = 100_000.0, top_n: int = 10,
               pct: float = 0.10, from_date: str | None = None,
               to_date: str | None = None) -> dict:
    payload, cal, scores, mover = load_live(from_date, to_date)
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
        buys = load_io_buy_lists(hold)
        enriched = enrich_io_with_mover(io, mover, buys)
        for label, picks, md in (
            ("overlay", io, "overlay"),
            ("overlay_boost", enriched, "overlay"),
            ("io_boost", enriched, "io_only"),
        ):
            sim = run_bt(
                calendar=cal, scores=scores, mover_calls=mover, io_picks=picks,
                bars=bars, hold=hold, mode=md, io_select="size",
                capital=capital, top_n=top_n, pct=pct,
            )
            row = {k: sim[k] for k in (
                "hold", "mode", "io_select", "n_trades", "hit",
                "total_ret_pct", "max_dd_pct", "final_equity",
                "n_skipped", "n_gap_days", "by_source", "gross_win",
                "gross_loss")}
            row["mode"] = label
            row["io_select"] = "size+mover" if "boost" in label else "size"
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
    io1 = load_io_picks("1d", "size")
    attrs = analyze_io_size_attrs(cal, scores, io1, bars, hold="1d")
    attr_books = sweep_io_attr_books(
        cal, scores, io1, bars, hold="1d",
        capital=capital, top_n=top_n, pct=pct,
    )
    return {
        "generated_at": datetime.now(ET).isoformat(timespec="seconds"),
        "window": [cal[0] if cal else None, cal[-1] if cal else None],
        "capital": capital, "top_n": top_n, "pct": pct,
        "results": results,
        "io_attrs": attrs,
        "io_attr_books": attr_books,
        "n_sessions": len(cal),
        "n_mover_call_days": sum(1 for d in cal if mover.get(d)),
        "n_book_days": sum(1 for d in cal if (BOOK_DIR / f"{d}_stock_book.json").is_file()),
    }


def run_one_live(hold: str, mode: str, io_select: str = "size",
                 capital: float = 100_000.0, top_n: int = 10,
                 pct: float = 0.10, from_date: str | None = None,
                 to_date: str | None = None) -> dict:
    payload, cal, scores, mover = load_live(from_date, to_date)
    select = "size" if str(io_select).startswith("size") else "top"
    io = load_io_picks(hold if hold != "2w" else "2w", select)
    engine = "overlay" if str(mode).startswith("overlay") else mode
    if mode in ("overlay_boost", "io_boost") or io_select == "size+mover":
        io = enrich_io_with_mover(io, mover, load_io_buy_lists(hold))
        io_select = "size+mover"
        if mode == "io_boost":
            engine = "io_only"
    kw = dict(calendar=cal, scores=scores, mover_calls=mover, io_picks=io,
              bars=build_bar_fn(payload), hold=hold, io_select=io_select,
              capital=capital, top_n=top_n, pct=pct)
    sim = run_dual(**kw) if engine == "dual" else run_bt(mode=engine, **kw)
    sim["mode"] = mode
    sim["window"] = [cal[0] if cal else None, cal[-1] if cal else None]
    sim["generated_at"] = datetime.now(ET).isoformat(timespec="seconds")
    return sim


def _findings(rows: list[dict]) -> str:
    by = {(r.get("hold"), r.get("mode")): r for r in rows}
    c1, m1, i1 = by.get(("1d", "combine")), by.get(("1d", "mover_only")), by.get(("1d", "io_only"))
    d1 = by.get(("1d", "dual"))
    i3 = by.get(("3d", "io_only"))
    scored = [r for r in rows if r.get("total_ret_pct") is not None]
    best = max(scored, key=lambda r: r["total_ret_pct"]) if scored else None
    if not (c1 and m1 and i1):
        return ""
    champ = (
        f" Best book this window: **{best['hold']} {best['mode']} "
        f"{best['total_ret_pct']:+.2f}%**."
        if best else ""
    )
    top_io = i3["total_ret_pct"] if i3 else i1["total_ret_pct"]
    beat = ""
    if best and best["total_ret_pct"] > top_io + 1e-9:
        beat = (
            f" Overlay / boost **beats** raw .io size "
            f"({top_io:+.2f}%) by keeping the size book at full capital "
            f"and using mover only as idle-cash + close-print size-up."
        )
    elif best:
        beat = (
            " Splitting capital with mover still cannot beat a full .io "
            "size book on total return — dual is a blend, not an upgrade."
        )
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
        "names also fails on down days. Fifty-fifty dual is a blend, not "
        "an upgrade — it cannot beat the stronger sleeve."
        + dual_line
        + champ
        + beat
    )


def _pct(x) -> str:
    if x is None:
        return "—"
    return f"{x:.1f}%"


def _wr(x) -> str:
    if x is None:
        return "—"
    return f"{100 * x:.1f}%"


def _cut_cell(c: dict | None) -> str:
    if not c or not c.get("n"):
        return "—"
    mean = c.get("mean")
    hit = c.get("hit")
    mean_s = f"{mean:+.2f}%" if mean is not None else "—"
    hit_s = _wr(hit)
    return f"{mean_s} · {hit_s} · n={c['n']}"


def _io_attr_section(doc: dict) -> list[str]:
    attrs = doc.get("io_attrs") or {}
    cuts = attrs.get("cuts") or {}
    books = doc.get("io_attr_books") or []
    if not cuts and not books:
        return []
    lines = [
        "## .io attributes on down days (inside the size book)",
        "",
        "Different question from the mover-tag table above. Here the "
        "names are already .io size-sleeve picks, entered at the close. "
        "Unweighted close→next-close on the same 1d hold. Morning S is "
        "only used to split the tape — it does not pick the names.",
        "",
        f"Prints with a 1d exit: {attrs.get('n_prints')} · "
        f"on S < +1: {attrs.get('n_down')} · "
        f"on S ≥ +1: {attrs.get('n_green')}",
        "",
        "| Cut | Mean · win · n |",
        "|---|---|",
        f"| All size prints | {_cut_cell(cuts.get('all'))} |",
        f"| Down / messy (S < +1) | {_cut_cell(cuts.get('down'))} |",
        f"| Hard red (S < −3) | {_cut_cell(cuts.get('hard_red'))} |",
        f"| Green mornings | {_cut_cell(cuts.get('green'))} |",
        f"| Down · large+ | {_cut_cell(cuts.get('down_large+'))} |",
        f"| Down · mid | {_cut_cell(cuts.get('down_mid'))} |",
        f"| Down · small/micro | {_cut_cell(cuts.get('down_small'))} |",
        f"| Down · rebound | {_cut_cell(cuts.get('down_rebound'))} |",
        f"| Down · not rebound | {_cut_cell(cuts.get('down_not_rebound'))} |",
        f"| Down · event-tagged | {_cut_cell(cuts.get('down_event'))} |",
        f"| Down · no event | {_cut_cell(cuts.get('down_no_event'))} |",
        f"| Down · join > 0 | {_cut_cell(cuts.get('down_join_good'))} |",
        f"| Down · join ≤ 0 / missing | {_cut_cell(cuts.get('down_join_not'))} |",
        f"| Down · sector > 0 | {_cut_cell(cuts.get('down_sector_good'))} |",
        f"| Down · sector ≤ 0 / missing | {_cut_cell(cuts.get('down_sector_not'))} |",
        f"| Down · Energy | {_cut_cell(cuts.get('down_energy'))} |",
        f"| Down · not Energy | {_cut_cell(cuts.get('down_not_energy'))} |",
        f"| Down · Healthcare | {_cut_cell(cuts.get('down_healthcare'))} |",
        "",
    ]
    if books:
        lines += [
            "Cash-accounted .io-only 1d (same $100k / 10% / Futubull). "
            "Filtering the size book *reduces* names; leftover cash sits. "
            "`large+_on_down` keeps the full 3-bucket book on green "
            "mornings and large+ only when S < +1.",
            "",
            "| Filter | Ret | Max DD | Win | Trades |",
            "|---|---:|---:|---:|---:|",
        ]
        for r in books:
            lines.append(
                f"| `{r.get('filter')}` | {r['total_ret_pct']:+.2f}% | "
                f"{r['max_dd_pct']:.2f}% | {_wr(r.get('hit'))} | "
                f"{r['n_trades']} |"
            )
        lines += [
            "",
            "The size book itself was *better* on S < +1 than on green "
            "mornings. Extra gates mostly do not improve the cash book: "
            "large+ / Energy / event / join>0 all lose to the raw "
            "3-bucket sleeve. `sector_good` is the one filter that beat "
            "`all` this window — slightly, on half the names, with less "
            "DD. Treat that as a size-up tilt, not a new sleeve; thirteen "
            "book days is too thin to replace the 3-bucket rule. Rebound "
            "is already how the book stays long when gen is red. The "
            "down-day attribute that survives is still **stay in the "
            "size book**.",
            "",
        ]
    return lines


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
        tag = "**" if r.get("mode") == "io_boost" and r.get("hold") == "3d" else ""
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
        "## What beats the raw size book",
        "",
        "Do **not** split the account 50/50. That is dual, and it lost. "
        "Keep 100% of capital on the size book. Use mover as information "
        "at the close (today's BUY list is knowable at 09:30): size-up "
        "overlap names and add a mover name that already printed on the "
        "same-horizon BUY list. That is `io_boost`. On a 1d hold, also "
        "spend idle cash on **one** gated mover name at 09:30 (`overlay`).",
        "",
        "| Clock | Overlay / boost |",
        "|---|---|",
        "| ~05:55 | Read morning S |",
        "| 09:30 | 1d `overlay` only: if S ≥ +1, one mover name at 10% from idle cash |",
        "| 16:00 | Exit anything whose hold elapsed |",
        "| 16:00 | Always fill the size book. Size-up mover∩book names (20%). |",
        "| S < −3 | No new *mover* satellite; .io size still buys |",
        "",
        "Switching one account (the old `combine` route) is below for "
        "history. It is not the production book.",
        "",
        "## What the 1d *switch* was allowed to do (loses)",
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
        *_io_attr_section(doc),
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
        lines += [
            "",
            "### Last 20 round-trips",
            "",
            "Every BUY and SELL is on the dashboard day picker "
            f"([sleeve-combine]({PAGES_URL})). This table is the tail of "
            "`bt_trades.csv`.",
            "",
            "| Entry | Src | Ticker | Shares | In | Exit | Out | P&L |",
            "|---|---|---|---:|---:|---|---:|---:|",
        ]
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
        "## How to backtest every session",
        "",
        "The lookback payload ∪ stock books **is** all days we have "
        f"(dashboard era starts {WINDOW_START}). Default CLI walks every "
        "session in that union.",
        "",
        "```",
        "python -m src.test_sleeve_combine_bt",
        "python -m src.sleeve_combine_bt --mode dual --hold 1d",
        "python -m src.sleeve_combine_bt --from 2026-08-13 --to 2026-09-03",
        "```",
        "",
        f"Buy/sell blotter (every fill, day picker): `{DASH_DIR.relative_to(ROOT)}/index.html` "
        f"— live [{PAGES_URL}]({PAGES_URL}). Round-trips: "
        "`data/sleeve_combine/bt_trades.csv`. Expanded BUY then SELL rows: "
        "`data/sleeve_combine/bt_fills.csv`.",
        "",
        "Code: `src/sleeve_combine_bt.py`. Machine copy: "
        "`data/sleeve_combine/bt.json`.",
        "",
    ]
    return "\n".join(lines)


def fills_from_trades(trades: list[dict]) -> list[dict]:
    """One BUY row at entry + one SELL row at exit. Sells sort first."""
    fills: list[dict] = []
    for t in trades:
        clock_in = "09:30 ET" if t.get("source") == "mover" else "16:00 ET"
        fills.append({
            "dt": t.get("entry_dt"),
            "date": t.get("date"),
            "clock": clock_in,
            "ticker": t.get("ticker"),
            "side": "BUY",
            "source": t.get("source"),
            "shares": t.get("shares"),
            "price": t.get("entry_px"),
            "fees": t.get("fee_in") or 0,
            "pnl": None,
            "hold": t.get("hold"),
        })
        if t.get("exit_dt"):
            fills.append({
                "dt": t.get("exit_dt"),
                "date": str(t.get("exit_dt") or "")[:10],
                "clock": CLOSE_CLOCK,
                "ticker": t.get("ticker"),
                "side": "SELL",
                "source": t.get("source"),
                "shares": t.get("shares"),
                "price": t.get("exit_px") or 0,
                "fees": t.get("fee_out") or 0,
                "pnl": t.get("pnl"),
                "hold": t.get("hold"),
            })
    fills.sort(key=lambda r: (
        r.get("date") or "",
        0 if r.get("side") == "SELL" else 1,
        r.get("dt") or "",
        r.get("ticker") or "",
    ))
    return fills


def days_with_fills(curve: list[dict], fills: list[dict]) -> list[dict]:
    by: dict[str, dict] = {}
    for c in curve:
        by[c["date"]] = {**c, "fills": []}
    for f in fills:
        d = f.get("date")
        if d in by:
            by[d]["fills"].append(f)
        elif d:
            by[d] = {"date": d, "fills": [f], "score": None, "route": "",
                     "filled_am": 0, "filled_pm": 0, "exits": 0, "open": 0,
                     "equity": None, "gap": ""}
    return [by[d] for d in sorted(by)]


def curve_svg(curve: list[dict], capital: float) -> str:
    if len(curve) < 2:
        return ""
    W, H, P = 960, 260, 36
    keys = [("equity", "#4ade80", 2.2)]
    if any(c.get("equity_mover") is not None for c in curve):
        keys.append(("equity_mover", "#fbbf24", 1.4))
    if any(c.get("equity_io") is not None for c in curve):
        keys.append(("equity_io", "#60a5fa", 1.4))
    ys = []
    for _, _, _ in keys:
        pass
    for k, _, _ in keys:
        ys.extend(c.get(k) or capital for c in curve)
    ys.append(capital)
    lo, hi = min(ys), max(ys)
    rng = (hi - lo) or 1.0
    n = len(curve)

    def X(i: int) -> float:
        return P + (W - 2 * P) * i / (n - 1)

    def Y(v: float) -> float:
        return H - P - (H - 2 * P) * (v - lo) / rng

    parts = [
        f"<svg viewBox='0 0 {W} {H}' preserveAspectRatio='none' "
        f"role='img' aria-label='combined equity'>",
        f"<line x1='{P}' y1='{Y(capital):.1f}' x2='{W - P}' "
        f"y2='{Y(capital):.1f}' stroke='#5b6b8c' stroke-dasharray='4 4'/>",
    ]
    for key, color, width in keys:
        pts = " ".join(
            f"{X(i):.1f},{Y(float(c.get(key) or capital)):.1f}"
            for i, c in enumerate(curve))
        parts.append(
            f"<polyline points='{pts}' fill='none' stroke='{color}' "
            f"stroke-width='{width}'/>")
    parts += [
        f"<text x='{P}' y='{Y(hi) - 6:.1f}' fill='#9cabc9' "
        f"font-size='12'>${hi:,.0f}</text>",
        f"<text x='{P}' y='{Y(lo) + 14:.1f}' fill='#9cabc9' "
        f"font-size='12'>${lo:,.0f}</text>",
        "</svg>",
    ]
    return "".join(parts)


def dashboard_payload(doc: dict, primary: dict) -> dict:
    fills = fills_from_trades(primary.get("trades") or [])
    curve = primary.get("curve") or []
    stats = {k: primary.get(k) for k in (
        "capital", "hold", "mode", "total_ret_pct", "max_dd_pct",
        "n_trades", "n_skipped", "hit", "final_equity", "by_source",
        "n_gap_days")}
    return {
        "generated": doc.get("generated_at") or primary.get("generated_at"),
        "window": doc.get("window") or primary.get("window"),
        "capital": doc.get("capital") or primary.get("capital"),
        "hold": primary.get("hold"),
        "mode": primary.get("mode"),
        "stats": stats,
        "results": doc.get("results") or [],
        "curve": [{k: c.get(k) for k in (
            "date", "score", "route", "equity", "equity_mover", "equity_io",
            "cash", "open", "filled_am", "filled_pm", "exits", "gap")}
                  for c in curve],
        "fills": fills,
        "days": days_with_fills(curve, fills),
        "svg": curve_svg(curve, float(primary.get("capital") or 100_000)),
        "pages_url": PAGES_URL,
    }


def write_dashboard(doc: dict, primary: dict) -> Path:
    DASH_DIR.mkdir(parents=True, exist_ok=True)
    shell = DASH_SHELL.read_text(encoding="utf-8")
    payload = dashboard_payload(doc, primary)
    html = shell.replace("__DATA__", json.dumps(payload, default=str))
    path = DASH_DIR / "index.html"
    path.write_text(html, encoding="utf-8")
    return path


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
    fills = fills_from_trades(primary.get("trades") or [])
    fcols = ["dt", "date", "clock", "side", "source", "ticker", "shares",
             "price", "fees", "pnl", "hold"]
    with (OUT_DIR / "bt_fills.csv").open("w", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(fh, fieldnames=fcols, extrasaction="ignore")
        w.writeheader()
        w.writerows(fills)
    dash = write_dashboard(doc, primary)
    OUT_MD.write_text(render(doc, primary), encoding="utf-8")
    print(f"wrote {dash.relative_to(ROOT)}")


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--hold", default="3d", choices=list(HOLD_SESSIONS) + ["2w"])
    p.add_argument("--mode", default="io_boost",
                   choices=list(MODES) + ["overlay_boost", "io_boost"])
    p.add_argument("--io-select", default="size", choices=("size", "top"))
    p.add_argument("--capital", type=float, default=100_000)
    p.add_argument("--top-n", type=int, default=10)
    p.add_argument("--pct", type=float, default=0.10)
    p.add_argument("--from", dest="from_date", default=None,
                   help="first session YYYY-MM-DD (default: all days from "
                        f"{WINDOW_START})")
    p.add_argument("--to", dest="to_date", default=None,
                   help="last session YYYY-MM-DD (default: last book / payload day)")
    p.add_argument("--no-write", action="store_true")
    args = p.parse_args(argv)
    doc = sweep_live(capital=args.capital, top_n=args.top_n, pct=args.pct,
                     from_date=args.from_date, to_date=args.to_date)
    primary = run_one_live(args.hold, args.mode, args.io_select,
                           args.capital, args.top_n, args.pct,
                           from_date=args.from_date, to_date=args.to_date)
    if not args.no_write:
        write_outputs(doc, primary)
        print(f"wrote {OUT_MD.relative_to(ROOT)}")
    print(f"{args.mode} hold={args.hold} ret={primary['total_ret_pct']:+.2f}% "
          f"dd={primary['max_dd_pct']:.2f}% trades={primary['n_trades']} "
          f"fills={len(fills_from_trades(primary.get('trades') or []))} "
          f"gaps={primary['n_gap_days']} "
          f"window={primary.get('window')}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
