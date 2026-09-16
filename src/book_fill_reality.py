"""$10k butterfly fill realities — research only.

Replays the published cash book (same ``pick_day`` lists, leftover
split, sell-first, min-hold, hard-red sit, whole shares, Futubull
fees, short equity ≥ 2× notional) under messy fills.

This is **not** PR 241's 1-share open→same-day-close lab. The ledger
is the real leftover-cash butterfly. A miss or partial on 8-13 changes
every later ticket.

Official OHLC only (``data/prices/ohlc.parquet`` / ``load_bar_store``).
Daily bars have no true tape — trade-through and market path are
documented proxies. Missing official open = no fill. Never invent a
print outside [low, high]. Never substitute close for a missing open.

Live ``flatten_robust``, hard-red sit, and Webull paper are untouched.

CLI: python -m src.book_fill_reality [--write]
"""
from __future__ import annotations

import argparse
import json
import math
import re
import time
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

from . import factor_mine as fm
from . import factor_mine_book as fmb
from . import factor_mine_combo as fmc
from . import open_bell_slip as obs
from . import paper_trade as pt

ROOT = Path(__file__).resolve().parent.parent
OHLC_PATH = ROOT / "data" / "prices" / "ohlc.parquet"
PANEL_PATH = ROOT / "data" / "factor_mine" / "panel.json"
ACTION_MD = ROOT / "03_scoreboard" / "FACTOR_MINE_ACTION.md"
OUT_MD = ROOT / "03_scoreboard" / "BOOK_FILL_REALITY.md"
OUT_JSON = ROOT / "03_scoreboard" / "book_fill_reality.json"
DASH_DIR = ROOT / "dashboard" / "factor-mine"
DASH_JSON = DASH_DIR / "book_fill_reality.json"
DASH_HTML = DASH_DIR / "book-fill-reality.html"
STRAT_JSON = ROOT / "dashboard" / "strategy-board" / "book_fill_reality.json"

ORDER_SENT = "09:30 ET"
TZ = ZoneInfo("America/New_York")
CAPITAL = float(fm.CAPITAL)
GAP_PCT = 0.08

# Same α machinery as PR 241. market_mid is that model.
DELAY_FRAC = obs.DELAY_FRAC

REALITIES = (
    "ideal",
    "limit_prior",
    "limit_open",
    "market_mid",
    "market_adverse",
    "market_favorable",
    "partial_50",
    "gap_miss",
)

# Optimistic published column — not "best possible."
IDEAL_NOTE = (
    "ideal is the published Book% fill (official 09:30 open). "
    "It is the optimistic published column, not a forecast of the "
    "best live fill."
)
FAVORABLE_NOTE = (
    "market_favorable fills at the session favorable extreme "
    "(buy at low / short at high) only when that print exists. "
    "Optimistic / unlikely — not a forecast."
)
ADVERSE_NOTE = (
    "market_adverse fills at the session adverse extreme the bar "
    "printed (buy at high / short at low). Absolute worst still "
    "inside [low, high]. Cannot invent outside the tape."
)
PROXY_NOTE = (
    "Daily OHLC has no true tape. Limit trade-through is "
    "low < limit (buy) / high > limit (short). Market path walks "
    "the printed high/low room. A missing official open is a miss "
    "— close is never substituted."
)

MUST_RUN = (
    "combo_sh_5050_shared",
    "combo_sh_3070_shared",
    "combo_sh_7030_shared",
    "combo_ps_5050_shared",
    "combo_p2s_5050_shared",
    "combo_seh_451540_shared",
    "union_hot_n4_h1",
    "union_news_pack_net2_h1",
    "short_news_r_h3",
    "flatten_h3",  # flatten_would wish-list (research)
)

FEATURED_STARTS = True  # start-date replay for featured / must-run
DEFAULT_FROM = "2026-08-13"
TIME_BUDGET_SEC = 11 * 60

LIVE_UNTOUCHED = (
    "flatten_robust",
    "hard_red_sit",
    "webull_paper",
)

REALITY_LABEL = {
    "ideal": "ideal (published 09:30)",
    "limit_prior": "limit @ prior close",
    "limit_open": "limit @ official open",
    "market_mid": "market mid (PR 241 α)",
    "market_adverse": "market adverse / WORST",
    "market_favorable": "market favorable / BEST (unlikely)",
    "partial_50": "partial 50% of market_mid",
    "gap_miss": "gap ≥8% against → miss",
}

RANGE_KEYS = ("market_adverse", "market_mid", "ideal", "market_favorable")


def _tick(v) -> str:
    return str(v or "").strip().upper()


def _finite(x):
    return obs._finite(x)


def _esc(s) -> str:
    return (str(s or "").replace("&", "&amp;").replace("<", "&lt;")
            .replace(">", "&gt;").replace('"', "&quot;"))


def _pct(v, digits: int = 2) -> str:
    if v is None:
        return "—"
    return f"{float(v):+.{digits}f}%"


def _usd(v) -> str:
    if v is None:
        return "—"
    return f"${float(v):,.2f}"


def order_is_buy(side: str, action: str) -> bool:
    """True when the ticket pays (buy / cover)."""
    long = str(side or "long") != "short"
    if action == "exit":
        return not long
    return long


def fill_side(side: str, action: str) -> str:
    return "long" if order_is_buy(side, action) else "short"


class Tape:
    """Official OHLC lookup + clock-clean prior / α. Never invents."""

    def __init__(self, store: dict | None = None, bars=None):
        self.store = store or {}
        self.bars = bars
        by_t: dict[str, list] = {}
        src = bars if bars is not None else self.store
        for key, rec in (src or {}).items():
            if not isinstance(key, tuple) or len(key) != 2:
                continue
            a, b = key
            if len(str(a)) == 10:
                d, t = str(a)[:10], _tick(b)
            else:
                t, d = _tick(a), str(b)[:10]
            if not d or not t:
                continue
            by_t.setdefault(t, []).append((d, rec or {}))
        for t in by_t:
            by_t[t].sort(key=lambda x: x[0])
        self._by_t = by_t
        self._slip: dict[tuple, dict] = {}

    def bar(self, ticker: str, date: str) -> dict:
        return obs.clock_bar(ticker, date, store=self.store, bars=self.bars)

    def prior_close(self, ticker: str, date: str):
        t = _tick(ticker)
        d = str(date or "")[:10]
        rows = self._by_t.get(t) or []
        prev = None
        for ds, rec in rows:
            if ds >= d:
                break
            prev = _finite((rec or {}).get("close"))
        return prev

    def prior_rows(self, ticker: str, date: str, n: int = obs.PRIOR_N) -> list:
        t = _tick(ticker)
        d = str(date or "")[:10]
        out = []
        for ds, rec in self._by_t.get(t) or []:
            if ds >= d:
                break
            out.append({
                "date": ds,
                "open": _finite((rec or {}).get("open")),
                "high": _finite((rec or {}).get("high")),
                "low": _finite((rec or {}).get("low")),
                "close": _finite((rec or {}).get("close")),
            })
        return out[-int(n or obs.PRIOR_N):]

    def slip(self, ticker: str, date: str) -> dict:
        key = (_tick(ticker), str(date or "")[:10])
        hit = self._slip.get(key)
        if hit is None:
            hit = obs.calibrate_slip(self.prior_rows(key[0], key[1]))
            self._slip[key] = hit
        return hit


def clip_fill(px, bar: dict | None):
    """Never invent a print outside the session [low, high]."""
    if px is None:
        return None
    try:
        fill = float(px)
    except (TypeError, ValueError):
        return None
    if fill <= 0 or math.isnan(fill) or math.isinf(fill):
        return None
    h = _finite((bar or {}).get("high"))
    lo = _finite((bar or {}).get("low"))
    if h is not None:
        fill = min(fill, h)
    if lo is not None:
        fill = max(fill, lo)
    if fill <= 0:
        return None
    return float(fill)


def market_extreme(bar: dict | None, side: str, *, favorable: bool):
    """Fill at a printed extreme. Missing extreme = miss. No invent."""
    o = _finite((bar or {}).get("open"))
    h = _finite((bar or {}).get("high"))
    lo = _finite((bar or {}).get("low"))
    if o is None or o <= 0:
        return None, "no_open"
    long = str(side or "long") != "short"
    if long:
        px = lo if favorable else h
        how = "favorable_low" if favorable else "adverse_high"
    else:
        px = h if favorable else lo
        how = "favorable_high" if favorable else "adverse_low"
    if px is None or px <= 0:
        return None, "no_extreme"
    return float(px), how


def limit_through(bar: dict | None, side: str, limit_px):
    """Strict trade-through: buy low < limit; short high > limit."""
    lim = _finite(limit_px)
    o = _finite((bar or {}).get("open"))
    h = _finite((bar or {}).get("high"))
    lo = _finite((bar or {}).get("low"))
    if lim is None or lim <= 0:
        return None, "no_prior" if limit_px is None else "no_open"
    if o is None or o <= 0:
        return None, "no_open"
    long = str(side or "long") != "short"
    if long:
        if lo is None:
            return None, "no_low"
        if lo < lim - 1e-12:
            fill = clip_fill(min(o, lim), bar)
            if fill is None:
                return None, "no_open"
            return float(fill), "limit_fill"
        return None, "limit_miss"
    if h is None:
        return None, "no_high"
    if h > lim + 1e-12:
        fill = clip_fill(max(o, lim), bar)
        if fill is None:
            return None, "no_open"
        return float(fill), "limit_fill"
    return None, "limit_miss"


def gap_against(bar: dict | None, side: str, prior_px, *, thresh=GAP_PCT):
    """True when the open gaps ≥ thresh against the order."""
    o = _finite((bar or {}).get("open"))
    p = _finite(prior_px)
    if o is None or p is None or p <= 0:
        return False
    gap = (o - p) / p
    long = str(side or "long") != "short"
    if long:
        return gap >= float(thresh) - 1e-15
    return gap <= -float(thresh) + 1e-15


def partial_shares(intended: int) -> tuple[int, bool]:
    """50% of intended. Floor 1 if intended ≥ 2; intended 1 is all-or-none."""
    n = int(intended or 0)
    if n < 1:
        return 0, False
    if n == 1:
        return 1, False
    got = max(1, n // 2)
    return got, got < n


def resolve_reality(reality: str, bar: dict | None, side: str, *,
                    prior_px=None, slip=None,
                    intended_shares: int = 1) -> dict:
    """One official-bar fill. Missing open / extreme → miss."""
    reality = str(reality or "ideal")
    o = _finite((bar or {}).get("open"))
    intended = int(intended_shares or 0)
    if o is None or o <= 0:
        return _miss("no_open", intended)
    if not (bar or {}).get("open") and o is None:
        return _miss("no_open", intended)

    def ok(px, how, shares=None, partial=False, kind=None):
        if px is None:
            return _miss(how, intended)
        fill = clip_fill(px, bar)
        if fill is None:
            return _miss(how or "no_open", intended)
        n = intended if shares is None else int(shares)
        if n < 1:
            return _miss(how, intended)
        return {
            "px": float(fill), "how": how, "shares": n,
            "miss": False, "partial": bool(partial),
            "kind": kind, "reason": "",
        }

    if reality == "ideal":
        px, how = obs.ideal_fill(bar, side)
        return ok(px, how)

    if reality == "limit_open":
        px, how = limit_through(bar, side, o)
        if how == "limit_miss":
            return _miss("limit_miss", intended)
        return ok(px, how)

    if reality == "limit_prior":
        px, how = limit_through(bar, side, prior_px)
        if how in ("limit_miss", "no_prior", "no_low", "no_high"):
            return _miss(how, intended)
        return ok(px, how)

    if reality == "market_mid":
        px, how = obs.market_fill(bar, side, slip)
        return ok(px, how or "market")

    if reality == "market_adverse":
        px, how = market_extreme(bar, side, favorable=False)
        return ok(px, how)

    if reality == "market_favorable":
        px, how = market_extreme(bar, side, favorable=True)
        return ok(px, how)

    if reality == "partial_50":
        base = resolve_reality(
            "market_mid", bar, side, prior_px=prior_px, slip=slip,
            intended_shares=intended)
        if base["miss"]:
            return base
        shares, partial = partial_shares(intended)
        if shares < 1:
            return _miss("partial_miss", intended)
        return ok(base["px"], "partial_50", shares=shares, partial=partial)

    if reality == "gap_miss":
        if gap_against(bar, side, prior_px):
            return _miss("gap_miss", intended,
                         reason="open gapped ≥8% against the order — miss")
        px, how = obs.ideal_fill(bar, side)
        return ok(px, how or "ideal_open")

    return _miss("unknown_reality", intended)


def _miss(how: str, intended: int, reason: str | None = None) -> dict:
    why = reason or {
        "no_open": "no 09:30 open",
        "no_extreme": "no printed extreme — miss (no invent)",
        "no_low": "no session low — miss",
        "no_high": "no session high — miss",
        "no_prior": "no prior close — miss",
        "limit_miss": "limit did not trade through — miss",
        "gap_miss": "open gapped ≥8% against the order — miss",
    }.get(how, f"{how} — leftover stays")
    return {
        "px": None, "how": how, "shares": 0,
        "miss": True, "partial": False,
        "kind": "fill_miss" if how != "no_open" else "no_price",
        "reason": why,
        "intended_shares": int(intended or 0),
    }


def make_exec_fill(reality: str, tape: Tape):
    """Callback for ``simulate_book`` / ``simulate_shared``."""

    def exec_fill(*, ticker, date, side, action, official_px,
                  intended_shares, bars=None):
        bar = tape.bar(ticker, date)
        # Missing official open is already a miss in the book; still
        # refuse to invent if the tape has no open.
        if _finite(bar.get("open")) is None:
            return _miss("no_open", intended_shares)
        fs = fill_side(side, action)
        prior_px = tape.prior_close(ticker, date)
        slip = tape.slip(ticker, date) if reality in (
            "market_mid", "partial_50") else None
        got = resolve_reality(
            reality, bar, fs, prior_px=prior_px, slip=slip,
            intended_shares=int(intended_shares or 0))
        if action == "exit" and got["miss"]:
            got = dict(got)
            got["reason"] = (got.get("reason") or got["how"]) + " — carry"
        elif got["miss"]:
            got = dict(got)
            if "leftover" not in (got.get("reason") or ""):
                got["reason"] = (got.get("reason") or got["how"]) + " — leftover stays"
        return got

    return exec_fill


def audit_reality_book(book: dict, *, capital: float = CAPITAL) -> dict:
    """Kind-based cash+holdings replay. Never sell unheld / overspend."""
    cash = float(capital)
    hold: dict[str, int] = {}
    fails: list[str] = []
    for t in book.get("trades") or []:
        kind = t.get("side")
        if kind in ("OPEN", "CLOSE"):
            continue
        ticker = t.get("ticker")
        shares = int(t.get("shares") or 0)
        try:
            px = float(t.get("price") or 0)
        except (TypeError, ValueError):
            px = 0.0
        fee = float(t.get("fees") or 0)
        date = t.get("date")
        if shares < 1 or not ticker:
            fails.append(f"{date} empty fill {kind} {ticker}")
            continue
        have = int(hold.get(ticker) or 0)
        if kind == "BUY":
            cost = shares * px + fee
            if cost > cash + 1.0:
                fails.append(
                    f"{date} buy {ticker} ${cost:.2f} > cash ${cash:.2f}")
            cash -= cost
            hold[ticker] = have + shares
        elif kind == "SHORT":
            cash += shares * px - fee
            hold[ticker] = have + shares
        elif kind == "SELL":
            if shares > have:
                fails.append(
                    f"{date} sold unheld {ticker} x{shares} held {have}")
            cash += shares * px - fee
            left = have - shares
            if left > 0:
                hold[ticker] = left
            else:
                hold.pop(ticker, None)
        elif kind == "COVER":
            if shares > have:
                fails.append(
                    f"{date} covered unheld {ticker} x{shares} held {have}")
            cash -= shares * px + fee
            left = have - shares
            if left > 0:
                hold[ticker] = left
            else:
                hold.pop(ticker, None)
        # Combo covers are not leftover-capped (same as the published
        # shared book). Only a BUY may not spend past leftover cash.
        if kind == "BUY" and cash < -1.0:
            fails.append(f"{date} cash negative ${cash:.2f} after BUY {ticker}")
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


def max_dd_stats(daily: list[dict], *, capital: float = CAPITAL) -> dict:
    """Classic peak-to-trough plus underwater vs the $10k start."""
    eqs = [float(d.get("equity") or 0) for d in daily or []]
    if not eqs:
        return {
            "max_dd_pct": None, "max_dd_vs_start_pct": None,
            "n_below_start": 0, "min_equity": None,
        }
    peak = float(capital)
    max_dd = 0.0
    min_eq = float(capital)
    n_below = 0
    for eq in eqs:
        min_eq = min(min_eq, eq)
        if eq < float(capital) - 1e-9:
            n_below += 1
        if eq > peak:
            peak = eq
        if peak > 0:
            max_dd = max(max_dd, (peak - eq) / peak)
    vs_start = (min_eq / float(capital) - 1.0) * 100.0
    return {
        "max_dd_pct": round(100.0 * max_dd, 3),
        "max_dd_vs_start_pct": round(vs_start, 3),
        "n_below_start": n_below,
        "min_equity": round(min_eq, 2),
    }


def count_fills(book: dict) -> dict:
    trades = [t for t in (book.get("trades") or [])
              if t.get("side") not in ("OPEN", "CLOSE")]
    skips = list(book.get("skips") or [])
    miss_kinds = {"fill_miss", "no_price", "limit_miss", "gap_miss"}
    n_miss = sum(1 for k in skips if (k.get("kind") in miss_kinds
                                      or "miss" in str(k.get("how") or "")
                                      or "miss" in str(k.get("reason") or "").lower()))
    n_partial = sum(1 for t in trades if t.get("partial"))
    return {
        "n_fills": len(trades),
        "n_miss": n_miss,
        "n_partial": n_partial,
        "n_skips": len(skips),
        "n_hard_red": sum(1 for k in skips if k.get("kind") == "hard_red"),
    }


def summarize_book(book: dict, *, capital: float = CAPITAL,
                   starts: list[dict] | None = None) -> dict:
    daily = list(book.get("daily") or [])
    dd = max_dd_stats(daily, capital=capital)
    fills = count_fills(book)
    n_green = sum(1 for d in daily if d.get("made_money"))
    starts = list(starts or [])
    start_green = sum(1 for s in starts if s.get("made_money"))
    audit = audit_reality_book(book, capital=capital)
    return {
        "book_pct": book.get("total_ret_pct"),
        "final_equity": book.get("final_equity"),
        "final_cash": book.get("cash"),
        "max_dd_pct": dd["max_dd_pct"],
        "max_dd_vs_start_pct": dd["max_dd_vs_start_pct"],
        "n_below_start": dd["n_below_start"],
        "min_equity": dd["min_equity"],
        "n_sessions": len(daily),
        "n_sessions_green": n_green,
        "win_session_pct": (None if not daily else
                            round(100.0 * n_green / len(daily), 1)),
        "start_green": start_green if starts else None,
        "start_n": len(starts) if starts else None,
        "audit_ok": bool(audit["ok"]),
        "audit_fails": (audit.get("fails") or [])[:8],
        **fills,
        "equity_daily": [
            {"date": d.get("date"), "equity": d.get("equity"),
             "cash": d.get("cash")}
            for d in daily
        ],
        "starts": [
            {"start": s.get("start"), "return_pct": s.get("return_pct"),
             "made_money": bool(s.get("made_money"))}
            for s in starts
        ],
    }


def clip_panel(panel: dict, from_date: str | None = None,
               to_date: str | None = None) -> dict:
    cal = [d for d in (panel.get("session_dates") or [])
           if (not from_date or d >= from_date)
           and (not to_date or d <= to_date)]
    keep = set(cal)
    rows = [r for r in (panel.get("rows") or []) if r.get("date") in keep]
    by: dict[str, list] = {}
    for r in rows:
        by.setdefault(r["date"], []).append(r)
    out = {k: v for k, v in panel.items()
           if k not in ("session_dates", "rows", "by_date")}
    out.update({
        "session_dates": cal,
        "rows": rows,
        "by_date": by,
        "from_date": cal[0] if cal else from_date,
        "to_date": cal[-1] if cal else to_date,
        "n_sessions": len(cal),
        "n_rows": len(rows),
    })
    return out


def load_panel(path: Path = PANEL_PATH, *, from_date=None, to_date=None):
    if not path.exists():
        return {"session_dates": [], "rows": [], "by_date": {}}
    raw = json.loads(path.read_text(encoding="utf-8"))
    panel = fm.rehydrate_panel(raw)
    return clip_panel(panel, from_date, to_date)


def blotter_names_from_action(path: Path = ACTION_MD) -> dict:
    """Featured table + 'all other blotters' from the published Action MD."""
    featured: list[str] = []
    others: list[str] = []
    if not path.exists():
        return {"featured": featured, "others": others, "all": []}
    text = path.read_text(encoding="utf-8")
    in_other = False
    for line in text.splitlines():
        if line.startswith("## All other blotters"):
            in_other = True
            continue
        if in_other and line.startswith("## "):
            break
        if in_other:
            m = re.search(r"`([a-z0-9_]+)`", line)
            if m:
                others.append(m.group(1))
            continue
        if line.startswith("| `"):
            m = re.match(r"\| `([a-z0-9_]+)`", line)
            if m:
                featured.append(m.group(1))
    seen = set()
    all_names = []
    for n in featured + others:
        if n not in seen:
            seen.add(n)
            all_names.append(n)
    return {"featured": featured, "others": others, "all": all_names}


def coverage_plan(names: dict, rec_by: dict, spec_by: dict) -> dict:
    """Must-run first, then featured, then the rest. Drop unknown silently? No."""
    featured = list(names.get("featured") or [])
    others = list(names.get("others") or [])
    must = [n for n in MUST_RUN]
    rest_feat = [n for n in featured if n not in must]
    rest = [n for n in others if n not in must and n not in featured]
    ordered = []
    seen = set()
    for n in must + rest_feat + rest:
        if n in seen:
            continue
        seen.add(n)
        ordered.append(n)
    runnable = []
    unknown = []
    for n in ordered:
        if n in spec_by or n in rec_by:
            runnable.append(n)
        else:
            unknown.append(n)
    return {
        "must_run": [n for n in must if n in rec_by or n in spec_by],
        "featured": [n for n in featured if n in rec_by or n in spec_by],
        "ordered": runnable,
        "unknown": unknown,
    }


def simulate_named(name: str, panel: dict, *, rec_by: dict, spec_by: dict,
                   bars, fees, regime, tape: Tape, reality: str,
                   start: str | None = None) -> dict:
    exec_fill = None if reality == "ideal" else make_exec_fill(reality, tape)
    spec = spec_by.get(name)
    if spec is not None:
        recs = fmc.recs_for_spec(spec, rec_by)
        if spec.get("pool") == "split":
            return fmc.simulate_split(
                panel, recs, spec["weights"], bars=bars, fees=fees,
                regime=regime, start=start, name=name, exec_fill=exec_fill)
        return fmc.simulate_shared(
            panel, recs, spec["weights"], bars=bars, fees=fees,
            regime=regime, start=start, name=name, net=spec.get("net") or "priority",
            exec_fill=exec_fill)
    rec = rec_by.get(name)
    if rec is None:
        raise KeyError(name)
    return fmb.simulate_book(
        panel, rec, bars=bars, fees=fees, regime=regime, start=start,
        exec_fill=exec_fill)


def replay_named_starts(name: str, panel: dict, **kw) -> list[dict]:
    cal = list(panel.get("session_dates") or [])
    out = []
    for start in cal:
        book = simulate_named(name, panel, start=start, **kw)
        out.append(fmb.slim_start_path(book, start, cal))
    return out


def grade_sleeve(name: str, panel: dict, *, rec_by, spec_by, bars, fees,
                 regime, tape: Tape, realities=REALITIES,
                 do_starts: bool = False) -> dict:
    columns = {}
    for reality in realities:
        book = simulate_named(
            name, panel, rec_by=rec_by, spec_by=spec_by, bars=bars,
            fees=fees, regime=regime, tape=tape, reality=reality)
        starts = []
        if do_starts:
            starts = replay_named_starts(
                name, panel, rec_by=rec_by, spec_by=spec_by, bars=bars,
                fees=fees, regime=regime, tape=tape, reality=reality)
        columns[reality] = summarize_book(book, starts=starts)
        columns[reality]["reality"] = reality
        columns[reality]["label"] = REALITY_LABEL.get(reality, reality)
    range_bar = {}
    for k in RANGE_KEYS:
        range_bar[k] = (columns.get(k) or {}).get("book_pct")
    return {
        "name": name,
        "alias": "flatten_would" if name == "flatten_h3" else None,
        "kind": "combo" if name in spec_by else "recipe",
        "must_run": name in MUST_RUN,
        "columns": columns,
        "range": range_bar,
    }


def build(*, panel=None, store=None, bars=None, fees=None, regime=None,
          from_date: str | None = DEFAULT_FROM, to_date: str | None = None,
          names: list[str] | None = None, realities=REALITIES,
          starts: str = "featured", time_budget: float = TIME_BUDGET_SEC) -> dict:
    t0 = time.monotonic()
    if panel is None:
        panel = load_panel(from_date=from_date, to_date=to_date)
    else:
        panel = clip_panel(panel, from_date, to_date)
    if store is None and bars is None:
        store = obs.load_bar_store()
    bars = bars if bars is not None else store
    fees = fees if fees is not None else pt.load_fees()
    regime = regime if regime is not None else fmb.load_regime()
    tape = Tape(store=store or {}, bars=bars)
    rec_by = {r["name"]: r for r in fm.build_recipes()}
    spec_by = {s["name"]: s for s in fmc.combo_specs()}
    listed = blotter_names_from_action()
    plan = coverage_plan(listed, rec_by, spec_by)
    want = list(plan["ordered"]) if names is None else list(names)
    feat_set = set(plan["featured"]) | set(plan["must_run"])
    reports = {}
    not_yet = []
    ran = []
    for name in want:
        elapsed = time.monotonic() - t0
        # Always finish must-run even if the budget is tight.
        if (elapsed > float(time_budget)
                and name not in plan["must_run"]
                and reports):
            not_yet.append(name)
            continue
        if name not in rec_by and name not in spec_by:
            not_yet.append(name)
            continue
        do_starts = (
            starts == "all"
            or (starts == "featured" and name in feat_set)
            or (starts == "must" and name in plan["must_run"])
        )
        try:
            reports[name] = grade_sleeve(
                name, panel, rec_by=rec_by, spec_by=spec_by, bars=bars,
                fees=fees, regime=regime, tape=tape, realities=realities,
                do_starts=do_starts)
            reports[name]["starts_ran"] = bool(do_starts)
            ran.append(name)
        except Exception as exc:
            reports[name] = {
                "name": name, "error": f"{type(exc).__name__}: {exc}",
                "columns": {}, "range": {},
            }
            ran.append(name)
    for n in plan["ordered"]:
        if n not in reports and n not in not_yet:
            not_yet.append(n)
    cal = list(panel.get("session_dates") or [])
    sh = reports.get("combo_sh_5050_shared") or {}
    sh_cols = sh.get("columns") or {}
    headline = _headline(sh_cols, cal)
    return {
        "generated_at": datetime.now(TZ).isoformat(),
        "order_sent": ORDER_SENT,
        "research_only": True,
        "live_wire": False,
        "live_untouched": list(LIVE_UNTOUCHED),
        "from_date": cal[0] if cal else from_date,
        "to_date": cal[-1] if cal else to_date,
        "n_sessions": len(cal),
        "capital": CAPITAL,
        "realities": list(realities),
        "sleeves": ran,
        "not_yet_run": not_yet,
        "must_run": list(plan["must_run"]),
        "featured": list(plan["featured"]),
        "n_blotters_listed": len(plan["ordered"]),
        "starts_policy": starts,
        "elapsed_sec": round(time.monotonic() - t0, 2),
        "proxy_note": PROXY_NOTE,
        "ideal_note": IDEAL_NOTE,
        "favorable_note": FAVORABLE_NOTE,
        "adverse_note": ADVERSE_NOTE,
        "misread": (
            "combo_sh_5050_shared Starts YES is wake-$10k-on-each-date "
            "replay (path finishes green), not a daily win rate. "
            "Win% is the share of sessions whose close equity beat the "
            "prior close — do not write 21/23 as a daily win rate."
        ),
        "published_vs_run": (
            "This run's ideal combo_sh_5050_shared is the current "
            "official-tape replay. The Action MD snapshot was +41.54% "
            "($14,153.61, 155 fills, 21/23 starts YES). Paths match "
            "through 2026-09-11; on 9/14–9/15 IRD / BNC / CMRC have no "
            "official 09:30 in ohlc.parquet or session_bar, so those "
            "lots carried instead of selling. Missing open = no fill — "
            "close is not substituted. That leftover carry is why this "
            "run's ideal is below the snapshot, not a different recipe."
        ),
        "headline": headline,
        "note": (
            "Research overlay on the $10k leftover-cash butterfly. "
            "Does not change live flatten_robust, hard-red sit, or "
            "Webull paper. PR 241 1-share open→close is a different path."
        ),
        "reports": reports,
        "spotlight": "combo_sh_5050_shared",
    }


def _headline(cols: dict, cal: list[str]) -> str:
    if not cols:
        return ("$10k butterfly fill realities — research only. "
                "combo_sh_5050_shared not yet graded.")
    bits = []
    for r in ("ideal", "market_mid", "market_adverse", "limit_open", "partial_50"):
        st = cols.get(r) or {}
        bits.append(f"{r} {_pct(st.get('book_pct'))}")
    mid = cols.get("market_mid") or {}
    eqs = [d.get("equity") for d in (mid.get("equity_daily") or [])]
    stay = bool(eqs) and all(float(e or 0) >= CAPITAL - 1e-9 for e in eqs)
    stay_s = ("market_mid equity stayed ≥ $10k from "
              f"{cal[0] if cal else '8/13'}" if stay else
              "market_mid equity dipped under $10k")
    return ("combo_sh_5050_shared $10k path: " + " · ".join(bits)
            + f" — {stay_s}. Research only.")


def render_md(payload: dict) -> str:
    reports = payload.get("reports") or {}
    sleeves = payload.get("sleeves") or []
    lines = [
        "# $10k butterfly fill realities — research only",
        "",
        payload.get("headline") or "",
        "",
        "## Scope",
        "",
        "- **Research only.** Live `flatten_robust`, hard-red sit, and "
        "Webull paper are **untouched**. This is not a wire into the "
        "published cash book.",
        "- Ledger is the real leftover-cash butterfly "
        "(`factor_mine_book` / combo shared pile), **not** PR 241's "
        "1-share open→same-day-close lab.",
        f"- Window `{payload.get('from_date')}` → `{payload.get('to_date')}` "
        f"({payload.get('n_sessions') or 0} sessions). Orders sent "
        f"{payload.get('order_sent') or ORDER_SENT}.",
        f"- {payload.get('proxy_note')}",
        f"- {payload.get('ideal_note')}",
        f"- {payload.get('favorable_note')}",
        f"- {payload.get('adverse_note')}",
        "",
        "## Correct a misread",
        "",
        payload.get("misread") or "",
        "",
        payload.get("published_vs_run") or "",
        "",
        "## Method",
        "",
        "- Same `pick_day` lists, leftover / sell-first / min-hold / "
        "hard-red sit as the published book. Day N open cash/held = "
        "day N−1 close after **that** reality's fills.",
        "- Whole shares, Futubull fees, short liability equity ≥ 2× "
        "notional. Missing official open = no fill.",
        "- Fill functions: `resolve_reality` + PR 241 "
        "`calibrate_slip` / `DELAY_FRAC` / `market_fill` for "
        "`market_mid`. Limit trade-through is strict "
        "(buy: low < limit; short: high > limit).",
        "- `partial_50`: 50% of the shares `market_mid` would have "
        "taken (floor 1 if intended ≥ 2; intended 1 is all-or-none). "
        "Fees on filled shares only. Unfilled cash stays leftover.",
        "- `gap_miss`: |open−prior|/prior ≥ 8% against the order "
        "(buy gaps up / short gaps down) → miss; else official open.",
        "",
        "Regenerate: `PYTHONPATH=. python3 -m src.book_fill_reality --write`.",
        "",
        "Dashboard: [book-fill-reality.html]"
        "(../dashboard/factor-mine/book-fill-reality.html).",
        "",
        f"Live untouched: {', '.join(f'`{n}`' for n in LIVE_UNTOUCHED)}.",
        "",
        "## Book% range (worst … mid … ideal … best)",
        "",
        "| Strategy | WORST adverse | mid (market) | ideal | BEST favorable | mid ≥$10k | Starts YES (ideal) | Audit |",
        "|---|---:|---:|---:|---:|---|---:|---|",
    ]
    for name in sleeves:
        rep = reports.get(name) or {}
        cols = rep.get("columns") or {}
        if not cols:
            err = rep.get("error") or "no columns"
            lines.append(f"| `{name}` | {err} | | | | | | |")
            continue
        w = (cols.get("market_adverse") or {}).get("book_pct")
        m = (cols.get("market_mid") or {}).get("book_pct")
        i = (cols.get("ideal") or {}).get("book_pct")
        b = (cols.get("market_favorable") or {}).get("book_pct")
        mid = cols.get("market_mid") or {}
        eqs = [d.get("equity") for d in (mid.get("equity_daily") or [])]
        stay = "YES" if eqs and all(float(e or 0) >= CAPITAL - 1e-9 for e in eqs) else "NO"
        ideal = cols.get("ideal") or {}
        sg = ideal.get("start_green")
        sn = ideal.get("start_n")
        starts = "—" if sg is None else f"{sg}/{sn}"
        alias = f" / flatten_would" if name == "flatten_h3" else ""
        ok = all((cols.get(r) or {}).get("audit_ok") for r in cols) 
        lines.append(
            f"| `{name}`{alias} | {_pct(w)} | {_pct(m)} | {_pct(i)} | {_pct(b)} "
            f"| {stay} | {starts} | {'PASS' if ok else 'FAIL'} |"
        )
    lines += [
        "",
        "## Every sleeve × reality",
        "",
        "| Strategy | Reality | Book% | Close $ | Max DD vs $10k | Sessions < $10k | Starts YES | Fills | Miss | Partial | Skips | Sess win% |",
        "|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for name in sleeves:
        rep = reports.get(name) or {}
        cols = rep.get("columns") or {}
        for r in payload.get("realities") or REALITIES:
            st = cols.get(r) or {}
            if not st:
                continue
            sg = st.get("start_green")
            sn = st.get("start_n")
            starts = "—" if sg is None else f"{sg}/{sn}"
            lines.append(
                f"| `{name}` | `{r}` | {_pct(st.get('book_pct'))} | "
                f"{_usd(st.get('final_equity'))} | "
                f"{_pct(st.get('max_dd_vs_start_pct'))} | "
                f"{st.get('n_below_start') if st.get('n_below_start') is not None else '—'} | "
                f"{starts} | {st.get('n_fills') or 0} | {st.get('n_miss') or 0} | "
                f"{st.get('n_partial') or 0} | {st.get('n_skips') or 0} | "
                f"{'—' if st.get('win_session_pct') is None else str(st.get('win_session_pct'))+'%'} |"
            )
    sh = reports.get("combo_sh_5050_shared") or {}
    sh_cols = sh.get("columns") or {}
    if sh_cols:
        lines += [
            "",
            "## Spotlight `combo_sh_5050_shared` daily close equity",
            "",
            "Starts YES is a **start-date** chip (wake $10k on that date, "
            "run to the end). Sess win% is the share of sessions whose "
            "close beat the prior close. Do not write Starts YES as a "
            "daily win rate.",
            "",
        ]
        dates = [d.get("date") for d in
                 ((sh_cols.get("ideal") or {}).get("equity_daily") or [])]
        header = "| Date |" + "".join(
            f" {REALITY_LABEL.get(r, r)} |"
            for r in ("ideal", "market_mid", "market_adverse",
                      "limit_open", "partial_50", "market_favorable")
        )
        lines.append(header)
        lines.append("|---|" + "---:|" * 6)
        by = {r: {d.get("date"): d.get("equity")
                  for d in ((sh_cols.get(r) or {}).get("equity_daily") or [])}
              for r in REALITIES}
        for d in dates:
            cells = " | ".join(_usd(by[r].get(d)) for r in (
                "ideal", "market_mid", "market_adverse",
                "limit_open", "partial_50", "market_favorable"))
            lines.append(f"| {d} | {cells} |")
    not_yet = payload.get("not_yet_run") or []
    lines += [
        "",
        "## Not yet run",
        "",
    ]
    if not_yet:
        lines.append(
            f"{len(not_yet)} published blotters were not graded in this "
            "pass (time budget or unknown recipe). Names:"
        )
        lines.append("")
        for n in not_yet:
            lines.append(f"- `{n}`")
    else:
        lines.append("All listed Action blotters were graded.")
    lines += [
        "",
        f"Elapsed {payload.get('elapsed_sec')}s. "
        f"Starts policy: `{payload.get('starts_policy')}`.",
        "",
    ]
    return "\n".join(lines) + "\n"


def _spark(values: list, *, w: int = 160, h: int = 28) -> str:
    nums = [float(v) for v in values if v is not None]
    if len(nums) < 2:
        return ""
    lo, hi = min(nums), max(nums)
    if hi - lo < 1e-9:
        hi = lo + 1.0
    pts = []
    for i, v in enumerate(nums):
        x = 1 + (w - 2) * i / (len(nums) - 1)
        y = h - 2 - (h - 4) * (v - lo) / (hi - lo)
        pts.append(f"{x:.1f},{y:.1f}")
    color = "#4ade80" if nums[-1] >= nums[0] else "#f87171"
    return (
        f"<svg width='{w}' height='{h}' viewBox='0 0 {w} {h}' "
        f"aria-hidden='true'><polyline fill='none' stroke='{color}' "
        f"stroke-width='1.5' points='{' '.join(pts)}'/></svg>"
    )


def _range_bar(rep: dict) -> str:
    cols = rep.get("columns") or {}
    vals = []
    for r, cls in (
        ("market_adverse", "worst"),
        ("market_mid", "mid"),
        ("ideal", "ideal"),
        ("market_favorable", "best"),
    ):
        v = (cols.get(r) or {}).get("book_pct")
        if v is not None:
            vals.append((r, float(v), cls))
    if not vals:
        return "<span class='mut'>—</span>"
    xs = [v for _, v, _ in vals]
    lo, hi = min(xs), max(xs)
    if hi - lo < 1e-9:
        hi = lo + 1.0
    chips = []
    for r, v, cls in vals:
        pct = 100.0 * (v - lo) / (hi - lo)
        chips.append(
            f"<span class='mk {cls}' style='left:{pct:.1f}%' "
            f"title='{r} {v:+.2f}%'></span>"
        )
    return (
        f"<div class='rbar' title='worst {xs[0]:+.2f} … best {xs[-1]:+.2f}'>"
        f"{''.join(chips)}</div>"
        f"<span class='mut'>{_pct(vals[0][1])} … {_pct(vals[-1][1])}</span>"
    )


def render_html(payload: dict) -> str:
    reports = payload.get("reports") or {}
    sleeves = payload.get("sleeves") or []
    rows = []
    for name in sleeves:
        rep = reports.get(name) or {}
        cols = rep.get("columns") or {}
        alias = " <span class='mut'>flatten_would</span>" if name == "flatten_h3" else ""
        for r in payload.get("realities") or REALITIES:
            st = cols.get(r) or {}
            if not st:
                continue
            cls = "pos" if (st.get("book_pct") or 0) > 0 else "neg"
            sg = st.get("start_green")
            sn = st.get("start_n")
            starts = "—" if sg is None else f"{sg}/{sn}"
            spark = ""
            if name == "combo_sh_5050_shared":
                spark = _spark([d.get("equity")
                                for d in (st.get("equity_daily") or [])])
            rows.append(
                "<tr>"
                f"<td class='tick'>{_esc(name)}{alias}</td>"
                f"<td>{_esc(REALITY_LABEL.get(r, r))}</td>"
                f"<td class='{cls}'>{_esc(_pct(st.get('book_pct')))}</td>"
                f"<td>{_esc(_usd(st.get('final_equity')))}</td>"
                f"<td>{_esc(_pct(st.get('max_dd_vs_start_pct')))}</td>"
                f"<td>{st.get('n_below_start') if st.get('n_below_start') is not None else '—'}</td>"
                f"<td>{starts}</td>"
                f"<td>{st.get('n_fills') or 0}</td>"
                f"<td>{st.get('n_miss') or 0}</td>"
                f"<td>{st.get('n_partial') or 0}</td>"
                f"<td>{'PASS' if st.get('audit_ok') else 'FAIL'}</td>"
                f"<td>{spark}</td>"
                "</tr>"
            )
    range_rows = []
    for name in sleeves:
        rep = reports.get(name) or {}
        if not (rep.get("columns") or {}):
            continue
        range_rows.append(
            "<tr>"
            f"<td class='tick'>{_esc(name)}</td>"
            f"<td>{_range_bar(rep)}</td>"
            "</tr>"
        )
    sh = reports.get("combo_sh_5050_shared") or {}
    sh_cols = sh.get("columns") or {}
    daily_rows = []
    dates = [d.get("date") for d in
             ((sh_cols.get("ideal") or {}).get("equity_daily") or [])]
    by = {r: {d.get("date"): d.get("equity")
              for d in ((sh_cols.get(r) or {}).get("equity_daily") or [])}
          for r in REALITIES}
    for d in dates:
        cells = "".join(
            f"<td>{_esc(_usd(by[r].get(d)))}</td>"
            for r in ("ideal", "market_mid", "market_adverse",
                      "limit_open", "partial_50")
        )
        daily_rows.append(f"<tr><td>{_esc(d)}</td>{cells}</tr>")
    not_yet = payload.get("not_yet_run") or []
    not_yet_html = ("<p class='mut'>All listed Action blotters were graded.</p>"
                    if not not_yet else
                    "<ul>" + "".join(f"<li><code>{_esc(n)}</code></li>"
                                     for n in not_yet) + "</ul>")
    return f"""<!DOCTYPE html>
<html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>$10k butterfly fill realities — research</title>
<style>
:root{{--bg:#0f1420;--card:#171e2e;--line:#262f45;--fg:#dfe6f2;--mut:#8b96ab;
--pos:#4ade80;--neg:#f87171;--gold:#fbbf24;--mid:#93c5fd;--best:#c4b5fd}}
*{{box-sizing:border-box}}body{{margin:0;background:var(--bg);color:var(--fg);
font:14px/1.45 -apple-system,Segoe UI,Roboto,sans-serif}}
.wrap{{max-width:1180px;margin:0 auto;padding:16px 14px 32px}}
h1{{font-size:20px;margin:0 0 6px}}h2{{font-size:15px;margin:16px 0 8px;color:#aeb9cf}}
.mut{{color:var(--mut)}}.pos{{color:var(--pos)}}.neg{{color:var(--neg)}}
.banner{{background:#3f1d1d;border:1px solid #7f1d1d;color:#fecaca;
border-radius:10px;padding:10px 12px;margin:0 0 12px}}
.card{{background:var(--card);border:1px solid var(--line);border-radius:10px;
padding:10px 12px;margin:0 0 12px}}
table{{width:100%;border-collapse:collapse;font-size:12px}}
th,td{{padding:5px 6px;border-bottom:1px solid var(--line);text-align:right}}
th{{color:var(--mut)}}td:first-child,th:first-child,td:nth-child(2),th:nth-child(2)
{{text-align:left}}
.tick{{font-family:ui-monospace,Menlo,Consolas,monospace}}
a{{color:#93c5fd}}
.rbar{{position:relative;height:10px;background:#1f2937;border-radius:6px;
margin:0 0 4px;min-width:140px}}
.mk{{position:absolute;top:-2px;width:8px;height:14px;border-radius:2px;
transform:translateX(-50%)}}
.mk.worst{{background:var(--neg)}}.mk.mid{{background:var(--mid)}}
.mk.ideal{{background:var(--gold)}}.mk.best{{background:var(--best)}}
</style></head><body><div class="wrap">
<h1>$10k butterfly fill realities</h1>
<p class="mut">Research overlay · leftover-cash book · orders sent
{ _esc(payload.get("order_sent") or ORDER_SENT) }
· window { _esc(payload.get("from_date")) } → { _esc(payload.get("to_date")) }
({ payload.get("n_sessions") or 0 } sessions) · generated
{ _esc(payload.get("generated_at")) }</p>
<div class="banner"><b>Research only.</b> Live <code>flatten_robust</code>,
hard-red sit, and Webull paper are <b>untouched</b>. This is not a wire
into the published cash book. PR 241 1-share open→close is a different path.</div>
<div class="card"><b>{ _esc(payload.get("headline")) }</b>
<p class="mut">{ _esc(payload.get("note")) }</p>
<p class="mut">{ _esc(payload.get("misread")) }</p>
<p class="mut">{ _esc(payload.get("published_vs_run")) }</p>
</div>
<p class="mut">
<a href="./">factor mine</a> ·
<a href="open-bell-slip.html">open-bell slip (1-share lab)</a> ·
<a href="../strategy-board/">strategy board</a> ·
<a href="https://github.com/SRoyaltyy/fullscan/blob/main/03_scoreboard/BOOK_FILL_REALITY.md">BOOK_FILL_REALITY.md</a>
</p>
<h2>Book% range — worst … mid … ideal … best</h2>
<p class="mut">Markers: red = market_adverse (worst plausible), blue = market_mid,
gold = ideal published open, purple = market_favorable (optimistic / unlikely).
All four equally “could happen”; extremes are labeled.</p>
<div style="overflow-x:auto"><table>
<tr><th>Strategy</th><th>Range</th></tr>
{ "".join(range_rows) }
</table></div>
<h2>Every strategy × reality</h2>
<p class="mut">Starts YES = wake $10k on each date and run to the end
(not a daily win rate). Sess &lt; $10k = close equity under the start cash.</p>
<div style="overflow-x:auto"><table>
<tr><th>Strategy</th><th>Reality</th><th>Book%</th><th>Close $</th>
<th>Max DD vs $10k</th><th>&lt;$10k</th><th>Starts YES</th>
<th>Fills</th><th>Miss</th><th>Partial</th><th>Audit</th><th>Equity</th></tr>
{ "".join(rows) }
</table></div>
<h2><code>combo_sh_5050_shared</code> daily close equity</h2>
<p class="mut">Needed to see whether the 8/13 start stays steadily above $10k
as we push forward. Official marks; fills change leftover and later tickets.</p>
<div style="overflow-x:auto"><table>
<tr><th>Date</th><th>ideal</th><th>market_mid</th><th>adverse</th>
<th>limit_open</th><th>partial_50</th></tr>
{ "".join(daily_rows) }
</table></div>
<h2>What the columns mean</h2>
<ol>
<li><b>ideal</b> — official 09:30 open. Optimistic published column, not best-possible.</li>
<li><b>limit_prior</b> — working limit at prior close. Fill iff session trades through
(buy: low &lt; limit; short: high &gt; limit). Miss = $0 position, leftover stays.</li>
<li><b>limit_open</b> — same trade-through at the official open.</li>
<li><b>market_mid</b> — PR 241 delayed market: {DELAY_FRAC:.0%} of post-open adverse
room, α from prior sessions only.</li>
<li><b>market_adverse</b> — buy at high / short at low when that print exists. WORST plausible.</li>
<li><b>market_favorable</b> — buy at low / short at high. BEST plausible, labeled unlikely.</li>
<li><b>partial_50</b> — 50% of market_mid shares (floor 1 if intended ≥ 2; 1-share all-or-none).</li>
<li><b>gap_miss</b> — |open−prior|/prior ≥ 8% against the order → miss.</li>
</ol>
<p class="mut">{ _esc(payload.get("proxy_note")) }</p>
<h2>Not yet run</h2>
{ not_yet_html }
<p class="mut">Code: <code>src/book_fill_reality.py</code>.
Regenerate with <code>python -m src.book_fill_reality --write</code>.
Elapsed { payload.get("elapsed_sec") }s.</p>
</div></body></html>
"""


def overlay_snippet() -> str:
    return """
<details id="bookFillReality" class="how" open>
<summary>$10k butterfly fill realities — research (not a wire)</summary>
<p class="mut">Official-open cash book vs limit / market / partial / gap
messiness. Live <code>flatten_robust</code>, hard-red sit, and Webull paper
are untouched. Starts YES is a start-date chip, not a daily win rate.</p>
<div id="bookFillRealityBody" class="mut">loading book-fill reality…</div>
<p class="mut"><a href="book-fill-reality.html" style="color:#93c5fd">full overlay</a>
 · <a href="https://github.com/SRoyaltyy/fullscan/blob/main/03_scoreboard/BOOK_FILL_REALITY.md" style="color:#93c5fd">BOOK_FILL_REALITY.md</a></p>
</details>
"""


def write_outputs(payload: dict) -> None:
    OUT_MD.parent.mkdir(parents=True, exist_ok=True)
    DASH_DIR.mkdir(parents=True, exist_ok=True)
    STRAT_JSON.parent.mkdir(parents=True, exist_ok=True)
    text = json.dumps(payload, indent=2)
    OUT_JSON.write_text(text, encoding="utf-8")
    DASH_JSON.write_text(text, encoding="utf-8")
    STRAT_JSON.write_text(text, encoding="utf-8")
    OUT_MD.write_text(render_md(payload), encoding="utf-8")
    DASH_HTML.write_text(render_html(payload), encoding="utf-8")


def run(*, write: bool = True, from_date: str | None = DEFAULT_FROM,
        to_date: str | None = None, panel=None, store=None, bars=None,
        fees=None, names=None, starts: str = "featured",
        time_budget: float = TIME_BUDGET_SEC) -> dict:
    payload = build(
        panel=panel, store=store, bars=bars, fees=fees,
        from_date=from_date, to_date=to_date, names=names,
        starts=starts, time_budget=time_budget)
    if write:
        write_outputs(payload)
        print(f"[book-fill-reality] wrote {OUT_MD}")
        print(f"[book-fill-reality] wrote {OUT_JSON}")
        print(f"[book-fill-reality] wrote {DASH_HTML}")
    return payload


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(
        description="Research: $10k butterfly fill realities "
                    "(does not change live flatten_robust / hard-red sit).")
    ap.add_argument("--from-date", default=DEFAULT_FROM)
    ap.add_argument("--to-date", default=None)
    ap.add_argument("--write", action="store_true")
    ap.add_argument("--no-write", action="store_false", dest="write")
    ap.add_argument("--names", default=None,
                    help="Comma-separated recipe/combo names (default: Action blotters).")
    ap.add_argument("--starts", default="featured",
                    choices=("none", "must", "featured", "all"))
    ap.add_argument("--time-budget", type=float, default=TIME_BUDGET_SEC)
    args = ap.parse_args(argv)
    names = None
    if args.names:
        names = [x.strip() for x in args.names.split(",") if x.strip()]
    payload = run(
        write=bool(args.write), from_date=args.from_date or None,
        to_date=args.to_date or None, names=names,
        starts=args.starts, time_budget=float(args.time_budget))
    print(
        f"[book-fill-reality] sleeves={len(payload.get('sleeves') or [])} "
        f"not_yet={len(payload.get('not_yet_run') or [])} "
        f"headline={payload.get('headline')}",
        flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
