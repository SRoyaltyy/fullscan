"""Open-bell slippage / no-fill simulation — research only.

Orders are modeled as sent at **09:30 ET** (same clock as the Open 09:30
pack / Webull paper path). This board asks what those entries would
look like under realistic fill assumptions versus the published
**ideal open** fill.

Two realities, side-by-side vs ideal open:

  (a) MARKET-like — fill delay + adverse slippage. α is calibrated
      from *prior* OHLC only (clock-clean). This session's high/low
      are the realized post-open path of an order already sent — not
      a signal input. Historically the adverse side of the open is
      the majority of sessions; the model is adverse-biased on
      purpose.

  (b) LIMIT at the intended open, or at the prior intended print
      (prior close). Fill or miss. Daily OHLC has no natural partial,
      so a touch is a full fill and a miss is flat $0.

KEEP/KILL uses Cyrus's standing bar: ≥30 filled fires and >55%
after-fee hit rate. After-fee P&L is always shown vs the zero-slip
(ideal-open) baseline. North star ~2%/day after fees is context, not
a second gate. Paper fills that assume the signal-day close are
fantasy — this sim is meant to stress that.

Live ``flatten_robust``, hard-red sit, and Webull paper execution
semantics are **not** changed. This file is a research overlay, not
a wire.

OHLC source: existing ``data/prices/ohlc.parquet`` / ``ticker_lookback``
official tape / ``ohlc_ripper.prior_bars``. No new price feed.

CLI: python -m src.open_bell_slip [--write]
"""
from __future__ import annotations

import argparse
import json
import math
import statistics
from datetime import datetime
from itertools import zip_longest
from pathlib import Path
from zoneinfo import ZoneInfo

ROOT = Path(__file__).resolve().parent.parent
OHLC_PATH = ROOT / "data" / "prices" / "ohlc.parquet"
PANEL_PATH = ROOT / "data" / "factor_mine" / "panel.json"
FEES_PATH = ROOT / "00_grounding" / "futubull_fees.json"
OUT_MD = ROOT / "03_scoreboard" / "OPEN_BELL_SLIP.md"
OUT_JSON = ROOT / "03_scoreboard" / "open_bell_slip.json"
DASH_DIR = ROOT / "dashboard" / "factor-mine"
DASH_JSON = DASH_DIR / "open_bell_slip.json"
DASH_HTML = DASH_DIR / "open-bell-slip.html"
STRAT_JSON = ROOT / "dashboard" / "strategy-board" / "open_bell_slip.json"

# 09:30 ET — matches Open 09:30 pack / Webull paper (PR #239).
ORDER_SENT = "09:30 ET"
TZ = ZoneInfo("America/New_York")

# Delayed market walks this fraction of the post-open adverse room
# (high−open for a buy, open−low for a short). Not fit on this
# session's close — that would mix the exit into the fill.
DELAY_FRAC = 0.35
PRIOR_N = 20
ALPHA_LO = 0.20
ALPHA_HI = 0.55

KEEP_MIN_FIRES = 30
KEEP_WIN = 0.55
SHARES = 1

# Research shopping lists only. Live sit still sits.
SLEEVE_HOT4 = "union_hot_n4_h1"
SLEEVE_COMBO = "combo_sh_macd_5050_shared"
SLEEVE_FLAT = "flatten_would"
SLEEVE_ORDER = (SLEEVE_COMBO, SLEEVE_HOT4, SLEEVE_FLAT)

REALITIES = ("ideal", "market", "limit_open", "limit_prior")

# Do not change these live names. Research overlay only.
LIVE_UNTOUCHED = (
    "flatten_robust",
    "hard_red_sit",
    "webull_paper",
)


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


def _pct(v) -> str:
    return "—" if v is None else f"{100.0 * float(v):.1f}%"


def _n(v) -> str:
    return "—" if v is None else f"{float(v):+.2f}"


def next_session(cal: list[str], date: str) -> str | None:
    if date not in cal:
        return None
    i = cal.index(date)
    if i + 1 >= len(cal):
        return None
    return cal[i + 1]


def prior_session(cal: list[str], date: str) -> str | None:
    if date not in cal:
        return None
    i = cal.index(date)
    if i <= 0:
        return None
    return cal[i - 1]


def load_fees(path: Path = FEES_PATH) -> dict:
    """Same JSON ``paper_trade.load_fees`` reads. No pandas import."""
    return json.loads(path.read_text(encoding="utf-8"))


def order_fees(shares: int, price: float, side: str, f: dict) -> float:
    """Futubull US-stock fees — same formula as ``paper_trade.order_fees``.

    Kept here so fill math does not import pandas (paper_trade does).
    """
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


def after_fee_pnl(shares: int, entry: float, exit_px: float, *,
                  side: str, fees) -> float:
    """Round-trip $ after Futubull fees. 1-share name-day grade."""
    shares = int(shares)
    if shares < 1 or entry is None or exit_px is None:
        return 0.0
    entry = float(entry)
    exit_px = float(exit_px)
    if entry <= 0 or exit_px <= 0:
        return 0.0
    if side == "short":
        fee_in = order_fees(shares, entry, "sell", fees)
        fee_out = order_fees(shares, exit_px, "buy", fees)
        return (shares * entry - fee_in) - (shares * exit_px + fee_out)
    fee_in = order_fees(shares, entry, "buy", fees)
    fee_out = order_fees(shares, exit_px, "sell", fees)
    return (shares * exit_px - fee_out) - (shares * entry + fee_in)


def load_bar_store(path: Path = OHLC_PATH) -> dict:
    """(date, ticker) → {open, high, low, close} from the official tape.

    Same parquet as ``price_store`` / ``open_close_grade`` / the
    ticker-lookback ripper. No network. Missing file → empty store.
    """
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
        if not d or not t:
            continue
        out[(d, t)] = {
            "open": _finite(rec.get("open")),
            "high": _finite(rec.get("high")),
            "low": _finite(rec.get("low")),
            "close": _finite(rec.get("close")),
            "src": "ohlc.parquet",
        }
    return out


def clock_bar(ticker: str, date: str, *, store: dict | None = None,
              bars=None) -> dict:
    """Official regular-session OHLC. Never Gap / last / Finviz Price."""
    t = _tick(ticker)
    d = str(date or "")[:10]
    empty = {"open": None, "high": None, "low": None, "close": None,
             "src": None}
    if not t or not d:
        return empty
    if bars is not None:
        raw = bars.get((t, d)) or bars.get((d, t)) or {}
        if raw:
            return {
                "open": _finite(raw.get("open")),
                "high": _finite(raw.get("high")),
                "low": _finite(raw.get("low")),
                "close": _finite(raw.get("close")),
                "src": raw.get("src") or "bars",
            }
    if store:
        hit = store.get((d, t))
        if hit:
            return dict(hit)
    try:
        from . import factor_mine as fm
        raw = fm._bar(t, d, None) or {}
    except Exception:
        raw = {}
    if not raw:
        try:
            from . import ticker_lookback as tl
            raw = tl.session_bar(t, d) or {}
        except Exception:
            raw = {}
    return {
        "open": _finite(raw.get("open")),
        "high": _finite(raw.get("high")),
        "low": _finite(raw.get("low")),
        "close": _finite(raw.get("close")),
        "src": raw.get("src") or ("session_bar" if raw else None),
    }


def prior_ohlc(ticker: str, date: str, *, store: dict | None = None,
               bars=None, n: int = PRIOR_N) -> list[dict]:
    """Completed OHLC sessions with date < ``date``. Clock-clean.

    Prefers the existing ``ohlc_ripper.prior_bars`` (same store the
    hot-score ripper uses). Falls back to the parquet store / injected
    bars. Never includes the session being filled.
    """
    t = _tick(ticker)
    d = str(date or "")[:10]
    n = int(n or PRIOR_N)
    if not t or not d or n <= 0:
        return []
    if bars is not None:
        rows = []
        for (a, b), rec in bars.items():
            td = a if len(str(a)) == 10 else b
            tk = b if len(str(a)) == 10 else a
            if _tick(tk) != t:
                continue
            ds = str(td)[:10]
            if ds < d:
                rows.append({
                    "date": ds,
                    "open": _finite(rec.get("open")),
                    "high": _finite(rec.get("high")),
                    "low": _finite(rec.get("low")),
                    "close": _finite(rec.get("close")),
                })
        rows.sort(key=lambda r: r["date"])
        return rows[-n:]
    try:
        from . import ohlc_ripper as ohlc
        got = ohlc.prior_bars(t, d, n=n) or []
        if got:
            return [
                {
                    "date": str(b.get("date") or "")[:10],
                    "open": _finite(b.get("open")),
                    "high": _finite(b.get("high")),
                    "low": _finite(b.get("low")),
                    "close": _finite(b.get("close")),
                }
                for b in got
                if str(b.get("date") or "")[:10] < d
            ][-n:]
    except Exception:
        pass
    if not store:
        return []
    rows = []
    for (ds, tk), rec in store.items():
        if tk == t and ds < d:
            rows.append({"date": ds, **{k: rec.get(k) for k in
                                       ("open", "high", "low", "close")}})
    rows.sort(key=lambda r: r["date"])
    return rows[-n:]


def calibrate_slip(prior: list[dict] | None) -> dict:
    """Adverse-slip stats from prior OHLC only. No session / future bars.

    ``buy_adverse_rate`` = share of prior days where close > open
    (tape ran away from a long market). Same idea inverted for shorts.
    α is DELAY_FRAC blended toward the typical adverse-side share of
    the prior range, clamped — never fit on today's close.
    """
    buy_rooms: list[float] = []
    sell_rooms: list[float] = []
    buy_share: list[float] = []
    sell_share: list[float] = []
    n_buy_adv = 0
    n_sell_adv = 0
    n = 0
    for b in prior or []:
        o = _finite((b or {}).get("open"))
        h = _finite((b or {}).get("high"))
        lo = _finite((b or {}).get("low"))
        c = _finite((b or {}).get("close"))
        if o is None or o <= 0:
            continue
        n += 1
        if h is not None:
            buy_rooms.append(max(0.0, (h - o) / o))
        if lo is not None:
            sell_rooms.append(max(0.0, (o - lo) / o))
        rng = None
        if h is not None and lo is not None:
            rng = h - lo
        if rng is not None and rng > 1e-12:
            if h is not None:
                buy_share.append(max(0.0, (h - o) / rng))
            if lo is not None:
                sell_share.append(max(0.0, (o - lo) / rng))
        if c is not None and c > o:
            n_buy_adv += 1
        if c is not None and c < o:
            n_sell_adv += 1

    def _med(xs):
        return None if not xs else float(statistics.median(xs))

    def _alpha(shares):
        if len(shares) < 5:
            return DELAY_FRAC
        med = float(statistics.median(shares))
        # DELAY_FRAC is the delay; typical adverse-side share scales it
        # so a name that usually opens at the high does not invent room.
        return max(ALPHA_LO, min(ALPHA_HI, DELAY_FRAC * (0.5 + med)))

    return {
        "n": n,
        "buy_adverse_rate": None if not n else round(n_buy_adv / n, 4),
        "sell_adverse_rate": None if not n else round(n_sell_adv / n, 4),
        "median_buy_room": None if _med(buy_rooms) is None
        else round(_med(buy_rooms), 6),
        "median_sell_room": None if _med(sell_rooms) is None
        else round(_med(sell_rooms), 6),
        "alpha_buy": round(_alpha(buy_share), 4),
        "alpha_sell": round(_alpha(sell_share), 4),
        "delay_frac": DELAY_FRAC,
    }


def ideal_fill(bar: dict | None, side: str = "long"):
    """Official 09:30 open. Missing open is a skip — never Gap / close."""
    o = _finite((bar or {}).get("open"))
    if o is None or o <= 0:
        return None, "no_open"
    return float(o), "ideal_open"


def market_fill(bar: dict | None, side: str, cal: dict | None):
    """Delayed market: walk α of the post-open adverse room.

    Long: open + α·max(0, high−open). Short: open − α·max(0, open−low).
    If this session's high/low is missing, fall back to prior median
    room × open (still clock-clean). Clip to the session range so we
    never invent a print outside the day's tape.
    """
    o = _finite((bar or {}).get("open"))
    h = _finite((bar or {}).get("high"))
    lo = _finite((bar or {}).get("low"))
    if o is None or o <= 0:
        return None, "no_open"
    cal = cal or {}
    long = str(side or "long") != "short"
    alpha = float(cal.get("alpha_buy" if long else "alpha_sell")
                  or DELAY_FRAC)
    alpha = max(ALPHA_LO, min(ALPHA_HI, alpha))
    if long:
        if h is not None:
            room = max(0.0, h - o)
        else:
            med = cal.get("median_buy_room")
            room = o * float(med or 0.0)
        fill = o + alpha * room
    else:
        if lo is not None:
            room = max(0.0, o - lo)
        else:
            med = cal.get("median_sell_room")
            room = o * float(med or 0.0)
        fill = o - alpha * room
    if h is not None:
        fill = min(fill, h)
    if lo is not None:
        fill = max(fill, lo)
    if fill <= 0:
        return None, "no_open"
    return float(fill), "market"


def limit_at_open(bar: dict | None, side: str):
    """LIMIT at the intended 09:30 open — fill only on a trade-through.

    A buy limit sent at the open (not in the auction) fills if the
    session low prints *through* the open. If the bar opened at the
    low and ran, miss. Shorts invert (high must print through).
    """
    o = _finite((bar or {}).get("open"))
    h = _finite((bar or {}).get("high"))
    lo = _finite((bar or {}).get("low"))
    if o is None or o <= 0:
        return None, "no_open"
    long = str(side or "long") != "short"
    if long:
        if lo is None:
            return None, "no_low"
        if lo < o - 1e-12:
            return float(o), "limit_fill"
        return None, "limit_miss"
    if h is None:
        return None, "no_high"
    if h > o + 1e-12:
        return float(o), "limit_fill"
    return None, "limit_miss"


def limit_at_prior(bar: dict | None, side: str, prior_px):
    """LIMIT at the prior intended print (prior close). Fill or miss.

    Long fills if the session traded at or through prior_px (low ≤
    prior). Fill px is min(open, prior) so a gap-down still uses the
    09:30 print, not a fiction below the tape. Shorts invert.
    """
    px = _finite(prior_px)
    o = _finite((bar or {}).get("open"))
    h = _finite((bar or {}).get("high"))
    lo = _finite((bar or {}).get("low"))
    if px is None or px <= 0:
        return None, "no_prior"
    if o is None or o <= 0:
        return None, "no_open"
    long = str(side or "long") != "short"
    if long:
        if lo is None:
            return None, "no_low"
        if lo > px + 1e-12:
            return None, "limit_miss"
        fill = min(o, px)
        if h is not None:
            fill = min(fill, h)
        if fill <= 0:
            return None, "no_open"
        return float(fill), "limit_fill"
    if h is None:
        return None, "no_high"
    if h < px - 1e-12:
        return None, "limit_miss"
    fill = max(o, px)
    if lo is not None:
        fill = max(fill, lo)
    return float(fill), "limit_fill"


def align_sleeves(sleeves: dict | None) -> list[dict]:
    """Zip sleeves by (date, ticker, side). Empty / unequal is fine.

    Never indexes ``sleeve_a[i]`` against ``sleeve_b[i]``. Missing
    members are ``None``. Used by the board and the unit tests so an
    empty flatten list cannot raise IndexError.
    """
    sleeves = sleeves or {}
    keys: list[tuple] = []
    seen: set[tuple] = set()
    by: dict[str, dict] = {name: {} for name in sleeves}
    for name, rows in sleeves.items():
        for r in rows or []:
            if not isinstance(r, dict):
                continue
            k = (str(r.get("date") or "")[:10],
                 _tick(r.get("ticker")),
                 "short" if (r.get("side") or "long") == "short" else "long")
            if not k[0] or not k[1]:
                continue
            by[name][k] = r
            if k not in seen:
                seen.add(k)
                keys.append(k)
    keys.sort()
    out = []
    for k in keys:
        out.append({
            "date": k[0],
            "ticker": k[1],
            "side": k[2],
            "sleeves": {name: by[name].get(k) for name in by},
        })
    return out


def zip_sleeve_rows(*lists):
    """``zip_longest`` over sleeve fire lists. Empty / unequal → None.

    Callers must not assume equal length. This helper exists so a
    stray ``rows[i]`` cannot IndexError when one sleeve is thin.
    """
    cleaned = [list(x or []) for x in lists]
    if not cleaned:
        return []
    return list(zip_longest(*cleaned, fillvalue=None))


def grade_intent(intent: dict, *, store: dict | None = None, bars=None,
                 fees=None, cal: list[str] | None = None,
                 prior: list[dict] | None = None) -> dict:
    """One 09:30 order across ideal / MARKET / LIMIT. After-fee o2c."""
    date = str(intent.get("date") or "")[:10]
    ticker = _tick(intent.get("ticker"))
    side = "short" if (intent.get("side") or "long") == "short" else "long"
    fees = fees if fees is not None else load_fees()
    bar = clock_bar(ticker, date, store=store, bars=bars)
    if prior is None:
        prior = prior_ohlc(ticker, date, store=store, bars=bars)
    slip = calibrate_slip(prior)
    prior_px = None
    if prior:
        prior_px = _finite(prior[-1].get("close"))
    if prior_px is None and cal:
        prev = prior_session(cal, date)
        if prev:
            prior_px = clock_bar(ticker, prev, store=store, bars=bars).get("close")
    exit_px = _finite(bar.get("close"))
    ideal_px, ideal_how = ideal_fill(bar, side)
    mkt_px, mkt_how = market_fill(bar, side, slip)
    lim_o_px, lim_o_how = limit_at_open(bar, side)
    lim_p_px, lim_p_how = limit_at_prior(bar, side, prior_px)

    def _leg(px, how):
        filled = px is not None and how not in (
            "no_open", "no_low", "no_high", "no_prior", "limit_miss")
        pnl = None
        if filled and exit_px is not None:
            pnl = round(after_fee_pnl(
                SHARES, float(px), float(exit_px), side=side, fees=fees), 4)
        adverse = None
        if filled and ideal_px is not None:
            if side == "short":
                adverse = float(px) < float(ideal_px) - 1e-12
            else:
                adverse = float(px) > float(ideal_px) + 1e-12
        return {
            "fill": None if px is None else round(float(px), 4),
            "how": how,
            "filled": bool(filled),
            "pnl": pnl,
            "win": None if pnl is None else bool(pnl > 0),
            "adverse": adverse,
        }

    return {
        "date": date,
        "ticker": ticker,
        "side": side,
        "sleeve": intent.get("sleeve"),
        "order_sent": ORDER_SENT,
        "open": bar.get("open"),
        "high": bar.get("high"),
        "low": bar.get("low"),
        "close": exit_px,
        "prior_px": None if prior_px is None else round(float(prior_px), 4),
        "src": bar.get("src"),
        "cal": {k: slip[k] for k in (
            "n", "buy_adverse_rate", "sell_adverse_rate",
            "alpha_buy", "alpha_sell", "delay_frac")},
        "ideal": _leg(ideal_px, ideal_how),
        "market": _leg(mkt_px, mkt_how),
        "limit_open": _leg(lim_o_px, lim_o_how),
        "limit_prior": _leg(lim_p_px, lim_p_how),
    }


def fire_stats(rows: list[dict] | None, reality: str) -> dict:
    """n fires / filled / after-fee hit / $ for one reality column."""
    rows = [r for r in (rows or []) if isinstance(r, dict)]
    n = len(rows)
    filled = []
    for r in rows:
        leg = (r.get(reality) or {})
        if leg.get("filled") and leg.get("pnl") is not None:
            filled.append(leg)
    wins = [f for f in filled if (f.get("pnl") or 0) > 0]
    adverse = [f for f in filled if f.get("adverse") is True]
    n_f = len(filled)
    n_miss = sum(1 for r in rows
                 if not ((r.get(reality) or {}).get("filled")))
    pnl = round(sum(float(f.get("pnl") or 0) for f in filled), 2)
    return {
        "reality": reality,
        "n_fires": n,
        "n_filled": n_f,
        "n_miss": n_miss,
        "n_wins": len(wins),
        "n_adverse": len(adverse),
        "win_rate": None if not n_f else round(len(wins) / n_f, 4),
        "adverse_rate": None if not n_f else round(len(adverse) / n_f, 4),
        "pnl": pnl,
    }


def decide_verdict(*, label: str, n_fires: int, n_filled: int,
                   win_rate, pnl, ideal_pnl=None) -> dict:
    """One-share name-day diagnostics cannot approve a portfolio strategy."""
    value = _finite(pnl)
    thin = int(n_filled or 0) < KEEP_MIN_FIRES
    verdict = ("INSUFFICIENT_EVIDENCE" if thin or value is None else
               "NEGATIVE_DIAGNOSTIC" if value <= 0 else "POSITIVE_DIAGNOSTIC")
    return {"label": verdict, "keep": False,
            "why": f"{label}: {verdict}; one-share same-day stress only. "
                   "Use stateful portfolio replay and prospective net expectancy for promotion.",
            "n_fires": int(n_fires or 0), "n_filled": int(n_filled or 0),
            "win_rate": win_rate, "pnl": value, "thin": thin,
            "vs_ideal_pnl": round(value - float(ideal_pnl), 2)
            if value is not None and ideal_pnl is not None else None}


def sleeve_report(name: str, rows: list[dict]) -> dict:
    """KEEP/KILL block for one shopping list across the four columns."""
    cols = {r: fire_stats(rows, r) for r in REALITIES}
    ideal_pnl = cols["ideal"]["pnl"]
    verdicts = {}
    for r in REALITIES:
        st = cols[r]
        verdicts[r] = decide_verdict(
            label=f"{name} / {r}",
            n_fires=st["n_fires"], n_filled=st["n_filled"],
            win_rate=st["win_rate"], pnl=st["pnl"],
            ideal_pnl=ideal_pnl if r != "ideal" else None,
        )
    return {
        "name": name,
        "n_intents": len(rows),
        "columns": cols,
        "verdicts": verdicts,
    }


def _dedupe_intents(rows: list[dict]) -> list[dict]:
    """One ticker, one side per morning. First claim wins."""
    seen: set[tuple] = set()
    out = []
    for r in rows:
        k = (r.get("date"), _tick(r.get("ticker")), r.get("side"))
        if k in seen or not k[0] or not k[1]:
            continue
        seen.add(k)
        out.append(r)
    return out


def sleeve_intents(panel: dict | None, *, recipes=None) -> dict[str, list]:
    """Leak-free 09:30 shopping lists. Does not send live orders.

    Hard-red sit mornings still list would-haves (same as OPEN_CLOSE_GRADE
    / HARD_RED_SIT research). Live flatten_robust continues to sit.
    """
    panel = panel or {}
    by_date = panel.get("by_date") or {}
    if not by_date:
        for row in panel.get("rows") or []:
            by_date.setdefault(row.get("date"), []).append(row)
    dates = list(panel.get("session_dates") or sorted(by_date))
    empty = {k: [] for k in SLEEVE_ORDER}
    if not dates and not by_date:
        return empty
    try:
        from . import combo_broker as cb
        from . import factor_mine as fm
        from . import factor_mine_combo as fmc
    except ImportError:
        return empty
    rec_by = {r["name"]: r for r in (recipes or fm.build_recipes())}
    out = {SLEEVE_COMBO: [], SLEEVE_HOT4: [], SLEEVE_FLAT: []}

    hot = rec_by.get(SLEEVE_HOT4)
    flat = rec_by.get("flatten_h3") or rec_by.get("flatten_h5")
    combo_recs = []
    try:
        spec = cb.combo_spec(SLEEVE_COMBO)
        combo_recs = fmc.recs_for_spec(spec, rec_by)
    except Exception:
        combo_recs = []

    for date in dates:
        rows = by_date.get(date) or []
        if hot is not None:
            for r in fm.pick_day(rows, hot):
                out[SLEEVE_HOT4].append({
                    "date": date, "ticker": _tick(r.get("ticker")),
                    "side": hot.get("side") or "long",
                    "sleeve": SLEEVE_HOT4,
                })
        if flat is not None:
            for r in fm.pick_day(rows, flat):
                out[SLEEVE_FLAT].append({
                    "date": date, "ticker": _tick(r.get("ticker")),
                    "side": flat.get("side") or "long",
                    "sleeve": SLEEVE_FLAT,
                })
        combo_day = []
        for rec in combo_recs:
            for r in fm.pick_day(rows, rec):
                combo_day.append({
                    "date": date, "ticker": _tick(r.get("ticker")),
                    "side": rec.get("side") or "long",
                    "sleeve": SLEEVE_COMBO,
                    "owner": rec.get("name"),
                })
        out[SLEEVE_COMBO].extend(_dedupe_intents(combo_day))
    return {k: _dedupe_intents(v) for k, v in out.items()}


def load_panel(path: Path = PANEL_PATH) -> dict:
    if not path.exists():
        return {}
    raw = json.loads(path.read_text(encoding="utf-8"))
    by_date = raw.get("by_date") or {}
    if not by_date:
        for row in raw.get("rows") or []:
            by_date.setdefault(row["date"], []).append(row)
        raw = dict(raw)
        raw["by_date"] = by_date
    return raw


def grade_sleeves(sleeves: dict[str, list], *, store=None, bars=None,
                  fees=None, cal=None) -> dict[str, list]:
    fees = fees if fees is not None else load_fees()
    graded = {}
    for name, intents in (sleeves or {}).items():
        rows = []
        for it in intents or []:
            rec = grade_intent(
                it, store=store, bars=bars, fees=fees, cal=cal)
            rec["sleeve"] = name
            rows.append(rec)
        graded[name] = rows
    return graded


def build(*, panel: dict | None = None, store: dict | None = None,
          bars=None, fees=None, from_date: str | None = None,
          to_date: str | None = None) -> dict:
    from . import book_era
    panel = panel if panel is not None else load_panel()
    fees = fees if fees is not None else load_fees()
    store = store if store is not None else load_bar_store()
    cal = list(panel.get("session_dates") or [])
    if from_date:
        cal = [d for d in cal if d >= from_date]
    if to_date:
        cal = [d for d in cal if d <= to_date]
    if not cal:
        cal = sorted({d for d, _ in (store or {})})
        if from_date:
            cal = [d for d in cal if d >= from_date]
        if to_date:
            cal = [d for d in cal if d <= to_date]
    if panel.get("by_date") and cal:
        slim = dict(panel)
        slim["session_dates"] = cal
        slim["by_date"] = {d: (panel.get("by_date") or {}).get(d) or []
                           for d in cal}
        panel = slim
    sleeves = sleeve_intents(panel)
    graded = grade_sleeves(sleeves, store=store, bars=bars,
                           fees=fees, cal=cal)
    reports = {name: sleeve_report(name, graded.get(name) or [])
               for name in SLEEVE_ORDER}
    aligned = align_sleeves(graded)
    now = datetime.now(TZ).isoformat()
    headline = _headline(reports)
    return {
        "generated_at": now,
        "order_sent": ORDER_SENT,
        "live_wire": False,
        "live_untouched": list(LIVE_UNTOUCHED),
        "research_only": True,
        "from_date": cal[0] if cal else from_date or book_era.DASHBOARD_START,
        "to_date": cal[-1] if cal else to_date,
        "n_sessions": len(cal),
        "keep_bar": {"min_fires": KEEP_MIN_FIRES, "win": KEEP_WIN,
                     "shares": SHARES, "delay_frac": DELAY_FRAC},
        "sleeves": SLEEVE_ORDER,
        "realities": list(REALITIES),
        "reports": reports,
        "headline": headline,
        "n_aligned": len(aligned),
        "sample": aligned[:12],
        "note": (
            "Research overlay only. Live flatten_robust, hard-red sit, "
            "and Webull paper execution are untouched. After-fee P&L "
            "is 1-share Futubull round-trip vs same-day close. Ideal "
            "open is the zero-slip baseline — not a live fill."
        ),
    }


def _headline(reports: dict) -> str:
    bits = []
    for name in SLEEVE_ORDER:
        rep = reports.get(name) or {}
        v = (rep.get("verdicts") or {}).get("market") or {}
        bits.append(
            f"{name} MARKET {(v.get('label') or 'KILL')}"
        )
    why = ((reports.get(SLEEVE_COMBO) or {}).get("verdicts") or {}).get(
        "market") or {}
    return (
        "Open-bell fills at 09:30 ET: "
        + " · ".join(bits)
        + ". "
        + str(why.get("why") or "")
    )


def render_md(payload: dict) -> str:
    reports = payload.get("reports") or {}
    lines = [
        "# Open-bell slippage / no-fill — research overlay",
        "",
        str(payload.get("headline") or ""),
        "",
        "Research only. Live `flatten_robust`, hard-red sit, and Webull "
        "paper execution are **not** changed. Orders are modeled as "
        f"sent at **{payload.get('order_sent') or ORDER_SENT}** "
        "(same clock as the Open 09:30 pack / PR #239). This board "
        "does not wire MARKET or LIMIT into the live book.",
        "",
        "## Plain board",
        "",
        "The published cash books fill at the official 09:30 open. "
        "That is the **ideal / zero-slip** column. Two other realities "
        "use the same shopping lists and the same official OHLC store:",
        "",
        "1. **MARKET-like** — a delayed market order walks into the "
        "post-open range. Buys slip toward the high; shorts slip "
        "toward the low. α comes from *prior* sessions only "
        f"(default delay {DELAY_FRAC:.0%} of the adverse room). "
        "Same-day high/low are the realized path after the order was "
        "sent — not a 09:30 gate. Historically the adverse side of "
        "the open prints on a majority of sessions.",
        "2. **LIMIT** — working order at the intended open, or at the "
        "prior close. Fill if the session trades through; miss if it "
        "does not. A miss is $0 after fees (no position). Daily OHLC "
        "has no natural partial, so a touch is a full fill.",
        "",
        "Grade is **1 share, Futubull fees, open → same-day close**. "
        "After-fee P&L is always shown versus the ideal-open baseline. "
        "A fat ideal Book% that dies under slip is still a KILL for "
        "the realistic column. North star ~2%/day after fees is "
        "context — this sim is meant to stress fantasy paper fills "
        "that assumed the signal-day close.",
        "",
        f"Window `{payload.get('from_date')} → {payload.get('to_date')}` "
        f"({payload.get('n_sessions')} sessions). KEEP bar: "
        f"**≥{KEEP_MIN_FIRES} filled fires** and "
        f"**>{100 * KEEP_WIN:.0f}% after-fee hit rate**.",
        "",
        "## KEEP / KILL vs ideal open",
        "",
        "| Sleeve | Reality | Intended | Filled | Miss | After-fee win | "
        "After-fee $ | vs ideal $ | Verdict |",
        "|---|---|---:|---:|---:|---:|---:|---:|---|",
    ]
    for name in payload.get("sleeves") or SLEEVE_ORDER:
        rep = reports.get(name) or {}
        cols = rep.get("columns") or {}
        verd = rep.get("verdicts") or {}
        for r in REALITIES:
            st = cols.get(r) or {}
            v = verd.get(r) or {}
            vs = v.get("vs_ideal_pnl")
            lines.append(
                f"| `{name}` | {r} | {st.get('n_fires') or 0} | "
                f"{st.get('n_filled') or 0} | {st.get('n_miss') or 0} | "
                f"{_pct(st.get('win_rate'))} | {_n(st.get('pnl'))} | "
                f"{'—' if vs is None else f'${vs:+.2f}'} | "
                f"**{v.get('label') or 'KILL'}** |"
            )
    lines += [
        "",
        "### Why, in plain language",
        "",
    ]
    for name in payload.get("sleeves") or SLEEVE_ORDER:
        verd = (reports.get(name) or {}).get("verdicts") or {}
        for r in ("market", "limit_open", "limit_prior"):
            why = (verd.get(r) or {}).get("why")
            if why:
                lines.append(f"- {why}")
    lines += [
        "",
        "## After-fee caveat",
        "",
        "Every graded fill pays the Futubull US round-trip on 1 share "
        "(`paper_trade.order_fees` / `00_grounding/futubull_fees.json`). "
        "A MARKET fill that is only a few cents worse than the open "
        "can flip a tiny winner into a loser after fees. LIMIT misses "
        "are $0 — they do not get a free pass as 'no loss' in the hit "
        "rate (they are not fills). Ideal-open P&L is the zero-slip "
        "baseline, not a live number.",
        "",
        "## Method (code names after the English)",
        "",
        f"- Clock: `{ORDER_SENT}` send. `ideal_fill` = official open. "
        f"`market_fill` = open ± `DELAY_FRAC` (`{DELAY_FRAC}`) of the "
        "post-open adverse room, α from `calibrate_slip` on "
        f"`ohlc_ripper.prior_bars` (date < session, n={PRIOR_N}). "
        "`limit_at_open` requires a trade-through (`low < open` buy / "
        "`high > open` short). `limit_at_prior` uses prior close.",
        "- Shopping lists: leak-free panel `pick_day` for "
        f"`{SLEEVE_COMBO}` (Webull paper combo), `{SLEEVE_HOT4}`, and "
        f"`flatten_h3` would-haves (`{SLEEVE_FLAT}`). Hard-red sit "
        "mornings still list names so the fill comparison is the "
        "card, not leftover cash. Live sit is unchanged.",
        "- Prices: `data/prices/ohlc.parquet` via `load_bar_store` / "
        "`ticker_lookback.session_bar`. No yfinance. No new ripper.",
        "- `align_sleeves` / `zip_sleeve_rows` zip unequal or empty "
        "sleeves without indexing — an empty flatten list is not an "
        "IndexError.",
        "",
        f"Live untouched: {', '.join(f'`{n}`' for n in LIVE_UNTOUCHED)}.",
        "",
        "Regenerate: `PYTHONPATH=. python3 -m src.open_bell_slip --write`.",
        "",
        "Dashboard overlay: [factor-mine open-bell slip]"
        "(../dashboard/factor-mine/open-bell-slip.html) "
        "(GitHub Pages `.io` research section).",
        "",
    ]
    return "\n".join(lines) + "\n"


def render_html(payload: dict) -> str:
    """Self-contained research overlay. Dark board, same family as .io."""
    reports = payload.get("reports") or {}
    rows = []
    for name in payload.get("sleeves") or SLEEVE_ORDER:
        rep = reports.get(name) or {}
        cols = rep.get("columns") or {}
        verd = rep.get("verdicts") or {}
        for r in REALITIES:
            st = cols.get(r) or {}
            v = verd.get(r) or {}
            vs = v.get("vs_ideal_pnl")
            cls = "pos" if v.get("keep") else "neg"
            rows.append(
                "<tr>"
                f"<td class='tick'>{_esc(name)}</td>"
                f"<td>{_esc(r)}</td>"
                f"<td>{st.get('n_fires') or 0}</td>"
                f"<td>{st.get('n_filled') or 0}</td>"
                f"<td>{st.get('n_miss') or 0}</td>"
                f"<td>{_esc(_pct(st.get('win_rate')))}</td>"
                f"<td>{_esc(_n(st.get('pnl')))}</td>"
                f"<td>{'—' if vs is None else f'${vs:+.2f}'}</td>"
                f"<td class='{cls}'><b>{_esc(v.get('label') or 'KILL')}</b></td>"
                "</tr>"
            )
    whys = []
    for name in payload.get("sleeves") or SLEEVE_ORDER:
        verd = (reports.get(name) or {}).get("verdicts") or {}
        for r in ("market", "limit_open", "limit_prior"):
            why = (verd.get(r) or {}).get("why")
            if why:
                whys.append(f"<li>{_esc(why)}</li>")
    return f"""<!DOCTYPE html>
<html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Open-bell slip — research overlay</title>
<style>
:root{{--bg:#0f1420;--card:#171e2e;--line:#262f45;--fg:#dfe6f2;--mut:#8b96ab;
--pos:#4ade80;--neg:#f87171;--gold:#fbbf24}}
*{{box-sizing:border-box}}body{{margin:0;background:var(--bg);color:var(--fg);
font:14px/1.45 -apple-system,Segoe UI,Roboto,sans-serif}}
.wrap{{max-width:1100px;margin:0 auto;padding:16px 14px 32px}}
h1{{font-size:20px;margin:0 0 6px}}h2{{font-size:15px;margin:16px 0 8px;color:#aeb9cf}}
.mut{{color:var(--mut)}}.pos{{color:var(--pos)}}.neg{{color:var(--neg)}}
.card{{background:var(--card);border:1px solid var(--line);border-radius:10px;
padding:10px 12px;margin:0 0 12px}}
table{{width:100%;border-collapse:collapse;font-size:12px}}
th,td{{padding:5px 6px;border-bottom:1px solid var(--line);text-align:right}}
th{{color:var(--mut)}}td:first-child,th:first-child,td:nth-child(2),th:nth-child(2)
{{text-align:left}}
.tick{{font-family:ui-monospace,Menlo,Consolas,monospace}}
a{{color:#93c5fd}}
</style></head><body><div class="wrap">
<h1>Open-bell slippage / no-fill</h1>
<p class="mut">Research overlay · orders sent { _esc(payload.get("order_sent") or ORDER_SENT) }
· window { _esc(payload.get("from_date")) } → { _esc(payload.get("to_date")) }
({ payload.get("n_sessions") or 0 } sessions) · generated { _esc(payload.get("generated_at")) }</p>
<div class="card"><b>{ _esc((payload.get("headline") or "").split(".")[0]) }</b>
<p class="mut">{ _esc(payload.get("note")) }</p>
<p>Live <code>flatten_robust</code>, hard-red sit, and Webull paper are
<b>untouched</b>. Ideal open is the zero-slip baseline — not a live fill.</p>
</div>
<p class="mut">
<a href="./">factor mine</a> ·
<a href="../strategy-board/">strategy board</a> ·
<a href="https://github.com/SRoyaltyy/fullscan/blob/main/03_scoreboard/OPEN_BELL_SLIP.md">OPEN_BELL_SLIP.md</a>
</p>
<h2>Ideal open vs MARKET-like vs LIMIT</h2>
<p class="mut">KEEP bar ≥{KEEP_MIN_FIRES} filled fires and &gt;{100 * KEEP_WIN:.0f}%
after-fee hit rate. After-fee $ is 1-share Futubull round-trip vs same-day close.</p>
<div style="overflow-x:auto"><table>
<tr><th>Sleeve</th><th>Reality</th><th>Intended</th><th>Filled</th><th>Miss</th>
<th>After-fee win</th><th>After-fee $</th><th>vs ideal $</th><th>Verdict</th></tr>
{ "".join(rows) }
</table></div>
<h2>Why</h2>
<ul>{ "".join(whys) }</ul>
<h2>What the columns mean</h2>
<ol>
<li><b>Ideal open</b> — official 09:30 print (published cash-book fill).</li>
<li><b>MARKET-like</b> — delayed market walks {DELAY_FRAC:.0%} of the post-open
adverse room (high−open buy / open−low short). α from prior OHLC only.</li>
<li><b>LIMIT at open</b> — fill only if the session trades <em>through</em> the
open after the print. Opened at the extreme and ran = miss.</li>
<li><b>LIMIT at prior px</b> — working order at yesterday's close. Gap-and-go
that never comes back = miss.</li>
</ol>
<p class="mut">Code names: <code>ideal_fill</code>, <code>market_fill</code>,
<code>limit_at_open</code>, <code>limit_at_prior</code>, <code>calibrate_slip</code>.
Regenerate with <code>python -m src.open_bell_slip --write</code>.</p>
</div></body></html>
"""


def _esc(s) -> str:
    return (str(s or "").replace("&", "&amp;").replace("<", "&lt;")
            .replace(">", "&gt;").replace('"', "&quot;"))


def overlay_snippet() -> str:
    """Fetch-and-paint block for factor-mine / strategy-board pages."""
    return """
<details id="openBellSlip" class="how" open>
<summary>Open-bell slip — research overlay (not a wire)</summary>
<p class="mut">Ideal 09:30 open vs MARKET-like delay/slip vs LIMIT fill/miss.
KEEP bar ≥30 filled fires and &gt;55% after-fee hit rate. Live
<code>flatten_robust</code>, hard-red sit, and Webull paper are untouched.</p>
<div id="openBellSlipBody" class="mut">loading open-bell slip…</div>
</details>
<script>
(function(){
  var host = document.getElementById('openBellSlipBody');
  if(!host) return;
  var urls = ['open_bell_slip.json','./open_bell_slip.json',
              '../factor-mine/open_bell_slip.json'];
  function paint(d){
    var reps = d.reports || {};
    var names = d.sleeves || Object.keys(reps);
    var html = '<p>'+ (d.headline || d.note || '') +'</p>';
    html += '<div class="tbl-wrap"><table><tr><th>Sleeve</th><th>Reality</th>'
      +'<th>Filled</th><th>After-fee win</th><th>After-fee $</th>'
      +'<th>vs ideal $</th><th>Verdict</th></tr>';
    names.forEach(function(name){
      var rep = reps[name] || {};
      var cols = rep.columns || {};
      var verd = rep.verdicts || {};
      ['ideal','market','limit_open','limit_prior'].forEach(function(r){
        var st = cols[r] || {}; var v = verd[r] || {};
        var vs = v.vs_ideal_pnl;
        html += '<tr><td>'+name+'</td><td>'+r+'</td><td>'
          +(st.n_filled||0)+'/'+(st.n_fires||0)+'</td><td>'
          +(st.win_rate==null?'—':(100*st.win_rate).toFixed(1)+'%')+'</td><td>'
          +(st.pnl==null?'—':(st.pnl>=0?'+':'')+Number(st.pnl).toFixed(2))
          +'</td><td>'+(vs==null?'—':((vs>=0?'+':'')+Number(vs).toFixed(2)))
          +'</td><td><b>'+(v.label||'KILL')+'</b></td></tr>';
      });
    });
    html += '</table></div><p class="mut"><a href="open-bell-slip.html" style="color:#93c5fd">full overlay</a>'
      +(d.live_untouched?' · live untouched: '+(d.live_untouched||[]).join(', '):'')
      +'</p>';
    host.innerHTML = html;
  }
  function tryFetch(i){
    if(i>=urls.length){ host.textContent = 'open-bell slip JSON not on this deploy yet. Run python -m src.open_bell_slip --write.'; return; }
    fetch(urls[i], {cache:'no-store'}).then(function(r){ if(!r.ok) throw 0; return r.json(); })
      .then(paint).catch(function(){ tryFetch(i+1); });
  }
  tryFetch(0);
})();
</script>
"""


def write_outputs(payload: dict) -> None:
    OUT_MD.parent.mkdir(parents=True, exist_ok=True)
    DASH_DIR.mkdir(parents=True, exist_ok=True)
    STRAT_JSON.parent.mkdir(parents=True, exist_ok=True)
    slim = dict(payload)
    text = json.dumps(slim, indent=2)
    OUT_JSON.write_text(text, encoding="utf-8")
    DASH_JSON.write_text(text, encoding="utf-8")
    STRAT_JSON.write_text(text, encoding="utf-8")
    OUT_MD.write_text(render_md(payload), encoding="utf-8")
    DASH_HTML.write_text(render_html(payload), encoding="utf-8")


def run(*, write: bool = True, from_date: str | None = None,
        to_date: str | None = None, panel=None, store=None,
        bars=None, fees=None) -> dict:
    payload = build(
        panel=panel, store=store, bars=bars, fees=fees,
        from_date=from_date, to_date=to_date)
    if write:
        write_outputs(payload)
        print(f"[open-bell-slip] wrote {OUT_MD}")
        print(f"[open-bell-slip] wrote {OUT_JSON}")
        print(f"[open-bell-slip] wrote {DASH_HTML}")
    return payload


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(
        description="Open-bell MARKET/LIMIT fill research "
                    "(does not change live flatten_robust / hard-red sit).")
    ap.add_argument("--from-date", default=None)
    ap.add_argument("--to-date", default=None)
    ap.add_argument("--write", action="store_true")
    ap.add_argument("--no-write", action="store_false", dest="write")
    args = ap.parse_args(argv)
    write = bool(args.write)
    payload = run(write=write, from_date=args.from_date,
                  to_date=args.to_date)
    print(
        f"[open-bell-slip] sessions={payload.get('n_sessions')} "
        f"headline={payload.get('headline')}",
        flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
