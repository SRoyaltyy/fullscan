"""Paper-trading engine + dashboard for the daily stock book.

Simulates 10 sleeves (5 horizons x 2 selection sets), each with its own
capital, following the daily stock book:

    {1d,3d,1w,2w,1m}_top   — top N overall BUY names from the book
    {1d,3d,1w,2w,1m}_size  — top 3 per size bucket (large+ / mid / small-micro)

Rules
- Rebuild from scratch every run: replay all books chronologically (idempotent).
- Entry/exit at the signal day's closing price (yfinance, auto-adjusted).
- Follow-the-book: hold a name while it stays in the sleeve's pick list;
  sell when it drops out (only after the horizon min-hold: 1d=1, 3d=3,
  1w=5, 2w=10, 1m=21 sessions). New names split leftover cash equally
  (whole shares). Horizon chooses WHICH book to follow and the hold floor.
- Every order is charged the Futubull US-stock fee schedule
  (00_grounding/futubull_fees.json).
- SPY tracked as benchmark with the same starting capital.

Outputs
- data/paper/equity_curve.csv   — daily equity per sleeve + SPY
- data/paper/trades.csv         — every simulated order
- data/paper/state.json         — positions / cash per sleeve (latest)
- 03_scoreboard/PAPER_TRADING.md — summary table
- dashboard/index.html          — self-contained equity dashboard

CLI: python -m src.paper_trade [--date YYYY-MM-DD] [--top 10] [--capital 10000]
"""
from __future__ import annotations

import argparse
import json
import math
import re
from collections import defaultdict, deque
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd

from . import config

ROOT = Path(__file__).resolve().parent.parent
BOOK_DIR = ROOT / "data" / "stock_book"
PAPER_DIR = ROOT / "data" / "paper"
SCOREBOARD = ROOT / "03_scoreboard"
DASH_DIR = ROOT / "dashboard"
FEES_PATH = ROOT / "00_grounding" / "futubull_fees.json"
PRICE_CACHE = PAPER_DIR / "prices_cache.csv"

HOLD_DAYS = {"1d": 1, "3d": 3, "1w": 5, "2w": 10, "1m": 21}
HORIZONS = list(HOLD_DAYS.keys())
AB_DIR = ROOT / "data" / "ab_checklist"
BACKFILL_DIR = ROOT / "data" / "ab_backfill"
_CTX_RE = re.compile(r"\b(LEAD|LAG)(?:,[^\s;]+)*")
_AB_RE = re.compile(r"\bab=([+-]?\d+(?:\.\d+)?)", re.I)
_UNIVERSE_PQ_RE = re.compile(
    r"^(\d{4}-\d{2}-\d{2})_(\d{4}-\d{2}-\d{2})_universe\.parquet$"
)
_BACKFILL_BY_DATE: dict[str, dict[str, dict]] | None = None


def _num(v):
    if v is None or v == "":
        return None
    try:
        x = float(v)
    except (TypeError, ValueError):
        return None
    if math.isnan(x):
        return None
    return x


def _parse_ab_context(reasons: str) -> str | None:
    if not reasons:
        return None
    m = _CTX_RE.search(reasons)
    return m.group(0) if m else None


def _clean_str(v) -> str | None:
    if v is None:
        return None
    try:
        if v != v:  # NaN
            return None
    except Exception:
        pass
    s = str(v).strip()
    if s in ("", "nan", "None", "—", "-"):
        return None
    return s


def _score_intish(v):
    n = _num(v)
    if n is None:
        return None
    if abs(n - round(n)) < 1e-9:
        return int(round(n))
    return round(n, 4)


def _latest_backfill_parquet() -> Path | None:
    """Newest finished liquid-universe PIT parquet (the 12-month audit)."""
    if not BACKFILL_DIR.exists():
        return None
    cands: list[tuple[str, str, Path]] = []
    for pth in BACKFILL_DIR.iterdir():
        m = _UNIVERSE_PQ_RE.match(pth.name)
        if not m or pth.stat().st_size < 1000:
            continue
        cands.append((m.group(2), m.group(1), pth))
    if not cands:
        return None
    # Latest window end; if tied, widest window (earliest start) — the 12-month audit.
    cands.sort(key=lambda x: (x[0], x[1]))
    best_end = cands[-1][0]
    tied = [c for c in cands if c[0] == best_end]
    tied.sort(key=lambda x: x[1])
    return tied[0][2]


def _backfill_by_date() -> dict[str, dict[str, dict]]:
    """asof_date -> ticker -> {ab_score, ab_context} from the 12-month PIT parquet."""
    global _BACKFILL_BY_DATE
    if _BACKFILL_BY_DATE is not None:
        return _BACKFILL_BY_DATE
    _BACKFILL_BY_DATE = {}
    pq = _latest_backfill_parquet()
    if pq is None:
        return _BACKFILL_BY_DATE
    try:
        df = pd.read_parquet(
            pq,
            columns=["Ticker", "asof_date", "score_enriched", "score", "context_label"],
        )
    except Exception as e:
        print(f"[paper] could not read backfill {pq.name}: {e}")
        return _BACKFILL_BY_DATE
    if df is None or df.empty or "Ticker" not in df.columns:
        return _BACKFILL_BY_DATE
    df["Ticker"] = df["Ticker"].astype(str).str.strip().str.upper()
    df["asof_date"] = df["asof_date"].astype(str).str.slice(0, 10)
    for rec in df.itertuples(index=False):
        score = _score_intish(getattr(rec, "score_enriched", None))
        if score is None:
            score = _score_intish(getattr(rec, "score", None))
        ctx = _clean_str(getattr(rec, "context_label", None))
        if score is None and not ctx:
            continue
        day = _BACKFILL_BY_DATE.setdefault(str(rec.asof_date), {})
        day[str(rec.Ticker)] = {"ab_score": score, "ab_context": ctx}
    print(f"[paper] PIT backfill {pq.name}: {len(df):,} rows, "
          f"{len(_BACKFILL_BY_DATE)} days")
    return _BACKFILL_BY_DATE


def _apply_backfill(date: str, out: dict[str, dict]) -> None:
    """Fill missing AB from the 12-month PIT audit. Never overwrite a live checklist / s_ab."""
    for t, rec in _backfill_by_date().get(date, {}).items():
        slot = out.setdefault(t, {
            "sector": None, "industry": None, "ab_score": None,
            "ab_context": None, "ab_kind": None,
        })
        if slot.get("ab_score") is None and rec.get("ab_score") is not None:
            slot["ab_score"] = rec["ab_score"]
            slot["ab_kind"] = "backfill"
        if not slot.get("ab_context") and rec.get("ab_context"):
            slot["ab_context"] = rec["ab_context"]


def load_day_meta(date: str) -> dict[str, dict]:
    """ticker -> sector / industry / AB score+context as of this book date.

    Stock-book CSV is the fill's sector. AB prefers that day's live checklist
    (score_enriched + context_label) or s_ab / `ab=` in reasons. Days before
    the checklist was wired (or names the daily file didn't cover) fall back
    to the 12-month PIT universe parquet — never yesterday's leftover.
    """
    out: dict[str, dict] = {}
    csv_path = BOOK_DIR / f"{date}_stock_book.csv"
    if csv_path.exists():
        try:
            df = pd.read_csv(csv_path, low_memory=False)
        except Exception as e:
            print(f"[paper] could not read {csv_path.name}: {e}")
            df = None
        if df is not None and "Ticker" in df.columns:
            df["Ticker"] = df["Ticker"].astype(str).str.strip().str.upper()
            for rec in df.to_dict("records"):
                t = rec.get("Ticker") or ""
                if not t:
                    continue
                reasons = str(rec.get("reasons") or "")
                sab = _num(rec.get("s_ab"))
                if sab is None:
                    m = _AB_RE.search(reasons)
                    sab = float(m.group(1)) if m else None
                out[t] = {
                    "sector": _clean_str(rec.get("sector")),
                    "industry": _clean_str(rec.get("industry")),
                    "ab_score": None if sab is None else round(sab, 4),
                    "ab_context": _parse_ab_context(reasons),
                    "ab_kind": "s_ab" if sab is not None else None,
                }

    ab_path = None
    for name in (
        f"{date}_ab_checklist_enriched.csv",
        f"{date}_ab_checklist_merged.csv",
        f"{date}_ab_checklist.csv",
    ):
        cand = AB_DIR / name
        if cand.exists():
            ab_path = cand
            break
    if ab_path is not None:
        try:
            want = {"Ticker", "score", "score_base", "score_enriched",
                    "context_label", "Sector", "Industry"}
            adf = pd.read_csv(ab_path, low_memory=False,
                              usecols=lambda c: c in want)
        except Exception as e:
            print(f"[paper] could not read {ab_path.name}: {e}")
            adf = None
        if adf is not None and "Ticker" in adf.columns:
            adf["Ticker"] = adf["Ticker"].astype(str).str.strip().str.upper()
            for rec in adf.to_dict("records"):
                t = rec.get("Ticker") or ""
                if not t:
                    continue
                slot = out.setdefault(t, {
                    "sector": None, "industry": None, "ab_score": None,
                    "ab_context": None, "ab_kind": None,
                })
                sec = _clean_str(rec.get("Sector"))
                ind = _clean_str(rec.get("Industry"))
                if sec and not slot.get("sector"):
                    slot["sector"] = sec
                if ind and not slot.get("industry"):
                    slot["industry"] = ind
                enr = _score_intish(rec.get("score_enriched"))
                base = (_score_intish(rec.get("score_base"))
                        or _score_intish(rec.get("score")))
                if enr is not None:
                    slot["ab_score"] = enr
                    slot["ab_kind"] = "checklist"
                elif base is not None:
                    slot["ab_score"] = base
                    slot["ab_kind"] = "checklist"
                ctx = _clean_str(rec.get("context_label"))
                if ctx:
                    slot["ab_context"] = ctx

    _apply_backfill(date, out)
    return out


TRAIL_N = 5
_META_CACHE: dict[str, dict[str, dict]] = {}
_SESSION_CAL: list[str] | None = None


def _meta(date: str) -> dict[str, dict]:
    if date not in _META_CACHE:
        _META_CACHE[date] = load_day_meta(date)
    return _META_CACHE[date]


def session_calendar() -> list[str]:
    """Trading sessions: PIT backfill asof dates plus any extra book dates."""
    global _SESSION_CAL
    if _SESSION_CAL is not None:
        return _SESSION_CAL
    days = set(_backfill_by_date().keys())
    if BOOK_DIR.exists():
        for pth in BOOK_DIR.glob("*_stock_book.json"):
            days.add(pth.name.replace("_stock_book.json", ""))
    _SESSION_CAL = sorted(days)
    return _SESSION_CAL


def sessions_ending(asof: str, n: int = TRAIL_N) -> list[str]:
    cal = session_calendar()
    i = -1
    for k, d in enumerate(cal):
        if d <= asof:
            i = k
    if i < 0:
        return []
    return cal[max(0, i - n + 1): i + 1]


def ab_trail(ticker: str, asof: str | None, n: int = TRAIL_N) -> list[dict]:
    """n sessions ending on asof (inclusive). Score + LEAD/LAG each day."""
    if not asof:
        return []
    t = str(ticker).upper()
    out: list[dict] = []
    for d in sessions_ending(str(asof)[:10], n):
        m = _meta(d).get(t) or {}
        out.append({
            "d": d,
            "s": m.get("ab_score"),
            "c": m.get("ab_context") or "",
            "k": m.get("ab_kind") or "",
        })
    return out


def attach_trails(trade_rows: list[dict], roundtrips: list[dict] | None = None) -> None:
    for r in trade_rows:
        r["ab_trail"] = ab_trail(r.get("ticker"), r.get("date"))
    for r in roundtrips or []:
        r["buy_trail"] = ab_trail(r.get("ticker"), r.get("buy_date"))
        r["sell_trail"] = ab_trail(r.get("ticker"), r.get("sell_date")) if r.get("sell_date") else []


def proposals_from_book(book: dict, top_n: int = 10) -> dict[str, dict]:
    """sleeve -> {pick, beyond: [{ticker, rank, bucket}]}."""
    books = book.get("books", {})
    out: dict[str, dict] = {}
    for h in HORIZONS:
        hb = books.get(h) or {}
        buy = [str(r.get("ticker") or "").upper() for r in (hb.get("buy") or [])]
        buy = [t for t in buy if t]
        pick = buy[:top_n]
        beyond = [{"ticker": t, "rank": i, "bucket": "top"}
                  for i, t in enumerate(buy[top_n:], start=top_n + 1)]
        out[f"{h}_top"] = {"pick": pick, "beyond": beyond, "buy_all": buy}
        sized: list[str] = []
        extra: list[dict] = []
        by_size = hb.get("buy_by_size") or {}
        for bucket in ("large+", "mid", "small/micro"):
            rows = [str(r.get("ticker") or "").upper() for r in (by_size.get(bucket) or [])]
            rows = [t for t in rows if t]
            sized += rows[:3]
            extra += [{"ticker": t, "rank": i, "bucket": bucket}
                      for i, t in enumerate(rows[3:], start=4)]
        out[f"{h}_size"] = {"pick": sized, "beyond": extra, "buy_all": buy}
    return out


def _skip_row(date: str, sleeve: str, ticker: str, kind: str, reason: str,
              extra: dict | None = None) -> dict:
    t = str(ticker).upper()
    m = _meta(date).get(t) or {}
    row = {
        "date": date, "sleeve": sleeve, "ticker": t,
        "kind": kind, "reason": reason,
        "sector": m.get("sector") or "",
        "industry": m.get("industry") or "",
        "ab_score": m.get("ab_score"),
        "ab_context": m.get("ab_context") or "",
        "ab_kind": m.get("ab_kind") or "",
        "ab_trail": ab_trail(t, date),
    }
    if extra:
        row.update(extra)
    return row


def collect_skips(books: list[tuple[str, Path]], prices: pd.DataFrame,
                  trade_rows: list[dict], top_n: int, capital: float) -> list[dict]:
    """Proposed buys/sells that did not become fills.

    beyond = on the book BUY list but past the sleeve cap (10 / 3-per-bucket).
    no_price = in the cap, no close so the engine skipped it (does not roll down).
    cash = in the cap, leftover split could not buy 1 whole share.
    min_hold = held name dropped off the list but locked (first day only).
    """
    risk_pol = load_risk_policy()
    date_ix = {d: i for i, (d, _) in enumerate(books)}
    sleeves = [f"{h}_{k}" for h in HORIZONS for k in ("top", "size")]
    held: dict[str, dict[str, str]] = {s: {} for s in sleeves}  # ticker -> entry_date
    cash: dict[str, float] = {s: capital for s in sleeves}
    emitted_lock: set[tuple[str, str]] = set()
    skips: list[dict] = []

    fills_by = {}
    for r in trade_rows:
        fills_by.setdefault((r["date"], r["sleeve"]), []).append(r)

    def price_of(date: str, t: str) -> float | None:
        day_px = prices.loc[:date] if len(prices) else prices
        if day_px is None or getattr(day_px, "empty", True):
            return None
        if t not in day_px.columns:
            return None
        v = day_px[t].iloc[-1]
        if v is None or (isinstance(v, float) and (v != v or v <= 0)):
            return None
        try:
            return float(v)
        except (TypeError, ValueError):
            return None

    for date, path in books:
        book = json.loads(path.read_text(encoding="utf-8"))
        props = proposals_from_book(book, top_n)
        weather_risk = str((book.get("meta") or {}).get("weather_risk") or "")
        entry_scale = 1.0
        if (weather_risk == "off" and risk_pol["scale"] < 1.0
                and date >= risk_pol["effective"]):
            entry_scale = risk_pol["scale"]

        for sleeve, info in props.items():
            pick = info["pick"]
            pset = set(pick)
            horizon = sleeve.split("_")[0]
            min_hold = HOLD_DAYS[horizon]

            for item in info["beyond"]:
                t = item["ticker"]
                if t in held[sleeve]:
                    continue
                cap = "10" if sleeve.endswith("_top") else "3/" + str(item.get("bucket") or "bucket")
                skips.append(_skip_row(
                    date, sleeve, t, "beyond",
                    f"rank {item['rank']} on {sleeve} — sleeve only takes {cap}",
                    {"rank": item["rank"], "bucket": item.get("bucket") or "",
                     "px": price_of(date, t)},
                ))

            day_fills = fills_by.get((date, sleeve), [])
            sold = {r["ticker"] for r in day_fills if r["side"] == "sell"}
            bought = {r["ticker"] for r in day_fills if r["side"] == "buy"}

            # cash after exits = last sell cash_after, else current cash
            sells = [r for r in day_fills if r["side"] == "sell"]
            if sells:
                cash[sleeve] = float(sells[-1].get("cash_after") or cash[sleeve])

            dropped = [t for t in list(held[sleeve]) if t not in pset]
            for t in dropped:
                if t in sold:
                    emitted_lock.discard((sleeve, t))
                    continue
                entry = held[sleeve][t]
                held_n = date_ix[date] - date_ix.get(entry, date_ix[date])
                key = (sleeve, t)
                if held_n < min_hold:
                    if key not in emitted_lock:
                        skips.append(_skip_row(
                            date, sleeve, t, "min_hold",
                            f"dropped from {sleeve} but min-hold {held_n}/{min_hold}d — no sell",
                            {"held": held_n, "min_hold": min_hold,
                             "px": price_of(date, t)},
                        ))
                        emitted_lock.add(key)
                else:
                    emitted_lock.discard(key)

            new = [t for t in pick if t not in held[sleeve]]
            n_new = len(new) or 1
            per = (cash[sleeve] * entry_scale) / n_new
            for t in new:
                if t in bought:
                    continue
                p = price_of(date, t)
                if p is None:
                    skips.append(_skip_row(
                        date, sleeve, t, "no_price",
                        f"in the {sleeve} cap but no close — not rolled down to rank 11+",
                        {"per": round(per, 2)},
                    ))
                    continue
                shares = int(per // p) if p else 0
                if shares < 1:
                    skips.append(_skip_row(
                        date, sleeve, t, "cash",
                        f"in the {sleeve} cap; split leftover {per:.2f} < 1 share @ {p:.2f}",
                        {"px": p, "per": round(per, 2),
                         "cash": round(cash[sleeve], 2)},
                    ))

            for r in day_fills:
                if r["side"] == "sell":
                    held[sleeve].pop(r["ticker"], None)
                    emitted_lock.discard((sleeve, r["ticker"]))
                else:
                    held[sleeve][r["ticker"]] = date
            buys = [r for r in day_fills if r["side"] == "buy"]
            if buys:
                cash[sleeve] = float(buys[-1].get("cash_after") or cash[sleeve])
            elif sells:
                cash[sleeve] = float(sells[-1].get("cash_after") or cash[sleeve])

    return skips


# ---------------------------------------------------------------- fees ----

def load_fees() -> dict:
    return json.loads(FEES_PATH.read_text(encoding="utf-8"))


def order_fees(shares: int, price: float, side: str, f: dict) -> float:
    """Futubull US-stock order fees for one order."""
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


# -------------------------------------------------------------- prices ----

def get_prices(tickers: list[str], start: str, end: str) -> pd.DataFrame:
    """Date-indexed close prices; incremental cache in data/paper/.

    Fetches a padded window (books are generated pre-market, so the signal
    day's own close may not exist yet — fills then use the last available
    close on/before the signal date, resolved in run_sim)."""
    cache = pd.DataFrame()
    if PRICE_CACHE.exists():
        cache = pd.read_csv(PRICE_CACHE, index_col=0, parse_dates=True)
    missing_cols = [t for t in tickers if t not in cache.columns]
    need_refresh = len(cache) == 0 or cache.index.max() < pd.Timestamp(end)
    if missing_cols or need_refresh:
        try:
            import yfinance as yf
        except ImportError:
            if cache.empty:
                raise SystemExit("[paper] yfinance missing and no prices_cache.csv")
            print("[paper] yfinance not installed — using prices_cache.csv")
            return cache
        fetch = sorted(set(tickers) | set(cache.columns))
        # yfinance `end` is EXCLUSIVE — pad so the signal day is included once
        # its close exists; start padded back for a pre-book price baseline.
        start_pad = (pd.Timestamp(start) - pd.Timedelta(days=10)).date().isoformat()
        end_excl = (pd.Timestamp(end) + pd.Timedelta(days=6)).date().isoformat()
        raw = yf.download(fetch, start=start_pad, end=end_excl, auto_adjust=True,
                          group_by="ticker", progress=False, threads=True)
        frames = {}
        for t in fetch:
            try:
                s = raw[(t, "Close")] if len(fetch) > 1 else raw["Close"]
            except KeyError:
                continue
            frames[t] = s.dropna()
        if not frames:
            raise SystemExit("[paper] yfinance returned no prices at all")
        new = pd.DataFrame(frames)
        new.index = pd.to_datetime(new.index)
        # new data wins on overlap; keep older cached rows outside the window
        combined = new.combine_first(cache) if not cache.empty else new
        cache = combined.dropna(how="all").sort_index()
        PAPER_DIR.mkdir(parents=True, exist_ok=True)
        cache.to_csv(PRICE_CACHE)
    return cache


# -------------------------------------------------------------- books -----

def list_books() -> list[tuple[str, Path]]:
    out = []
    for p in sorted(BOOK_DIR.glob("*_stock_book.json")):
        out.append((p.name.replace("_stock_book.json", ""), p))
    return out


def picks_from_book(book: dict, top_n: int) -> dict[str, list[str]]:
    """sleeve -> ordered pick list for one book."""
    books = book.get("books", {})
    picks: dict[str, list[str]] = {}
    for h in HORIZONS:
        hb = books.get(h) or {}
        top = [r["ticker"] for r in (hb.get("buy") or [])[:top_n]]
        picks[f"{h}_top"] = top
        sized: list[str] = []
        by_size = hb.get("buy_by_size") or {}
        for bucket in ("large+", "mid", "small/micro"):
            sized += [r["ticker"] for r in (by_size.get(bucket) or [])[:3]]
        picks[f"{h}_size"] = sized
    return picks


# ------------------------------------------------------------- engine -----

def load_risk_policy() -> dict:
    """Risk-off entry scaling from 00_grounding/book_policy.json.

    Applied only from `risk_scaling_effective` onward so the idempotent
    replay never rewrites history that predates the rule. book_learn
    re-evaluates the scale nightly against realized returns.
    """
    path = ROOT / "00_grounding" / "book_policy.json"
    out = {"scale": 1.0, "effective": "9999-12-31"}
    try:
        pol = json.loads(path.read_text(encoding="utf-8"))
        scale = float(pol.get("risk_off_entry_scale", 1.0))
        if 0.0 <= scale <= 1.0:
            out["scale"] = scale
            out["effective"] = str(pol.get("risk_scaling_effective") or "9999-12-31")
    except (OSError, ValueError, TypeError):
        pass
    return out


def run_sim(books: list[tuple[str, Path]], prices: pd.DataFrame,
            capital: float, top_n: int, fees: dict):
    sleeves = [f"{h}_{k}" for h in HORIZONS for k in ("top", "size")]
    st = {
        s: {"cash": capital, "pos": {}, "realized": 0.0, "fees": 0.0,
            "trades": 0, "wins": 0, "closed": 0}
        for s in sleeves
    }
    risk_pol = load_risk_policy()
    curve_rows: list[dict] = []
    trade_rows: list[dict] = []
    spy0 = None
    date_ix = {d: i for i, (d, _) in enumerate(books)}

    for date, path in books:
        day_px = prices.loc[:date]
        if day_px.empty:
            continue
        px = day_px.iloc[-1]  # close on (or last close before) signal date

        def price_of(t: str) -> float | None:
            v = px.get(t)
            if v is None or (isinstance(v, float) and (math.isnan(v) or v <= 0)):
                return None
            return float(v)

        book = json.loads(path.read_text(encoding="utf-8"))
        picks = picks_from_book(book, top_n)
        day_meta = load_day_meta(date)

        def _fill_meta(ticker: str) -> dict:
            m = day_meta.get(str(ticker).upper(), {}) or {}
            return {
                "cash_before": None,  # filled at call site
                "cash_after": round(S["cash"], 2),
                "sector": m.get("sector"),
                "industry": m.get("industry"),
                "ab_score": m.get("ab_score"),
                "ab_context": m.get("ab_context"),
                "ab_kind": m.get("ab_kind"),
            }
        # LLM weather call → action: on risk-off days (from the book's own
        # meta, so replay is deterministic) scale down new-entry deployment
        # and keep the rest in cash. Gated by effective date.
        weather_risk = str((book.get("meta") or {}).get("weather_risk") or "")
        entry_scale = 1.0
        if (weather_risk == "off" and risk_pol["scale"] < 1.0
                and date >= risk_pol["effective"]):
            entry_scale = risk_pol["scale"]

        for sleeve, targets in picks.items():
            S = st[sleeve]
            tset = set(targets)
            horizon = sleeve.split("_")[0]
            min_hold = HOLD_DAYS[horizon]

            # exits: dropped off the book AND min-hold has elapsed
            for t in list(S["pos"]):
                if t in tset:
                    continue
                pos = S["pos"][t]
                held = date_ix[date] - date_ix.get(pos["entry_date"], date_ix[date])
                if held < min_hold:
                    continue  # still locked
                p = price_of(t)
                if p is None:
                    continue  # can't price -> carry position
                pos = S["pos"].pop(t)
                fee = order_fees(pos["shares"], p, "sell", fees)
                proceeds = pos["shares"] * p - fee
                S["cash"] += proceeds
                pnl = proceeds - pos["cost"]
                S["realized"] += pnl
                S["fees"] += fee
                S["trades"] += 1
                S["closed"] += 1
                S["wins"] += 1 if pnl > 0 else 0
                cash_before = round(S["cash"] - proceeds, 2)
                extra = _fill_meta(t)
                extra["cash_before"] = cash_before
                extra["cash_after"] = round(S["cash"], 2)
                trade_rows.append({"date": date, "sleeve": sleeve, "ticker": t,
                                   "side": "sell", "shares": pos["shares"],
                                   "price": round(p, 4), "fees": fee,
                                   "amount": round(proceeds, 2),
                                   "realized_pnl": round(pnl, 2),
                                   "reason": f"dropped from {sleeve} after {held}d (min {min_hold}d)",
                                   **extra})

            # entries: new names split available cash equally
            # (× entry_scale on risk-off days — rest stays in cash)
            new = [t for t in targets if t not in S["pos"]]
            if new:
                per = (S["cash"] * entry_scale) / len(new)
                for t in new:
                    p = price_of(t)
                    if p is None or per <= 0:
                        continue
                    shares = int(per // p)
                    if shares < 1:
                        continue
                    fee = order_fees(shares, p, "buy", fees)
                    cost = shares * p + fee
                    if cost > S["cash"]:
                        shares = int((S["cash"] - fee) // p)
                        if shares < 1:
                            continue
                        fee = order_fees(shares, p, "buy", fees)
                        cost = shares * p + fee
                    S["cash"] -= cost
                    S["pos"][t] = {"shares": shares, "entry_date": date,
                                   "entry_px": p, "cost": cost}
                    S["fees"] += fee
                    S["trades"] += 1
                    reason = f"entered {sleeve} book"
                    if entry_scale < 1.0:
                        reason += f" (risk-off: deploying {entry_scale:.0%} of cash)"
                    extra = _fill_meta(t)
                    extra["cash_before"] = round(S["cash"] + cost, 2)
                    extra["cash_after"] = round(S["cash"], 2)
                    trade_rows.append({"date": date, "sleeve": sleeve,
                                       "ticker": t, "side": "buy",
                                       "shares": shares, "price": round(p, 4),
                                       "fees": fee, "amount": round(cost, 2),
                                       "realized_pnl": "",
                                       "reason": reason,
                                       **extra})

        # mark-to-market
        spy = price_of("SPY")
        if spy and spy0 is None:
            spy0 = spy
        for sleeve in sleeves:
            S = st[sleeve]
            invested = 0.0
            for t, pos in S["pos"].items():
                p = price_of(t)
                invested += pos["shares"] * (p if p else pos["entry_px"])
            curve_rows.append({"date": date, "sleeve": sleeve,
                               "equity": round(S["cash"] + invested, 2),
                               "cash": round(S["cash"], 2),
                               "invested": round(invested, 2),
                               "fees_cum": round(S["fees"], 2),
                               "realized_cum": round(S["realized"], 2)})
        if spy and spy0:
            curve_rows.append({"date": date, "sleeve": "SPY (benchmark)",
                               "equity": round(capital * spy / spy0, 2),
                               "cash": "", "invested": "", "fees_cum": "",
                               "realized_cum": ""})
    return st, curve_rows, trade_rows


def match_roundtrips(trade_rows: list[dict], prices: pd.DataFrame) -> list[dict]:
    """FIFO: pair each buy lot with later sells of the same ticker in the same sleeve.

    One row per closed round-trip (bought then sold) and one row per leftover
    open lot. This is what the dashboard shows as 'closed' vs 'open'.
    """
    lots: dict[tuple[str, str], deque] = defaultdict(deque)
    closed: list[dict] = []

    def _held(buy: str, sell: str) -> int:
        try:
            return max(0, (pd.Timestamp(sell) - pd.Timestamp(buy)).days)
        except Exception:
            return 0

    for r in trade_rows:
        key = (r["sleeve"], str(r["ticker"]).upper())
        if r["side"] == "buy":
            lots[key].append({
                "buy_date": r["date"],
                "buy_px": float(r["price"]),
                "buy_fees": float(r.get("fees") or 0),
                "shares": int(r["shares"]),
                "buy_amount": float(r.get("amount") or 0),
                "sector": r.get("sector"),
                "industry": r.get("industry"),
                "ab_score": r.get("ab_score"),
                "ab_context": r.get("ab_context"),
                "ab_kind": r.get("ab_kind"),
            })
            continue
        if r["side"] != "sell":
            continue
        remaining = int(r["shares"])
        sold_total = remaining or 1
        sell_px = float(r["price"])
        sell_fees = float(r.get("fees") or 0)
        sell_amount = float(r.get("amount") or 0)
        sell_date = r["date"]
        while remaining > 0 and lots[key]:
            lot = lots[key][0]
            take = min(lot["shares"], remaining)
            frac_sell = take / sold_total
            frac_lot = take / lot["shares"] if lot["shares"] else 1.0
            buy_cost = lot["buy_amount"] * frac_lot
            sell_net = sell_amount * frac_sell
            pnl = sell_net - buy_cost
            closed.append({
                "status": "closed",
                "sleeve": r["sleeve"],
                "ticker": key[1],
                "shares": take,
                "buy_date": lot["buy_date"],
                "buy_px": round(lot["buy_px"], 4),
                "sell_date": sell_date,
                "sell_px": round(sell_px, 4),
                "last": None,
                "held_cal_days": _held(lot["buy_date"], sell_date),
                "realized_pnl": round(pnl, 2),
                "unrealized_pnl": None,
                "buy_fees": round(lot["buy_fees"] * frac_lot, 4),
                "sell_fees": round(sell_fees * frac_sell, 4),
                "sector": lot.get("sector") or r.get("sector"),
                "industry": lot.get("industry") or r.get("industry"),
                "ab_score": lot.get("ab_score"),
                "ab_context": lot.get("ab_context"),
                "ab_kind": lot.get("ab_kind"),
                "sell_ab_score": r.get("ab_score"),
                "sell_ab_context": r.get("ab_context"),
            })
            lot["shares"] -= take
            lot["buy_amount"] -= buy_cost
            lot["buy_fees"] = lot["buy_fees"] * (1.0 - frac_lot)
            remaining -= take
            if lot["shares"] <= 0:
                lots[key].popleft()

    last_px = prices.iloc[-1] if len(prices) else pd.Series(dtype=float)
    open_rows: list[dict] = []
    for (sleeve, ticker), q in lots.items():
        for lot in q:
            if lot["shares"] <= 0:
                continue
            cur = last_px.get(ticker)
            last = float(cur) if cur == cur and cur else lot["buy_px"]
            mtm = lot["shares"] * last - lot["buy_amount"]
            open_rows.append({
                "status": "open",
                "sleeve": sleeve,
                "ticker": ticker,
                "shares": lot["shares"],
                "buy_date": lot["buy_date"],
                "buy_px": round(lot["buy_px"], 4),
                "sell_date": None,
                "sell_px": None,
                "last": round(last, 4),
                "held_cal_days": None,
                "realized_pnl": None,
                "unrealized_pnl": round(mtm, 2),
                "buy_fees": round(lot["buy_fees"], 4),
                "sell_fees": 0.0,
                "sector": lot.get("sector"),
                "industry": lot.get("industry"),
                "ab_score": lot.get("ab_score"),
                "ab_context": lot.get("ab_context"),
                "ab_kind": lot.get("ab_kind"),
                "sell_ab_score": None,
                "sell_ab_context": None,
            })
    return closed + open_rows


# ------------------------------------------------------------ outputs -----

def sleeve_stats(sleeve: str, S: dict, prices: pd.DataFrame, capital: float) -> dict:
    px = prices.iloc[-1] if len(prices) else pd.Series(dtype=float)
    invested = 0.0
    unrealized = 0.0
    open_wins = 0
    for t, pos in S["pos"].items():
        v = px.get(t)
        last = float(v) if v == v and v else pos["entry_px"]
        invested += pos["shares"] * last
        mtm = pos["shares"] * last - pos["cost"]
        unrealized += mtm
        if mtm > 0:
            open_wins += 1
    equity = S["cash"] + invested
    closed = S["closed"]
    opened = len(S["pos"])
    return {"sleeve": sleeve, "equity": round(equity, 2),
            "return_pct": round(100 * (equity / capital - 1), 2),
            "cash": round(S["cash"], 2), "open": opened,
            "trades": S["trades"], "fees": round(S["fees"], 2),
            "realized": round(S["realized"], 2),
            "unrealized": round(unrealized, 2),
            "closed": closed,
            "closed_wins": S["wins"],
            "open_wins": open_wins,
            "win_rate": round(100 * S["wins"] / closed, 1) if closed else None,
            "open_win_rate": round(100 * open_wins / opened, 1) if opened else None}


def write_dashboard(curve: pd.DataFrame, stats: list[dict], st: dict,
                    prices: pd.DataFrame, date: str, capital: float,
                    fees: dict, trade_rows: list[dict] | None = None,
                    last_picks: dict[str, list[str]] | None = None,
                    book_dates: list[str] | None = None,
                    roundtrips: list[dict] | None = None,
                    skipped: list[dict] | None = None) -> None:
    DASH_DIR.mkdir(parents=True, exist_ok=True)
    curve = curve.copy()
    curve["date"] = pd.to_datetime(curve["date"])
    pivot = curve.pivot_table(index="date", columns="sleeve", values="equity",
                              aggfunc="last").sort_index()
    series = {c: [None if (v != v) else v for v in pivot[c].tolist()]
              for c in pivot.columns}
    payload = {
        "generated": datetime.now(ZoneInfo(config.TZ)).isoformat(),
        "dates": [str(d.date()) for d in pivot.index],
        "series": series,
        "stats": stats,
        "capital": capital,
        "fees": {k: fees[k] for k in fees if not k.startswith("_")},
        "rules": {
            "hold_days": HOLD_DAYS,
            "hold_applied": True,
            "fill": "signal-day close",
            "top": "top-N overall BUY names on that horizon's book",
            "size": "top 3 per size bucket (large+ / mid / small-micro)",
            "risk_off_entry_scale": load_risk_policy(),
            "ab_trail_sessions": TRAIL_N,
        },
    }
    positions = []
    px = prices.iloc[-1] if len(prices) else pd.Series(dtype=float)
    date_ix = {d: i for i, d in enumerate(book_dates or [])}
    last_picks = last_picks or {}
    for sleeve, S in st.items():
        horizon = sleeve.split("_")[0]
        min_hold = HOLD_DAYS.get(horizon, 1)
        on_list = set(last_picks.get(sleeve) or [])
        for t, pos in S["pos"].items():
            cur = px.get(t)
            cur = float(cur) if cur == cur and cur else pos["entry_px"]
            held = date_ix.get(date, 0) - date_ix.get(pos["entry_date"], date_ix.get(date, 0))
            positions.append({
                "sleeve": sleeve, "ticker": t, "shares": pos["shares"],
                "entry_date": pos["entry_date"], "entry_px": round(pos["entry_px"], 2),
                "last": round(cur, 2),
                "unrealized": round(pos["shares"] * cur - pos["cost"], 2),
                "on_book": t in on_list,
                "held_sessions": held,
                "min_hold": min_hold,
            })
    payload["positions"] = positions
    fills = []
    for r in trade_rows or []:
        pnl = r.get("realized_pnl")
        if pnl == "" or pnl is None:
            pnl = None
        else:
            try:
                pnl = round(float(pnl), 2)
            except (TypeError, ValueError):
                pnl = None
        fills.append({
            "date": r["date"], "sleeve": r["sleeve"], "ticker": r["ticker"],
            "side": r["side"], "shares": int(r["shares"]),
            "price": float(r["price"]), "fees": float(r.get("fees") or 0),
            "amount": float(r.get("amount") or 0), "realized_pnl": pnl,
            "reason": r.get("reason") or "",
            "cash_before": r.get("cash_before"),
            "cash_after": r.get("cash_after"),
            "sector": r.get("sector") or "",
            "industry": r.get("industry") or "",
            "ab_score": r.get("ab_score"),
            "ab_context": r.get("ab_context") or "",
            "ab_kind": r.get("ab_kind") or "",
            "ab_trail": r.get("ab_trail") or [],
        })
    payload["trades"] = fills
    payload["roundtrips"] = roundtrips or []
    payload["skipped"] = skipped or []

    shell = Path(__file__).with_name("paper_dash.html").read_text(encoding="utf-8")
    html = shell.replace("__DATA__", json.dumps(payload))
    (DASH_DIR / "index.html").write_text(html, encoding="utf-8")


def write_report(stats: list[dict], date: str, capital: float) -> None:
    SCOREBOARD.mkdir(parents=True, exist_ok=True)
    L = [
        "# Paper trading — Futubull-fee simulation",
        "",
        f"As of **{date}** · ${capital:,.0f} starting capital per sleeve · "
        "fees per `00_grounding/futubull_fees.json`",
        "",
        "Sleeves: `{horizon}_top` = top-N overall buys, `{horizon}_size` = top 3 "
        "per size bucket. Fill at signal-day close. Sell only after min-hold "
        "(1d=1, 3d=3, 1w=5, 2w=10, 1m=21 sessions) AND the name has left the book.",
        "",
        "| Sleeve | Equity | Return | Cash | Open pos | Trades | Fees paid | Realized P/L | Unrealized P/L | Closed win | Open win |",
        "|--------|--------|--------|------|----------|--------|-----------|--------------|----------------|------------|----------|",
    ]
    for s in stats:
        wr = f"{s['win_rate']}%" if s["win_rate"] is not None else "—"
        ow = f"{s.get('open_win_rate')}%" if s.get("open_win_rate") is not None else "—"
        L.append(f"| {s['sleeve']} | ${s['equity']:,.2f} | {s['return_pct']:+.2f}% | "
                 f"${s['cash']:,.2f} | {s['open']} | {s['trades']} | "
                 f"${s['fees']:,.2f} | ${s['realized']:+,.2f} | "
                 f"${s.get('unrealized', 0):+,.2f} | {wr} | {ow} |")
    L += ["", "Equity curves + positions: `dashboard/index.html`", ""]
    (SCOREBOARD / "PAPER_TRADING.md").write_text("\n".join(L), encoding="utf-8")


# ------------------------------------------------------------ driver ------

def run(date: str | None = None, top_n: int = 10, capital: float | None = None) -> None:
    fees = load_fees()
    capital = capital or float(fees["paper_account"]["starting_capital_per_sleeve"])
    books = list_books()
    if date:
        books = [b for b in books if b[0] <= date]
    if not books:
        raise SystemExit("[paper] no stock books found — run stock_book first")

    # collect every ticker we may need to price
    tickers = {"SPY"}
    for _, p in books:
        bk = json.loads(p.read_text(encoding="utf-8"))
        for picks in picks_from_book(bk, top_n).values():
            tickers.update(picks)
    start, end = books[0][0], books[-1][0]
    prices = get_prices(sorted(tickers), start, end)

    st, curve_rows, trade_rows = run_sim(books, prices, capital, top_n, fees)
    if not curve_rows:
        raise SystemExit(
            f"[paper] no price data on/before {books[0][0]} — cannot simulate. "
            "Check yfinance connectivity.")

    trips = match_roundtrips(trade_rows, prices)
    attach_trails(trade_rows, trips)
    skips = collect_skips(books, prices, trade_rows, top_n, capital)
    PAPER_DIR.mkdir(parents=True, exist_ok=True)
    curve = pd.DataFrame(curve_rows)
    curve.to_csv(PAPER_DIR / "equity_curve.csv", index=False)
    pd.DataFrame(trade_rows).to_csv(PAPER_DIR / "trades.csv", index=False)
    if trips:
        pd.DataFrame(trips).to_csv(PAPER_DIR / "roundtrips.csv", index=False)
    if skips:
        pd.DataFrame(skips).to_csv(PAPER_DIR / "skipped.csv", index=False)
    (PAPER_DIR / "state.json").write_text(json.dumps(st, indent=2, default=str),
                                          encoding="utf-8")

    stats = [sleeve_stats(s, st[s], prices, capital) for s in st]
    last = books[-1][0]
    last_picks = picks_from_book(json.loads(books[-1][1].read_text(encoding="utf-8")), top_n)
    write_report(stats, last, capital)
    write_dashboard(curve, stats, st, prices, last, capital, fees, trade_rows,
                    last_picks=last_picks, book_dates=[d for d, _ in books],
                    roundtrips=trips, skipped=skips)
    n_closed = sum(1 for t in trips if t["status"] == "closed")
    n_open = sum(1 for t in trips if t["status"] == "open")
    print(f"[paper] {len(books)} book(s), {len(trade_rows)} trades "
          f"({n_closed} closed pairs, {n_open} open lots, {len(skips)} not taken), "
          f"curves → dashboard/index.html, summary → 03_scoreboard/PAPER_TRADING.md")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", default=None)
    ap.add_argument("--top", type=int, default=10)
    ap.add_argument("--capital", type=float, default=None)
    args = ap.parse_args()
    run(date=args.date, top_n=args.top, capital=args.capital)




if __name__ == "__main__":
    main()
