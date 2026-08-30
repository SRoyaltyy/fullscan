"""Ticker-first lookback — any name, every session we have artifacts for.

CLI:
  python -m src.ticker_lookback --tickers TEM,ELF,AAPL
"""
from __future__ import annotations

import argparse
import json
import math
import random
import re
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
BOOK_DIR = ROOT / "data" / "stock_book"
JOIN_DIR = ROOT / "data" / "join"
EXPORT_DIR = ROOT / "data" / "exports"
AB_DIR = ROOT / "data" / "ab_checklist"
PEER_DIR = ROOT / "data" / "peers"
UNIVERSE_DIR = ROOT / "data" / "universe"
QUOTE_DIR = ROOT / "data" / "quote_colors"
CATALYST_DIR = ROOT / "01_daily" / "catalyst"
PAPER_DIR = ROOT / "data" / "paper"
PRICE_STORE = ROOT / "data" / "prices" / "ohlc.parquet"
DAILY = ROOT / "01_daily"
SCORE = ROOT / "03_scoreboard"
ET = ZoneInfo("America/New_York")

EPS = 0.05
PRICE_EPS = 0.5  # ±0.5% prints yellow on forward 1d/3d/1w
RELVOL_SPIKE = 1.5
RELVOL_DEAD = 0.7
CORE = ("s_join", "s_general", "s_ab", "s_peer")
BOX_ICON = {"good": "\U0001f7e2", "bad": "\U0001f534", "neutral": "\U0001f7e1", "missing": "\u2b1b"}
TONE_RANK = {"bad": 0, "neutral": 1, "good": 2}
RANDOM_N = 10
# Finviz export units: Market Cap = $ millions, Average Volume = thousands of shares.
RANDOM_MIN_MCAP_M = 100.0
RANDOM_MIN_AVG_VOL_K = 500.0
BOX_COLS = (
    ("join", "join"), ("sector", "sect"), ("gen", "gen"), ("news", "news"),
    ("digest", "dig"), ("judge", "jdg"), ("ab", "AB"), ("peer", "peer"),
    ("heat", "heat"), ("vol", "vol"), ("catal", "cat"), ("buy", "buy"),
)
JOIN_FAMILIES = (
    "mom", "rsi", "sma20", "profit", "earn", "earnsurp", "rvol",
    "themes", "analyst", "peg", "q_mom", "range", "ext", "roe",
)
_INDEX = None
_PRICE_PANEL = None


def _tick(s):
    return str(s or "").strip().upper()


def _num(x, default=None):
    if x is None or (isinstance(x, float) and math.isnan(x)):
        return default
    try:
        raw = str(x).strip().replace(",", "")
        if raw.endswith("%"):
            raw = raw[:-1]
        v = float(raw)
    except (TypeError, ValueError):
        return default
    return default if math.isnan(v) else v


def _polarity(x, eps=EPS):
    v = _num(x)
    if v is None:
        return "missing"
    if v >= eps:
        return "good"
    if v <= -eps:
        return "bad"
    return "neutral"


def price_tone(x, eps=PRICE_EPS):
    """Red / yellow / green for a forward percent change."""
    return _polarity(x, eps=eps)


def is_trading_date(date_str) -> bool:
    """True for Mon–Fri. Weekend artifact dumps are not market sessions."""
    try:
        d = datetime.strptime(str(date_str)[:10], "%Y-%m-%d")
    except (TypeError, ValueError):
        return False
    return d.weekday() < 5


def _tone_rank(tone):
    return TONE_RANK.get(str(tone or "").lower())


def objectively_better(prev_boxes, next_boxes) -> bool:
    """True when next day's factor colors improved with no cell worse.

    Comparable cells only (red / yellow / green on both days). Missing is
    ignored. Rank: red < yellow < green.
    """
    improved = False
    for key, _ in BOX_COLS:
        a = _tone_rank((prev_boxes or {}).get(key))
        b = _tone_rank((next_boxes or {}).get(key))
        if a is None or b is None:
            continue
        if b < a:
            return False
        if b > a:
            improved = True
    return improved


def annotate_signal_improved(days):
    """Mark the later day when its signal is objectively better than the prior day."""
    for i, day in enumerate(days or []):
        day["signal_improved"] = False
        if i == 0:
            continue
        day["signal_improved"] = objectively_better(
            days[i - 1].get("boxes"), day.get("boxes"))
    return days


def latest_finviz_path(asof=None):
    files = sorted(EXPORT_DIR.glob("finviz_????-??-??.csv"))
    if asof:
        files = [p for p in files if p.stem.split("_", 1)[-1] <= asof]
    return files[-1] if files else None


def liquid_universe(asof=None, min_mcap_m=RANDOM_MIN_MCAP_M,
                    min_avg_vol_k=RANDOM_MIN_AVG_VOL_K):
    """Tickers with market cap > $100M and average volume > 500K shares."""
    path = latest_finviz_path(asof)
    if path is None:
        return []
    df = _csv(path)
    if df.empty:
        return []
    tcol = "Ticker" if "Ticker" in df.columns else df.columns[0]
    seen, out = set(), []
    for rec in df.to_dict(orient="records"):
        t = _tick(rec.get(tcol))
        if not t or t in seen:
            continue
        mcap = _num(rec.get("Market Cap"))
        adv = _num(rec.get("Average Volume") or rec.get("Avg Volume")
                   or rec.get("Avg Vol"))
        if mcap is None or adv is None:
            continue
        if mcap > min_mcap_m and adv > min_avg_vol_k:
            seen.add(t)
            out.append(t)
    return out


def pick_random_tickers(n=RANDOM_N, asof=None, seed=None):
    names = liquid_universe(asof)
    if not names:
        raise SystemExit(
            "no liquid names (mcap>$100M, avg vol>500K) in Finviz exports")
    rng = random.Random(seed)
    if len(names) <= n:
        return names
    return rng.sample(names, n)


def _csv(path):
    if not path.exists():
        return pd.DataFrame()
    try:
        return pd.read_csv(path, low_memory=False)
    except Exception:
        return pd.DataFrame()


def _jload(path):
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None


def _dates_from(folder, pattern):
    out = []
    for p in folder.glob(pattern):
        m = re.search(r"(20\d{2}-\d{2}-\d{2})", p.name)
        if m:
            out.append(m.group(1))
    return sorted(set(out))


def session_dates():
    dates = set()
    dates.update(_dates_from(BOOK_DIR, "????-??-??_stock_book.csv"))
    dates.update(_dates_from(JOIN_DIR, "????-??-??_ranked.csv"))
    dates.update(_dates_from(EXPORT_DIR, "finviz_????-??-??.csv"))
    dates.update(_dates_from(AB_DIR, "????-??-??_ab_slim.csv"))
    dates.update(_dates_from(AB_DIR, "????-??-??_ab_checklist_enriched.csv"))
    dates.update(_dates_from(AB_DIR, "????-??-??_ab_checklist.csv"))
    dates.update(_dates_from(PEER_DIR, "????-??-??_peer_rs.csv"))
    dates.update(_dates_from(QUOTE_DIR, "????-??-??_quote_colors_detail.json"))
    return sorted(d for d in dates if is_trading_date(d))


def _by_ticker(df):
    if df is None or df.empty:
        return {}
    tcol = "Ticker" if "Ticker" in df.columns else df.columns[0]
    out = {}
    for rec in df.to_dict(orient="records"):
        t = _tick(rec.get(tcol))
        if t and t not in out:
            out[t] = rec
    return out


def build_index():
    global _INDEX
    if _INDEX is not None:
        return _INDEX
    dates = session_dates()
    sessions = []
    for d in dates:
        book_json = _jload(BOOK_DIR / f"{d}_stock_book.json") or {}
        green = _jload(BOOK_DIR / f"{d}_green.json") or {}
        book_map = _by_ticker(_csv(BOOK_DIR / f"{d}_stock_book.csv"))
        join_map = _by_ticker(_csv(JOIN_DIR / f"{d}_ranked.csv"))
        fv_map = _by_ticker(_csv(EXPORT_DIR / f"finviz_{d}.csv"))
        ab_path = AB_DIR / f"{d}_ab_slim.csv"
        if not ab_path.exists():
            ab_path = AB_DIR / f"{d}_ab_checklist_enriched.csv"
        if not ab_path.exists():
            ab_path = AB_DIR / f"{d}_ab_checklist.csv"
        ab_map = _by_ticker(_csv(ab_path))
        peer_map = _by_ticker(_csv(PEER_DIR / f"{d}_peer_rs.csv"))
        univ_map = _by_ticker(_csv(UNIVERSE_DIR / f"{d}_membership.csv"))
        quote_map = _jload(QUOTE_DIR / f"{d}_quote_colors_detail.json") or {}
        catalyst_payload = _jload(CATALYST_DIR / f"{d}_dossiers.json") or {}
        catalyst_map = {
            _tick(r.get("ticker")): r
            for r in (catalyst_payload.get("dossiers") or [])
            if isinstance(r, dict) and _tick(r.get("ticker"))
        }
        buys, sells = {}, {}
        for h, entry in (book_json.get("books") or {}).items():
            for i, r in enumerate(entry.get("buy") or [], 1):
                t = _tick(r.get("ticker"))
                buys.setdefault(t, {})[h] = {"rank": i, "score": r.get("score"), "reasons": r.get("reasons")}
            for i, r in enumerate(entry.get("sell") or [], 1):
                t = _tick(r.get("ticker"))
                sells.setdefault(t, {})[h] = {"rank": i, "score": r.get("score")}
        green_buy = {_tick(x.get("ticker") if isinstance(x, dict) else x) for x in (green.get("green_buy") or [])}
        live_buy = {_tick(x.get("ticker") if isinstance(x, dict) else x) for x in (green.get("live_buy") or [])}
        sessions.append({
            "date": d,
            "has": {"book": bool(book_map), "join": bool(join_map), "finviz": bool(fv_map),
                     "ab": bool(ab_map), "peer": bool(peer_map), "universe": bool(univ_map),
                     "quote_colors": bool(quote_map), "catalyst": bool(catalyst_map),
                     "green": bool(green)},
            "n_book": len(book_map), "n_join": len(join_map), "n_finviz": len(fv_map),
            "n_ab": len(ab_map), "n_peer": len(peer_map),
            "book": book_map, "join": join_map, "finviz": fv_map, "ab": ab_map,
            "peer": peer_map, "universe": univ_map, "quote_colors": quote_map,
            "catalyst": catalyst_map, "buys": buys, "sells": sells,
            "green_buy": green_buy, "live_buy": live_buy,
            "green_meta": {"n_pile": green.get("n_pile"), "n_universe": green.get("n_universe"), "pile_used": green.get("pile_used")},
        })
    paper_hits = {}
    trades = _csv(PAPER_DIR / "trades.csv")
    if not trades.empty and "ticker" in trades.columns:
        for rec in trades.to_dict(orient="records"):
            t = _tick(rec.get("ticker"))
            paper_hits.setdefault(t, []).append({
                "date": str(rec.get("date") or "")[:10], "side": rec.get("side"),
                "sleeve": rec.get("sleeve"), "price": _num(rec.get("price")),
                "reason": str(rec.get("reason") or "")[:180],
            })
    _INDEX = {"sessions": sessions, "paper": paper_hits, "dates": dates}
    return _INDEX


def _price_panel():
    global _PRICE_PANEL
    if _PRICE_PANEL is not None:
        return _PRICE_PANEL
    if not PRICE_STORE.exists():
        _PRICE_PANEL = pd.DataFrame()
        return _PRICE_PANEL
    try:
        df = pd.read_parquet(PRICE_STORE)
        df["date"] = pd.to_datetime(df["date"]).dt.normalize()
        df["ticker"] = df["ticker"].astype(str).str.upper()
        _PRICE_PANEL = df.drop_duplicates(
            ["date", "ticker"], keep="last"
        ).pivot(index="date", columns="ticker", values="close").sort_index()
    except Exception:
        _PRICE_PANEL = pd.DataFrame()
    return _PRICE_PANEL


def _asof_close(ticker: str, date: str, current_finviz: dict | None = None):
    """Session close on `date`: OHLC panel first, then that day's Finviz Price."""
    t = _tick(ticker)
    panel = _price_panel()
    if not panel.empty and t in panel.columns:
        idx = panel.index.searchsorted(pd.Timestamp(date))
        if idx < len(panel.index) and panel.index[idx].date().isoformat() == date:
            px = _num(panel[t].iloc[idx])
            if px:
                return px
    return _num((current_finviz or {}).get("Price"))


def _fwd_from_panel(ticker: str, date: str) -> dict[str, float | None]:
    panel = _price_panel()
    t = _tick(ticker)
    horizons = {"1d": 1, "2d": 2, "3d": 3, "1w": 5}
    out = {h: None for h in horizons}
    if panel.empty or t not in panel.columns:
        return out
    idx = panel.index.searchsorted(pd.Timestamp(date))
    if idx >= len(panel.index) or panel.index[idx].date().isoformat() != date:
        return out
    entry = _num(panel[t].iloc[idx])
    if not entry:
        return out
    for h, n in horizons.items():
        if idx + n < len(panel.index):
            exitp = _num(panel[t].iloc[idx + n])
            if exitp:
                out[h] = round(100 * (exitp / entry - 1), 3)
    return out


def _fwd_from_sessions(ticker: str, date: str, sessions: list[dict] | None,
                       entry: float | None) -> dict[str, float | None]:
    """Fill forward % from later trading-session Finviz closes."""
    horizons = {"1d": 1, "2d": 2, "3d": 3, "1w": 5}
    out = {h: None for h in horizons}
    if not entry or not sessions:
        return out
    t = _tick(ticker)
    future = [s for s in sessions
              if s.get("date") and s["date"] > date and is_trading_date(s["date"])]
    for h, n in horizons.items():
        if len(future) < n:
            continue
        row = (future[n - 1].get("finviz") or {}).get(t) or {}
        exitp = _num(row.get("Price"))
        if exitp:
            out[h] = round(100 * (exitp / entry - 1), 3)
    return out


def forward_returns(ticker: str, date: str,
                    sessions: list[dict] | None = None,
                    current_finviz: dict | None = None) -> dict[str, float | None]:
    """Close on `date` → close 1 / 2 / 3 / 5 trading sessions later."""
    out = _fwd_from_panel(ticker, date)
    if all(v is not None for v in out.values()):
        return out
    entry = _asof_close(ticker, date, current_finviz=current_finviz)
    fallback = _fwd_from_sessions(ticker, date, sessions, entry)
    for h, v in fallback.items():
        if out.get(h) is None:
            out[h] = v
    return out


def forward_price_changes(ticker: str, date: str,
                          sessions: list[dict] | None = None,
                          current_finviz: dict | None = None) -> dict:
    """As-of close plus forward 1d / 3d / 1w percent changes.

    1d = next trading session, 3d = three sessions later, 1w = five sessions later.
    """
    t = _tick(ticker)
    price = _asof_close(t, date, current_finviz=current_finviz)
    fwd = forward_returns(
        t, date, sessions=sessions, current_finviz=current_finviz)
    return {
        "price": None if price is None else round(price, 4),
        "1d": fwd.get("1d"),
        "3d": fwd.get("3d"),
        "1w": fwd.get("1w"),
    }


def _join_family_tone(val):
    if val is None or (isinstance(val, float) and math.isnan(val)):
        return "missing"
    s = str(val).strip().lower()
    if not s or s in {"nan", "none", "neutral", "mid", "past", "flat"}:
        return "neutral"
    good = {"uptrend", "above", "yes", "good", "cheap", "strong_buy", "buy", "big_beat", "beat", "fast", "up", "high", "spike", "hot", "lead", "bull", "positive"}
    bad = {"downtrend", "below", "no", "poor", "rich", "expensive", "sell", "strong_sell", "miss", "big_miss", "slow", "down", "low", "quiet", "dead", "lag", "veto", "bear", "negative", "overbought"}
    if s in good:
        return "good"
    if s in bad:
        return "bad"
    if "beat" in s or "buy" in s or "up" in s:
        return "good"
    if "miss" in s or "sell" in s or "down" in s:
        return "bad"
    return "neutral"


def _s_from_join(j):
    if not j:
        return None
    v = _num(j.get("score_norm"))
    if v is not None:
        return max(-1.0, min(1.0, v / 2.0))
    v = _num(j.get("total_score"))
    if v is not None:
        return max(-1.0, min(1.0, v / 3.0))
    return None


def _s_from_ab(ab):
    if not ab:
        return None
    v = None
    for key in ("ab_raw", "score_enriched", "score_merged", "score"):
        v = _num(ab.get(key))
        if v is not None:
            break
    return None if v is None else max(-1.0, min(1.0, v / 12.0))


def _s_from_peer(p):
    if not p:
        return None
    v = _num(p.get("rs_week"))
    if v is None:
        v = _num(p.get("rs_month"))
    return None if v is None else max(-1.0, min(1.0, v / 8.0))


def _fv_relvol(fv):
    if not fv:
        return None
    for k in ("Relative Volume", "Rel Volume", "Rel Vol", "RelVol", "relvol"):
        v = _num(fv.get(k))
        if v is not None:
            return v
    vol = _num(fv.get("Volume"))
    adv = _num(fv.get("Average Volume") or fv.get("Avg Volume") or fv.get("Avg Vol"))
    if vol and adv and adv > 0:
        adv_shares = adv * 1000 if adv < vol else adv
        return vol / adv_shares if adv_shares else None
    return None
