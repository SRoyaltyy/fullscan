"""Ticker-first lookback — any name, every session we have artifacts for.

Date-first report is `book_lookback.py` / `01_daily/YYYY-MM-DD_lookback.md`.
This flips the axis: pass tickers (they do NOT have to be in a printed book),
walk every session that left a book / join / Finviz / AB / peer file, and
rebuild the same boxes the Aug-20 lookback page uses.

Full-market sources (not just the printed 15):
  stock_book CSV   ~2.7k later dates, ~11.5k on Aug 13-14
  join ranked      ~5.9k
  Finviz export    ~11.6k
  AB slim          ~2.7k
  peer RS          ~5.1k

CLI:
  python -m src.ticker_lookback --tickers TEM,ELF,AAPL
"""
from __future__ import annotations

import argparse
import json
import math
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
PAPER_DIR = ROOT / "data" / "paper"
DAILY = ROOT / "01_daily"
SCORE = ROOT / "03_scoreboard"
ET = ZoneInfo("America/New_York")

EPS = 0.05
RELVOL_SPIKE = 1.5
RELVOL_DEAD = 0.7
CORE = ("s_join", "s_general", "s_ab", "s_peer")
BOX_ICON = {"good": "\U0001f7e2", "bad": "\U0001f534", "neutral": "\U0001f7e1", "missing": "\u2b1b"}
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


def _tick(s) -> str:
    return str(s or "").strip().upper()


def _num(x, default=None):
    if x is None or (isinstance(x, float) and math.isnan(x)):
        return default
    try:
        v = float(x)
    except (TypeError, ValueError):
        return default
    return default if math.isnan(v) else v


def _polarity(x, eps: float = EPS) -> str:
    v = _num(x)
    if v is None:
        return "missing"
    if v >= eps:
        return "good"
    if v <= -eps:
        return "bad"
    return "neutral"


def _csv(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    try:
        return pd.read_csv(path, low_memory=False)
    except Exception:
        return pd.DataFrame()


def _jload(path: Path):
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None


def _dates_from(folder: Path, pattern: str) -> list[str]:
    out = []
    for p in folder.glob(pattern):
        m = re.search(r"(20\d{2}-\d{2}-\d{2})", p.name)
        if m:
            out.append(m.group(1))
    return sorted(set(out))


def session_dates() -> list[str]:
    dates = set()
    dates.update(_dates_from(BOOK_DIR, "????-??-??_stock_book.csv"))
    dates.update(_dates_from(JOIN_DIR, "????-??-??_ranked.csv"))
    dates.update(_dates_from(EXPORT_DIR, "finviz_????-??-??.csv"))
    dates.update(_dates_from(AB_DIR, "????-??-??_ab_slim.csv"))
    dates.update(_dates_from(PEER_DIR, "????-??-??_peer_rs.csv"))
    return sorted(dates)


def _by_ticker(df: pd.DataFrame) -> dict:
    if df is None or df.empty:
        return {}
    tcol = "Ticker" if "Ticker" in df.columns else df.columns[0]
    out = {}
    for rec in df.to_dict(orient="records"):
        t = _tick(rec.get(tcol))
        if t and t not in out:
            out[t] = rec
    return out


def build_index() -> dict:
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
        ab_map = _by_ticker(_csv(AB_DIR / f"{d}_ab_slim.csv"))
        peer_map = _by_ticker(_csv(PEER_DIR / f"{d}_peer_rs.csv"))
        univ_map = _by_ticker(_csv(UNIVERSE_DIR / f"{d}_membership.csv"))
        buys, sells = {}, {}
        for h, entry in (book_json.get("books") or {}).items():
            for i, r in enumerate(entry.get("buy") or [], 1):
                t = _tick(r.get("ticker"))
                buys.setdefault(t, {})[h] = {
                    "rank": i, "score": r.get("score"), "reasons": r.get("reasons"),
                }
            for i, r in enumerate(entry.get("sell") or [], 1):
                t = _tick(r.get("ticker"))
                sells.setdefault(t, {})[h] = {"rank": i, "score": r.get("score")}
        green_buy = {
            _tick(x.get("ticker") if isinstance(x, dict) else x)
            for x in (green.get("green_buy") or [])
        }
        live_buy = {
            _tick(x.get("ticker") if isinstance(x, dict) else x)
            for x in (green.get("live_buy") or [])
        }
        sessions.append({
            "date": d,
            "has": {
                "book": bool(book_map), "join": bool(join_map),
                "finviz": bool(fv_map), "ab": bool(ab_map),
                "peer": bool(peer_map), "universe": bool(univ_map),
                "green": bool(green),
            },
            "n_book": len(book_map), "n_join": len(join_map),
            "n_finviz": len(fv_map), "n_ab": len(ab_map), "n_peer": len(peer_map),
            "book": book_map, "join": join_map, "finviz": fv_map,
            "ab": ab_map, "peer": peer_map, "universe": univ_map,
            "buys": buys, "sells": sells,
            "green_buy": green_buy, "live_buy": live_buy,
            "green_meta": {
                "n_pile": green.get("n_pile"),
                "n_universe": green.get("n_universe"),
                "pile_used": green.get("pile_used"),
            },
        })
    paper_hits = {}
    trades = _csv(PAPER_DIR / "trades.csv")
    if not trades.empty and "ticker" in trades.columns:
        for rec in trades.to_dict(orient="records"):
            t = _tick(rec.get("ticker"))
            paper_hits.setdefault(t, []).append({
                "date": str(rec.get("date") or "")[:10],
                "side": rec.get("side"),
                "sleeve": rec.get("sleeve"),
                "price": _num(rec.get("price")),
                "reason": str(rec.get("reason") or "")[:180],
            })
    _INDEX = {"sessions": sessions, "paper": paper_hits, "dates": dates}
    return _INDEX
