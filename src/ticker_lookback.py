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
