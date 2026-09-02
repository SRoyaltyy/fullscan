"""Scenario lab for the book × mine overlay.

Replays the same panel as boring_winners_backtest, then runs named
sleeves that change one knob at a time:

  source     book | overlay | mine
  seats      10 / 25 / 50
  hold       1 / 2 / 3 / 5 trading sessions (locked names keep their seat)
  color      all | blue | green (panel cond==good)
  hard_red   none | stand_down | haircut_5 | limit_5

Hold-N: a name bought on D cannot be sold until it has aged N sessions.
New candidates only fill empty seats. If the book is full of locked
names, today's buys are skipped (n_skip).

hard_red (lattice live from 2026-08-31):
  stand_down  no new buys
  haircut_5   new buy fills at open×0.95 only if that day's low printed
              through the limit; otherwise fills at the session close
  limit_5     new buy only if low ≤ open×0.95; fill at open×0.95
              (not close-to-close ≤ −5)

Does not rebuild the mine parquet. Does not overwrite the paper-trading
dashboard — writes dashboard/boring-winners/index.html.
"""
from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import pandas as pd

from src import bt_report
from src.boring_winners_backtest import (
    CLIP,
    MAX_EXTRAS,
    SEATS,
    _pack_seat,
    fill_overlay,
    fill_returns_from_finviz,
    fill_seats,
    load_book_buys,
    load_book_universe,
    load_finviz_px,
    load_panel,
)
from src.gainer_asof import load_day_context
from src.price_store import _load_store

ROOT = Path(__file__).resolve().parent.parent
OUT_JSON = ROOT / "03_scoreboard" / "boring_winners_lab.json"
OUT_MD = ROOT / "03_scoreboard" / "BORING_WINNERS_LAB.md"
OUT_CSV = ROOT / "03_scoreboard" / "boring_winners_lab_daily.csv"
DASH_DIR = ROOT / "dashboard" / "boring-winners"
TEMPLATE = Path(__file__).with_name("boring_winners_dash.html")
CAPITAL = 10_000.0
LIMIT_PCT = 0.05
CLOSE_WHEN = "16:00 ET close"
LIMIT_WHEN = "intraday (low ≤ open×0.95)"
