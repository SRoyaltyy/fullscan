"""Leak-free join maps for Excel color/hysteresis mines.

PIT rule: at the 9:30 open of date D, only tapes dated **before** D
are knowable. Same-day AB / book / weather / Finviz export are ignored
(those files are typically written after the open).

Finviz snapshot cohorts (current CSV) are labeled `finviz_snapshot`
and are not a historical as-of.

Does not import flatten_robust.
"""
from __future__ import annotations

import csv
import glob
import json
import os
from bisect import bisect_left

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
REPO = os.path.dirname(ROOT)

AB_DIR = os.path.join(REPO, "data", "ab_checklist")
BOOK_DIR = os.path.join(REPO, "data", "stock_book")
WX_DIR = os.path.join(REPO, "01_daily", "weather")


def _dates_lt(sorted_dates, iso):
    i = bisect_left(sorted_dates, iso)
    return sorted_dates[i - 1] if i else None


def load_ab_tones():
    """date -> ticker -> good|bad|neutral from the last completed AB tape."""
    by = {}
    files = sorted(glob.glob(os.path.join(AB_DIR, "????-??-??_ab_checklist_enriched.csv")))
    if not files:
        files = sorted(glob.glob(os.path.join(AB_DIR, "????-??-??_ab_checklist.csv")))
    for path in files:
        iso = os.path.basename(path)[:10]
        mp = {}
        try:
            with open(path, encoding="utf-8", errors="replace") as fh:
                for rec in csv.DictReader(fh):
                    t = (rec.get("Ticker") or rec.get("ticker") or "").strip().upper()
                    if not t:
                        continue
                    raw = rec.get("score") or rec.get("ab_raw") or rec.get("score_enriched")
                    try:
                        v = float(raw)
                    except (TypeError, ValueError):
                        continue
                    mp[t] = "good" if v > 0 else "bad" if v < 0 else "neutral"
        except OSError:
            continue
        if mp:
            by[iso] = mp
    return by


def load_book_buys():
    """date -> set of tickers on the overnight 1d buy list."""
    by = {}
    for path in sorted(glob.glob(os.path.join(BOOK_DIR, "????-??-??_stock_book.json"))):
        iso = os.path.basename(path)[:10]
        try:
            raw = json.load(open(path, encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            continue
        books = (raw.get("books") or {}).get("1d") or {}
        buy = books.get("buy") or []
        names = set()
        for rec in buy:
            if isinstance(rec, str):
                names.add(rec.strip().upper())
            elif isinstance(rec, dict):
                t = (rec.get("ticker") or rec.get("Ticker") or "").strip().upper()
                if t:
                    names.add(t)
        if names:
            by[iso] = names
    return by


def load_weather_risk():
    """date -> risk on|off|mixed|unknown from that day's weather file."""
    by = {}
    for path in sorted(glob.glob(os.path.join(WX_DIR, "????-??-??_weather.json"))):
        iso = os.path.basename(path)[:10]
        try:
            raw = json.load(open(path, encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            continue
        risk = str((raw.get("signals") or {}).get("risk") or "unknown").lower()
        if risk not in ("on", "off", "mixed", "unknown"):
            risk = "unknown"
        by[iso] = risk
    return by


def prior_lookup(by_date, iso, ticker=None):
    """Latest dated tape strictly before `iso`. ticker=None returns the payload."""
    if not by_date:
        return None, None
    keys = getattr(by_date, "_sorted", None)
    if keys is None:
        keys = sorted(by_date)
        try:
            by_date._sorted = keys
        except Exception:
            pass
    prev = _dates_lt(keys, iso)
    if prev is None:
        return None, None
    payload = by_date[prev]
    if ticker is None:
        return prev, payload
    if isinstance(payload, set):
        return prev, (ticker.upper() in payload)
    if isinstance(payload, dict):
        return prev, payload.get(ticker.upper())
    return prev, None


def _fz_num(v):
    v = (v or "").replace("%", "").replace(",", "").strip()
    if not v:
        return None
    mult = 1.0
    if v.endswith(("K", "M", "B")):
        mult = {"K": 1e3, "M": 1e6, "B": 1e9}[v[-1]]
        v = v[:-1]
    try:
        return float(v) * mult
    except ValueError:
        return None


def load_finviz_asof_highvol(exports_dir=None):
    """date -> set of tickers with Volatility (Month) > 8% on that export.

    Only dated `finviz_YYYY-MM-DD.csv` files. `finviz_latest.csv` is skipped
    (that is the current snapshot, not an as-of tape).
    """
    exports_dir = exports_dir or os.path.join(REPO, "data", "exports")
    by = {}
    for path in sorted(glob.glob(os.path.join(exports_dir, "finviz_????-??-??.csv"))):
        iso = os.path.basename(path)[7:17]
        names = set()
        try:
            with open(path, encoding="utf-8", errors="replace") as fh:
                for rec in csv.DictReader(fh):
                    t = (rec.get("Ticker") or "").strip().upper()
                    if not t:
                        continue
                    vm = _fz_num(rec.get("Volatility (Month)"))
                    if vm is not None and vm > 8:
                        names.add(t)
        except OSError:
            continue
        if names:
            by[iso] = names
    return by


def coverage(by_date):
    keys = sorted(by_date)
    return {
        "n_dates": len(keys),
        "first": keys[0] if keys else None,
        "last": keys[-1] if keys else None,
    }
