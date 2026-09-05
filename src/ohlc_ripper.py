"""20-session prior OHLC ranker for probable next-day rippers.

Sweep on 2026-08-13 → latest (yesterday-liquid → today's tape):

  * names that close ≥5% / make the top-25 gainer tape are **moderately**
    extended on the prior 20 bars (ret_5 ≈ +4–5%, rvol ≈ 1.2)
  * names that make the top-25 **loser** tape are **already exploded**
    (ret_5 ≈ +15%, vol_rg ≈ 6, atr expanding)
  * a "hot" score (20d momentum + rvol + 10-bar breakout) on yesterday's
    liquid tape, top 80, plus the existing capture watchlist, hits
    ~27% of next-day top-25 gainers and ~19% of all liquid ≥5% names
  * coiled / NR7 ranking does **not** find the rip — that was the miss

Every bar is strictly before ``asof``. Same-day Change% is never an input.
This list does not change live flatten_robust fills.
"""
from __future__ import annotations

import numpy as np

from . import candle_factor as cf
from . import gainer_asof as ga

LOOKBACK = 20
HOT_TOP_N = 80


def _tick(v) -> str:
    return str(v or "").strip().upper()


def prior_bars(ticker: str, asof: str, n: int = LOOKBACK) -> list[dict]:
    t = _tick(ticker)
    d = str(asof or "")[:10]
    if not t or not d:
        return []
    return [b for b in (cf._ticker_bars().get(t) or []) if b["date"] < d][-n:]


def features(ticker: str, asof: str, n: int = LOOKBACK) -> dict:
    bars = prior_bars(ticker, asof, n=n)
    feat = from_bars(bars)
    feat["ticker"] = _tick(ticker)
    feat["asof"] = str(asof or "")[:10]
    return feat


def from_bars(bars: list[dict]) -> dict:
    z = {
        "ok": False, "n": len(bars),
        "ret_1": 0.0, "ret_5": 0.0, "ret_10": 0.0,
        "rvol": 1.0, "nr7": False, "break_10": False,
        "compression": 1.0, "last_green": False, "last_red": False,
        "hot_score": 0.0,
    }
    if len(bars) < 5:
        return z
    o = np.array([b["open"] for b in bars], dtype=float)
    h = np.array([b["high"] for b in bars], dtype=float)
    low = np.array([b["low"] for b in bars], dtype=float)
    c = np.array([b["close"] for b in bars], dtype=float)
    v = np.array([float(b.get("volume") or 0) for b in bars], dtype=float)
    z["ok"] = True
    z["n"] = int(len(c))
    z["ret_1"] = float(100.0 * (c[-1] / c[-2] - 1.0)) if c[-2] else 0.0
    z["ret_5"] = float(100.0 * (c[-1] / c[-6] - 1.0)) if len(c) >= 6 and c[-6] else 0.0
    z["ret_10"] = float(100.0 * (c[-1] / c[-11] - 1.0)) if len(c) >= 11 and c[-11] else 0.0
    v20 = float(v[-20:].mean()) if len(v) >= 8 else float(v.mean())
    z["rvol"] = float(v[-1] / v20) if v20 > 0 else 1.0
    ranges = h - low
    z["nr7"] = bool(len(ranges) >= 7 and ranges[-1] <= ranges[-7:].min() + 1e-12)
    prior10h = float(h[-11:-1].max()) if len(h) >= 11 else float(h[:-1].max())
    z["break_10"] = bool(c[-1] > prior10h)
    avg_rng = float(ranges[-10:-1].mean()) if len(ranges) >= 11 else float(ranges[:-1].mean())
    z["compression"] = float(ranges[-1] / avg_rng) if avg_rng > 0 else 1.0
    z["last_green"] = bool(c[-1] > o[-1])
    z["last_red"] = bool(c[-1] < o[-1])
    z["hot_score"] = hot_score(z)
    return z


def hot_score(feat: dict) -> float:
    """Higher = more like yesterday's already-moving tape (ripper *and* loser).

    Used as a **ranker on the liquid universe**, not a live flatten veto.
    Extreme extension (ret_5 > 18, rvol > 2.8) is cut at collect time.
    """
    if not feat.get("ok"):
        return 0.0
    return (
        0.08 * max(float(feat.get("ret_5") or 0), 0.0)
        + 0.04 * max(float(feat.get("ret_10") or 0), 0.0)
        + 0.4 * min(float(feat.get("rvol") or 1.0), 3.0)
        + (1.2 if feat.get("break_10") else 0.0)
        + (0.3 if feat.get("last_green") else 0.0)
    )


def too_extended(feat: dict) -> bool:
    return float(feat.get("ret_5") or 0) > 18.0 or float(feat.get("rvol") or 0) > 2.8


def liquid_hot(prior_date: str | None, asof: str, top_n: int = HOT_TOP_N) -> list[str]:
    """Yesterday-liquid names ranked by 20-day OHLC hot score (asof = today)."""
    if not prior_date or not asof:
        return []
    rows = ga._liquid_tape(
        ga.load_finviz(prior_date), top_n=0, min_change=0.0, liquid=True,
        min_mcap_m=None, side="up", skip_change=True,
    )
    scored = []
    for raw in rows:
        t = _tick(raw.get("ticker"))
        if not t:
            continue
        feat = features(t, asof)
        if not feat.get("ok") or too_extended(feat):
            continue
        scored.append((feat["hot_score"], t))
    scored.sort(reverse=True)
    out, seen = [], set()
    for _, t in scored:
        if t in seen:
            continue
        seen.add(t)
        out.append(t)
        if len(out) >= int(top_n):
            break
    return out
