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
Live flatten_robust keeps the 3d book and spends a reserved
10% cash sleeve at 09:30 on the top 2 continuation names.
"""
from __future__ import annotations

import numpy as np

from . import candle_factor as cf
from . import gainer_asof as ga

LOOKBACK = 20
INDICATOR_LOOKBACK = 60   # RSI(14) + MACD(12/26/9) need more than the hot-20
HOT_TOP_N = 80
CONT_TOP_N = 8
CONT_RET5_MAX = 10.0
RSI_OS = 30.0
RSI_OB = 70.0
FLOW_RVOL = 1.5
FLOW_RET_MAX = 1.2       # |prior 1d %| — volume arrived, price barely moved


def _tick(v) -> str:
    return str(v or "").strip().upper()


def prior_bars(ticker: str, asof: str, n: int = LOOKBACK) -> list[dict]:
    t = _tick(ticker)
    d = str(asof or "")[:10]
    if not t or not d:
        return []
    return [b for b in (cf._ticker_bars().get(t) or []) if b["date"] < d][-n:]


def features(ticker: str, asof: str, n: int = LOOKBACK) -> dict:
    bars = prior_bars(ticker, asof, n=max(int(n or LOOKBACK), INDICATOR_LOOKBACK))
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
    rng = float(h[-1] - low[-1])
    z["close_loc"] = float((c[-1] - low[-1]) / rng) if rng > 1e-12 else 0.5
    z["rsi"] = _rsi(c)
    macd, sig, hist, xup, xdn = _macd(c)
    z["macd"] = macd
    z["macd_sig"] = sig
    z["macd_hist"] = hist
    z["macd_cross_up"] = xup
    z["macd_cross_down"] = xdn
    z["rsi_os"] = bool(z["rsi"] is not None and z["rsi"] <= RSI_OS)
    z["rsi_ob"] = bool(z["rsi"] is not None and z["rsi"] >= RSI_OB)
    z["macd_up"] = bool(hist is not None and hist > 0)
    z["macd_down"] = bool(hist is not None and hist < 0)
    z["flow_in"] = bool(
        float(z.get("rvol") or 0) >= FLOW_RVOL
        and abs(float(z.get("ret_1") or 99)) <= FLOW_RET_MAX
    )
    z["hot_score"] = hot_score(z)
    return z


def _rsi(closes: np.ndarray, period: int = 14) -> float | None:
    """Wilder RSI on prior closes. None until we have period+1 prints."""
    if len(closes) < period + 1:
        return None
    d = np.diff(closes.astype(float))
    gains = np.where(d > 0, d, 0.0)
    losses = np.where(d < 0, -d, 0.0)
    ag = float(gains[:period].mean())
    al = float(losses[:period].mean())
    for i in range(period, len(d)):
        ag = (ag * (period - 1) + float(gains[i])) / period
        al = (al * (period - 1) + float(losses[i])) / period
    if al <= 1e-12:
        return 100.0 if ag > 0 else 50.0
    return round(100.0 - 100.0 / (1.0 + ag / al), 2)


def _ema(x: np.ndarray, span: int) -> np.ndarray:
    a = 2.0 / (span + 1.0)
    out = np.empty(len(x), dtype=float)
    out[0] = float(x[0])
    for i in range(1, len(x)):
        out[i] = a * float(x[i]) + (1.0 - a) * out[i - 1]
    return out


def _macd(closes: np.ndarray, fast: int = 12, slow: int = 26,
          signal: int = 9) -> tuple:
    """(macd, signal, hist, cross_up, cross_down) on the last prior bar."""
    need = slow + signal
    if len(closes) < need:
        return None, None, None, False, False
    line = _ema(closes.astype(float), fast) - _ema(closes.astype(float), slow)
    sig = _ema(line, signal)
    hist = line - sig
    xup = bool(len(hist) >= 2 and hist[-2] <= 0 and hist[-1] > 0)
    xdn = bool(len(hist) >= 2 and hist[-2] >= 0 and hist[-1] < 0)
    return (round(float(line[-1]), 4), round(float(sig[-1]), 4),
            round(float(hist[-1]), 4), xup, xdn)


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


def continuation(prior_date: str | None, asof: str,
                 top_n: int = CONT_TOP_N,
                 ret5_max: float = CONT_RET5_MAX) -> list[str]:
    """Yesterday's liquid gainers that are not already exploded.

    Sweep (2026-08-14 → latest): top 8, ~7.5 names/day, 9/375 next-day
    top-25 (vs flatten 2), 15 liquid ≥5%, 7 top-25 losers, g/l 1.29.
    Earnings reaction is noisier (g/l 0.37) and is not mixed in here.
    Same-day Change% is never an input.
    """
    if not prior_date or not asof:
        return []
    out: list[str] = []
    seen: set[str] = set()
    for raw in ga.liquid_gainers(
        ga.load_finviz(prior_date), top_n=60, min_change=0.0, liquid=True,
    ):
        t = _tick(raw.get("ticker"))
        if not t or t in seen:
            continue
        feat = features(t, asof)
        if not feat.get("ok") or too_extended(feat):
            continue
        if float(feat.get("ret_5") or 0) > float(ret5_max):
            continue
        seen.add(t)
        out.append(t)
        if len(out) >= int(top_n):
            break
    return out
