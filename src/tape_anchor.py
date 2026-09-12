"""Deterministic pre-open tape anchor from Channel 1.

Why this exists (2026-09-12 diagnosis): the general and sector calls are graded
on close-vs-prior-close, so the overnight gap that is already printed in
index futures, European cash and sector futures at 05:55 is part of the
answer. The rubric treated futures as "confirmation only" (weight 0.5, bucket
±0.5) and let LLM-judged bonds/Fed/sentiment components with 33–48% sign
accuracy dominate the total. Over every leak-free pre-open snapshot in the
repo, sign(ES) alone hit 67% of directions, Europe 76%, NQ 71%, while the
engine hit 58% and predicted "flat" (a 4%-of-days outcome) 15% of the time.

This module turns Channel 1 into one number per topic — the anchor, in the
same score units the rest of the pipeline consumes (0.5% of overnight tape ≈
+3 points, the old "mild" threshold) — so Python, not the LLM, owns the part
of the call that is already observable.

Nothing here calls the network; it only reads the Channel 1 dict.
"""
from __future__ import annotations

import math

# score units per 1% of anchored tape move. 0.5% -> 3.0 (mild), 1.0% -> 6.0
SCORE_PER_PCT = 6.0
ANCHOR_CLIP = 12.0          # score units; a 2% overnight move saturates

# General anchor: equal-weight mean of what is available, each in % terms.
GENERAL_LEGS = (
    ("ES", 1.0),
    ("NQ", 0.6),
    ("EUROPE", 0.8),
    ("VIX_D1", -0.15),      # VIX up 1 point ≈ −0.15% expected tape
)

# Sector anchor legs: (tape symbol, beta) where beta = expected sector ETF %
# move per 1% move in the leg. Summed, not averaged, so a leg that barely
# moved contributes almost nothing. Symbols are Finviz futures tickers as
# stored in ch1["finviz_futures_tape"]["rows"] plus the synthetic ES/NQ legs
# resolved from the Yahoo blocks when the Finviz tape is missing. ZN is the
# 10-year note *price* (1% ≈ 15 bp of yield): up = yields down, which helps
# bond proxies (Utilities, Real Estate, Staples) and hurts banks.
# Betas are priors, not fits: the repo has too few sector-tape days to fit
# them honestly. The replay harness reports how they do out of sample.
SECTOR_LEGS = {
    "Energy": (("CL", 0.35), ("QA", 0.15), ("ES", 0.6)),
    "Technology": (("NQ", 0.8), ("ES", 0.3)),
    "Communication Services": (("NQ", 0.5), ("ES", 0.5)),
    "Consumer Cyclical": (("ES", 0.8), ("ER2", 0.2), ("NQ", 0.2)),
    "Consumer Defensive": (("ES", 0.45), ("ZN", 0.4)),
    "Healthcare": (("ES", 0.6),),
    "Financial": (("ES", 0.8), ("ZN", -0.6)),
    "Industrials": (("ES", 0.8), ("ER2", 0.2), ("HG", 0.1)),
    "Basic Materials": (("ES", 0.6), ("HG", 0.3), ("GC", 0.1), ("DX", -0.3)),
    "Utilities": (("ES", 0.3), ("ZN", 0.75)),
    "Real Estate": (("ES", 0.5), ("ZN", 0.75)),
}
# Own-ETF pre-market % vs prior close (ch1["etf_premarket"][ETF]) is the
# sector's actual gap; when present it takes this share of the anchor.
ETF_PREMARKET_SHARE = 0.7
# Legs that count as "equity tape"; without at least one the anchor is off.
EQUITY_LEGS = {"ES", "NQ", "ER2", "YM", "EUROPE"}


def _num(v) -> float | None:
    try:
        f = float(v)
    except (TypeError, ValueError):
        return None
    if math.isnan(f) or math.isinf(f):
        return None
    return f


def tape_values(ch1: dict | None) -> dict[str, float]:
    """Flatten Channel 1 into {leg: pct_move}. Only legs that are present."""
    out: dict[str, float] = {}
    if not isinstance(ch1, dict):
        return out
    fut = ch1.get("futures") or {}
    es = _num((fut.get("ES=F") or {}).get("pct_1d"))
    nq = _num((fut.get("NQ=F") or {}).get("pct_1d"))
    if es is not None:
        out["ES"] = es
    if nq is not None:
        out["NQ"] = nq
    gs = ch1.get("global_sessions") or {}
    eu = _num((gs.get("europe") or {}).get("composite_avg"))
    asia = _num((gs.get("asia") or {}).get("composite_avg"))
    if eu is not None:
        out["EUROPE"] = eu
    if asia is not None:
        out["ASIA"] = asia
    vix = (ch1.get("vix") or {}).get("vix") or {}
    vd = _num(vix.get("delta_1d"))
    if vd is not None:
        out["VIX_D1"] = vd
    tape = ch1.get("finviz_futures_tape") or {}
    for row in tape.get("rows") or []:
        sym = row.get("ticker")
        chg = _num(row.get("change"))
        if sym and chg is not None and sym not in out:
            out[sym] = chg
    # Finviz tape rows can override the synthetic ES/NQ if Yahoo was missing
    fx = ch1.get("commodities_fx") or {}
    if "CL" not in out:
        cl = _num((fx.get("CL=F") or {}).get("pct_1d"))
        if cl is not None:
            out["CL"] = cl
    if "DX" not in out:
        dx = _num((fx.get("DXY") or {}).get("pct_1d"))
        if dx is not None:
            out["DX"] = dx
    if "GC" not in out:
        gc = _num((fx.get("GC=F") or {}).get("pct_1d"))
        if gc is not None:
            out["GC"] = gc
    pm = ch1.get("etf_premarket") or {}
    for etf, blk in pm.items():
        v = _num((blk or {}).get("pct_vs_prev_close"))
        if v is not None:
            out[f"PM:{etf}"] = v
    return out


def _weighted_mean(vals: dict[str, float], legs) -> tuple[float | None, list]:
    num = den = 0.0
    used = []
    for sym, w in legs:
        v = vals.get(sym)
        if v is None:
            continue
        num += w * v
        den += abs(w)
        used.append((sym, v, w))
    if den == 0.0:
        return None, used
    return num / den, used


def _beta_sum(vals: dict[str, float], legs) -> tuple[float | None, list]:
    total = 0.0
    used = []
    equity_seen = False
    for sym, beta in legs:
        v = vals.get(sym)
        if v is None:
            continue
        total += beta * v
        used.append((sym, v, beta))
        equity_seen = equity_seen or sym in EQUITY_LEGS
    if not equity_seen:
        return None, used
    return total, used


def general_anchor(ch1: dict | None) -> dict:
    """Anchor for the S&P call. Returns pct, score, and the legs used."""
    vals = tape_values(ch1)
    pct, used = _weighted_mean(vals, GENERAL_LEGS)
    return _pack(pct, used)


def sector_anchor(sector: str, etf: str | None, ch1: dict | None) -> dict:
    """Anchor for one sector ETF: beta-sum of its overnight drivers, blended
    with the ETF's own pre-market gap when Channel 1 carries one."""
    vals = tape_values(ch1)
    legs = SECTOR_LEGS.get(sector, (("ES", 0.8),))
    pct, used = _beta_sum(vals, legs)
    pm = vals.get(f"PM:{etf}") if etf else None
    if pm is not None:
        if pct is None:
            pct = pm
        else:
            pct = ETF_PREMARKET_SHARE * pm + (1.0 - ETF_PREMARKET_SHARE) * pct
        used.append((f"PM:{etf}", pm, ETF_PREMARKET_SHARE))
    return _pack(pct, used)


def _pack(pct: float | None, used: list) -> dict:
    if pct is None:
        return {"available": False, "pct": None, "score": 0.0, "legs": []}
    score = max(-ANCHOR_CLIP, min(ANCHOR_CLIP, pct * SCORE_PER_PCT))
    return {
        "available": True,
        "pct": round(pct, 4),
        "score": round(score, 3),
        "legs": [{"leg": s, "pct": round(v, 3), "w": w} for s, v, w in used],
    }
