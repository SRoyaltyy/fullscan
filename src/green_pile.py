"""Green pile: every *present* name-layer is green, silent yellows ignored.

A name is green when:
  join, general, AB, peer are all >= EPS (actually fired for the name)
  sector and news are not red
  relative volume is not dead (< 0.7) when we have a Finviz print

Yellow on news/digest/judge/sector (flat / no headline) is treated as
no data — not a veto. Black / missing heat and catalyst are ignored.

Live BUY fills from the pile when it has ≥ GREEN_MIN liquid names after
the $400M / sector / industry caps. Otherwise the ranker keeps the old
weighted walk. SELL always ranks on core weights (no pile, no add-ons).
"""
from __future__ import annotations

import pandas as pd

EPS = 0.05
RELVOL_DEAD = 0.7
GREEN_MIN = 8
MIN_LIQUID_MCAP_M = 400.0
CORE = ("s_join", "s_general", "s_ab", "s_peer")
CORE_LABEL = {"s_join": "join", "s_general": "general", "s_ab": "AB", "s_peer": "peer"}


def _num(s: pd.Series) -> pd.Series:
    return pd.to_numeric(s, errors="coerce").fillna(0.0)


def green_mask(df: pd.DataFrame) -> pd.Series:
    if df is None or df.empty:
        return pd.Series(dtype=bool)
    ok = pd.Series(True, index=df.index)
    for col in CORE:
        if col not in df.columns:
            return pd.Series(False, index=df.index)
        ok &= _num(df[col]) >= EPS
    if "s_sector" in df.columns:
        ok &= _num(df["s_sector"]) > -EPS
    if "s_news" in df.columns:
        ok &= _num(df["s_news"]) > -EPS
    rel = None
    for c in ("relvol", "rel_vol", "Relative Volume"):
        if c in df.columns:
            rel = pd.to_numeric(df[c], errors="coerce")
            break
    if rel is not None:
        ok &= ~((rel > 0) & (rel < RELVOL_DEAD))
    return ok


def describe_row(row) -> str:
    bits = []
    for col, lab in (("s_join", "join"), ("s_general", "gen"),
                     ("s_ab", "AB"), ("s_peer", "peer")):
        try:
            bits.append(f"{lab}={float(row.get(col) or 0):+.2f}")
        except (TypeError, ValueError):
            pass
    return " ".join(bits)


def core_fired(df: pd.DataFrame) -> dict[str, bool]:
    out: dict[str, bool] = {}
    if df is None or df.empty:
        return {col: False for col in CORE}
    for col in CORE:
        if col not in df.columns:
            out[col] = False
            continue
        out[col] = bool(_num(df[col]).abs().ge(EPS).any())
    return out


def pile_status(df: pd.DataFrame) -> dict:
    """Family-level diagnosis the book writes into meta.green_pile."""
    fired = core_fired(df)
    missing = [CORE_LABEL[c] for c, ok in fired.items() if not ok]
    mask = green_mask(df)
    n = int(mask.sum()) if len(mask) else 0
    n_liq = n
    if df is not None and not df.empty and "market_cap_m" in df.columns:
        mcap = pd.to_numeric(df["market_cap_m"], errors="coerce").fillna(0.0)
        size = (
            df["size"].astype(str).str.lower()
            if "size" in df.columns
            else pd.Series("", index=df.index)
        )
        n_liq = int((mask & (mcap >= MIN_LIQUID_MCAP_M) & ~size.eq("micro")).sum())
    n_uni = int(len(df)) if df is not None and not df.empty else 0
    used = n_liq >= GREEN_MIN
    if used:
        reason = (
            f"pile {n_liq} ≥ {GREEN_MIN} liquid all-green names — "
            "BUY 15 from the pile; SELL stays on core weights"
        )
    elif missing:
        reason = (
            f"pile {n_liq} < {GREEN_MIN} — {', '.join(missing)} did not fire "
            "(family all ~0). Fallback weighted walk; SELL stays on core"
        )
    else:
        reason = (
            f"pile {n_liq} < {GREEN_MIN} liquid all-green names. "
            "Fallback weighted walk; SELL stays on core"
        )
    return {
        "n_pile": n_liq,
        "n_pile_raw": n,
        "n_pile_liquid": n_liq,
        "n_universe": n_uni,
        "used": used,
        "min": GREEN_MIN,
        "eps": EPS,
        "relvol_dead": RELVOL_DEAD,
        "core_fired": {CORE_LABEL[c]: v for c, v in fired.items()},
        "missing_core": missing,
        "buy_mode": "green_pile" if used else "weighted_fallback",
        "sell_mode": "core_weights",
        "caps": {
            "min_mcap_m": 400.0,
            "max_per_sector": 4,
            "max_per_industry": 3,
            "max_large_mega": 4,
        },
        "reason": reason,
    }
