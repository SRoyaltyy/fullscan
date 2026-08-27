"""Green pile: every *present* name-layer is green, silent yellows ignored.

A name is green when:
  join, general, AB, peer are all >= EPS (actually fired for the name)
  sector and news are not red
  relative volume is not dead (< 0.7) when we have a Finviz print

Yellow on news/digest/judge/sector (flat / no headline) is treated as
no data — not a veto. Black / missing heat and catalyst are ignored.

Days where AB+peer are still all zeros will not produce a pile; the
ranker then keeps the old weighted walk.
"""
from __future__ import annotations

import pandas as pd

EPS = 0.05
RELVOL_DEAD = 0.7
GREEN_MIN = 8
CORE = ("s_join", "s_general", "s_ab", "s_peer")


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
