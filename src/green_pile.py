"""Green pile: this ticker's tape is clean — not a weighted beauty contest.

A name is green when:
  join, AB, peer are all >= EPS (name-specific tape must have fired)
  sector and news are not red
  Finviz relative volume is not red (< 0.7) when a print exists
  a hard-red general (market stamp × beta ≤ −HARD_RED) is a veto
  unless the same-day sector call is green (relative-strength exception)

General is a market-wide SPX stamp, not a name-specific tape. A modest
red general (typical −0.07 on a slightly down open) must not empty the
pile and dump the book onto a weighted walk. Missing / yellow on news,
digest, judge, sector, general, or relvol (no Finviz print) is not a
veto. A red is — except the hard-general exception above.

BUY 15 is filled from that pile, ranked by green_rank = mean of the
three name cores (no opp, no weights). Same $400M / 4-per-sector /
3-per-industry / 4 large-mega caps. If the liquid pile is thinner than
GREEN_MIN (usually no AB/peer file yet), keep the weighted walk so
pre-open does not go blank.

SELL never shorts a green name. When the pile is used, shorts rank on
core weights from the non-green remainder. Otherwise core weights on
the full universe.
"""
from __future__ import annotations

import pandas as pd

EPS = 0.05
HARD_RED = 0.25
RELVOL_DEAD = 0.7
GREEN_MIN = 8
MIN_LIQUID_MCAP_M = 400.0
CORE = ("s_join", "s_ab", "s_peer")
CORE_LABEL = {"s_join": "join", "s_ab": "AB", "s_peer": "peer"}


def _num(s: pd.Series) -> pd.Series:
    return pd.to_numeric(s, errors="coerce").fillna(0.0)


def _relvol(df: pd.DataFrame) -> pd.Series | None:
    for c in ("relvol", "rel_vol", "Relative Volume"):
        if c in df.columns:
            return pd.to_numeric(df[c], errors="coerce")
    return None


def attach_ranks(df: pd.DataFrame) -> pd.DataFrame:
    """green_rank = equal mean of the name cores. s_tape = mean(AB, peer)."""
    if df is None or df.empty:
        return df
    out = df.copy()
    cores = [_num(out[c]) if c in out.columns else pd.Series(0.0, index=out.index) for c in CORE]
    out["green_rank"] = sum(cores) / float(len(CORE))
    ab = _num(out["s_ab"]) if "s_ab" in out.columns else pd.Series(0.0, index=out.index)
    peer = _num(out["s_peer"]) if "s_peer" in out.columns else pd.Series(0.0, index=out.index)
    out["s_tape"] = (ab + peer) / 2.0
    return out


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
    if "s_general" in df.columns:
        gen = _num(df["s_general"])
        sec = (
            _num(df["s_sector"])
            if "s_sector" in df.columns
            else pd.Series(0.0, index=df.index)
        )
        hard_gen = gen <= -HARD_RED
        sector_support = sec >= EPS
        ok &= ~(hard_gen & ~sector_support)
    rel = _relvol(df)
    if rel is not None:
        printed = rel.notna() & (rel > 0)
        dead = printed & (rel < RELVOL_DEAD)
        ok &= ~dead
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
            "BUY 15 from the pile by green_rank (no opp); "
            "SELL is core weights on the non-green remainder"
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
        "hard_red": HARD_RED,
        "relvol_dead": RELVOL_DEAD,
        "rank_col": "green_rank",
        "core_fired": {CORE_LABEL[c]: v for c, v in fired.items()},
        "missing_core": missing,
        "buy_mode": "green_pile" if used else "weighted_fallback",
        "sell_mode": "core_weights_ex_green" if used else "core_weights",
        "caps": {
            "min_mcap_m": 400.0,
            "max_per_sector": 4,
            "max_per_industry": 3,
            "max_large_mega": 4,
        },
        "reason": reason,
    }
