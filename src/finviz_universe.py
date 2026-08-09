"""Load Finviz export → ticker expand with preferred liquid sets."""
from __future__ import annotations

import glob
import os
import re
from functools import lru_cache

import pandas as pd

from .event_edges import BUCKET_DESC_KEYWORDS, BUCKET_PREFERRED, BUCKET_TO_INDUSTRIES


def _find_csv() -> str | None:
    env = os.environ.get("FINVIZ_CSV", "").strip()
    if env and os.path.isfile(env):
        return env
    latest = "data/finviz/latest.csv"
    if os.path.isfile(latest):
        return latest
    paths = sorted(glob.glob("data/finviz/*.csv"))
    return paths[-1] if paths else None


@lru_cache(maxsize=1)
def load_universe(path: str | None = None) -> pd.DataFrame:
    p = path or _find_csv()
    if not p:
        return pd.DataFrame()
    df = pd.read_csv(p, low_memory=False)
    cols = {c.lower().strip(): c for c in df.columns}

    def col(*names):
        for n in names:
            if n.lower() in cols:
                return cols[n.lower()]
        return None

    tcol = col("Ticker", "ticker")
    icol = col("Industry", "industry")
    scol = col("Sector", "sector")
    dcol = col("Finviz_Description", "Description", "description")
    mcol = col("Market Cap", "Market Cap")
    ccol = col("Country", "country")
    if not tcol or not icol:
        return pd.DataFrame()
    out = pd.DataFrame({
        "ticker": df[tcol].astype(str).str.strip().str.upper(),
        "industry": df[icol].astype(str).str.strip(),
        "sector": df[scol].astype(str).str.strip() if scol else "",
        "description": df[dcol].fillna("").astype(str) if dcol else "",
        "market_cap": pd.to_numeric(df[mcol], errors="coerce") if mcol else float("nan"),
        "country": df[ccol].astype(str).str.strip() if ccol else "",
    })
    out = out[out["ticker"].str.len().gt(0)]
    return out.reset_index(drop=True)


def tickers_for_bucket(
    bucket: str,
    universe: pd.DataFrame | None = None,
    max_names: int = 6,
    min_mcap: float | None = 1000,
    prefer_us: bool = True,
) -> list[str]:
    """Preferred liquid list first; fall back to industry (US-biased)."""
    u = universe if universe is not None else load_universe()
    preferred = list(BUCKET_PREFERRED.get(bucket, ()))
    if not u.empty and preferred:
        have = set(u["ticker"])
        hit = [t for t in preferred if t in have]
        if hit:
            return hit[:max_names]
        # preferred not in this snapshot — still return preferred as soft list
        if preferred:
            return preferred[:max_names]

    if u.empty:
        return preferred[:max_names]

    industries = BUCKET_TO_INDUSTRIES.get(bucket, ())
    if not industries:
        return preferred[:max_names]
    sub = u[u["industry"].isin(industries)].copy()
    if prefer_us and "country" in sub.columns and sub["country"].notna().any():
        us = sub[sub["country"].str.upper().isin(["USA", "UNITED STATES", "US"])]
        if len(us) >= 2:
            sub = us
    kws = BUCKET_DESC_KEYWORDS.get(bucket)
    if kws is not None and len(sub) > 0:
        pat = re.compile("|".join(re.escape(k) for k in kws), re.I)
        mask = sub["description"].str.contains(pat, na=False)
        if mask.any():
            sub = sub[mask]
    if min_mcap is not None:
        sub = sub[sub["market_cap"].fillna(0) >= min_mcap]
    sub = sub.sort_values("market_cap", ascending=False, na_position="last")
    return sub["ticker"].head(max_names).tolist()
