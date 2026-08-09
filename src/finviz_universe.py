"""Load Finviz export (with Industry + Finviz_Description) → ticker expand.

Looks for, in order:
  1) FINVIZ_CSV env path
  2) data/finviz/latest.csv
  3) newest data/finviz/*.csv
"""
from __future__ import annotations

import glob
import os
import re
from functools import lru_cache

import pandas as pd

from .event_edges import BUCKET_DESC_KEYWORDS, BUCKET_TO_INDUSTRIES


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
    # normalize columns
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
    if not tcol or not icol:
        return pd.DataFrame()
    out = pd.DataFrame({
        "ticker": df[tcol].astype(str).str.strip().str.upper(),
        "industry": df[icol].astype(str).str.strip(),
        "sector": df[scol].astype(str).str.strip() if scol else "",
        "description": df[dcol].fillna("").astype(str) if dcol else "",
        "market_cap": pd.to_numeric(df[mcol], errors="coerce") if mcol else float("nan"),
    })
    out = out[out["ticker"].str.len() > 0]
    out = out[~out["ticker"].str.startswith("nan", na=False)]
    return out.reset_index(drop=True)


def tickers_for_bucket(
    bucket: str,
    universe: pd.DataFrame | None = None,
    max_names: int = 40,
    min_mcap: float | None = None,
) -> list[str]:
    """Expand edge bucket → ticker list using Industry (+ optional desc keywords)."""
    u = universe if universe is not None else load_universe()
    if u.empty:
        return []
    industries = BUCKET_TO_INDUSTRIES.get(bucket, ())
    if not industries:
        return []
    sub = u[u["industry"].isin(industries)].copy()
    kws = BUCKET_DESC_KEYWORDS.get(bucket)
    if kws and len(sub) > 0:
        pat = re.compile("|".join(re.escape(k) for k in kws), re.I)
        mask = sub["description"].str.contains(pat, na=False)
        # if keywords match anyone, prefer them; else keep industry set
        if mask.any():
            sub = sub[mask]
    if min_mcap is not None and "market_cap" in sub.columns:
        sub = sub[sub["market_cap"].fillna(0) >= min_mcap]
    sub = sub.sort_values("market_cap", ascending=False, na_position="last")
    return sub["ticker"].head(max_names).tolist()


def industry_counts(universe: pd.DataFrame | None = None) -> dict[str, int]:
    u = universe if universe is not None else load_universe()
    if u.empty:
        return {}
    return u["industry"].value_counts().to_dict()
