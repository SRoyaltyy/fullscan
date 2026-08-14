"""Optional peer-RS helpers used by stock_book when peer_rs outputs exist."""
from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
PEER_DIR = ROOT / "data" / "peers"


def load_peer_rs(date: str) -> pd.DataFrame:
    exact = PEER_DIR / f"{date}_peer_rs.csv"
    if exact.exists():
        df = pd.read_csv(exact)
    else:
        files = sorted(PEER_DIR.glob("????-??-??_peer_rs.csv"))
        files = [f for f in files if f.name[:10] <= date]
        if not files:
            return pd.DataFrame()
        df = pd.read_csv(files[-1])
    if df.empty:
        return df
    df["Ticker"] = df["Ticker"].astype(str).str.strip().str.upper()
    return df.drop_duplicates(subset=["Ticker"], keep="first")


def peer_signal(rs_week, rs_month, horizon: str) -> float:
    """Map peer RS (%) into [-1, 1]. Longer horizons lean on month RS."""
    w = float(rs_week) if rs_week == rs_week else 0.0
    m = float(rs_month) if rs_month == rs_month else 0.0
    if horizon in ("1d", "3d"):
        raw, scale = 0.7 * w + 0.3 * m, 12.0
    elif horizon == "1w":
        raw, scale = 0.5 * w + 0.5 * m, 15.0
    else:
        raw, scale = 0.35 * w + 0.65 * m, 20.0
    return float(np.tanh(raw / scale))
