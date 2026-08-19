"""Point-in-time peer + sector context for each asof day.

Peers (no Finviz required):
  From price_store OHLC + data/peers correlations map:
    ret_5d(stock) − median(ret_5d(peers))  ≈ week RS
    beat_pct, peers_advancing

Sector (when artifacts exist):
  Nearest 01_daily/sectors/<date>/_BOARD.md with date <= asof
  → Dir / Score for the stock's Finviz Sector name

Industry (when Finviz export <= asof exists):
  median Performance(Week) of same Industry on that export
  else: median 5d return of industry members present in price store
"""
from __future__ import annotations

import re
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
SECTOR_DIR = ROOT / "01_daily" / "sectors"
PEERS_DIR = ROOT / "data" / "peers"
EXPORT_DIR = ROOT / "data" / "exports"


def load_corr_map() -> dict[str, list[str]]:
    try:
        from . import peer_rs

        return peer_rs._load_correlations()
    except Exception as e:
        print(f"[context] corr map unavailable: {e}")
        return {}


def _ret_nd(ohlc: pd.DataFrame, asof: str, n: int = 5) -> float:
    if ohlc is None or ohlc.empty:
        return np.nan
    df = ohlc.copy()
    if not isinstance(df.index, pd.DatetimeIndex):
        df.index = pd.to_datetime(df.index)
    df.columns = [c.lower() for c in df.columns]
    if "close" not in df.columns:
        return np.nan
    sub = df[df.index <= pd.Timestamp(asof)]
    if len(sub) < n + 1:
        return np.nan
    c0 = float(sub["close"].iloc[-(n + 1)])
    c1 = float(sub["close"].iloc[-1])
    if c0 <= 0:
        return np.nan
    return c1 / c0 - 1.0


def peer_context_asof(
    ticker: str,
    asof: str,
    groups: dict[str, pd.DataFrame],
    corr_map: dict[str, list[str]],
    n: int = 5,
) -> dict:
    t = ticker.upper()
    peers = [p for p in corr_map.get(t, []) if p in groups]
    own = _ret_nd(groups.get(t), asof, n)
    peer_rets = []
    for p in peers:
        r = _ret_nd(groups.get(p), asof, n)
        if np.isfinite(r):
            peer_rets.append(r)
    if not peer_rets or not np.isfinite(own):
        return {
            "rs_5d": np.nan,
            "own_5d": own,
            "peer_med_5d": np.nan,
            "beat_pct_5d": np.nan,
            "peers_up_5d": np.nan,
            "n_peers_used": len(peer_rets),
            "P01": 0,
            "P02": 0,
        }
    med = float(np.median(peer_rets))
    beat = float(np.mean([own > r for r in peer_rets]))
    rs = own - med
    p01 = 1 if (rs > 0 and beat >= 0.5) else (-1 if (rs < 0 and beat <= 0.5) else 0)
    p02 = 1 if med > 0 else (-1 if med < 0 else 0)
    return {
        "rs_5d": rs,
        "own_5d": own,
        "peer_med_5d": med,
        "beat_pct_5d": beat,
        "peers_up_5d": float(np.mean([r > 0 for r in peer_rets])),
        "n_peers_used": len(peer_rets),
        "P01": p01,
        "P02": p02,
    }


def load_sector_boards() -> list[tuple[str, dict[str, dict]]]:
    """Ascending list of (date, {Sector: {dir, score, conf}})."""
    if not SECTOR_DIR.exists():
        return []
    out = []
    for board in sorted(SECTOR_DIR.glob("*/_BOARD.md")):
        d = board.parent.name
        if not re.match(r"\d{4}-\d{2}-\d{2}", d):
            continue
        text = board.read_text(encoding="utf-8", errors="replace")
        sec: dict[str, dict] = {}
        for line in text.splitlines():
            if not line.startswith("|") or line.startswith("|-"):
                continue
            parts = [c.strip() for c in line.strip("|").split("|")]
            if len(parts) < 6:
                continue
            name, etf, direction, mag, score_s, conf_s = parts[:6]
            if name.lower() in ("sector", ""):
                continue
            try:
                score = float(score_s)
            except ValueError:
                score = np.nan
            sec[name] = {
                "dir": direction.lower().strip(),
                "score": score,
                "mag": mag,
                "etf": etf,
                "board_date": d,
            }
        if sec:
            out.append((d, sec))
    return out


def sector_context_asof(
    sector_name: str | None,
    asof: str,
    boards: list[tuple[str, dict[str, dict]]],
) -> dict:
    empty = {"sector_dir": None, "sector_score": np.nan, "sector_board_date": None, "P04": 0}
    if not sector_name or not boards:
        return empty
    # latest board on or before asof; else earliest available (still label date)
    cand = [(d, s) for d, s in boards if d <= asof]
    if not cand:
        return empty
    d, secmap = cand[-1]
    info = secmap.get(sector_name)
    if info is None:
        for k, v in secmap.items():
            if k.lower() == str(sector_name).lower():
                info = v
                break
    if not info:
        return {**empty, "sector_board_date": d}
    direction = info.get("dir")
    p04 = 1 if direction == "up" else (-1 if direction == "down" else 0)
    return {
        "sector_dir": direction,
        "sector_score": info.get("score"),
        "sector_board_date": d,
        "P04": p04,
    }


def industry_members_from_latest_export() -> dict[str, list[str]]:
    files = sorted(EXPORT_DIR.glob("finviz_????-??-??.csv"))
    if not files:
        return {}
    df = pd.read_csv(files[-1], low_memory=False)
    tcol = "Ticker" if "Ticker" in df.columns else df.columns[0]
    df["Ticker"] = df[tcol].astype(str).str.strip().str.upper()
    if "Industry" not in df.columns:
        return {}
    out: dict[str, list[str]] = {}
    for ind, g in df.groupby(df["Industry"].astype(str)):
        out[ind] = g["Ticker"].tolist()
    return out


def ticker_sector_industry_from_latest_export(ticker: str) -> tuple[str | None, str | None]:
    files = sorted(EXPORT_DIR.glob("finviz_????-??-??.csv"))
    if not files:
        return None, None
    df = pd.read_csv(files[-1], low_memory=False)
    tcol = "Ticker" if "Ticker" in df.columns else df.columns[0]
    df["Ticker"] = df[tcol].astype(str).str.strip().str.upper()
    hit = df[df["Ticker"] == ticker.upper()]
    if hit.empty:
        return None, None
    row = hit.iloc[0]
    return (
        str(row["Sector"]) if "Sector" in row.index and pd.notna(row["Sector"]) else None,
        str(row["Industry"]) if "Industry" in row.index and pd.notna(row["Industry"]) else None,
    )


def industry_context_asof(
    industry: str | None,
    asof: str,
    groups: dict[str, pd.DataFrame],
    members: dict[str, list[str]],
    n: int = 5,
) -> dict:
    empty = {"ind_med_5d": np.nan, "ind_pct_up_5d": np.nan, "P03": 0, "ind_n": 0}
    if not industry:
        return empty
    tickers = members.get(industry, [])
    rets = []
    for t in tickers:
        if t not in groups:
            continue
        r = _ret_nd(groups[t], asof, n)
        if np.isfinite(r):
            rets.append(r)
    if not rets:
        return empty
    med = float(np.median(rets))
    return {
        "ind_med_5d": med,
        "ind_pct_up_5d": float(np.mean([r > 0 for r in rets])),
        "P03": 1 if med > 0 else (-1 if med < 0 else 0),
        "ind_n": len(rets),
    }


def context_label(p01, p02, p03, p04) -> str:
    bits = []
    if p01 == 1:
        bits.append("LEAD")
    elif p01 == -1:
        bits.append("LAG")
    if p02 == 1:
        bits.append("peers↑")
    elif p02 == -1:
        bits.append("peers↓")
    if p03 == 1:
        bits.append("ind↑")
    elif p03 == -1:
        bits.append("ind↓")
    if p04 == 1:
        bits.append("sec↑")
    elif p04 == -1:
        bits.append("sec↓")
    return ",".join(bits) if bits else "—"
