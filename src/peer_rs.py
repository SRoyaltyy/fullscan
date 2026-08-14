"""Peer relative strength — Finviz Compare-style layer.

Uses data/peers/correlations.csv (ticker → up to 10 correlated peers) and the
dated Finviz export to compute:

  rs_week  = Performance(Week)_stock  − median(Performance(Week)_peers)
  rs_month = Performance(Month)_stock − median(Performance(Month)_peers)
  beat_week_pct = fraction of peers the stock beat on the week

Positive rs = stock outperforming its peer basket (leadership).
Negative rs = lagging peers (relative weakness).

CLI:
  python -m src.peer_rs [--date YYYY-MM-DD]
"""
from __future__ import annotations

import argparse
from pathlib import Path

import numpy as np
import pandas as pd

from . import config

ROOT = Path(__file__).resolve().parent.parent
CORR_PATH = ROOT / "data" / "peers" / "correlations.csv"
EXPORT_DIR = ROOT / "data" / "exports"
OUT_DIR = ROOT / "data" / "peers"
DAILY = ROOT / "01_daily"

PERF_WEEK = "Performance (Week)"
PERF_MONTH = "Performance (Month)"
PERF_Q = "Performance (Quarter)"
CHANGE = "Change"


def _pct(x) -> float:
    if x is None or (isinstance(x, float) and np.isnan(x)):
        return np.nan
    s = str(x).replace("%", "").replace(",", "").strip()
    if s in ("", "-", "nan", "None", "—"):
        return np.nan
    try:
        return float(s)
    except ValueError:
        return np.nan


def _resolve_export(date: str | None) -> tuple[str, Path]:
    files = sorted(EXPORT_DIR.glob("finviz_*.csv"))
    if not files:
        raise SystemExit("[peer_rs] no data/exports/finviz_*.csv — run Finviz export first")
    if date is None:
        path = files[-1]
        return path.stem.replace("finviz_", ""), path
    path = EXPORT_DIR / f"finviz_{date}.csv"
    if not path.exists():
        older = [f for f in files if f.stem.replace("finviz_", "") <= date]
        if not older:
            raise SystemExit(f"[peer_rs] no finviz export for {date}")
        path = older[-1]
        date = path.stem.replace("finviz_", "")
    return date, path


def _load_correlations() -> dict[str, list[str]]:
    if not CORR_PATH.exists():
        raise SystemExit(f"[peer_rs] missing {CORR_PATH}")
    df = pd.read_csv(CORR_PATH)
    out: dict[str, list[str]] = {}
    peer_cols = [c for c in df.columns if c.startswith("peer_")]
    for _, r in df.iterrows():
        t = str(r.get("ticker", "")).strip().upper()
        if not t or t == "NAN":
            continue
        peers = []
        for c in peer_cols:
            p = str(r.get(c, "")).strip().upper()
            if p and p not in ("NAN", "NONE", t):
                peers.append(p)
        out[t] = peers
    return out


def run(date: str | None = None) -> Path:
    date, export_path = _resolve_export(date)
    corr = _load_correlations()

    raw = pd.read_csv(export_path, low_memory=False)
    tcol = "Ticker" if "Ticker" in raw.columns else raw.columns[0]
    raw[tcol] = raw[tcol].astype(str).str.strip().str.upper()
    raw = raw.drop_duplicates(subset=[tcol], keep="first")

    week = raw.set_index(tcol)[PERF_WEEK].map(_pct) if PERF_WEEK in raw.columns else pd.Series(dtype=float)
    month = raw.set_index(tcol)[PERF_MONTH].map(_pct) if PERF_MONTH in raw.columns else pd.Series(dtype=float)
    quarter = raw.set_index(tcol)[PERF_Q].map(_pct) if PERF_Q in raw.columns else pd.Series(dtype=float)
    change = raw.set_index(tcol)[CHANGE].map(_pct) if CHANGE in raw.columns else pd.Series(dtype=float)

    rows = []
    for ticker, peers in corr.items():
        if ticker not in week.index and ticker not in change.index:
            continue
        own_w = float(week.get(ticker, np.nan)) if ticker in week.index else np.nan
        own_m = float(month.get(ticker, np.nan)) if ticker in month.index else np.nan
        own_q = float(quarter.get(ticker, np.nan)) if ticker in quarter.index else np.nan
        own_ch = float(change.get(ticker, np.nan)) if ticker in change.index else np.nan

        present = [p for p in peers if p in week.index]
        if not present:
            rows.append({
                "Ticker": ticker,
                "n_peers": len(peers),
                "n_peers_present": 0,
                "perf_week": own_w,
                "perf_month": own_m,
                "perf_quarter": own_q,
                "change": own_ch,
                "peer_med_week": np.nan,
                "peer_med_month": np.nan,
                "rs_week": np.nan,
                "rs_month": np.nan,
                "rs_quarter": np.nan,
                "beat_week_pct": np.nan,
                "peers": "|".join(peers[:10]),
                "peers_used": "",
            })
            continue

        peer_w = week.reindex(present).dropna()
        peer_m = month.reindex(present).dropna()
        peer_q = quarter.reindex(present).dropna()

        med_w = float(peer_w.median()) if len(peer_w) else np.nan
        med_m = float(peer_m.median()) if len(peer_m) else np.nan
        med_q = float(peer_q.median()) if len(peer_q) else np.nan

        rs_w = own_w - med_w if own_w == own_w and med_w == med_w else np.nan
        rs_m = own_m - med_m if own_m == own_m and med_m == med_m else np.nan
        rs_q = own_q - med_q if own_q == own_q and med_q == med_q else np.nan

        beat = np.nan
        if own_w == own_w and len(peer_w):
            beat = float((peer_w < own_w).mean())

        rows.append({
            "Ticker": ticker,
            "n_peers": len(peers),
            "n_peers_present": len(present),
            "perf_week": own_w,
            "perf_month": own_m,
            "perf_quarter": own_q,
            "change": own_ch,
            "peer_med_week": med_w,
            "peer_med_month": med_m,
            "rs_week": rs_w,
            "rs_month": rs_m,
            "rs_quarter": rs_q,
            "beat_week_pct": beat,
            "peers": "|".join(peers[:10]),
            "peers_used": "|".join(present[:10]),
        })

    out = pd.DataFrame(rows)
    if out.empty:
        raise SystemExit("[peer_rs] no rows scored")

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    path = OUT_DIR / f"{date}_peer_rs.csv"
    out.to_csv(path, index=False)

    ranked = out.dropna(subset=["rs_week"]).sort_values("rs_week", ascending=False)
    L = [
        f"# Peer relative strength — {date}",
        "",
        "Finviz Compare-style: stock Performance − median(peer Performance).",
        f"Source: `{export_path.name}` × `data/peers/correlations.csv`.",
        "",
        f"- Universe with peers scored: **{len(out):,}**",
        f"- With usable week RS: **{ranked.shape[0]:,}**",
        "",
        "## Top 20 leadership (rs_week)",
        "",
        "| Ticker | rs_week | own_w | peer_med_w | beat% | peers_used |",
        "|---|---|---|---|---|---|",
    ]
    for _, r in ranked.head(20).iterrows():
        L.append(
            f"| {r['Ticker']} | {r['rs_week']:+.1f} | {r['perf_week']:+.1f} | "
            f"{r['peer_med_week']:+.1f} | {100*(r['beat_week_pct'] or 0):.0f}% | "
            f"{str(r['peers_used'])[:40]} |"
        )
    L += [
        "",
        "## Bottom 20 lagging (rs_week)",
        "",
        "| Ticker | rs_week | own_w | peer_med_w | beat% | peers_used |",
        "|---|---|---|---|---|---|",
    ]
    for _, r in ranked.tail(20).iloc[::-1].iterrows():
        L.append(
            f"| {r['Ticker']} | {r['rs_week']:+.1f} | {r['perf_week']:+.1f} | "
            f"{r['peer_med_week']:+.1f} | {100*(r['beat_week_pct'] or 0):.0f}% | "
            f"{str(r['peers_used'])[:40]} |"
        )
    md = DAILY / f"{date}_peer_rs.md"
    DAILY.mkdir(parents=True, exist_ok=True)
    md.write_text("\n".join(L) + "\n", encoding="utf-8")

    print(
        f"[peer_rs] {date}: {len(out):,} tickers | "
        f"rs_week usable={ranked.shape[0]:,} -> {path.name}, {md.name}"
    )
    if len(ranked):
        top = ranked.iloc[0]
        bot = ranked.iloc[-1]
        print(
            f"[peer_rs] leadership: {top['Ticker']} rs_week={top['rs_week']:+.1f} | "
            f"laggard: {bot['Ticker']} rs_week={bot['rs_week']:+.1f}"
        )
    return path


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", default=None)
    args = ap.parse_args()
    run(args.date)


if __name__ == "__main__":
    main()
