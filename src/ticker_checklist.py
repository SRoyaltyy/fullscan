"""Daily per-ticker checklist — items 1,3,4,5 with full date/price audit trail."""
from __future__ import annotations

import argparse
import json
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd

from . import config
from .peer_rs import _load_correlations
from . import price_store as ps

ROOT = Path(__file__).resolve().parent.parent
EXPORT_DIR = ROOT / "data" / "exports"
OUT_DIR = ROOT / "data" / "checklist"
DAILY = ROOT / "01_daily"
ET = ZoneInfo(config.TZ)


def _load_export(date: str):
    path = EXPORT_DIR / f"finviz_{date}.csv"
    if not path.exists():
        files = sorted(EXPORT_DIR.glob("finviz_????-??-??.csv"))
        files = [f for f in files if f.stem.replace("finviz_", "") <= date]
        if not files:
            raise SystemExit(f"[checklist] no finviz export for {date}")
        path = files[-1]
        date = path.stem.replace("finviz_", "")
    df = pd.read_csv(path, low_memory=False)
    tcol = "Ticker" if "Ticker" in df.columns else df.columns[0]
    df[tcol] = df[tcol].astype(str).str.strip().str.upper()
    df = df.drop_duplicates(subset=[tcol], keep="first").set_index(tcol)
    return df, date


def run(date: str | None = None, tickers: list[str] | None = None) -> Path:
    exports = sorted(EXPORT_DIR.glob("finviz_????-??-??.csv"))
    if not exports:
        raise SystemExit("[checklist] no finviz exports")
    if date is None:
        date = exports[-1].stem.replace("finviz_", "")

    finviz, date = _load_export(date)
    names = ([t.upper() for t in tickers if t.upper() in finviz.index] if tickers else list(finviz.index))

    store = ps._load_store()
    if not len(store):
        raise SystemExit(
            "[checklist] price store empty. Run:\n"
            "  python -m src.price_store bootstrap --days 400\n"
            "then daily: python -m src.price_store update"
        )
    store = store[store["date"] <= pd.Timestamp(date)]
    if store.empty:
        raise SystemExit(f"[checklist] price store has no bars on/before {date}")

    corr = _load_correlations()
    need = set(names)
    for t in names:
        need.update(corr.get(t, [])[:10])
    need = need & set(store["ticker"].unique())
    print(f"[checklist] {date}: {len(names):,} names | OHLC {store['ticker'].nunique():,} | peer panel {len(need):,}")

    sub = store[store["ticker"].isin(need)]
    close_panel = sub.pivot_table(index="date", columns="ticker", values="close", aggfunc="last").sort_index()
    ohlc_groups = {t: g.set_index("date").sort_index() for t, g in store.groupby("ticker")}

    rows = []
    for t in names:
        meta = finviz.loc[t] if t in finviz.index else None
        ohlc = ohlc_groups.get(t)
        if ohlc is not None:
            ohlc = ohlc.copy()
            ohlc.columns = [c.lower() for c in ohlc.columns]

        c1 = ps.candle_bias(ohlc if ohlc is not None else pd.DataFrame(), lookback=10)
        closes = ohlc["close"] if ohlc is not None and "close" in ohlc.columns else pd.Series(dtype=float)
        c3 = ps.consecutive_down(closes)
        c45 = ps.peer_compare_7d(t, corr.get(t, []), close_panel, horizon=7)

        score = int(c1.get("bull", 0) + c3.get("bull", 0) + c45.get("bull_outperform", 0) + c45.get("bull_breadth", 0))
        flags = [c1.get("pass"), c3.get("pass"), c45.get("pass_outperform"), c45.get("pass_breadth")]
        n_pass = sum(1 for x in flags if x is True)
        n_fail = sum(1 for x in flags if x is False)

        rows.append({
            "Ticker": t,
            "asof_date": date,
            "sector": (meta.get("Sector") if meta is not None else ""),
            "industry": (meta.get("Industry") if meta is not None else ""),
            "price": float(closes.iloc[-1]) if len(closes) else np.nan,
            "c1_candle_pass": c1.get("pass"),
            "c1_candle_bull": c1.get("bull"),
            "c1_asof": c1.get("asof"),
            "c1_window_start": c1.get("window_start"),
            "c1_window_end": c1.get("window_end"),
            "c1_n_sessions": c1.get("n"),
            "c1_green_body": c1.get("green"),
            "c1_red_body": c1.get("red"),
            "c1_sessions": c1.get("sessions"),
            "c1_detail": c1.get("detail"),
            "c3_down_pass": c3.get("pass"),
            "c3_down_bull": c3.get("bull"),
            "c3_down_n": c3.get("n"),
            "c3_asof": c3.get("asof"),
            "c3_steps": c3.get("steps"),
            "c3_detail": c3.get("detail"),
            "c4_peer_outperform_pass": c45.get("pass_outperform"),
            "c4_peer_outperform_bull": c45.get("bull_outperform"),
            "c4_asof": c45.get("asof"),
            "c4_d0": c45.get("d0"),
            "c4_d1": c45.get("d1"),
            "c4_px_d0": c45.get("px_d0"),
            "c4_px_d1": c45.get("px_d1"),
            "c4_ret_7d": c45.get("ret_7d"),
            "c4_baseline_date": c45.get("baseline_date"),
            "c4_baseline_px": c45.get("baseline_px"),
            "c4_rel_d0": c45.get("rel_d0"),
            "c4_rel_d1": c45.get("rel_d1"),
            "c4_peer_med_rel_d0": c45.get("peer_med_rel_d0"),
            "c4_peer_med_rel_d1": c45.get("peer_med_rel_d1"),
            "c4_rs_7d": c45.get("rs_7d"),
            "c4_overtake_7d": c45.get("overtake_7d"),
            "c4_leadership_7d": c45.get("leadership_7d"),
            "c4_peers_used": c45.get("peers_used"),
            "c4_peer_rets": c45.get("peer_rets"),
            "c4_detail": c45.get("detail"),
            "c5_peer_breadth_pass": c45.get("pass_breadth"),
            "c5_peer_breadth_bull": c45.get("bull_breadth"),
            "c5_peer_breadth_7d": c45.get("peer_breadth_7d"),
            "c5_peer_med_ret_7d": c45.get("peer_med_ret_7d"),
            "checklist_score": score,
            "n_pass": n_pass,
            "n_fail": n_fail,
            "n_bars": int(len(closes)),
        })

    out = pd.DataFrame(rows)
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    DAILY.mkdir(parents=True, exist_ok=True)
    csv_path = OUT_DIR / f"{date}_checklist.csv"
    out.to_csv(csv_path, index=False)

    ranked = out.sort_values("checklist_score", ascending=False)
    (OUT_DIR / f"{date}_checklist.json").write_text(json.dumps({
        "date": date, "generated": datetime.now(ET).isoformat(), "n": len(out),
        "column_legend": {
            "c1_sessions": "date:open->close:body:G|R (last 10 sessions)",
            "c3_steps": "prior_date:prior_close->date:close",
            "c4_rel_*": "price/baseline_1y - 1 (Finviz Compare line)",
            "c4_peer_rets": "PEER:d0->d1:px0->px1:ret%",
            "c4_rs_7d": "(rel_d1-rel_d0)-(peer_med_rel_d1-peer_med_rel_d0)",
        },
        "top": ranked.head(30).to_dict("records"),
    }, indent=2, default=str), encoding="utf-8")

    L = [
        f"# Ticker checklist (1,3,4,5) — {date}", "",
        "Every value is tied to **dates and prices** (see CSV).", "",
        "## How numbers are built", "",
        "### 1 candle_bias", "- Last 10 sessions in `c1_sessions`: `date:open->close:body:G|R`", "",
        "### 3 consecutive_down", "- `c3_steps`: `prior_date:px -> date:px`", "",
        "### 4 peer outperform", "- Baseline ~1Y: `c4_baseline_date` / `c4_baseline_px`",
        "- `rel = price/baseline - 1` at d0/d1", "- `c4_rs_7d = (rel_d1-rel_d0) - (peer_med_rel_d1-peer_med_rel_d0)`", "",
        "### 5 peer breadth", "- Share of peers with positive 7d return; detail in `c4_peer_rets`", "",
        f"- Names: **{len(out):,}** | CSV: `data/checklist/{date}_checklist.csv`", "",
        "## Top 15", "",
        "| Ticker | Score | c1 window | c3_n | d0→d1 | rel d0→d1 | rs7 | overtake | breadth |",
        "|---|---|---|---|---|---|---|---|---|",
    ]
    for _, r in ranked.head(15).iterrows():
        L.append(
            f"| {r['Ticker']} | {r['checklist_score']:+d} | {r['c1_window_start']}→{r['c1_window_end']} | "
            f"{r['c3_down_n']} | {r['c4_d0']}→{r['c4_d1']} | "
            f"{r['c4_rel_d0'] if pd.notna(r['c4_rel_d0']) else '—'}→{r['c4_rel_d1'] if pd.notna(r['c4_rel_d1']) else '—'} | "
            f"{r['c4_rs_7d'] if pd.notna(r['c4_rs_7d']) else '—'} | {r['c4_overtake_7d']} | {r['c5_peer_breadth_7d']} |"
        )
    md = DAILY / f"{date}_checklist.md"
    md.write_text("\n".join(L) + "\n", encoding="utf-8")
    print(f"[checklist] {date}: {len(out):,} -> {csv_path.name}, {md.name}")
    return csv_path


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", default=None)
    ap.add_argument("--tickers", default=None)
    args = ap.parse_args()
    tickers = [t.strip() for t in args.tickers.split(",")] if args.tickers else None
    run(date=args.date, tickers=tickers)


if __name__ == "__main__":
    main()
