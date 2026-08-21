"""Daily per-ticker checklist — items 1,3,4,5 with full date/price audit trail.

CLI:
  python -m src.ticker_checklist --date 2026-08-14
  python -m src.ticker_checklist --backfill-from 2026-03-01
  python -m src.ticker_checklist --backfill-from 2026-03-01 --backfill-to 2026-08-14
"""
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
HISTORY_PARQUET = OUT_DIR / "checklist_history.parquet"
HISTORY_CSV = OUT_DIR / "checklist_history.csv"


def _load_export(date: str):
    path = EXPORT_DIR / f"finviz_{date}.csv"
    if not path.exists():
        files = sorted(EXPORT_DIR.glob("finviz_????-??-??.csv"))
        files = [f for f in files if f.stem.replace("finviz_", "") <= date]
        if not files:
            files = sorted(EXPORT_DIR.glob("finviz_????-??-??.csv"))
            if not files:
                return None, date
            path = files[-1]
        else:
            path = files[-1]
    df = pd.read_csv(path, low_memory=False)
    tcol = "Ticker" if "Ticker" in df.columns else df.columns[0]
    df[tcol] = df[tcol].astype(str).str.strip().str.upper()
    df = df.drop_duplicates(subset=[tcol], keep="first").set_index(tcol)
    return df, date


def _score_universe(asof, names, finviz, store, close_panel_full, corr):
    asof_ts = pd.Timestamp(asof)
    panel = close_panel_full.loc[close_panel_full.index <= asof_ts]
    if panel.empty:
        return pd.DataFrame()
    sub_store = store[store["date"] <= asof_ts]
    ohlc_groups = {t: g.set_index("date").sort_index() for t, g in sub_store.groupby("ticker")}
    rows = []
    for t in names:
        meta = finviz.loc[t] if finviz is not None and t in finviz.index else None
        ohlc = ohlc_groups.get(t)
        if ohlc is not None:
            ohlc = ohlc.copy()
            ohlc.columns = [c.lower() for c in ohlc.columns]
        c1 = ps.candle_bias(ohlc if ohlc is not None else pd.DataFrame(), lookback=10)
        closes = ohlc["close"] if ohlc is not None and "close" in ohlc.columns else pd.Series(dtype=float)
        c3 = ps.consecutive_down(closes)
        c45 = ps.peer_compare_7d(t, corr.get(t, []), panel, horizon=7)
        score = int(c1.get("bull", 0) + c3.get("bull", 0) + c45.get("bull_outperform", 0) + c45.get("bull_breadth", 0))
        flags = [c1.get("pass"), c3.get("pass"), c45.get("pass_outperform"), c45.get("pass_breadth")]
        n_pass = sum(1 for x in flags if x is True)
        n_fail = sum(1 for x in flags if x is False)
        rows.append({
            "Ticker": t, "asof_date": asof,
            "sector": (meta.get("Sector") if meta is not None else ""),
            "industry": (meta.get("Industry") if meta is not None else ""),
            "price": float(closes.iloc[-1]) if len(closes) else np.nan,
            "c1_candle_pass": c1.get("pass"), "c1_candle_bull": c1.get("bull"),
            "c1_asof": c1.get("asof"), "c1_window_start": c1.get("window_start"),
            "c1_window_end": c1.get("window_end"), "c1_n_sessions": c1.get("n"),
            "c1_green_body": c1.get("green"), "c1_red_body": c1.get("red"),
            "c1_sessions": c1.get("sessions"), "c1_detail": c1.get("detail"),
            "c3_down_pass": c3.get("pass"), "c3_down_bull": c3.get("bull"),
            "c3_down_n": c3.get("n"), "c3_asof": c3.get("asof"),
            "c3_steps": c3.get("steps"), "c3_detail": c3.get("detail"),
            "c4_peer_outperform_pass": c45.get("pass_outperform"),
            "c4_peer_outperform_bull": c45.get("bull_outperform"),
            "c4_asof": c45.get("asof"), "c4_d0": c45.get("d0"), "c4_d1": c45.get("d1"),
            "c4_px_d0": c45.get("px_d0"), "c4_px_d1": c45.get("px_d1"),
            "c4_ret_7d": c45.get("ret_7d"),
            "c4_baseline_date": c45.get("baseline_date"), "c4_baseline_px": c45.get("baseline_px"),
            "c4_rel_d0": c45.get("rel_d0"), "c4_rel_d1": c45.get("rel_d1"),
            "c4_peer_med_rel_d0": c45.get("peer_med_rel_d0"), "c4_peer_med_rel_d1": c45.get("peer_med_rel_d1"),
            "c4_rs_7d": c45.get("rs_7d"), "c4_overtake_7d": c45.get("overtake_7d"),
            "c4_leadership_7d": c45.get("leadership_7d"), "c4_peers_used": c45.get("peers_used"),
            "c4_peer_rets": c45.get("peer_rets"), "c4_detail": c45.get("detail"),
            "c5_peer_breadth_pass": c45.get("pass_breadth"), "c5_peer_breadth_bull": c45.get("bull_breadth"),
            "c5_peer_breadth_7d": c45.get("peer_breadth_7d"), "c5_peer_med_ret_7d": c45.get("peer_med_ret_7d"),
            "checklist_score": score, "n_pass": n_pass, "n_fail": n_fail, "n_bars": int(len(closes)),
        })
    return pd.DataFrame(rows)


def _write_day_outputs(date: str, out: pd.DataFrame) -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    csv_path = OUT_DIR / f"{date}_checklist.csv"
    out.to_csv(csv_path, index=False)
    ranked = out.sort_values("checklist_score", ascending=False)
    (OUT_DIR / f"{date}_checklist.json").write_text(json.dumps({
        "date": date, "generated": datetime.now(ET).isoformat(), "n": len(out),
        "top": ranked.head(20).to_dict("records"),
    }, indent=2, default=str), encoding="utf-8")
    print(f"[checklist] wrote {csv_path.name} n={len(out):,}")


def _append_history(frames: list) -> None:
    chunk = pd.concat(frames, ignore_index=True)
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    if HISTORY_PARQUET.exists():
        old = pd.read_parquet(HISTORY_PARQUET)
        new_asofs = set(chunk["asof_date"].astype(str))
        old = old[~old["asof_date"].astype(str).isin(new_asofs)]
        hist = pd.concat([old, chunk], ignore_index=True)
    else:
        hist = chunk
    hist.to_parquet(HISTORY_PARQUET, index=False)
    print(f"[checklist] checkpoint history rows={len(hist):,} days={hist['asof_date'].nunique()}")


def run(date=None, tickers=None):
    exports = sorted(EXPORT_DIR.glob("finviz_????-??-??.csv"))
    if date is None:
        if not exports:
            raise SystemExit("[checklist] no finviz exports and no --date")
        date = exports[-1].stem.replace("finviz_", "")
    finviz, _ = _load_export(date)
    store = ps._load_store()
    if not len(store):
        raise SystemExit("[checklist] price store empty — run bootstrap first")
    store = store[store["date"] <= pd.Timestamp(date)]
    if store.empty:
        raise SystemExit(f"[checklist] no bars on/before {date}")
    names = ([t.upper() for t in tickers if t.upper() in set(store["ticker"])] if tickers else sorted(store["ticker"].unique()))
    corr = _load_correlations()
    need = set(names)
    for t in names:
        need.update(corr.get(t, [])[:10])
    need &= set(store["ticker"].unique())
    sub = store[store["ticker"].isin(need)]
    close_panel = sub.pivot_table(index="date", columns="ticker", values="close", aggfunc="last").sort_index()
    print(f"[checklist] single day {date}: {len(names):,} names")
    out = _score_universe(date, names, finviz, store, close_panel, corr)
    _write_day_outputs(date, out)
    if not out.empty:
        _append_history([out])
    return OUT_DIR / f"{date}_checklist.csv"


def backfill(from_date: str, to_date: str | None = None, tickers=None):
    store = ps._load_store()
    if not len(store):
        raise SystemExit("[checklist] price store empty — run bootstrap first")
    store["date"] = pd.to_datetime(store["date"])
    dates = sorted(store["date"].dt.normalize().unique())
    start = pd.Timestamp(from_date)
    end = pd.Timestamp(to_date) if to_date else dates[-1]
    asofs = [d for d in dates if start <= d <= end]
    if not asofs:
        raise SystemExit(f"[checklist] no trading days between {from_date} and {end.date()}")
    finviz, _ = _load_export(str(pd.Timestamp(asofs[-1]).date()))
    corr = _load_correlations()
    all_tickers = sorted(store["ticker"].unique())
    names = ([t.upper() for t in tickers if t.upper() in set(all_tickers)] if tickers else all_tickers)
    need = set(names)
    for t in names:
        need.update(corr.get(t, [])[:10])
    need &= set(all_tickers)
    sub = store[store["ticker"].isin(need)]
    print(f"[checklist] BACKFILL {pd.Timestamp(asofs[0]).date()} → {pd.Timestamp(asofs[-1]).date()} ({len(asofs)} sessions) × {len(names):,} names")
    close_panel = sub.pivot_table(index="date", columns="ticker", values="close", aggfunc="last").sort_index()
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    frames = []
    done = set()
    if HISTORY_PARQUET.exists():
        try:
            prev = pd.read_parquet(HISTORY_PARQUET, columns=["asof_date"])
            done = set(prev["asof_date"].astype(str).unique())
            print(f"[checklist] history exists — skip {len(done)} scored days")
        except Exception as e:
            print(f"[checklist] history read failed ({e})")
    for i, d in enumerate(asofs, 1):
        asof = pd.Timestamp(d).date().isoformat()
        if asof in done:
            print(f"[checklist] skip {asof} ({i}/{len(asofs)})")
            continue
        print(f"[checklist] score {asof} ({i}/{len(asofs)}) …")
        day = _score_universe(asof, names, finviz, store, close_panel, corr)
        if day.empty:
            continue
        _write_day_outputs(asof, day)
        frames.append(day)
        if len(frames) >= 5:
            _append_history(frames)
            frames = []
    if frames:
        _append_history(frames)
    if not HISTORY_PARQUET.exists():
        raise SystemExit("[checklist] backfill produced no rows")
    hist = pd.read_parquet(HISTORY_PARQUET)
    hist.to_csv(HISTORY_CSV, index=False)
    print(f"[checklist] HISTORY: {hist['asof_date'].nunique()} days × {hist['Ticker'].nunique():,} → {HISTORY_PARQUET.name}")
    return HISTORY_PARQUET


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", default=None)
    ap.add_argument("--tickers", default=None)
    ap.add_argument("--backfill-from", default=None)
    ap.add_argument("--backfill-to", default=None)
    args = ap.parse_args()
    tickers = [t.strip() for t in args.tickers.split(",")] if args.tickers else None
    if args.backfill_from:
        backfill(from_date=args.backfill_from, to_date=args.backfill_to, tickers=tickers)
    else:
        run(date=args.date, tickers=tickers)


if __name__ == "__main__":
    main()
