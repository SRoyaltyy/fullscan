"""Build standing-keep dumps (A–O fills + AH/FR/H/I) from the prices parquet.

Writes Yahoo/rows-shaped JSON so mine_hi_horizon.slim_ticker can load them.
Never Excel STOCKHISTORY. Live flatten_robust is not imported.

  python engine/capture_standing_slim.py --workers 4
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from concurrent.futures import ProcessPoolExecutor, as_completed
from datetime import date

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from backtest import pick_anchors, run_anchor  # noqa: E402
from clock import VISIBLE  # noqa: E402
from mine_hi_soft_regime import ah_from_h, fr_from_vol  # noqa: E402

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
REPO = os.path.dirname(ROOT)
ROWS_DIR = os.environ.get("ROWS_DIR", os.path.join(ROOT, "data", "rows"))
OUT_DIR = os.environ.get(
    "ALL_COLS_DIR", os.path.join(ROOT, "research", "all_cols_sample")
)
META_3603 = os.path.join(ROOT, "research", "all_cols_sample", "_meta.json")
PARQUET = os.path.join(REPO, "data", "prices", "ohlc.parquet")
LIVE_UNTOUCHED = "flatten_robust"


def locked_tickers():
    meta = json.load(open(META_3603, encoding="utf-8"))
    return [t for t in (meta.get("tickers") or []) if t]


def write_rows_from_parquet(tickers):
    """Materialize excel_bot/data/rows/<T>.json from data/prices/ohlc.parquet."""
    import pyarrow as pa
    import pyarrow.compute as pc
    import pyarrow.parquet as pq

    os.makedirs(ROWS_DIR, exist_ok=True)
    want = [t for t in tickers]
    table = pq.read_table(PARQUET)
    table = table.filter(pc.is_in(table["ticker"], value_set=pa.array(want)))
    by = {t: [] for t in want}
    tick_col = table["ticker"].to_pylist()
    date_col = table["date"].to_pylist()
    o = table["open"].to_pylist()
    h = table["high"].to_pylist()
    low = table["low"].to_pylist()
    c = table["close"].to_pylist()
    v = table["volume"].to_pylist()
    for i, t in enumerate(tick_col):
        bucket = by.get(t)
        if bucket is None:
            continue
        d = date_col[i]
        if hasattr(d, "date"):
            d = d.date()
        bucket.append({
            "date": d.isoformat() if hasattr(d, "isoformat") else str(d)[:10],
            "open": o[i], "high": h[i], "low": low[i],
            "close": c[i], "volume": v[i],
        })
    n_ok = 0
    for t, rows in by.items():
        rows.sort(key=lambda r: r["date"])
        if len(rows) < 40:
            continue
        json.dump(rows, open(os.path.join(ROWS_DIR, f"{t}.json"), "w"))
        n_ok += 1
    return n_ok


def deser(rows):
    return [{**r, "date": date.fromisoformat(r["date"])} for r in rows]


def capture_one(ticker):
    src = os.path.join(ROWS_DIR, f"{ticker}.json")
    dst = os.path.join(OUT_DIR, f"{ticker}.json")
    if not os.path.exists(src):
        return ticker, 0, "no_rows"
    try:
        rows = deser(json.load(open(src)))
        if len(rows) < 40:
            return ticker, 0, "short_history"
        anchors = pick_anchors(rows, rows[0]["date"], rows[-1]["date"])
        if not anchors:
            return ticker, 0, "no_anchors"
        days_raw = run_anchor(ticker, rows, anchors[-1])
        if not days_raw:
            return ticker, 0, "no_days"
        Hs, Is, vols = [], [], []
        prev_c = None
        slim_days = []
        for d in days_raw:
            o, c = d.get("open"), d.get("close")
            hv = ((c - o) / o) if o and c else None
            iv = ((c - prev_c) / prev_c) if c and prev_c else None
            Hs.append(hv)
            Is.append(iv)
            vols.append(d.get("volume"))
            prev_c = c
        for ei, d in enumerate(days_raw):
            fills = d.get("fills") or []
            cells = {}
            for idx, let in enumerate(VISIBLE):
                cells[let] = {"f": fills[idx] if idx < len(fills) else None}
            cells["H"] = {"v": Hs[ei]}
            cells["I"] = {"v": Is[ei]}
            cells["AH"] = {"v": ah_from_h(Hs, ei)}
            cells["FR"] = {"v": fr_from_vol(vols, ei)}
            slim_days.append({
                "date": d["date"],
                "open": d.get("open"), "close": d.get("close"),
                "high": d.get("high"), "low": d.get("low"),
                "volume": d.get("volume"),
                "fills": fills,
                "cells": cells,
            })
        os.makedirs(OUT_DIR, exist_ok=True)
        json.dump({
            "ticker": ticker,
            "source": "rows_cache",
            "seed": "yahoo_rows_cache",
            "standing_slim": True,
            "live_untouched": LIVE_UNTOUCHED,
            "days": slim_days,
        }, open(dst, "w"), default=str)
        return ticker, len(slim_days), None
    except Exception as e:  # noqa: BLE001
        return ticker, 0, f"{type(e).__name__}: {e}"[:160]


def main():
    os.chdir(ROOT)
    ap = argparse.ArgumentParser()
    ap.add_argument("--workers", type=int, default=min(4, os.cpu_count() or 2))
    ap.add_argument("--limit", type=int, default=0)
    ap.add_argument("--skip-existing", action="store_true", default=True)
    ap.add_argument("--rows-only", action="store_true")
    args = ap.parse_args()
    tickers = locked_tickers()
    if args.limit:
        tickers = tickers[: args.limit]
    print(f"[standing-slim] rows from parquet → {ROWS_DIR} n={len(tickers)}",
          flush=True)
    n_rows = write_rows_from_parquet(tickers)
    print(f"[standing-slim] wrote {n_rows} row files", flush=True)
    if args.rows_only:
        return
    os.makedirs(OUT_DIR, exist_ok=True)
    todo, skip = [], 0
    for t in tickers:
        dst = os.path.join(OUT_DIR, f"{t}.json")
        if args.skip_existing and os.path.exists(dst):
            skip += 1
        elif os.path.exists(os.path.join(ROWS_DIR, f"{t}.json")):
            todo.append(t)
    print(f"[standing-slim] capture todo={len(todo)} skip={skip} "
          f"workers={args.workers} → {OUT_DIR}", flush=True)
    ok = err = 0
    if args.workers <= 1 or len(todo) <= 1:
        jobs = [capture_one(t) for t in todo]
    else:
        jobs = []
        with ProcessPoolExecutor(max_workers=args.workers) as pool:
            futs = {pool.submit(capture_one, t): t for t in todo}
            for fut in as_completed(futs):
                jobs.append(fut.result())
    for name, n, e in jobs:
        if e:
            err += 1
            if err <= 8:
                print(f"  FAIL {name}: {e}", flush=True)
        else:
            ok += 1
            if ok % 50 == 0:
                print(f"  ok {name} days={n} ({ok}/{len(todo)})", flush=True)
    print(f"DONE ok={ok} err={err} skip={skip}", flush=True)


if __name__ == "__main__":
    main()
