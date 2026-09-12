"""Rebuild A–O color grids from the excel-state Yahoo rows cache (offline).

Seeds A–F (STOCKHISTORY aliases of IR:IW) via backtest.seed_anchor from
rows dated on or before each tile anchor. Never the xlsx cached spill.

Does not fetch. Does not write live flatten_robust. Resume-safe.

  python engine/rebuild_grids.py --workers 6
  python engine/rebuild_grids.py --limit 200 --seed 7
"""
from __future__ import annotations

import argparse
import glob
import json
import os
import random
import sys
from datetime import date

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from backtest import build_ticker, pick_anchors  # noqa: E402

ROWS_DIR = os.environ.get("ROWS_DIR", "data/rows")
GRIDS_DIR = os.environ.get("GRIDS_DIR", "grids")


def deser(rows):
    return [{**r, "date": date.fromisoformat(r["date"])} for r in rows]


def rebuild_one(ticker):
    src = os.path.join(ROWS_DIR, f"{ticker}.json")
    dst = os.path.join(GRIDS_DIR, f"{ticker}.json")
    try:
        rows = deser(json.load(open(src)))
        if len(rows) < 40:
            return ticker, 0, "short_history"
        anchors = pick_anchors(rows, rows[0]["date"], rows[-1]["date"])
        if not anchors:
            return ticker, 0, "no_anchors"
        days = build_ticker(ticker, rows, anchors)
        if not days:
            return ticker, 0, "no_days"
        os.makedirs(GRIDS_DIR, exist_ok=True)
        json.dump({"ticker": ticker, "days": days, "source": "rows_cache"},
                  open(dst, "w"))
        return ticker, len(days), None
    except Exception as e:  # noqa: BLE001
        return ticker, 0, f"{type(e).__name__}: {e}"[:160]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--workers", type=int, default=min(6, os.cpu_count() or 2))
    ap.add_argument("--limit", type=int, default=0)
    ap.add_argument("--seed", type=int, default=7)
    ap.add_argument("--resume", action="store_true", default=True)
    ap.add_argument("--no-resume", action="store_false", dest="resume")
    args = ap.parse_args()

    tickers = sorted(os.path.basename(p)[:-5]
                     for p in glob.glob(os.path.join(ROWS_DIR, "*.json"))
                     if not os.path.basename(p).startswith("_"))
    if args.limit and args.limit < len(tickers):
        rng = random.Random(args.seed)
        tickers = sorted(rng.sample(tickers, args.limit))
    if args.resume:
        tickers = [t for t in tickers
                   if not os.path.exists(os.path.join(GRIDS_DIR, f"{t}.json"))]
    print(f"rebuild {len(tickers)} tickers workers={args.workers}", flush=True)
    ok = err = 0
    if args.workers > 1 and len(tickers) > 1:
        from multiprocessing import Pool
        with Pool(args.workers) as pool:
            for i, (t, n, e) in enumerate(
                    pool.imap_unordered(rebuild_one, tickers, chunksize=4), 1):
                if e:
                    err += 1
                    if err <= 8:
                        print(f"  FAIL {t}: {e}", flush=True)
                else:
                    ok += 1
                if i % 100 == 0:
                    print(f"  ... {i}/{len(tickers)} ok={ok} err={err}",
                          flush=True)
    else:
        for t in tickers:
            _, n, e = rebuild_one(t)
            if e:
                err += 1
                print(f"  FAIL {t}: {e}", flush=True)
            else:
                ok += 1
    print(f"DONE ok={ok} err={err} grids={len(glob.glob(os.path.join(GRIDS_DIR, '*.json')))}")


if __name__ == "__main__":
    main()
