"""Lean A–JL (275 col) capture from the excel-state rows cache (no Yahoo).

This is the sample --all-cols rebuild. Daily rows 2–145, columns A–JL:
value + fill. `run.py --all-cols` dumps the same 275 cols but walks rows
1–364 and re-fetches Yahoo — minutes/ticker. This path is ~1.2 s/ticker.

Research only. Live flatten_robust is not changed.

  python engine/capture_all_cols.py --from-split --n-disc 100 --n-hold 60
  python engine/capture_all_cols.py --tickers AAPL MSFT --out research/all_cols_sample
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import time
from datetime import date

from openpyxl.utils import get_column_letter

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from backtest import pick_anchors, seed_anchor  # noqa: E402
from colors import ColorEngine  # noqa: E402
from evaluator import Evaluator  # noqa: E402
from stockhistory import serial  # noqa: E402
from xlrt import is_arr  # noqa: E402

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
ROWS_DIR = os.environ.get("ROWS_DIR", os.path.join(ROOT, "data", "rows"))
MODEL = os.path.join(HERE, "model.json")
SPLIT_PATH = os.path.join(HERE, "holdout_split.json")
ALL_COLS = 275
ROW_START, ROW_END = 2, 145


def deser(rows):
    return [{**r, "date": date.fromisoformat(r["date"])} for r in rows]


def stride_pick(xs, n):
    if n <= 0:
        return []
    if len(xs) <= n:
        return list(xs)
    step = len(xs) / n
    return [xs[int(i * step)] for i in range(n)]


def tickers_from_split(n_disc, n_hold):
    split = json.load(open(SPLIT_PATH))
    have = {fn[:-5] for fn in os.listdir(ROWS_DIR) if fn.endswith(".json")}
    disc = sorted(t for t in split["discovery"] if t in have)
    hold = sorted(t for t in split["holdout"] if t in have)
    return stride_pick(disc, n_disc), stride_pick(hold, n_hold)


def capture_ticker(ticker, outdir):
    src = os.path.join(ROWS_DIR, f"{ticker}.json")
    if not os.path.exists(src):
        return ticker, 0, 0.0, "no_rows"
    t0 = time.time()
    rows = deser(json.load(open(src)))
    if len(rows) < 40:
        return ticker, 0, time.time() - t0, "short_history"
    anchors = pick_anchors(rows, rows[0]["date"], rows[-1]["date"])
    if not anchors:
        return ticker, 0, time.time() - t0, "no_anchors"
    anchor = anchors[-1]
    ev = Evaluator(MODEL, today=serial(anchor))
    ev.seed(seed_anchor(ev, ticker, rows, anchor))
    ce = ColorEngine(ev, MODEL)
    days = []
    for r in range(ROW_START, ROW_END + 1):
        a = ev.get_cell(f"A{r}")
        if not isinstance(a, (int, float)) or a < 30000:
            continue
        cells = {}
        for c in range(1, ALL_COLS + 1):
            letter = get_column_letter(c)
            coord = f"{letter}{r}"
            v = ev.get_cell(coord)
            if is_arr(v):
                v = v[0][0]
            if hasattr(v, "code"):
                val, err = None, v.code
            else:
                val, err = v, None
            fill = ce.fill_for(coord)
            if val is None and not fill and not err:
                continue
            cells[letter] = {"v": val, "f": fill, "e": err}
        ohlc = {}
        for name, col in (("close", "B"), ("open", "C"), ("high", "D"),
                          ("low", "E"), ("volume", "F")):
            rec = cells.get(col) or {}
            ohlc[name] = rec.get("v") if isinstance(rec.get("v"), (int, float)) else None
        days.append({"date": int(a), **ohlc, "cells": cells})
    os.makedirs(outdir, exist_ok=True)
    json.dump({"ticker": ticker, "anchor": str(anchor), "all_cols": True,
               "n_cols": ALL_COLS, "days": days},
              open(os.path.join(outdir, f"{ticker}.json"), "w"), default=str)
    return ticker, len(days), time.time() - t0, None


def main():
    os.chdir(ROOT)
    ap = argparse.ArgumentParser()
    ap.add_argument("--tickers", nargs="*", default=[])
    ap.add_argument("--from-split", action="store_true")
    ap.add_argument("--n-disc", type=int, default=100)
    ap.add_argument("--n-hold", type=int, default=60)
    ap.add_argument("--out", default="research/all_cols_sample")
    ap.add_argument("--skip-existing", action="store_true", default=True)
    ap.add_argument("--force", action="store_true",
                    help="rebuild even if the ticker dump already exists")
    args = ap.parse_args()
    if args.force:
        args.skip_existing = False
    if args.from_split:
        disc, hold = tickers_from_split(args.n_disc, args.n_hold)
        tickers = disc + hold
        print(f"from-split disc={len(disc)} hold={len(hold)}", flush=True)
    else:
        tickers = args.tickers
        disc = hold = []
    outdir = args.out if os.path.isabs(args.out) else os.path.join(ROOT, args.out)
    os.makedirs(outdir, exist_ok=True)
    print(f"capture_all_cols n={len(tickers)} -> {outdir}", flush=True)
    times = []
    ok = err = skipped = 0
    for t in tickers:
        dest = os.path.join(outdir, f"{t}.json")
        if args.skip_existing and os.path.exists(dest):
            skipped += 1
            times.append(0.0)
            print(f"  skip {t}", flush=True)
            continue
        name, n, sec, e = capture_ticker(t, outdir)
        times.append(sec)
        if e:
            err += 1
            print(f"  FAIL {name} {sec:.1f}s {e}", flush=True)
        else:
            ok += 1
            print(f"  ok {name} days={n} {sec:.1f}s", flush=True)
    measured = [s for s in times if s > 0]
    meta = {
        "n_ok": ok, "n_err": err, "n_skipped": skipped,
        "n_target": len(tickers),
        "n_disc": len(disc) if disc else None,
        "n_hold": len(hold) if hold else None,
        "seconds": times,
        "seconds_measured": measured,
        "minutes_per_ticker": (sum(measured) / len(measured) / 60.0) if measured else None,
        "tickers": tickers,
        "discovery": disc,
        "holdout": hold,
        "path": "lean_rows_cache",
        "cols": "A..JL",
        "n_cols": ALL_COLS,
        "rows": f"{ROW_START}-{ROW_END}",
        "note": "run.py --all-cols is the full 1-364 Yahoo path; this is the cheap sample rebuild",
    }
    json.dump(meta, open(os.path.join(outdir, "_meta.json"), "w"), indent=1)
    print(f"DONE ok={ok} skip={skipped} err={err} min/ticker="
          f"{meta['minutes_per_ticker']}")


if __name__ == "__main__":
    main()
