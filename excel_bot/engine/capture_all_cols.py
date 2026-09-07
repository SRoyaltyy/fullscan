"""Lean --all-cols capture from the excel-state rows cache (no Yahoo).

One latest anchor per ticker. Daily rows 2–145, columns A–JL (275):
value + fill. Research sample only. Live flatten_robust is not changed.

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

ROWS_DIR = os.environ.get("ROWS_DIR", "data/rows")
MODEL = "engine/model.json"
ALL_COLS = 275
ROW_START, ROW_END = 2, 145


def deser(rows):
    return [{**r, "date": date.fromisoformat(r["date"])} for r in rows]


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
    ap = argparse.ArgumentParser()
    ap.add_argument("--tickers", nargs="*", default=[])
    ap.add_argument("--out", default="research/all_cols_sample")
    args = ap.parse_args()
    tickers = args.tickers
    print(f"capture_all_cols n={len(tickers)} -> {args.out}", flush=True)
    times = []
    ok = err = 0
    for t in tickers:
        name, n, sec, e = capture_ticker(t, args.out)
        times.append(sec)
        if e:
            err += 1
            print(f"  FAIL {name} {sec:.1f}s {e}", flush=True)
        else:
            ok += 1
            print(f"  ok {name} days={n} {sec:.1f}s", flush=True)
    meta = {
        "n_ok": ok, "n_err": err,
        "seconds": times,
        "minutes_per_ticker": (sum(times) / len(times) / 60.0) if times else None,
        "tickers": tickers,
    }
    json.dump(meta, open(os.path.join(args.out, "_meta.json"), "w"), indent=1)
    print(f"DONE ok={ok} err={err} min/ticker="
          f"{meta['minutes_per_ticker']}")


if __name__ == "__main__":
    main()
