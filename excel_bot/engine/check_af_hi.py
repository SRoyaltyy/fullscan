"""Prove Yahoo A–F seed → Excel H/I match the emulator formulas.

  python engine/check_af_hi.py --ticker AAPL
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import date, datetime, timedelta
from pathlib import Path

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from backtest import run_anchor, pick_anchors  # noqa: E402
from evaluator import Evaluator  # noqa: E402
from stockhistory import serial  # noqa: E402
from backtest import seed_anchor  # noqa: E402

ROWS = Path(__file__).resolve().parent.parent / "data" / "rows"


def load_rows(ticker):
    raw = json.loads((ROWS / f"{ticker}.json").read_text())
    out = []
    for r in raw:
        d = r["date"]
        if isinstance(d, str):
            d = date.fromisoformat(d[:10])
        out.append({**r, "date": d})
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--ticker", default="AAPL")
    args = ap.parse_args()
    rows = load_rows(args.ticker)
    anchors = pick_anchors(rows, rows[0]["date"], rows[-1]["date"])
    anchor = anchors[-1]
    from backtest import MODEL
    ev = Evaluator(MODEL, today=serial(anchor))
    ev.seed(seed_anchor(ev, args.ticker, rows, anchor))
    n = ok = 0
    misses = []
    for r in range(3, 146):
        a = ev.get_cell(f"A{r}")
        if not isinstance(a, (int, float)) or a < 30000:
            continue
        b, c = ev.get_cell(f"B{r}"), ev.get_cell(f"C{r}")
        bp = ev.get_cell(f"B{r-1}")
        h, i = ev.get_cell(f"H{r}"), ev.get_cell(f"I{r}")
        if not all(isinstance(x, (int, float)) for x in (b, c, bp, h, i)):
            continue
        if c == 0 or bp == 0:
            continue
        h_hat = (b - c) / c
        i_hat = (b - bp) / bp
        n += 1
        if abs(h - h_hat) < 1e-12 and abs(i - i_hat) < 1e-12:
            ok += 1
        else:
            misses.append((r, h, h_hat, i, i_hat))
    print(f"ticker={args.ticker} anchor={anchor}  H/I match {ok}/{n}")
    if misses[:5]:
        print("misses", misses[:5])
    if ok != n or n < 20:
        raise SystemExit(2)
    print("PASS: Yahoo A–F seed reproduces Excel H and I exactly")


if __name__ == "__main__":
    main()
