"""Auditable trade ledger: every cluster with exact dates and prices.

  python engine/trade_ledger.py BBAI [--def tol2_core_score_ml3]

For each cluster prints:
  first day (hindsight start), confirmation day (real entry, close price),
  last cluster day (hindsight exit), flip day (real exit, close price),
  and the three returns: pre-move / hindsight / real.
"""
import argparse
import json
import os
import sys
from datetime import datetime, timedelta

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from sweep import days_features, detect_def, definition_matrix


def s2d(n):
    return (datetime(1899, 12, 30) + timedelta(days=float(n))).date()


def pct(a):
    return f"{a:+.1%}" if a is not None else "   n/a"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("ticker")
    ap.add_argument("--def", dest="dname", default="tol2_core_score_ml3")
    ap.add_argument("--all-defs", action="store_true")
    args = ap.parse_args()

    g = json.load(open(f"grids/{args.ticker.upper()}.json"))
    days = days_features(g["days"])
    closes = [d["close"] for d in days]

    defs = definition_matrix() if args.all_defs else [
        d for d in definition_matrix() if d["name"] == args.dname]
    for definition in defs:
        clusters = detect_def(days, definition)
        print(f"\n=== {args.ticker.upper()} | {definition['name']} "
              f"({len(clusters)} clusters) ===")
        print(f"{'side':5s} {'start(open)':>16s} {'confirm(close)':>16s} "
              f"{'end(close)':>16s} {'flip(close)':>16s} "
              f"{'pre-move':>9s} {'hindsight':>10s} {'REAL':>8s}")
        for c in clusters:
            i, j, side = c["start"], c["end"] - 1, c["side"]
            ei, xi = c.get("entry_idx"), c.get("exit_idx")
            o = days[i]["open"]
            pre = hind = real = None
            if o and closes[j]:
                hind = (closes[j] - o) / o * side
            if ei is not None and closes[ei]:
                pre = (closes[ei] - o) / o * side if o else None
                if xi is not None and xi < len(closes) and closes[xi]:
                    real = (closes[xi] - closes[ei]) / closes[ei] * side
            row = [
                "GREEN" if side == 1 else "RED",
                f"{s2d(days[i]['date'])} {o:5.2f}" if o else f"{s2d(days[i]['date'])}   n/a",
                f"{s2d(days[ei]['date'])} {closes[ei]:5.2f}" if ei is not None else "       n/a",
                f"{s2d(days[j]['date'])} {closes[j]:5.2f}",
                f"{s2d(days[xi]['date'])} {closes[xi]:5.2f}" if xi is not None else "   open  ",
                pct(pre), pct(hind), pct(real),
            ]
            print(f"{row[0]:5s} {row[1]:>16s} {row[2]:>16s} {row[3]:>16s} "
                  f"{row[4]:>16s} {row[5]:>9s} {row[6]:>10s} {row[7]:>8s}")


if __name__ == "__main__":
    main()
