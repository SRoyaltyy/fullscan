"""Timing test (rulebook §5): is day-t color knowable at start of day t?

Method: run the engine on real BBAI data. Perturb ONE trading day's OHLCV
by a large factor. Recompute. Compare per-day fills between the two runs.

  - fills of the PERTURBED day change  -> day-t colors use day-t OHLC
    -> colors knowable only at day-t CLOSE -> entry at close/next open.
  - fills of the perturbed day UNCHANGED but NEXT day changes
    -> day-t colors use only data through day t-1
    -> knowable at start of day t -> entry at day-t OPEN is leak-free.
"""
import copy
import json
import sys
import os
from datetime import datetime, timedelta

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from backtest import run_anchor


def rows_from_grid(ticker):
    g = json.load(open(f"grids/{ticker}.json"))
    rows = []
    for d in g["days"]:
        if any(d[k] is None for k in ("open", "close", "high", "low", "volume")):
            continue
        rows.append({
            "date": (datetime(1899, 12, 30) + timedelta(days=d["date"])).date(),
            "open": d["open"], "close": d["close"], "high": d["high"],
            "low": d["low"], "volume": d["volume"],
        })
    return rows


def main(ticker="BBAI"):
    rows = rows_from_grid(ticker)
    anchor = max(r["date"] for r in rows)
    base = {d["date"]: d["fills"] for d in run_anchor(ticker, rows, anchor)}

    # test 5 sample days spread across the window (skip first/last 10)
    idxs = [10, len(rows) // 4, len(rows) // 2,
            3 * len(rows) // 4, len(rows) - 11]
    print(f"ticker={ticker} anchor={anchor} days={len(rows)}")
    same_changed_total = 0
    for i in idxs:
        rows2 = copy.deepcopy(rows)
        r = rows2[i]
        r["open"] *= 1.7
        r["close"] *= 0.55
        r["high"] *= 1.9
        r["low"] *= 0.4
        r["volume"] *= 4.0
        pert = {d["date"]: d["fills"] for d in run_anchor(ticker, rows2, anchor)}
        d = int((rows[i]["date"] - datetime(1899, 12, 30).date()).days)
        same = base.get(d) != pert.get(d)
        # neighbors: did ANY other day's fills change?
        others = [k for k in base if k != d and base[k] != pert.get(k)]
        same_changed_total += 1 if same else 0
        print(f"perturb {rows[i]['date']}: same-day fills changed={same} "
              f"| other days changed: {others[:6]}"
              f"{'...' if len(others) > 6 else ''}")
    print()
    if same_changed_total == 0:
        print("VERDICT: day-t colors do NOT use day-t OHLC -> knowable at "
              "start of day t -> ENTRY AT OPEN is leak-free.")
    elif same_changed_total == len(idxs):
        print("VERDICT: day-t colors DO use day-t OHLC -> knowable only at "
              "close -> entry must be confirmation CLOSE (or next open).")
    else:
        print(f"VERDICT: mixed ({same_changed_total}/{len(idxs)} same-day "
              "changed) -> inspect before trusting open entry.")


if __name__ == "__main__":
    main(sys.argv[1] if len(sys.argv) > 1 else "BBAI")
