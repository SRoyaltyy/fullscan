"""Exit-rule shootout for confirmed GREEN clusters (long side).

Entry is identical for every rule: close of the confirmation day (first day
the cluster is knowable in real time). Only the EXIT varies:

  flip        hold to the day the cluster flips (baseline)
  holdN       sell at close N trading days after entry, or at flip, sooner of the two
  tpX%        limit sell at entry*(1+X); fills on first day whose HIGH >= target;
              if never hit, exit at flip close
  trailY%     track highest close since entry; exit at first close that is
              Y% below that peak; else flip close
  MFE         (hindsight reference, NOT tradable) max high after entry vs entry

Usage:
  python engine/exit_rules.py                 # cohort table
  python engine/exit_rules.py BBAI            # per-cluster ledger for one ticker
"""
import csv
import glob
import json
import math
import os
import sys
from collections import defaultdict
from datetime import datetime, timedelta

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from sweep import days_features, detect_def

FINVIZ = "data/finviz_with_descriptions.csv"
DEFNAME = "tol2_core_score_ml3"
DEF = {"name": DEFNAME, "kind": "tolerant", "key": "core_score",
       "thresh": 0.5, "tol": 2, "min_len": 3}

TP_LEVELS = [0.03, 0.05, 0.08, 0.10, 0.15]
TRAIL_LEVELS = [0.03, 0.05, 0.08]
HOLD_NS = [1, 2, 3, 5, 8]


def s2d(n):
    return (datetime(1899, 12, 30) + timedelta(days=float(n))).date()


def load_finviz():
    out = {}
    with open(FINVIZ, encoding="utf-8", errors="replace") as fh:
        for rec in csv.DictReader(fh):
            t = rec.get("Ticker", "").strip()
            if not t:
                continue
            def num(k):
                v = (rec.get(k) or "").replace("%", "").replace(",", "").strip()
                try:
                    return float(v)
                except ValueError:
                    return None
            out[t] = {"mcap": num("Market Cap"), "beta": num("Beta"),
                      "pm": num("Profit Margin")}
    return out


def simulate(days, c):
    """All exit-rule outcomes for one green cluster. None if unevaluable."""
    ei, xi = c.get("entry_idx"), c.get("exit_idx")
    if ei is None or xi is None or xi >= len(days):
        return None
    entry = days[ei]["close"]
    if not entry:
        return None
    closes = [d["close"] for d in days]
    highs = [d["high"] for d in days]
    out = {}

    out["flip"] = (closes[xi] - entry) / entry

    for n in HOLD_NS:
        k = min(ei + n, xi)
        out[f"hold{n}"] = (closes[k] - entry) / entry if closes[k] else None

    for x in TP_LEVELS:
        tgt = entry * (1 + x)
        fill = None
        for k in range(ei + 1, xi + 1):
            if highs[k] and highs[k] >= tgt:
                fill = x  # limit fills at target price
                break
        out[f"tp{int(x*100)}"] = fill if fill is not None else out["flip"]
        out[f"tp{int(x*100)}_hit"] = 1 if fill is not None else 0

    for y in TRAIL_LEVELS:
        peak = entry
        ret = out["flip"]
        for k in range(ei + 1, xi + 1):
            if closes[k]:
                peak = max(peak, closes[k])
                if closes[k] <= peak * (1 - y):
                    ret = (closes[k] - entry) / entry
                    break
        out[f"trail{int(y*100)}"] = ret

    mfe = None
    for k in range(ei + 1, xi + 1):
        if highs[k]:
            r = (highs[k] - entry) / entry
            mfe = r if mfe is None else max(mfe, r)
    out["MFE"] = mfe
    return out


def tstat(vals):
    n = len(vals)
    if n < 2:
        return float("nan")
    m = sum(vals) / n
    v = sum((x - m) ** 2 for x in vals) / (n - 1)
    return m / math.sqrt(v / n) if v > 0 else float("nan")


def ledger(ticker):
    g = json.load(open(f"grids/{ticker.upper()}.json"))
    days = days_features(g["days"])
    closes = [d["close"] for d in days]
    rules = ["flip", "hold3", "tp5", "tp8", "tp10", "trail5", "MFE"]
    print(f"=== {ticker.upper()} green clusters | entry = confirmation close "
          f"| def {DEFNAME} ===")
    print(f"{'confirm':>16s} {'flip':>16s} " +
          " ".join(f"{r:>7s}" for r in rules))
    for c in detect_def(days, DEF):
        if c["side"] != 1:
            continue
        sim = simulate(days, c)
        if sim is None:
            continue
        ei, xi = c["entry_idx"], c["exit_idx"]
        row = " ".join(
            f"{sim[r]*100:+6.1f}%" if sim.get(r) is not None else "    n/a"
            for r in rules)
        print(f"{s2d(days[ei]['date'])} {closes[ei]:5.2f} "
              f"{s2d(days[xi]['date'])} {closes[xi]:5.2f} {row}")


def cohort_table():
    fz = load_finviz()
    buckets = defaultdict(lambda: defaultdict(list))
    files = sorted(glob.glob("grids/*.json"))
    n_trades = 0
    for f in files:
        t = os.path.basename(f)[:-5]
        try:
            g = json.load(open(f))
            days = days_features(g["days"])
        except Exception:
            continue
        rec = fz.get(t)
        cohorts = ["ALL"]
        if rec:
            mc, b, pm = rec["mcap"], rec["beta"], rec["pm"]
            if mc is not None and 1000 <= mc < 10000:
                cohorts.append("mid(1-10B)")
                if b is not None and b > 1.5:
                    cohorts.append("mid:beta>1.5")
                if b is not None and b > 1.5 and pm is not None and pm < 0:
                    cohorts.append("mid:BBAI-like")
        for c in detect_def(days, DEF):
            if c["side"] != 1:
                continue
            sim = simulate(days, c)
            if sim is None:
                continue
            n_trades += 1
            for co in cohorts:
                for k, v in sim.items():
                    if not k.endswith("_hit") and v is not None:
                        buckets[co][k].append(v)

    print(f"green-cluster exit-rule shootout | def {DEFNAME} | "
          f"{len(files)} grids | {n_trades} trades")
    rules = (["flip", "MFE"] + [f"hold{n}" for n in HOLD_NS] +
             [f"tp{int(x*100)}" for x in TP_LEVELS] +
             [f"trail{int(y*100)}" for y in TRAIL_LEVELS])
    for co in ["ALL", "mid(1-10B)", "mid:beta>1.5", "mid:BBAI-like"]:
        if co not in buckets:
            continue
        print(f"\n--- {co} ---")
        print(f"{'rule':8s} {'n':>6s} {'avg':>8s} {'t':>7s} {'hit%':>6s}")
        for r in rules:
            vals = buckets[co].get(r, [])
            if not vals:
                continue
            m = sum(vals) / len(vals)
            hit = ""
            if r.startswith("tp"):
                hits = buckets[co].get(r + "_hit", [])
                hit = f"{sum(hits)/len(hits):5.0%}" if hits else ""
            print(f"{r:8s} {len(vals):6d} {m*100:+7.2f}% {tstat(vals):+7.2f} {hit:>6s}")


if __name__ == "__main__":
    if len(sys.argv) > 1:
        ledger(sys.argv[1])
    else:
        cohort_table()
