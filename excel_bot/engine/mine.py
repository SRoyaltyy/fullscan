"""Discovery mining per strategy_spec.md.

Universe : DISCOVERY tickers only (engine/holdout_split.json). Holdout untouched.
Matrix   : 33 cluster definitions x 13 exit rules x 2 sides x ~20 finviz cohorts.
Costs    : 0.10% round trip mcap>=300M, 0.30% below. NET returns only.
Output   : engine/mine_results.json + ranked stdout table (n>=100).

  python engine/mine.py [--min-n 100] [--out engine/mine_results.json]
"""
import argparse
import glob
import json
import math
import os
import sys
from collections import defaultdict

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from sweep import days_features, detect_def, definition_matrix
from cohort_analysis import load_finviz, cohorts_of
from exit_rules import TP_LEVELS, TRAIL_LEVELS, HOLD_NS

COST_HI, COST_LO = 0.001, 0.003   # round-trip, by mcap bucket ($300M)


def simulate_side(days, c):
    """All exit-rule outcomes, both sides. Returns signed raw returns
    (positive = trade made money) or None if unevaluable."""
    ei, xi = c.get("entry_idx"), c.get("exit_idx")
    side = c["side"]
    if ei is None or xi is None or xi >= len(days):
        return None
    entry = days[ei]["close"]
    if not entry:
        return None
    closes = [d["close"] for d in days]
    highs = [d["high"] for d in days]
    lows = [d["low"] for d in days]
    raw = lambda px: (px - entry) / entry * side   # signed P&L at price px
    out = {}

    out["flip"] = raw(closes[xi]) if closes[xi] else None

    for n in HOLD_NS:
        k = min(ei + n, xi)
        out[f"hold{n}"] = raw(closes[k]) if closes[k] else None

    for x in TP_LEVELS:
        tgt = entry * (1 + x * side)          # long: above; short: below
        fill = None
        for k in range(ei + 1, xi + 1):
            px = highs[k] if side == 1 else lows[k]
            if px is None:
                continue
            if (side == 1 and px >= tgt) or (side == -1 and px <= tgt):
                fill = x
                break
        out[f"tp{int(x*100)}"] = fill if fill is not None else out["flip"]

    for y in TRAIL_LEVELS:
        best = entry                           # peak (long) / trough (short)
        ret = out["flip"]
        for k in range(ei + 1, xi + 1):
            if closes[k] is None:
                continue
            best = max(best, closes[k]) if side == 1 else min(best, closes[k])
            stop = best * (1 - y) if side == 1 else best * (1 + y)
            if (side == 1 and closes[k] <= stop) or \
               (side == -1 and closes[k] >= stop):
                ret = raw(closes[k])
                break
        out[f"trail{int(y*100)}"] = ret

    mfe = None
    for k in range(ei + 1, xi + 1):
        px = highs[k] if side == 1 else lows[k]
        if px is not None:
            r = raw(px)
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


def work_grid(f):
    """Mine one grid file -> (ticker_or_None, {(def,side,rule,cohort): [nets]}).
    Discovery-set membership decided by caller via globals set in init."""
    t = os.path.basename(f)[:-5]
    if t not in _G["discovery"]:
        return None, {}
    try:
        feats = days_features(json.load(open(f))["days"])
    except Exception:
        return None, {}
    rec = _G["fz"].get(t)
    cohorts = ["ALL"] + (cohorts_of(rec) if rec else [])
    mc = rec["mcap"] if rec else None
    cost = COST_LO if (mc is not None and mc < 300) else COST_HI
    out = defaultdict(list)
    for definition in _G["defs"]:
        for c in detect_def(feats, definition):
            sim = simulate_side(feats, c)
            if sim is None:
                continue
            side = "long_green" if c["side"] == 1 else "short_red"
            for rule, ret in sim.items():
                if ret is None or rule == "MFE":
                    continue
                net = ret - cost
                for co in cohorts:
                    out[(definition["name"], side, rule, co)].append(net)
    return t, out


def _init_pool(discovery, fz, defs):
    _G["discovery"] = discovery
    _G["fz"] = fz
    _G["defs"] = defs


_G = {}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--min-n", type=int, default=100)
    ap.add_argument("--workers", type=int, default=6)
    ap.add_argument("--out", default="engine/mine_results.json")
    args = ap.parse_args()

    split = json.load(open("engine/holdout_split.json"))
    discovery = set(split["discovery"])
    fz = load_finviz()

    cells = defaultdict(list)       # (def, side, rule, cohort) -> [net returns]
    tickers_seen = 0
    files = [f for f in sorted(glob.glob("grids/*.json"))
             if not os.path.basename(f).startswith("_")]
    if args.workers > 1:
        from multiprocessing import Pool
        with Pool(args.workers, initializer=_init_pool,
                  initargs=(discovery, fz, definition_matrix())) as pool:
            for t, out in pool.imap_unordered(work_grid, files, chunksize=8):
                if t is None:
                    continue
                tickers_seen += 1
                for k, v in out.items():
                    cells[k].extend(v)
    else:
        _init_pool(discovery, fz, definition_matrix())
        for f in files:
            t, out = work_grid(f)
            if t is None:
                continue
            tickers_seen += 1
            for k, v in out.items():
                cells[k].extend(v)

    rows = []
    for (dn, side, rule, co), vals in cells.items():
        n = len(vals)
        if n < args.min_n:
            continue
        m = sum(vals) / n
        rows.append({
            "def": dn, "side": side, "exit": rule, "cohort": co,
            "n": n, "avg_net": m, "t": tstat(vals),
            "win": sum(1 for v in vals if v > 0) / n,
        })
    rows.sort(key=lambda r: -r["t"])

    json.dump({"created": "2026-07-27", "spec": "strategy_spec.md",
               "discovery_tickers_seen": tickers_seen,
               "min_n": args.min_n, "cells": rows},
              open(args.out, "w"), indent=1)

    print(f"discovery grids mined: {tickers_seen} | cells with n>={args.min_n}: "
          f"{len(rows)}")
    print(f"\n{'def':26s} {'side':11s} {'exit':7s} {'cohort':24s} "
          f"{'n':>6s} {'avg_net':>8s} {'t':>7s} {'win':>6s}")
    for r in rows[:40]:
        print(f"{r['def']:26s} {r['side']:11s} {r['exit']:7s} {r['cohort']:24s} "
              f"{r['n']:6d} {r['avg_net']*100:+7.2f}% {r['t']:+7.2f} "
              f"{r['win']:5.0%}")
    print(f"\nfull results: {args.out}")


if __name__ == "__main__":
    main()
