"""Holdout validation (rulebook §3): each candidate cell gets ONE trial
on the untouched 40% of tickers. PASS = same sign AND t >= 2 net of costs.

  python engine/holdout.py [--top 25] [--min-n 300]

Reads engine/mine_results.json (discovery mining), takes the top cells,
re-evaluates exactly those (def, side, exit, cohort) cells on holdout grids.
"""
import argparse
import glob
import json
import os
import sys
from collections import defaultdict

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from sweep import days_features, detect_def, definition_matrix
from cohort_analysis import load_finviz, cohorts_of
from mine import simulate_side, tstat, COST_HI, COST_LO

DEFS = {d["name"]: d for d in definition_matrix()}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--top", type=int, default=25)
    ap.add_argument("--min-n", type=int, default=300)
    ap.add_argument("--results", default="engine/mine_results.json")
    args = ap.parse_args()

    mine = json.load(open(args.results))
    split = json.load(open("engine/holdout_split.json"))
    holdout = set(split["holdout"])
    fz = load_finviz()

    # candidate cells: top by discovery t-stat
    cands = [c for c in mine["cells"] if c["n"] >= args.min_n]
    cands.sort(key=lambda c: -c["t"])
    cands = cands[:args.top]
    if not cands:
        print("no candidates; run mine.py first")
        return

    wanted = defaultdict(list)          # def name -> list of (side,exit,cohort)
    for c in cands:
        wanted[c["def"]].append((c["side"], c["exit"], c["cohort"]))

    hits = defaultdict(list)            # cell key -> [net returns] on holdout
    files = [f for f in sorted(glob.glob("grids/*.json"))
             if not os.path.basename(f).startswith("_")]
    seen = 0
    for f in files:
        t = os.path.basename(f)[:-5]
        if t not in holdout:
            continue
        try:
            feats = days_features(json.load(open(f))["days"])
        except Exception:
            continue
        seen += 1
        rec = fz.get(t)
        cohorts = set(["ALL"] + (cohorts_of(rec) if rec else []))
        mc = rec["mcap"] if rec else None
        cost = COST_LO if (mc is not None and mc < 300) else COST_HI
        for dn, want in wanted.items():
            definition = DEFS[dn]
            for c in detect_def(feats, definition):
                sim = simulate_side(feats, c)
                if sim is None:
                    continue
                side = "long_green" if c["side"] == 1 else "short_red"
                for wside, wexit, wco in want:
                    if side != wside or wco not in cohorts:
                        continue
                    ret = sim.get(wexit)
                    if ret is not None:
                        hits[(dn, wside, wexit, wco)].append(ret - cost)

    print(f"holdout grids evaluated: {seen}\n")
    print(f"{'def':26s} {'side':10s} {'exit':6s} {'cohort':24s} | "
          f"{'disc_n':>6s} {'disc_t':>6s} | {'hold_n':>6s} {'hold_avg':>8s} "
          f"{'hold_t':>7s} {'win':>5s} | verdict")
    passed = 0
    verdicts = []
    for c in cands:
        key = (c["def"], c["side"], c["exit"], c["cohort"])
        vals = hits.get(key, [])
        if len(vals) >= 30:
            m = sum(vals) / len(vals)
            t_ = tstat(vals)
            w = sum(1 for v in vals if v > 0) / len(vals)
            ok = m > 0 and t_ >= 2.0
            passed += 1 if ok else 0
            v = "PASS" if ok else "FAIL"
        else:
            m, t_, w, v = float("nan"), float("nan"), float("nan"), "THIN"
        verdicts.append({"cell": {k: c[k] for k in ("def", "side", "exit",
                        "cohort", "n", "avg_net", "t", "win")},
                        "holdout": {"n": len(vals), "avg_net": m, "t": t_,
                                    "win": w}, "verdict": v})
        print(f"{c['def']:26s} {c['side']:10s} {c['exit']:6s} {c['cohort']:24s} | "
              f"{c['n']:6d} {c['t']:+6.2f} | {len(vals):6d} "
              f"{m*100 if m==m else float('nan'):+7.2f}% {t_:+7.2f} "
              f"{w:4.0%} | {v}")
    json.dump(verdicts, open("engine/holdout_verdicts.json", "w"), indent=1)
    print(f"\n{passed}/{len(cands)} candidates PASSED holdout "
          f"(engine/holdout_verdicts.json)")


if __name__ == "__main__":
    main()
