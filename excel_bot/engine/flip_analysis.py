"""Flip analysis: are cluster ENDS tradable in real time?

When a cluster ends, the flip is observable at that day's close (colors
actually changed). Question: what happens AFTER the flip?
  - green cluster ends -> mean reversion down? (short signal)
  - red cluster ends   -> bounce up?           (long signal / loss avoidance)

For every cluster-end flip in every grid: forward returns at +1/+3/+5/+10
trading days from the flip-day close.
"""
import glob
import json
import math
import os
import sys
from collections import defaultdict

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from sweep import days_features, detect_def, definition_matrix


def tstat(rets):
    n = len(rets)
    if n < 5:
        return None
    avg = sum(rets) / n
    sd = math.sqrt(sum((r - avg) ** 2 for r in rets) / (n - 1)) if n > 1 else 0
    return avg, avg / (sd / math.sqrt(n)) if sd > 0 else 0, n


def main():
    files = sorted(f for f in glob.glob("grids/*.json")
                   if not os.path.basename(f).startswith("_"))
    tickers = {}
    for f in files:
        g = json.load(open(f))
        if len(g["days"]) >= 40:
            tickers[g["ticker"]] = days_features(g["days"])
    print(f"{len(tickers)} tickers")
    horizons = (1, 3, 5, 10)

    # use a few representative definitions, not the whole matrix
    defs = [d for d in definition_matrix()
            if d["name"] in ("tol2_core_score_ml3", "tol2_A_ml3",
                             "hyst_core_score_e3_x2", "strict_core_score_ml3",
                             "strict_A_ml2")]
    for definition in defs:
        flips = defaultdict(list)  # (side, horizon) -> [returns]
        for t, days in tickers.items():
            closes = [d["close"] for d in days]
            clusters = detect_def(days, definition)
            for c in clusters:
                xi = c.get("exit_idx")          # flip day (observable close)
                if xi is None or xi >= len(closes) - 1:
                    continue
                base = closes[xi]
                if not base:
                    continue
                for h in horizons:
                    if xi + h < len(closes) and closes[xi + h]:
                        r = (closes[xi + h] - base) / base
                        flips[(c["side"], h)].append(r)
        print(f"\n== {definition['name']} ==")
        print("  green-cluster end (expect DOWN if mean-reverting):")
        for h in horizons:
            r = flips[(1, h)]
            s = tstat(r)
            if s:
                print(f"    +{h:2d}d: n={s[2]:5d} avg={s[0]:+7.3%} t={s[1]:+6.2f}")
        print("  red-cluster end (expect UP if bouncing):")
        for h in horizons:
            r = flips[(-1, h)]
            s = tstat(r)
            if s:
                print(f"    +{h:2d}d: n={s[2]:5d} avg={s[0]:+7.3%} t={s[1]:+6.2f}")


if __name__ == "__main__":
    main()
