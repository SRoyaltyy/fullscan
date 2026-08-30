"""Causality test (rulebook §5): corrupt-the-future, assert-identical-decisions.

For each sampled grid:
  1. Compute all cluster decisions + exit-rule outcomes on full data.
  2. Pick random cutoff k. Corrupt EVERYTHING after k (fills AND OHLCV).
  3. Recompute. Every decision whose last relevant date <= k must be
     BIT-IDENTICAL to baseline. Any change = look-forward leak = FAIL.

Negative control: corrupting data BEFORE k must change something
(proves the test is actually sensitive).

Exit code 0 = pass, 1 = leak detected.
"""
import copy
import glob
import json
import os
import random
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from sweep import days_features, detect_def, definition_matrix
from exit_rules import simulate

DEFS = [d for d in definition_matrix()][:6]


def decision_set(days, definition):
    feats = days_features(days)
    out = {}
    for c in detect_def(feats, definition):
        key = (c["start"], c["side"], c["end"])
        out[key] = (c.get("entry_idx"), c.get("exit_idx"),
                    simulate(feats, c))
    return out


def corrupt_after(days, k, rng):
    bad = copy.deepcopy(days)
    for d in bad[k + 1:]:
        d["fills"] = [rng.choice([None, "FF0000", "00FF00", "808080",
                                  "FFFF00", "000000"]) for _ in d["fills"]]
        for fld in ("open", "close", "high", "low", "volume"):
            if d[fld] is not None:
                d[fld] = d[fld] * rng.uniform(0.2, 5.0)
    return bad


def last_decision_idx(key, val, n_days):
    xi = val[0][1] if isinstance(val, tuple) else None
    return None  # unused


def test_grid(path, rng, n_cutoffs=3, verbose=False):
    days = json.load(open(path))["days"]
    if len(days) < 30:
        return 0, 0
    checked = failed = 0
    for definition in DEFS:
        base = decision_set(days, definition)
        for _ in range(n_cutoffs):
            k = rng.randrange(10, len(days) - 5)
            pert = decision_set(corrupt_after(days, k, rng), definition)
            for key, val in base.items():
                ei, xi, sim = val
                last = xi if xi is not None else key[2] - 1
                if last > k:
                    continue  # legitimately depends on post-cutoff data
                checked += 1
                if key not in pert or pert[key] != val:
                    failed += 1
                    print(f"LEAK: {os.path.basename(path)} def={definition['name']} "
                          f"cutoff={k} cluster={key}")
                    if verbose and key in pert:
                        print(f"  base={val}\n  pert={pert[key]}")
    return checked, failed


def negative_control(rng):
    """Corrupting BEFORE cutoff must flip at least one decision."""
    path = "grids/BBAI.json"
    days = json.load(open(path))["days"]
    definition = DEFS[0]
    base = decision_set(days, definition)
    bad = copy.deepcopy(days)
    for d in bad[5:15]:
        d["fills"] = ["FF0000" if f != "FF0000" else "00FF00"
                      for f in d["fills"]]
    pert = decision_set(bad, definition)
    changed = sum(1 for k in base if k not in pert or pert[k] != base[k])
    print(f"negative control: {changed}/{len(base)} decisions changed by "
          f"past corruption (must be > 0)")
    return changed > 0


def main():
    rng = random.Random(20260727)
    if not negative_control(rng):
        print("FAIL: negative control insensitive — test is broken")
        sys.exit(1)
    files = sorted(glob.glob("grids/*.json"))
    files = [f for f in files if not os.path.basename(f).startswith("_")]
    sample = rng.sample(files, min(30, len(files)))
    total_c = total_f = 0
    for f in sample:
        c, f_ = test_grid(f, rng)
        total_c += c
        total_f += f_
    print(f"\ncausality test: {total_c} pre-cutoff decisions checked across "
          f"{len(sample)} grids x {len(DEFS)} definitions")
    if total_f:
        print(f"FAIL: {total_f} decisions changed by future corruption")
        sys.exit(1)
    print("PASS: no look-forward leaks in the mining code path")


if __name__ == "__main__":
    main()
