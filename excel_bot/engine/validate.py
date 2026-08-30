"""Validate the evaluator against Excel's cached values.

Usage:
  python validate.py                -> sample smoke test
  python validate.py --full         -> evaluate every formula cell, report mismatches
  python validate.py --full --save  -> also save mismatches to mismatches.json
"""
import json
import re
import sys

from openpyxl.utils import column_index_from_string, get_column_letter

from evaluator import Evaluator, norm_literal, split_coord
from xlrt import Err, is_err, is_arr


def build_seeds(model):
    """Cached values for the two STOCKHISTORY spill regions (treated as inputs)."""
    seeds = {}
    cached = model["cached"]
    for region in ("IR1:IW137", "AP1:AU87"):
        a, b = region.split(":")
        c1, r1 = split_coord(a)
        c2, r2 = split_coord(b)
        for rr in range(r1, r2 + 1):
            for cc in range(c1, c2 + 1):
                coord = f"{get_column_letter(cc)}{rr}"
                if coord in cached:
                    seeds[coord] = norm_literal(cached[coord])
    return seeds


def values_match(got, want, tol=1e-9):
    want = norm_literal(want)
    if isinstance(got, Err) and isinstance(want, Err):
        return got.code == want.code
    if isinstance(got, Err) or isinstance(want, Err):
        return False
    if got is None and want is None:
        return True
    if got is None or want is None:
        # Excel displays formula-derived empties as 0; cache may omit them
        return (got in (None, 0, 0.0, "")) and (want in (None, 0, 0.0, ""))
    if isinstance(got, bool) or isinstance(want, bool):
        return got is want or got == want
    if isinstance(got, (int, float)) and isinstance(want, (int, float)):
        d = abs(got - want)
        return d <= tol or d <= tol * max(abs(got), abs(want))
    if isinstance(got, str) and isinstance(want, str):
        return got == want
    return False


def label(v):
    if isinstance(v, Err):
        return v.code
    if is_arr(v):
        return f"array{len(v)}x{len(v[0]) if v else 0}"
    return repr(v)


def main():
    full = "--full" in sys.argv
    save = "--save" in sys.argv
    model = json.load(open("engine/model.json"))
    seeds = build_seeds(model)
    today = model["cached"].get("P1")  # cached TODAY()
    ev = Evaluator("engine/model.json", today=today)
    ev.seed(seeds)

    if not full:
        samples = ["A2", "H2", "I2", "G3", "S1", "T1", "U1", "V1", "W1", "X1",
                   "Y1", "Q1", "R1", "AA1", "DB2", "DD2", "DF2", "AQ101",
                   "AR101", "AS101", "FN2", "FO2", "HH2", "CP2", "CT2",
                   "B105", "J105", "K105", "L105"]
        ok = bad = 0
        for coord in samples:
            want = model["cached"].get(coord)
            got = ev.get_cell(coord)
            cmp_got = got[0][0] if is_arr(got) else got
            m = values_match(cmp_got, want)
            ok += m
            bad += (not m)
            print(f"{'OK ' if m else 'BAD'} {coord:8s} got={label(got)[:60]:60s} want={label(norm_literal(want))[:40]}")
        print(f"\n{ok} ok / {bad} bad")
        if ev.parse_errors:
            print("parse errors:", dict(list(ev.parse_errors.items())[:5]))
        return

    # full sweep
    results = {"ok": 0, "bad": 0, "no_cache": 0, "parse_err": 0}
    mismatches = []
    coords = sorted(model["formulas"].keys(),
                    key=lambda c: (int(re.search(r"\d+", c).group()),
                                   column_index_from_string(re.match(r"[A-Z]+", c).group())))
    for coord in coords:
        want = model["cached"].get(coord)
        got = ev.get_cell(coord)
        if isinstance(got, Err) and got.code.startswith("#PARSE"):
            results["parse_err"] += 1
            mismatches.append({"cell": coord, "got": got.code,
                               "formula": model["formulas"][coord]["f"][:300]})
            continue
        if want is None:
            results["no_cache"] += 1
            continue
        cmp_got = got[0][0] if is_arr(got) else got
        if values_match(cmp_got, want):
            results["ok"] += 1
        else:
            results["bad"] += 1
            mismatches.append({"cell": coord, "got": label(got)[:200],
                               "want": label(norm_literal(want))[:200],
                               "formula": model["formulas"][coord]["f"][:300]})
    print(results)
    if save:
        json.dump(mismatches, open("engine/mismatches.json", "w"), indent=1)
        print(f"saved {len(mismatches)} mismatches to engine/mismatches.json")
    else:
        for m in mismatches[:25]:
            print(m["cell"], "| got", m["got"][:50], "| want", m.get("want", "")[:30],
                  "|", m["formula"][:80])


if __name__ == "__main__":
    main()
