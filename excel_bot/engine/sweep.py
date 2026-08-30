"""Definition sweep: throw every cluster definition at the wall, score what sticks.

Loads grids/*.json (per-day OHLCV + 15 fills), applies a matrix of cluster
definitions, computes trades per cluster (open of first day -> close of last,
plus a confirmation-lagged variant), and ranks definitions.

Output: outputs/sweep_leaderboard.csv + outputs/sweep_leaderboard.html
"""
import glob
import json
import math
import os
import sys
from collections import defaultdict

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from signals import classify_fill, _runs, _tolerant_runs, _hysteresis


# ----------------------------------------------------------- day features --
def days_features(days):
    """Annotate each day with color-derived features."""
    out = []
    for d in days:
        fams, scores = [], []
        for f in d["fills"]:
            fam, sc = classify_fill(f)
            fams.append(fam)
            scores.append(sc)
        out.append({**d, "fams": fams, "scores": scores,
                    "a_score": scores[0],
                    "row_score": sum(scores),
                    "core_score": sum(scores[:10])})  # A..J only
    return out


# ------------------------------------------------------------- detectors ---
def regimes_from(days, key, thresh):
    if key == "a":
        return [1 if d["fams"][0] == "green" else (-1 if d["fams"][0] == "red" else 0)
                for d in days]
    return [1 if d[key] >= thresh else (-1 if d[key] <= -thresh else 0)
            for d in days]


def detect_def(days, definition):
    kind = definition["kind"]
    key = definition.get("key", "a")
    thresh = definition.get("thresh", 0)
    min_len = definition.get("min_len", 2)
    if kind == "hyst":
        return _hysteresis([d[key] for d in days],
                           definition["enter"], definition["exit"], min_len)
    regimes = regimes_from(days, key, thresh)
    if kind == "strict":
        return _runs(regimes, min_len)
    if kind == "tolerant":
        return _tolerant_runs(regimes, definition["tol"], min_len)
    raise ValueError(kind)


def definition_matrix():
    defs = []
    for ml in (1, 2, 3):
        defs.append({"name": f"strict_A_ml{ml}", "kind": "strict",
                     "key": "a", "min_len": ml})
    for tol in (2, 3):
        for ml in (2, 3, 4):
            defs.append({"name": f"tol{tol}_A_ml{ml}", "kind": "tolerant",
                         "key": "a", "tol": tol, "min_len": ml})
    for key in ("row_score", "core_score"):
        for ml in (2, 3):
            defs.append({"name": f"strict_{key}_ml{ml}", "kind": "strict",
                         "key": key, "thresh": 0.5, "min_len": ml})
            for tol in (2, 3):
                defs.append({"name": f"tol{tol}_{key}_ml{ml}", "kind": "tolerant",
                             "key": key, "thresh": 0.5, "tol": tol, "min_len": ml})
    for key in ("row_score", "core_score"):
        for enter in (3, 5, 8):
            for ex in (0, 2):
                defs.append({"name": f"hyst_{key}_e{enter}_x{ex}",
                             "kind": "hyst", "key": key, "enter": enter,
                             "exit": ex, "min_len": 2})
    return defs


# ---------------------------------------------------------------- trades ---
def trades_for(days, clusters):
    """Three return flavors per cluster (side-adjusted):
      ret_h : hindsight  open[start] -> close[end-1]   (user's Excel method)
      ret_c : entry-lagged close[entry_idx] -> close[end-1]
      ret_rt: fully real-time close[entry_idx] -> close[exit_idx]
    """
    out = []
    for c in clusters:
        i, j, side = c["start"], c["end"] - 1, c["side"]
        o, cl = days[i]["open"], days[j]["close"]
        if not o or not cl:
            continue
        ret_h = (cl - o) / o * side
        ei = c.get("entry_idx")
        ret_c = ret_rt = None
        if ei is not None and ei <= j and days[ei]["close"]:
            ce = days[ei]["close"]
            ret_c = (cl - ce) / ce * side
            xi = c.get("exit_idx")
            if xi is not None and xi >= ei and days[xi]["close"]:
                ret_rt = (days[xi]["close"] - ce) / ce * side
        out.append({"side": side, "ret": ret_h, "ret_c": ret_c, "ret_rt": ret_rt,
                    "len": j - i + 1, "date0": days[i]["date"]})
    return out


def score(trades, key="ret"):
    rets = [t[key] for t in trades if t[key] is not None]
    if len(rets) < 5:
        return None
    n = len(rets)
    wins = sum(1 for r in rets if r > 0)
    avg = sum(rets) / n
    var = sum((r - avg) ** 2 for r in rets) / (n - 1) if n > 1 else 0
    sd = math.sqrt(var)
    tstat = avg / (sd / math.sqrt(n)) if sd > 0 else 0
    gross_w = sum(r for r in rets if r > 0)
    gross_l = -sum(r for r in rets if r < 0)
    pf = gross_w / gross_l if gross_l > 0 else float("inf")
    return {"n": n, "win": wins / n, "avg": avg, "tstat": tstat, "pf": pf,
            "sum": sum(rets)}


def main():
    files = sorted(f for f in glob.glob("grids/*.json")
                   if not os.path.basename(f).startswith("_"))
    tickers = {}
    for f in files:
        g = json.load(open(f))
        if len(g["days"]) >= 30:
            tickers[g["ticker"]] = days_features(g["days"])
    print(f"{len(tickers)} tickers loaded, "
          f"{sum(len(v) for v in tickers.values())} ticker-days")

    # baseline: every day long open->close
    base = [((d["close"] - d["open"]) / d["open"])
            for days in tickers.values() for d in days if d["open"] and d["close"]]
    print(f"baseline daily long open->close: n={len(base)}, "
          f"win={sum(1 for r in base if r > 0)/len(base):.3f}, "
          f"avg={sum(base)/len(base):+.4%}")

    rows = []
    for definition in definition_matrix():
        all_trades = []
        per_ticker = {}
        for t, days in tickers.items():
            tr = trades_for(days, detect_def(days, definition))
            all_trades.extend(tr)
            per_ticker[t] = tr
        s = score(all_trades, "ret")
        if s is None:
            continue
        srt = score(all_trades, "ret_rt")
        longs = score([t for t in all_trades if t["side"] == 1], "ret_rt")
        shorts = score([t for t in all_trades if t["side"] == -1], "ret_rt")
        # consistency: split tickers in half, and early/late halves of trades
        tick_list = sorted(per_ticker)
        h = len(tick_list) // 2
        s1 = score([x for t in tick_list[:h] for x in per_ticker[t]], "ret_rt")
        s2 = score([x for t in tick_list[h:] for x in per_ticker[t]], "ret_rt")
        dates = sorted(t["date0"] for t in all_trades)
        mid = dates[len(dates) // 2]
        e1 = score([t for t in all_trades if t["date0"] <= mid], "ret_rt")
        e2 = score([t for t in all_trades if t["date0"] > mid], "ret_rt")
        rows.append({
            "def": definition["name"], **{k: round(v, 4) if isinstance(v, float) else v
                                          for k, v in s.items()},
            "n_rt": srt["n"] if srt else 0,
            "win_rt": round(srt["win"], 4) if srt else None,
            "avg_rt": round(srt["avg"], 4) if srt else None,
            "tstat_rt": round(srt["tstat"], 3) if srt else None,
            "win_long": round(longs["win"], 3) if longs else None,
            "win_short": round(shorts["win"], 3) if shorts else None,
            "win_halfA": round(s1["win"], 3) if s1 else None,
            "win_halfB": round(s2["win"], 3) if s2 else None,
            "win_early": round(e1["win"], 3) if e1 else None,
            "win_late": round(e2["win"], 3) if e2 else None,
        })
    rows.sort(key=lambda r: -(r["tstat_rt"] if r["tstat_rt"] else -9))

    os.makedirs("outputs", exist_ok=True)
    import csv as csvmod
    with open("outputs/sweep_leaderboard.csv", "w", newline="") as fh:
        w = csvmod.DictWriter(fh, fieldnames=list(rows[0].keys()))
        w.writeheader()
        w.writerows(rows)

    print(f"\n{'definition':26s} {'n':>5s} {'HIND win%':>9s} {'HIND avg':>9s} "
          f"{'RT win%':>8s} {'RT avg':>8s} {'RT t':>6s} {'long':>5s} {'short':>5s}")
    for r in rows[:22]:
        print(f"{r['def']:26s} {r['n']:5d} {r['win']:9.1%} {r['avg']:+9.2%} "
              f"{(r['win_rt'] or 0):8.1%} {(r['avg_rt'] or 0):+8.2%} "
              f"{(r['tstat_rt'] or 0):6.2f} "
              f"{(r['win_long'] or 0):5.2f} {(r['win_short'] or 0):5.2f}")
    print("\nworst 5 by RT tstat:")
    for r in rows[-5:]:
        print(f"{r['def']:26s} {r['n']:5d} {r['win']:9.1%} {r['avg']:+9.2%} "
              f"{(r['win_rt'] or 0):8.1%} {(r['avg_rt'] or 0):+8.2%} "
              f"{(r['tstat_rt'] or 0):6.2f}")
    print("\nsaved outputs/sweep_leaderboard.csv")


if __name__ == "__main__":
    main()
