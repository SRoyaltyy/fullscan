"""Strategy card generator (rulebook §9).

For a given strategy spec, walks ALL grids (tagging each trade discovery or
holdout), simulates every trade with full detail, and emits:

  strategies/<name>/trades.csv  -- one row per trade:
      ticker, split, buy_date, buy_price, sell_date, sell_price,
      gross_ret, net_ret, exit_reason, entry_colors, exit_colors
      (colors in HUMAN-READABLE names per rulebook §2, one per column A..O)
  strategies/<name>/card.json   -- full spec + per-split stats
  strategies/<name>/README.md   -- plain-English logic

  python engine/cards.py --def tol2_core_score_ml3 --side long_green \
      --exit hold2 --cohort "mid(1-10B):all" [--name my_strategy]
"""
import argparse
import csv
import glob
import json
import math
import os
import sys
from collections import defaultdict
from datetime import datetime, timedelta

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from sweep import days_features, detect_def, definition_matrix
from cohort_analysis import load_finviz, cohorts_of
from signals import classify_fill
from mine import COST_HI, COST_LO, tstat
from exit_rules import TP_LEVELS, TRAIL_LEVELS, HOLD_NS

DEFS = {d["name"]: d for d in definition_matrix()}


def color_name(rgb):
    fam, sc = classify_fill(rgb)
    if fam == "green":
        return {2.0: "deep green", 1.5: "green"}.get(sc, "light green")
    if fam == "red":
        return {-2.0: "deep red", -1.5: "red"}.get(sc, "pink")
    if fam in ("purple", "blue", "orange"):
        return fam
    return "white"


def s2d(n):
    return (datetime(1899, 12, 30) + timedelta(days=float(n))).date()


def simulate_detail(days, c, exit_rule):
    """Like mine.simulate_side but for ONE rule, returning full trade detail."""
    ei, xi = c.get("entry_idx"), c.get("exit_idx")
    side = c["side"]
    if ei is None:
        return None
    entry = days[ei]["close"]
    if not entry:
        return None
    closes = [d["close"] for d in days]
    highs = [d["high"] for d in days]
    lows = [d["low"] for d in days]
    last = xi if xi is not None and xi < len(days) else len(days) - 1
    raw = lambda px: (px - entry) / entry * side

    reason, out_idx = "flip", last
    if exit_rule == "flip":
        if xi is None or xi >= len(days):
            reason = "open_at_data_end"
    elif exit_rule.startswith("hold"):
        n = int(exit_rule[4:])
        out_idx = min(ei + n, last)
        reason = f"hold{n}" if ei + n <= last else "flip_before_hold"
    elif exit_rule.startswith("tp"):
        x = int(exit_rule[2:]) / 100
        tgt = entry * (1 + x * side)
        reason = "flip_no_tp"
        for k in range(ei + 1, last + 1):
            px = highs[k] if side == 1 else lows[k]
            if px is not None and ((side == 1 and px >= tgt) or
                                   (side == -1 and px <= tgt)):
                out_idx, reason = k, f"tp{int(x*100)}"
                break
        if reason.startswith("tp"):
            return {"exit_idx": out_idx, "reason": reason, "gross": x}
    elif exit_rule.startswith("trail"):
        y = int(exit_rule[5:]) / 100
        best, reason = entry, "flip_no_trail"
        for k in range(ei + 1, last + 1):
            if closes[k] is None:
                continue
            best = max(best, closes[k]) if side == 1 else min(best, closes[k])
            stop = best * (1 - y) if side == 1 else best * (1 + y)
            if (side == 1 and closes[k] <= stop) or \
               (side == -1 and closes[k] >= stop):
                out_idx, reason = k, f"trail{int(y*100)}"
                break
    else:
        raise ValueError(exit_rule)

    px = closes[out_idx]
    if px is None:
        return None
    return {"exit_idx": out_idx, "reason": reason, "gross": raw(px),
            "exit_price": px}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--def", dest="dn", required=True)
    ap.add_argument("--side", required=True,
                    choices=["long_green", "short_red"])
    ap.add_argument("--exit", dest="exit_rule", required=True)
    ap.add_argument("--cohort", required=True)
    ap.add_argument("--name")
    args = ap.parse_args()
    name = args.name or f"{args.dn}__{args.side}__{args.exit_rule}__" \
        + args.cohort.replace(":", "-").replace(" ", "")
    outdir = f"strategies/{name}"
    os.makedirs(outdir, exist_ok=True)

    definition = DEFS[args.dn]
    want_side = 1 if args.side == "long_green" else -1
    split = json.load(open("engine/holdout_split.json"))
    disco, hold = set(split["discovery"]), set(split["holdout"])
    fz = load_finviz()

    trades = []
    files = [f for f in sorted(glob.glob("grids/*.json"))
             if not os.path.basename(f).startswith("_")]
    for f in files:
        t = os.path.basename(f)[:-5]
        rec = fz.get(t)
        cohorts = ["ALL"] + (cohorts_of(rec) if rec else [])
        if args.cohort not in cohorts:
            continue
        try:
            days = days_features(json.load(open(f))["days"])
        except Exception:
            continue
        mc = rec["mcap"] if rec else None
        cost = COST_LO if (mc is not None and mc < 300) else COST_HI
        for c in detect_def(days, definition):
            if c["side"] != want_side:
                continue
            sim = simulate_detail(days, c, args.exit_rule)
            if sim is None:
                continue
            ei, x_i = c["entry_idx"], sim["exit_idx"]
            entry_px = days[ei]["close"]
            trades.append({
                "ticker": t,
                "split": "discovery" if t in disco else
                         "holdout" if t in hold else "unknown",
                "buy_date": str(s2d(days[ei]["date"])),
                "buy_price": round(entry_px, 4),
                "sell_date": str(s2d(days[x_i]["date"])),
                "sell_price": round(sim.get("exit_price") or
                                    entry_px * (1 + sim["gross"] * want_side), 4),
                "gross_ret": round(sim["gross"], 5),
                "net_ret": round(sim["gross"] - cost, 5),
                "exit_reason": sim["reason"],
                "entry_colors": "|".join(color_name(f)
                                         for f in days[ei]["fills"]),
                "exit_colors": "|".join(color_name(f)
                                        for f in days[x_i]["fills"]),
            })

    with open(f"{outdir}/trades.csv", "w", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=list(trades[0].keys()))
        w.writeheader()
        w.writerows(trades)

    stats = {}
    for sp in ("discovery", "holdout", "unknown"):
        vals = [t["net_ret"] for t in trades if t["split"] == sp]
        if len(vals) >= 2:
            stats[sp] = {
                "n": len(vals), "avg_net": sum(vals) / len(vals),
                "t": tstat(vals),
                "win": sum(1 for v in vals if v > 0) / len(vals),
                "tickers": len({t["ticker"] for t in trades
                                if t["split"] == sp}),
            }

    card = {
        "name": name,
        "spec": {"cohort_filter": args.cohort,
                 "cluster_definition": definition,
                 "side": args.side,
                 "entry_rule": "buy at cluster confirmation-day CLOSE "
                               "(leak-free per timing verdict, NOTES.md)",
                 "exit_rule": args.exit_rule},
        "cost_model": "0.10% round trip mcap>=$300M; 0.30% below",
        "color_legend": "deep green > green > light green > white > "
                        "pink < red < deep red (+purple/blue/orange)",
        "stats": stats,
        "n_trades": len(trades),
        "caveats": ["6.5-month single-regime window; deep-history regime "
                    "validation pending", "overlapping trades inflate t",
                    "survivorship bias: current listings only"],
    }
    json.dump(card, open(f"{outdir}/card.json", "w"), indent=1)

    with open(f"{outdir}/README.md", "w") as fh:
        fh.write(f"# Strategy: {name}\n\n"
                 f"- **Universe:** stocks matching `{args.cohort}`\n"
                 f"- **Signal:** `{args.dn}` = {json.dumps(definition)}\n"
                 f"- **Side:** {args.side.replace('_', ' ')}\n"
                 f"- **Entry:** buy at the close of the day the cluster is "
                 f"confirmed (the first day its color is knowable)\n"
                 f"- **Exit:** {args.exit_rule}\n"
                 f"- **Costs:** 0.1% round trip (0.3% microcaps)\n\n"
                 f"## Results\n\n```\n{json.dumps(stats, indent=2)}\n```\n\n"
                 f"Every trade in trades.csv: exact dates, prices, exit "
                 f"reason, and the highlight colors (human-readable) of the "
                 f"entry and exit days for eyeball-checking against Excel.\n")

    print(f"card written: {outdir}/ ({len(trades)} trades)")
    print(json.dumps(stats, indent=2))


if __name__ == "__main__":
    main()
