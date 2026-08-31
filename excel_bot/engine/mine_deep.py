"""Deep-history strategy tournament.

Same idea as mine.py (33 cluster defs x 14 exit rules x 2 sides x all finviz
cohort labels) but run over the FULL-SPAN deep grids (grids_deep/, ~2.5y of
colors per ticker) with:
  - realistic round-trip costs (Futubull-flavoured, configurable)
  - discovery / holdout split (engine/holdout_split.json) BOTH reported;
    a combo only wins if it works on tickers it has never seen
  - regime gate: no more than one losing year (2022-2026)
  - automatic strategy-card emission into strategies/ (the daily bot and
    the portfolio sim pick new cards up with zero wiring changes)
  - trade-ledger emission: winners' trades are appended to
    backtest/trades.csv so portfolio.py / the dashboard include them
    immediately (same schema as backfill.py)

Usage:
  python engine/mine_deep.py --min-n 300 --max-cards 10
"""
import argparse
import csv
import glob
import json
import math
import os
import re
import sys
from collections import defaultdict
from datetime import date

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from sweep import days_features, detect_def, definition_matrix
from cohort_analysis import load_finviz, cohorts_of
from exit_rules import TP_LEVELS, TRAIL_LEVELS, HOLD_NS
from mine import simulate_side
from cards import color_name, s2d
from backfill import TRADE_FIELDS

GRIDS_DIR = "grids_deep"
BT_DIR = "backtest"
TRADES_CSV = os.path.join(BT_DIR, "trades.csv")
COST_LONG = 0.0015     # ~$14 round trip on a $10k Futu order + slippage
COST_SHORT = 0.0020    # + borrow
YEARS = ("2022", "2023", "2024", "2025", "2026")

_G = {}


# ------------------------------------------------------------- pass 1: scan
def scan_grid(path):
    """One deep grid -> {(def,side,rule,cohort): {split: stats, years: {}}}."""
    t = os.path.basename(path)[:-5]
    split = _G["split"].get(t)
    if split is None:
        return {}
    try:
        feats = days_features(json.load(open(path))["days"])
    except Exception:
        return {}
    if len(feats) < 60:
        return {}
    rec = _G["fz"].get(t)
    cohorts = ["ALL"] + (cohorts_of(rec) if rec else [])
    out = {}
    for definition in _G["defs"]:
        for c in detect_def(feats, definition):
            sim = simulate_side(feats, c)
            if sim is None:
                continue
            side = "long_green" if c["side"] == 1 else "short_red"
            cost = COST_LONG if c["side"] == 1 else COST_SHORT
            yr = str(s2d(feats[c["entry_idx"]]["date"]))[:4]
            for rule, ret in sim.items():
                if ret is None or rule == "MFE":
                    continue
                net = ret - cost
                for co in cohorts:
                    k = (definition["name"], side, rule, co)
                    cell = out.get(k)
                    if cell is None:
                        cell = out[k] = {"disc": [0, 0.0, 0.0, 0],
                                         "hold": [0, 0.0, 0.0, 0],
                                         "years": {}}
                    slot = cell["disc"] if split == "discovery" else cell["hold"]
                    slot[0] += 1
                    slot[1] += net
                    slot[2] += net * net
                    slot[3] += 1 if net > 0 else 0
                    y = cell["years"].setdefault(yr, [0, 0.0])
                    y[0] += 1
                    y[1] += net
    return out


def _init_scan(split_map, fz, defs):
    _G["split"] = split_map
    _G["fz"] = fz
    _G["defs"] = defs


def merge_cell(dst, src):
    for split in ("disc", "hold"):
        for i in range(4):
            dst[split][i] += src[split][i]
    for y, v in src["years"].items():
        d = dst["years"].setdefault(y, [0, 0.0])
        d[0] += v[0]
        d[1] += v[1]


def stat_block(slot):
    n, s, sq, w = slot
    if n < 2:
        return None
    avg = s / n
    var = max(sq - s * s / n, 0) / (n - 1)
    t = avg / math.sqrt(var / n) if var > 0 else 0
    return {"n": n, "avg_net": avg, "t": t, "win": w / n}


# ---------------------------------------------------------- pass 2: emit ---
def emit_trades(path, winners, fz_rec):
    """Trade rows (backfill schema) for winning combos in one grid."""
    t = os.path.basename(path)[:-5]
    try:
        feats = days_features(json.load(open(path))["days"])
    except Exception:
        return []
    cohorts = {"ALL"} | set(cohorts_of(fz_rec) if fz_rec else [])
    n = len(feats)
    closes = [d["close"] for d in feats]
    opens = [d["open"] for d in feats]
    rows = []
    for wname, definition, side_s, rule, co in winners:
        if co != "ALL" and co not in cohorts:
            continue
        side = 1 if side_s == "long_green" else -1
        for c in detect_def(feats, definition):
            if c["side"] != side:
                continue
            ei, xi = c.get("entry_idx"), c.get("exit_idx")
            if ei is None or ei >= n or not closes[ei]:
                continue
            entry = closes[ei]
            if rule == "flip":
                if xi is None or xi >= n or not closes[xi]:
                    continue
                ex_i, ex_px, reason = xi, closes[xi], "flip"
            elif rule.startswith("hold"):
                hn = int(rule[4:])
                ex_i = min(ei + hn, (xi if xi is not None else n - 1), n - 1)
                ex_px, reason = closes[ex_i], "hold"
            elif rule.startswith("tp"):
                x = int(rule[2:]) / 100.0
                tgt = entry * (1 + x * side)
                hi = xi if xi is not None else n - 1
                ex_i = ex_px = None
                reason = "tp_miss_flip"
                for k in range(ei + 1, hi + 1):
                    px = feats[k]["high"] if side == 1 else feats[k]["low"]
                    if px is None:
                        continue
                    if (side == 1 and px >= tgt) or (side == -1 and px <= tgt):
                        ex_i, ex_px, reason = k, tgt, "tp_hit"
                        break
                if ex_i is None:
                    ex_i, ex_px = hi, closes[hi]
            elif rule.startswith("trail"):
                y = int(rule[5:]) / 100.0
                hi = xi if xi is not None else n - 1
                best = entry
                ex_i, ex_px, reason = hi, closes[hi], "trail_end"
                for k in range(ei + 1, hi + 1):
                    cc = closes[k]
                    if not cc:
                        continue
                    best = max(best, cc) if side == 1 else min(best, cc)
                    stop = best * (1 - y) if side == 1 else best * (1 + y)
                    if (side == 1 and cc <= stop) or (side == -1 and cc >= stop):
                        ex_i, ex_px, reason = k, cc, "trail"
                        break
            else:
                continue
            if not ex_px:
                continue
            ret_c = (ex_px - entry) / entry * side
            no = opens[ei + 1] if ei + 1 < n else None
            ret_o = ((ex_px - no) / no * side) if no and ex_i > ei else None
            d_e = feats[ei]
            rows.append({
                "ticker": t, "strategy": wname,
                "side": "LONG" if side == 1 else "SHORT",
                "signal_date": str(s2d(d_e["date"])),
                "cluster_start": str(s2d(feats[c["start"]]["date"])),
                "cluster_len_days": c["end"] - c["start"],
                "entry_close": f"{entry:.4f}",
                "next_open": f"{no:.4f}" if no else "",
                "exit_date": str(s2d(feats[ex_i]["date"])),
                "exit_price": f"{ex_px:.4f}",
                "exit_reason": reason,
                "ret_close_entry": f"{ret_c*100:+.2f}%",
                "ret_open_entry": (f"{ret_o*100:+.2f}%"
                                   if ret_o is not None else ""),
                "hold_days": (s2d(feats[ex_i]["date"]) -
                              s2d(d_e["date"])).days,
                "cohorts": "|".join(sorted(cohorts - {"ALL"})),
                "signal_colors": "|".join(color_name(f) for f in d_e["fills"]),
            })
    return rows


# ------------------------------------------------------------------- main --
def slug(s):
    return re.sub(r"[^a-z0-9]+", "_", s.lower()).strip("_")[:32]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--min-n", type=int, default=300)
    ap.add_argument("--max-cards", type=int, default=10)
    ap.add_argument("--workers", type=int, default=min(4, os.cpu_count() or 2))
    args = ap.parse_args()

    split = json.load(open("engine/holdout_split.json"))
    split_map = {t: "discovery" for t in split["discovery"]}
    split_map.update({t: "holdout" for t in split["holdout"]})
    fz = load_finviz()
    defs = definition_matrix()
    files = sorted(f for f in glob.glob(os.path.join(GRIDS_DIR, "*.json"))
                   if not os.path.basename(f).startswith("_"))
    print(f"[scan] {len(files)} deep grids, {len(defs)} definitions, "
          f"{len(HOLD_NS) + len(TP_LEVELS) + len(TRAIL_LEVELS) + 1} exits",
          flush=True)

    cells = {}
    from multiprocessing import Pool
    with Pool(args.workers, initializer=_init_scan,
              initargs=(split_map, fz, defs)) as pool:
        for i, out in enumerate(pool.imap_unordered(scan_grid, files,
                                                    chunksize=8)):
            for k, cell in out.items():
                dst = cells.get(k)
                if dst is None:
                    dst = cells[k] = {
                        "disc": [0, 0.0, 0.0, 0], "hold": [0, 0.0, 0.0, 0],
                        "years": {}}
                merge_cell(dst, cell)
            if (i + 1) % 500 == 0:
                print(f"  ... {i+1}/{len(files)}", flush=True)

    # ---- existing combos (don't re-issue cards for them)
    existing = set()
    for cj in glob.glob("strategies/*/card.json"):
        card = json.load(open(cj))
        sp = card["spec"]
        existing.add((sp["cluster_definition"]["name"], sp["side"],
                      sp["exit_rule"], sp["cohort_filter"]))

    rows = []
    for (dn, side, rule, co), cell in cells.items():
        d, h = stat_block(cell["disc"]), stat_block(cell["hold"])
        if not d or not h:
            continue
        if d["n"] < args.min_n or h["n"] < max(100, args.min_n // 3):
            continue
        if d["t"] < 3 or h["t"] < 2 or h["avg_net"] <= 0:
            continue
        losing = sum(1 for y in YEARS
                     if cell["years"].get(y, [0, 0.0])[0] >= 30
                     and cell["years"][y][1] / cell["years"][y][0] < 0)
        if losing > 1:
            continue
        rows.append({"def": dn, "side": side, "exit": rule, "cohort": co,
                     "disc": d, "hold": h, "losing_years": losing,
                     "robust_t": min(d["t"], h["t"]),
                     "dup": (dn, side, rule, co) in existing})
    rows.sort(key=lambda r: -r["robust_t"])
    print(f"[gate] {len(rows)} combos passed "
          f"(of {len(cells)} with any trades)", flush=True)

    # ---- leaderboard
    os.makedirs(BT_DIR, exist_ok=True)
    L = [f"# Deep strategy tournament — {date.today()}",
         "",
         f"{len(files)} tickers x 33 defs x 14 exits x 2 sides x cohorts, "
         f"full-history deep grids. NET of costs (long {COST_LONG:.2%}, "
         f"short {COST_SHORT:.2%} round trip). Gates: discovery n>={args.min_n} "
         f"t>=3; holdout n>={max(100, args.min_n//3)} t>=2 avg>0; "
         f"<=1 losing year.",
         "",
         "| # | def | side | exit | cohort | disc n/win/avg/t | "
         "hold n/win/avg/t | dup |",
         "|---|---|---|---|---|---|---|---|"]
    for i, r in enumerate(rows[:60]):
        L.append(f"| {i+1} | {r['def']} | {r['side']} | {r['exit']} | "
                 f"{r['cohort']} | {r['disc']['n']}/{r['disc']['win']:.0%}/"
                 f"{r['disc']['avg_net']:+.2%}/{r['disc']['t']:.1f} | "
                 f"{r['hold']['n']}/{r['hold']['win']:.0%}/"
                 f"{r['hold']['avg_net']:+.2%}/{r['hold']['t']:.1f} | "
                 f"{'DUP' if r['dup'] else ''} |")
    lb = os.path.join(BT_DIR, f"mine_leaderboard_{date.today()}.md")
    with open(lb, "w", encoding="utf-8") as fh:
        fh.write("\n".join(L) + "\n")
    print(f"[leaderboard] {lb}", flush=True)

    # ---- cards for the top fresh combos
    fresh = [r for r in rows if not r["dup"]][:args.max_cards]
    def_by_name = {d["name"]: d for d in defs}
    winners = []
    for i, r in enumerate(fresh):
        wname = f"M{i+1:02d}_{'lg' if r['side']=='long_green' else 'sr'}_" \
                f"{r['exit']}_{slug(r['cohort'])}"
        card = {
            "name": wname,
            "spec": {
                "cohort_filter": r["cohort"],
                "cluster_definition": def_by_name[r["def"]],
                "side": r["side"],
                "entry_rule": "buy at cluster confirmation-day CLOSE "
                              "(leak-free per timing verdict, NOTES.md)",
                "exit_rule": r["exit"],
            },
            "cost_model": f"long {COST_LONG:.2%} / short {COST_SHORT:.2%} "
                          "round trip (Futu fees + slippage)",
            "stats": {"discovery": r["disc"], "holdout": r["hold"]},
            "caveats": ["mined on full-history deep grids "
                        f"({date.today()}); cohort tags = current finviz "
                        "snapshot", "overlapping trades inflate t",
                        "survivorship bias: current listings only"],
        }
        os.makedirs(f"strategies/{wname}", exist_ok=True)
        json.dump(card, open(f"strategies/{wname}/card.json", "w"), indent=1)
        winners.append((wname, def_by_name[r["def"]], r["side"], r["exit"],
                        r["cohort"]))
        print(f"[card] {wname}  disc_t={r['disc']['t']:.1f} "
              f"hold_t={r['hold']['t']:.1f} avg={r['hold']['avg_net']:+.2%}",
              flush=True)

    # ---- append winners' trades to the ledger
    if winners:
        new_rows = 0
        write_header = not os.path.exists(TRADES_CSV)
        with open(TRADES_CSV, "a", newline="", encoding="utf-8") as fh:
            w = csv.DictWriter(fh, fieldnames=TRADE_FIELDS)
            if write_header:
                w.writeheader()
            for f in files:
                t = os.path.basename(f)[:-5]
                for tr in emit_trades(f, winners, fz.get(t)):
                    w.writerow(tr)
                    new_rows += 1
        print(f"[ledger] +{new_rows} trades appended for "
              f"{len(winners)} new strategies", flush=True)


if __name__ == "__main__":
    main()
