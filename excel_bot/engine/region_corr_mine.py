"""Clock-clean correlation mine for Excel H/I density + strength.

For row T (a session), features may use:
  - any completed row above T
  - atoms knowable at T's 09:30 (open + locked open fills / open-44)

Labels live on rows below T (and on T itself only as *outcomes*):
  H = (high-low)/open   — intraday range strength
  I = close/prev_close-1 — daily return
  green I = I > 0
  red   I = I < 0

Horizon is free: fixed 1/3/5/10-day windows *and* hold-until-the-current
I-region ends. That is the screenshot intuition: deep green already printed
on rows above → more green + larger |I|/H until the paint breaks.

DF/DG/DH are close same-row. They enter only as lag t−1+ via
excel_open_features.open_features (prior bar only).

Research only. Does not import flatten_robust.
"""
from __future__ import annotations

import argparse
import csv
import glob
import json
import os
import sys
from collections import defaultdict

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)

from excel_clock_gate import (  # noqa: E402
    SAME_ROW_LEAK_ABORT,
    assert_excel_clock_gate,
    assert_feature_legal,
    gate_payload,
)
from excel_open_features import feature_flags, open_features  # noqa: E402

HORIZONS = (1, 3, 5, 10)
MIN_N_DEFAULT = 80


def _f(x):
    try:
        if x is None or x == "":
            return None
        return float(x)
    except (TypeError, ValueError):
        return None


def _hi_from_ohlc(days):
    """Attach H (range) and I (daily) from OHLCV. I[0] is None."""
    out = []
    prev_c = None
    for d in days:
        o = _f(d.get("open") if "open" in d else d.get("o"))
        h = _f(d.get("high") if "high" in d else d.get("h"))
        l = _f(d.get("low") if "low" in d else d.get("l"))
        c = _f(d.get("close") if "close" in d else d.get("c"))
        v = _f(d.get("volume") if "volume" in d else d.get("v"))
        H = ((h - l) / o) if o and h is not None and l is not None and o > 0 else None
        I = ((c / prev_c) - 1.0) if c and prev_c and prev_c > 0 else None
        body = ((c - o) / o) if o and c is not None and o > 0 else None
        rec = {
            "date": d.get("date"),
            "o": o, "h": h, "l": l, "c": c, "v": v,
            "open": o, "high": h, "low": l, "close": c,
            "H": H, "I": I, "body": body,
            "fills": d.get("fills") or [],
        }
        out.append(rec)
        prev_c = c if c is not None else prev_c
    return out


def _streaks(days):
    """Prior-completed green/red I streaks ending at i-1. Index aligned to days."""
    g = [0] * len(days)
    r = [0] * len(days)
    gsum = [0.0] * len(days)
    rsum = [0.0] * len(days)
    gs = rs = 0
    gacc = racc = 0.0
    for i, d in enumerate(days):
        g[i], r[i], gsum[i], rsum[i] = gs, rs, gacc, racc
        I = d["I"]
        if I is None:
            gs = rs = 0
            gacc = racc = 0.0
        elif I > 0:
            gs += 1
            rs = 0
            gacc += I
            racc = 0.0
        elif I < 0:
            rs += 1
            gs = 0
            racc += I
            gacc = 0.0
        else:
            gs = rs = 0
            gacc = racc = 0.0
    return g, r, gsum, rsum


def _bar(d):
    return {"o": d["o"], "h": d["h"], "l": d["l"], "c": d["c"], "v": d["v"]}


def features_at_open(days, t):
    """Open-knowable snapshot for session t. Never reads H/L/C of t."""
    if t <= 0 or t >= len(days):
        raise ValueError("need a prior bar and a today row")
    today_o = days[t]["o"]
    prior = [_bar(d) for d in days[:t]]
    xl = open_features(prior, today_o)
    flags = feature_flags(xl)
    if xl.get("same_row_df") or xl.get("same_row_bb") or xl.get("same_row_bq"):
        raise ValueError("open_features leaked same-row DF/BB/BQ")
    for col in SAME_ROW_LEAK_ABORT:
        assert_feature_legal("value", col, 1)
    return xl, flags


def _fwd(days, t, h):
    """Outcomes on rows t .. t+h-1 (later than the feature row above t)."""
    sl = days[t:t + h]
    if len(sl) < h:
        return None
    Is = [d["I"] for d in sl if d["I"] is not None]
    Hs = [d["H"] for d in sl if d["H"] is not None]
    if not Is:
        return None
    n = len(Is)
    green = sum(1 for x in Is if x > 0)
    red = sum(1 for x in Is if x < 0)
    o0 = days[t]["o"]
    cN = sl[-1]["c"]
    hold = ((cN / o0) - 1.0) if o0 and cN and o0 > 0 else None
    return {
        "n": n,
        "green_density": green / n,
        "red_density": red / n,
        "mean_I": sum(Is) / n,
        "mean_abs_I": sum(abs(x) for x in Is) / n,
        "mean_H": (sum(Hs) / len(Hs)) if Hs else None,
        "sum_I": sum(Is),
        "hold_open_to_last_close": hold,
    }


def _hold_to_region_end(days, t, side):
    """Hold from open[t] until the forward I-run of `side` breaks.

    side=+1: stay through green I days, exit on first I<=0 close (or tape end).
    side=-1: stay through red I days, exit on first I>=0 close.
    Features never include I[t]; I[t] is the first outcome print.
    """
    if t >= len(days) or not days[t]["o"]:
        return None
    end = None
    for k in range(t, len(days)):
        I = days[k]["I"]
        if I is None:
            end = k
            break
        if side > 0 and I <= 0:
            end = k
            break
        if side < 0 and I >= 0:
            end = k
            break
        end = k
    if end is None:
        return None
    sl = days[t:end + 1]
    Is = [d["I"] for d in sl if d["I"] is not None]
    Hs = [d["H"] for d in sl if d["H"] is not None]
    o0, cN = days[t]["o"], days[end]["c"]
    hold = ((cN / o0) - 1.0) if o0 and cN and o0 > 0 else None
    n = len(Is)
    if n == 0:
        return None
    return {
        "hold_days": end - t + 1,
        "green_density": sum(1 for x in Is if x > 0) / n,
        "red_density": sum(1 for x in Is if x < 0) / n,
        "mean_I": sum(Is) / n,
        "mean_abs_I": sum(abs(x) for x in Is) / n,
        "mean_H": (sum(Hs) / len(Hs)) if Hs else None,
        "sum_I": sum(Is),
        "hold_open_to_last_close": hold,
    }


def _conditions(flags, g_streak, r_streak, gsum, rsum):
    """Whitelist IF clauses. Short list on purpose — not a combinatorial bomb."""
    cond = {
        "uncond": True,
        "g_streak>=1": g_streak >= 1,
        "g_streak>=3": g_streak >= 3,
        "g_streak>=5": g_streak >= 5,
        "g_streak>=8": g_streak >= 8,
        "g_streak>=12": g_streak >= 12,
        "r_streak>=1": r_streak >= 1,
        "r_streak>=3": r_streak >= 3,
        "r_streak>=5": r_streak >= 5,
        "deep_green_sum>=0.08": g_streak >= 3 and gsum >= 0.08,
        "deep_green_sum>=0.15": g_streak >= 5 and gsum >= 0.15,
        "deep_red_sum<=-0.08": r_streak >= 3 and rsum <= -0.08,
    }
    flag_keys = (
        "prior_hammer", "prior_hanging", "prior_shooting",
        "prior_bull_engulf", "prior_bear_engulf",
        "prior_morning", "prior_evening",
        "prior_doji", "prior_bullish", "prior_bearish",
        "FR_ge1", "AH_ge1", "FQ", "JC", "JB",
        "J_ge0", "J_lt0", "J_le-1",
        "EP_ge03", "ER_p1", "ER_m1",
    )
    for k in flag_keys:
        cond[k] = bool(flags.get(k))
    cond["g5+hammer"] = g_streak >= 5 and bool(flags.get("prior_hammer"))
    cond["g5+FR"] = g_streak >= 5 and bool(flags.get("FR_ge1"))
    cond["g5+bullish"] = g_streak >= 5 and bool(flags.get("prior_bullish"))
    cond["r3+hammer"] = r_streak >= 3 and bool(flags.get("prior_hammer"))
    cond["r3+morning"] = r_streak >= 3 and bool(flags.get("prior_morning"))
    cond["g3+J_ge0"] = g_streak >= 3 and bool(flags.get("J_ge0"))
    cond["g3+J_lt0"] = g_streak >= 3 and bool(flags.get("J_lt0"))
    return cond


def iter_rows(days):
    """Yield (t, cond, fixed-horizon outcomes, region-end outcomes)."""
    days = _hi_from_ohlc(days)
    if len(days) < 8:
        return
    g, r, gsum, rsum = _streaks(days)
    for t in range(6, len(days)):
        if days[t]["o"] is None:
            continue
        try:
            xl, flags = features_at_open(days, t)
        except Exception:
            continue
        cond = _conditions(flags, g[t], r[t], gsum[t], rsum[t])
        cond["_meta"] = {
            "g_streak": g[t], "r_streak": r[t],
            "df": xl.get("df"), "dg": xl.get("dg"), "dh": xl.get("dh"),
        }
        fixed = {h: _fwd(days, t, h) for h in HORIZONS}
        ends = {
            "hold_green_region": _hold_to_region_end(days, t, +1),
            "hold_red_region": _hold_to_region_end(days, t, -1),
        }
        yield t, cond, fixed, ends


def _acc():
    return {"n": 0, "green": 0.0, "red": 0.0, "mean_I": 0.0,
            "mean_abs_I": 0.0, "mean_H": 0.0, "nH": 0,
            "sum_I": 0.0, "hold": 0.0, "n_hold": 0, "hold_days": 0.0}


def _add(cell, o):
    if not o:
        return
    cell["n"] += 1
    cell["green"] += o.get("green_density") or 0.0
    cell["red"] += o.get("red_density") or 0.0
    cell["mean_I"] += o.get("mean_I") or 0.0
    cell["mean_abs_I"] += o.get("mean_abs_I") or 0.0
    if o.get("mean_H") is not None:
        cell["mean_H"] += o["mean_H"]
        cell["nH"] += 1
    cell["sum_I"] += o.get("sum_I") or 0.0
    if o.get("hold_open_to_last_close") is not None:
        cell["hold"] += o["hold_open_to_last_close"]
        cell["n_hold"] += 1
    if o.get("hold_days") is not None:
        cell["hold_days"] += o["hold_days"]


def _fin(cell):
    n = cell["n"]
    if n <= 0:
        return None
    out = {
        "n": n,
        "green_density": cell["green"] / n,
        "red_density": cell["red"] / n,
        "mean_I": cell["mean_I"] / n,
        "mean_abs_I": cell["mean_abs_I"] / n,
        "mean_H": (cell["mean_H"] / cell["nH"]) if cell["nH"] else None,
        "sum_I": cell["sum_I"] / n,
        "hold_open_to_last_close": (cell["hold"] / cell["n_hold"]) if cell["n_hold"] else None,
    }
    if cell["hold_days"]:
        out["hold_days"] = cell["hold_days"] / n
    return out


def _lift(rule, base, key):
    if not rule or not base:
        return None
    a, b = rule.get(key), base.get(key)
    if a is None or b is None or b == 0:
        return None
    return a / b


def mine_days_map(ticker_days, min_n=MIN_N_DEFAULT):
    """ticker -> raw day list. Returns ranked rows vs unconditional base."""
    cells = defaultdict(_acc)
    for _tkr, raw in ticker_days.items():
        for _t, cond, fixed, ends in iter_rows(raw):
            names = [k for k, v in cond.items() if v is True and k != "_meta"]
            for rule in names:
                for h, o in fixed.items():
                    _add(cells[(rule, f"h{h}")], o)
                for name, o in ends.items():
                    _add(cells[(rule, name)], o)
    bases = {b: _fin(cells[("uncond", b)]) for b in
             [f"h{h}" for h in HORIZONS] + ["hold_green_region", "hold_red_region"]}
    rows = []
    for (rule, bucket), acc in cells.items():
        if rule == "uncond":
            continue
        stats = _fin(acc)
        if not stats or stats["n"] < min_n:
            continue
        base = bases.get(bucket)
        rows.append({
            "rule": rule,
            "bucket": bucket,
            **stats,
            "base_n": (base or {}).get("n"),
            "base_green_density": (base or {}).get("green_density"),
            "base_mean_I": (base or {}).get("mean_I"),
            "base_mean_abs_I": (base or {}).get("mean_abs_I"),
            "base_mean_H": (base or {}).get("mean_H"),
            "base_hold": (base or {}).get("hold_open_to_last_close"),
            "lift_green": _lift(stats, base, "green_density"),
            "lift_abs_I": _lift(stats, base, "mean_abs_I"),
            "lift_H": _lift(stats, base, "mean_H"),
            "lift_hold": _lift(stats, base, "hold_open_to_last_close"),
            "delta_mean_I": (stats["mean_I"] - base["mean_I"]) if base else None,
        })
    rows.sort(key=lambda r: (
        -(r["lift_green"] or 0),
        -(r["lift_abs_I"] or 0),
        -r["n"],
    ))
    return {"base": bases, "rows": rows, "gate": gate_payload()}


def load_grids(paths):
    out = {}
    for p in paths:
        try:
            g = json.load(open(p, encoding="utf-8"))
        except Exception:
            continue
        days = g.get("days") or []
        t = g.get("ticker") or os.path.basename(p)[:-5]
        if len(days) >= 20:
            out[t] = days
    return out


def write_md(result, path, title):
    lines = [
        f"# {title}",
        "",
        "Clock: rows above T + 09:30-knowable atoms. H/I on T and below are labels.",
        "DF/DG/DH enter as lag-1 text only. `flatten_robust` untouched.",
        "",
        "| rule | bucket | n | green dens | lift green | mean I | ΔI | |I| lift | H lift | hold | lift hold |",
        "|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for r in result["rows"][:80]:
        def fmt(x, p=3):
            if x is None:
                return ""
            return f"{x:.{p}f}"
        lines.append(
            f"| `{r['rule']}` | {r['bucket']} | {r['n']} | "
            f"{fmt(r['green_density'])} | {fmt(r['lift_green'])} | "
            f"{fmt(r['mean_I'])} | {fmt(r['delta_mean_I'])} | "
            f"{fmt(r['lift_abs_I'])} | {fmt(r['lift_H'])} | "
            f"{fmt(r['hold_open_to_last_close'])} | {fmt(r['lift_hold'])} |"
        )
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    with open(path, "w", encoding="utf-8") as fh:
        fh.write("\n".join(lines) + "\n")


def write_csv(result, path):
    if not result["rows"]:
        return
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    keys = list(result["rows"][0].keys())
    with open(path, "w", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(fh, fieldnames=keys)
        w.writeheader()
        w.writerows(result["rows"])


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--grids", default="grids")
    ap.add_argument("--min-n", type=int, default=MIN_N_DEFAULT)
    ap.add_argument("--limit", type=int, default=0)
    ap.add_argument("--out-md", default="excel_bot/research/REGION_CORR_MINE.md")
    ap.add_argument("--out-csv", default="excel_bot/research/region_corr_mine.csv")
    args = ap.parse_args()
    assert_excel_clock_gate()
    files = sorted(
        f for f in glob.glob(os.path.join(args.grids, "*.json"))
        if not os.path.basename(f).startswith("_")
    )
    if args.limit:
        files = files[: args.limit]
    ticker_days = load_grids(files)
    print(f"[region-corr] {len(ticker_days)} tickers from {args.grids}", flush=True)
    result = mine_days_map(ticker_days, min_n=args.min_n)
    write_md(result, args.out_md, "Region / H-I correlation mine")
    write_csv(result, args.out_csv)
    print(f"[region-corr] {len(result['rows'])} rules kept (n>={args.min_n})")
    print(f"[region-corr] {args.out_md}")
    for r in result["rows"][:15]:
        print(
            f"  {r['rule']:22s} {r['bucket']:20s} n={r['n']:5d} "
            f"g={r['green_density']:.3f} lift_g={r['lift_green'] or 0:.2f} "
            f"dI={r['delta_mean_I'] or 0:+.4f}"
        )


if __name__ == "__main__":
    main()
