"""Mine a small --all-cols sample. Conservative close clock on deeper
formula states; open clock only for yesterday-deeper + today's A.

Writes a compact table (n / effect / tape) — not a 17MB dump.
"""
from __future__ import annotations

import glob
import json
import math
import os
import sys
from collections import defaultdict
from datetime import date, datetime, timedelta

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from clock import COST_FUTU_LONG, COST_FUTU_SHORT, SHIP, hold_exit_idx  # noqa: E402
from signals import classify_fill  # noqa: E402

SAMPLE = os.environ.get("ALL_COLS_DIR", "research/all_cols_sample")
OUT_MD = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                      "..", "research", "ALL_COLS_MINE.md")
SB_MD = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                     "..", "..", "03_scoreboard", "EXCEL_BOT_MINE.md")
OUT_JSON = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                        "..", "research", "all_cols_mine.json")
HOLDS = (1, 2, 3, 5, 8)


def s2d(n):
    return (datetime(1899, 12, 30) + timedelta(days=float(n))).date()


def tstat(vals):
    n = len(vals)
    if n < 2:
        return float("nan")
    m = sum(vals) / n
    v = sum((x - m) ** 2 for x in vals) / (n - 1)
    return m / math.sqrt(v / n) if v > 0 else float("nan")


def load_spy():
    p = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                     "..", "research", "spy_tape.json")
    if not os.path.exists(p):
        return {}
    raw = json.load(open(p))
    rows = raw if isinstance(raw, list) else raw.get("days") or []
    out, prev = {}, None
    for r in rows:
        iso = str(r["date"])[:10]
        c = r.get("close")
        if c is None:
            continue
        if prev is not None:
            out[iso] = 1 if c > prev else -1
        prev = c
    return out


def num(cell):
    if not cell:
        return None
    v = cell.get("v")
    return v if isinstance(v, (int, float)) else None


def fam(cell):
    if not cell:
        return "none"
    return classify_fill(cell.get("f"))[0]


def gates(day, prev=None):
    """Named boolean gates. clock is attached on the pattern list."""
    L, O, EL = num(day["cells"].get("L")), num(day["cells"].get("O")), num(day["cells"].get("EL"))
    V, AD, JA = num(day["cells"].get("V")), num(day["cells"].get("AD")), num(day["cells"].get("JA"))
    IZ = num(day["cells"].get("IZ"))
    a_fam = fam(day["cells"].get("A"))
    deeper_g = deeper_r = 0
    for let, rec in day["cells"].items():
        if let in "ABCDEFGHIJKLMNO":
            continue
        f = fam(rec)
        if f == "green":
            deeper_g += 1
        elif f == "red":
            deeper_r += 1
    g = {
        "L_ge1": L is not None and L >= 1,
        "L_le-1": L is not None and L <= -1,
        "L_le-3": L is not None and L <= -3,
        "O_ge1": O is not None and O >= 1,
        "O_le-1": O is not None and O <= -1,
        "EL_ge2": EL is not None and EL >= 2,
        "EL_le-2": EL is not None and EL <= -2,
        "V_le-1": V is not None and V <= -1,
        "AD_ge1": AD is not None and AD >= 1,
        "AD_le-1": AD is not None and AD <= -1,
        "JA_eq1": JA == 1,
        "IZ_eq1": IZ == 1,
        "deeper_g5": deeper_g >= 5,
        "deeper_r5": deeper_r >= 5,
        "A_green": a_fam == "green",
        "A_red": a_fam == "red",
    }
    if prev:
        pL, pEL = num(prev["cells"].get("L")), num(prev["cells"].get("EL"))
        g["lag_Lge1_Agreen"] = (pL is not None and pL >= 1) and a_fam == "green"
        g["lag_Lle-1_Ared"] = (pL is not None and pL <= -1) and a_fam == "red"
        g["lag_ELge2_Agreen"] = (pEL is not None and pEL >= 2) and a_fam == "green"
        g["lag_ELle-2_Ared"] = (pEL is not None and pEL <= -2) and a_fam == "red"
    else:
        g["lag_Lge1_Agreen"] = g["lag_Lle-1_Ared"] = False
        g["lag_ELge2_Agreen"] = g["lag_ELle-2_Ared"] = False
    return g


# (name, side, clock)  side +1 long / -1 short
PATS = [
    ("L_ge1", 1, "close"), ("L_le-1", -1, "close"), ("L_le-3", -1, "close"),
    ("O_ge1", 1, "close"), ("O_le-1", -1, "close"),
    ("EL_ge2", 1, "close"), ("EL_le-2", -1, "close"),
    ("V_le-1", -1, "close"),
    ("AD_ge1", 1, "close"), ("AD_le-1", -1, "close"),
    ("JA_eq1", 1, "close"), ("IZ_eq1", 1, "close"),
    ("deeper_g5", 1, "close"), ("deeper_r5", -1, "close"),
    ("lag_Lge1_Agreen", 1, "open"), ("lag_Lle-1_Ared", -1, "open"),
    ("lag_ELge2_Agreen", 1, "open"), ("lag_ELle-2_Ared", -1, "open"),
]


def sim(days, ei, side, clock, hold_n):
    last = len(days) - 1
    k = hold_exit_idx(ei, hold_n, clock, last)
    need = ei + (hold_n - 1 if clock == "open" else hold_n)
    if need > last:
        return None
    entry = days[ei]["open"] if clock == "open" else days[ei]["close"]
    ex = days[k]["close"]
    if not entry or not ex:
        return None
    return (ex - entry) / entry * side


def verdict(n, n_tickers, t, avg, early_s, late_s):
    if n_tickers < 50 or n < 80:
        return "THIN"
    if avg <= 0 or (t == t and t < 2):
        return "FAIL"
    if early_s is not None and late_s is not None and early_s * late_s <= 0:
        return "FAIL"
    if t >= 3 and n >= 300 and n_tickers >= 50:
        return "PASS"
    return "FAIL"


def main():
    split = json.load(open("engine/holdout_split.json"))
    disc, hold = set(split["discovery"]), set(split["holdout"])
    spy = load_spy()
    files = sorted(f for f in glob.glob(os.path.join(SAMPLE, "*.json"))
                   if not os.path.basename(f).startswith("_"))
    cells = defaultdict(lambda: {
        "raw": [], "tickers": set(), "dates": set(),
        "early": [], "late": [], "spy_up": [], "spy_dn": [],
        "disc": [], "hold": [],
    })
    for path in files:
        g = json.load(open(path))
        t = g["ticker"]
        days = g["days"]
        split_t = ("discovery" if t in disc else "holdout" if t in hold else None)
        prev = None
        for i, day in enumerate(days):
            gts = gates(day, prev)
            iso = str(s2d(day["date"]))
            tape = spy.get(iso, 0)
            half = "early" if iso < "2026-05-01" else "late"
            for name, side, clock in PATS:
                if not gts.get(name):
                    continue
                for h in HOLDS:
                    raw = sim(days, i, side, clock, h)
                    if raw is None:
                        continue
                    cost = COST_FUTU_LONG if side == 1 else COST_FUTU_SHORT
                    net = raw - cost
                    key = (name, clock, "long" if side == 1 else "short", f"hold{h}")
                    c = cells[key]
                    c["raw"].append(net)
                    c["tickers"].add(t)
                    c["dates"].add(iso)
                    c[half].append(net)
                    if tape == 1:
                        c["spy_up"].append(net)
                    elif tape == -1:
                        c["spy_dn"].append(net)
                    if split_t == "discovery":
                        c["disc"].append(net)
                    elif split_t == "holdout":
                        c["hold"].append(net)
            prev = day

    def blk(vals):
        if len(vals) < 2:
            return None
        m = sum(vals) / len(vals)
        return {"n": len(vals), "avg": m, "t": tstat(vals),
                "win": sum(1 for v in vals if v > 0) / len(vals)}

    rows = []
    for (name, clock, side, rule), c in cells.items():
        allb = blk(c["raw"])
        if not allb:
            continue
        early, late = blk(c["early"]), blk(c["late"])
        up, dn = blk(c["spy_up"]), blk(c["spy_dn"])
        d, h = blk(c["disc"]), blk(c["hold"])
        v = verdict(allb["n"], len(c["tickers"]), allb["t"], allb["avg"],
                    None if not early else early["avg"],
                    None if not late else late["avg"])
        rows.append({
            "def": name, "clock": clock, "side": side, "exit": rule,
            "n": allb["n"], "avg_net": allb["avg"], "t": allb["t"],
            "win": allb["win"], "n_tickers": len(c["tickers"]),
            "n_dates": len(c["dates"]),
            "early": early, "late": late, "spy_up": up, "spy_dn": dn,
            "discovery": d, "holdout": h, "verdict": v,
            "cost_model": "futubull",
        })
    rows.sort(key=lambda r: (-(r["t"] if r["t"] == r["t"] else -9), -r["n"]))

    meta_path = os.path.join(SAMPLE, "_meta.json")
    meta = json.load(open(meta_path)) if os.path.exists(meta_path) else {}
    tickers = [os.path.basename(f)[:-5] for f in files]
    meta["n_discovery"] = sum(1 for t in tickers if t in disc)
    meta["n_holdout"] = sum(1 for t in tickers if t in hold)
    payload = {
        "generated": str(date.today()),
        "sample_tickers": tickers,
        "n_tickers": len(files),
        "n_discovery": meta["n_discovery"],
        "n_holdout": meta["n_holdout"],
        "minutes_per_ticker": meta.get("minutes_per_ticker"),
        "n_rows": len(rows),
        "n_pass": sum(1 for r in rows if r["verdict"] == "PASS"),
        "n_fail": sum(1 for r in rows if r["verdict"] == "FAIL"),
        "n_thin": sum(1 for r in rows if r["verdict"] == "THIN"),
        "live_untouched": "flatten_robust",
        "cells": rows,
    }
    json.dump(payload, open(OUT_JSON, "w"), indent=1)

    def fmt_blk(b):
        if not b:
            return "—"
        return f"{b['n']}/{b['avg']*100:+.2f}%/t={b['t']:.1f}"

    L = [
        "# Excel --all-cols sample mine",
        "",
        f"_Generated {date.today()} · live `flatten_robust` is not changed. "
        f"No merge without Cyrus._",
        "",
        f"Sample **{payload['n_tickers']}** tickers, lean A–JL capture "
        f"({meta.get('minutes_per_ticker') and round(meta['minutes_per_ticker']*60, 2)} s/ticker). "
        f"Deeper formula states (L/O/EL/V/AD/JA/IZ + past-O fills) enter at "
        f"**close** (timing untested past A–O). Lag gates use yesterday L/EL + "
        f"today A → **open**. Cost = futubull 0.15%/0.20%. Holds 1/2/3/5/8.",
        "",
        f"Verdicts: **PASS {payload['n_pass']}** · **FAIL {payload['n_fail']}** · "
        f"**THIN {payload['n_thin']}**. N={payload['n_tickers']} "
        f"({meta.get('n_discovery', '?')} discovery) cannot clear the "
        f"50-ticker ship bar — THIN is the honest ceiling this cycle, not a keep.",
        "",
        "| verdict | def | clock | side | exit | n | avg net | t | win | tickers | "
        "early | late | spy↑ | spy↓ | disc | hold |",
        "|---|---|---|---|---|---:|---:|---:|---:|---:|---|---|---|---|---|---|",
    ]
    shown = rows[:40]
    for r in shown:
        L.append(
            f"| {r['verdict']} | `{r['def']}` | {r['clock']} | {r['side']} | "
            f"{r['exit']} | {r['n']} | {r['avg_net']*100:+.2f}% | {r['t']:.1f} | "
            f"{r['win']:.0%} | {r['n_tickers']} | {fmt_blk(r['early'])} | "
            f"{fmt_blk(r['late'])} | {fmt_blk(r['spy_up'])} | "
            f"{fmt_blk(r['spy_dn'])} | {fmt_blk(r['discovery'])} | "
            f"{fmt_blk(r['holdout'])} |"
        )
    L += [
        "",
        f"Tickers: {', '.join(payload['sample_tickers'])}.",
        "",
        "Research only. Live frozen.",
        "",
    ]
    text = "\n".join(L)
    for path in (OUT_MD, SB_MD):
        os.makedirs(os.path.dirname(path), exist_ok=True)
        open(path, "w").write(text + "\n")
    print(f"tickers={len(files)} rows={len(rows)} "
          f"PASS={payload['n_pass']} FAIL={payload['n_fail']} "
          f"THIN={payload['n_thin']}")
    print(f"wrote {OUT_MD}")


if __name__ == "__main__":
    main()
