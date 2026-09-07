"""Mine the WHOLE Excel emulator (A–JL), not the stored A–O strip.

PIT:
  open — A-keyed fills; yesterday's deeper state + today's A
  close — any same-day deeper value/fill; core_score (A..J includes D,E,F,H,I)
Never core_score at open. Sleeve holds 1/2/3/5/8. Ship bar + uncond baseline.

Research only. Live flatten_robust is not changed.
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
from clock import (  # noqa: E402
    COST_FUTU_LONG, COST_FUTU_SHORT, SHIP, hold_exit_idx,
)
from mine_clock import apply_horizon_sibling  # noqa: E402
from signals import classify_fill  # noqa: E402

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
REPO = os.path.dirname(ROOT)
SAMPLE = os.environ.get("ALL_COLS_DIR", os.path.join(ROOT, "research", "all_cols_sample"))
OUT_MD = os.path.join(ROOT, "research", "ALL_COLS_MINE.md")
SB_MD = os.path.join(REPO, "03_scoreboard", "EXCEL_BOT_MINE.md")
CYCLE_MD = os.path.join(ROOT, "research", "MINE_CYCLE.md")
OUT_JSON = os.path.join(ROOT, "research", "all_cols_mine.json")
AO_MD = os.path.join(ROOT, "research", "AO_FIRST_MINE.md")
HOLDS = (1, 2, 3, 5, 8)

# Same-day deeper / composites → CLOSE. Timing untested past A–O.
VALUE_CLOSE = [
    ("L_ge1", "L", ">=", 1, 1),
    ("L_le-1", "L", "<=", -1, -1),
    ("L_le-3", "L", "<=", -3, -1),
    ("O_ge1", "O", ">=", 1, 1),
    ("O_le-1", "O", "<=", -1, -1),
    ("EL_ge2", "EL", ">=", 2, 1),
    ("EL_le-2", "EL", "<=", -2, -1),
    ("V_le-1", "V", "<=", -1, -1),
    ("AD_ge1", "AD", ">=", 1, 1),
    ("AD_le-1", "AD", "<=", -1, -1),
    ("JA_eq1", "JA", "==", 1, 1),
    ("IZ_eq1", "IZ", "==", 1, 1),
    ("DD_ge2", "DD", ">=", 2, 1),
    ("CP_ge1", "CP", ">=", 1, 1),
    ("CP_le-1", "CP", "<=", -1, -1),
    ("HN_ge3", "HN", ">=", 3, 1),
    ("HN_le-2", "HN", "<=", -2, -1),
    ("IB_ge3", "IB", ">=", 3, 1),
    ("T_ge2", "T", ">=", 2, 1),
    ("AA_le-1", "AA", "<=", -1, -1),
]
# Past-O CF fills mined as close (untested clock).
FILL_CLOSE = [
    "EL", "P", "AA", "CU", "CV", "HF", "HB", "HI", "HH",
    "GR", "GZ", "GU", "HD", "HS", "FP", "GG",
]
# A fill may enter at open. core_score = A..J → close only.
OPEN_A = [("A_green", "A", "green", 1), ("A_red", "A", "red", -1)]
LAG_OPEN = [
    ("lag_Lge1_Agreen", "L", ">=", 1, "A_green", 1),
    ("lag_Lle-1_Ared", "L", "<=", -1, "A_red", -1),
    ("lag_ELge2_Agreen", "EL", ">=", 2, "A_green", 1),
    ("lag_ELle-2_Ared", "EL", "<=", -2, "A_red", -1),
    ("lag_JAge1_Agreen", "JA", "==", 1, "A_green", 1),
    ("lag_IZeq1_Agreen", "IZ", "==", 1, "A_green", 1),
]


def _pats():
    out = []
    out.append(("A_green", 1, "open"))
    out.append(("A_red", -1, "open"))
    for name, _c, _op, _th, side in VALUE_CLOSE:
        out.append((name, side, "close"))
    for col in FILL_CLOSE:
        out.append((f"{col}_fill_green", 1, "close"))
        out.append((f"{col}_fill_red", -1, "close"))
    out.append(("deeper_g5", 1, "close"))
    out.append(("deeper_r5", -1, "close"))
    out.append(("core_score_ge2", 1, "close"))
    out.append(("core_score_le-2", -1, "close"))
    for name, _c, _op, _th, _a, side in LAG_OPEN:
        out.append((name, side, "open"))
    return out


PATS = _pats()


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
    p = os.path.join(ROOT, "research", "spy_tape.json")
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


def score_fill(cell):
    if not cell:
        return 0.0
    return classify_fill(cell.get("f"))[1]


def _cmp(v, op, thresh):
    if v is None:
        return False
    if op == ">=":
        return v >= thresh
    if op == "<=":
        return v <= thresh
    if op == "==":
        return v == thresh
    return False


def gates(day, prev=None):
    cells = day["cells"]
    g = {}
    a_fam = fam(cells.get("A"))
    g["A_green"] = a_fam == "green"
    g["A_red"] = a_fam == "red"
    for name, col, op, thresh, _side in VALUE_CLOSE:
        g[name] = _cmp(num(cells.get(col)), op, thresh)
    for col in FILL_CLOSE:
        f = fam(cells.get(col))
        g[f"{col}_fill_green"] = f == "green"
        g[f"{col}_fill_red"] = f == "red"
    deeper_g = deeper_r = 0
    core = 0.0
    for let, rec in cells.items():
        if len(let) == 1 and let in "ABCDEFGHIJ":
            core += score_fill(rec)
        if let in "ABCDEFGHIJKLMNO":
            continue
        f = fam(rec)
        if f == "green":
            deeper_g += 1
        elif f == "red":
            deeper_r += 1
    g["deeper_g5"] = deeper_g >= 5
    g["deeper_r5"] = deeper_r >= 5
    g["core_score_ge2"] = core >= 2
    g["core_score_le-2"] = core <= -2
    if prev:
        pc = prev["cells"]
        for name, col, op, thresh, akey, _side in LAG_OPEN:
            g[name] = _cmp(num(pc.get(col)), op, thresh) and g.get(akey, False)
    else:
        for name, *_rest in LAG_OPEN:
            g[name] = False
    return g


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
    """Compat helper (tests). Full ship bar is applied in pack_row."""
    if n_tickers < 50 or n < 80:
        return "THIN"
    if avg <= 0 or (t == t and t < 2):
        return "FAIL"
    if early_s is not None and late_s is not None and early_s * late_s <= 0:
        return "FAIL"
    if t >= 3 and n >= 300 and n_tickers >= 50:
        return "PASS"
    return "FAIL"


def _slot():
    return [0, 0.0, 0.0, 0, -1e9, 0.0]


def _push(slot, net):
    slot[0] += 1
    slot[1] += net
    slot[2] += net * net
    if net > 0:
        slot[3] += 1
        slot[5] += net
    if net > slot[4]:
        slot[4] = net


def _blk(slot):
    if isinstance(slot, list):
        n, s, sq, w, mx, pos = slot
    else:
        return None
    if n < 2:
        return None
    m = s / n
    var = max(sq - s * s / n, 0.0) / (n - 1)
    t = m / math.sqrt(var / n) if var > 0 else 0.0
    return {"n": n, "avg_net": m, "t": t, "win": w / n}


def lottery_stats(n, s, mx, pos):
    if n < 3:
        return True, 1.0, float("nan")
    frac = (mx / pos) if (pos > 0 and mx > 0) else 0.0
    trimmed = (s - mx) / (n - 1)
    return frac > SHIP["max_trade_frac"] or trimmed <= 0, frac, trimmed


def pack_row(name, clock, side, rule, cell, baselines):
    d, h = _blk(cell["disc"]), _blk(cell["hold"])
    if not d or d["n"] < 80:
        return None
    early, late = _blk(cell["early"]), _blk(cell["late"])
    up, dn = _blk(cell["spy_up"]), _blk(cell["spy_dn"])
    hn = int(rule[4:])
    b = baselines.get(f"{clock}_{side}_h{hn}")
    sl = cell["disc"]
    lot_bad, lot_frac, trimmed = lottery_stats(sl[0], sl[1], sl[4], sl[5])
    reasons = []
    if d["n"] < SHIP["disc_n"]:
        reasons.append("thin_disc")
    if not h or h["n"] < SHIP["hold_n"]:
        reasons.append("thin_hold")
    if d["t"] < SHIP["disc_t"]:
        reasons.append("disc_t")
    if h and h["t"] < SHIP["hold_t"]:
        reasons.append("hold_t")
    if d["avg_net"] <= 0:
        reasons.append("disc_sign")
    if h and h["avg_net"] <= 0:
        reasons.append("hold_sign")
    if len(cell["tickers"]) < SHIP["n_tickers"]:
        reasons.append("ticker_bar")
    if len(cell["dates"]) < SHIP["n_dates"]:
        reasons.append("date_bar")
    if lot_bad:
        reasons.append("lottery")
    if early and late and early["n"] >= 40 and late["n"] >= 40:
        if late["avg_net"] <= 0 or (early["avg_net"] > 0) != (late["avg_net"] > 0):
            reasons.append("tape_split")
    if up and dn and up["n"] >= 40 and dn["n"] >= 40:
        if up["avg_net"] <= 0 or dn["avg_net"] <= 0:
            reasons.append("spy_regime")
    cmp = h if h else d
    if b and cmp and cmp["avg_net"] < b["avg_net"] + 0.002:
        reasons.append("no_edge_vs_uncond")
    n_t = len(cell["tickers"])
    if n_t < 50 or d["n"] < 80:
        verdict = "THIN"
    elif not reasons:
        verdict = "PASS"
    else:
        verdict = "FAIL"
    return {
        "def": name, "clock": clock, "side": side, "exit": rule,
        "cohort": "ALL", "cost_model": "futubull",
        "n_tickers": n_t, "n_dates": len(cell["dates"]),
        "discovery": d, "holdout": h, "early": early, "late": late,
        "spy_up": up, "spy_dn": dn, "baseline": b,
        "lottery_frac": lot_frac, "trimmed_avg": trimmed,
        "verdict": verdict, "fail_reasons": reasons,
        "live_untouched": "flatten_robust",
    }


def fmt_blk(b):
    if not b:
        return "—"
    key = "avg_net" if "avg_net" in b else "avg"
    return f"{b['n']}/{b[key]*100:+.2f}%/t={b['t']:.1f}"


def main():
    os.chdir(ROOT)
    split = json.load(open(os.path.join(HERE, "holdout_split.json")))
    disc, hold = set(split["discovery"]), set(split["holdout"])
    spy = load_spy()
    files = sorted(f for f in glob.glob(os.path.join(SAMPLE, "*.json"))
                   if not os.path.basename(f).startswith("_"))
    print(f"[mine_all_cols] files={len(files)} pats={len(PATS)} spy={len(spy)}",
          flush=True)
    cells = defaultdict(lambda: {
        "disc": _slot(), "hold": _slot(),
        "early": _slot(), "late": _slot(),
        "spy_up": _slot(), "spy_dn": _slot(),
        "tickers": set(), "dates": set(),
    })
    base = defaultdict(lambda: _slot())
    for i, path in enumerate(files, 1):
        g = json.load(open(path))
        t = g["ticker"]
        days = g["days"]
        split_t = ("discovery" if t in disc else "holdout" if t in hold else None)
        last = len(days) - 1
        for ei, day in enumerate(days):
            for clock, side in (("open", 1), ("close", 1),
                                ("open", -1), ("close", -1)):
                dummy = ei
                for h in HOLDS:
                    raw = sim(days, dummy, side, clock, h)
                    if raw is None:
                        continue
                    cost = COST_FUTU_LONG if side == 1 else COST_FUTU_SHORT
                    _push(base[(clock, "long" if side == 1 else "short", h)],
                          raw - cost)
        prev = None
        for ei, day in enumerate(days):
            gts = gates(day, prev)
            iso = str(s2d(day["date"]))
            tape = spy.get(iso, 0)
            half = "early" if iso < "2026-05-01" else "late"
            for name, side, clock in PATS:
                if not gts.get(name):
                    continue
                if clock == "open" and "core_score" in name:
                    raise ValueError("core_score cannot enter open")
                for h in HOLDS:
                    raw = sim(days, ei, side, clock, h)
                    if raw is None:
                        continue
                    cost = COST_FUTU_LONG if side == 1 else COST_FUTU_SHORT
                    net = raw - cost
                    if split_t is None:
                        continue
                    key = (name, clock, "long" if side == 1 else "short", f"hold{h}")
                    c = cells[key]
                    _push(c["disc"] if split_t == "discovery" else c["hold"], net)
                    _push(c[half], net)
                    if tape == 1:
                        _push(c["spy_up"], net)
                    elif tape == -1:
                        _push(c["spy_dn"], net)
                    c["tickers"].add(t)
                    c["dates"].add(iso)
            prev = day
        if i % 25 == 0:
            print(f"  ... {i}/{len(files)}", flush=True)

    baselines = {}
    for k, sl in base.items():
        b = _blk(sl)
        if b:
            baselines[f"{k[0]}_{k[1]}_h{k[2]}"] = b

    rows = []
    for (name, clock, side, rule), cell in cells.items():
        row = pack_row(name, clock, side, rule, cell, baselines)
        if row:
            rows.append(row)
    apply_horizon_sibling(rows)
    rows.sort(key=lambda r: (
        0 if r["verdict"] == "PASS" else 1 if r["verdict"] == "FAIL" else 2,
        -((r["holdout"] or {}).get("t") or -9),
    ))
    return rows, baselines, files


def render(rows, baselines, files):
    meta_path = os.path.join(SAMPLE, "_meta.json")
    meta = json.load(open(meta_path)) if os.path.exists(meta_path) else {}
    tickers = [os.path.basename(f)[:-5] for f in files]
    n_pass = sum(1 for r in rows if r["verdict"] == "PASS")
    n_fail = sum(1 for r in rows if r["verdict"] == "FAIL")
    n_thin = sum(1 for r in rows if r["verdict"] == "THIN")
    spt = meta.get("minutes_per_ticker")
    sec = round(spt * 60, 2) if spt else None
    keep = [r for r in rows if r["verdict"] == "PASS"]
    primary = [r for r in keep if r["exit"] in ("hold1", "hold2")]
    L = [
        "# Excel A–JL (whole emulator) sample mine",
        "",
        f"_Generated {date.today()} · live `flatten_robust` is not changed. "
        f"No merge without Cyrus._",
        "",
        "## This is the whole Excel, not A–O",
        "",
        "`model.json` already covers A..JL (275 cols). Stored daily grids "
        "only persist A–O fills — that is a **storage gap**, not a missing "
        "emulator. `run.py --all-cols` dumps all 275. This sample rebuilds "
        "A–JL from excel-state rows (lean path) and mines under PIT.",
        "",
        "### Cost",
        "",
        f"- Lean rows-cache capture (this sample): **{sec} s/ticker** "
        f"({spt} min/ticker) · rows 2–145 · 275 cols. "
        f"N={len(files)} → ~{round((spt or 0)*len(files), 2)} min.",
        "- `run.py --all-cols` (Yahoo + rows 1–364): minutes/ticker — not "
        "used for this sample. Full 3603 via lean path ≈ 70 min.",
        "",
        "### PIT",
        "",
        "- Open: A-keyed fills; yesterday deeper value + today A.",
        "- Close: same-day deeper values/fills; **core_score** (A..J includes "
        "D,E,F,H,I — landmine, CLOSE only).",
        "- Sleeve holds 1/2/3/5/8. Futubull 0.15%/0.20%. Ship bar + "
        "≥20 bp vs uncond. hold3/5/8 need hold2 edge.",
        "",
        f"Sample **{len(files)}** tickers "
        f"({meta.get('n_disc') or meta.get('n_discovery', '?')} discovery / "
        f"{meta.get('n_hold') or meta.get('n_holdout', '?')} holdout). "
        f"Patterns **{len(PATS)}**. Cells **{len(rows)}**. "
        f"**PASS {n_pass}** · **FAIL {n_fail}** · **THIN {n_thin}**.",
        "",
        "A–O first mine is a **parallel thin track** "
        "(`AO_FIRST_MINE.md`) — not a substitute for this surface.",
        "",
        "### Unconditional baseline (sample, futubull)",
        "",
        "| clock | side | hold | n | avg net | t | win |",
        "|---|---|---:|---:|---:|---:|---:|",
    ]
    for clock in ("open", "close"):
        for side in ("long", "short"):
            for h in HOLDS:
                b = baselines.get(f"{clock}_{side}_h{h}")
                if not b:
                    continue
                L.append(f"| {clock} | {side} | {h} | {b['n']} | "
                         f"{b['avg_net']*100:+.2f}% | {b['t']:.1f} | "
                         f"{b['win']:.0%} |")
    L += ["", "### Primary (hold1/2 PASS)", ""]
    if not primary:
        L += ["*(none — no sleeve-shaped keeper on this sample)*", ""]
    else:
        L += [
            "| def | clock | side | exit | disc | hold | base | tickers | why |",
            "|---|---|---|---|---|---|---|---:|---|",
        ]
        for r in primary:
            L.append(
                f"| `{r['def']}` | {r['clock']} | {r['side']} | {r['exit']} | "
                f"{fmt_blk(r['discovery'])} | {fmt_blk(r['holdout'])} | "
                f"{fmt_blk(r['baseline'])} | {r['n_tickers']} | "
                f"{','.join(r['fail_reasons']) or '—'} |"
            )
        L.append("")
    L += ["### Top cells (PASS then FAIL then THIN)", ""]
    L += [
        "| verdict | def | clock | side | exit | disc | hold | base | "
        "tickers | why |",
        "|---|---|---|---|---|---|---|---|---:|---|",
    ]
    for r in rows[:45]:
        L.append(
            f"| {r['verdict']} | `{r['def']}` | {r['clock']} | {r['side']} | "
            f"{r['exit']} | {fmt_blk(r['discovery'])} | "
            f"{fmt_blk(r['holdout'])} | {fmt_blk(r['baseline'])} | "
            f"{r['n_tickers']} | {','.join(r['fail_reasons']) or '—'} |"
        )
    L += [
        "",
        f"Tickers: {', '.join(tickers)}.",
        "",
        "Research only. Live frozen. No strategy cards.",
        "",
    ]
    slim = lambda r: {k: r[k] for k in (
        "def", "clock", "side", "exit", "n_tickers", "n_dates",
        "discovery", "holdout", "baseline", "verdict", "fail_reasons",
        "live_untouched",
    ) if k in r}
    payload = {
        "generated": str(date.today()),
        "sample_tickers": tickers,
        "n_tickers": len(files),
        "n_discovery": meta.get("n_disc") or meta.get("n_discovery"),
        "n_holdout": meta.get("n_hold") or meta.get("n_holdout"),
        "minutes_per_ticker": spt,
        "n_pats": len(PATS),
        "n_rows": len(rows),
        "n_pass": n_pass, "n_fail": n_fail, "n_thin": n_thin,
        "baselines": baselines,
        "keepers": [slim(r) for r in keep],
        "primary": [slim(r) for r in primary],
        "cells": [slim(r) for r in rows],
        "live_untouched": "flatten_robust",
        "surface": "A-JL",
        "ao_is_not_whole_excel": True,
    }
    return "\n".join(L) + "\n", payload


def render_standing(all_md, payload):
    ao_note = ""
    if os.path.exists(AO_MD):
        ao_note = (
            "A–O parallel thin track is in `AO_FIRST_MINE.md` "
            "(L3/S1 FAIL; 6 hysteresis hold1/2 research candidates). "
            "**A–O-only is not the whole Excel.**"
        )
    return "\n".join([
        "# Excel emulator mine — whole workbook (A–JL)",
        "",
        f"_Generated {date.today()} · live `flatten_robust` is not changed. "
        f"No merge without Cyrus._",
        "",
        "## Priority (Cyrus override)",
        "",
        "Mine the **whole emulator** (A..JL, 275 cols). Stored daily grids "
        "only persist A–O fills — that is the gap, not a missing model. "
        "`model.json` already has max_col 275. `run.py --all-cols` dumps it.",
        "",
        f"A–JL sample: **{payload['n_tickers']}** tickers · "
        f"**PASS {payload['n_pass']}** · **FAIL {payload['n_fail']}** · "
        f"**THIN {payload['n_thin']}** · "
        f"{payload.get('minutes_per_ticker') and round(payload['minutes_per_ticker']*60, 2)} s/ticker.",
        "",
        ao_note,
        "",
        "Full A–JL table: `ALL_COLS_MINE.md`. Research only. No live wire.",
        "",
    ]) + "\n"


if __name__ == "__main__":
    rows, baselines, files = main()
    md, payload = render(rows, baselines, files)
    standing = render_standing(md, payload)
    open(OUT_MD, "w").write(md)
    # Standing scoreboard leads with A–JL; keep the full table there too.
    open(SB_MD, "w").write(standing + "\n" + md)
    open(CYCLE_MD, "w").write(standing)
    json.dump(payload, open(OUT_JSON, "w"), indent=1)
    print(f"tickers={payload['n_tickers']} rows={payload['n_rows']} "
          f"PASS={payload['n_pass']} FAIL={payload['n_fail']} "
          f"THIN={payload['n_thin']}")
    print(f"wrote {OUT_MD}")
