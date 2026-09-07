"""First A–JL cell/formula cut (War room deliverable).

Uses clock_map.json: fill gates at fill_mine, value gates at value_mine.
Unknown → close. core_score-like → CLOSE only. Sleeve holds 1/2/3.
Futubull costs labeled. Lottery filter. Both-tape split required for PASS.

  python engine/mine_formula_cut.py
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
from classify_clocks import feature_clock  # noqa: E402
from clock import COST_FUTU_LONG, COST_FUTU_SHORT, SHIP, hold_exit_idx  # noqa: E402
from mine_clock import apply_hold1_sibling  # noqa: E402
from signals import classify_fill  # noqa: E402

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
REPO = os.path.dirname(ROOT)
SAMPLE = os.environ.get("ALL_COLS_DIR", os.path.join(ROOT, "research", "all_cols_sample"))
MAP = os.path.join(ROOT, "research", "clock_map.json")
OUT_MD = os.path.join(ROOT, "research", "FORMULA_CUT.md")
OUT_JSON = os.path.join(ROOT, "research", "formula_cut.json")
SB = os.path.join(REPO, "03_scoreboard", "EXCEL_BOT_MINE.md")
HOLDS = (1, 2, 3)
SKIP_VALUE = set("A IR P1 O1 W".split())  # date / ticker / text


def s2d(n):
    return (datetime(1899, 12, 30) + timedelta(days=float(n))).date()


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


def sim(days, ei, side, clock, hold_n):
    last = len(days) - 1
    need = ei + (hold_n - 1 if clock == "open" else hold_n)
    if need > last:
        return None
    k = hold_exit_idx(ei, hold_n, clock, last)
    entry = days[ei]["open"] if clock == "open" else days[ei]["close"]
    ex = days[k]["close"]
    if not entry or not ex:
        return None
    return (ex - entry) / entry * side


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
    n, s, sq, w, mx, pos = slot
    if n < 2:
        return None
    m = s / n
    var = max(sq - s * s / n, 0.0) / (n - 1)
    t = m / math.sqrt(var / n) if var > 0 else 0.0
    return {"n": n, "avg_net": m, "t": t, "win": w / n}


def lottery_ok(n, s, mx, pos):
    if n < 3:
        return False
    frac = (mx / pos) if (pos > 0 and mx > 0) else 0.0
    trimmed = (s - mx) / (n - 1)
    return frac <= SHIP["max_trade_frac"] and trimmed > 0, frac


def clocks_from_map(inv):
    fill, value = {}, {}
    for rec in inv["columns"]:
        fill[rec["col"]] = rec["fill"]
        value[rec["col"]] = rec["value"]
    return fill, value


def pats_from_map(inv):
    """One fill-green/red + value ±1 per column, clocked leak-free."""
    fill, value = clocks_from_map(inv)
    pats = []
    for rec in inv["columns"]:
        col = rec["col"]
        fc = feature_clock(fill, value, col, "fill")
        vc = feature_clock(fill, value, col, "value")
        pats.append((f"{col}_fill_green", col, "fill_green", 1, fc))
        pats.append((f"{col}_fill_red", col, "fill_red", -1, fc))
        if col not in SKIP_VALUE and col not in ("IR", "IS", "IT", "IU", "IV", "IW"):
            pats.append((f"{col}_ge1", col, "ge1", 1, vc))
            pats.append((f"{col}_le-1", col, "le-1", -1, vc))
    # landmine named defs — CLOSE only
    pats.append(("core_score_ge2", "AJ", "core_ge2", 1, "close"))
    pats.append(("core_score_le-2", "AJ", "core_le-2", -1, "close"))
    return pats


def hit(kind, col, day):
    cells = day["cells"]
    if kind == "fill_green":
        return fam(cells.get(col)) == "green"
    if kind == "fill_red":
        return fam(cells.get(col)) == "red"
    if kind == "ge1":
        v = num(cells.get(col))
        return v is not None and v >= 1
    if kind == "le-1":
        v = num(cells.get(col))
        return v is not None and v <= -1
    if kind == "core_ge2":
        return sum(score_fill(cells.get(c)) for c in "ABCDEFGHIJ") >= 2
    if kind == "core_le-2":
        return sum(score_fill(cells.get(c)) for c in "ABCDEFGHIJ") <= -2
    return False


def pack(name, clock, side, rule, cell, cost_model="futubull"):
    d, h = _blk(cell["disc"]), _blk(cell["hold"])
    if not d or d["n"] < 80:
        return None
    early, late = _blk(cell["early"]), _blk(cell["late"])
    sl = cell["disc"]
    lot_ok, lot_frac = lottery_ok(sl[0], sl[1], sl[4], sl[5])
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
    if not lot_ok:
        reasons.append("lottery")
    tape_ok = False
    if early and late and early["n"] >= 40 and late["n"] >= 40:
        tape_ok = early["avg_net"] > 0 and late["avg_net"] > 0
        if not tape_ok:
            reasons.append("tape_split")
    else:
        reasons.append("tape_thin")
    n_t = len(cell["tickers"])
    if n_t < 50 or d["n"] < 80:
        verdict = "THIN"
    elif not reasons:
        verdict = "PASS"
    else:
        verdict = "FAIL"
    return {
        "def": name, "clock": clock, "side": side, "exit": rule,
        "cohort": "ALL", "cost_model": cost_model, "n_tickers": n_t,
        "discovery": d, "holdout": h, "early": early, "late": late,
        "lottery_frac": lot_frac, "verdict": verdict,
        "fail_reasons": reasons, "tape_ok": tape_ok,
        "live_untouched": "flatten_robust",
    }


def fmt(b):
    if not b:
        return "—"
    return f"{b['n']}/{b['avg_net']*100:+.2f}%/t={b['t']:.1f}"


def main():
    os.chdir(ROOT)
    inv = json.load(open(MAP))
    split = json.load(open(os.path.join(HERE, "holdout_split.json")))
    disc, hold = set(split["discovery"]), set(split["holdout"])
    spy = load_spy()
    pats = pats_from_map(inv)
    files = sorted(f for f in glob.glob(os.path.join(SAMPLE, "*.json"))
                   if not os.path.basename(f).startswith("_"))
    live = set()
    for path in files[:12]:
        days = json.load(open(path))["days"]
        for day in days:
            for let, rec in day["cells"].items():
                if fam(rec) in ("green", "red"):
                    live.add(let)
                v = num(rec)
                if v is not None and abs(v) >= 1:
                    live.add(let)
    live.update("ABCDEFGHIJ")
    pats = [p for p in pats if p[1] in live or p[2].startswith("core_")]
    print(f"[formula_cut] files={len(files)} pats={len(pats)} live_cols={len(live)}",
          flush=True)
    cells = defaultdict(lambda: {
        "disc": _slot(), "hold": _slot(),
        "early": _slot(), "late": _slot(),
        "tickers": set(),
    })
    for i, path in enumerate(files, 1):
        g = json.load(open(path))
        t = g["ticker"]
        days = g["days"]
        split_t = ("discovery" if t in disc else "holdout" if t in hold else None)
        if split_t is None:
            continue
        for ei, day in enumerate(days):
            iso = str(s2d(day["date"]))
            half = "early" if iso < "2026-05-01" else "late"
            for name, col, kind, side, clock in pats:
                if clock == "open" and "core_score" in name:
                    raise ValueError("core_score open")
                if not hit(kind, col, day):
                    continue
                for h in HOLDS:
                    raw = sim(days, ei, side, clock, h)
                    if raw is None:
                        continue
                    cost = COST_FUTU_LONG if side == 1 else COST_FUTU_SHORT
                    net = raw - cost
                    key = (name, clock, "long" if side == 1 else "short", f"hold{h}")
                    c = cells[key]
                    _push(c["disc"] if split_t == "discovery" else c["hold"], net)
                    _push(c[half], net)
                    c["tickers"].add(t)
        if i % 25 == 0:
            print(f"  ... {i}/{len(files)}", flush=True)

    rows = []
    for (name, clock, side, rule), cell in cells.items():
        row = pack(name, clock, side, rule, cell)
        if row:
            rows.append(row)
    apply_hold1_sibling(rows)
    rows.sort(key=lambda r: (
        0 if r["verdict"] == "PASS" else 1 if r["verdict"] == "FAIL" else 2,
        -((r["holdout"] or {}).get("t") or -9),
    ))
    n_pass = sum(1 for r in rows if r["verdict"] == "PASS")
    n_fail = sum(1 for r in rows if r["verdict"] == "FAIL")
    n_thin = sum(1 for r in rows if r["verdict"] == "THIN")
    keep = [r for r in rows if r["verdict"] == "PASS"]
    scored = [r for r in rows if r["verdict"] != "THIN"]
    L = [
        "# First A–JL cell/formula cut",
        "",
        f"_Generated {date.today()} · live `flatten_robust` is not changed. "
        f"No merge without Cyrus._",
        "",
        "## Protocol",
        "",
        "- Clocks from `CLOCK_MAP.md` / `clock_map.json` (timing_test + "
        "formula deps). Unknown → **close**. `core_score` = **CLOSE**.",
        "- Sleeve-native holds **1 / 2 / 3**. Cost = **futubull** "
        "0.15% long / 0.20% short (labeled on every row).",
        "- Lottery: one trade ≤25% of gross wins; drop-best mean >0.",
        "- PASS needs ship bar **and** both tape halves avg>0 (n≥40 each).",
        "",
        f"Sample **{len(files)}** tickers. Patterns **{len(pats)}**. "
        f"Cells scored **{len(rows)}**. "
        f"**PASS {n_pass}** · **FAIL {n_fail}** · **THIN {n_thin}**.",
        "",
    ]
    if not keep:
        L += [
            "## Clean null",
            "",
            "No cell/formula cleared n + effect + both-tape + lottery + "
            "hold1-sibling on this 185-ticker A–JL sample. That is the "
            "honest first-cut result, not a keep.",
            "",
            "A prior pass listed 20 open-entry PASSes on unmeasured fills "
            "(GU/GW/HC/GY/HD/CP). Those were demoted: only A–O fills have "
            "a `timing_test` license to enter at open. At close they do "
            "not clear the bar (the same-day color *is* the move).",
            "",
        ]
    else:
        L += ["## PASS", "",
              "| def | clock | side | exit | cost | disc | hold | early | late | tickers |",
              "|---|---|---|---|---|---|---|---|---|---:|"]
        for r in keep:
            L.append(
                f"| `{r['def']}` | {r['clock']} | {r['side']} | {r['exit']} | "
                f"{r['cost_model']} | {fmt(r['discovery'])} | "
                f"{fmt(r['holdout'])} | {fmt(r['early'])} | "
                f"{fmt(r['late'])} | {r['n_tickers']} |"
            )
        L.append("")
    near = [r for r in scored
            if r.get("holdout") and r["holdout"]["t"] >= 2
            and r["discovery"]["avg_net"] > 0
            and r["clock"] == r["clock"]]
    L += ["## Near (holdout t≥2, disc avg>0, still FAIL)", ""]
    if not near:
        L += ["*(none)*", ""]
    else:
        L += ["| def | clock | side | exit | disc | hold | early | late | why |",
              "|---|---|---|---|---|---|---|---|---|"]
        for r in near[:20]:
            L.append(
                f"| `{r['def']}` | {r['clock']} | {r['side']} | {r['exit']} | "
                f"{fmt(r['discovery'])} | {fmt(r['holdout'])} | "
                f"{fmt(r['early'])} | {fmt(r['late'])} | "
                f"{','.join(r['fail_reasons'])} |"
            )
        L.append("")
    L += ["Research only. No cards. No live wire.", ""]
    slim = lambda r: {k: r[k] for k in (
        "def", "clock", "side", "exit", "cost_model", "n_tickers",
        "discovery", "holdout", "early", "late", "verdict",
        "fail_reasons", "tape_ok", "live_untouched",
    )}
    payload = {
        "generated": str(date.today()),
        "n_tickers": len(files),
        "n_pats": len(pats),
        "n_pass": n_pass, "n_fail": n_fail, "n_thin": n_thin,
        "cost_model": "futubull",
        "holds": list(HOLDS),
        "keepers": [slim(r) for r in keep],
        "near": [slim(r) for r in near[:30]],
        "clean_null": n_pass == 0,
        "live_untouched": "flatten_robust",
        "core_score_entry": "close",
    }
    return "\n".join(L) + "\n", payload


if __name__ == "__main__":
    md, payload = main()
    open(OUT_MD, "w").write(md)
    json.dump(payload, open(OUT_JSON, "w"), indent=1)
    # Prefix standing scoreboard with this cut; keep A–JL table below.
    standing = (
        f"# Excel emulator — clock map + first formula cut\n\n"
        f"_Generated {date.today()} · live `flatten_robust` frozen. "
        f"No merge._\n\n"
        f"1. Column clocks: `excel_bot/research/CLOCK_MAP.md` "
        f"(timing_test + formula deps; unknown → close).\n"
        f"2. First cell/formula cut: **PASS {payload['n_pass']}** · "
        f"**FAIL {payload['n_fail']}** · **THIN {payload['n_thin']}** · "
        f"cost=futubull · holds 1/2/3. "
        f"{'Clean null.' if payload['clean_null'] else 'See FORMULA_CUT.md.'}\n\n"
        f"A–O is a parallel thin track (`AO_FIRST_MINE.md`). "
        f"A–JL sample table: `ALL_COLS_MINE.md`.\n\n"
    )
    open(SB, "w").write(standing + md)
    print(f"PASS={payload['n_pass']} FAIL={payload['n_fail']} "
          f"THIN={payload['n_thin']} null={payload['clean_null']}")
