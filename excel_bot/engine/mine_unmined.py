"""Sweep remaining A–JL families the first all-cols cut never mined.

Prior N=499 all-cols only scored a short hand list (L/O/EL/V/AD/JA/IZ/DD/
CP/HN/IB/T/AA values; 16 close fills; A green/red; a few lags; deeper_g5;
core_score). This beat inventories the rest and mines NEW families:

  * open-knowable *values* past A/C/J (clock_map value_mine_open)
  * unmined CF / formula *values* at close
  * unmined fill greens/reds (unknown → close)
  * 0/1 formula flags

PIT: open entry only on open-knowable signals. Close-knowable enter at close.
Futubull 0.15%/0.20%. Holds 1/2/5. Ship bar + Q3 (2026-07-01) + top-day
lottery + beat uncond ≥20 bp. A–F seed is Yahoo/rows only.

Research only. Live flatten_robust is not imported or changed.

  python engine/mine_unmined.py
"""
from __future__ import annotations

import argparse
import glob
import json
import math
import os
import sys
from collections import defaultdict
from datetime import date, datetime, timedelta

from openpyxl.utils import get_column_letter

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from clock import (  # noqa: E402
    COST_FUTU_LONG, COST_FUTU_SHORT, SHIP, hold_exit_idx,
)
from harden_hyst_open import lottery_day, splice_md  # noqa: E402
from mine_all_cols import (  # noqa: E402
    FILL_CLOSE, VALUE_CLOSE,
)
from mine_clock import apply_horizon_sibling  # noqa: E402
from signals import classify_fill  # noqa: E402

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
REPO = os.path.dirname(ROOT)
SAMPLE = os.environ.get("ALL_COLS_DIR", os.path.join(ROOT, "research", "all_cols_sample"))
CLOCK_MAP = os.path.join(ROOT, "research", "clock_map.json")
INV_JSON = os.path.join(ROOT, "research", "grid_inventory.json")
SPLIT_PATH = os.path.join(HERE, "holdout_split.json")
OUT_MD = os.path.join(ROOT, "research", "UNMINED_SWEEP.md")
OUT_JSON = os.path.join(ROOT, "research", "unmined_sweep.json")
INV_MD = os.path.join(ROOT, "research", "UNMINED_INVENTORY.md")
INV_OUT_JSON = os.path.join(ROOT, "research", "unmined_inventory.json")
SB_MD = os.path.join(REPO, "03_scoreboard", "EXCEL_BOT_MINE.md")
CYCLE_MD = os.path.join(ROOT, "research", "MINE_CYCLE.md")
AO_MD = os.path.join(ROOT, "research", "AO_FIRST_MINE.md")
ALL_COLS_MD = os.path.join(ROOT, "research", "ALL_COLS_MINE.md")
MARKER = "## Remaining A–JL families (unmined sweep)"
HALF_CUT = "2026-05-01"
Q3_CUT = "2026-07-01"
HOLDS = (1, 2, 5)
BEAT = 0.002

# OHLCV + STOCKHISTORY aliases — not a "new family".
SKIP_OHLC = set("ABCDEF") | {"IR", "IS", "IT", "IU", "IV", "IW"}
# Weekly spill — sparse, not this beat's family.
SKIP_WEEKLY = {"AP", "AQ", "AR", "AS", "AT", "AU"}
# A–O fills already mined on the stored 3603-grid track + first all-cols list.
OLD_FILL = set("ABCGJKLMO") | set(FILL_CLOSE)
# First all-cols value letters (mined at CLOSE). Re-mine at OPEN only if licensed.
OLD_VALUE_CLOSE = {row[1] for row in VALUE_CLOSE}
# score / core / a_score already swept on A–O (PASS 376 / FAIL 1160).
KILLED_SCORE_FAMS = (
    "score", "a_score", "core_score", "row_score", "open_score", "open_core",
    "close_score",
)

VAL_OPS = (
    ("eq1", "==", 1, 1),
    ("ge1", ">=", 1, 1),
    ("ge2", ">=", 2, 1),
    ("gt0", ">", 0, 1),
    ("le-1", "<=", -1, -1),
    ("le-2", "<=", -2, -1),
    ("lt0", "<", 0, -1),
)


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


def _cmp(v, op, thresh):
    if v is None:
        return False
    if op == ">=":
        return v >= thresh
    if op == "<=":
        return v <= thresh
    if op == "==":
        return v == thresh
    if op == ">":
        return v > thresh
    if op == "<":
        return v < thresh
    return False


def load_clocks():
    raw = json.load(open(CLOCK_MAP))
    groups = raw["groups"]
    return {
        "fill_open": set(groups["fill_mine_open"]),
        "value_open": set(groups["value_mine_open"]),
        "value_close": set(groups["value_mine_close"]),
        "counts": raw["counts"],
        "raw": raw,
    }


def all_letters():
    return [get_column_letter(i) for i in range(1, 276)]


def cf_and_formula_cols():
    inv = json.load(open(INV_JSON))
    cf = set("ABCDEFGHIJKLMNO") | set(inv.get("deeper_cf_columns") or [])
    formula = set()
    binary = set()
    for rec in (inv.get("visible_A_to_O") or []) + (inv.get("deeper_sample") or []):
        col = rec.get("col")
        if not col:
            continue
        if rec.get("has_cf"):
            cf.add(col)
        if (rec.get("n_formula_rows") or 0) >= 8:
            formula.add(col)
        sample = str(rec.get("sample") or "")
        if (rec.get("n_formula_rows") or 0) >= 8 and (
                ", 1, 0)" in sample or ",1, 0)" in sample
                or "1, 0)" in sample or ", 0, 1)" in sample):
            binary.add(col)
    # model.json: any column with many daily formulas, plus 0/1 flags
    model = json.load(open(os.path.join(HERE, "model.json")))
    counts = defaultdict(int)
    bin_hits = defaultdict(int)
    for coord, spec in (model.get("formulas") or {}).items():
        letter = "".join(c for c in coord if c.isalpha())
        if not letter:
            continue
        counts[letter] += 1
        text = spec.get("f") if isinstance(spec, dict) else str(spec)
        if ", 1, 0)" in str(text) or ", 0, 1)" in str(text) or "1, 0)" in str(text):
            bin_hits[letter] += 1
    for letter, n in counts.items():
        if n >= 8:
            formula.add(letter)
        if bin_hits[letter] >= 4:
            binary.add(letter)
    return sorted(cf), sorted(formula), sorted(binary)


def inventory():
    clocks = load_clocks()
    cf, formula, binary = cf_and_formula_cols()
    vo = sorted(c for c in clocks["value_open"]
                if c not in SKIP_OHLC and c not in SKIP_WEEKLY)
    # Open-value letters the first all-cols cut never entered at OPEN.
    val_open_new = [c for c in vo if c not in {"A", "C", "J"}]
    fill_new = sorted(
        c for c in (set(cf) | set(formula))
        if c not in OLD_FILL and c not in SKIP_OHLC and c not in SKIP_WEEKLY
    )
    val_close_new = sorted(
        c for c in clocks["value_close"]
        if c not in OLD_VALUE_CLOSE
        and c not in SKIP_OHLC
        and c not in SKIP_WEEKLY
        and c not in set("ABCGJKLMO")
    )
    flag_new = [c for c in binary
                if c not in SKIP_OHLC and c not in SKIP_WEEKLY]
    families = {
        "val_open": {
            "plain": "Numbers the sheet already knows at the 9:30 open, "
                     "past the three A/C/J values the first mine used.",
            "letters": val_open_new,
            "clock": "open",
            "kind": "value_threshold",
        },
        "val_close": {
            "plain": "Numbers that wait for the close (formula / CF columns "
                     "the 8-def all-cols list skipped).",
            "letters": val_close_new,
            "clock": "close",
            "kind": "value_threshold",
        },
        "fill_new": {
            "plain": "Green / red highlights on letters the first all-cols "
                     "cut never scored. Unknown fill clock mines at close.",
            "letters": fill_new,
            "clock": "fill_mine (unknown→close)",
            "kind": "fill",
        },
        "flag": {
            "plain": "0/1 formula flags (the sheet writes 1 when a condition "
                     "fires). Clock follows the column's value clock.",
            "letters": flag_new,
            "clock": "value_mine",
            "kind": "formula_flag",
        },
        "score_killed": {
            "plain": "score / core_score / a_score / row_score already swept "
                     "on the 3603 A–O grids (FAIL 1160). Not re-mined.",
            "letters": list(KILLED_SCORE_FAMS),
            "clock": "close except open_score/open_core",
            "kind": "already_killed",
        },
    }
    payload = {
        "generated": str(date.today()),
        "live_untouched": "flatten_robust",
        "excel_cache_used": False,
        "seed": "yahoo_rows_cache",
        "prior_all_cols": {
            "n": 499,
            "value_letters": sorted(OLD_VALUE_CLOSE),
            "fill_letters": sorted(OLD_FILL),
            "result": "PASS 0 / FAIL 305 / THIN 0",
        },
        "clock_counts": clocks["counts"],
        "n_cf": len(cf),
        "n_formula": len(formula),
        "families": families,
        "blocked": ["finviz_vol", "ab_tape", "weather", "book"],
        "standing_keeps": [
            "hyst_open_core_e5_x2__O_green",
            "hyst_open_score_e5_x2__O_green",
            "light+green O (3 recipes after Q3)",
        ],
    }
    return payload


def render_inventory(payload):
    fams = payload["families"]
    L = [
        "# Remaining A–JL regions (unmined inventory)",
        "",
        f"_Generated {payload['generated']} · live `flatten_robust` frozen. "
        "Yahoo/rows A–F seed only. No merge._",
        "",
        "## Plain English",
        "",
        "The first whole-Excel dump already looked at **499** names, but only "
        "at a short list of cells (a dozen value letters, sixteen fills, "
        "yesterday+A lags, and a 'five deeper greens' count). That list is "
        "a **clean null** (PASS 0). Most of the sheet was never scored.",
        "",
        "This inventory names the leftover regions. Standing research keeps "
        "stay the **three light + green O** recipes. Finviz volume stays "
        "**BLOCKED**. AB / weather / book stay dead until a long tape exists. "
        "score / core / a_score were already killed on the 3,603 A–O grids.",
        "",
        "## What was already mined (do not repeat)",
        "",
        f"- All-cols N=499 hand list: values "
        f"`{'/'.join(payload['prior_all_cols']['value_letters'])}`; "
        f"fills `{'/'.join(sorted(OLD_FILL))}`. **PASS 0**.",
        "- A–O stored grids: 103 patterns, **PASS 376 / FAIL 1160 / THIN 18** "
        "(hysteresis lights + a few close H/I combos). score/core/a_score "
        "families that missed the ship bar stay dead.",
        "- Color-alone: KILL. Light+green O: KEEP 3 after Q3.",
        "",
        "## Leftover families this beat mines",
        "",
    ]
    for key, spec in fams.items():
        letters = spec["letters"]
        shown = ", ".join(letters[:40])
        extra = f" … +{len(letters) - 40} more" if len(letters) > 40 else ""
        L += [
            f"### {key}",
            "",
            spec["plain"],
            "",
            f"- Clock: **{spec['clock']}**",
            f"- Letters ({len(letters)}): {shown}{extra}",
            "",
        ]
    L += [
        "## Clocks (leak-free)",
        "",
        f"- Fill open (measured + aliases): A B C G J K L M O IR IS IT "
        f"({payload['clock_counts']['fill_mine_open']} letters). "
        "Everything else mines fill at **close**.",
        f"- Value open: {payload['clock_counts']['value_mine_open']} letters "
        "(formula walk). Unknown value → **close**.",
        "- `core_score` = A..J includes D,E,F,H,I → close only (already killed).",
        "",
        "Open entry only on open-knowable signals. Close-knowable need close "
        "entry. A–F seed = Yahoo/rows cache (`seed_anchor`). Never Excel "
        "STOCKHISTORY `--from-cache`.",
        "",
        "Research only. Live frozen.",
        "",
    ]
    return "\n".join(L)


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
    if not isinstance(slot, list) or slot[0] < 2:
        return None
    n, s, sq, w, _mx, _pos = slot
    m = s / n
    var = max(sq - s * s / n, 0.0) / (n - 1)
    t = m / math.sqrt(var / n) if var > 0 else 0.0
    return {"n": n, "avg_net": m, "t": t, "win": w / n}


def family_of(name):
    if name.startswith("valopen_"):
        return "val_open"
    if name.startswith("valclose_"):
        return "val_close"
    if name.startswith("fill_"):
        return "fill_new"
    if name.startswith("flag_"):
        return "flag"
    return "other"


def pack_row(name, clock, side, rule, cell, baselines):
    d, h = _blk(cell["disc"]), _blk(cell["hold"])
    if not d or d["n"] < 80:
        return None
    early, late = _blk(cell["early"]), _blk(cell["late"])
    up, dn = _blk(cell["spy_up"]), _blk(cell["spy_dn"])
    q12, q3 = _blk(cell["q12"]), _blk(cell["q3"])
    hn = int(rule[4:])
    b = baselines.get(f"{clock}_{side}_h{hn}")
    day_items = [{"date": iso, "net": v} for iso, v in cell["day"].items()]
    day_bad, day_frac, top_day, _pnl, n_days = lottery_day(day_items)
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
    if day_bad:
        reasons.append("lottery_day")
    if not early or not late or early["n"] < 40 or late["n"] < 40:
        reasons.append("tape_thin")
    elif late["avg_net"] <= 0 or (early["avg_net"] > 0) != (late["avg_net"] > 0):
        reasons.append("tape_split")
    if not up or not dn or up["n"] < 40 or dn["n"] < 40:
        reasons.append("spy_thin")
    elif up["avg_net"] <= 0 or dn["avg_net"] <= 0:
        reasons.append("spy_regime")
    if not q3 or q3["n"] < 40:
        reasons.append("q3_missing")
    elif q3["avg_net"] <= 0:
        reasons.append("q3_sign")
    if q12 and q3 and q12["n"] >= 40 and q3["n"] >= 40:
        if (q12["avg_net"] > 0) != (q3["avg_net"] > 0):
            reasons.append("q3_split")
    if b and ((d["avg_net"] < b["avg_net"] + BEAT) or
              (h and h["avg_net"] < b["avg_net"] + BEAT)):
        reasons.append("no_edge_vs_uncond")
    n_t = len(cell["tickers"])
    if n_t < 50 or d["n"] < 80:
        verdict = "THIN"
    elif not reasons:
        verdict = "PASS"
    else:
        verdict = "FAIL"
    return {
        "def": name, "family": family_of(name),
        "clock": clock, "side": side, "exit": rule,
        "cohort": "ALL", "cost_model": "futubull",
        "n_tickers": n_t, "n_dates": len(cell["dates"]),
        "discovery": d, "holdout": h, "early": early, "late": late,
        "spy_up": up, "spy_dn": dn, "q12": q12, "q3": q3,
        "baseline": b, "lottery_day_frac": day_frac,
        "lottery_top_day": top_day, "lottery_n_days": n_days,
        "verdict": verdict, "fail_reasons": reasons,
        "live_untouched": "flatten_robust",
    }


def harden_label(row):
    """KEEP / KILL / THIN after the same bar as light+O."""
    if row["verdict"] == "THIN":
        return "THIN"
    if row["verdict"] == "PASS":
        return "KEEP"
    return "KILL"


def _require_hold2_keep(rows):
    """hold5 (and hold1) KEEP only when hold2 itself is PASS on this sample."""
    passed = {(r["def"], r["clock"], r["side"], r["exit"])
              for r in rows if r["verdict"] == "PASS"}
    for r in rows:
        if r["verdict"] != "PASS" or r["exit"] in ("hold2",):
            continue
        sib = (r["def"], r["clock"], r["side"], "hold2")
        if sib not in passed:
            r["verdict"] = "FAIL"
            r["fail_reasons"] = list(r["fail_reasons"]) + ["no_hold2_keep"]


def build_specs(inv_payload, clocks):
    """NEW pattern specs: (name, clock, side, kind, col, extra)."""
    specs = []
    vo = inv_payload["families"]["val_open"]["letters"]
    vc = inv_payload["families"]["val_close"]["letters"]
    fills = inv_payload["families"]["fill_new"]["letters"]
    flags = inv_payload["families"]["flag"]["letters"]
    for col in vo:
        clock = "open" if col in clocks["value_open"] else "close"
        if clock != "open":
            continue
        for opname, op, th, side in VAL_OPS:
            specs.append((f"valopen_{col}_{opname}", clock, side, "val", col,
                          (op, th)))
    for col in vc:
        clock = "open" if col in clocks["value_open"] else "close"
        if clock != "close":
            continue
        for opname, op, th, side in VAL_OPS:
            specs.append((f"valclose_{col}_{opname}", clock, side, "val", col,
                          (op, th)))
    for col in fills:
        clock = "open" if col in clocks["fill_open"] else "close"
        specs.append((f"fill_{col}_green", clock, 1, "fill", col, "green"))
        specs.append((f"fill_{col}_red", clock, -1, "fill", col, "red"))
    for col in flags:
        clock = "open" if col in clocks["value_open"] else "close"
        # 0/1 flags already covered as val_*_eq1 for some letters; still
        # emit a named flag so the family is visible even if redundant.
        specs.append((f"flag_{col}_eq1", clock, 1, "val", col, ("==", 1)))
    # Drop illegal open that reads a close-knowable fill letter as a fill.
    cleaned = []
    for spec in specs:
        name, clock, side, kind, col, extra = spec
        if clock == "open" and kind == "fill" and col not in clocks["fill_open"]:
            continue
        if clock == "open" and kind == "val" and col not in clocks["value_open"]:
            continue
        cleaned.append(spec)
    return cleaned


def index_specs(specs):
    by_col = defaultdict(list)
    by_name = {}
    for spec in specs:
        by_col[spec[4]].append(spec)
        by_name[spec[0]] = spec
    return by_col, by_name


def fire_day(cells, by_col):
    """Return {name: True} for specs that fire on this day."""
    g = {}
    for col, group in by_col.items():
        rec = cells.get(col)
        if rec is None:
            continue
        v = num(rec)
        f = fam(rec)
        for name, _clock, _side, kind, _c, extra in group:
            if kind == "fill":
                if f == extra:
                    g[name] = True
            elif v is not None:
                op, th = extra
                if _cmp(v, op, th):
                    g[name] = True
    return g


def mine(specs):
    split = json.load(open(SPLIT_PATH))
    disc, hold = set(split["discovery"]), set(split["holdout"])
    spy = load_spy()
    files = sorted(f for f in glob.glob(os.path.join(SAMPLE, "*.json"))
                   if not os.path.basename(f).startswith("_"))
    print(f"[mine_unmined] files={len(files)} specs={len(specs)} spy={len(spy)}",
          flush=True)
    by_col, by_name = index_specs(specs)
    cells = defaultdict(lambda: {
        "disc": _slot(), "hold": _slot(),
        "early": _slot(), "late": _slot(),
        "spy_up": _slot(), "spy_dn": _slot(),
        "q12": _slot(), "q3": _slot(),
        "tickers": set(), "dates": set(),
        "day": defaultdict(float),
    })
    base = defaultdict(lambda: _slot())
    n_bad_seed = 0
    for i, path in enumerate(files, 1):
        g = json.load(open(path))
        src, seed = g.get("source"), g.get("seed")
        if (src and src != "rows_cache") or (seed and seed != "yahoo_rows_cache"):
            n_bad_seed += 1
            continue
        t = g["ticker"]
        days = g["days"]
        split_t = ("discovery" if t in disc else "holdout" if t in hold else None)
        if split_t is None:
            continue
        for ei, day in enumerate(days):
            for clock, side in (("open", 1), ("close", 1),
                                ("open", -1), ("close", -1)):
                for h in HOLDS:
                    raw = sim(days, ei, side, clock, h)
                    if raw is None:
                        continue
                    cost = COST_FUTU_LONG if side == 1 else COST_FUTU_SHORT
                    _push(base[(clock, "long" if side == 1 else "short", h)],
                          raw - cost)
        for ei, day in enumerate(days):
            gts = fire_day(day["cells"], by_col)
            if not gts:
                continue
            iso = str(s2d(day["date"]))
            tape = spy.get(iso, 0)
            half = "early" if iso < HALF_CUT else "late"
            qkey = "q12" if iso < Q3_CUT else "q3"
            for name in gts:
                _n, clock, side, _kind, _col, _extra = by_name[name]
                if clock == "open" and "core_score" in name:
                    raise ValueError("core_score cannot enter open")
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
                    _push(c[qkey], net)
                    if tape == 1:
                        _push(c["spy_up"], net)
                    elif tape == -1:
                        _push(c["spy_dn"], net)
                    c["tickers"].add(t)
                    c["dates"].add(iso)
                    if split_t == "discovery":
                        c["day"][iso] += net
        if i % 25 == 0:
            print(f"  ... {i}/{len(files)}", flush=True)
    if n_bad_seed:
        raise RuntimeError(f"refusing {n_bad_seed} dumps that are not rows_cache")
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
    _require_hold2_keep(rows)
    for r in rows:
        r["keep"] = harden_label(r)
    rows.sort(key=lambda r: (
        0 if r["keep"] == "KEEP" else 1 if r["verdict"] == "PASS" else
        2 if r["verdict"] == "FAIL" else 3,
        -((r["holdout"] or {}).get("t") or -9),
    ))
    return rows, baselines, files


def fmt_blk(b):
    if not b:
        return "—"
    key = "avg_net" if "avg_net" in b else "avg"
    return f"{b['n']}/{b[key]*100:+.2f}%/t={b['t']:.1f}"


def _plain_row(r):
    side = "buy" if r["side"] == "long" else "short"
    when = "at the open" if r["clock"] == "open" else "at the close"
    hold = {"hold1": "and sell the same day's close",
            "hold2": "and sell the next day's close",
            "hold5": "and sell five sessions later"}.get(r["exit"], r["exit"])
    fam = r.get("family")
    if fam == "val_open":
        what = "an open-knowable number on the sheet is past a fixed threshold"
    elif fam == "val_close":
        what = "a close-knowable number on the sheet is past a fixed threshold"
    elif fam == "fill_new":
        what = "a leftover column is highlighted green or red"
    elif fam == "flag":
        what = "a 0/1 formula flag is on"
    else:
        what = "the cell fires"
    return f"If {what}, {side} {when} {hold}."


def render_sweep(rows, baselines, files, inv_payload, specs):
    meta_path = os.path.join(SAMPLE, "_meta.json")
    meta = json.load(open(meta_path)) if os.path.exists(meta_path) else {}
    tickers = [os.path.basename(f)[:-5] for f in files]
    split = json.load(open(SPLIT_PATH))
    n_disc = sum(1 for t in tickers if t in set(split["discovery"]))
    n_hold = sum(1 for t in tickers if t in set(split["holdout"]))
    n_pass = sum(1 for r in rows if r["verdict"] == "PASS")
    n_fail = sum(1 for r in rows if r["verdict"] == "FAIL")
    n_thin = sum(1 for r in rows if r["verdict"] == "THIN")
    n_keep = sum(1 for r in rows if r["keep"] == "KEEP")
    n_kill = sum(1 for r in rows if r["keep"] == "KILL")
    by_fam = defaultdict(lambda: {"KEEP": 0, "KILL": 0, "THIN": 0, "n": 0})
    for r in rows:
        b = by_fam[r["family"]]
        b[r["keep"]] += 1
        b["n"] += 1
    spt = meta.get("minutes_per_ticker")
    sec = round(spt * 60, 2) if spt else None
    L = [
        "# Remaining A–JL families — unmined sweep",
        "",
        f"_Generated {date.today()} · live `flatten_robust` frozen. "
        "Yahoo/rows A–F seed only. No merge._",
        "",
        "## Plain English",
        "",
        "We rebuilt a **real hundreds-of-names** A–JL dump from the Yahoo "
        "row cache (never Excel's STOCKHISTORY cache) and scored the "
        "leftover cells the first 499-name cut skipped: leftover open "
        "numbers, leftover close numbers, leftover green/red highlights, "
        "and 0/1 formula flags. Fees are Futubull. We only buy at the open "
        "when the sheet already knows the number or the color at 9:30; "
        "everything else waits for the close.",
        "",
        "A keeper has to work on both ticker halves, both calendar halves "
        "(cut 2026-05-01), **Q3** (cut 2026-07-01), both SPY tapes, and "
        "must beat 'just buy everyone' by 20 bps. The fattest single day "
        "cannot be more than 25% of winning-day P&L. hold1 needs a hold2 "
        "sibling; hold5 needs hold2 edge.",
        "",
        f"Sample **{len(files)}** tickers ({n_disc} discovery / {n_hold} "
        f"holdout) · lean capture **{sec} s/ticker** · specs **{len(specs)}** "
        f"· scored cells **{len(rows)}**.",
        "",
        f"**KEEP {n_keep} · KILL {n_kill} · THIN {n_thin}** "
        f"(raw PASS {n_pass} / FAIL {n_fail} / THIN {n_thin}).",
        "",
        "Standing research keeps stay the three light+green O recipes. "
        "Finviz volume stays BLOCKED. AB / weather / book stay dead.",
        "",
        "### Family scoreboard",
        "",
        "| family | what it is | KEEP | KILL | THIN |",
        "|---|---|---:|---:|---:|",
    ]
    labels = {
        "val_open": "open-knowable numbers past A/C/J",
        "val_close": "close-knowable leftover numbers",
        "fill_new": "leftover green/red highlights",
        "flag": "0/1 formula flags",
    }
    for fam, label in labels.items():
        b = by_fam.get(fam) or {"KEEP": 0, "KILL": 0, "THIN": 0}
        L.append(f"| `{fam}` | {label} | {b['KEEP']} | {b['KILL']} | {b['THIN']} |")
    L += [
        "",
        "### Unconditional baseline (this sample, Futubull)",
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
    keeps = [r for r in rows if r["keep"] == "KEEP"]
    L += ["", "### KEEP (hardened)", ""]
    if not keeps:
        L += [
            "*(none — every leftover family is a clean null on this sample)*",
            "",
        ]
    else:
        L += [
            "These cleared the same bar as light+O (both halves, Q3, both "
            "tapes, top-day lottery, beat baseline, horizon sibling).",
            "",
            "| meaning | def | clock | side | exit | disc | hold | Q3 | "
            "day-lottery | tickers |",
            "|---|---|---|---|---|---|---|---|---|---:|",
        ]
        for r in keeps:
            L.append(
                f"| {_plain_row(r)} | `{r['def']}` | {r['clock']} | "
                f"{r['side']} | {r['exit']} | {fmt_blk(r['discovery'])} | "
                f"{fmt_blk(r['holdout'])} | {fmt_blk(r['q3'])} | "
                f"{(r.get('lottery_day_frac') or 0)*100:.1f}% | "
                f"{r['n_tickers']} |"
            )
        L.append("")
    L += [
        "### Near-miss / top KILL (FAIL, hold1/2 first)",
        "",
        "| keep | def | family | clock | side | exit | disc | hold | Q3 | "
        "tickers | why |",
        "|---|---|---|---|---|---|---|---|---|---:|---|",
    ]
    hold12 = [r for r in rows if r["exit"] in ("hold1", "hold2")]
    show = (hold12 or rows)[:36]
    for r in show:
        L.append(
            f"| {r['keep']} | `{r['def']}` | {r['family']} | {r['clock']} | "
            f"{r['side']} | {r['exit']} | {fmt_blk(r['discovery'])} | "
            f"{fmt_blk(r['holdout'])} | {fmt_blk(r['q3'])} | "
            f"{r['n_tickers']} | {','.join(r['fail_reasons']) or '—'} |"
        )
    exhausted = []
    for fam, label in labels.items():
        b = by_fam.get(fam) or {"KEEP": 0, "KILL": 0, "THIN": 0, "n": 0}
        if b["KEEP"] == 0 and (b["KILL"] + b["THIN"] > 0 or b["n"] == 0):
            exhausted.append(
                f"- **{label}** (`{fam}`): clean null — KEEP 0 / KILL "
                f"{b['KILL']} / THIN {b['THIN']}."
            )
    L += [
        "",
        "### Ghost that looked like a KEEP (then died)",
        "",
        "Column **DE** paints red when the cell equals 0 (green when it "
        "equals 1). The formula that writes DE also reads same-day volume "
        "and same-day return H, so we only enter at the **close**. "
        "`fill_DE_red` short hold5 printed +1.12% / +0.67% on the two "
        "ticker halves, but the highlight **never shows up in Q3** "
        "(q3 n=0) and the late 2026 half is only 39 trades. hold2 on the "
        "same cell is a wash (+0.02% holdout, t=0.08). That is a first-half "
        "ghost, not a keeper — **KILL** `q3_missing` / `tape_thin` / "
        "`no_hold2_keep`.",
        "",
        "### Exhaustion",
        "",
    ]
    if n_keep:
        L.append(
            f"{n_keep} leftover cells KEEP. The regions with KEEP 0 are "
            "null on this sample; the KEEP rows still need another regime "
            "before anyone would wire a card."
        )
    else:
        L.append(
            "Every leftover family on this sample is a **clean null**. "
            "That is a finding, not a pause: these letters do not buy a "
            "leak-free edge under the ship bar in Jan–Sep 2026."
        )
    if exhausted:
        L += ["", *exhausted]
    L += [
        "",
        "A–F seed: Yahoo/rows cache via `seed_anchor`. "
        f"Capture path `{meta.get('path')}` · "
        f"excel STOCKHISTORY cache used: "
        f"{bool(meta.get('excel_stockhistory_cache'))}.",
        "",
        "Research only. No cards. Live `flatten_robust` untouched.",
        "",
    ]
    slim_keys = (
        "def", "family", "clock", "side", "exit", "n_tickers", "n_dates",
        "discovery", "holdout", "early", "late", "spy_up", "spy_dn",
        "q12", "q3", "baseline", "lottery_day_frac", "lottery_top_day",
        "verdict", "keep", "fail_reasons", "live_untouched",
    )
    slim = lambda r: {k: r[k] for k in slim_keys if k in r}
    hold12 = [r for r in rows if r["exit"] in ("hold1", "hold2")]
    ghosts = [r for r in rows if r["def"] == "fill_DE_red"]
    saved = []
    seen = set()
    for r in keeps + ghosts + (hold12[:80] if hold12 else rows[:80]):
        key = (r["def"], r["exit"])
        if key in seen:
            continue
        seen.add(key)
        saved.append(r)
    payload = {
        "generated": str(date.today()),
        "n_tickers": len(files),
        "n_discovery": n_disc,
        "n_holdout": n_hold,
        "minutes_per_ticker": spt,
        "n_specs": len(specs),
        "n_rows": len(rows),
        "n_pass": n_pass, "n_fail": n_fail, "n_thin": n_thin,
        "n_keep": n_keep, "n_kill": n_kill,
        "family_counts": {k: dict(v) for k, v in by_fam.items()},
        "baselines": baselines,
        "keepers": [slim(r) for r in keeps],
        "cells": [slim(r) for r in saved],
        "live_untouched": "flatten_robust",
        "excel_cache_used": False,
        "seed": "yahoo_rows_cache",
        "q3_cut": Q3_CUT,
        "half_cut": HALF_CUT,
        "surface": "A-JL-unmined",
        "ao_is_not_whole_excel": True,
        "standing_keeps": inv_payload["standing_keeps"],
        "finviz": "BLOCKED",
    }
    return "\n".join(L) + "\n", payload


def render_scoreboard_block(md):
    return MARKER + "\n\n" + md.split("## Plain English", 1)[-1]


def write_inventory():
    payload = inventory()
    md = render_inventory(payload)
    open(INV_MD, "w", encoding="utf-8").write(md)
    json.dump(payload, open(INV_OUT_JSON, "w"), indent=2)
    return payload, md


def main():
    os.chdir(ROOT)
    ap = argparse.ArgumentParser()
    ap.add_argument("--inventory-only", action="store_true")
    args = ap.parse_args()
    inv_payload, inv_md = write_inventory()
    print(f"wrote {INV_MD}", flush=True)
    if args.inventory_only:
        return inv_payload, [], {}, []
    clocks = load_clocks()
    specs = build_specs(inv_payload, clocks)
    print(f"specs={len(specs)} val_open={len(inv_payload['families']['val_open']['letters'])} "
          f"val_close={len(inv_payload['families']['val_close']['letters'])} "
          f"fill={len(inv_payload['families']['fill_new']['letters'])} "
          f"flag={len(inv_payload['families']['flag']['letters'])}", flush=True)
    rows, baselines, files = mine(specs)
    md, payload = render_sweep(rows, baselines, files, inv_payload, specs)
    open(OUT_MD, "w", encoding="utf-8").write(md)
    json.dump(payload, open(OUT_JSON, "w"), indent=2)
    block = MARKER + "\n\n" + md
    sb = splice_md(SB_MD, MARKER, block,
                   require_any=("first A–JL cut", "A–O clock cycle"))
    ao = splice_md(AO_MD, MARKER, block, require="VISIBLE_COLS A..O")
    cy_note = (
        MARKER + "\n\n"
        f"Unmined A–JL sweep N={payload['n_tickers']} · "
        f"KEEP {payload['n_keep']} · KILL {payload['n_kill']} · "
        f"THIN {payload['n_thin']}. "
        "Standing keeps remain light+green O. Finviz BLOCKED. "
        "See `UNMINED_SWEEP.md`.\n"
    )
    cy = splice_md(CYCLE_MD, MARKER, cy_note,
                   require_any=("first A–JL cut", "A–O clock cycle"))
    open(SB_MD, "w", encoding="utf-8").write(sb)
    open(AO_MD, "w", encoding="utf-8").write(ao)
    open(CYCLE_MD, "w", encoding="utf-8").write(cy)
    # Pointer on the standing 499 / 8-def null — do not wipe it.
    if os.path.exists(ALL_COLS_MD):
        ac = splice_md(
            ALL_COLS_MD, MARKER,
            MARKER + "\n\n"
            "The 8-def / 16-fill list above is the **old** near-miss "
            "(N=499, PASS 0). Leftover P–JL / formula / fill families "
            f"are scored in `UNMINED_SWEEP.md` "
            f"(N={payload['n_tickers']}, KEEP {payload['n_keep']}).\n",
            require="whole Excel",
        )
        open(ALL_COLS_MD, "w", encoding="utf-8").write(ac)
    print(f"KEEP {payload['n_keep']} KILL {payload['n_kill']} THIN {payload['n_thin']} "
          f"N={payload['n_tickers']}", flush=True)
    return inv_payload, rows, payload, files


if __name__ == "__main__":
    main()
