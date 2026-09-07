"""H / I multi-horizon harness — not Futubull-only.

Predict columns H (intraday %) and I (daily % vs yesterday) on the
ticker train/test split. Horizons 1d, 2d, 3d, 1w (~5), 2w (~10).

Same-row H/I are labels, never features. Prior-row H/I lags are fair.
Standing light+O ± AH/FR is re-scored, not discarded.
Close-entry pair+lag (AA-today OK) is retargeted to H/I.

  python engine/mine_hi_horizon.py
  python engine/mine_hi_horizon.py --render-only
"""
from __future__ import annotations

import argparse
import glob
import json
import math
import os
import sys
from collections import defaultdict
from datetime import date

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from clock import OPEN_CORE_IDX, OPEN_IDX, SHIP, VISIBLE  # noqa: E402
from harden_hyst_open import apply_hold1_keep, splice_md  # noqa: E402
from mine_next_region import Q1_CUT, deeper  # noqa: E402
from mine_pair_lag import (  # noqa: E402
    FILL_OPEN, N_LAG, SKIP_VALUE_OPS, VALUE_OPEN_44, atom_name,
    collapse_twins, family_of, meaning_of, stamp_labels,
)
from mine_pair_lag_close import (  # noqa: E402
    HIGHLIGHT_GHOSTS, VALUE_CLOSE_CORE, assert_close_gate, value_close_today,
)
from mine_unmined import (  # noqa: E402
    HALF_CUT, Q3_CUT, _blk, _cmp, _push, _slot, fam, load_spy, num, pack_row,
    s2d,
)
from mine_pair_lag import load_clocks  # noqa: E402
from signals import _hysteresis, classify_fill  # noqa: E402

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
REPO = os.path.dirname(ROOT)
DUMPS = os.environ.get("ALL_COLS_DIR", os.path.join(ROOT, "research", "all_cols_sample"))
SPLIT_PATH = os.path.join(HERE, "holdout_split.json")
SPY_PATH = os.path.join(ROOT, "research", "spy_tape.json")
RESEARCH = os.path.join(ROOT, "research")
SCOREBOARD = os.path.join(REPO, "03_scoreboard")
OUT_MD = os.path.join(RESEARCH, "HI_HORIZON.md")
OUT_JSON = os.path.join(RESEARCH, "hi_horizon.json")
PLAN_MD = os.path.join(RESEARCH, "HI_HORIZON_PLAN.md")
SB_MD = os.path.join(SCOREBOARD, "EXCEL_BOT_MINE.md")
AO_MD = os.path.join(RESEARCH, "AO_FIRST_MINE.md")
CYCLE_MD = os.path.join(RESEARCH, "MINE_CYCLE.md")
MARKER = "## H/I multi-horizon (standing keep + close pair)"

HORIZONS = (1, 2, 3, 5, 10)
HORIZON_PLAIN = {1: "1d", 2: "2d", 3: "3d", 5: "1w", 10: "2w"}
# Primary scoreboard label: stacked daily I (the move). H print alongside.
PRIMARY = "I_sum"
LABELS = ("H", "I", "I_sum")
FLAT_BPS = 0.0015  # |SPY| < 15 bp → flat
BEAT = 0.002
NOW_OPS = (("eq1", "==", 1), ("ge1", ">=", 1), ("lt1", "<", 1), ("gt0", ">", 0))
LAG_OPS = (("lt1", "<", 1), ("eq1", "==", 1), ("ge1", ">=", 1))
# Close-today for the H/I pair pilot — no H/I same-row.
CLOSE_TODAY = tuple(
    c for c in VALUE_CLOSE_CORE if c not in ("H", "I") and c not in HIGHLIGHT_GHOSTS
)
FILL_CLOSE_TODAY = ("D", "E", "F", "N")  # not H/I fills
LAG_FEATURE = (
    "O", "AA", "H", "I", "ES", "Q", "AH", "FR", "N", "C", "J", "DE",
)
PAIR_TOP = 20
STANDING = (
    "light_O",
    "light_O_AH",
    "light_O_FR",
    "light_O_AH_FR",
)
STANDING_PLAIN = {
    "light_O": (
        "the five morning cells (A, B, C, G, J) add up to a strong green "
        "(+5 or more) and the light stays on until that sum falls to +2, "
        "and O is green"
    ),
    "light_O_AH": (
        "same five-cell light + green O, and the recent 5% down-day "
        "count (AH) is at least 1"
    ),
    "light_O_FR": (
        "same five-cell light + green O, and recent volume over 1M "
        "and/or G ≥ 3 (FR) is at least 1"
    ),
    "light_O_AH_FR": (
        "same five-cell light + green O, and both AH and FR are at least 1"
    ),
}


def load_spy_regimes():
    """SPY up / down / flat from closes. Flat is |day| < 15 bp."""
    raw = json.load(open(SPY_PATH))
    rows = raw if isinstance(raw, list) else raw.get("days") or []
    tape, prev = {}, None
    for r in rows:
        iso = str(r["date"])[:10]
        c = r.get("close")
        if c is None:
            continue
        if prev is not None and prev:
            ret = (c - prev) / prev
            if abs(ret) < FLAT_BPS:
                tape[iso] = 0
            else:
                tape[iso] = 1 if ret > 0 else -1
        prev = c
    return tape


def hi_cell(cells, col, close=None, open_=None, prev_c=None):
    v = num(cells.get(col))
    if v is not None:
        return float(v)
    if col == "H" and close and open_:
        return (close - open_) / open_
    if col == "I" and close and prev_c:
        return (close - prev_c) / prev_c
    return None


def heat_bucket(open_core):
    if open_core is None:
        return "mixed"
    if open_core >= 5:
        return "hot"
    if open_core <= 0:
        return "cold"
    return "mixed"


def tape_name(v):
    return {1: "spy_up", -1: "spy_dn", 0: "spy_flat"}.get(v, "spy_unk")


def assert_hi_gate(clocks, today):
    assert_close_gate(clocks, today)
    if "H" in today or "I" in today:
        raise ValueError("H/I same-row cannot be close-today features")
    for col in FILL_CLOSE_TODAY:
        if col in ("H", "I"):
            raise ValueError("H/I fill cannot be a feature")


def build_close_atoms(clocks_by, today):
    atoms = []
    for col in today:
        if clocks_by[col]["value_mine"] != "close":
            raise ValueError(f"{col} is not value-close")
        if col in ("H", "I") or col in VALUE_OPEN_44:
            raise ValueError(f"{col} leaked into close-today")
        for opname, op, th in NOW_OPS:
            atoms.append({
                "name": atom_name(col, 0, opname),
                "col": col, "lag": 0, "kind": "num",
                "opname": opname, "op": op, "th": th,
            })
    for col in FILL_CLOSE_TODAY:
        if col in FILL_OPEN or col in ("H", "I"):
            raise ValueError(f"illegal close fill {col}")
        atoms.append({
            "name": atom_name(col, 0, "green"),
            "col": col, "lag": 0, "kind": "fill",
            "opname": "green", "op": "==", "th": "green",
        })
    lag_cols = []
    for col in LAG_FEATURE:
        if col not in lag_cols:
            lag_cols.append(col)
    for col in lag_cols:
        for lag in range(1, N_LAG + 1):
            for opname, op, th in LAG_OPS:
                atoms.append({
                    "name": atom_name(col, lag, opname),
                    "col": col, "lag": lag, "kind": "num",
                    "opname": opname, "op": op, "th": th,
                })
    for a in atoms:
        if a["lag"] == 0 and a["col"] in ("H", "I"):
            raise ValueError(f"same-row H/I feature: {a['name']}")
    return atoms


def slim_ticker(path):
    g = json.load(open(path))
    src, seed = g.get("source"), g.get("seed")
    if (src and src != "rows_cache") or (seed and seed != "yahoo_rows_cache"):
        return None
    days_in = g["days"]
    n = len(days_in)
    isos, Hs, Is = [None] * n, [None] * n, [None] * n
    cores, o_green = [0.0] * n, [False] * n
    ah, fr = [None] * n, [None] * n
    fills, nums = {}, {}
    want_fill = set("ABCDEFGHIJKLMNO")
    want_num = set(CLOSE_TODAY) | set(LAG_FEATURE) | set(VALUE_OPEN_44) | {"H", "I"}
    prev_c = None
    for ei, day in enumerate(days_in):
        cells = day.get("cells") or {}
        iso = str(s2d(day["date"]))
        isos[ei] = iso
        o, c = day.get("open"), day.get("close")
        Hs[ei] = hi_cell(cells, "H", close=c, open_=o)
        Is[ei] = hi_cell(cells, "I", close=c, prev_c=prev_c)
        sc = []
        for let in VISIBLE:
            rec = cells.get(let)
            fam_, s = classify_fill((rec or {}).get("f"))
            sc.append(s)
            if let == "O":
                o_green[ei] = fam_ == "green"
            if let in want_fill:
                fills.setdefault(let, []).append(fam_)
        cores[ei] = sum(sc[i] for i in OPEN_CORE_IDX if i < len(sc))
        for col in want_num:
            nums.setdefault(col, []).append(num(cells.get(col)))
        ah[ei] = nums["AH"][ei] if "AH" in nums else num(cells.get("AH"))
        fr[ei] = nums["FR"][ei] if "FR" in nums else num(cells.get("FR"))
        prev_c = c
    # pad fills/nums to n
    for col in list(fills):
        if len(fills[col]) != n:
            fills[col] = (fills[col] + ["none"] * n)[:n]
    for col in list(nums):
        if len(nums[col]) != n:
            nums[col] = (nums[col] + [None] * n)[:n]
    return {
        "ticker": g["ticker"], "n": n, "iso": isos,
        "H": Hs, "I": Is, "core": cores, "o_green": o_green,
        "ah": ah, "fr": fr, "fills": fills, "nums": nums,
    }


def labels_for(slim, ei, k):
    last = ei + k - 1
    if last >= slim["n"]:
        return None
    h = slim["H"][last]
    i_print = slim["I"][last]
    acc = 1.0
    any_i = False
    for j in range(ei, last + 1):
        iv = slim["I"][j]
        if iv is None:
            return None
        acc *= (1.0 + iv)
        any_i = True
    if not any_i:
        return None
    return {"H": h, "I": i_print, "I_sum": acc - 1.0}


def fire_atom_slim(slim, ei, atom):
    src = ei - atom["lag"]
    if src < 0:
        return False
    if atom["kind"] == "fill":
        row = slim["fills"].get(atom["col"])
        return bool(row) and row[src] == atom["th"]
    row = slim["nums"].get(atom["col"])
    if not row:
        return False
    v = row[src]
    if v is None:
        return False
    return _cmp(v, atom["op"], atom["th"])


def standing_entries(slim):
    """First day of each long five-cell hysteresis run, with O green."""
    clusters = _hysteresis(slim["core"], 5, 2, 2)
    out = []
    for c in clusters:
        if c.get("side") != 1:
            continue
        ei = c["entry_idx"]
        if ei >= slim["n"] or not slim["o_green"][ei]:
            continue
        a = slim["ah"][ei]
        f = slim["fr"][ei]
        ah_on = isinstance(a, (int, float)) and a >= 1
        fr_on = isinstance(f, (int, float)) and f >= 1
        names = ["light_O"]
        if ah_on:
            names.append("light_O_AH")
        if fr_on:
            names.append("light_O_FR")
        if ah_on and fr_on:
            names.append("light_O_AH_FR")
        out.append((ei, names))
    return out


def _cell():
    return {
        "disc": _slot(), "hold": _slot(),
        "early": _slot(), "late": _slot(),
        "spy_up": _slot(), "spy_dn": _slot(),
        "q12": _slot(), "q3": _slot(), "q1": _slot(),
        "tickers": set(), "dates": set(),
        "day": defaultdict(float),
        "ticker_pnl": defaultdict(float),
        "month": defaultdict(lambda: _slot()),
        "heat_hot": _slot(), "heat_mixed": _slot(), "heat_cold": _slot(),
        "spy_flat": _slot(),
    }


def push_hit(cells, key, slim, ei, lab, split_t, spy):
    iso = slim["iso"][ei]
    tape = spy.get(iso, None)
    half = "early" if iso < HALF_CUT else "late"
    qkey = "q12" if iso < Q3_CUT else "q3"
    mon = iso[:7]
    heat = heat_bucket(slim["core"][ei])
    c = cells[key]
    net = lab
    _push(c["disc"] if split_t == "discovery" else c["hold"], net)
    _push(c[half], net)
    _push(c[qkey], net)
    if iso < Q1_CUT:
        _push(c["q1"], net)
    if tape == 1:
        _push(c["spy_up"], net)
    elif tape == -1:
        _push(c["spy_dn"], net)
    elif tape == 0:
        _push(c["spy_flat"], net)
    _push(c[f"heat_{heat}"], net)
    c["tickers"].add(slim["ticker"])
    c["dates"].add(iso)
    c["ticker_pnl"][slim["ticker"]] += net
    if split_t == "discovery":
        c["day"][iso] += net


def pack_hi(cells, baselines):
    rows = []
    for (name, label, hz, clock), cell in cells.items():
        # pack_row wants clock/side/rule; reuse Futubull ship bar on the H/I print
        row = pack_row(name, clock, "long", f"hold{hz}", cell, baselines)
        if not row:
            continue
        row["family"] = "standing" if name in STANDING else family_of(name)
        row["label"] = label
        row["horizon"] = hz
        row["horizon_plain"] = HORIZON_PLAIN.get(hz, f"{hz}d")
        row["clock"] = clock
        right_b = baselines.get(f"{label}_{hz}")
        if right_b:
            row["baseline"] = right_b
            reasons0 = [x for x in (row.get("fail_reasons") or [])
                        if x != "no_edge_vs_uncond"]
            d, h = row.get("discovery") or {}, row.get("holdout") or {}
            if ((d.get("avg_net") or 0) < right_b["avg_net"] + BEAT) or (
                h and (h.get("avg_net") or 0) < right_b["avg_net"] + BEAT
            ):
                reasons0.append("no_edge_vs_uncond")
            row["fail_reasons"] = reasons0
            if row.get("verdict") == "PASS" and reasons0:
                row["verdict"] = "FAIL"
            if row.get("verdict") == "FAIL" and not reasons0:
                row["verdict"] = "PASS"
        deeper(row, cell)
        row["heat_hot"] = _blk(cell["heat_hot"])
        row["heat_mixed"] = _blk(cell["heat_mixed"])
        row["heat_cold"] = _blk(cell["heat_cold"])
        row["spy_flat"] = _blk(cell["spy_flat"])
        reasons = list(row.get("fail_reasons") or [])
        up, dn = row.get("spy_up") or {}, row.get("spy_dn") or {}
        if up.get("n", 0) >= 40 and dn.get("n", 0) >= 40:
            if (up.get("avg_net", 0) > 0) != (dn.get("avg_net", 0) > 0):
                reasons.append("regime_split")
                row["verdict"] = "KILL"
                row["keep"] = "KILL"
        row["fail_reasons"] = list(dict.fromkeys(reasons))
        if row.get("verdict") == "KEEP" and "regime_split" in row["fail_reasons"]:
            row["verdict"] = "KILL"
        row["keep"] = row["verdict"]
        row["plain"] = (
            STANDING_PLAIN.get(name) or meaning_of(name)
        )
        rows.append(row)
    apply_hold1_keep(rows)
    for r in rows:
        r["keep"] = r["verdict"]
    return rows


def walk(files, atoms, pairs, spy, disc, hold, do_standing=True,
         close_h=(1,), close_labs=(PRIMARY,)):
    """Standing always uses every H/I horizon. Close atoms use close_h/labs."""
    cells = defaultdict(_cell)
    base = defaultdict(lambda: _slot())
    n_bad = 0
    standing_n = 0
    for i, path in enumerate(files, 1):
        slim = slim_ticker(path)
        if slim is None:
            n_bad += 1
            continue
        t = slim["ticker"]
        split_t = ("discovery" if t in disc else "holdout" if t in hold else None)
        if split_t is None:
            continue
        if do_standing:
            for ei, names in standing_entries(slim):
                standing_n += 1
                for k in HORIZONS:
                    labs = labels_for(slim, ei, k)
                    if not labs:
                        continue
                    for lab_name, val in labs.items():
                        if val is None:
                            continue
                        for nm in names:
                            push_hit(
                                cells, (nm, lab_name, k, "open"),
                                slim, ei, val, split_t, spy,
                            )
        fired_cache = None
        if atoms:
            fired_cache = []
            for ei in range(slim["n"]):
                fired_cache.append({a["name"] for a in atoms if fire_atom_slim(slim, ei, a)})
        for ei in range(slim["n"]):
            for k in HORIZONS:
                labs = labels_for(slim, ei, k)
                if not labs:
                    continue
                for lab_name, val in labs.items():
                    if val is None:
                        continue
                    _push(base[(lab_name, k)], val)
            if not atoms:
                continue
            fired = fired_cache[ei]
            if not fired:
                continue
            hit = list(fired)
            for p in pairs:
                if p["left"] in fired and p["right"] in fired:
                    hit.append(p["name"])
            for k in close_h:
                labs = labels_for(slim, ei, k)
                if not labs:
                    continue
                for lab_name in close_labs:
                    val = labs.get(lab_name)
                    if val is None:
                        continue
                    for name in hit:
                        push_hit(
                            cells, (name, lab_name, k, "close"),
                            slim, ei, val, split_t, spy,
                        )
        if i % 400 == 0:
            print(f"  ... {i}/{len(files)}", flush=True)
    if n_bad:
        raise RuntimeError(f"refusing {n_bad} dumps that are not rows_cache")
    return cells, base, standing_n


def build_pairs(alive, today):
    close_now = [a for a in alive if a["lag"] == 0 and a["col"] in today]
    fills = [a for a in alive if a["lag"] == 0 and a["kind"] == "fill"]
    now = close_now + fills
    lag = [a for a in alive if a["lag"] >= 1]
    pairs = []
    for left in lag:
        for right in now:
            pairs.append({
                "name": f"{left['name']}__and__{right['name']}",
                "left": left["name"], "right": right["name"],
            })
    now_s = sorted(now, key=lambda a: a["name"])
    for i, a in enumerate(now_s):
        for b in now_s[i + 1:]:
            if a["col"] == b["col"]:
                continue
            pairs.append({
                "name": f"{a['name']}__and__{b['name']}",
                "left": a["name"], "right": b["name"],
            })
    by = {a["name"]: a for a in alive}
    extra = (("O_l2_lt1", "AA_l0_eq1"), ("O_l1_lt1", "AA_l0_eq1"))
    for a, b in extra:
        if a in by and b in by:
            pairs.append({
                "name": f"{a}__and__{b}",
                "left": a, "right": b,
            })
    seen, out = set(), []
    for p in pairs:
        if p["name"] in seen:
            continue
        recs = [by.get(p["left"]), by.get(p["right"])]
        if any(r is None for r in recs):
            continue
        if any(r["lag"] == 0 and r["col"] in ("H", "I") for r in recs):
            continue
        if not any(r["lag"] == 0 and r["col"] in today or (
            r["lag"] == 0 and r["kind"] == "fill"
        ) for r in recs):
            continue
        seen.add(p["name"])
        out.append(p)
    return out


def select_alive(rows, atoms):
    by_name = {a["name"]: a for a in atoms}
    cand = []
    for r in rows:
        if r.get("family") == "standing":
            continue
        if r.get("label") != PRIMARY or r.get("horizon") != 1:
            continue
        if r["family"] != "single":
            continue
        d = r.get("discovery") or {}
        if d.get("n", 0) < 200 or (d.get("avg_net") or 0) <= 0:
            continue
        a = by_name.get(r["def"])
        if a:
            cand.append((r, a))
    best = {}
    for r, a in cand:
        key = (a["col"], a["lag"], a["kind"])
        cur = best.get(key)
        if cur is None or (r["discovery"].get("t") or -9) > (cur[0]["discovery"].get("t") or -9):
            best[key] = (r, a)
    now, lag = [], []
    for r, a in best.values():
        (now if a["lag"] == 0 else lag).append((r, a))
    now.sort(key=lambda x: -((x[0].get("discovery") or {}).get("t") or -9))
    lag.sort(key=lambda x: -((x[0].get("discovery") or {}).get("t") or -9))
    picked = [a for _r, a in now[:PAIR_TOP]] + [a for _r, a in lag[:PAIR_TOP]]
    must = {"O_l2_lt1", "AA_l0_eq1", "AA_l0_ge1", "O_l0_lt1", "O_l1_lt1"}
    have = {a["name"] for a in picked}
    for a in atoms:
        if a["name"] in must and a["name"] not in have:
            picked.append(a)
            have.add(a["name"])
    return picked


def mine(atoms, today, limit=0):
    split = json.load(open(SPLIT_PATH))
    disc, hold = set(split["discovery"]), set(split["holdout"])
    spy = load_spy_regimes()
    files = [f for f in sorted(glob.glob(os.path.join(DUMPS, "*.json")))
             if not os.path.basename(f).startswith("_")]
    if limit:
        files = files[:limit]
    print(f"[hi] pass1 files={len(files)} atoms={len(atoms)}", flush=True)
    cells1, base, standing_n = walk(
        files, atoms, [], spy, disc, hold,
        do_standing=True, close_h=(1, 2), close_labs=(PRIMARY,),
    )
    baselines = {}
    for (lab, hz), sl in base.items():
        b = _blk(sl)
        if b:
            # pack_row looks up f"{clock}_{side}_h{hn}" — feed I_sum@1 as close_long
            baselines[f"close_long_h{hz}"] = b
            baselines[f"open_long_h{hz}"] = b
            baselines[f"{lab}_{hz}"] = b
    singles = pack_hi(cells1, baselines)
    close_singles = [r for r in singles if r.get("family") != "standing"]
    standing_rows = [r for r in singles if r.get("family") == "standing"]
    alive = select_alive(close_singles, atoms)
    pairs = build_pairs(alive, today)
    print(f"[hi] pass2 alive={len(alive)} pairs={len(pairs)} "
          f"standing_entries={standing_n}", flush=True)
    need = {a["name"]: a for a in atoms if a["name"] in {
        p["left"] for p in pairs
    } | {p["right"] for p in pairs}}
    cells2, _, _ = walk(
        files, list(need.values()), pairs, spy, disc, hold,
        do_standing=False, close_h=(1, 2), close_labs=(PRIMARY,),
    )
    pair_rows = [r for r in pack_hi(cells2, baselines) if r["family"] == "pair"]
    rows = standing_rows + close_singles + pair_rows
    rows.sort(key=lambda r: (
        0 if r.get("family") == "standing" else 1,
        0 if r["keep"] == "KEEP" else 1 if r["keep"] == "KILL" else 2,
        0 if r.get("label") == PRIMARY else 1,
        r.get("horizon") or 99,
        -((r.get("holdout") or {}).get("t") or -9),
    ))
    return rows, baselines, files, pairs, alive, standing_n


def _pct(b):
    if not b or b.get("avg_net") is None:
        return "—"
    return f"{b['avg_net']*100:+.2f}% (n={b['n']})"


def render_plan_ok():
    if not os.path.exists(PLAN_MD):
        raise FileNotFoundError("HI_HORIZON_PLAN.md missing")
    txt = open(PLAN_MD, encoding="utf-8").read()
    if "Plain English" not in txt or "column H" not in txt:
        raise ValueError("H/I plan is incomplete")


def render(rows, baselines, files, meta):
    standing = [r for r in rows if r.get("family") == "standing"]
    others = [r for r in rows if r.get("family") != "standing"]
    # Primary board: I_sum at each horizon for standing + best others
    prim_st = [r for r in standing if r.get("label") == PRIMARY]
    prim_ot = [r for r in others if r.get("label") == PRIMARY and r.get("horizon") == 1]
    keeps = [r for r in rows if r["keep"] == "KEEP"]
    kills = [r for r in rows if r["keep"] == "KILL"]
    thins = [r for r in rows if r["keep"] == "THIN"]
    L = [
        MARKER,
        "",
        f"_Generated {date.today()} · live `flatten_robust` frozen. "
        "Yahoo/rows A–F seed only. No merge. Label is H/I, not Futubull-only. "
        "T/BA highlight ghosts not the search._",
        "",
        "## Plain English",
        "",
        "The mine now predicts **H** (intraday %, close vs open) and **I** "
        "(daily %, close vs yesterday) on the ticker train/test split. "
        "Horizons: 1d, 2d, 3d, 1 week, 2 weeks. Same-row H and I are the "
        "labels — they are never inputs. Yesterday’s H/I may be a feature. "
        "Standing five-cell light + green O ± AH/FR is re-scored here. "
        "Close-entry pairs may use AA today. Futubull open→close is not "
        "the sole label.",
        "",
    ]
    st_keep = [r for r in prim_st if r["keep"] == "KEEP"]
    if st_keep:
        L.append(
            f"**Standing KEEP {len(st_keep)}** on stacked daily I "
            f"(of {len(prim_st)} standing I-horizon rows). "
            "Research only — not a card."
        )
    else:
        L.append(
            f"**Standing check:** no stacked-I KEEP across the five "
            f"horizons ({len(prim_st)} rows scored). "
            "The old Futubull keep is not discarded; it does not "
            "automatically print as an H/I keep."
        )
    pair_keep = [r for r in prim_ot if r["keep"] == "KEEP" and r["family"] == "pair"]
    single_keep = [r for r in prim_ot if r["keep"] == "KEEP" and r["family"] == "single"]
    f_green = [r for r in prim_ot if r["keep"] == "KEEP" and "F_l0_green" in r["def"]]
    aa_rows = [r for r in prim_ot if "AA_l0_" in r["def"] and r.get("horizon") == 1
               and r.get("label") == PRIMARY]
    if pair_keep or single_keep:
        L.append(
            f"**Close-entry KEEP {len(single_keep)} singles / {len(pair_keep)} pairs** "
            f"on 1d stacked I. {len(f_green)} of those are today's F-green "
            "(volume fill) or a twin of it — same-close association with the "
            "I print, not a lagged forecast. AA-today pairs do not KEEP "
            "(SPY-down red / no edge). Research only — not a card. "
            "Trees only if a non-twin pair KEEPs; this F-green cluster is "
            "one print, not a new family."
        )
    else:
        n_pkill = sum(1 for r in prim_ot if r["family"] == "pair" and r["keep"] == "KILL")
        n_pthin = sum(1 for r in prim_ot if r["family"] == "pair" and r["keep"] == "THIN")
        L.append(
            f"**Close-entry pair+lag (AA-today, no same-row H/I):** "
            f"1d stacked I is a clean null or ghost — "
            f"KEEP 0 · KILL {n_pkill} · THIN {n_pthin}."
        )
    L += [
        "",
        f"Dumps **{meta['n_dumps']}**. Close atoms **{meta['n_atoms']}**. "
        f"Alive **{meta.get('n_alive', 0)}**. Pairs **{meta['n_pairs']}**. "
        f"Everyone-else 1d stacked I "
        f"{_pct(baselines.get('I_sum_1'))}. "
        f"Everyone-else 1d H {_pct(baselines.get('H_1'))}. "
        "Both SPY tapes required for a global KEEP. "
        "Light+O is the check, not a new search.",
        "",
        "### Standing light+O ± AH/FR on stacked daily I (holdout moves)",
        "",
        "| meaning | horizon | holdout I-stack | vs everyone | Q1 | spy↑ | spy↓ | "
        "heat hot | July | top-5 | verdict | why |",
        "|---|---|---|---|---|---|---|---|---|---|---|---|",
    ]
    shown_st = sorted(
        prim_st,
        key=lambda r: (STANDING.index(r["def"]) if r["def"] in STANDING else 9,
                       r.get("horizon") or 99),
    )
    if not shown_st:
        L.append("| — | — | — | — | — | — | — | — | — | — | — | — |")
    for r in shown_st:
        vs = "—"
        b = baselines.get("I_sum_" + str(r.get("horizon")))
        if r.get("holdout") and b:
            vs = f"{(r['holdout']['avg_net'] - b['avg_net'])*100:+.2f} pp"
        L.append(
            f"| {r.get('plain')} | {r.get('horizon_plain')} | "
            f"{_pct(r.get('holdout'))} | {vs} | {_pct(r.get('q1'))} | "
            f"{_pct(r.get('spy_up'))} | {_pct(r.get('spy_dn'))} | "
            f"{_pct(r.get('heat_hot'))} | "
            f"{(r.get('july_share') or 0)*100:.0f}% | "
            f"{(r.get('top5_share') or 0)*100:.0f}% | **{r['keep']}** | "
            f"{','.join(r.get('fail_reasons') or []) or '—'} |"
        )
    L += [
        "",
        "Code names (after the English): "
        + ", ".join(f"`{r['def']}` {r.get('horizon_plain')}" for r in shown_st[:8])
        + ".",
        "",
        "### Same standing recipes on 1d H (intraday print)",
        "",
        "| meaning | holdout H | spy↑ | spy↓ | verdict | why |",
        "|---|---|---|---|---|---|",
    ]
    h1 = [r for r in standing if r.get("label") == "H" and r.get("horizon") == 1]
    h1.sort(key=lambda r: STANDING.index(r["def"]) if r["def"] in STANDING else 9)
    if not h1:
        L.append("| — | — | — | — | — | — |")
    for r in h1:
        L.append(
            f"| {r.get('plain')} | {_pct(r.get('holdout'))} | "
            f"{_pct(r.get('spy_up'))} | {_pct(r.get('spy_dn'))} | "
            f"**{r['keep']}** | {','.join(r.get('fail_reasons') or []) or '—'} |"
        )
    L += [
        "",
        "### Close-entry pair+lag on 1d stacked I (AA today legal)",
        "",
        "| meaning | clock | holdout I-stack | vs everyone | Q1 | spy↑ | spy↓ | "
        "July | top-5 | verdict | why |",
        "|---|---|---|---|---|---|---|---|---|---|---|",
    ]
    # Show unique-ish KEEPs first (collapse F-green twins), then AA-today, then others.
    shown, seen = [], set()
    prefer = [r for r in prim_ot if r["def"] in (
        "F_l0_green", "F_l0_green__and__G_l0_ge1", "O_l2_lt1__and__AA_l0_eq1",
        "AA_l0_eq1",
    )]
    prefer += [r for r in prim_ot if r["keep"] == "KEEP" and r["def"] == "F_l0_green"]
    prefer += [r for r in prim_ot if r["keep"] == "KEEP"]
    prefer += [r for r in prim_ot if "AA_l0_" in r["def"]]
    prefer += [r for r in prim_ot if r["keep"] != "KEEP"]
    for r in prefer:
        if r["def"] in seen:
            continue
        seen.add(r["def"])
        shown.append(r)
        if len(shown) >= 20:
            break
    if not shown:
        L.append("| — | — | — | — | — | — | — | — | — | — | — |")
    for r in shown:
        vs = "—"
        b = baselines.get("I_sum_1")
        if r.get("holdout") and b:
            vs = f"{(r['holdout']['avg_net'] - b['avg_net'])*100:+.2f} pp"
        L.append(
            f"| {r.get('plain')} | close | {_pct(r.get('holdout'))} | {vs} | "
            f"{_pct(r.get('q1'))} | {_pct(r.get('spy_up'))} | "
            f"{_pct(r.get('spy_dn'))} | "
            f"{(r.get('july_share') or 0)*100:.0f}% | "
            f"{(r.get('top5_share') or 0)*100:.0f}% | **{r['keep']}** | "
            f"{','.join(r.get('fail_reasons') or []) or '—'} |"
        )
    L += [
        "",
        "Code names (after the English): "
        + ", ".join(f"`{r['def']}`" for r in shown[:8])
        + (" …" if len(shown) > 8 else "") + ".",
        "",
        "### Soft regimes",
        "",
        "Sheet heat is the morning five-cell sum (hot ≥5 / mixed / cold ≤0). "
        "Tape is SPY up / down / flat. A global KEEP needs SPY-up and "
        "SPY-down to agree. Conditional notes stay in the why column "
        "(`regime_split`).",
        "",
        "### What this does not change",
        "",
        "- Standing open Futubull keep is still the research baseline, "
        "not a card. This file only re-scores it on H/I.",
        "- Open-entry locked-44 pair+lag stays the accepted null "
        "(KEEP 0 / KILL 4266).",
        "- T / BA / CZ / EH / IB / HO / IL / GV stay KILL as highlight "
        "ghosts — not this search.",
        "- Same-row H and I are never features.",
        "- Finviz BLOCKED. Live `flatten_robust` frozen. No cards.",
        f"- Q1 cut **{Q1_CUT}**. Q3 cut **{Q3_CUT}**.",
        "",
        f"Board totals (all labels × horizons): KEEP {len(keeps)} · "
        f"KILL {len(kills)} · THIN {len(thins)}.",
        "",
        "Research only. One 2026 regime.",
        "",
    ]
    return "\n".join(L)


def write_outputs(rows, baselines, files, meta):
    md = render(rows, baselines, files, meta)
    open(OUT_MD, "w", encoding="utf-8").write(md + "\n")
    n_keep = sum(1 for r in rows if r["keep"] == "KEEP")
    n_kill = sum(1 for r in rows if r["keep"] == "KILL")
    n_thin = sum(1 for r in rows if r["keep"] == "THIN")
    payload = {
        "generated": str(date.today()),
        "spec": "H/I multi-horizon; standing check + close pair; not Futubull-only",
        "live_untouched": "flatten_robust",
        "excel_cache_used": False,
        "seed": "yahoo_rows_cache",
        "entry": "open-standing + close-pair",
        "labels": list(LABELS),
        "horizons": list(HORIZONS),
        "gate": meta["gate"],
        "n_dumps": len(files),
        "n_atoms": meta["n_atoms"],
        "n_alive": meta.get("n_alive", 0),
        "n_pairs": meta["n_pairs"],
        "q1_cut": Q1_CUT,
        "q3_cut": Q3_CUT,
        "standing_open": meta["standing_open"],
        "finviz": "BLOCKED",
        "n_keep": n_keep,
        "n_kill": n_kill,
        "n_thin": n_thin,
        "inventory": meta,
        "baselines": baselines,
        "rows": [{k: v for k, v in r.items() if k != "months"} for r in rows],
    }
    json.dump(payload, open(OUT_JSON, "w"), indent=2)
    block = md if md.startswith(MARKER) else MARKER + "\n\n" + md
    sb = splice_md(SB_MD, MARKER, block,
                   require_any=("first A–JL cut", "A–O clock cycle"))
    ao = splice_md(AO_MD, MARKER, block, require="VISIBLE_COLS A..O")
    cy = splice_md(
        CYCLE_MD, MARKER,
        MARKER + "\n\n"
        f"H/I multi-horizon: KEEP {n_keep} · KILL {n_kill} · THIN {n_thin}. "
        "Standing light+O re-scored. Close pair AA-today, no same-row H/I. "
        "See `HI_HORIZON.md`.\n",
        require_any=("first A–JL cut", "A–O clock cycle"),
    )
    open(SB_MD, "w", encoding="utf-8").write(sb)
    open(AO_MD, "w", encoding="utf-8").write(ao)
    open(CYCLE_MD, "w", encoding="utf-8").write(cy)
    return payload


def remaining_inventory(today, atoms, pairs, n_alive=0):
    return {
        "generated": str(date.today()),
        "live_untouched": "flatten_robust",
        "excel_cache_used": False,
        "labels": list(LABELS),
        "horizons": list(HORIZONS),
        "gate": (
            "OPEN_SAME_ROW_LABELS + CLOCK_MAP; same-row H/I are labels only"
        ),
        "value_close_today": list(today),
        "fill_close_today": list(FILL_CLOSE_TODAY),
        "n_atoms": len(atoms),
        "n_alive": n_alive,
        "n_pairs": len(pairs),
        "n_dumps": 0,
        "standing_open": "five-cell light+O ± AH/FR re-scored on H/I, not a card",
        "not_reopened": [
            "T/BA highlight ghosts",
            "CZ/EH/IB/HO/IL/GV leftover-close ghosts",
            "Futubull-only close-entry pair+lag as the sole label",
        ],
    }


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--render-only", action="store_true")
    ap.add_argument("--limit", type=int, default=0)
    args = ap.parse_args()
    render_plan_ok()
    stamp_labels()
    clocks, by = load_clocks()
    if "same_row_open" not in clocks:
        from classify_clocks import build
        clocks = build()
        by = {r["col"]: r for r in clocks["columns"]}
    today = CLOSE_TODAY
    assert_hi_gate(clocks, today)
    atoms = build_close_atoms(by, today)
    if args.render_only:
        prev = json.load(open(OUT_JSON, encoding="utf-8"))
        files = [None] * int(prev.get("n_dumps") or 0)
        payload = write_outputs(
            prev.get("rows") or [],
            prev.get("baselines") or {},
            files,
            prev.get("inventory") or remaining_inventory(today, atoms, []),
        )
        print(f"render-only KEEP {payload['n_keep']} KILL {payload['n_kill']}",
              flush=True)
        return payload
    rows, baselines, files, pairs, alive, standing_n = mine(
        atoms, today, limit=args.limit)
    meta = remaining_inventory(today, atoms, pairs, n_alive=len(alive))
    meta["n_dumps"] = len(files)
    meta["standing_entries"] = standing_n
    payload = write_outputs(rows, baselines, files, meta)
    print(f"KEEP {payload['n_keep']} KILL {payload['n_kill']} "
          f"THIN {payload['n_thin']} atoms={len(atoms)} alive={len(alive)} "
          f"pairs={len(pairs)} standing_entries={standing_n}", flush=True)
    for r in rows[:20]:
        print(
            f"  {r['keep']:4} {r.get('family','?'):8} "
            f"{r.get('label','?'):5} {r.get('horizon_plain','?'):3} "
            f"{r['def'][:40]:40} {','.join(r.get('fail_reasons') or [])}",
            flush=True,
        )
    return payload


if __name__ == "__main__":
    main()
