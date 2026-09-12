"""Pairwise + lag miner — open-entry, Excel-locked same-row gate.

Same-row day X is open only for the 44 value_mine_open cols and the
timing-tested open fills. Upper rows (t−1…t−N) of any letter are fair.
Do not invent clocks. Source: CLOCK_MAP.md / clock_map.json.

O green fill is open; O number is close. AA today is close.
B/G/K/M numbers, L value, D/E/F/H/I same-row, core_score → close.

  python engine/mine_pair_lag.py
  python engine/mine_pair_lag.py --render-only
"""
from __future__ import annotations

import argparse
import glob
import json
import os
import sys
from collections import defaultdict
from datetime import date

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from clock import COST_FUTU_LONG  # noqa: E402
from harden_hyst_open import apply_hold1_keep, splice_md  # noqa: E402
from mine_next_region import Q1_CUT, deeper  # noqa: E402
from mine_unmined import (  # noqa: E402
    HALF_CUT, Q3_CUT, _blk, _cmp, _push, _slot, fam, load_spy, num, pack_row,
    s2d, sim,
)

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
REPO = os.path.dirname(ROOT)
DUMPS = os.environ.get("ALL_COLS_DIR", os.path.join(ROOT, "research", "all_cols_sample"))
CLOCK_MAP = os.path.join(ROOT, "research", "clock_map.json")
SPLIT_PATH = os.path.join(HERE, "holdout_split.json")
RESEARCH = os.path.join(ROOT, "research")
SCOREBOARD = os.path.join(REPO, "03_scoreboard")
OUT_MD = os.path.join(RESEARCH, "PAIR_LAG.md")
OUT_JSON = os.path.join(RESEARCH, "pair_lag.json")
INV_MD = os.path.join(RESEARCH, "PAIR_LAG_INVENTORY.md")
PLAN_MD = os.path.join(RESEARCH, "PAIR_LAG_PLAN.md")
LABELS_MD = os.path.join(RESEARCH, "OPEN_SAME_ROW_LABELS.md")
SB_MD = os.path.join(SCOREBOARD, "EXCEL_BOT_MINE.md")
AO_MD = os.path.join(RESEARCH, "AO_FIRST_MINE.md")
CYCLE_MD = os.path.join(RESEARCH, "MINE_CYCLE.md")
MARKER = "## Pair+lag mine (open-entry pilot)"
HOLDS = (1, 2)
N_LAG = 5
PAIR_TOP = 30

# Excel-locked. Do not invent. Must match clock_map groups.
VALUE_OPEN_44 = (
    "A", "C", "J", "Q", "Z", "AC", "AH", "BT", "BV", "CG", "CH", "DC", "DE",
    "EB", "EK", "EN", "EP", "EQ", "ER", "ES", "ET", "EU", "EV", "FQ", "FR",
    "FS", "FU", "GD", "GE", "GF", "HF", "HG", "HW", "II", "IR", "IT", "IY",
    "IZ", "JB", "JC", "JD", "JE", "JF", "JL",
)
FILL_OPEN = ("A", "B", "C", "G", "J", "K", "L", "M", "O", "IR", "IS", "IT")
LANDMINE_VALUE = ("B", "G", "K", "M", "O", "L", "D", "E", "F", "H", "I", "N", "AA")
# Date serials — in the 44 as open values, not useful as thresholds.
SKIP_VALUE_OPS = {"A", "IR"}
TEXT_COLS = {"EQ"}
# Close-same-row numbers that are still fair as *lags*.
LAG_EXTRA = ("O", "AA", "H")
NOW_OPS = (("eq1", "==", 1), ("ge1", ">=", 1), ("lt1", "<", 1), ("gt0", ">", 0))
LAG_OPS = (("lt1", "<", 1), ("eq1", "==", 1), ("ge1", ">=", 1))
LAGS = tuple(range(1, N_LAG + 1))


def load_clocks():
    raw = json.load(open(CLOCK_MAP, encoding="utf-8"))
    by = {r["col"]: r for r in raw["columns"]}
    return raw, by


def assert_locked_gate(clocks):
    """Refuse to invent clocks — Excel's list must match the map."""
    vo = tuple(clocks["groups"]["value_mine_open"])
    fo = tuple(clocks["groups"]["fill_mine_open"])
    if vo != VALUE_OPEN_44:
        raise ValueError(f"value_mine_open drifted: {vo} != locked 44")
    if fo != FILL_OPEN:
        raise ValueError(f"fill_mine_open drifted: {fo} != locked fills")
    if len(VALUE_OPEN_44) != 44:
        raise ValueError("locked value-open list is not 44")
    by = {r["col"]: r for r in clocks["columns"]}
    for col in LANDMINE_VALUE:
        if by[col]["value_mine"] == "open":
            raise ValueError(f"landmine {col} is value-open — do not invent")
    if by["O"]["fill_mine"] != "open" or by["O"]["value_mine"] != "close":
        raise ValueError("O fill/value lock broken")
    if by["AA"]["value_mine"] == "open":
        raise ValueError("AA today is not licensed same-row open")


def atom_name(col, lag, opname):
    return f"{col}_l{lag}_{opname}"


def build_atoms(clocks_by):
    """Open-entry atoms: same-row 44+fills, or any letter at lag≥1."""
    atoms = []
    for col in VALUE_OPEN_44:
        if clocks_by[col]["value_mine"] != "open":
            raise ValueError(f"{col} is not value-open; refuse same-row")
        if col in SKIP_VALUE_OPS:
            continue
        if col in TEXT_COLS:
            for label in ("S", "L"):
                atoms.append({
                    "name": atom_name(col, 0, f"eq{label}"),
                    "col": col, "lag": 0, "kind": "text",
                    "opname": f"eq{label}", "op": "==", "th": label,
                })
            continue
        for opname, op, th in NOW_OPS:
            atoms.append({
                "name": atom_name(col, 0, opname),
                "col": col, "lag": 0, "kind": "num",
                "opname": opname, "op": op, "th": th,
            })
    for col in FILL_OPEN:
        if clocks_by[col]["fill_mine"] != "open":
            raise ValueError(f"{col} fill is not open; refuse same-row fill")
        atoms.append({
            "name": atom_name(col, 0, "green"),
            "col": col, "lag": 0, "kind": "fill",
            "opname": "green", "op": "==", "th": "green",
        })
    # Lags of the 44 (upper rows of open-knowable cols).
    for col in VALUE_OPEN_44:
        if col in SKIP_VALUE_OPS or col in TEXT_COLS:
            continue
        for lag in LAGS:
            for opname, op, th in LAG_OPS:
                atoms.append({
                    "name": atom_name(col, lag, opname),
                    "col": col, "lag": lag, "kind": "num",
                    "opname": opname, "op": op, "th": th,
                })
    # Example-shape lags: O/AA/H numbers at t−1…t−5 only.
    for col in LAG_EXTRA:
        if col in VALUE_OPEN_44:
            raise ValueError(f"{col} is value-open; do not treat as lag-only")
        if clocks_by[col]["value_mine"] == "open":
            raise ValueError(f"{col} became value-open; refuse invented clock")
        for lag in LAGS:
            for opname, op, th in LAG_OPS:
                atoms.append({
                    "name": atom_name(col, lag, opname),
                    "col": col, "lag": lag, "kind": "num",
                    "opname": opname, "op": op, "th": th,
                })
    # No same-row landmine values.
    for a in atoms:
        if a["lag"] == 0 and a["kind"] == "num" and a["col"] in LANDMINE_VALUE:
            raise ValueError(f"landmine same-row value {a['name']}")
        if a["lag"] == 0 and a["kind"] == "num" and a["col"] not in VALUE_OPEN_44:
            raise ValueError(f"same-row value not in locked 44: {a['name']}")
        if a["lag"] == 0 and a["kind"] == "fill" and a["col"] not in FILL_OPEN:
            raise ValueError(f"same-row fill not in locked fills: {a['name']}")
    return atoms


def collapse_twins(atoms):
    """One atom per (col, lag, kind) — prefer ge1, then eq1, then lt1."""
    rank = {"ge1": 3, "eq1": 2, "green": 2, "eqS": 2, "eqL": 2, "lt1": 1, "gt0": 0}
    best = {}
    for a in atoms:
        key = (a["col"], a["lag"], a["kind"])
        cur = best.get(key)
        if cur is None or rank.get(a["opname"], 0) > rank.get(cur["opname"], 0):
            best[key] = a
    return list(best.values())


def build_pairs(alive_atoms):
    """Pair only alive atoms so the 44×44×lag grid does not explode."""
    now = [a for a in alive_atoms if a["lag"] == 0]
    lag = [a for a in alive_atoms if a["lag"] >= 1]
    pairs = []
    for left in lag:
        for right in now:
            if left["name"] == right["name"]:
                continue
            pairs.append({
                "name": f"{left['name']}__and__{right['name']}",
                "left": left["name"],
                "right": right["name"],
            })
    now_s = sorted(now, key=lambda a: a["name"])
    for i, a in enumerate(now_s):
        for b in now_s[i + 1:]:
            if a["col"] == b["col"]:
                continue
            pairs.append({
                "name": f"{a['name']}__and__{b['name']}",
                "left": a["name"],
                "right": b["name"],
            })
    by = {a["name"]: a for a in alive_atoms}
    extra = (
        ("O_l2_lt1", "ES_l0_eq1"),
        ("O_l2_lt1", "AA_l1_eq1"),
        ("O_l1_lt1", "AA_l1_eq1"),
    )
    for a, b in extra:
        if a in by and b in by:
            pairs.append({
                "name": f"{a}__and__{b}",
                "left": a,
                "right": b,
            })
    seen, out = set(), []
    for p in pairs:
        if p["name"] in seen:
            continue
        # Both legs open-knowable: lag≥1 any col, or same-row 44/fills.
        for leg in (p["left"], p["right"]):
            rec = by.get(leg)
            if rec is None:
                break
            if rec["lag"] == 0 and rec["kind"] == "num" and rec["col"] not in VALUE_OPEN_44:
                break
            if rec["lag"] == 0 and rec["kind"] == "fill" and rec["col"] not in FILL_OPEN:
                break
            if rec["lag"] == 0 and rec["col"] in LANDMINE_VALUE and rec["kind"] != "fill":
                break
        else:
            seen.add(p["name"])
            out.append(p)
    return out


def remaining_inventory(clocks, atoms, pairs, n_alive=0):
    return {
        "generated": str(date.today()),
        "live_untouched": "flatten_robust",
        "excel_cache_used": False,
        "entry": "open",
        "gate": "Excel-locked CLOCK_MAP value_mine_open 44 + fill_mine_open",
        "n_lag": N_LAG,
        "value_open_44": list(VALUE_OPEN_44),
        "fill_open": list(FILL_OPEN),
        "landmine_value": list(LANDMINE_VALUE),
        "n_atoms": len(atoms),
        "n_alive": n_alive,
        "n_pairs": len(pairs),
        "same_row_open_n": 44,
        "standing_open": "five-cell light+O ± AH/FR is baseline, not a card",
        "not_reopened": [
            "T/BA close shortboard (highlight ghost)",
            "weekly AP–AU + leftover-open+A lags",
            "same-day multi-letter fill counts",
        ],
        "example_shape": (
            "O[t−2]<1 ∧ AA[t]=1 is close-entry (AA same-row unlicensed); "
            "open siblings are O[t−2]<1 ∧ ES[t]=1 and O[t−2]<1 ∧ AA[t−1]=1"
        ),
        "next": "close-entry pass (AA-today pairs); trees only if a pair KEEPs",
    }


def render_plan(meta):
    return "\n".join([
        "# Pair+lag mine plan (Excel-locked open gate)",
        "",
        f"_Generated {meta['generated']} · live `flatten_robust` frozen. "
        "Yahoo/rows A–F seed only. No merge._",
        "",
        "## Plain English",
        "",
        "Excel locked the same-row open gate. This mine does **not** "
        "invent clocks. Same-row day X is the 44 value-open letters "
        "(numbers/text) plus the timing-tested open fills. Yesterday "
        "and older of any letter is fair. O green is open; O as a "
        "number is close. AA today is close.",
        "",
        "## Locked same-row open values (44)",
        "",
        "`" + ", ".join(VALUE_OPEN_44) + "`",
        "",
        "## Locked same-row open fills (not values)",
        "",
        "`" + ", ".join(FILL_OPEN) + "`",
        "",
        "## Same-row landmines (not open values)",
        "",
        "B/G/K/M numbers; O number; L value; D/E/F/H/I same-row; "
        "core_score → close only. AA today → close.",
        "",
        f"Atoms **{meta['n_atoms']}**. Alive for pairing "
        f"**{meta.get('n_alive', 0)}**. Pairs **{meta['n_pairs']}**. "
        f"Lags 1…{meta['n_lag']}. Entry **open**.",
        "",
        meta["example_shape"] + ".",
        "",
        "Highlight ghosts (T/BA, weekly+lag, same-day fill counts) "
        "are not reopened. Light+O ± AH/FR is baseline, not a card.",
        "",
        "Source: `CLOCK_MAP.md` / `clock_map.json` / "
        "`OPEN_SAME_ROW_LABELS.md`.",
        "",
        "Research only. Live frozen.",
        "",
    ])


def render_inventory(meta, clocks):
    return "\n".join([
        "# Pair+lag inventory (Excel-locked 44)",
        "",
        f"_Generated {meta['generated']} · live `flatten_robust` frozen. "
        "Yahoo/rows only. No merge._",
        "",
        "## Plain English",
        "",
        "Open-entry pairwise + lag on the **Excel-locked 44** value-open "
        "letters, with timing-tested open fills as optional legs. "
        "Lags t−1…t−5 of those letters plus O / AA / H numbers.",
        "",
        f"Same-row values: **44**. Open fills: **{len(FILL_OPEN)}**. "
        f"Atoms {meta['n_atoms']} · alive {meta.get('n_alive', 0)} · "
        f"pairs {meta['n_pairs']}.",
        "",
        "Not reopened: " + "; ".join(meta["not_reopened"]) + ".",
        "",
        "Research only. Live frozen.",
        "",
    ])


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
    }


def fire_atom(days, ei, atom):
    src = ei - atom["lag"]
    if src < 0:
        return False
    rec = days[src]["cells"].get(atom["col"])
    if atom["kind"] == "text":
        v = None if not rec else rec.get("v")
        if not isinstance(v, str):
            return False
        return v.strip().upper() == atom["th"]
    if atom["kind"] == "fill":
        return fam(rec) == atom["th"]
    v = num(rec)
    if v is None:
        return False
    return _cmp(v, atom["op"], atom["th"])


def family_of(name):
    return "pair" if "__and__" in name else "single"


def meaning_of(name):
    def one(tok):
        col, rest = tok.split("_", 1)
        parts = rest.split("_")
        lag = int(parts[0][1:])
        op = parts[1]
        when = "today" if lag == 0 else (
            "yesterday" if lag == 1 else f"{lag} days ago")
        words = {
            "eq1": "equals 1", "ge1": "is at least 1",
            "lt1": "is under 1", "gt0": "is above 0", "lt0": "is below 0",
            "eqS": "is S", "eqL": "is L", "green": "is green",
        }.get(op, op)
        return f"{col} {when} {words}"
    if "__and__" in name:
        a, b = name.split("__and__")
        return f"{one(a)} AND {one(b)}"
    return one(name)


def walk(files, atoms, pairs, spy, disc, hold, baselines=None):
    cells = defaultdict(_cell)
    base = baselines if baselines is not None else defaultdict(lambda: _slot())
    fill_base = baselines is None
    n_bad = 0
    want = {a["name"] for a in atoms}
    for i, path in enumerate(files, 1):
        g = json.load(open(path))
        src, seed = g.get("source"), g.get("seed")
        if (src and src != "rows_cache") or (seed and seed != "yahoo_rows_cache"):
            n_bad += 1
            continue
        t = g["ticker"]
        days = g["days"]
        split_t = ("discovery" if t in disc else "holdout" if t in hold else None)
        if split_t is None:
            continue
        for ei, day in enumerate(days):
            if fill_base:
                for h in HOLDS:
                    raw = sim(days, ei, 1, "open", h)
                    if raw is None:
                        continue
                    _push(base[("open", "long", h)], raw - COST_FUTU_LONG)
            fired = {a["name"] for a in atoms if fire_atom(days, ei, a)}
            if not fired:
                continue
            hit = [n for n in fired if n in want]
            for p in pairs:
                if p["left"] in fired and p["right"] in fired:
                    hit.append(p["name"])
            if not hit:
                continue
            iso = str(s2d(day["date"]))
            tape = spy.get(iso, 0)
            half = "early" if iso < HALF_CUT else "late"
            qkey = "q12" if iso < Q3_CUT else "q3"
            mon = iso[:7]
            for name in hit:
                for h in HOLDS:
                    raw = sim(days, ei, 1, "open", h)
                    if raw is None:
                        continue
                    net = raw - COST_FUTU_LONG
                    key = (name, "open", "long", f"hold{h}")
                    c = cells[key]
                    _push(c["disc"] if split_t == "discovery" else c["hold"], net)
                    _push(c[half], net)
                    _push(c[qkey], net)
                    if iso < Q1_CUT:
                        _push(c["q1"], net)
                    if tape == 1:
                        _push(c["spy_up"], net)
                    elif tape == -1:
                        _push(c["spy_dn"], net)
                    _push(c["month"][mon], net)
                    c["tickers"].add(t)
                    c["dates"].add(iso)
                    c["ticker_pnl"][t] += net
                    if split_t == "discovery":
                        c["day"][iso] += net
        if i % 400 == 0:
            print(f"  ... {i}/{len(files)}", flush=True)
    if n_bad:
        raise RuntimeError(f"refusing {n_bad} dumps that are not rows_cache")
    return cells, base


def pack_cells(cells, baselines):
    rows = []
    for (name, clock, side, rule), cell in cells.items():
        row = pack_row(name, clock, side, rule, cell, baselines)
        if not row:
            continue
        row["family"] = family_of(name)
        deeper(row, cell)
        rows.append(row)
    apply_hold1_keep(rows)
    for r in rows:
        r["keep"] = r["verdict"]
        r["plain"] = meaning_of(r["def"])
    return rows


def select_alive(rows, atoms):
    """Hold2 singles with enough discovery mass and a green mean."""
    by_name = {a["name"]: a for a in atoms}
    cand = []
    for r in rows:
        if r["exit"] != "hold2" or r["family"] != "single":
            continue
        d = r.get("discovery") or {}
        if d.get("n", 0) < 200 or (d.get("avg_net") or 0) <= 0:
            continue
        a = by_name.get(r["def"])
        if a:
            cand.append((r, a))
    # Collapse twins, keep best discovery t.
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
    must = {"O_l2_lt1", "AA_l1_eq1", "ES_l0_eq1", "O_l0_green"}
    have = {a["name"] for a in picked}
    for a in atoms:
        if a["name"] in must and a["name"] not in have:
            picked.append(a)
            have.add(a["name"])
    return picked


def mine(atoms, limit=0):
    split = json.load(open(SPLIT_PATH))
    disc, hold = set(split["discovery"]), set(split["holdout"])
    spy = load_spy()
    files = [f for f in sorted(glob.glob(os.path.join(DUMPS, "*.json")))
             if not os.path.basename(f).startswith("_")]
    if limit:
        files = files[:limit]
    print(f"[pair_lag] pass1 singles files={len(files)} atoms={len(atoms)}",
          flush=True)
    cells1, base = walk(files, atoms, [], spy, disc, hold, baselines=None)
    baselines = {}
    for k, sl in base.items():
        b = _blk(sl)
        if b:
            baselines[f"{k[0]}_{k[1]}_h{k[2]}"] = b
    singles = pack_cells(cells1, baselines)
    alive = select_alive(singles, atoms)
    pairs = build_pairs(alive)
    print(f"[pair_lag] pass2 pairs alive={len(alive)} pairs={len(pairs)}",
          flush=True)
    need = {a["name"]: a for a in atoms if a["name"] in {
        p["left"] for p in pairs
    } | {p["right"] for p in pairs}}
    cells2, _ = walk(files, list(need.values()), pairs, spy, disc, hold,
                     baselines=base)
    pairs_rows = pack_cells(cells2, baselines)
    # walk() on pass2 also re-pushes the need-atoms as singles — drop those
    # so singles stay from pass1.
    pair_only = [r for r in pairs_rows if r["family"] == "pair"]
    rows = singles + pair_only
    rows.sort(key=lambda r: (
        0 if r["keep"] == "KEEP" else 1 if r["keep"] == "KILL" else 2,
        0 if r["family"] == "pair" else 1,
        -((r.get("holdout") or {}).get("t") or -9),
    ))
    return rows, baselines, files, pairs, alive


def _pct(b):
    if not b or b.get("avg_net") is None:
        return "—"
    return f"{b['avg_net']*100:+.2f}% (n={b['n']})"


def render(rows, baselines, files, meta):
    keeps = [r for r in rows if r["keep"] == "KEEP"]
    kills = [r for r in rows if r["keep"] == "KILL"]
    thins = [r for r in rows if r["keep"] == "THIN"]
    by_fam = defaultdict(lambda: {"KEEP": 0, "KILL": 0, "THIN": 0})
    for r in rows:
        by_fam[r["family"]][r["keep"]] += 1
    pair_keep = by_fam["pair"]["KEEP"]
    L = [
        MARKER,
        "",
        f"_Generated {date.today()} · live `flatten_robust` frozen. "
        "Yahoo/rows A–F seed only. No merge. Excel-locked 44. "
        "T/BA highlight ghosts not reopened._",
        "",
        "## Plain English",
        "",
        "Open-entry **pair + lag** on Excel's locked same-row gate: "
        "the 44 value-open letters (numbers/text) and the timing-tested "
        "open fills. Yesterday-and-older of any letter is fair. "
        "O green is open; O as a number is close. AA today is close — "
        "O two days ago under 1 **and** AA today equals 1 waits for "
        "close-entry. Open siblings (O two days ago under 1 and today's "
        "ES equals 1; or yesterday's AA equals 1) are in this pilot.",
        "",
    ]
    if keeps:
        L.append(
            f"**Pilot KEEP {len(keeps)}** · KILL {len(kills)} · "
            f"THIN {len(thins)}. Research only — not a card. "
            + (
                "A pair cleared the bar; trees are next, not this beat."
                if pair_keep else
                "Keeps are singles only; trees stay skipped."
            )
        )
    else:
        L.append(
            f"**Clean null.** KEEP 0 · KILL {len(kills)} · THIN {len(thins)}. "
            "The locked 44 plus open-fill legs and lags do not clear "
            "ship + Q1 / July / five-name on this tape. Trees skipped. "
            "Next: close-entry (AA-today pairs) — not a remine of T/BA."
        )
    L += [
        "",
        f"Dumps **{len(files)}**. Atoms **{meta['n_atoms']}**. "
        f"Alive **{meta.get('n_alive', 0)}**. Pairs **{meta['n_pairs']}**. "
        f"Open-long everyone-else hold2 {_pct(baselines.get('open_long_h2'))}. "
        "Both SPY tapes required. Light+O ± AH/FR baseline.",
        "",
        "### Family scoreboard",
        "",
        "| family | what it is | KEEP | KILL | THIN |",
        "|---|---|---:|---:|---:|",
        f"| single | locked-44 number/text, open fill, or a lag | "
        f"{by_fam['single']['KEEP']} | {by_fam['single']['KILL']} | "
        f"{by_fam['single']['THIN']} |",
        f"| pair | two open-knowable legs (lag any col ∨ same-row 44/fill) | "
        f"{by_fam['pair']['KEEP']} | {by_fam['pair']['KILL']} | "
        f"{by_fam['pair']['THIN']} |",
        "",
        "### Pilot table (hold2, English first)",
        "",
        "| meaning | clock | holdout | vs everyone | Q1 | spy↑ | spy↓ | "
        "July | top-5 | verdict | why |",
        "|---|---|---|---|---|---|---|---|---|---|---|",
    ]
    shown = [r for r in keeps if r["exit"] == "hold2"]
    shown += [r for r in rows if r["keep"] != "KEEP" and r["exit"] == "hold2"]
    shown = shown[:28]
    if not shown:
        L.append("| — | — | — | — | — | — | — | — | — | — | — |")
    for r in shown:
        vs = "—"
        if r.get("holdout") and r.get("baseline"):
            vs = (f"{(r['holdout']['avg_net'] - r['baseline']['avg_net'])*100:+.2f} pp")
        L.append(
            f"| {r.get('plain') or meaning_of(r['def'])} | open | "
            f"{_pct(r.get('holdout'))} | {vs} | {_pct(r.get('q1'))} | "
            f"{_pct(r.get('spy_up'))} | {_pct(r.get('spy_dn'))} | "
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
        "### What this does not change",
        "",
        "- Standing open keeps stay **five-cell light + green O**, "
        "with or without AH/FR. Baseline, not a card.",
        "- T / BA / CZ / EH / IB / HO / IL / GV stay **KILL** as "
        "highlight / leftover-close ghosts.",
        "- Weekly+lag and same-day fill counts stay exhausted.",
        "- Close-entry pair+lag (AA today) is **next**.",
        "- Finviz BLOCKED. Live `flatten_robust` frozen. No cards.",
        f"- Q1 cut **{Q1_CUT}**. Q3 cut **{Q3_CUT}**. Futubull 0.15%/0.20%.",
        "",
        "Research only. One 2026 regime.",
        "",
    ]
    return "\n".join(L)


def write_outputs(rows, baselines, files, meta, clocks):
    open(PLAN_MD, "w", encoding="utf-8").write(render_plan(meta))
    open(INV_MD, "w", encoding="utf-8").write(render_inventory(meta, clocks))
    md = render(rows, baselines, files, meta)
    open(OUT_MD, "w", encoding="utf-8").write(md + "\n")
    n_keep = sum(1 for r in rows if r["keep"] == "KEEP")
    n_kill = sum(1 for r in rows if r["keep"] == "KILL")
    n_thin = sum(1 for r in rows if r["keep"] == "THIN")
    payload = {
        "generated": str(date.today()),
        "spec": "open-entry pair+lag on Excel-locked 44; ship + Q1/name-ghost",
        "live_untouched": "flatten_robust",
        "excel_cache_used": False,
        "seed": "yahoo_rows_cache",
        "entry": "open",
        "gate": meta["gate"],
        "n_dumps": len(files),
        "n_atoms": meta["n_atoms"],
        "n_alive": meta.get("n_alive", 0),
        "n_pairs": meta["n_pairs"],
        "q1_cut": Q1_CUT,
        "q3_cut": Q3_CUT,
        "standing_open": meta["standing_open"],
        "finviz": "BLOCKED",
        "trees": "skipped_no_pair_keep" if not any(
            r["keep"] == "KEEP" and r["family"] == "pair" for r in rows
        ) else "deferred_pair_keep",
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
        f"Pair+lag open (locked 44): KEEP {n_keep} · KILL {n_kill} · "
        f"THIN {n_thin}. Trees {payload['trees']}. Light+O baseline. "
        "See `PAIR_LAG.md` / `OPEN_SAME_ROW_LABELS.md`.\n",
        require_any=("first A–JL cut", "A–O clock cycle"),
    )
    open(SB_MD, "w", encoding="utf-8").write(sb)
    open(AO_MD, "w", encoding="utf-8").write(ao)
    open(CYCLE_MD, "w", encoding="utf-8").write(cy)
    return payload


def stamp_labels():
    """Keep OPEN_SAME_ROW_LABELS.md as the Excel lock; do not invent."""
    if not os.path.exists(LABELS_MD):
        raise FileNotFoundError("OPEN_SAME_ROW_LABELS.md missing — Excel gate")
    old = open(LABELS_MD, encoding="utf-8").read()
    if "A, C, J, Q, Z, AC, AH, BT, BV, CG, CH, DC, DE" not in old:
        raise ValueError("labels file does not carry the locked 44")
    if "fill_mine_open" not in old and "A, B, C, G, J, K, L, M, O" not in old:
        raise ValueError("labels file missing open fills")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--render-only", action="store_true")
    ap.add_argument("--limit", type=int, default=0)
    args = ap.parse_args()
    stamp_labels()
    clocks, by = load_clocks()
    if "same_row_open" not in clocks:
        from classify_clocks import build
        clocks = build()
        by = {r["col"]: r for r in clocks["columns"]}
    assert_locked_gate(clocks)
    atoms = build_atoms(by)
    if args.render_only:
        prev = json.load(open(OUT_JSON, encoding="utf-8"))
        files = [None] * int(prev.get("n_dumps") or 0)
        payload = write_outputs(
            prev.get("rows") or [],
            prev.get("baselines") or {},
            files,
            prev.get("inventory") or remaining_inventory(clocks, atoms, []),
            clocks,
        )
        print(f"render-only KEEP {payload['n_keep']} KILL {payload['n_kill']}",
              flush=True)
        return payload
    rows, baselines, files, pairs, alive = mine(atoms, limit=args.limit)
    meta = remaining_inventory(clocks, atoms, pairs, n_alive=len(alive))
    payload = write_outputs(rows, baselines, files, meta, clocks)
    print(f"KEEP {payload['n_keep']} KILL {payload['n_kill']} "
          f"THIN {payload['n_thin']} atoms={len(atoms)} alive={len(alive)} "
          f"pairs={len(pairs)} trees={payload['trees']}", flush=True)
    for r in rows[:16]:
        print(f"  {r['keep']:4} {r['def'][:48]:48} {r['exit']} "
              f"{','.join(r.get('fail_reasons') or [])}", flush=True)
    return payload


if __name__ == "__main__":
    main()
