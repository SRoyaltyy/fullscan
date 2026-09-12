"""Pairwise + lag miner — close-entry, Excel-locked same-row gate.

Open-entry locked-44 is accepted (KEEP 0 / KILL 4266). This beat lets
AA-today and other value-close same-row cols be the today leg. Lags of
any letter in the pilot set stay fair. Open landmines stay open — the
44 and open fills are not treated as close-today.

O green fill is open (not a close fill). O number is close.
B/G/K/M numbers are close; their fills stay open.
D/E/F/H/I/N same-row values and fills are close.
core_score → close only. Highlight ghosts are not the search.

Bounded first: A–O value-close + AA. Then expand through AO, skipping
T/BA and the other killed highlight letters.

  python engine/mine_pair_lag_close.py
  python engine/mine_pair_lag_close.py --bound
  python engine/mine_pair_lag_close.py --render-only
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
from mine_pair_lag import (  # noqa: E402
    FILL_OPEN, LAGS, N_LAG, PAIR_TOP, SKIP_VALUE_OPS, TEXT_COLS,
    VALUE_OPEN_44, atom_name, collapse_twins, family_of, fire_atom,
    load_clocks, meaning_of, stamp_labels,
)
from mine_unmined import (  # noqa: E402
    HALF_CUT, Q3_CUT, _blk, _push, _slot, pack_row, s2d, sim,
)
from mine_unmined import load_spy  # noqa: E402

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
REPO = os.path.dirname(ROOT)
DUMPS = os.environ.get("ALL_COLS_DIR", os.path.join(ROOT, "research", "all_cols_sample"))
SPLIT_PATH = os.path.join(HERE, "holdout_split.json")
RESEARCH = os.path.join(ROOT, "research")
SCOREBOARD = os.path.join(REPO, "03_scoreboard")
OUT_MD = os.path.join(RESEARCH, "PAIR_LAG_CLOSE.md")
OUT_JSON = os.path.join(RESEARCH, "pair_lag_close.json")
INV_MD = os.path.join(RESEARCH, "PAIR_LAG_CLOSE_INVENTORY.md")
PLAN_MD = os.path.join(RESEARCH, "PAIR_LAG_CLOSE_PLAN.md")
SB_MD = os.path.join(SCOREBOARD, "EXCEL_BOT_MINE.md")
AO_MD = os.path.join(RESEARCH, "AO_FIRST_MINE.md")
CYCLE_MD = os.path.join(RESEARCH, "MINE_CYCLE.md")
MARKER = "## Pair+lag mine (close-entry)"
HOLDS = (1, 2)

# Named landmines that become legal today legs at close.
VALUE_CLOSE_CORE = (
    "B", "D", "E", "F", "G", "H", "I", "K", "L", "M", "N", "O", "AA",
)
# Expand through AO. Skip open-44, skip killed highlight letters.
VALUE_CLOSE_EXPAND = (
    "P", "R", "S", "U", "V", "W", "X", "Y",
    "AB", "AD", "AE", "AF", "AG", "AI", "AJ", "AK", "AL", "AM", "AN", "AO",
)
# Timing-tested close fills only. Open fills are not close.
FILL_CLOSE_TODAY = ("D", "E", "F", "H", "I", "N")
# Do not search killed highlight ghosts (fills or values) as today legs.
HIGHLIGHT_GHOSTS = ("T", "BA", "CZ", "EH", "IB", "HO", "IL", "GV")
NOW_OPS = (("eq1", "==", 1), ("ge1", ">=", 1), ("lt1", "<", 1), ("gt0", ">", 0))
LAG_OPS = (("lt1", "<", 1), ("eq1", "==", 1), ("ge1", ">=", 1))


def value_close_today(bound=False):
    cols = VALUE_CLOSE_CORE if bound else VALUE_CLOSE_CORE + VALUE_CLOSE_EXPAND
    return tuple(c for c in cols if c not in HIGHLIGHT_GHOSTS)


def assert_close_gate(clocks, today):
    """Refuse to invent clocks or treat open landmines as close."""
    vo = tuple(clocks["groups"]["value_mine_open"])
    fo = tuple(clocks["groups"]["fill_mine_open"])
    vc = set(clocks["groups"]["value_mine_close"])
    fc = set(clocks["groups"]["fill_mine_close"])
    if vo != VALUE_OPEN_44:
        raise ValueError(f"value_mine_open drifted: {vo} != locked 44")
    if fo != FILL_OPEN:
        raise ValueError(f"fill_mine_open drifted: {fo} != locked fills")
    by = {r["col"]: r for r in clocks["columns"]}
    for col in today:
        if col in VALUE_OPEN_44:
            raise ValueError(f"{col} is value-open — not a close-today leg")
        if col in HIGHLIGHT_GHOSTS:
            raise ValueError(f"{col} is a killed highlight ghost")
        if col not in vc or by[col]["value_mine"] != "close":
            raise ValueError(f"{col} is not value_mine_close")
    for col in FILL_CLOSE_TODAY:
        if col in FILL_OPEN:
            raise ValueError(f"{col} fill is open — not a close fill")
        if col not in fc or by[col]["fill_mine"] != "close":
            raise ValueError(f"{col} fill is not fill_mine_close")
    if by["O"]["fill_mine"] != "open" or by["O"]["value_mine"] != "close":
        raise ValueError("O fill/value lock broken")
    if by["AA"]["value_mine"] != "close":
        raise ValueError("AA today must stay value-close")
    for col in ("B", "G", "K", "M"):
        if by[col]["fill_mine"] != "open" or by[col]["value_mine"] != "close":
            raise ValueError(f"{col} fill-open / value-close lock broken")


def build_atoms(clocks_by, today):
    """Close-entry atoms: same-row value-close (+ close fills), lags of the set."""
    atoms = []
    for col in today:
        if clocks_by[col]["value_mine"] != "close":
            raise ValueError(f"{col} is not value-close; refuse close-today")
        if col in VALUE_OPEN_44:
            raise ValueError(f"{col} is value-open; do not treat as close")
        if col in SKIP_VALUE_OPS:
            continue
        for opname, op, th in NOW_OPS:
            atoms.append({
                "name": atom_name(col, 0, opname),
                "col": col, "lag": 0, "kind": "num",
                "opname": opname, "op": op, "th": th,
                "role": "close_today",
            })
    for col in FILL_CLOSE_TODAY:
        if clocks_by[col]["fill_mine"] != "close":
            raise ValueError(f"{col} fill is not close; refuse")
        if col in FILL_OPEN:
            raise ValueError(f"{col} fill is open; do not treat as close")
        atoms.append({
            "name": atom_name(col, 0, "green"),
            "col": col, "lag": 0, "kind": "fill",
            "opname": "green", "op": "==", "th": "green",
            "role": "close_today",
        })
    lag_cols = []
    for col in VALUE_OPEN_44:
        if col in SKIP_VALUE_OPS or col in TEXT_COLS:
            continue
        lag_cols.append(col)
    for col in today:
        if col not in lag_cols:
            lag_cols.append(col)
    for col in lag_cols:
        for lag in LAGS:
            for opname, op, th in LAG_OPS:
                atoms.append({
                    "name": atom_name(col, lag, opname),
                    "col": col, "lag": lag, "kind": "num",
                    "opname": opname, "op": op, "th": th,
                    "role": "lag",
                })
    for a in atoms:
        if a["lag"] == 0 and a["kind"] == "num":
            if a["col"] in VALUE_OPEN_44:
                raise ValueError(f"open value treated as close-today: {a['name']}")
            if a["col"] not in today:
                raise ValueError(f"same-row value not in close-today: {a['name']}")
        if a["lag"] == 0 and a["kind"] == "fill":
            if a["col"] in FILL_OPEN:
                raise ValueError(f"open fill treated as close: {a['name']}")
            if a["col"] not in FILL_CLOSE_TODAY:
                raise ValueError(f"same-row fill not timing-close: {a['name']}")
        if a["lag"] == 0 and a["col"] in HIGHLIGHT_GHOSTS:
            raise ValueError(f"highlight ghost today: {a['name']}")
    return atoms


def build_pairs(alive_atoms, today):
    """Pair lag × close-today and close-today × close-today."""
    close_now = [
        a for a in alive_atoms
        if a["lag"] == 0 and (
            (a["kind"] == "num" and a["col"] in today)
            or (a["kind"] == "fill" and a["col"] in FILL_CLOSE_TODAY)
        )
    ]
    lag = [a for a in alive_atoms if a["lag"] >= 1]
    pairs = []
    for left in lag:
        for right in close_now:
            pairs.append({
                "name": f"{left['name']}__and__{right['name']}",
                "left": left["name"],
                "right": right["name"],
            })
    now_s = sorted(close_now, key=lambda a: a["name"])
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
    extra = (("O_l2_lt1", "AA_l0_eq1"), ("O_l1_lt1", "AA_l0_eq1"))
    for a, b in extra:
        if a in by and b in by:
            pairs.append({
                "name": f"{a}__and__{b}",
                "left": a,
                "right": b,
            })
    today_set = set(today)
    seen, out = set(), []
    for p in pairs:
        if p["name"] in seen:
            continue
        recs = [by.get(p["left"]), by.get(p["right"])]
        if any(r is None for r in recs):
            continue
        has_close_today = False
        ok = True
        for rec in recs:
            if rec["lag"] == 0 and rec["kind"] == "num" and rec["col"] in VALUE_OPEN_44:
                ok = False
                break
            if rec["lag"] == 0 and rec["kind"] == "fill" and rec["col"] in FILL_OPEN:
                ok = False
                break
            if rec["lag"] == 0 and rec["col"] in HIGHLIGHT_GHOSTS:
                ok = False
                break
            if rec["lag"] == 0 and (
                (rec["kind"] == "num" and rec["col"] in today_set)
                or (rec["kind"] == "fill" and rec["col"] in FILL_CLOSE_TODAY)
            ):
                has_close_today = True
        if ok and has_close_today:
            seen.add(p["name"])
            out.append(p)
    return out


def remaining_inventory(today, atoms, pairs, n_alive=0, bound=False):
    return {
        "generated": str(date.today()),
        "live_untouched": "flatten_robust",
        "excel_cache_used": False,
        "entry": "close",
        "gate": (
            "Excel-locked CLOCK_MAP value_mine_close today + "
            "fill_mine_close timing fills; open 44/fills not close-today"
        ),
        "bound": bound,
        "n_lag": N_LAG,
        "value_open_44": list(VALUE_OPEN_44),
        "fill_open": list(FILL_OPEN),
        "value_close_today": list(today),
        "fill_close_today": list(FILL_CLOSE_TODAY),
        "highlight_ghosts_excluded": list(HIGHLIGHT_GHOSTS),
        "n_atoms": len(atoms),
        "n_alive": n_alive,
        "n_pairs": len(pairs),
        "standing_open": "five-cell light+O ± AH/FR is baseline, not a card",
        "not_reopened": [
            "T/BA close shortboard (highlight ghost)",
            "CZ/EH/IB/HO/IL/GV leftover-close ghosts",
            "weekly AP–AU + leftover-open+A lags",
            "same-day multi-letter fill counts",
            "open-entry locked-44 pair+lag (accepted null)",
        ],
        "example_shape": (
            "O[t−2]<1 ∧ AA[t]=1 is close-entry (AA same-row is value-close); "
            "O green today is still open, not a close fill"
        ),
        "next": "trees only if a pair KEEPs; else stop this pair+lag line",
    }


def render_plan(meta):
    today = meta["value_close_today"]
    return "\n".join([
        "# Pair+lag mine plan (close-entry)",
        "",
        f"_Generated {meta['generated']} · live `flatten_robust` frozen. "
        "Yahoo/rows A–F seed only. No merge._",
        "",
        "## Plain English",
        "",
        "Open-entry on the locked 44 is a clean null. This mine enters "
        "at the **close**. AA today and the other value-close same-row "
        "letters are legal today legs. Yesterday-and-older of the pilot "
        "letters is fair. The 44 value-open letters and the timing-tested "
        "open fills are **not** treated as close — O green stays open; "
        "O as a number is close. B/G/K/M fills stay open; those numbers "
        "are close.",
        "",
        "Bounded first: A–O value-close + AA. Then expand through AO, "
        "skipping killed highlight letters (T/BA and peers).",
        "",
        "## Close-today values (this pilot)",
        "",
        "`" + ", ".join(today) + "`",
        "",
        "## Close-today fills (timing-close only)",
        "",
        "`" + ", ".join(FILL_CLOSE_TODAY) + "`",
        "",
        "## Not close-today (open landmines stay open)",
        "",
        "Value-open 44. Open fills A,B,C,G,J,K,L,M,O (+ IR/IS/IT). "
        "O green. Light+O ± AH/FR is baseline, not this search.",
        "",
        f"Atoms **{meta['n_atoms']}**. Alive for pairing "
        f"**{meta.get('n_alive', 0)}**. Pairs **{meta['n_pairs']}**. "
        f"Lags 1…{meta['n_lag']}. Entry **close**.",
        "",
        meta["example_shape"] + ".",
        "",
        "Highlight ghosts are not reopened as the search. "
        "Source: `CLOCK_MAP.md` / `clock_map.json` / "
        "`OPEN_SAME_ROW_LABELS.md`.",
        "",
        "Research only. Live frozen.",
        "",
    ])


def render_inventory(meta):
    return "\n".join([
        "# Pair+lag inventory (close-entry)",
        "",
        f"_Generated {meta['generated']} · live `flatten_robust` frozen. "
        "Yahoo/rows only. No merge._",
        "",
        "## Plain English",
        "",
        "Close-entry pairwise + lag. Today legs are value-close same-row "
        "letters (AA and the A–AO expand) plus timing-close fills. "
        "Lags t−1…t−5 of the locked 44 and those close letters.",
        "",
        f"Close-today values: **{len(meta['value_close_today'])}**. "
        f"Close-today fills: **{len(FILL_CLOSE_TODAY)}**. "
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
                    raw = sim(days, ei, 1, "close", h)
                    if raw is None:
                        continue
                    _push(base[("close", "long", h)], raw - COST_FUTU_LONG)
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
                    raw = sim(days, ei, 1, "close", h)
                    if raw is None:
                        continue
                    net = raw - COST_FUTU_LONG
                    key = (name, "close", "long", f"hold{h}")
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
    spy = load_spy()
    files = [f for f in sorted(glob.glob(os.path.join(DUMPS, "*.json")))
             if not os.path.basename(f).startswith("_")]
    if limit:
        files = files[:limit]
    print(f"[pair_lag_close] pass1 singles files={len(files)} atoms={len(atoms)}",
          flush=True)
    cells1, base = walk(files, atoms, [], spy, disc, hold, baselines=None)
    baselines = {}
    for k, sl in base.items():
        b = _blk(sl)
        if b:
            baselines[f"{k[0]}_{k[1]}_h{k[2]}"] = b
    singles = pack_cells(cells1, baselines)
    alive = select_alive(singles, atoms)
    pairs = build_pairs(alive, today)
    print(f"[pair_lag_close] pass2 pairs alive={len(alive)} pairs={len(pairs)}",
          flush=True)
    need = {a["name"]: a for a in atoms if a["name"] in {
        p["left"] for p in pairs
    } | {p["right"] for p in pairs}}
    cells2, _ = walk(files, list(need.values()), pairs, spy, disc, hold,
                     baselines=base)
    pairs_rows = pack_cells(cells2, baselines)
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
        "Yahoo/rows A–F seed only. No merge. Close-entry. "
        "Excel-locked gate. T/BA highlight ghosts not the search._",
        "",
        "## Plain English",
        "",
        "Close-entry **pair + lag**. AA today and the other value-close "
        "same-row letters are legal today legs. Yesterday-and-older of "
        "the pilot letters is fair. The locked 44 and open fills are "
        "not treated as close. O green is still open; O as a number is "
        "close. O two days ago under 1 **and** AA today equals 1 is in "
        "this board.",
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
            "Close-today AA / O-number / A–AO value-close plus timing-close "
            "fills and lags do not clear ship + Q1 / July / five-name on "
            "this tape. Trees skipped. Not a remine of T/BA or light+O."
        )
    L += [
        "",
        f"Dumps **{len(files)}**. Atoms **{meta['n_atoms']}**. "
        f"Alive **{meta.get('n_alive', 0)}**. Pairs **{meta['n_pairs']}**. "
        f"Close-long everyone-else hold2 {_pct(baselines.get('close_long_h2'))}. "
        "Both SPY tapes required. Light+O ± AH/FR baseline.",
        "",
        "### Family scoreboard",
        "",
        "| family | what it is | KEEP | KILL | THIN |",
        "|---|---|---:|---:|---:|",
        f"| single | value-close today, close fill, or a lag | "
        f"{by_fam['single']['KEEP']} | {by_fam['single']['KILL']} | "
        f"{by_fam['single']['THIN']} |",
        f"| pair | lag × close-today, or two close-today legs | "
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
            f"| {r.get('plain') or meaning_of(r['def'])} | close | "
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
        "- Open-entry locked-44 pair+lag stays the accepted null "
        "(KEEP 0 / KILL 4266).",
        "- T / BA / CZ / EH / IB / HO / IL / GV stay **KILL** as "
        "highlight / leftover-close ghosts — not this search.",
        "- Weekly+lag and same-day fill counts stay exhausted.",
        "- Finviz BLOCKED. Live `flatten_robust` frozen. No cards.",
        f"- Q1 cut **{Q1_CUT}**. Q3 cut **{Q3_CUT}**. Futubull 0.15%/0.20%.",
        "",
        "Research only. One 2026 regime.",
        "",
    ]
    return "\n".join(L)


def write_outputs(rows, baselines, files, meta):
    open(PLAN_MD, "w", encoding="utf-8").write(render_plan(meta))
    open(INV_MD, "w", encoding="utf-8").write(render_inventory(meta))
    md = render(rows, baselines, files, meta)
    open(OUT_MD, "w", encoding="utf-8").write(md + "\n")
    n_keep = sum(1 for r in rows if r["keep"] == "KEEP")
    n_kill = sum(1 for r in rows if r["keep"] == "KILL")
    n_thin = sum(1 for r in rows if r["keep"] == "THIN")
    payload = {
        "generated": str(date.today()),
        "spec": "close-entry pair+lag; AA-today legal; open 44 not close-today",
        "live_untouched": "flatten_robust",
        "excel_cache_used": False,
        "seed": "yahoo_rows_cache",
        "entry": "close",
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
        f"Pair+lag close (AA-today): KEEP {n_keep} · KILL {n_kill} · "
        f"THIN {n_thin}. Trees {payload['trees']}. Light+O baseline. "
        "See `PAIR_LAG_CLOSE.md` / `OPEN_SAME_ROW_LABELS.md`.\n",
        require_any=("first A–JL cut", "A–O clock cycle"),
    )
    open(SB_MD, "w", encoding="utf-8").write(sb)
    open(AO_MD, "w", encoding="utf-8").write(ao)
    open(CYCLE_MD, "w", encoding="utf-8").write(cy)
    return payload


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--render-only", action="store_true")
    ap.add_argument("--bound", action="store_true",
                    help="A–O value-close + AA only (no AO expand)")
    ap.add_argument("--limit", type=int, default=0)
    args = ap.parse_args()
    stamp_labels()
    clocks, by = load_clocks()
    if "same_row_open" not in clocks:
        from classify_clocks import build
        clocks = build()
        by = {r["col"]: r for r in clocks["columns"]}
    today = value_close_today(bound=args.bound)
    assert_close_gate(clocks, today)
    atoms = build_atoms(by, today)
    if args.render_only:
        prev = json.load(open(OUT_JSON, encoding="utf-8"))
        files = [None] * int(prev.get("n_dumps") or 0)
        payload = write_outputs(
            prev.get("rows") or [],
            prev.get("baselines") or {},
            files,
            prev.get("inventory") or remaining_inventory(
                today, atoms, [], bound=args.bound),
        )
        print(f"render-only KEEP {payload['n_keep']} KILL {payload['n_kill']}",
              flush=True)
        return payload
    rows, baselines, files, pairs, alive = mine(atoms, today, limit=args.limit)
    meta = remaining_inventory(
        today, atoms, pairs, n_alive=len(alive), bound=args.bound)
    payload = write_outputs(rows, baselines, files, meta)
    print(f"KEEP {payload['n_keep']} KILL {payload['n_kill']} "
          f"THIN {payload['n_thin']} atoms={len(atoms)} alive={len(alive)} "
          f"pairs={len(pairs)} trees={payload['trees']}", flush=True)
    for r in rows[:16]:
        print(f"  {r['keep']:4} {r['def'][:48]:48} {r['exit']} "
              f"{','.join(r.get('fail_reasons') or [])}", flush=True)
    return payload


if __name__ == "__main__":
    main()
