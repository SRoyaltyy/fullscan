"""Same-day multi-letter counts/joins — last A–JL threshold-family beat.

Single-letter leftover space, weekly AP–AU, and leftover-open+A lags
are already exhausted. This beat scores the remaining *kind*: how many
leftover letters fire on the same day (counts), and a few named AND
joins of leftover letters that are not T/BA.

Ship bar + deeper Q1 / July-share / five-name bar before KEEP.
Does not remine leftover four singles, weekly/lag, or T/BA.
Does not reopen light+O / AH/FR.

Research only. Live flatten_robust is not imported or changed.

  python engine/mine_same_day.py
  python engine/mine_same_day.py --render-only
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
from clock import COST_FUTU_LONG, COST_FUTU_SHORT  # noqa: E402
from harden_hyst_open import apply_hold1_keep, splice_md  # noqa: E402
from mine_next_region import Q1_CUT, deeper  # noqa: E402
from mine_unmined import (  # noqa: E402
    HALF_CUT, Q3_CUT, SKIP_OHLC, SKIP_WEEKLY, _blk, _cmp, _push, _slot,
    fam, inventory, load_clocks, load_spy, num, pack_row, s2d, sim,
)

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
REPO = os.path.dirname(ROOT)
DUMPS = os.environ.get("ALL_COLS_DIR", os.path.join(ROOT, "research", "all_cols_sample"))
HARDEN = os.path.join(ROOT, "research", "unmined_harden.json")
SPLIT_PATH = os.path.join(HERE, "holdout_split.json")
RESEARCH = os.path.join(ROOT, "research")
SCOREBOARD = os.path.join(REPO, "03_scoreboard")
OUT_MD = os.path.join(RESEARCH, "SAME_DAY.md")
OUT_JSON = os.path.join(RESEARCH, "same_day.json")
INV_MD = os.path.join(RESEARCH, "SAME_DAY_INVENTORY.md")
SB_MD = os.path.join(SCOREBOARD, "EXCEL_BOT_MINE.md")
AO_MD = os.path.join(RESEARCH, "AO_FIRST_MINE.md")
CYCLE_MD = os.path.join(RESEARCH, "MINE_CYCLE.md")
MARKER = "## Same-day multi-letter counts (A–JL)"
HOLDS = (1, 2)

# Already scored and killed, or standing open. Do not remine.
SHORTBOARD = {"T", "BA", "CZ", "EH", "IB", "HO", "IL", "GV"}
SKIP_TBA = {"T", "BA"}
STANDING_OPEN = {"AH", "FR"}
# Named AND joins of leftover letters that are not T/BA / peers.
AND_PAIRS = (("FK", "FL"), ("U", "R"), ("EJ", "AM"), ("IK", "FC"))
COUNT_KS = (2, 3, 4)
FILL_KS = (3, 5, 7)
OPS = {
    "eq1": ("==", 1),
    "ge1": (">=", 1),
    "ge2": (">=", 2),
    "gt0": (">", 0),
    "le-1": ("<=", -1),
    "le-2": ("<=", -2),
    "lt0": ("<", 0),
}


def load_survivors():
    raw = json.load(open(HARDEN, encoding="utf-8"))
    out = []
    for r in raw.get("survivors") or []:
        if r.get("keep") != "KEEP" or r.get("exit") != "hold2":
            continue
        letter = r.get("letter")
        if not letter:
            continue
        out.append({
            "letter": letter,
            "def": r["def"],
            "clock": r["clock"],
            "family": r.get("family"),
            "atom": parse_def(r["def"], letter),
        })
    return out


def parse_def(defn, letter):
    if defn.startswith("fill_") and defn.endswith("_green"):
        return ("fill", letter, "green")
    tail = defn.split("_")[-1]
    op = OPS.get(tail)
    if op is None:
        raise ValueError(f"unparsed survivor def {defn}")
    return ("val", letter, op)


def pools(survivors, inv_payload):
    close = [s for s in survivors if s["clock"] == "close"]
    fresh = [s for s in close if s["letter"] not in SHORTBOARD]
    keepclose = [s for s in close if s["letter"] not in SKIP_TBA]
    val_open = [
        c for c in inv_payload["families"]["val_open"]["letters"]
        if c not in STANDING_OPEN
    ]
    fill_new = [
        c for c in inv_payload["families"]["fill_new"]["letters"]
        if c not in SKIP_TBA and c not in STANDING_OPEN
    ]
    return {
        "fresh": fresh,
        "keepclose": keepclose,
        "val_open": val_open,
        "fill_new": fill_new,
    }


def remaining_inventory():
    inv = inventory()
    survivors = load_survivors()
    p = pools(survivors, inv)
    clocks = load_clocks()
    return {
        "generated": str(date.today()),
        "live_untouched": "flatten_robust",
        "excel_cache_used": False,
        "prior_kinds": [
            "leftover four singles (val_open / val_close / fill_new / flag)",
            "weekly spill AP–AU",
            "leftover-open + today's A lag",
            "T/BA close shortboard",
            "CZ/EH/IB/HO/IL/GV close peers",
        ],
        "this_kind": "same-day multi-letter counts and AND joins",
        "fresh_letters": [s["letter"] for s in p["fresh"]],
        "keepclose_letters": [s["letter"] for s in p["keepclose"]],
        "and_pairs": [f"{a}∧{b}" for a, b in AND_PAIRS],
        "n_val_open_lag_atoms": len(p["val_open"]),
        "n_fill_new": len(p["fill_new"]),
        "skipped_shortboard": sorted(SHORTBOARD),
        "skipped_tba": sorted(SKIP_TBA),
        "skipped_standing_open": sorted(STANDING_OPEN),
        "skipped_weekly": sorted(SKIP_WEEKLY),
        "skipped_ohlc": sorted(SKIP_OHLC),
        "standing_open": "five-cell light+O ± AH/FR untouched",
        "close_shortboard": "KILL not reopened",
        "clocks": clocks["counts"],
    }


def render_inventory(meta):
    fresh = "/".join(meta["fresh_letters"])
    keepc = "/".join(meta["keepclose_letters"])
    pairs = ", ".join(meta["and_pairs"])
    return "\n".join([
        "# Same-day multi-letter surface (last A–JL kind)",
        "",
        f"_Generated {meta['generated']} · live `flatten_robust` frozen. "
        "Yahoo/rows only. No merge._",
        "",
        "## Plain English",
        "",
        "Single-letter leftover space, weekly spill, and leftover-open+A "
        "lags are already exhausted. T/BA and the close-peer shortboard "
        "are already KILL. What is still unmined is one *kind*: "
        "**how many leftover letters fire on the same day**, plus a few "
        "named AND joins that are not T∧BA.",
        "",
        "This beat does **not** remine leftover singles, weekly/lag, "
        "or T/BA. AH and FR stay out (standing open stack).",
        "",
        f"Fresh leftover-close pool (T/BA/peers out): `{fresh}`.",
        f"Keep-close pool (T/BA out, peers in): `{keepc}`.",
        f"Named AND joins: {pairs}.",
        f"Leftover-open count atoms (AH/FR out): "
        f"{meta['n_val_open_lag_atoms']}.",
        f"Leftover-fill green-count letters (T/BA/AH/FR out): "
        f"{meta['n_fill_new']}.",
        "",
        "If every count and join dies under the ship bar plus Q1 / July "
        "/ five-name prove, remaining A–JL pattern kinds are exhausted. "
        "A KEEP that clears that bar means the surface is not exhausted.",
        "",
        "Standing five-cell light+O ± AH/FR untouched. Finviz BLOCKED.",
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


def family_of(name):
    if name.startswith("close_count_fresh"):
        return "count_fresh"
    if name.startswith("close_count_keepclose"):
        return "count_keepclose"
    if name.startswith("close_count_fillnew"):
        return "count_fill"
    if name.startswith("open_count_"):
        return "count_open"
    if name.startswith("close_and_"):
        return "and_join"
    return "other"


def meaning_of(name, family):
    if family == "count_fresh":
        return "at least k leftover-close KEEP letters fire (T/BA/peers out)"
    if family == "count_keepclose":
        return "at least k leftover-close KEEP letters fire (T/BA out)"
    if family == "count_fill":
        return "at least k leftover fills are green the same close"
    if family == "count_open":
        return "at least k leftover-open numbers are ≥1 this morning (AH/FR out)"
    if family == "and_join":
        return "two leftover letters both fire the same close (not T/BA)"
    return name


def build_specs(p):
    specs = []
    for k in COUNT_KS:
        specs.append((f"close_count_fresh_ge{k}", "close", 1, "count_fresh", k))
        specs.append((f"close_count_keepclose_ge{k}", "close", 1, "count_keepclose", k))
        specs.append((f"open_count_valopen_ge{k}", "open", 1, "count_open", k))
    for k in FILL_KS:
        specs.append((f"close_count_fillnew_ge{k}", "close", 1, "count_fill", k))
    for a, b in AND_PAIRS:
        specs.append((f"close_and_{a}_{b}", "close", 1, "and_join", (a, b)))
    return specs


def atom_hits(cells, atoms):
    hit = set()
    for kind, letter, extra in atoms:
        rec = cells.get(letter)
        if kind == "fill":
            if fam(rec) == extra:
                hit.add(letter)
        elif kind == "val":
            v = num(rec)
            if v is not None and _cmp(v, extra[0], extra[1]):
                hit.add(letter)
    return hit


def fire_day(cells, p, specs):
    fresh_atoms = [s["atom"] for s in p["fresh"]]
    keep_atoms = [s["atom"] for s in p["keepclose"]]
    fresh_hit = atom_hits(cells, fresh_atoms)
    keep_hit = atom_hits(cells, keep_atoms)
    n_fresh = len(fresh_hit)
    n_keep = len(keep_hit)
    n_fill = sum(1 for c in p["fill_new"] if fam(cells.get(c)) == "green")
    n_open = 0
    for c in p["val_open"]:
        v = num(cells.get(c))
        if v is not None and _cmp(v, ">=", 1):
            n_open += 1
    hit = []
    for name, _clock, _side, kind, extra in specs:
        if kind == "count_fresh" and n_fresh >= extra:
            hit.append(name)
        elif kind == "count_keepclose" and n_keep >= extra:
            hit.append(name)
        elif kind == "count_fill" and n_fill >= extra:
            hit.append(name)
        elif kind == "count_open" and n_open >= extra:
            hit.append(name)
        elif kind == "and_join":
            a, b = extra
            if a in keep_hit and b in keep_hit:
                hit.append(name)
    return hit


def mine(specs, p):
    split = json.load(open(SPLIT_PATH))
    disc, hold = set(split["discovery"]), set(split["holdout"])
    spy = load_spy()
    files = [f for f in sorted(glob.glob(os.path.join(DUMPS, "*.json")))
             if not os.path.basename(f).startswith("_")]
    by_name = {s[0]: s for s in specs}
    cells = defaultdict(_cell)
    base = defaultdict(lambda: _slot())
    print(f"[same_day] files={len(files)} specs={len(specs)}", flush=True)
    n_bad = 0
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
            for clock, side in (("open", 1), ("close", 1)):
                for h in HOLDS:
                    raw = sim(days, ei, side, clock, h)
                    if raw is None:
                        continue
                    cost = COST_FUTU_LONG if side == 1 else COST_FUTU_SHORT
                    _push(base[(clock, "long" if side == 1 else "short", h)],
                          raw - cost)
            fired = fire_day(day["cells"], p, specs)
            if not fired:
                continue
            iso = str(s2d(day["date"]))
            tape = spy.get(iso, 0)
            half = "early" if iso < HALF_CUT else "late"
            qkey = "q12" if iso < Q3_CUT else "q3"
            mon = iso[:7]
            for name in fired:
                _n, clock, side, _k, _e = by_name[name]
                for h in HOLDS:
                    raw = sim(days, ei, side, clock, h)
                    if raw is None:
                        continue
                    cost = COST_FUTU_LONG if side == 1 else COST_FUTU_SHORT
                    net = raw - cost
                    key = (name, clock, "long" if side == 1 else "short",
                           f"hold{h}")
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
    baselines = {}
    for k, sl in base.items():
        b = _blk(sl)
        if b:
            baselines[f"{k[0]}_{k[1]}_h{k[2]}"] = b
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
    rows.sort(key=lambda r: (
        0 if r["keep"] == "KEEP" else 1 if r["keep"] == "KILL" else 2,
        -((r.get("holdout") or {}).get("t") or -9),
    ))
    return rows, baselines, files


def _pct(b):
    if not b or b.get("avg_net") is None:
        return "—"
    return f"{b['avg_net']*100:+.2f}% (n={b['n']})"


def render(rows, baselines, files, inv_meta, specs):
    keeps = [r for r in rows if r["keep"] == "KEEP"]
    kills = [r for r in rows if r["keep"] == "KILL"]
    thins = [r for r in rows if r["keep"] == "THIN"]
    by_fam = defaultdict(lambda: {"KEEP": 0, "KILL": 0, "THIN": 0})
    for r in rows:
        by_fam[r["family"]][r["keep"]] += 1
    exhausted = len(keeps) == 0
    L = [
        MARKER,
        "",
        f"_Generated {date.today()} · live `flatten_robust` frozen. "
        "Yahoo/rows A–F seed only. No merge. T/BA, weekly/lag, and "
        "light+O untouched._",
        "",
        "## Plain English",
        "",
        "The leftover four families, weekly spill, and leftover-open+A "
        "lags already used every single-letter and lag kind. This beat "
        "scores the last unused kind: **same-day multi-letter counts "
        "and AND joins** — how many leftover letters fire together "
        "today. Same ship bar as light+O, plus the deeper Q1 / July / "
        "five-name bar that killed T/BA.",
        "",
    ]
    if exhausted:
        L.append(
            f"**A–JL surface exhausted.** KEEP 0 · KILL {len(kills)} · "
            f"THIN {len(thins)}. Counts of leftover-close KEEPs, leftover "
            "fill greens, leftover-open numbers, and named AND joins "
            "all die under Q1 / July / five-name prove — the same ghost "
            "as T/BA. No remaining A–JL threshold-family kind is unmined "
            "on this tape. Next work, if any, is a new tape or an "
            "external join, not more sheet-letter twins."
        )
    else:
        L.append(
            f"**A–JL surface not exhausted.** KEEP {len(keeps)} · "
            f"KILL {len(kills)} · THIN {len(thins)}. A same-day count "
            "or join cleared the deeper bar. Research only — not a card."
        )
    L += [
        "",
        f"Dumps **{len(files)}**. Specs **{len(specs)}**. "
        f"Close-long everyone-else hold2 {_pct(baselines.get('close_long_h2'))}. "
        "Standing five-cell light+O ± AH/FR untouched.",
        "",
        "### Family scoreboard",
        "",
        "| family | what it is | KEEP | KILL | THIN |",
        "|---|---|---:|---:|---:|",
        f"| count_fresh | leftover-close KEEP count, T/BA/peers out | "
        f"{by_fam['count_fresh']['KEEP']} | {by_fam['count_fresh']['KILL']} | "
        f"{by_fam['count_fresh']['THIN']} |",
        f"| count_keepclose | leftover-close KEEP count, T/BA out | "
        f"{by_fam['count_keepclose']['KEEP']} | {by_fam['count_keepclose']['KILL']} | "
        f"{by_fam['count_keepclose']['THIN']} |",
        f"| count_fill | leftover fills green the same close | "
        f"{by_fam['count_fill']['KEEP']} | {by_fam['count_fill']['KILL']} | "
        f"{by_fam['count_fill']['THIN']} |",
        f"| count_open | leftover-open numbers ≥1 this morning (AH/FR out) | "
        f"{by_fam['count_open']['KEEP']} | {by_fam['count_open']['KILL']} | "
        f"{by_fam['count_open']['THIN']} |",
        f"| and_join | two leftover letters both fire (not T/BA) | "
        f"{by_fam['and_join']['KEEP']} | {by_fam['and_join']['KILL']} | "
        f"{by_fam['and_join']['THIN']} |",
        "",
        "### Multi-letter table",
        "",
        "| family | meaning | hold | clock | holdout | vs everyone | Q1 | Q3 | "
        "July | top-5 | verdict | why |",
        "|---|---|---|---|---|---|---|---|---|---|---|---|",
    ]
    shown = keeps + [r for r in rows if r["keep"] != "KEEP" and r["exit"] == "hold2"]
    if not shown:
        L.append("| — | none scored | — | — | — | — | — | — | — | — | — | — |")
    for r in shown:
        vs = "—"
        if r.get("holdout") and r.get("baseline"):
            vs = (f"{(r['holdout']['avg_net'] - r['baseline']['avg_net'])*100:+.2f} pp")
        L.append(
            f"| {r['family']} | {meaning_of(r['def'], r['family'])} | "
            f"{r['exit']} | {r['clock']} | {_pct(r.get('holdout'))} | {vs} | "
            f"{_pct(r.get('q1'))} | {_pct(r.get('q3'))} | "
            f"{(r.get('july_share') or 0)*100:.0f}% | "
            f"{(r.get('top5_share') or 0)*100:.0f}% | **{r['keep']}** | "
            f"{','.join(r.get('fail_reasons') or []) or '—'} |"
        )
    L += [
        "",
        "Code names (after the English): "
        + ", ".join(f"`{r['def']}`" for r in shown[:10])
        + (" …" if len(shown) > 10 else "") + ".",
        "",
        "### What this does not change",
        "",
        "- Standing open keeps stay **five-cell light + green O**, "
        "with or without AH/FR from the open-stack beat.",
        "- T / BA / CZ / EH / IB / HO / IL / GV stay **KILL**. "
        "Not reopened.",
        "- Leftover four families, weekly AP–AU, and leftover-open+A "
        "lags are not re-swept.",
        "- Finviz BLOCKED. Live `flatten_robust` frozen. No cards.",
        f"- Q1 cut **{Q1_CUT}**. Q3 cut **{Q3_CUT}**. Futubull 0.15%/0.20%.",
        "",
        "Research only. One 2026 regime.",
        "",
    ]
    return "\n".join(L)


def write_outputs(rows, baselines, files, inv_meta, specs):
    inv_md = render_inventory(inv_meta)
    open(INV_MD, "w", encoding="utf-8").write(inv_md)
    md = render(rows, baselines, files, inv_meta, specs)
    open(OUT_MD, "w", encoding="utf-8").write(md + "\n")
    n_keep = sum(1 for r in rows if r["keep"] == "KEEP")
    n_kill = sum(1 for r in rows if r["keep"] == "KILL")
    n_thin = sum(1 for r in rows if r["keep"] == "THIN")
    payload = {
        "generated": str(date.today()),
        "spec": "same-day multi-letter counts/joins; ship + Q1/name-ghost",
        "live_untouched": "flatten_robust",
        "excel_cache_used": False,
        "seed": "yahoo_rows_cache",
        "n_dumps": len(files),
        "n_specs": len(specs),
        "q1_cut": Q1_CUT,
        "q3_cut": Q3_CUT,
        "standing_open": "five-cell light+O ± AH/FR untouched",
        "close_shortboard": "KILL not reopened",
        "finviz": "BLOCKED",
        "ajl_surface": "exhausted" if n_keep == 0 else "not_exhausted",
        "n_keep": n_keep,
        "n_kill": n_kill,
        "n_thin": n_thin,
        "inventory": inv_meta,
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
        f"Same-day multi-letter: KEEP {n_keep} · KILL {n_kill} · "
        f"THIN {n_thin}. A–JL surface {payload['ajl_surface']}. "
        "Light+O untouched. T/BA untouched. See `SAME_DAY.md`.\n",
        require_any=("first A–JL cut", "A–O clock cycle"),
    )
    open(SB_MD, "w", encoding="utf-8").write(sb)
    open(AO_MD, "w", encoding="utf-8").write(ao)
    open(CYCLE_MD, "w", encoding="utf-8").write(cy)
    return payload


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--render-only", action="store_true")
    args = ap.parse_args()
    inv_payload = inventory()
    inv_meta = remaining_inventory()
    p = pools(load_survivors(), inv_payload)
    specs = build_specs(p)
    if args.render_only:
        prev = json.load(open(OUT_JSON, encoding="utf-8"))
        files = [None] * int(prev.get("n_dumps") or 0)
        payload = write_outputs(
            prev.get("rows") or [],
            prev.get("baselines") or {},
            files,
            prev.get("inventory") or inv_meta,
            specs,
        )
        print(f"render-only KEEP {payload['n_keep']} KILL {payload['n_kill']} "
              f"THIN {payload['n_thin']} surface={payload['ajl_surface']}",
              flush=True)
        return payload
    rows, baselines, files = mine(specs, p)
    payload = write_outputs(rows, baselines, files, inv_meta, specs)
    print(f"KEEP {payload['n_keep']} KILL {payload['n_kill']} "
          f"THIN {payload['n_thin']} specs={len(specs)} "
          f"surface={payload['ajl_surface']}", flush=True)
    for r in rows[:24]:
        print(f"  {r['keep']:4} {r['def']:32} {r['exit']} "
              f"{','.join(r.get('fail_reasons') or [])}", flush=True)
    return payload


if __name__ == "__main__":
    main()
