"""Next A–JL region after the leftover four families.

Single-letter leftover space (val_open / val_close / fill_new / flag)
covers every A–JL letter. This beat inventories what is still unmined
and scores two new families on the full Yahoo/rows dumps:

  * weekly AP–AU (skipped as sparse; never scored; close entry)
  * lag leftover-open + today's A green (yesterday's leftover open
    number + this morning's A; open entry)

Ship bar + deeper Q1 / name-ghost / July-share bar before KEEP.
Does not remine T/BA or reopen light+O / AH/FR stacks.

Research only. Live flatten_robust is not imported or changed.

  python engine/mine_next_region.py
  python engine/mine_next_region.py --render-only
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
from clock import COST_FUTU_LONG, COST_FUTU_SHORT, SHIP  # noqa: E402
from harden_hyst_open import apply_hold1_keep, lottery_day, splice_md  # noqa: E402
from mine_unmined import (  # noqa: E402
    BEAT, HALF_CUT, Q3_CUT, SKIP_OHLC, SKIP_WEEKLY, VAL_OPS, _blk, _cmp,
    _push, _slot, fam, inventory, load_clocks, load_spy, num, s2d, sim,
)

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
REPO = os.path.dirname(ROOT)
DUMPS = os.environ.get("ALL_COLS_DIR", os.path.join(ROOT, "research", "all_cols_sample"))
SPLIT_PATH = os.path.join(HERE, "holdout_split.json")
RESEARCH = os.path.join(ROOT, "research")
SCOREBOARD = os.path.join(REPO, "03_scoreboard")
OUT_MD = os.path.join(RESEARCH, "NEXT_REGION.md")
OUT_JSON = os.path.join(RESEARCH, "next_region.json")
INV_MD = os.path.join(RESEARCH, "NEXT_REGION_INVENTORY.md")
SB_MD = os.path.join(SCOREBOARD, "EXCEL_BOT_MINE.md")
AO_MD = os.path.join(RESEARCH, "AO_FIRST_MINE.md")
CYCLE_MD = os.path.join(RESEARCH, "MINE_CYCLE.md")
MARKER = "## Next A–JL region (weekly + leftover lag)"
Q1_CUT = "2026-04-01"
HOLDS = (1, 2)
WEEKLY = tuple(sorted(SKIP_WEEKLY))
# Standing leftover-open KEEPs — do not reopen as a stack; skip in lags.
SKIP_LAG = {"AH", "FR"}


def remaining_inventory():
    inv = inventory()
    clocks = load_clocks()
    fams = inv["families"]
    four = set()
    for k in ("val_open", "val_close", "fill_new", "flag"):
        four |= set(fams[k]["letters"])
    return {
        "generated": str(date.today()),
        "live_untouched": "flatten_robust",
        "excel_cache_used": False,
        "letter_space": "exhausted",
        "n_letters": 275,
        "four_family_letters": len(four),
        "skipped_ohlc": sorted(SKIP_OHLC),
        "skipped_weekly": list(WEEKLY),
        "weekly_clock": "close (unknown → close)",
        "val_open_lag_letters": [
            c for c in fams["val_open"]["letters"] if c not in SKIP_LAG
        ],
        "not_this_beat": [
            "same-day multi-letter counts (close-cluster already July ghosts)",
            "first-cut 8-def remine at 3603 (would resurrect T/BA/IB)",
            "OHLC / STOCKHISTORY aliases A–F / IR–IW (not a new family)",
        ],
        "standing_open": "five-cell light+O ± AH/FR untouched",
        "clocks": clocks["counts"],
    }


def render_inventory(meta):
    return "\n".join([
        "# Remaining A–JL surface (after leftover four)",
        "",
        f"_Generated {meta['generated']} · live `flatten_robust` frozen. "
        "Yahoo/rows only. No merge._",
        "",
        "## Plain English",
        "",
        "Every A–JL letter already sits in one leftover family "
        "(open leftover numbers, close leftover numbers, leftover fills, "
        "or 0/1 flags), or in a skip bucket (OHLC / weekly). "
        "**Single-letter leftover space is exhausted.**",
        "",
        "What is still unmined is not a new letter — it is a new *kind*:",
        "",
        "1. **Weekly spill AP–AU** — skipped as sparse; never scored. "
        "Clock unknown → **close**.",
        "2. **Yesterday leftover-open + today's A** — only six old lags "
        "(L/EL/JA/IZ + A) were ever mined. The leftover open numbers "
        "were never joined to this morning's A. AH and FR stay out "
        "(standing open stack, not reopened).",
        "3. Same-day multi-letter counts — not this beat; close-cluster "
        "already showed July / five-name ghosts.",
        "4. First-cut 8-def remine at 3603 — not a new family, and would "
        "resurrect T/BA/IB.",
        "",
        f"Weekly letters: `{'/'.join(WEEKLY)}`. "
        f"Lag letters: {len(meta['val_open_lag_letters'])} leftover-open "
        "names (AH/FR excluded).",
        "",
        "Standing five-cell light+O ± AH/FR untouched. T/BA shortboard "
        "stays KILL. Finviz BLOCKED.",
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
    if name.startswith("weekly_"):
        return "weekly"
    if name.startswith("lag_"):
        return "lag_open"
    return "other"


def build_specs(inv_payload):
    specs = []
    for col in WEEKLY:
        for opname, op, th, side in VAL_OPS:
            specs.append((f"weekly_{col}_{opname}", "close", side, "val",
                          col, (op, th)))
        specs.append((f"weekly_{col}_green", "close", 1, "fill", col, "green"))
        specs.append((f"weekly_{col}_red", "close", -1, "fill", col, "red"))
    for col in inv_payload["families"]["val_open"]["letters"]:
        if col in SKIP_LAG:
            continue
        for opname, op, th, side in (("ge1", ">=", 1, 1), ("gt0", ">", 0, 1),
                                     ("eq1", "==", 1, 1)):
            specs.append((f"lag_{col}_{opname}_Agreen", "open", side, "lag",
                          col, (op, th)))
    return specs


def fire_day(prev_cells, cells, specs_by_col):
    """Return fired spec names for this day."""
    hit = []
    a_green = fam(cells.get("A")) == "green"
    for col, group in specs_by_col.items():
        rec = cells.get(col)
        prev = (prev_cells or {}).get(col) if prev_cells else None
        v, f = num(rec), fam(rec)
        pv = num(prev)
        for name, _clock, _side, kind, _c, extra in group:
            if kind == "fill" and f == extra:
                hit.append(name)
            elif kind == "val" and v is not None:
                op, th = extra
                if _cmp(v, op, th):
                    hit.append(name)
            elif kind == "lag" and a_green and pv is not None:
                op, th = extra
                if _cmp(pv, op, th):
                    hit.append(name)
    return hit


def deeper(row, cell):
    reasons = list(row.get("fail_reasons") or [])
    q1 = _blk(cell["q1"])
    row["q1"] = q1
    if not q1 or q1["n"] < 40:
        reasons.append("q1_thin")
    elif q1["avg_net"] <= 0:
        reasons.append("q1_sign")
    months = {m: _blk(sl) for m, sl in cell["month"].items()}
    months = {m: b for m, b in months.items() if b}
    row["months"] = {m: months[m] for m in sorted(months)}
    pos = [(m, b["avg_net"] * b["n"]) for m, b in months.items()
           if b["avg_net"] > 0 and b["n"] >= 20]
    gross = sum(v for _m, v in pos)
    j = months.get("2026-07")
    july = 0.0
    if j and gross > 0 and j.get("avg_net", 0) > 0:
        july = (j["avg_net"] * j["n"]) / gross
    row["july_share"] = july
    if july > 0.40:
        reasons.append("month_lottery")
    thick = [m for m, b in months.items() if b["n"] >= 20]
    if len(thick) >= 3 and any(months[m]["avg_net"] <= 0 for m in thick):
        reasons.append("month_split")
    pnl = cell["ticker_pnl"]
    total = sum(pnl.values())
    top5 = sorted(pnl, key=pnl.get, reverse=True)[:5]
    share = (sum(pnl[t] for t in top5) / total) if total else 0.0
    row["top5_share"] = share
    row["top5_tickers"] = top5
    if share > 0.25:
        reasons.append("ticker_ghost")
    row["fail_reasons"] = list(dict.fromkeys(reasons))
    size = {"thin_disc", "thin_hold", "ticker_bar", "date_bar",
            "tape_thin", "spy_thin", "q3_missing", "q1_thin"}
    quality = set(row["fail_reasons"]) - size
    if row.get("n_tickers", 0) < 50 or not row.get("discovery") or row["discovery"]["n"] < 80:
        row["verdict"] = "THIN"
        row["keep"] = "THIN"
    elif quality:
        row["verdict"] = "KILL"
        row["keep"] = "KILL"
    else:
        row["verdict"] = "KEEP"
        row["keep"] = "KEEP"
    return row


def mine(specs):
    split = json.load(open(SPLIT_PATH))
    disc, hold = set(split["discovery"]), set(split["holdout"])
    spy = load_spy()
    files = [f for f in sorted(glob.glob(os.path.join(DUMPS, "*.json")))
             if not os.path.basename(f).startswith("_")]
    by_col = defaultdict(list)
    by_name = {}
    for spec in specs:
        by_col[spec[4]].append(spec)
        by_name[spec[0]] = spec
    cells = defaultdict(_cell)
    base = defaultdict(lambda: _slot())
    print(f"[next_region] files={len(files)} specs={len(specs)}", flush=True)
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
            prev = days[ei - 1]["cells"] if ei else None
            fired = fire_day(prev, day["cells"], by_col)
            if not fired:
                continue
            iso = str(s2d(day["date"]))
            tape = spy.get(iso, 0)
            half = "early" if iso < HALF_CUT else "late"
            qkey = "q12" if iso < Q3_CUT else "q3"
            mon = iso[:7]
            for name in fired:
                _n, clock, side, _k, _c, _e = by_name[name]
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
    from mine_unmined import pack_row
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
    L = [
        MARKER,
        "",
        f"_Generated {date.today()} · live `flatten_robust` frozen. "
        "Yahoo/rows A–F seed only. No merge. T/BA and light+O untouched._",
        "",
        "## Plain English",
        "",
        "The leftover four families already used every A–JL letter. "
        "This beat scores the **next region**: weekly spill AP–AU "
        "(close), and yesterday's leftover-open number plus this "
        "morning's green A (open). Same ship bar as light+O, plus "
        "the deeper Q1 / July-share / five-name bar that killed the "
        "close shortboard.",
        "",
    ]
    if keeps:
        L.append(
            f"**New-family KEEP {len(keeps)}** · KILL {len(kills)} · "
            f"THIN {len(thins)}. Research only — not a card."
        )
    else:
        L.append(
            f"**Clean null.** KEEP 0 · KILL {len(kills)} · THIN {len(thins)}. "
            "Weekly spill is sparse or a Q1 / July / name ghost. "
            "Leftover-open lags do not add a shippable open edge on "
            "top of A. Single-letter leftover space stays exhausted. "
            "Next region, if any, is same-day multi-letter counts "
            "(already ghosted on T/BA) or a new tape — not more "
            "A–JL threshold twins."
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
        f"| weekly | weekly spill AP–AU (close) | "
        f"{by_fam['weekly']['KEEP']} | {by_fam['weekly']['KILL']} | "
        f"{by_fam['weekly']['THIN']} |",
        f"| lag_open | yesterday leftover-open + today's A green | "
        f"{by_fam['lag_open']['KEEP']} | {by_fam['lag_open']['KILL']} | "
        f"{by_fam['lag_open']['THIN']} |",
        "",
        "### New-family table",
        "",
        "| family | meaning | hold | clock | holdout | vs everyone | Q1 | Q3 | "
        "July | top-5 | verdict | why |",
        "|---|---|---|---|---|---|---|---|---|---|---|---|",
    ]
    shown = keeps + [r for r in rows if r["keep"] != "KEEP" and r["exit"] == "hold2"]
    shown = shown[:24]
    if not shown:
        L.append("| — | none scored | — | — | — | — | — | — | — | — | — | — |")
    for r in shown:
        vs = "—"
        if r.get("holdout") and r.get("baseline"):
            vs = (f"{(r['holdout']['avg_net'] - r['baseline']['avg_net'])*100:+.2f} pp")
        if r["family"] == "weekly":
            meaning = "weekly spill cell fires (close)"
        else:
            meaning = "yesterday leftover-open number + today's green A"
        L.append(
            f"| {r['family']} | {meaning} | {r['exit']} | {r['clock']} | "
            f"{_pct(r.get('holdout'))} | {vs} | {_pct(r.get('q1'))} | "
            f"{_pct(r.get('q3'))} | {(r.get('july_share') or 0)*100:.0f}% | "
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
        "with or without AH/FR from the open-stack beat.",
        "- T / BA / CZ / EH / IB / HO / IL / GV stay **KILL**. "
        "Not reopened.",
        "- Leftover four families are not re-swept.",
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
    payload = {
        "generated": str(date.today()),
        "spec": "weekly AP-AU + lag leftover-open+A; ship + Q1/name-ghost",
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
        "letter_space": "exhausted",
        "n_keep": sum(1 for r in rows if r["keep"] == "KEEP"),
        "n_kill": sum(1 for r in rows if r["keep"] == "KILL"),
        "n_thin": sum(1 for r in rows if r["keep"] == "THIN"),
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
        f"Next A–JL region: weekly+lag KEEP {payload['n_keep']} · "
        f"KILL {payload['n_kill']} · THIN {payload['n_thin']}. "
        "Letter space exhausted. Light+O untouched. T/BA untouched. "
        "See `NEXT_REGION.md`.\n",
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
    if args.render_only:
        prev = json.load(open(OUT_JSON, encoding="utf-8"))
        inv_meta = prev.get("inventory") or remaining_inventory()
        files = [None] * int(prev.get("n_dumps") or 0)
        specs = [None] * int(prev.get("n_specs") or 0)
        payload = write_outputs(
            prev.get("rows") or [],
            prev.get("baselines") or {},
            files,
            inv_meta,
            specs,
        )
        print(f"render-only KEEP {payload['n_keep']} KILL {payload['n_kill']} "
              f"THIN {payload['n_thin']}", flush=True)
        return payload
    inv_payload = inventory()
    inv_meta = remaining_inventory()
    specs = build_specs(inv_payload)
    rows, baselines, files = mine(specs)
    payload = write_outputs(rows, baselines, files, inv_meta, specs)
    print(f"KEEP {payload['n_keep']} KILL {payload['n_kill']} "
          f"THIN {payload['n_thin']} specs={len(specs)}", flush=True)
    for r in rows[:20]:
        print(f"  {r['keep']:4} {r['def']:36} {r['exit']} "
              f"{','.join(r.get('fail_reasons') or [])}", flush=True)
    return payload


if __name__ == "__main__":
    main()
