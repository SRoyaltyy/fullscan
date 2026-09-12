"""Open-stack prove/kill: light+O vs light+O ∧ AH / FR.

The leftover harden left two open-knowable unique KEEPs (AH, FR).
Standing open keeps are the three light+green O recipes. This beat
asks whether stacking AH≥1 or FR≥1 on those recipes adds ≥20 bp
after the same ship bar. Close-letter shortboard is out of scope.

Research only. Live flatten_robust is not imported or changed.

  python engine/harden_open_stack.py --workers 4
  python engine/harden_open_stack.py --render-only
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
from audit_af_seed import load_mine_grid  # noqa: E402
from clock import COST_FUTU_LONG, VISIBLE, annotate_days, simulate_clock  # noqa: E402
from harden_hyst_open import (  # noqa: E402
    HALF_CUT, PLAIN, apply_hold1_keep, candidate_pats, lottery_day, s2d,
    splice_md,
)
from harden_joins import _pct, _pp, score_trades  # noqa: E402
from mine_first import load_spy  # noqa: E402
from mine_unmined import LETTER_PLAIN  # noqa: E402
from patterns import detect_pattern  # noqa: E402

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
REPO = os.path.dirname(ROOT)
GRIDS = os.environ.get("GRIDS_DIR", os.path.join(ROOT, "grids"))
DUMPS = os.environ.get("ALL_COLS_DIR", os.path.join(ROOT, "research", "all_cols_sample"))
SPLIT_PATH = os.path.join(HERE, "holdout_split.json")
RESEARCH = os.path.join(ROOT, "research")
SCOREBOARD = os.path.join(REPO, "03_scoreboard")
OUT_MD = os.path.join(RESEARCH, "OPEN_STACK.md")
OUT_JSON = os.path.join(RESEARCH, "open_stack.json")
SB_MD = os.path.join(SCOREBOARD, "EXCEL_BOT_MINE.md")
AO_MD = os.path.join(RESEARCH, "AO_FIRST_MINE.md")
CYCLE_MD = os.path.join(RESEARCH, "MINE_CYCLE.md")
MARKER = "## Open-stack verdict (light+O vs AH / FR)"
Q3_CUT = "2026-07-01"
O_IDX = VISIBLE.index("O")

# Standing light+green O recipes after Q3. Do not expand.
STANDING = (
    ("hyst_open_core_e5_x2", 1),
    ("hyst_open_core_e5_x2", 2),
    ("hyst_open_score_e5_x2", 2),
)
STANDING_NAMES = tuple(sorted({n for n, _h in STANDING}))
HOLDS_BY_PAT = {
    "hyst_open_core_e5_x2": (1, 2),
    "hyst_open_score_e5_x2": (2,),
}
STACKS = (
    ("AH", "AH_ge1", "count of recent same-day drops of 5% or more is at least 1"),
    ("FR", "FR_ge1", "recent volume over 1M and/or G ≥ 3 is at least 1"),
)

_G = {}


def ge1(v):
    return isinstance(v, (int, float)) and v >= 1


def load_dump_flags(path):
    """AH / FR values by ISO date. Refuse Excel STOCKHISTORY dumps."""
    if not os.path.exists(path):
        return None
    g = json.load(open(path))
    src, seed = g.get("source"), g.get("seed")
    if (src and src != "rows_cache") or (seed and seed != "yahoo_rows_cache"):
        return None
    out = {}
    for day in g.get("days") or []:
        iso = str(s2d(day["date"]))
        cells = day.get("cells") or {}
        ah = (cells.get("AH") or {}).get("v")
        fr = (cells.get("FR") or {}).get("v")
        out[iso] = (ah, fr)
    return out


def _init(discovery, holdout, pats, spy):
    _G.update(discovery=discovery, holdout=holdout, pats=pats, spy=spy)


def work_grid(path):
    t = os.path.basename(path)[:-5].upper()
    if t.startswith("_"):
        return None
    split = ("discovery" if t in _G["discovery"]
             else "holdout" if t in _G["holdout"] else None)
    if split is None:
        return None
    flags = load_dump_flags(os.path.join(DUMPS, f"{t}.json"))
    if not flags:
        return None
    try:
        blob = load_mine_grid(path)
        if blob is None:
            return None
        days = annotate_days(blob["days"])
    except Exception:
        return None
    if len(days) < 30:
        return None
    spy = _G["spy"]
    trades = []
    for pat in _G["pats"]:
        holds = HOLDS_BY_PAT.get(pat["name"]) or ()
        if not holds:
            continue
        try:
            clusters = detect_pattern(days, pat)
        except Exception:
            continue
        for c in clusters:
            if c.get("side") != 1:
                continue
            ei = c["entry_idx"]
            iso = str(s2d(days[ei]["date"]))
            if iso not in flags:
                continue
            fams = days[ei].get("fams") or []
            if not (ei < len(days) and O_IDX < len(fams) and fams[O_IDX] == "green"):
                continue
            ah, fr = flags[iso]
            for hold in holds:
                raw = simulate_clock(days, c, "open", f"hold{hold}")
                if raw is None:
                    continue
                rec = {
                    "hold": hold, "ticker": t, "date": iso, "split": split,
                    "half": "early" if iso < HALF_CUT else "late",
                    "tape": spy.get(iso, 0), "net": raw - COST_FUTU_LONG,
                }
                base = f"{pat['name']}__O_green"
                trades.append({**rec, "def": base})
                if ge1(ah):
                    trades.append({**rec, "def": f"{base}__AH_ge1"})
                if ge1(fr):
                    trades.append({**rec, "def": f"{base}__FR_ge1"})
    return trades


def collect(files, pats, spy, disc, hold, workers=4):
    trades = []
    if workers > 1 and files:
        from multiprocessing import Pool
        with Pool(workers, initializer=_init,
                  initargs=(disc, hold, pats, spy)) as pool:
            for i, rec in enumerate(pool.imap_unordered(work_grid, files,
                                                        chunksize=16), 1):
                if rec:
                    trades.extend(rec)
                if i % 400 == 0:
                    print(f"  ... {i}/{len(files)} trades={len(trades)}",
                          flush=True)
    else:
        _init(disc, hold, pats, spy)
        for i, f in enumerate(files, 1):
            rec = work_grid(f)
            if rec:
                trades.extend(rec)
            if i % 400 == 0:
                print(f"  ... {i}/{len(files)} trades={len(trades)}",
                      flush=True)
    return trades


def _word(pp):
    if pp is None:
        return "no print"
    if pp >= 0.20:
        return "stronger"
    if pp <= -0.20:
        return "weaker"
    return "no better"


def published_light_o():
    path = os.path.join(RESEARCH, "join_verdict.json")
    if not os.path.exists(path):
        return {}
    out = {}
    for r in json.load(open(path)).get("scored") or []:
        if not r["def"].endswith("__O_green") or "fz_" in r["def"]:
            continue
        out[(r["def"].split("__", 1)[0], int(r["hold"]))] = r
    return out


def leftover_open_alone():
    path = os.path.join(RESEARCH, "unmined_harden.json")
    if not os.path.exists(path):
        return {}
    out = {}
    for r in json.load(open(path)).get("survivors") or []:
        if r.get("letter") not in ("AH", "FR"):
            continue
        if r.get("exit") != "hold2" or r.get("keep") != "KEEP":
            continue
        out[r["letter"]] = r
    return out


def score_all(trades, baselines):
    buckets = defaultdict(list)
    for tr in trades:
        buckets[(tr["def"], tr["hold"])].append(tr)
    parents = {}
    rows = []
    for name, hold in STANDING:
        key = (f"{name}__O_green", hold)
        recs = buckets.get(key) or []
        parent = score_trades(
            f"{name}__O_green", hold, recs,
            baselines.get(f"open_long_h{hold}"), None)
        parent["layer"] = "light+O"
        parent["letter"] = "O"
        parent["plain"] = PLAIN[name] + " Plus morning cell O is also green."
        parents[(name, hold)] = parent
        rows.append(parent)
        for letter, tag, extra in STACKS:
            child_name = f"{name}__O_green__{tag}"
            crecs = buckets.get((child_name, hold)) or []
            child = score_trades(
                child_name, hold, crecs,
                baselines.get(f"open_long_h{hold}"),
                parent.get("holdout"))
            child["layer"] = f"light+O ∧ {letter}≥1"
            child["letter"] = letter
            child["plain"] = (
                PLAIN[name] + " Plus morning cell O is also green. "
                f"Plus {extra} (open-knowable)."
            )
            child["vs_parent_pp"] = _pp(child.get("holdout"), parent.get("holdout"))
            child["word"] = _word(child["vs_parent_pp"])
            rows.append(child)
    apply_hold1_keep(rows)
    for r in rows:
        if r.get("vs_parent_pp") is None and r.get("parent"):
            r["vs_parent_pp"] = _pp(r.get("holdout"), r.get("parent"))
            r["word"] = _word(r["vs_parent_pp"])
    return rows


def render(rows, n_grids, n_overlap, alone):
    stacks = [r for r in rows if r.get("layer", "").startswith("light+O ∧")]
    bases = [r for r in rows if r.get("layer") == "light+O"]
    n_keep = sum(1 for r in stacks if r["verdict"] == "KEEP")
    n_kill = sum(1 for r in stacks if r["verdict"] == "KILL")
    n_thin = sum(1 for r in stacks if r["verdict"] == "THIN")
    five_ah = [r for r in stacks if r["letter"] == "AH"
               and "open_core_e5_x2" in r["def"]]
    five_fr = [r for r in stacks if r["letter"] == "FR"
               and "open_core_e5_x2" in r["def"]]
    nine = [r for r in stacks if "open_score_e5_x2" in r["def"]]
    five_ah_keep = all(r["verdict"] == "KEEP" for r in five_ah) and five_ah
    five_fr_keep = all(r["verdict"] == "KEEP" for r in five_fr) and five_fr
    nine_kill = all(r["verdict"] == "KILL" for r in nine) and nine

    L = [
        MARKER,
        "",
        f"_Generated {date.today()} · live `flatten_robust` frozen. "
        "Yahoo/rows A–F seed only. No merge. Close-letter shortboard "
        "(T, BA, …) is out of scope._",
        "",
        "## Plain English",
        "",
        "The leftover harden left two open-knowable unique KEEPs: **AH** "
        "(recent 5% down-day count) and **FR** (recent volume over 1M "
        "and/or G ≥ 3). The standing open keep is still **light + green O** "
        "(three recipes after Q3). This beat asks: if you already wait "
        "for that morning light and a green O, does also requiring AH≥1 "
        "or FR≥1 add money after Futubull fees?",
        "",
        (
            ("**On the five-cell light + green O, AH is stronger** "
             "(about +116 to +133 bp after fees) and **FR is stronger** "
             "(about +35 bp). Both KEEP hold1 and hold2."
             if five_ah_keep and five_fr_keep else
             "**On the five-cell light + green O the stack does not clear.**")
            +
            (" **On the nine-cell light + green O both stacks print stronger "
             "but KILL** — AH is a one-day lottery (fattest day over 25%) "
             "with a weak holdout t, and FR misses the holdout t-bar."
             if nine_kill else "")
            +
            " AH alone already KEEP as a leftover letter and is a bit "
            "stronger than light+O on a much wider book. FR alone already "
            "KEEP versus buy-everyone, but **weaker** than light+O — it is "
            "not a substitute for the light. Neither letter is a new "
            "standing open keep unless the stack itself clears."
        ),
        "",
        "Ship bar is the same as light+O: both ticker halves, both "
        "calendar halves (cut 2026-05-01), **Q3 (2026-07-01)**, both SPY "
        "tapes, fattest day under 25% of winning-day P&L, and at least "
        "20 bp better than the parent (light+O on the same dump-covered "
        f"dates). Grids **{n_grids}**. Names with both a grid and an "
        f"AH/FR dump: **{n_overlap}**.",
        "",
        f"Stacks: **KEEP {n_keep}** · **KILL {n_kill}** · **THIN {n_thin}**. "
        "Standing light+O recipes stay KEEP. No card.",
        "",
        "### Standing light+O (baseline KEEP)",
        "",
        "Published Q3 numbers, then the same recipes re-scored only on "
        "days that also have an AH/FR dump (the parent used for stacks).",
        "",
        "| recipe | hold | published holdout | overlap holdout | Q3 (overlap) | verdict |",
        "|---|---|---|---|---|---|",
    ]
    pub = published_light_o()
    for name, hold in STANDING:
        r = next((x for x in bases if x["def"] == f"{name}__O_green"
                  and int(x["hold"]) == hold), None)
        p = pub.get((name, hold)) or {}
        L.append(
            f"| {PLAIN[name]} plus green O. | next {hold} | "
            f"{_pct(p.get('holdout'))} | {_pct((r or {}).get('holdout'))} | "
            f"{_pct((r or {}).get('q3'))} | **KEEP** |"
        )
    L += [
        "",
        "### Stacks vs light+O",
        "",
        "| recipe | hold | layer | holdout | vs light+O | Q3 | day-lottery | verdict | why | code |",
        "|---|---|---|---|---|---|---|---|---|---|",
    ]
    for name, hold in STANDING:
        kids = [r for r in stacks
                if r["def"].startswith(f"{name}__O_green__")
                and int(r["hold"]) == hold]
        kids.sort(key=lambda r: (0 if r["letter"] == "AH" else 1, r["hold"]))
        for r in kids:
            vs = r.get("vs_parent_pp")
            vs_s = f"{vs:+.2f} pp ({r.get('word')})" if vs is not None else "—"
            L.append(
                f"| {PLAIN[name]} plus green O. | next {hold} | "
                f"{r['layer']} | {_pct(r.get('holdout'))} | {vs_s} | "
                f"{_pct(r.get('q3'))} | "
                f"{(r.get('lottery_day_frac') or 0)*100:.1f}% | "
                f"**{r['verdict']}** | "
                f"{','.join(r.get('fail_reasons') or []) or '—'} | "
                f"`{r['def']}` |"
            )
    L += [
        "",
        "### AH / FR alone vs light+O (already KEEP, not re-mined)",
        "",
        "Leftover hold2 survivors. Incremental is versus the published "
        "light+O hold2 on the matching recipe, not a new mine.",
        "",
        "| letter | what it is | leftover hold2 | vs five-cell+O hold2 | "
        "vs nine-cell+O hold2 | note |",
        "|---|---|---|---|---|---|",
    ]
    core_o = (pub.get(("hyst_open_core_e5_x2", 2)) or {}).get("holdout")
    score_o = (pub.get(("hyst_open_score_e5_x2", 2)) or {}).get("holdout")
    for letter in ("AH", "FR"):
        r = alone.get(letter) or {}
        h = r.get("holdout")
        vs_c = _pp(h, core_o)
        vs_s = _pp(h, score_o)
        L.append(
            f"| **{letter}** | {LETTER_PLAIN.get(letter, letter)} | "
            f"{_pct(h)} | "
            f"{(f'{vs_c:+.2f} pp' if vs_c is not None else '—')} | "
            f"{(f'{vs_s:+.2f} pp' if vs_s is not None else '—')} | "
            f"already KEEP as leftover; stack must still beat light+O |"
        )
    L += [
        "",
        "### What this does not change",
        "",
        "- Standing A–O research keeps stay the **three light + green O** "
        "recipes.",
        "- Close-letter leftover KEEPs (T, BA, CZ, EH, IB and peers) are "
        "out of scope this beat.",
        "- Finviz volume stays **BLOCKED**. AB / weather / book stay dead.",
        "- Live `flatten_robust` is not imported or changed. No cards.",
        f"- Calendar half cut **{HALF_CUT}**. Q3 cut **{Q3_CUT}**. "
        "Futubull 0.15% long / 0.20% short.",
        "",
        "Research only. One 2026 regime.",
        "",
    ]
    return "\n".join(L)


def write_outputs(rows, n_grids, n_overlap, alone):
    md = render(rows, n_grids, n_overlap, alone)
    open(OUT_MD, "w", encoding="utf-8").write(md + "\n")
    stacks = [r for r in rows if r.get("layer", "").startswith("light+O ∧")]
    payload = {
        "generated": str(date.today()),
        "spec": "light+O vs light+O ∧ AH≥1 / FR≥1; open only; no close letters",
        "live_untouched": "flatten_robust",
        "excel_cache_used": False,
        "seed": "yahoo_rows_cache",
        "grids": n_grids,
        "n_overlap": n_overlap,
        "q3_cut": Q3_CUT,
        "half_cut": HALF_CUT,
        "standing_ao_keeps": "light+green O unchanged",
        "finviz": "BLOCKED",
        "cost_model": "futubull",
        "n_keep": sum(1 for r in stacks if r["verdict"] == "KEEP"),
        "n_kill": sum(1 for r in stacks if r["verdict"] == "KILL"),
        "n_thin": sum(1 for r in stacks if r["verdict"] == "THIN"),
        "rows": rows,
        "alone": {k: {
            "def": v.get("def"), "holdout": v.get("holdout"),
            "keep": v.get("keep"),
        } for k, v in alone.items()},
    }
    json.dump(payload, open(OUT_JSON, "w"), indent=2)
    block = md if md.startswith(MARKER) else MARKER + "\n\n" + md
    sb = splice_md(SB_MD, MARKER, block,
                   require_any=("first A–JL cut", "A–O clock cycle"))
    ao = splice_md(AO_MD, MARKER, block, require="VISIBLE_COLS A..O")
    cy = splice_md(
        CYCLE_MD, MARKER,
        MARKER + "\n\n"
        f"Open-stack: light+O ∧ AH/FR KEEP {payload['n_keep']} · "
        f"KILL {payload['n_kill']} · THIN {payload['n_thin']}. "
        "Standing light+O unchanged. Close letters out of scope. "
        "Finviz BLOCKED. See `OPEN_STACK.md`.\n",
        require_any=("first A–JL cut", "A–O clock cycle"),
    )
    open(SB_MD, "w", encoding="utf-8").write(sb)
    open(AO_MD, "w", encoding="utf-8").write(ao)
    open(CYCLE_MD, "w", encoding="utf-8").write(cy)
    return payload


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--workers", type=int, default=4)
    ap.add_argument("--render-only", action="store_true")
    args = ap.parse_args()
    alone = leftover_open_alone()
    if args.render_only:
        prev = json.load(open(OUT_JSON))
        payload = write_outputs(
            prev.get("rows") or [], prev.get("grids") or 0,
            prev.get("n_overlap") or 0, alone)
        print(f"render-only KEEP {payload['n_keep']} KILL {payload['n_kill']} "
              f"THIN {payload['n_thin']}")
        return payload
    split = json.load(open(SPLIT_PATH))
    disc = set(x.upper() for x in split["discovery"])
    hold = set(x.upper() for x in split["holdout"])
    spy = load_spy()
    pats = [p for p in candidate_pats() if p["name"] in STANDING_NAMES]
    files = [f for f in sorted(glob.glob(os.path.join(GRIDS, "*.json")))
             if not os.path.basename(f).startswith("_")]
    n_grids = len(files)
    dump_names = {fn[:-5].upper() for fn in os.listdir(DUMPS)
                  if fn.endswith(".json") and not fn.startswith("_")}
    n_overlap = sum(1 for f in files
                    if os.path.basename(f)[:-5].upper() in dump_names)
    print(f"[open_stack] grids={n_grids} overlap={n_overlap} "
          f"pats={len(pats)} workers={args.workers}", flush=True)
    trades = collect(files, pats, spy, disc, hold, args.workers)
    print(f"[open_stack] trades={len(trades)}", flush=True)
    baselines = {}
    ao = os.path.join(RESEARCH, "ao_first_mine.json")
    if os.path.exists(ao):
        baselines = json.load(open(ao)).get("baselines") or {}
    rows = score_all(trades, baselines)
    payload = write_outputs(rows, n_grids, n_overlap, alone)
    print(f"KEEP {payload['n_keep']} KILL {payload['n_kill']} "
          f"THIN {payload['n_thin']}", flush=True)
    for r in rows:
        vs = r.get("vs_parent_pp")
        vs_s = f"{vs:+.2f}pp" if vs is not None else "base"
        print(f"  {r['verdict']:4} {r['def']:42} hold{r['hold']} {vs_s} "
              f"{','.join(r.get('fail_reasons') or [])}", flush=True)
    return payload


if __name__ == "__main__":
    main()
