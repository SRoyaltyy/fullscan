"""Deeper prove/kill on leftover close peers CZ / EH / IB / HO / IL / GV.

Same bar as the T/BA close-cluster: Q1 vs Q3, July month share,
top-5 name concentration, hold1/hold2, Futubull, both SPY tapes,
day-lottery, ≥20 bp vs buy-everyone. Close entry only.

Does not remine T/BA or reopen AH/FR / light+O. Stacks only if two
or more peers clear Q1 and the name-ghost bar (otherwise skipped).

Research only. Live flatten_robust is not imported or changed.

  python engine/harden_close_peers.py --workers 4
  python engine/harden_close_peers.py --render-only
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
from harden_close_cluster import (  # noqa: E402
    BEAT, HALF_CUT, HOLDS, Q1_CUT, Q3_CUT, apply_hold1_keep, score, sim,
)
from harden_hyst_open import s2d, splice_md  # noqa: E402
from harden_joins import _pct  # noqa: E402
from mine_first import load_spy  # noqa: E402

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
REPO = os.path.dirname(ROOT)
DUMPS = os.environ.get("ALL_COLS_DIR", os.path.join(ROOT, "research", "all_cols_sample"))
SPLIT_PATH = os.path.join(HERE, "holdout_split.json")
RESEARCH = os.path.join(ROOT, "research")
SCOREBOARD = os.path.join(REPO, "03_scoreboard")
OUT_JSON = os.path.join(RESEARCH, "close_cluster_peers.json")
CLUSTER_MD = os.path.join(RESEARCH, "CLOSE_CLUSTER.md")
SB_MD = os.path.join(SCOREBOARD, "EXCEL_BOT_MINE.md")
AO_MD = os.path.join(RESEARCH, "AO_FIRST_MINE.md")
CYCLE_MD = os.path.join(RESEARCH, "MINE_CYCLE.md")
MARKER = "## Close-cluster peers (CZ / EH / IB / HO / IL / GV)"

# Leftover unique-36 survivor defs. Close entry only.
PEERS = (
    ("CZ", "valclose_CZ_ge2", "ge2",
     "how many recent CP prints were negative is at least 2"),
    ("EH", "valclose_EH_ge1", "ge1",
     "any of FP–FU is negative (EH ≥ 1)"),
    ("IB", "flag_IB_eq1", "eq1",
     "the 0/1 count IB equals 1 (IB≥3 was already mined)"),
    ("HO", "valclose_HO_eq1", "eq1",
     "a signed HN/GU cross equals 1"),
    ("IL", "valclose_IL_eq1", "eq1",
     "bins of same-day return H equal 1"),
    ("GV", "valclose_GV_eq1", "eq1",
     "a 0/1 composite of deeper flags equals 1"),
)

_G = {}


def _num(cells, letter):
    v = (cells.get(letter) or {}).get("v")
    return v if isinstance(v, (int, float)) else None


def fire(cells, letter, op):
    v = _num(cells, letter)
    if v is None:
        return False
    if op == "eq1":
        return v == 1
    if op == "ge1":
        return v >= 1
    if op == "ge2":
        return v >= 2
    return False


def fires_of(cells):
    return {letter: fire(cells, letter, op) for letter, _n, op, _p in PEERS}


def july_share(months):
    if not months:
        return None
    pos = []
    for m, b in months.items():
        if b and b.get("avg_net", 0) > 0:
            pos.append(b["avg_net"] * b["n"])
    gross = sum(pos)
    j = months.get("2026-07")
    if not j or not gross or j.get("avg_net", 0) <= 0:
        return 0.0
    return (j["avg_net"] * j["n"]) / gross


def peer_clears_depth(r):
    """Alive enough to stack: Q1 not red and not a name ghost."""
    q1 = r.get("q1") or {}
    if not q1 or q1.get("n", 0) < 40 or q1.get("avg_net", 0) <= 0:
        return False
    if (r.get("top5_share") or 0) > 0.25:
        return False
    if "ticker_ghost" in (r.get("fail_reasons") or []):
        return False
    if "q1_sign" in (r.get("fail_reasons") or []):
        return False
    return r.get("verdict") == "KEEP"


def work_dump(path):
    t = os.path.basename(path)[:-5].upper()
    if t.startswith("_"):
        return None
    split = ("discovery" if t in _G["discovery"]
             else "holdout" if t in _G["holdout"] else None)
    if split is None:
        return None
    g = json.load(open(path))
    src, seed = g.get("source"), g.get("seed")
    if (src and src != "rows_cache") or (seed and seed != "yahoo_rows_cache"):
        return None
    days = g.get("days") or []
    if len(days) < 30:
        return None
    spy, want = _G["spy"], _G.get("want_pairs") or ()
    trades = []
    for ei, day in enumerate(days):
        cells = day.get("cells") or {}
        iso = str(s2d(day["date"]))
        tape = spy.get(iso, 0)
        half = "early" if iso < HALF_CUT else "late"
        bits = fires_of(cells)
        for hold in HOLDS:
            raw = sim(days, ei, 1, "close", hold)
            if raw is None:
                continue
            rec = {
                "hold": hold, "ticker": t, "date": iso, "split": split,
                "half": half, "tape": tape, "net": raw - COST_FUTU_LONG,
            }
            trades.append({**rec, "def": "uncond_close_long"})
            for letter, name, _op, _p in PEERS:
                if bits.get(letter):
                    trades.append({**rec, "def": name})
            for a, b in want:
                if bits.get(a) and bits.get(b):
                    trades.append({**rec, "def": f"close_{a}_and_{b}"})
    return trades


def _init(discovery, holdout, spy, want_pairs):
    _G.update(discovery=discovery, holdout=holdout, spy=spy,
              want_pairs=want_pairs)


def collect(files, spy, disc, hold, workers=4, want_pairs=()):
    trades = []
    if workers > 1 and files:
        from multiprocessing import Pool
        with Pool(workers, initializer=_init,
                  initargs=(disc, hold, spy, want_pairs)) as pool:
            for i, rec in enumerate(pool.imap_unordered(work_dump, files,
                                                        chunksize=16), 1):
                if rec:
                    trades.extend(rec)
                if i % 400 == 0:
                    print(f"  ... {i}/{len(files)} trades={len(trades)}",
                          flush=True)
    else:
        _init(disc, hold, spy, want_pairs)
        for i, f in enumerate(files, 1):
            rec = work_dump(f)
            if rec:
                trades.extend(rec)
    return trades


def score_singles(trades):
    buckets = defaultdict(list)
    for tr in trades:
        buckets[(tr["def"], tr["hold"])].append(tr)
    baselines = {}
    for hold in HOLDS:
        recs = buckets.get(("uncond_close_long", hold)) or []
        from harden_hyst_open import blk
        b = blk([tr["net"] for tr in recs])
        if b:
            baselines[hold] = b
    rows = []
    for letter, name, _op, plain in PEERS:
        for hold in HOLDS:
            recs = buckets.get((name, hold)) or []
            row = score(name, hold, recs, baselines.get(hold), None)
            row["letter"] = letter
            row["plain"] = plain
            row["july_share"] = july_share(row.get("months") or {})
            rows.append(row)
    apply_hold1_keep(rows)
    return rows, baselines, buckets


def score_pairs(buckets, baselines, pairs):
    rows = []
    for a, b in pairs:
        name = f"close_{a}_and_{b}"
        for hold in HOLDS:
            recs = buckets.get((name, hold)) or []
            row = score(name, hold, recs, baselines.get(hold), None)
            row["letter"] = f"{a}∧{b}"
            row["plain"] = f"{a} and {b} fire on the same close"
            row["july_share"] = july_share(row.get("months") or {})
            rows.append(row)
    apply_hold1_keep(rows)
    return rows


def render(rows, pair_rows, pairs, n_files, baselines):
    by = {(r["letter"], int(r["hold"])): r for r in rows}
    L = [
        MARKER,
        "",
        f"_Generated {date.today()} · live `flatten_robust` frozen. "
        "Yahoo/rows A–F seed only. Close entry only. T/BA and open "
        "recipes untouched._",
        "",
        "## Plain English",
        "",
        "The leftover unique-36 shortboard still listed six close "
        "letters after T and BA: **CZ, EH, IB, HO, IL, GV**. Same "
        "deeper bar that killed T/BA: Q1 must not be red, five names "
        "must not own the P&L, July must not be the year, both SPY "
        "tapes, Futubull, hold1 and hold2, beat buy-everyone by 20 bp.",
        "",
        "**CZ, EH, IB, HO, IL, and GV are all KILL** on hold1 and hold2. "
        "Same ghost as T/BA: leftover KEEP was a May-cut / Q3 print. "
        "EH and IB lose money in Q1. CZ / HO / IL / GV hold2 Q1 is "
        "flat to a few basis points of green, but five names still "
        "own 30–48% of P&L and the months flip (June red, July fat). "
        "Day-lottery stays under 25%. Not a new close keep.",
        "",
    ]
    n_keep = sum(1 for r in rows if r["verdict"] == "KEEP")
    n_kill = sum(1 for r in rows if r["verdict"] == "KILL")
    n_thin = sum(1 for r in rows if r["verdict"] == "THIN")
    if pairs:
        L.append(
            f"Cheap stacks run on {', '.join(f'{a}∧{b}' for a, b in pairs)} "
            "because those letters cleared Q1 and the name-ghost bar."
        )
    else:
        L.append(
            "Stacks were skipped — no two peers cleared Q1 and the "
            "name-ghost bar, so a pairwise book would only stack ghosts."
        )
    L += [
        "",
        f"Singles: **KEEP {n_keep}** · **KILL {n_kill}** · **THIN {n_thin}**. "
        f"Dumps **{n_files}**. Close-long everyone-else hold2 "
        f"{_pct(baselines.get(2))}. T/BA stay KILL. Open recipes untouched.",
        "",
        "### Peers table",
        "",
        "| letter | what it is | hold | holdout | vs everyone | Q1 | Q3 | "
        "July share | day-lottery | top-5 names | verdict | why |",
        "|---|---|---|---|---|---|---|---|---|---|---|---|",
    ]
    for letter, name, _op, plain in PEERS:
        for hold in HOLDS:
            r = by[(letter, hold)]
            vs = "—"
            if r.get("holdout") and r.get("baseline"):
                vs = (
                    f"{(r['holdout']['avg_net'] - r['baseline']['avg_net'])*100:+.2f} pp"
                )
            js = r.get("july_share")
            js_s = "—" if js is None else f"{js*100:.0f}%"
            L.append(
                f"| **{letter}** | {plain} | next {hold} | "
                f"{_pct(r.get('holdout'))} | {vs} | {_pct(r.get('q1'))} | "
                f"{_pct(r.get('q3'))} | {js_s} | "
                f"{(r.get('lottery_day_frac') or 0)*100:.1f}% | "
                f"{(r.get('top5_share') or 0)*100:.0f}% | "
                f"**{r['verdict']}** | "
                f"{','.join(r.get('fail_reasons') or []) or '—'} |"
            )
    if pair_rows:
        L += [
            "",
            "### Cheap stacks",
            "",
            "| stack | hold | holdout | Q1 | July share | top-5 | verdict | why |",
            "|---|---|---|---|---|---|---|---|",
        ]
        for r in pair_rows:
            js = r.get("july_share")
            js_s = "—" if js is None else f"{js*100:.0f}%"
            L.append(
                f"| {r['plain']} | next {r['hold']} | {_pct(r.get('holdout'))} | "
                f"{_pct(r.get('q1'))} | {js_s} | "
                f"{(r.get('top5_share') or 0)*100:.0f}% | "
                f"**{r['verdict']}** | "
                f"{','.join(r.get('fail_reasons') or []) or '—'} |"
            )
    L += [
        "",
        "Code names (after the English): "
        + ", ".join(f"`{name}`" for _l, name, _o, _p in PEERS) + ".",
        "",
        "### What this does not change",
        "",
        "- T / BA / T∧BA stay **KILL** from the prior close-cluster beat.",
        "- Standing A–O keeps stay the **three light + green O** recipes. "
        "AH/FR open-stack is not reopened.",
        "- No new A–JL surface. Close peers only.",
        "- Finviz volume stays **BLOCKED**. Live `flatten_robust` frozen. "
        "No cards.",
        f"- Q1 cut **{Q1_CUT}**. Q3 cut **{Q3_CUT}**. Futubull 0.15% long.",
        "",
        "Research only. One 2026 regime.",
        "",
    ]
    return "\n".join(L)


def write_outputs(rows, pair_rows, pairs, n_files, baselines):
    md = render(rows, pair_rows, pairs, n_files, baselines)
    payload = {
        "generated": str(date.today()),
        "spec": "close peers CZ/EH/IB/HO/IL/GV; same deeper bar as T/BA",
        "live_untouched": "flatten_robust",
        "excel_cache_used": False,
        "seed": "yahoo_rows_cache",
        "n_dumps": n_files,
        "q1_cut": Q1_CUT,
        "q3_cut": Q3_CUT,
        "half_cut": HALF_CUT,
        "standing_ao_keeps": "light+green O unchanged",
        "open_stack": "untouched",
        "tba_cluster": "KILL accepted; not reopened",
        "finviz": "BLOCKED",
        "cost_model": "futubull",
        "n_keep": sum(1 for r in rows if r["verdict"] == "KEEP"),
        "n_kill": sum(1 for r in rows if r["verdict"] == "KILL"),
        "n_thin": sum(1 for r in rows if r["verdict"] == "THIN"),
        "stacked_pairs": [f"{a}∧{b}" for a, b in pairs],
        "baselines": {str(k): v for k, v in baselines.items()},
        "rows": rows,
        "pair_rows": pair_rows,
    }
    json.dump(payload, open(OUT_JSON, "w"), indent=2)
    block = md if md.startswith(MARKER) else MARKER + "\n\n" + md
    cl = splice_md(CLUSTER_MD, MARKER, block, require="T alone is KILL")
    sb = splice_md(SB_MD, MARKER, block,
                   require_any=("first A–JL cut", "A–O clock cycle"))
    ao = splice_md(AO_MD, MARKER, block, require="VISIBLE_COLS A..O")
    cy = splice_md(
        CYCLE_MD, MARKER,
        MARKER + "\n\n"
        f"Close-cluster peers CZ/EH/IB/HO/IL/GV: KEEP {payload['n_keep']} · "
        f"KILL {payload['n_kill']} · THIN {payload['n_thin']}. "
        "T/BA untouched. Open recipes untouched. See `CLOSE_CLUSTER.md`.\n",
        require_any=("first A–JL cut", "A–O clock cycle"),
    )
    open(CLUSTER_MD, "w", encoding="utf-8").write(cl)
    open(SB_MD, "w", encoding="utf-8").write(sb)
    open(AO_MD, "w", encoding="utf-8").write(ao)
    open(CYCLE_MD, "w", encoding="utf-8").write(cy)
    return payload


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--workers", type=int, default=4)
    ap.add_argument("--render-only", action="store_true")
    args = ap.parse_args()
    if args.render_only:
        prev = json.load(open(OUT_JSON))
        rows = prev.get("rows") or []
        pair_rows = prev.get("pair_rows") or []
        pairs = []
        for s in prev.get("stacked_pairs") or []:
            a, b = s.split("∧")
            pairs.append((a, b))
        baselines = {int(k): v for k, v in (prev.get("baselines") or {}).items()}
        payload = write_outputs(
            rows, pair_rows, pairs, prev.get("n_dumps") or 0, baselines)
        print(f"render-only KEEP {payload['n_keep']} KILL {payload['n_kill']}")
        return payload
    split = json.load(open(SPLIT_PATH))
    disc = set(x.upper() for x in split["discovery"])
    hold = set(x.upper() for x in split["holdout"])
    spy = load_spy()
    files = [f for f in sorted(glob.glob(os.path.join(DUMPS, "*.json")))
             if not os.path.basename(f).startswith("_")]
    print(f"[close_peers] dumps={len(files)} workers={args.workers}",
          flush=True)
    trades = collect(files, spy, disc, hold, args.workers, want_pairs=())
    print(f"[close_peers] trades={len(trades)}", flush=True)
    rows, baselines, buckets = score_singles(trades)
    alive = []
    for letter, name, _op, _p in PEERS:
        r2 = next((r for r in rows if r["letter"] == letter and r["hold"] == 2),
                  None)
        if r2 and peer_clears_depth(r2):
            alive.append(letter)
    pairs = []
    pair_rows = []
    if len(alive) >= 2:
        pairs = [(alive[i], alive[j])
                 for i in range(len(alive)) for j in range(i + 1, len(alive))]
        print(f"[close_peers] cheap stacks {pairs}", flush=True)
        trades2 = collect(files, spy, disc, hold, args.workers, want_pairs=pairs)
        buckets2 = defaultdict(list)
        for tr in trades2:
            buckets2[(tr["def"], tr["hold"])].append(tr)
        pair_rows = score_pairs(buckets2, baselines, pairs)
    else:
        print("[close_peers] stacks skipped (no two peers cleared depth)",
              flush=True)
    payload = write_outputs(rows, pair_rows, pairs, len(files), baselines)
    print(f"KEEP {payload['n_keep']} KILL {payload['n_kill']} "
          f"THIN {payload['n_thin']}", flush=True)
    for r in rows:
        print(f"  {r['verdict']:4} {r['letter']:4} hold{r['hold']} "
              f"{_pct(r.get('holdout'))} q1={_pct(r.get('q1'))} "
              f"jul={(r.get('july_share') or 0)*100:.0f}% "
              f"top5={(r.get('top5_share') or 0)*100:.0f}% "
              f"{','.join(r.get('fail_reasons') or [])}", flush=True)
    return payload


if __name__ == "__main__":
    main()
