"""Join verdict + second-regime / as-of Finviz on KEEP-6 lights.

Incremental: light alone vs light+open-color vs leak-free joins
(AB / weather / book / Finviz). Snapshot Finviz is not as-of.

  python engine/harden_joins.py              # remine O + as-of volM
  python engine/harden_joins.py --render-only
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
from clock import COST_FUTU_LONG, SHIP, VISIBLE, annotate_days, simulate_clock  # noqa: E402
from harden_hyst_open import (  # noqa: E402
    CANDIDATE_KEYS, CANDIDATE_NAMES, HALF_CUT, PLAIN, apply_hold1_keep,
    blk, candidate_pats, lottery_day, s2d, splice_md,
)
from mine_clock import lottery as lottery_trade  # noqa: E402
from mine_first import load_spy  # noqa: E402
from patterns import detect_pattern  # noqa: E402
from pit_joins import (  # noqa: E402
    coverage, load_ab_tones, load_book_buys, load_finviz_asof_highvol,
    load_weather_risk, prior_lookup,
)

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
REPO = os.path.dirname(ROOT)
GRIDS = os.environ.get("GRIDS_DIR", os.path.join(ROOT, "grids"))
SPLIT_PATH = os.path.join(HERE, "holdout_split.json")
RESEARCH = os.path.join(ROOT, "research")
SCOREBOARD = os.path.join(REPO, "03_scoreboard")
MARKER = "## Join verdict (light vs color vs leak-free joins)"
REGIME_CUT = "2026-07-01"  # Q1–Q2 vs Q3 2026 — held out past the May 1 half
O_IDX = VISIBLE.index("O")

_G = {}


def _pct(b):
    if not b or b.get("avg_net") is None:
        return "—"
    return f"{b['avg_net']*100:+.2f}% (n={b['n']})"


def _pp(child, parent):
    if not child or not parent:
        return None
    if child.get("avg_net") is None or parent.get("avg_net") is None:
        return None
    return (child["avg_net"] - parent["avg_net"]) * 100


def score_trades(name, hold, trades, baseline, parent=None, extra_reasons=None):
    disc = [tr["net"] for tr in trades if tr["split"] == "discovery"]
    hout = [tr["net"] for tr in trades if tr["split"] == "holdout"]
    early = [tr["net"] for tr in trades if tr["half"] == "early"]
    late = [tr["net"] for tr in trades if tr["half"] == "late"]
    q12 = [tr["net"] for tr in trades if tr["date"] < REGIME_CUT]
    q3 = [tr["net"] for tr in trades if tr["date"] >= REGIME_CUT]
    up = [tr["net"] for tr in trades if tr["tape"] == 1]
    dn = [tr["net"] for tr in trades if tr["tape"] == -1]
    d, h = blk(disc), blk(hout)
    e, l = blk(early), blk(late)
    r1, r2 = blk(q12), blk(q3)
    su, sd = blk(up), blk(dn)
    lot_bad, lot_frac, _tr = lottery_trade(disc if disc else hout)
    day_bad, day_frac, top_day, _pnl, _n = lottery_day(
        [tr for tr in trades if tr["split"] == "discovery"] or trades)
    tickers = {tr["ticker"] for tr in trades}
    dates = {tr["date"] for tr in trades}
    reasons = list(extra_reasons or [])
    if not d or d["n"] < SHIP["disc_n"]:
        reasons.append("thin_disc")
    if not h or h["n"] < SHIP["hold_n"]:
        reasons.append("thin_hold")
    if d and d["t"] < SHIP["disc_t"]:
        reasons.append("disc_t")
    if h and h["t"] < SHIP["hold_t"]:
        reasons.append("hold_t")
    if d and d["avg_net"] <= 0:
        reasons.append("disc_sign")
    if h and h["avg_net"] <= 0:
        reasons.append("hold_sign")
    if len(tickers) < SHIP["n_tickers"]:
        reasons.append("ticker_bar")
    if len(dates) < SHIP["n_dates"]:
        reasons.append("date_bar")
    if lot_bad:
        reasons.append("lottery_trade")
    if day_bad:
        reasons.append("lottery_day")
    if e and l and e["n"] >= SHIP["min_tape_n"] and l["n"] >= SHIP["min_tape_n"]:
        if l["avg_net"] <= 0 or (e["avg_net"] > 0) != (l["avg_net"] > 0):
            reasons.append("tape_split")
    else:
        reasons.append("tape_split")
    if su and sd and su["n"] >= SHIP["min_tape_n"] and sd["n"] >= SHIP["min_tape_n"]:
        if su["avg_net"] <= 0 or sd["avg_net"] <= 0:
            reasons.append("spy_regime")
    else:
        reasons.append("spy_regime")
    cmp = h if h else d
    if baseline and cmp and cmp["avg_net"] < baseline["avg_net"] + 0.002:
        reasons.append("no_edge_vs_uncond")
    if parent and cmp and cmp["avg_net"] < parent["avg_net"] + 0.002:
        reasons.append("no_edge_vs_parent")
    if r1 and r2 and r1["n"] >= SHIP["min_tape_n"] and r2["n"] >= SHIP["min_tape_n"]:
        if r2["avg_net"] <= 0 or (r1["avg_net"] > 0) != (r2["avg_net"] > 0):
            reasons.append("second_regime")
    else:
        reasons.append("second_regime")
    reasons = list(dict.fromkeys(reasons))
    size = {"thin_disc", "thin_hold", "ticker_bar", "date_bar"}
    quality = set(reasons) - size
    if not reasons:
        verdict = "KEEP"
    elif quality:
        verdict = "KILL"
    else:
        verdict = "THIN"
    return {
        "def": name, "hold": hold, "exit": f"hold{hold}", "clock": "open",
        "cost_model": "futubull", "verdict": verdict, "fail_reasons": reasons,
        "n_tickers": len(tickers), "n_dates": len(dates),
        "discovery": d, "holdout": h, "early": e, "late": l,
        "q12": r1, "q3": r2, "spy_up": su, "spy_dn": sd, "parent": parent,
        "lottery_day_frac": day_frac, "lottery_top_day": top_day,
        "live_untouched": "flatten_robust",
    }


def _init(discovery, holdout, pats, spy, fz_asof):
    _G.update(discovery=discovery, holdout=holdout, pats=pats, spy=spy,
              fz_asof=fz_asof)


def work_grid(path):
    t = os.path.basename(path)[:-5].upper()
    if t.startswith("_"):
        return None
    split = ("discovery" if t in _G["discovery"]
             else "holdout" if t in _G["holdout"] else None)
    if split is None:
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
    last = len(days) - 1
    spy = _G["spy"]
    trades = []
    for pat in _G["pats"]:
        try:
            clusters = detect_pattern(days, pat)
        except Exception:
            continue
        for c in clusters:
            if c.get("side") != 1:
                continue
            ei = c["entry_idx"]
            iso = str(s2d(days[ei]["date"]))
            fams = days[ei].get("fams") or []
            o_green = ei < len(days) and O_IDX < len(fams) and fams[O_IDX] == "green"
            _asof, hi_vol = prior_lookup(_G["fz_asof"], iso, t)
            for hold in (1, 2):
                raw = simulate_clock(days, c, "open", f"hold{hold}")
                if raw is None:
                    continue
                rec = {
                    "hold": hold, "ticker": t, "date": iso, "split": split,
                    "half": "early" if iso < HALF_CUT else "late",
                    "tape": spy.get(iso, 0), "net": raw - COST_FUTU_LONG,
                }
                trades.append({**rec, "def": pat["name"]})
                if o_green:
                    trades.append({**rec, "def": f"{pat['name']}__O_green"})
                if hi_vol:
                    trades.append({**rec, "def": f"{pat['name']}__fz_volM_asof"})
                if o_green and hi_vol:
                    trades.append({**rec, "def": f"{pat['name']}__O_green__fz_volM_asof"})
    return trades


def collect(files, pats, spy, disc, hold, fz_asof, workers=4):
    trades = []
    if workers > 1 and files:
        from multiprocessing import Pool
        with Pool(workers, initializer=_init,
                  initargs=(disc, hold, pats, spy, fz_asof)) as pool:
            for i, rec in enumerate(pool.imap_unordered(work_grid, files,
                                                        chunksize=16), 1):
                if rec:
                    trades.extend(rec)
                if i % 400 == 0:
                    print(f"  ... {i}/{len(files)} trades={len(trades)}",
                          flush=True)
    else:
        _init(disc, hold, pats, spy, fz_asof)
        for f in files:
            rec = work_grid(f)
            if rec:
                trades.extend(rec)
    return trades


def load_existing_join_cells():
    path = os.path.join(RESEARCH, "color_join_mine.json")
    if not os.path.exists(path):
        return []
    return json.load(open(path)).get("cells") or []


def render(existing, scored, fz_cov, n_grids):
    parents = json.load(open(os.path.join(RESEARCH, "hyst_open_harden.json")))
    parent_h = {(r["def"], int(r["hold"])): r for r in parents["candidates"]}
    color = json.load(open(os.path.join(RESEARCH, "color_harden.json")))
    color_r = {(r["def"], int(r["hold"])): r for r in color["recipes"]}

    joins = [c for c in existing if c.get("family") == "join"]
    ab = [c for c in joins if c["def"].endswith("__ab_good")]
    wx = [c for c in joins if "__wx_" in c["def"]]
    book = [c for c in joins if "__book_" in c["def"]]
    fz_snap = [c for c in joins if "__fz_volM" in c["def"] and "asof" not in c["def"]]

    asof_rows = [r for r in scored if r["def"].endswith("__fz_volM_asof")]
    triple = [r for r in scored if "__O_green__fz_volM_asof" in r["def"]]
    o_rows = [r for r in scored if r["def"].endswith("__O_green")
              and "fz_" not in r["def"]]

    L = [
        MARKER,
        "",
        "_Generated 2026-09-07. Joins on top of the six KEEP lights. "
        "Futubull fees. Live `flatten_robust` frozen. No cards._",
        "",
        "### Incremental layers",
        "",
        "Same ship bar as harden: both 2026 halves (cut 2026-05-01), SPY up "
        "and down, top-day lottery, beat the previous layer by ≥20 bp. "
        "AB / weather / overnight book / Finviz **as-of** use the last file "
        "**dated before** the entry. Same-day files are not knowable at 9:30.",
        "",
        f"Finviz Elite dated exports: **{fz_cov['n_dates']}** days "
        f"({fz_cov['first']}–{fz_cov['last']}). One spring file (2026-04-26), "
        "then a gap until mid-August. That is not a 2026 history. The old "
        "snapshot `fz_volM` KEEP is **not shippable**.",
        "",
        "Second-regime cut for surviving color folds: **2026-07-01** "
        "(Q1–Q2 vs Q3), past the May 1 half already used to KEEP the lights.",
        "",
        f"Grids **{n_grids}**, Yahoo/rows only.",
        "",
        "### Light alone vs light+color vs joins",
        "",
        "| layer | meaning | hold | holdout after fees | vs light | verdict | why |",
        "|---|---|---|---|---|---|---|",
    ]

    def add_row(layer, meaning, hold, hout, vs, verdict, why):
        L.append(
            f"| {layer} | {meaning} | next {hold} | {hout} | {vs} | "
            f"**{verdict}** | {why or '—'} |"
        )

    for name, rule in CANDIDATE_KEYS:
        hold = int(rule[4:])
        p = parent_h[(name, hold)]
        add_row("light alone", PLAIN[name], hold, _pct(p.get("holdout")),
                "—", "KEEP", "already hardened")
        cr = color_r.get((name, hold))
        if cr:
            vs = (f"{cr['best_vs_parent_pp']:+.2f} pp"
                  if cr.get("best_vs_parent_pp") is not None else "—")
            add_row("light + color",
                    f"best open color on that light ({cr.get('best_letter') or '—'} green)",
                    hold, _pct(cr.get("best_holdout")), vs, cr["verdict"],
                    ",".join(cr.get("fail_reasons") or []) or "green O ≥20 bp")

    # snapshot Finviz — demote
    for c in sorted(fz_snap, key=lambda x: -((x.get("holdout") or {}).get("avg_net") or -9)):
        vs = _pp(c.get("holdout"), c.get("parent"))
        vs_s = f"{vs:+.2f} pp" if vs is not None else "—"
        add_row("Finviz snapshot (not as-of)",
                "today's high-vol tag on the light — current CSV, not 2026 history",
                c["hold"], _pct(c.get("holdout")), vs_s, "BLOCKED",
                "no historical as-of; cannot ship")

    for c in asof_rows:
        vs = _pp(c.get("holdout"), c.get("parent"))
        vs_s = f"{vs:+.2f} pp" if vs is not None else "—"
        add_row("Finviz as-of high-vol",
                "prior Elite export had Volatility (Month) > 8%",
                c["hold"], _pct(c.get("holdout")), vs_s, c["verdict"],
                ",".join(c.get("fail_reasons") or []))

    for c in triple:
        vs = _pp(c.get("holdout"), c.get("parent"))
        vs_s = f"{vs:+.2f} pp" if vs is not None else "—"
        add_row("light + O + as-of high-vol",
                "green O and prior Elite high-vol together",
                c["hold"], _pct(c.get("holdout")), vs_s, c["verdict"],
                ",".join(c.get("fail_reasons") or []))

    for label, rows in (("AB (yesterday)", ab), ("weather (yesterday)", wx),
                        ("overnight book (yesterday)", book)):
        if not rows:
            add_row(label, "no joinable tape", 1, "—", "—", "KILL", "no coverage")
            continue
        best = max(rows, key=lambda x: (x.get("holdout") or {}).get("avg_net") or -9)
        vs = _pp(best.get("holdout"), best.get("parent"))
        vs_s = f"{vs:+.2f} pp" if vs is not None else "—"
        add_row(label, best.get("plain") or label, best["hold"],
                _pct(best.get("holdout")), vs_s, best["verdict"],
                ",".join(best.get("fail_reasons") or []))

    L += [
        "",
        "### Second regime (Q1–Q2 vs Q3) on light + green O",
        "",
        "Held-out regime starts **2026-07-01**. KEEP only if Q3 stays green, "
        "still beats the parent light by 20 bp on the ticker-holdout, and "
        "clears the usual tape / lottery bar.",
        "",
        "| meaning | hold | Q1–Q2 | Q3 (held out) | ticker-holdout | vs light | fattest day | verdict | why |",
        "|---|---|---|---|---|---|---|---|---|",
    ]
    if not o_rows:
        L.append("| (re-mine not run) | — | — | — | — | — | — | **THIN** | run `harden_joins.py` |")
    for r in sorted(o_rows, key=lambda x: (x["def"], x["hold"])):
        vs = _pp(r.get("holdout"), r.get("parent"))
        vs_s = f"{vs:+.2f} pp" if vs is not None else "—"
        L.append(
            f"| {PLAIN.get(r['def'].split('__')[0], r['def'])} plus green O. | "
            f"next {r['hold']} | {_pct(r.get('q12'))} | {_pct(r.get('q3'))} | "
            f"{_pct(r.get('holdout'))} | {vs_s} | "
            f"{(r.get('lottery_day_frac') or 0)*100:.1f}% | **{r['verdict']}** | "
            f"{','.join(r.get('fail_reasons') or []) or '—'} |"
        )
    L += [
        "",
        "**Join finding.** The only incremental KEEP versus the light is a "
        "green **O**. AB / weather / book are yesterday-knowable but too "
        "short (late Aug–Sep) — **KILL**. Finviz high-vol as a **snapshot** "
        "is **BLOCKED** (not 2026 history). Dated Elite exports are too "
        "gappy to ship. Light+O still has to clear Q3; see the table. "
        "Research only. No live wire.",
        "",
    ]
    return "\n".join(L) + "\n"


def write_outputs(existing, scored, fz_cov, n_grids):
    md = render(existing, scored, fz_cov, n_grids)
    open(os.path.join(RESEARCH, "JOIN_VERDICT.md"), "w", encoding="utf-8").write(md)
    # Also refresh the standing color-join report lead-in with a pointer.
    payload = {
        "generated": str(date.today()),
        "spec": "join verdict + Q3 second regime + Finviz as-of",
        "regime_cut": REGIME_CUT,
        "grids": n_grids,
        "finviz_asof": fz_cov,
        "excel_cache_used": False,
        "cost_model": "futubull",
        "live_untouched": "flatten_robust",
        "scored": scored,
    }
    json.dump(payload, open(os.path.join(RESEARCH, "join_verdict.json"), "w"),
              indent=2)
    ao = os.path.join(RESEARCH, "AO_FIRST_MINE.md")
    sb = os.path.join(SCOREBOARD, "EXCEL_BOT_MINE.md")
    cy = os.path.join(RESEARCH, "MINE_CYCLE.md")
    ao_text = splice_md(ao, MARKER, md, require="VISIBLE_COLS A..O")
    sb_text = splice_md(sb, MARKER, md,
                        require_any=("first A–JL cut", "A–O clock cycle"))
    n_keep = sum(1 for r in scored if r["verdict"] == "KEEP")
    n_kill = sum(1 for r in scored if r["verdict"] == "KILL")
    note = (
        MARKER + "\n\n"
        f"Re-scored cells KEEP {n_keep} · KILL {n_kill}. "
        "Snapshot Finviz volM is BLOCKED. AB/weather/book KILL (short tape). "
        "See `JOIN_VERDICT.md`.\n"
    )
    cy_text = splice_md(cy, MARKER, note,
                        require_any=("first A–JL cut", "A–O clock cycle"))
    open(ao, "w", encoding="utf-8").write(ao_text)
    open(sb, "w", encoding="utf-8").write(sb_text)
    open(cy, "w", encoding="utf-8").write(cy_text)
    # Point COLOR_JOIN_MINE at the new verdict (do not wipe it).
    cj = os.path.join(RESEARCH, "COLOR_JOIN_MINE.md")
    if os.path.exists(cj):
        old = open(cj, encoding="utf-8").read()
        pointer = (
            "\n\n**Update — Finviz snapshot `fz_volM` rows above that say "
            "KEEP are BLOCKED.** That tag is today’s CSV, not 2026 history. "
            "See the second-regime section.\n"
            "\n## Second-regime / as-of Finviz\n\n"
            "_Generated 2026-09-07. No new color-alone or AB/weather/book "
            "mines. Live `flatten_robust` frozen._\n\n"
            "### Split we used\n\n"
            "The first harden already cut the year at **2026-05-01**. This "
            "beat holds out a later slice: **Q1–Q2 vs Q3**, cut "
            f"**{REGIME_CUT}**. Same Futubull fees, both SPY tapes, "
            "top-day lottery, and the fold must still beat the parent "
            "light by 20 bp.\n\n"
            "### Light + green O — does Q3 still pay?\n\n"
            "See the Q3 table on `JOIN_VERDICT.md` / "
            "`03_scoreboard/EXCEL_BOT_MINE.md`. Three recipes still KEEP "
            "(five-cell + O hold1/2, nine-cell + O hold2). Snapshot "
            "`fz_volM` is **BLOCKED**; as-of Elite high-vol is **KILL** "
            "(gappy). `fz_volM` cannot ship until as-of history covers "
            "both tape halves.\n"
        )
        if "## Second-regime / as-of Finviz" in old:
            old = old.split("## Second-regime / as-of Finviz", 1)[0].rstrip() + pointer
        else:
            old = old.rstrip() + pointer
        open(cj, "w", encoding="utf-8").write(old)
    return payload


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--workers", type=int, default=4)
    ap.add_argument("--render-only", action="store_true")
    args = ap.parse_args()
    existing = load_existing_join_cells()
    fz_asof = load_finviz_asof_highvol()
    fz_cov = coverage(fz_asof)
    print(f"finviz as-of dates={fz_cov}", flush=True)
    scored = []
    n_grids = 0
    if not args.render_only:
        split = json.load(open(SPLIT_PATH))
        disc, hold = set(x.upper() for x in split["discovery"]), set(
            x.upper() for x in split["holdout"])
        spy = load_spy()
        pats = [p for p in candidate_pats() if p["name"] in CANDIDATE_NAMES]
        files = [f for f in sorted(glob.glob(os.path.join(GRIDS, "*.json")))
                 if not os.path.basename(f).startswith("_")]
        n_grids = len(files)
        print(f"[joins] grids={n_grids} pats={len(pats)} workers={args.workers}",
              flush=True)
        trades = collect(files, pats, spy, disc, hold, fz_asof, args.workers)
        print(f"[joins] trades={len(trades)}", flush=True)
        parents = json.load(open(os.path.join(RESEARCH, "hyst_open_harden.json")))
        parent_h = {(r["def"], int(r["hold"])): r.get("holdout")
                    for r in parents["candidates"]}
        baselines = {}
        ao = os.path.join(RESEARCH, "ao_first_mine.json")
        if os.path.exists(ao):
            baselines = json.load(open(ao)).get("baselines") or {}
        buckets = defaultdict(list)
        for tr in trades:
            buckets[(tr["def"], tr["hold"])].append(tr)
        for (name, hold), recs in sorted(buckets.items()):
            parent = None
            extra = []
            if "__" in name:
                pname = name.split("__", 1)[0]
                parent = parent_h.get((pname, hold))
            if "fz_volM_asof" in name:
                extra.append("finviz_asof_gappy")
            scored.append(score_trades(
                name, hold, recs,
                baselines.get(f"open_long_h{hold}"),
                parent, extra))
        apply_hold1_keep(scored)
    else:
        prev = os.path.join(RESEARCH, "join_verdict.json")
        if os.path.exists(prev):
            scored = json.load(open(prev)).get("scored") or []
            n_grids = json.load(open(prev)).get("grids") or 0
    write_outputs(existing, scored, fz_cov, n_grids)
    print("wrote JOIN_VERDICT.md")
    for r in scored:
        if "__" in r["def"]:
            print(f"  {r['verdict']:7} {r['def']:40} hold{r['hold']} "
                  f"{','.join(r['fail_reasons'][:4])}")


if __name__ == "__main__":
    main()
