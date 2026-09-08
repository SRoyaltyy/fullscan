"""Open-only shade fills → 2d / 3d cumulative stacked I.

Re-cut of the #150/#151 expand panel (M fill IZ=1 → #95CA82, never M's
number). Primary labels are HI_HORIZON clock-clean stacked daily I:

  I_sum[k] = ∏_{j=0..k-1} (1 + I[t+j]) − 1    for k=2, 3

Same-day H is a baseline compare only. Same-row H/I are never features.
Open fills only: A B C G J K L M O IR IS IT. This expand paints M.

H-fill diagnostic (close-knowable, never KEEP): Excel CF paints H>5% as
#95CA82 and H>0 as pale #DCEDD5 — that is same-day outcome paint.

Live flatten_robust frozen. No cards. No live push.

  python3 engine/mine_shade_2d3d.py --build-panel
  python3 engine/mine_shade_2d3d.py
  python3 engine/mine_shade_2d3d.py --render-only
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from collections import defaultdict
from datetime import date, timedelta
from multiprocessing import Pool

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from clock import COST_FUTU_LONG, VISIBLE  # noqa: E402
from expand_shade_panel import (  # noqa: E402
    extend_spy, load_iy, load_ticker_rows, merge_rows, paint_m,
    panel_stats, write_rows, yahoo_one,
)
from harden_hyst_open import splice_md  # noqa: E402
from mine_hi_horizon import labels_for as hi_labels_for, load_spy_regimes  # noqa: E402
from mine_next_region import Q1_CUT, deeper  # noqa: E402
from mine_shade_open import (  # noqa: E402
    BEAT, CLOSE_FILL, FILL_IDX, HALF_CUT, HEX_NAME, OPEN_ALIASES, OPEN_FILL,
    SIZE_FAIL, assert_open_gate, classify, dump_inventory, ghost_score,
    heat_cuts, heat_name, meaning_of, parent_of, recipe_hits, soft_majority,
    tape_name, trailing_i, _avg, _band, _blk, _cell, _july_share, _push,
    _slot, _top_share, _worst,
)
from mine_unmined import pack_row  # noqa: E402
from clock import annotate_days  # noqa: E402
from harden_hyst_open import s2d  # noqa: E402
from signals import classify_fill  # noqa: E402

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
REPO = os.path.dirname(ROOT)
ROWS = os.environ.get("ROWS_DIR", os.path.join(ROOT, "data", "rows"))
GRIDS = os.environ.get("GRIDS_DIR", os.path.join(ROOT, "grids"))
SPLIT_PATH = os.path.join(HERE, "holdout_split.json")
RESEARCH = os.path.join(ROOT, "research")
SCOREBOARD = os.path.join(REPO, "03_scoreboard")
OUT_MD = os.path.join(RESEARCH, "SHADE_2D3D.md")
OUT_JSON = os.path.join(RESEARCH, "shade_2d3d.json")
STATS = os.path.join(RESEARCH, "shade_panel_stats.json")
CARDS = os.path.join(RESEARCH, "SHADE_KEEP_CARDS.md")
SB_MD = os.path.join(SCOREBOARD, "EXCEL_BOT_MINE.md")
CYCLE_MD = os.path.join(RESEARCH, "MINE_CYCLE.md")
CLOCK_MAP = os.path.join(RESEARCH, "clock_map.json")
MARKER = "## Shade 2d/3d cumulative (open-entry)"
CYCLE_MARKER = "## Shade 2d/3d cumulative (open-entry stacked I)"

# HI_HORIZON: stacked daily I is the cumulative move. H print is baseline.
PRIMARY = (("I_sum", 2), ("I_sum", 3))
BASELINE_H = ("H", 1)
HORIZON_PLAIN = {1: "1d", 2: "2d", 3: "3d"}
LABEL_PLAIN = {
    ("H", 1): "same-day H (intraday %, baseline only)",
    ("I_sum", 2): "2d stacked I (HI_HORIZON I_sum, compound daily I)",
    ("I_sum", 3): "3d stacked I (HI_HORIZON I_sum, compound daily I)",
}

# Open-knowable shade recipes that fire on this M-only expand.
FOCUS = (
    "M_green", "M_ge15", "M_hex_95CA82",
    "M_onset_green", "M_onset_hex_95CA82",
)
PARENTS = {
    "M_ge15": "M_green",
    "M_hex_95CA82": "M_green",
    "M_onset_green": "M_green",
    "M_onset_hex_95CA82": "M_green",
}
# Close-knowable H CF — diagnostic only, never a KEEP path.
HFILL_GT05 = "Hfill_gt05"
HFILL_GT00 = "Hfill_gt00"
# Excel CF (first match, lower priority number wins):
#   H > 0.05 → #95CA82   H > 0 → #DCEDD5
#   0 ≥ H ≥ −0.029 → #FF5050   H < −0.03 → #FFC7CE
H_CF_MID = "95CA82"
H_CF_PALE = "DCEDD5"

PANEL_START = date(2018, 9, 1)


def assert_clock_map():
    """Refuse to invent open fills. Source: CLOCK_MAP + OPEN_SAME_ROW_LABELS."""
    blob = json.load(open(CLOCK_MAP))
    groups = blob.get("groups") or {}
    licensed = tuple(groups.get("fill_mine_open") or blob.get("fill_mine_open") or [])
    want = tuple(OPEN_FILL) + OPEN_ALIASES
    if licensed != want and set(licensed) != set(want):
        raise AssertionError(
            f"clock_map fill_mine_open {licensed} != licensed open fills {want}"
        )
    for c in ("H", "I", "D", "E", "F", "N"):
        if c in licensed:
            raise AssertionError(f"close fill {c} leaked into fill_mine_open")
    assert_open_gate()
    assert FILL_IDX["M"] == VISIBLE.index("M") == 12
    assert "H" not in OPEN_FILL and "I" not in OPEN_FILL


def stacked_i(ivals, ei, k):
    """HI_HORIZON I_sum: compound daily I over k sessions starting at ei."""
    acc = 1.0
    for j in range(ei, ei + k):
        if j >= len(ivals) or ivals[j] is None:
            return None
        acc *= (1.0 + ivals[j])
    return acc - 1.0


def labels_at(days, ei):
    """Same definitions as mine_hi_horizon.labels_for for I_sum; H is 1d only."""
    ivals = [d.get("i_ret") for d in days]
    out = {}
    h = days[ei].get("h_ret")
    if h is not None:
        out[BASELINE_H] = h
    for k in (2, 3):
        v = stacked_i(ivals, ei, k)
        if v is not None:
            out[("I_sum", k)] = v
    return out


def _hi_slim(days):
    return {
        "n": len(days),
        "H": [d.get("h_ret") for d in days],
        "I": [d.get("i_ret") for d in days],
    }


def assert_label_match(days):
    """I_sum 2/3 must match HI_HORIZON labels_for. Abort on drift."""
    slim = _hi_slim(days)
    for ei in range(max(0, len(days) - 8), max(0, len(days) - 3)):
        mine = labels_at(days, ei)
        for k in (2, 3):
            ref = hi_labels_for(slim, ei, k)
            got = mine.get(("I_sum", k))
            if ref is None:
                if got is not None:
                    raise AssertionError(f"I_sum {k} extra at {ei}")
                continue
            if got is None or abs(got - ref["I_sum"]) > 1e-12:
                raise AssertionError(
                    f"I_sum {k} drift at {ei}: mine={got} hi={ref['I_sum']}"
                )


def hfill_hits(h_ret):
    """Close-knowable H CF. Not an open feature. Diagnostic only."""
    if h_ret is None:
        return []
    out = []
    if h_ret > 0.05:
        out.append((HFILL_GT05, "hfill", "H", H_CF_MID))
    if h_ret > 0:
        out.append((HFILL_GT00, "hfill", "H", H_CF_PALE if h_ret <= 0.05 else H_CF_MID))
    return out


def meaning_hfill(name):
    if name == HFILL_GT05:
        return (
            "same-row H > 5% paints #95CA82 (Excel CF pri 126) — "
            "close-knowable outcome paint, not an open feature"
        )
    if name == HFILL_GT00:
        return (
            "same-row H > 0 paints pale #DCEDD5 (Excel CF pri 171) — "
            "close-knowable outcome paint, not an open feature"
        )
    return meaning_of(name)


def push_hit(cell, net, iso, split, tape, heat, ticker, lo, hi):
    _push(cell["disc"] if split == "discovery" else cell["hold"], net)
    _push(cell["early"] if iso < HALF_CUT else cell["late"], net)
    _push(cell["q12"] if iso < "2026-07-01" else cell["q3"], net)
    if iso < Q1_CUT:
        _push(cell["q1"], net)
    if tape == 1:
        _push(cell["spy_up"], net)
    elif tape == -1:
        _push(cell["spy_dn"], net)
    else:
        _push(cell["spy_flat"], net)
    bucket = heat_name(heat, lo, hi)
    _push(cell[f"heat_{bucket}"], net)
    _push(cell["reg"][(bucket, tape_name(tape))], net)
    cell["tickers"].add(ticker)
    cell["dates"].add(iso)
    cell["ticker_pnl"][ticker] += net
    _push(cell["month"][iso[:7]], net)
    if split == "discovery":
        cell["day"][iso] += net


def work_ticker(job):
    ticker, iy, discovery, holdout, spy = job
    split = ("discovery" if ticker in discovery
             else "holdout" if ticker in holdout else None)
    if split is None:
        return None
    rows = load_ticker_rows(ticker)
    days_raw = paint_m(rows, iy)
    if len(days_raw) < 40:
        return None
    days = annotate_days(days_raw)
    try:
        assert_label_match(days)
    except AssertionError:
        return None
    n = len(days)
    hexes, fams, scores, ivals = [], [], [], []
    dump = defaultdict(int)
    for d in days:
        fills = [(str(x).upper().replace("#", "") if x else None)
                 for x in (d.get("fills") or [])]
        fills = (fills + [None] * 15)[:15]
        if fills[12] and len(fills[12]) == 8 and fills[12].startswith("FF"):
            fills[12] = fills[12][2:]
        hexes.append(fills)
        fams.append(d.get("fams") or ["none"] * 15)
        scores.append(d.get("scores") or [0.0] * 15)
        ivals.append(d.get("i_ret"))
        dump[( "M", fills[FILL_IDX["M"]] or "none")] += 1
    recs_out = []
    book = []
    heats = []
    for ei in range(n):
        labs = labels_at(days, ei)
        if not labs:
            continue
        iso = str(s2d(days[ei]["date"]))
        heat = trailing_i(ivals, ei)
        tape = spy.get(iso)
        if tape is None:
            tape = 0
        if split == "discovery" and heat is not None:
            heats.append(heat)
        recs = recipe_hits(hexes, fams, scores, ei)
        recs = [r for r in recs if r[0] in FOCUS]
        recs.extend(hfill_hits(days[ei].get("h_ret")))
        for (lab, hz), raw in labs.items():
            net = raw - COST_FUTU_LONG
            book.append((ticker, iso, split, lab, hz, net, heat, tape))
            for name, fam, letter, hx in recs:
                recs_out.append((name, fam, letter, hx, lab, hz, net,
                                 ticker, iso, split, heat, tape))
    return {
        "hits": recs_out, "book": book, "heats": heats,
        "dump": dump, "n": n, "ticker": ticker,
    }


def _init():
    pass


def collect(tickers, iy, discovery, holdout, spy, workers=4):
    jobs = [(t, iy, discovery, holdout, spy) for t in tickers]
    hits, book, heats, dumps = [], [], [], []
    n_ok = 0
    if workers > 1 and jobs:
        with Pool(workers) as pool:
            for i, rec in enumerate(pool.imap_unordered(work_ticker, jobs,
                                                        chunksize=8), 1):
                if rec:
                    n_ok += 1
                    hits.extend(rec["hits"])
                    book.extend(rec["book"])
                    heats.extend(rec["heats"])
                    dumps.append(rec["dump"])
                if i % 200 == 0:
                    print(f"  mine … {i}/{len(jobs)} ok={n_ok} hits={len(hits)}",
                          flush=True)
    else:
        for i, job in enumerate(jobs, 1):
            rec = work_ticker(job)
            if rec:
                n_ok += 1
                hits.extend(rec["hits"])
                book.extend(rec["book"])
                heats.extend(rec["heats"])
                dumps.append(rec["dump"])
            if i % 200 == 0:
                print(f"  mine … {i}/{len(jobs)} ok={n_ok} hits={len(hits)}",
                      flush=True)
    return hits, book, heats, dumps, n_ok


def pack_all(hits, book, heats, baselines):
    lo, hi = heat_cuts(heats)
    cells = defaultdict(_cell)
    book_cells = defaultdict(_cell)
    for name, _fam, _let, _hx, lab, hz, net, t, iso, split, heat, tape in hits:
        push_hit(cells[(name, lab, hz)], net, iso, split, tape, heat, t, lo, hi)
    for t, iso, split, lab, hz, net, heat, tape in book:
        push_hit(book_cells[(lab, hz)], net, iso, split, tape, heat, t, lo, hi)
    by_name = {}
    rows = []
    for (name, label, hz), cell in cells.items():
        row = pack_row(name, "open", "long", f"hold{hz}", cell, {})
        if not row:
            continue
        if name.startswith("Hfill_"):
            row["family"] = "hfill_diagnostic"
            row["clock"] = "close"
            row["plain"] = meaning_hfill(name)
        else:
            row["family"] = (
                "onset" if "onset" in name
                else "greener" if "_ge" in name
                else "hex" if "_hex_" in name
                else "green_on"
            )
            row["clock"] = "open"
            row["plain"] = meaning_of(name)
        row["label"] = label
        row["horizon"] = hz
        b = baselines.get(f"{label}_{hz}") or _blk(book_cells[(label, hz)]["hold"])
        if not b:
            b = _blk(book_cells[(label, hz)]["disc"])
        row["baseline"] = b
        reasons = [x for x in (row.get("fail_reasons") or [])
                   if x != "no_edge_vs_uncond"]
        d, h = row.get("discovery") or {}, row.get("holdout") or {}
        if b and (
            (d.get("avg_net") or 0) < (b.get("avg_net") or 0) + BEAT
            or (h and (h.get("avg_net") or 0) < (b.get("avg_net") or 0) + BEAT)
        ):
            reasons.append("no_edge_vs_uncond")
        row["fail_reasons"] = reasons
        deeper(row, cell)
        row["heat_lo"] = lo
        row["heat_hi"] = hi
        row["reg_cells"] = {
            f"{hk}_{tk}": _blk(sl) for (hk, tk), sl in cell["reg"].items()
        }
        if name.startswith("Hfill_"):
            row["fail_reasons"] = list(dict.fromkeys(
                list(row.get("fail_reasons") or []) + ["hfill_outcome_paint"]
            ))
            row["verdict"] = "KILL"
            row["keep"] = "KILL"
            row["clock"] = "close"
        rows.append(row)
        by_name[(name, label, hz)] = row
    for r in rows:
        p = parent_of(r["def"]) if not r["def"].startswith("Hfill_") else None
        if not p:
            continue
        parent = by_name.get((p, r["label"], r["horizon"]))
        if not parent:
            continue
        r["parent"] = p
        ph = parent.get("holdout") or {}
        h = r.get("holdout") or {}
        if ph.get("avg_net") is not None and h.get("avg_net") is not None:
            r["vs_parent_pp"] = (h["avg_net"] - ph["avg_net"]) * 100
            if h["avg_net"] < ph["avg_net"] + BEAT:
                r["fail_reasons"] = list(dict.fromkeys(
                    list(r.get("fail_reasons") or []) + ["no_edge_vs_parent"]
                ))
                if r.get("keep") == "KEEP":
                    r["verdict"] = "KILL"
                    r["keep"] = "KILL"
    for r in rows:
        if r["def"].startswith("Hfill_"):
            continue
        classify(r)
        if "no_edge_vs_parent" in (r.get("fail_reasons") or []):
            if r.get("keep") == "KEEP":
                r["verdict"] = "KILL"
                r["keep"] = "KILL"
    return rows, lo, hi, book_cells


def ghost_trades(hits, book, label, hz):
    """Per-recipe hold/disc trades for ghost_score (same tuple shape)."""
    want = set(FOCUS) | {HFILL_GT05}
    recs = {n: [] for n in want}
    for name, _fam, _let, _hx, lab, hzi, net, t, iso, split, _heat, tape in hits:
        if lab == label and hzi == hz and name in recs:
            recs[name].append((t, iso, split, net, tape))
    b = [(t, iso, split, net, tape) for t, iso, split, lab, hzi, net, _h, tape
         in book if lab == label and hzi == hz]
    return recs, b


def run_ghost(hits, book, labels):
    out = []
    for label, hz in labels:
        recs, b = ghost_trades(hits, book, label, hz)
        for name in FOCUS:
            if name == "M_green":
                continue
            score = ghost_score(name, recs.get(name) or [],
                                recs.get(PARENTS[name]) or [], b)
            score["label"] = label
            score["horizon"] = hz
            score["plain"] = meaning_of(name)
            out.append(score)
        hs = ghost_score(HFILL_GT05, recs.get(HFILL_GT05) or [],
                         recs.get("M_green") or [], b)
        hs["label"] = label
        hs["horizon"] = hz
        hs["plain"] = meaning_hfill(HFILL_GT05)
        hs["clock"] = "close"
        hs["diagnostic"] = True
        out.append(hs)
    return out


def family_verdict(rows):
    prim = [r for r in rows
            if (r.get("label"), r.get("horizon")) in PRIMARY
            and r["def"] in FOCUS and r["def"] != "M_green"
            and not r["def"].startswith("Hfill_")]
    keep = [r for r in prim if r.get("keep") == "KEEP"]
    kill = [r for r in prim if r.get("keep") == "KILL"]
    thin = [r for r in prim if r.get("keep") == "THIN"]
    if keep:
        names = ", ".join(f"`{r['def']}` {r['label']}@{r['horizon']}d"
                          for r in keep[:6])
        return "KEEP", (
            f"Open shade beats any-green parent and the book on stacked I: "
            f"{names}. Research only — not a live wire."
        ), keep, kill, thin
    if thin and not kill:
        return "THIN", "Not enough 2d/3d stacked-I trades to judge shade.", keep, kill, thin
    return "null", (
        "Open-only shade fills do not beat any-green parent on the same "
        "letter for 2d or 3d stacked I after Futubull fees, both SPY tapes, "
        "and the ghost bar. Same-day H baseline is not a KEEP either."
    ), keep, kill, thin


def soft_regime(rows, hits, book, lo, hi):
    keepers = [r for r in rows if r.get("keep") == "KEEP"
               and (r.get("label"), r.get("horizon")) in PRIMARY
               and not r["def"].startswith("Hfill_")]
    if not keepers:
        return []
    out = []
    names = {(r["def"], r["label"], r["horizon"]) for r in keepers}
    for name, lab, hz in sorted(names):
        recs = [h for h in hits if h[0] == name and h[4] == lab and h[5] == hz]
        books = [b for b in book if b[3] == lab and b[4] == hz]
        for hk in ("cold", "mixed", "hot"):
            for tk in ("up", "dn", "flat"):
                rh = [h for h in recs
                      if heat_name(h[10], lo, hi) == hk
                      and tape_name(h[11]) == tk]
                bh = [b for b in books
                      if heat_name(b[6], lo, hi) == hk
                      and tape_name(b[7]) == tk]
                hout = [h[6] for h in rh if h[9] == "holdout"]
                bout = [b[5] for b in bh if b[2] == "holdout"]
                n_h = len(hout)
                avg_h = (sum(hout) / n_h) if n_h else None
                avg_b = (sum(bout) / len(bout)) if bout else None
                vs = ((avg_h - avg_b) if avg_h is not None and avg_b is not None
                      else None)
                reasons = []
                if n_h < 40:
                    reasons.append("thin")
                if avg_h is not None and avg_h <= 0:
                    reasons.append("hold_sign")
                if vs is not None and vs < BEAT:
                    reasons.append("no_edge_vs_book")
                verd = "THIN" if "thin" in reasons else (
                    "KILL" if reasons else "KEEP")
                out.append({
                    "def": name, "label": lab, "horizon": hz,
                    "heat": hk, "spy": tk,
                    "holdout_n": n_h, "holdout_avg": avg_h,
                    "vs_book_pp": None if vs is None else vs * 100,
                    "verdict": verd, "fail_reasons": reasons,
                    "live_untouched": "flatten_robust",
                })
    return out


def _pct(b):
    if not b or b.get("avg_net") is None:
        return "—"
    return f"{b['avg_net']*100:+.2f}% (n={b['n']})"


def visual_line():
    return (
        "Deep green/red look predictive because the loudest paints are "
        "same-row outcome color: Excel CF paints H>5% `#95CA82` and H>0 "
        "pale `#DCEDD5` (I>3% mint) after the close — that is H/I itself, "
        "not an open forecast; the one open-knowable mid-green (M `#95CA82` "
        "from yesterday's H × IY) is not that paint."
    )


def render(rows, ghost, soft, n_grids, n_days, verd, why, lo, hi, panel, tip):
    def line(r):
        h = r.get("holdout") or {}
        b = r.get("baseline") or {}
        vs_b = "—"
        if h.get("avg_net") is not None and b.get("avg_net") is not None:
            vs_b = f"{(h['avg_net']-b['avg_net'])*100:+.2f} pp"
        vs_p = r.get("vs_parent_pp")
        vs_p_s = "—" if vs_p is None else f"{vs_p:+.2f} pp"
        q1 = r.get("q1") or {}
        why_s = ",".join(r.get("fail_reasons") or []) or "—"
        return (
            f"| {r.get('plain') or r['def']} | {_pct(h)} | {vs_b} | {vs_p_s} | "
            f"{_pct(q1)} | {_pct(r.get('spy_up'))} | {_pct(r.get('spy_dn'))} | "
            f"{(r.get('top5_share') or 0)*100:.0f}% | "
            f"{(r.get('july_share') or 0)*100:.0f}% | **{r.get('keep')}** | "
            f"{why_s} | `{r['def']}` |"
        )

    prim = [r for r in rows if (r.get("label"), r.get("horizon")) in PRIMARY
            and not r["def"].startswith("Hfill_")]
    h1 = [r for r in rows if (r.get("label"), r.get("horizon")) == BASELINE_H
          and not r["def"].startswith("Hfill_")]
    diag = [r for r in rows if r["def"].startswith("Hfill_")]
    n_keep = sum(1 for r in prim if r.get("keep") == "KEEP")
    n_kill = sum(1 for r in prim if r.get("keep") == "KILL")
    n_thin = sum(1 for r in prim if r.get("keep") == "THIN")
    p = panel or {}
    L = [
        "# Shade hex — 2d / 3d cumulative stacked I",
        "",
        f"_Generated {date.today().isoformat()} · live `flatten_robust` "
        "frozen · Yahoo/rows A–F seed only · no cards · no live push._",
        "",
        "## Plain English",
        "",
        "Same-day H already **DEMOTEd** open shade (M mid `#95CA82`) on the "
        "expanded universe. This beat re-cuts the **same open-only fills** "
        "against **2d and 3d cumulative** labels — not a new letter search.",
        "",
        f"**Family verdict: {verd}** — KEEP {n_keep} · KILL {n_kill} · "
        f"THIN {n_thin} on I_sum 2d/3d (H baseline excluded from the count).",
        "",
        why,
        "",
        f"**Cyrus (visual):** {visual_line()}",
        "",
        "### Labels (clock-clean, documented)",
        "",
        "Same definitions as `mine_hi_horizon.labels_for` / `HI_HORIZON.md`:",
        "",
        "- **2d cumulative** = `I_sum` horizon 2 = "
        "`(1+I[t])×(1+I[t+1]) − 1`",
        "- **3d cumulative** = `I_sum` horizon 3 = "
        "`(1+I[t])×(1+I[t+1])×(1+I[t+2]) − 1`",
        "- **Same-day H** = Excel H (close vs open) — **baseline compare only**",
        "- Same-row H/I are **labels**, never features. Lags from rows above "
        "are fair. Miner asserts I_sum against `labels_for` per ticker.",
        "",
        "### Open-only gate (standing bar)",
        "",
        "Source: `OPEN_SAME_ROW_LABELS.md` + `CLOCK_MAP.md` + `clock.py`. "
        "`excel_clock_gate.py` is not on this branch; the miner asserts "
        "`clock_map.json` `fill_mine_open` = A B C G J K L M O IR IS IT.",
        "",
        "- Features at the open: only those 12 fills. This expand paints **M** "
        "(CF `IZ=1` from *prior-row* H and static IY). Known at 9:30.",
        "- **Never** M's number `-(low-open)/open`. Close fills D E F H I N "
        "do not start a trade.",
        "- Onset uses yesterday's M fill (lag). Soft-regime heat is the "
        "prior-5 mean of I (`[ei-5, ei)`). SPY tape is a ship-bar slice.",
        "- `FILL_IDX` maps through `VISIBLE` (M=12). The 4.4k / +10.6% KEEP "
        "had `enumerate(OPEN_FILL)` so M read **H's fill** (H>5%). Aborted.",
        "",
        f"Panel **{p.get('n_tickers_with_rows') or n_grids}** tickers · "
        f"**{p.get('date_min') or '?'} → {p.get('date_max') or '?'}** · "
        f"**{p.get('name_days') or 0:,}** name-days · calendar days "
        f"**{n_days}**. Futubull 0.15% long off the recipe and the "
        "buy-everyone book. Beat book **and** any-green parent by ≥20 bp. "
        "Ghost: top-5 / drop-5 / July / day lottery / Q1. Both SPY tapes. "
        f"Heat terciles cold ≤ {lo*100:.2f}%, hot ≥ {hi*100:.2f}%. "
        f"Tip `{tip}`.",
        "",
        "A / G / K / L multi-shade and O mint are **not painted** on this "
        "expand (same M-only panel as #150/#151). IR/IS/IT have no CF.",
        "",
        "### 2d stacked I (primary)",
        "",
        "| meaning | holdout | vs book | vs parent | Q1 | SPY↑ | SPY↓ | "
        "top-5 | July | verdict | why | code |",
        "|---|---|---|---|---|---|---|---|---|---|---|---|",
    ]
    show2 = [r for r in prim if r.get("horizon") == 2]
    show2 = sorted(show2, key=lambda r: (
        0 if r.get("keep") == "KEEP" else 1,
        0 if r["def"] == "M_green" else 1,
        -((r.get("holdout") or {}).get("avg_net") or -9),
    ))
    for r in show2:
        L.append(line(r))
    L += [
        "",
        "### 3d stacked I (primary)",
        "",
        "| meaning | holdout | vs book | vs parent | Q1 | SPY↑ | SPY↓ | "
        "top-5 | July | verdict | why | code |",
        "|---|---|---|---|---|---|---|---|---|---|---|---|",
    ]
    show3 = [r for r in prim if r.get("horizon") == 3]
    show3 = sorted(show3, key=lambda r: (
        0 if r.get("keep") == "KEEP" else 1,
        0 if r["def"] == "M_green" else 1,
        -((r.get("holdout") or {}).get("avg_net") or -9),
    ))
    for r in show3:
        L.append(line(r))
    L += [
        "",
        "### Same-day H (baseline compare only)",
        "",
        "Not the search. Standing 1d H DEMOTE from #150 is the compare.",
        "",
        "| meaning | holdout | vs book | vs parent | Q1 | SPY↑ | SPY↓ | "
        "top-5 | July | verdict | why | code |",
        "|---|---|---|---|---|---|---|---|---|---|---|---|",
    ]
    for r in sorted(h1, key=lambda r: (
        0 if r["def"] == "M_green" else 1,
        -((r.get("holdout") or {}).get("avg_net") or -9),
    )):
        L.append(line(r))
    L += [
        "",
        "### H-fill diagnostic (close-knowable — not a KEEP path)",
        "",
        "Excel paints column H from **today's H number**. Using that fill as "
        "if it were an open shade is the 4.4k leak. These rows are forced "
        "`hfill_outcome_paint` / clock=close.",
        "",
        "| meaning | label | holdout | vs book | verdict | why | code |",
        "|---|---|---|---|---|---|---|",
    ]
    for r in sorted(diag, key=lambda r: (r.get("horizon") or 0, r["def"])):
        h = r.get("holdout") or {}
        b = r.get("baseline") or {}
        vs_b = "—"
        if h.get("avg_net") is not None and b.get("avg_net") is not None:
            vs_b = f"{(h['avg_net']-b['avg_net'])*100:+.2f} pp"
        L.append(
            f"| {r.get('plain')} | {r.get('label')}@{r.get('horizon')}d | "
            f"{_pct(h)} | {vs_b} | **KILL** | "
            f"{','.join(r.get('fail_reasons') or [])} | `{r['def']}` |"
        )
    if ghost:
        L += [
            "",
            "### Ghost / name check (2d and 3d)",
            "",
            "Holdout top-5 / drop-5 leftover vs book and any-green M / July / "
            "day lottery / Q1 / both SPY tapes. Futubull 0.15% on every print.",
            "",
            "| recipe | label | holdout | vs book | vs parent | drop-5 | "
            "top-5 | July | day | Q1 | ghost |",
            "|---|---|---|---|---|---|---|---|---|---|---|",
        ]
        for s in ghost:
            if s["def"] not in FOCUS and s["def"] != HFILL_GT05:
                continue
            if s["def"] == "M_green":
                continue
            L.append(
                f"| `{s['def']}` | {s.get('label')}@{s.get('horizon')}d | "
                f"{_pct(s.get('holdout'))} | "
                f"{(s.get('vs_book_pp') if s.get('vs_book_pp') is not None else 0):+.2f} pp | "
                f"{(s.get('vs_parent_pp') if s.get('vs_parent_pp') is not None else 0):+.2f} pp | "
                f"{_pct(s.get('drop5_holdout'))} | "
                f"{(s.get('top5_holdout_share') or 0)*100:.1f}% "
                f"({', '.join(s.get('top5_tickers') or [])}) | "
                f"{(s.get('july_holdout_share') or 0)*100:.1f}% | "
                f"{(s.get('day_lottery_holdout') or 0)*100:.1f}% | "
                f"{_pct(s.get('q1_holdout'))} | **{s.get('ghost')}** |"
            )
    if soft:
        L += [
            "",
            "### Soft-regime (KEEP I_sum candidates only)",
            "",
            "| recipe | label | heat | SPY | holdout | vs book | verdict | why |",
            "|---|---|---|---|---|---|---|---|",
        ]
        for c in soft:
            hs = ("—" if c.get("holdout_avg") is None
                  else f"{c['holdout_avg']*100:+.2f}% (n={c['holdout_n']})")
            vs = ("—" if c.get("vs_book_pp") is None
                  else f"{c['vs_book_pp']:+.2f} pp")
            L.append(
                f"| `{c['def']}` | {c['label']}@{c['horizon']}d | {c['heat']} | "
                f"{c['spy']} | {hs} | {vs} | **{c['verdict']}** | "
                f"{','.join(c.get('fail_reasons') or []) or '—'} |"
            )
    L += [
        "",
        "### What this does not change",
        "",
        "- Live `flatten_robust` is frozen. No card. No push.",
        "- Same-day H shade DEMOTE from #150 stays demoted.",
        "- Standing five-cell light+green O ± AH/FR is not remine.",
        "- Close-entry fills stay out of the open clock.",
        "- H-fill `#95CA82` is outcome paint — not an open shade KEEP.",
        "",
        "Research only. Expanded panel is the #150/#151 Yahoo tape. "
        "Live frozen.",
        "",
    ]
    return "\n".join(L)


def render_cards(rows, verd, tip, panel):
    prim = [r for r in rows if (r.get("label"), r.get("horizon")) in PRIMARY
            and r["def"] in ("M_ge15", "M_hex_95CA82", "M_onset_hex_95CA82",
                             "M_green")
            and not r["def"].startswith("Hfill_")]
    p = panel or {}
    L = [
        "",
        "## 2d / 3d stacked I recut (follow-on)",
        "",
        f"_Generated {date.today().isoformat()} · tip `{tip}` · "
        "**research cards only** · live `flatten_robust` frozen · "
        f"family **{verd}**._",
        "",
        "Primary labels are HI_HORIZON `I_sum` 2d and 3d (compound daily I). "
        "Same-day H is baseline only. Same expand panel as #150/#151 "
        f"({p.get('n_tickers_with_rows') or '?'} names, "
        f"{p.get('date_min') or '?'} → {p.get('date_max') or '?'}).",
        "",
        f"**Cyrus (visual):** {visual_line()}",
        "",
        "| field | value |",
        "|---|---|",
        f"| family | **{verd}** |",
        f"| panel | {p.get('n_tickers_with_rows')} names / "
        f"{p.get('name_days') or 0:,} name-days |",
        f"| live | frozen (`flatten_robust`) |",
        "",
        "| recipe | label | holdout | vs book | vs parent | verdict |",
        "|---|---|---|---|---|---|",
    ]
    for r in sorted(prim, key=lambda x: (x.get("horizon") or 0, x["def"])):
        h = r.get("holdout") or {}
        b = r.get("baseline") or {}
        vs_b = "—"
        if h.get("avg_net") is not None and b.get("avg_net") is not None:
            vs_b = f"{(h['avg_net']-b['avg_net'])*100:+.2f} pp"
        vs_p = r.get("vs_parent_pp")
        vs_p_s = "—" if vs_p is None else f"{vs_p:+.2f} pp"
        L.append(
            f"| `{r['def']}` | {r['label']}@{r['horizon']}d | {_pct(h)} | "
            f"{vs_b} | {vs_p_s} | **{r.get('keep')}** |"
        )
    L += [
        "",
        "Status: retired research · not live. Must beat any-green parent "
        "on the same letter, not just the book.",
        "",
    ]
    return "\n".join(L)


def splice_cards(block):
    old = open(CARDS, encoding="utf-8").read()
    marker = "## 2d / 3d stacked I recut (follow-on)"
    if marker in old:
        head = old.split(marker, 1)[0].rstrip() + "\n"
    else:
        head = old.rstrip() + "\n"
    return head + block


def _write(path, text):
    """Write only after content is built — never truncate-then-splice."""
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    tmp = path + ".tmp"
    open(tmp, "w", encoding="utf-8").write(text)
    os.replace(tmp, path)


def splice_scoreboard(verd, n_keep, n_kill, tip):
    block = (
        f"{MARKER}\n\n"
        f"_Generated {date.today().isoformat()} · live `flatten_robust` "
        f"frozen. Open shade → 2d/3d stacked I (HI_HORIZON `I_sum`). "
        f"Family **{verd}**. KEEP {n_keep} · KILL {n_kill}. "
        f"See `SHADE_2D3D.md`. Tip `{tip}`. Not live._\n"
    )
    if os.path.exists(SB_MD) and os.path.getsize(SB_MD) > 100:
        try:
            return splice_md(SB_MD, MARKER, block, require="PASS 376")
        except ValueError:
            old = open(SB_MD, encoding="utf-8").read().rstrip() + "\n\n"
            return old + block
    return block


def splice_cycle(verd, n_keep, n_kill):
    block = (
        f"{CYCLE_MARKER}\n\n"
        f"Open shade → 2d/3d stacked I: family **{verd}**. "
        f"KEEP {n_keep} · KILL {n_kill}. See `SHADE_2D3D.md`.\n"
    )
    if os.path.exists(CYCLE_MD):
        return splice_md(CYCLE_MD, CYCLE_MARKER, block, require="Excel emulator")
    return block


def jsonable(rows):
    out = []
    for r in rows:
        rec = {}
        for k, v in r.items():
            if k in ("tickers", "dates"):
                continue
            rec[k] = sorted(v) if isinstance(v, set) else v
        out.append(rec)
    return out


def git_tip():
    try:
        import subprocess
        return subprocess.check_output(
            ["git", "rev-parse", "--short=8", "HEAD"],
            cwd=REPO, text=True).strip()
    except Exception:
        return "uncommitted"


def rows_ready(ticker, start, end):
    rows = load_ticker_rows(ticker)
    if len(rows) < 40:
        return False
    if rows[0]["date"] > start + timedelta(days=60):
        return False
    if rows[-1]["date"] < end - timedelta(days=14):
        return False
    return True


def build_panel(workers=4, max_tickers=0, yahoo_years=8):
    split = json.load(open(SPLIT_PATH))
    want = sorted(set(split["discovery"]) | set(split["holdout"]) | {"SPY"})
    if max_tickers:
        want = [t for t in want if t != "SPY"][:max_tickers] + ["SPY"]
    end = date.today()
    start = end - timedelta(days=365 * yahoo_years)
    start = min(start, PANEL_START)
    os.makedirs(ROWS, exist_ok=True)
    jobs = [(t, start, end) for t in want if not rows_ready(t, start, end)]
    print(f"yahoo need {len(jobs)} / {len(want)}  {start}→{end}", flush=True)
    ok = err = 0
    if jobs:
        with Pool(workers) as pool:
            for i, (t, rows, e) in enumerate(
                    pool.imap_unordered(yahoo_one, jobs, chunksize=4), 1):
                if e or len(rows) < 20:
                    err += 1
                else:
                    write_rows(t, merge_rows(load_ticker_rows(t), rows))
                    ok += 1
                if i % 100 == 0:
                    print(f"  yahoo … {i}/{len(jobs)} ok={ok} err={err}",
                          flush=True)
    print(f"  yahoo done ok={ok} err={err}", flush=True)
    spy = load_ticker_rows("SPY")
    if len(spy) >= 40:
        n, a, b = extend_spy(spy)
        print(f"spy tape {n} {a} → {b}", flush=True)
    have = sorted(t for t in want if t != "SPY" and len(load_ticker_rows(t)) >= 40)
    stats = panel_stats(have)
    stats.update({
        "generated": date.today().isoformat(),
        "live_untouched": "flatten_robust",
        "n_grids_painted": 0,
        "n_grid_days": 0,
        "m_cf": "IZ=1 → #95CA82 (prior-row H>0 and IY=1)",
        "index_fix": "FILL_IDX uses VISIBLE (M=12), not OPEN_FILL order",
        "label_def": "HI_HORIZON I_sum 2d/3d",
        "yahoo_start": start.isoformat(),
        "yahoo_end": end.isoformat(),
    })
    json.dump(stats, open(STATS, "w"), indent=2)
    print(json.dumps(stats, indent=2), flush=True)
    return stats


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--workers", type=int, default=min(4, os.cpu_count() or 2))
    ap.add_argument("--build-panel", action="store_true")
    ap.add_argument("--render-only", action="store_true")
    ap.add_argument("--max-tickers", type=int, default=0)
    ap.add_argument("--skip-build", action="store_true")
    args = ap.parse_args()
    assert_clock_map()
    print("clock gate OK — fill_mine_open A B C G J K L M O IR IS IT",
          flush=True)
    built = False
    if args.build_panel:
        build_panel(workers=args.workers, max_tickers=args.max_tickers)
        built = True
        if args.skip_build:
            return
    if args.render_only and os.path.exists(OUT_JSON):
        payload = json.load(open(OUT_JSON))
        rows = payload.get("rows") or []
        verd, why, keep, kill, thin = family_verdict(rows)
        tip = payload.get("tip") or git_tip()
        md = render(
            rows, payload.get("ghost") or [], payload.get("soft_regime") or [],
            payload.get("n_grids") or 0, payload.get("n_days") or 0,
            verd, why, payload.get("heat_lo") or -0.0045,
            payload.get("heat_hi") or 0.0048,
            payload.get("panel") or {}, tip,
        )
        _write(OUT_MD, md)
        _write(CARDS, splice_cards(
            render_cards(rows, verd, tip, payload.get("panel") or {})))
        _write(SB_MD, splice_scoreboard(verd, len(keep), len(kill), tip))
        if os.path.exists(CYCLE_MD):
            _write(CYCLE_MD, splice_cycle(verd, len(keep), len(kill)))
        print(f"render-only VERDICT {verd} KEEP={len(keep)} KILL={len(kill)}")
        print(OUT_MD)
        return
    if not args.skip_build and not args.render_only and not built:
        have = [fn[:-5] for fn in os.listdir(ROWS) if fn.endswith(".json")] if os.path.isdir(ROWS) else []
        if len(have) < 100:
            print("rows thin — building panel first", flush=True)
            build_panel(workers=args.workers, max_tickers=args.max_tickers)
    split = json.load(open(SPLIT_PATH))
    discovery, holdout = set(split["discovery"]), set(split["holdout"])
    spy = load_spy_regimes()
    iy = load_iy()
    tickers = sorted(
        t for t in (discovery | holdout)
        if len(load_ticker_rows(t)) >= 40
    )
    if args.max_tickers:
        tickers = tickers[: args.max_tickers]
    print(f"mine {len(tickers)} tickers workers={args.workers}", flush=True)
    hits, book, heats, dumps, n_ok = collect(
        tickers, iy, discovery, holdout, spy, workers=args.workers)
    baselines = {}
    by_lab = defaultdict(list)
    for _t, _iso, split_s, lab, hz, net, _heat, _tape in book:
        if split_s == "holdout":
            by_lab[(lab, hz)].append(net)
    for k, vals in by_lab.items():
        if len(vals) >= 2:
            baselines[f"{k[0]}_{k[1]}"] = {
                "n": len(vals), "avg_net": sum(vals) / len(vals), "t": 0.0,
            }
    rows, lo, hi, _bc = pack_all(hits, book, heats, baselines)
    verd, why, keep, kill, thin = family_verdict(rows)
    soft = soft_regime(rows, hits, book, lo, hi)
    ghost = run_ghost(hits, book, list(PRIMARY) + [BASELINE_H])
    n_days = len({b[1] for b in book})
    panel = json.load(open(STATS)) if os.path.exists(STATS) else panel_stats(tickers)
    tip = git_tip()
    md = render(rows, ghost, soft, n_ok, n_days, verd, why, lo, hi, panel, tip)
    _write(OUT_MD, md)
    payload = {
        "generated": str(date.today()),
        "live_untouched": "flatten_robust",
        "entry": "open",
        "cost_model": "futubull",
        "label_def": "HI_HORIZON I_sum = compound daily I; H baseline only",
        "family_verdict": verd,
        "why": why,
        "visual": visual_line(),
        "n_grids": n_ok,
        "n_hits": len(hits),
        "n_book": len(book),
        "n_days": n_days,
        "n_keep": len(keep),
        "n_kill": len(kill),
        "n_thin": len(thin),
        "heat_lo": lo, "heat_hi": hi,
        "open_fills": "".join(OPEN_FILL),
        "open_aliases": list(OPEN_ALIASES),
        "close_fills_excluded": "".join(CLOSE_FILL),
        "m_number_excluded": True,
        "hfill_diagnostic_not_keep": True,
        "tip": tip,
        "panel": panel,
        "rows": jsonable(rows),
        "ghost": ghost,
        "soft_regime": soft,
        "dump": dump_inventory(dumps),
    }
    json.dump(payload, open(OUT_JSON, "w"), indent=2)
    _write(CARDS, splice_cards(render_cards(rows, verd, tip, panel)))
    _write(SB_MD, splice_scoreboard(verd, len(keep), len(kill), tip))
    if os.path.exists(CYCLE_MD):
        _write(CYCLE_MD, splice_cycle(verd, len(keep), len(kill)))
    print(f"VERDICT {verd} KEEP={len(keep)} KILL={len(kill)} "
          f"THIN={len(thin)} grids={n_ok}")
    print(visual_line())
    print(OUT_MD)


if __name__ == "__main__":
    main()
# flatten_robust labeled untouched — this miner does not import or write live.
