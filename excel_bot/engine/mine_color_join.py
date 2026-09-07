"""Color → forward-gain + PIT joins on stored A–O grids.

Colors are first-class signals. Open entry only on open-knowable fills
(A,B,C,G,J,K,L,M,O). Close-knowable fills (D,E,F,H,I,N) never enter open.

Then join the 6 KEEP open-hysteresis lights to prior-day AB / weather /
overnight book (last tape dated before the entry date) and Finviz
snapshot cohorts. A combo that beats the parent light is a win.

Futubull fees kept. Walk-forward + both-tape + top-day lottery.
Does not emit live cards. Does not import flatten_robust.

  python engine/mine_color_join.py --workers 4
"""
from __future__ import annotations

import argparse
import glob
import json
import os
import sys
from collections import defaultdict

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from clock import (  # noqa: E402
    CLOSE_LETTERS, COST_FUTU_LONG, COST_FUTU_SHORT, OPEN_LETTERS, SHIP,
    VISIBLE, annotate_days, simulate_clock,
)
from cohort_analysis import cohorts_of, load_finviz  # noqa: E402
from harden_hyst_open import (  # noqa: E402
    CANDIDATE_KEYS, CANDIDATE_NAMES, HALF_CUT, apply_hold1_keep, blk,
    candidate_pats, fmt_blk, load_baselines, lottery_day, s2d, splice_md,
)
from mine_clock import lottery as lottery_trade  # noqa: E402
from mine_first import load_spy  # noqa: E402
from patterns import detect_pattern  # noqa: E402
from pit_joins import (  # noqa: E402
    coverage, load_ab_tones, load_book_buys, load_weather_risk, prior_lookup,
)

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
REPO = os.path.dirname(ROOT)
GRIDS = os.environ.get("GRIDS_DIR", os.path.join(ROOT, "grids"))
SPLIT_PATH = os.path.join(HERE, "holdout_split.json")
RESEARCH = os.path.join(ROOT, "research")
SCOREBOARD = os.path.join(REPO, "03_scoreboard")
HOLDS = (1, 2)
JOIN_COHORTS = (
    "opt:Yes", "volM:high(>8%)", "volM:low(<3%)",
    "mid(1-10B):all", "mcap:large(>10B)",
)
OPEN_IDX = {c: VISIBLE.index(c) for c in OPEN_LETTERS}
PARENT_JSON = os.path.join(RESEARCH, "hyst_open_harden.json")

_G = {}


def color_plain(letter, fam, hold):
    when = ("same day's close" if hold == 1 else "next day's close")
    side = "buy" if fam == "green" else "short"
    return (
        f"Morning cell {letter} is highlighted {fam} (known at 9:30). "
        f"{side.capitalize()} at that open and sell at the {when}."
    )


def fold_plain(parent, extra):
    return f"Same morning light as `{parent}`, and {extra}."


def pack(name, hold, side, trades, baseline, parent=None, family="color",
         plain=""):
    disc = [tr["net"] for tr in trades if tr["split"] == "discovery"]
    hout = [tr["net"] for tr in trades if tr["split"] == "holdout"]
    early = [tr["net"] for tr in trades if tr["half"] == "early"]
    late = [tr["net"] for tr in trades if tr["half"] == "late"]
    up = [tr["net"] for tr in trades if tr["tape"] == 1]
    dn = [tr["net"] for tr in trades if tr["tape"] == -1]
    d, h = blk(disc), blk(hout)
    e, l = blk(early), blk(late)
    su, sd = blk(up), blk(dn)
    lot_bad, lot_frac, trimmed = lottery_trade(disc if disc else hout)
    day_bad, day_frac, top_day, top_pnl, n_days = lottery_day(
        [tr for tr in trades if tr["split"] == "discovery"] or trades)
    tickers = {tr["ticker"] for tr in trades}
    dates = {tr["date"] for tr in trades}
    reasons = []
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
    elif not e or not l:
        reasons.append("tape_split")
    if su and sd and su["n"] >= SHIP["min_tape_n"] and sd["n"] >= SHIP["min_tape_n"]:
        if su["avg_net"] <= 0 or sd["avg_net"] <= 0:
            reasons.append("spy_regime")
    elif not su or not sd:
        reasons.append("spy_regime")
    cmp = h if h else d
    if baseline and cmp and cmp["avg_net"] < baseline["avg_net"] + 0.002:
        reasons.append("no_edge_vs_uncond")
    if parent and cmp and cmp["avg_net"] < parent["avg_net"] + 0.002:
        reasons.append("no_edge_vs_parent")
    SIZE = {"thin_disc", "thin_hold", "ticker_bar", "date_bar"}
    quality = set(reasons) - SIZE
    if not reasons:
        verdict = "KEEP"
    elif quality:
        verdict = "KILL"
    else:
        verdict = "THIN"
    return {
        "def": name, "clock": "open", "side": side, "exit": f"hold{hold}",
        "hold": hold, "family": family, "cost_model": "futubull",
        "plain": plain, "n_tickers": len(tickers), "n_dates": len(dates),
        "discovery": d, "holdout": h, "early": e, "late": l,
        "spy_up": su, "spy_dn": sd, "baseline": baseline, "parent": parent,
        "lottery_day_frac": day_frac, "lottery_top_day": top_day,
        "lottery_trade_frac": lot_frac, "trimmed_avg": trimmed,
        "verdict": verdict, "fail_reasons": reasons,
        "live_untouched": "flatten_robust",
    }


def _init(discovery, holdout, pats, spy, joins, fz_co):
    _G.update(discovery=discovery, holdout=holdout, pats=pats, spy=spy,
              joins=joins, fz_co=fz_co)


def _emit(trades, name, hold, side, raw, t, iso, split, spy, tape_joins=None):
    cost = COST_FUTU_LONG if side == 1 else COST_FUTU_SHORT
    rec = {
        "def": name, "hold": hold, "side": "long" if side == 1 else "short",
        "ticker": t, "date": iso, "split": split,
        "half": "early" if iso < HALF_CUT else "late",
        "tape": spy.get(iso, 0), "raw": raw, "net": raw - cost,
    }
    if tape_joins:
        rec.update(tape_joins)
    trades.append(rec)


def work_grid(path):
    t = os.path.basename(path)[:-5].upper()
    if t.startswith("_"):
        return None
    split = ("discovery" if t in _G["discovery"]
             else "holdout" if t in _G["holdout"] else None)
    if split is None:
        return None
    try:
        days = annotate_days(json.load(open(path))["days"])
    except Exception:
        return None
    if len(days) < 30:
        return None
    spy, last = _G["spy"], len(days) - 1
    ab_map, book_map, wx_map = _G["joins"]
    fz_co = _G["fz_co"].get(t) or set()
    trades = []
    for ei, day in enumerate(days):
        iso = str(s2d(day["date"]))
        fams = day.get("fams") or []
        for letter, idx in OPEN_IDX.items():
            if idx >= len(fams):
                continue
            fam = fams[idx]
            if fam not in ("green", "red"):
                continue
            side = 1 if fam == "green" else -1
            dummy = {"side": side, "entry_idx": ei, "exit_idx": last}
            for hold in HOLDS:
                raw = simulate_clock(days, dummy, "open", f"hold{hold}")
                if raw is None:
                    continue
                _emit(trades, f"color_{letter}_{fam}", hold, side, raw,
                      t, iso, split, spy)
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
            ab_d, ab_t = prior_lookup(ab_map, iso, t)
            _bk_d, in_buy = prior_lookup(book_map, iso, t)
            wx_d, wx_risk = prior_lookup(wx_map, iso)
            joins = {
                "ab": ab_t, "book_buy": bool(in_buy),
                "wx": wx_risk, "ab_asof": ab_d, "wx_asof": wx_d,
            }
            for hold in HOLDS:
                raw = simulate_clock(days, c, "open", f"hold{hold}")
                if raw is None:
                    continue
                for letter, idx in OPEN_IDX.items():
                    if idx < len(fams) and fams[idx] == "green":
                        _emit(trades, f"{pat['name']}__{letter}_green",
                              hold, 1, raw, t, iso, split, spy)
                if ab_t == "good":
                    _emit(trades, f"{pat['name']}__ab_good", hold, 1,
                          raw, t, iso, split, spy, joins)
                if in_buy:
                    _emit(trades, f"{pat['name']}__book_1d_buy", hold, 1,
                          raw, t, iso, split, spy, joins)
                if wx_risk == "off":
                    _emit(trades, f"{pat['name']}__wx_risk_off", hold, 1,
                          raw, t, iso, split, spy, joins)
                if wx_risk == "on":
                    _emit(trades, f"{pat['name']}__wx_risk_on", hold, 1,
                          raw, t, iso, split, spy, joins)
                for co in fz_co:
                    if co in JOIN_COHORTS:
                        slug = co.split(":")[0]
                        _emit(trades, f"{pat['name']}__fz_{slug}", hold, 1,
                              raw, t, iso, split, spy)
    return trades


def collect(files, pats, spy, disc, hold, joins, fz_co, workers=4):
    trades, seen = [], 0
    if workers > 1 and files:
        from multiprocessing import Pool
        with Pool(workers, initializer=_init,
                  initargs=(disc, hold, pats, spy, joins, fz_co)) as pool:
            for i, rec in enumerate(pool.imap_unordered(work_grid, files,
                                                        chunksize=16), 1):
                if rec:
                    seen += 1
                    trades.extend(rec)
                if i % 400 == 0:
                    print(f"  ... {i}/{len(files)} trades={len(trades)}",
                          flush=True)
    else:
        _init(disc, hold, pats, spy, joins, fz_co)
        for i, f in enumerate(files, 1):
            rec = work_grid(f)
            if rec:
                seen += 1
                trades.extend(rec)
    return trades, seen


def load_parents():
    if not os.path.exists(PARENT_JSON):
        return {}
    raw = json.load(open(PARENT_JSON))
    out = {}
    for r in raw.get("candidates") or []:
        out[(r["def"], r["exit"])] = r.get("holdout")
    return out


def parent_of(name, hold=None):
    if "__" not in name:
        return None, None
    pname = name.split("__", 1)[0]
    if pname not in CANDIDATE_NAMES:
        return None, None
    if hold is None:
        return pname, None
    return pname, f"hold{hold}"


def score_all(trades, baselines, parents):
    buckets = defaultdict(list)
    for tr in trades:
        buckets[(tr["def"], tr["hold"], tr["side"])].append(tr)
    rows = []
    for (name, hold, side), recs in buckets.items():
        base = baselines.get(f"open_{side}_h{hold}")
        parent = None
        family = "color"
        plain = name
        if name.startswith("color_"):
            parts = name.split("_")
            letter, fam = parts[1], parts[2]
            family = "color"
            plain = color_plain(letter, fam, hold)
        elif name.endswith(tuple(f"__{L}_green" for L in OPEN_LETTERS)):
            family = "hyst_color"
            letter = name.rsplit("__", 1)[1].split("_")[0]
            plain = fold_plain(name.split("__")[0],
                               f"morning cell {letter} is also green")
            pname, prule = parent_of(name, hold)
            parent = parents.get((pname, prule)) if pname else None
        elif "__" in name:
            family = "join"
            if name.endswith("__ab_good"):
                plain = fold_plain(name.split("__")[0],
                                   "yesterday's AB tape already liked the name")
            elif name.endswith("__book_1d_buy"):
                plain = fold_plain(name.split("__")[0],
                                   "the overnight 1-day book had it as a buy")
            elif name.endswith("__wx_risk_off"):
                plain = fold_plain(name.split("__")[0],
                                   "prior weather was risk-off")
            elif name.endswith("__wx_risk_on"):
                plain = fold_plain(name.split("__")[0],
                                   "prior weather was risk-on")
            elif "__fz_" in name:
                slug = name.rsplit("__fz_", 1)[1]
                plain = fold_plain(name.split("__")[0],
                                   f"Finviz snapshot cohort {slug} "
                                   "(not a historical as-of)")
            pname, prule = parent_of(name, hold)
            parent = parents.get((pname, prule)) if pname else None
        rows.append(pack(name, hold, side, recs, base, parent, family, plain))
    apply_hold1_keep(rows)
    rank = {"KEEP": 0, "THIN": 1, "KILL": 2}
    rows.sort(key=lambda r: (
        rank.get(r["verdict"], 9),
        0 if r["family"] == "hyst_color" else 1 if r["family"] == "join" else 2,
        -((r["holdout"] or {}).get("t") or -9),
    ))
    return rows


def _pct(b):
    if not b:
        return "—"
    return f"{b['avg_net']*100:+.2f}% (n={b['n']})"


def render(rows, n_grids, cov):
    keep = [r for r in rows if r["verdict"] == "KEEP"]
    thin = [r for r in rows if r["verdict"] == "THIN"]
    kill = [r for r in rows if r["verdict"] == "KILL"]
    colors = [r for r in rows if r["family"] == "color"]
    folds = [r for r in rows if r["family"] == "hyst_color"]
    joins = [r for r in rows if r["family"] == "join"]
    L = [
        "## Color + join mine (open-knowable fills, PIT joins)",
        "",
        "_Generated today. Colors are signals, not decoration. "
        "Futubull fees kept. Live `flatten_robust` frozen. No cards._",
        "",
        "### What a color means",
        "",
        "The sheet paints a cell green or red. That paint is the signal. "
        "At 9:30 we can already see columns **A, B, C, G, J, K, L, M, O**. "
        "Columns D, E, F, H, I, N wait until the close — they never start "
        "an open trade. Green = buy at the open. Red = short at the open. "
        "Sell after 1 session (same-day close) or 2 sessions (next close).",
        "",
        "Everyone-else baseline, same clock and fees: next-1 **−0.07%**, "
        "next-2 **+0.41%**.",
        "",
        "### Joins (yesterday's tape only)",
        "",
        "AB / weather / overnight book use the last file **dated before** "
        "the entry date. Same-day files are not knowable at 9:30. "
        f"AB coverage {cov['ab']['n_dates']} days "
        f"({cov['ab']['first']}–{cov['ab']['last']}); "
        f"book {cov['book']['n_dates']} days; "
        f"weather {cov['wx']['n_dates']} days. "
        "Finviz cohorts are the **current snapshot**, not 2026 history.",
        "",
        f"Grids **{n_grids}**. Cells scored **{len(rows)}**. "
        f"**KEEP {len(keep)}** · **THIN {len(thin)}** · **KILL {len(kill)}**.",
        "",
        "**Color alone does not clear the bar.** One morning cell being "
        "green is not enough — the late half of 2026 goes red on same-day "
        "holds. The already-proved light **plus a green O** adds about "
        "+20–45 bp over the light itself. High-vol Finviz names add "
        "+24–64 bp, but that tag is today's snapshot, not 2026 history. "
        "AB / weather / overnight book only exist for late Aug–Sep, so "
        "they fail walk-forward (no first half).",
        "",
        "### Color → next-N-days (open letters only)",
        "",
        "| meaning | hold | side | holdout after fees | vs everyone | "
        "first half | second half | fattest day | verdict | code |",
        "|---|---|---|---|---|---|---|---|---|---|",
    ]
    show = [r for r in colors if r["exit"] in ("hold1", "hold2")]
    show.sort(key=lambda r: (0 if r["verdict"] == "KEEP" else 1,
                             -((r["holdout"] or {}).get("avg_net") or -9)))
    for r in show[:24]:
        b = r.get("baseline") or {}
        vs = "—"
        if r.get("holdout") and b:
            vs = f"{(r['holdout']['avg_net']-b['avg_net'])*100:+.2f} pp"
        L.append(
            f"| {r['plain']} | next {r['hold']} | {r['side']} | "
            f"{_pct(r.get('holdout'))} | {vs} | {_pct(r.get('early'))} | "
            f"{_pct(r.get('late'))} | {r['lottery_day_frac']*100:.1f}% | "
            f"**{r['verdict']}** | `{r['def']}` |"
        )
    L += [
        "",
        "### Light + a green cell (fold into the 6 KEEP hysteresis names)",
        "",
        "| meaning | hold | holdout | vs parent light | verdict | code |",
        "|---|---|---|---|---|---|",
    ]
    folds_show = sorted(folds, key=lambda r: (
        0 if r["verdict"] == "KEEP" else 1,
        -((r["holdout"] or {}).get("avg_net") or -9)))
    for r in folds_show[:18]:
        vs = "—"
        if r.get("holdout") and r.get("parent"):
            vs = f"{(r['holdout']['avg_net']-r['parent']['avg_net'])*100:+.2f} pp"
        L.append(
            f"| {r['plain']} | next {r['hold']} | {_pct(r.get('holdout'))} | "
            f"{vs} | **{r['verdict']}** | `{r['def']}` |"
        )
    L += [
        "",
        "### Light + AB / weather / book / Finviz (useful combo = win)",
        "",
        "| meaning | hold | holdout | vs parent | dates | verdict | why | code |",
        "|---|---|---|---|---:|---|---|---|",
    ]
    joins_show = sorted(joins, key=lambda r: (
        0 if r["verdict"] == "KEEP" else 1 if r["verdict"] == "THIN" else 2,
        -((r["holdout"] or {}).get("avg_net") or -9)))
    for r in joins_show[:24]:
        vs = "—"
        if r.get("holdout") and r.get("parent"):
            vs = f"{(r['holdout']['avg_net']-r['parent']['avg_net'])*100:+.2f} pp"
        L.append(
            f"| {r['plain']} | next {r['hold']} | {_pct(r.get('holdout'))} | "
            f"{vs} | {r['n_dates']} | **{r['verdict']}** | "
            f"{','.join(r.get('fail_reasons') or []) or '—'} | `{r['def']}` |"
        )
    L += [
        "",
        "KEEP still research-only. Join tapes only exist for late Aug–Sep "
        "2026, so most join cells are THIN on dates. Color cells use the "
        "full Jan–Sep window. No live wire.",
        "",
    ]
    return "\n".join(L) + "\n"


def rejudge(rows, parents):
    """Attach the matching-hold parent and re-apply the +20 bp fold bar."""
    size = {"thin_disc", "thin_hold", "ticker_bar", "date_bar"}
    for r in rows:
        pname, prule = parent_of(r["def"], r.get("hold"))
        parent = parents.get((pname, prule)) if pname else None
        r["parent"] = parent
        reasons = [x for x in (r.get("fail_reasons") or [])
                   if x != "no_edge_vs_parent"]
        cmp = r.get("holdout") or r.get("discovery")
        if parent and cmp and cmp.get("avg_net", 0) < parent["avg_net"] + 0.002:
            reasons.append("no_edge_vs_parent")
        r["fail_reasons"] = reasons
        quality = set(reasons) - size
        if not reasons:
            r["verdict"] = "KEEP"
        elif quality:
            r["verdict"] = "KILL"
        else:
            r["verdict"] = "THIN"
    apply_hold1_keep(rows)
    return rows


def slim(r):
    keys = ("def", "clock", "side", "exit", "hold", "family", "plain",
            "cost_model", "n_tickers", "n_dates", "discovery", "holdout",
            "early", "late", "spy_up", "spy_dn", "lottery_day_frac",
            "parent", "verdict", "fail_reasons", "live_untouched")
    return {k: r[k] for k in keys if k in r}


def write_outputs(rows, n_grids, cov):
    from datetime import date
    md = render(rows, n_grids, cov).replace("today", str(date.today()), 1)
    ao = os.path.join(RESEARCH, "AO_FIRST_MINE.md")
    sb = os.path.join(SCOREBOARD, "EXCEL_BOT_MINE.md")
    cycle = os.path.join(RESEARCH, "MINE_CYCLE.md")
    ao_text = splice_md(ao, "## Color + join mine", md,
                        require="VISIBLE_COLS A..O")
    sb_text = splice_md(sb, "## Color + join mine", md,
                        require="first A–JL cut")
    with open(ao, "w", encoding="utf-8") as fh:
        fh.write(ao_text)
    with open(sb, "w", encoding="utf-8") as fh:
        fh.write(sb_text)
    note = (
        "## Color + join mine\n\n"
        f"KEEP {sum(1 for r in rows if r['verdict']=='KEEP')} · "
        f"THIN {sum(1 for r in rows if r['verdict']=='THIN')} · "
        f"KILL {sum(1 for r in rows if r['verdict']=='KILL')}. "
        "Open-knowable fills only. PIT joins (prior AB/weather/book). "
        "See `AO_FIRST_MINE.md`.\n"
    )
    cycle_text = splice_md(cycle, "## Color + join mine", note,
                           require="first A–JL cut")
    with open(cycle, "w", encoding="utf-8") as fh:
        fh.write(cycle_text)
    payload = {
        "generated": str(date.today()),
        "spec": "color→forward-gain + PIT joins; futubull; open fills only",
        "grids": n_grids,
        "n_keep": sum(1 for r in rows if r["verdict"] == "KEEP"),
        "n_thin": sum(1 for r in rows if r["verdict"] == "THIN"),
        "n_kill": sum(1 for r in rows if r["verdict"] == "KILL"),
        "open_letters": OPEN_LETTERS,
        "close_letters_never_open": CLOSE_LETTERS,
        "join_coverage": cov,
        "cost_model": "futubull",
        "live_untouched": "flatten_robust",
        "cells": [slim(r) for r in rows if r["verdict"] in ("KEEP", "THIN")
                  or r["family"] in ("color", "hyst_color", "join")],
    }
    json.dump(payload, open(os.path.join(RESEARCH, "color_join_mine.json"),
                            "w"), indent=1)
    open(os.path.join(RESEARCH, "COLOR_JOIN_MINE.md"), "w",
         encoding="utf-8").write(
        "# Excel color + join mine\n\n" + md)
    return payload


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--workers", type=int, default=min(4, os.cpu_count() or 2))
    ap.add_argument("--rejudge", action="store_true")
    args = ap.parse_args()
    os.chdir(ROOT)
    if args.rejudge:
        raw = json.load(open(os.path.join(RESEARCH, "color_join_mine.json")))
        rows = rejudge(raw["cells"], load_parents())
        payload = write_outputs(rows, raw.get("grids") or 3603,
                                raw.get("join_coverage") or {})
        print(f"[rejudge] KEEP={payload['n_keep']} THIN={payload['n_thin']} "
              f"KILL={payload['n_kill']}", flush=True)
        return
    split = json.load(open(SPLIT_PATH))
    disc, hold = set(x.upper() for x in split["discovery"]), set(
        x.upper() for x in split["holdout"])
    spy = load_spy()
    pats = candidate_pats()
    ab, book, wx = load_ab_tones(), load_book_buys(), load_weather_risk()
    cov = {"ab": coverage(ab), "book": coverage(book), "wx": coverage(wx)}
    fz = load_finviz()
    fz_co = {t.upper(): set(cohorts_of(rec)) for t, rec in fz.items()}
    files = [f for f in sorted(glob.glob(os.path.join(GRIDS, "*.json")))
             if not os.path.basename(f).startswith("_")]
    print(f"[color_join] grids={len(files)} hyst={len(pats)} "
          f"ab={cov['ab']['n_dates']} book={cov['book']['n_dates']} "
          f"wx={cov['wx']['n_dates']}", flush=True)
    trades, seen = collect(files, pats, spy, disc, hold,
                           (ab, book, wx), fz_co, args.workers)
    rows = score_all(trades, load_baselines(), load_parents())
    payload = write_outputs(rows, seen or len(files), cov)
    print(f"[done] KEEP={payload['n_keep']} THIN={payload['n_thin']} "
          f"KILL={payload['n_kill']}", flush=True)
    for r in rows:
        if r["verdict"] == "KEEP":
            print(f"  KEEP {r['def']:42s} {r['exit']} "
                  f"{fmt_blk(r.get('holdout'))}", flush=True)


if __name__ == "__main__":
    main()
