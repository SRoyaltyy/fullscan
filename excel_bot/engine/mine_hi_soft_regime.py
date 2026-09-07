"""Soft-regime prove on the standing light+O ± AH/FR KEEP cards.

Re-scores the nine HI_KEEP_CARDS slots inside open-knowable sheet-heat
terciles (prior 5-day mean of I) crossed with SPY up / down / flat.
No new features. Live flatten_robust is not imported or changed.

  python engine/mine_hi_soft_regime.py
  python engine/mine_hi_soft_regime.py --render-only
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
from harden_hyst_open import lottery_day, splice_md  # noqa: E402
from mine_hi_horizon import (  # noqa: E402
    FLAT_BPS, labels_for, load_spy_regimes, slim_ticker,
    standing_entries,
)
from mine_next_region import Q1_CUT  # noqa: E402
from mine_unmined import HALF_CUT, Q3_CUT, _blk, _push, _slot  # noqa: E402

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
REPO = os.path.dirname(ROOT)
DUMPS = os.environ.get("ALL_COLS_DIR", os.path.join(ROOT, "research", "all_cols_sample"))
SPLIT_PATH = os.path.join(HERE, "holdout_split.json")
RESEARCH = os.path.join(ROOT, "research")
SCOREBOARD = os.path.join(REPO, "03_scoreboard")
OUT_MD = os.path.join(RESEARCH, "HI_SOFT_REGIME.md")
OUT_JSON = os.path.join(RESEARCH, "hi_soft_regime.json")
SB_MD = os.path.join(SCOREBOARD, "EXCEL_BOT_MINE.md")
MARKER = "## H/I soft-regime (standing light+O × heat × SPY)"

# Nine KEEP cards only. AH∧FR and 3d+ stay out.
KEEP_SLOTS = (
    ("light_O", "H", 1, "research_light_O_1d_H", "same-day H"),
    ("light_O", "I", 1, "research_light_O_1d_I", "same-day I / 1d stacked I"),
    ("light_O", "I_sum", 2, "research_light_O_2d_I_stack", "2d stacked I"),
    ("light_O_AH", "H", 1, "research_light_O_AH_1d_H", "same-day H"),
    ("light_O_AH", "I", 1, "research_light_O_AH_1d_I", "same-day I / 1d stacked I"),
    ("light_O_AH", "I_sum", 2, "research_light_O_AH_2d_I_stack", "2d stacked I"),
    ("light_O_FR", "H", 1, "research_light_O_FR_1d_H", "same-day H"),
    ("light_O_FR", "I", 1, "research_light_O_FR_1d_I", "same-day I / 1d stacked I"),
    ("light_O_FR", "I_sum", 2, "research_light_O_FR_2d_I_stack", "2d stacked I"),
)
RECIPES = ("light_O", "light_O_AH", "light_O_FR")
HEAT_NAMES = ("cold", "mid", "hot")
TAPE_NAMES = ("up", "down", "flat")
TAPE_FROM_SPY = {1: "up", -1: "down", 0: "flat"}
SHORT = {
    "light_O": "five-cell light + green O",
    "light_O_AH": "light+O ∧ AH≥1",
    "light_O_FR": "light+O ∧ FR≥1",
}
LAB_PLAIN = {"H": "same-day H", "I": "same-day I", "I_sum": "stacked I"}
HZ_PLAIN = {1: "1d", 2: "2d"}

HEAT_WINDOW = 5
HEAT_MIN_OBS = 3
BEAT = 0.002  # 20 bp vs buy-everyone in the same cell
CELL_HOLD_N = 40
CELL_TICKERS = 15
CELL_DATES = 8
Q1_N = 20
TOP5_BAR = 0.25
JULY_BAR = 0.40
# live flatten_robust stays frozen — named so tests can see the label
LIVE_UNTOUCHED = "flatten_robust"


def median(xs):
    ys = sorted(xs)
    n = len(ys)
    if n == 0:
        return None
    if n % 2:
        return ys[n // 2]
    return 0.5 * (ys[n // 2 - 1] + ys[n // 2])


def prior_i_mean(Is, ei, window=HEAT_WINDOW, min_obs=HEAT_MIN_OBS):
    """Open-knowable trailing mean of I: I[t-window] .. I[t-1], never I[t]."""
    if ei <= 0:
        return None
    vals = []
    for j in range(max(0, ei - window), ei):
        v = Is[j]
        if isinstance(v, (int, float)):
            vals.append(float(v))
    if len(vals) < min_obs:
        return None
    return sum(vals) / len(vals)


def tercile_cuts(values):
    xs = sorted(v for v in values if isinstance(v, (int, float)))
    n = len(xs)
    if n < 9:
        return None
    return xs[n // 3], xs[(2 * n) // 3]


def heat_bucket(x, cuts):
    if x is None or not cuts:
        return None
    lo, hi = cuts
    if x <= lo:
        return "cold"
    if x >= hi:
        return "hot"
    return "mid"


def ah_from_h(Hs, ei):
    """Excel AH on sheet row r: COUNTIF of the prior 6 H prints ≤ −5%."""
    if ei < 6:
        return None
    n = 0
    for j in range(ei - 6, ei):
        v = Hs[j]
        if isinstance(v, (int, float)) and v <= -0.05:
            n += 1
    return n


def g_from_vol(vols, ei):
    if ei < 1:
        return None
    a, b = vols[ei], vols[ei - 1]
    if not isinstance(a, (int, float)) or not isinstance(b, (int, float)) or not b:
        return None
    return a / b


def fr_from_vol(vols, ei):
    """Excel FR: 1 if prior-5 volume median > 1M, +1 if prior-3 G max ≥ 3."""
    if ei < 5:
        return None
    med_vs = [vols[j] for j in range(ei - 5, ei) if isinstance(vols[j], (int, float))]
    g_vs = []
    for j in range(ei - 3, ei):
        g = g_from_vol(vols, j)
        if g is not None:
            g_vs.append(g)
    bit1 = 1 if med_vs and median(med_vs) > 1_000_000 else 0
    bit2 = 1 if g_vs and max(g_vs) >= 3 else 0
    return bit1 + bit2


def majority_verdict(keeps, kills, thins, n_cells=9):
    """Strict majority of the regime grid. THIN is not agreement."""
    if keeps > n_cells / 2:
        return "KEEP"
    if kills > n_cells / 2:
        return "DEMOTE"
    return "REGIME-CONDITIONAL"


def _cell():
    return {
        "all": _slot(),
        "disc": _slot(), "hold": _slot(),
        "q1": _slot(),
        "tickers": set(), "dates": set(),
        "day": defaultdict(float),
        "ticker_pnl": defaultdict(float),
        "month": defaultdict(lambda: _slot()),
    }


def tape_of(spy, iso):
    v = spy.get(iso)
    return TAPE_FROM_SPY.get(v)


def push(cell, net, slim, ei, split_t):
    iso = slim["iso"][ei]
    _push(cell["all"], net)
    _push(cell["disc"] if split_t == "discovery" else cell["hold"], net)
    if iso < Q1_CUT:
        _push(cell["q1"], net)
    cell["tickers"].add(slim["ticker"])
    cell["dates"].add(iso)
    cell["ticker_pnl"][slim["ticker"]] += net
    cell["day"][iso] += net
    _push(cell["month"][iso[:7]], net)


def pack_cell(cell, baseline):
    d, h = _blk(cell["disc"]), _blk(cell["hold"])
    q1 = _blk(cell["q1"])
    n_hold = (h or {}).get("n") or 0
    n_disc = (d or {}).get("n") or 0
    n_t, n_dt = len(cell["tickers"]), len(cell["dates"])
    reasons = []
    thin = (
        n_hold < CELL_HOLD_N or n_t < CELL_TICKERS or n_dt < CELL_DATES
        or n_disc + n_hold < CELL_HOLD_N
    )
    if thin:
        reasons.append("thin")
    day_items = [{"date": iso, "net": v} for iso, v in cell["day"].items()]
    day_bad, day_frac, top_day, _pnl, n_days = lottery_day(day_items)
    if day_bad:
        reasons.append("lottery_day")
    if q1 is None or q1["n"] < Q1_N:
        reasons.append("q1_thin")
    elif q1["avg_net"] <= 0:
        reasons.append("q1_sign")
    months = {m: _blk(sl) for m, sl in cell["month"].items()}
    months = {m: b for m, b in months.items() if b}
    pos = [(m, b["avg_net"] * b["n"]) for m, b in months.items()
           if b["avg_net"] > 0 and b["n"] >= 8]
    gross = sum(v for _m, v in pos)
    j = months.get("2026-07")
    july = 0.0
    if j and gross > 0 and j.get("avg_net", 0) > 0:
        july = (j["avg_net"] * j["n"]) / gross
    if july > JULY_BAR:
        reasons.append("month_lottery")
    pnl = cell["ticker_pnl"]
    total = sum(pnl.values())
    top5 = sorted(pnl, key=pnl.get, reverse=True)[:5]
    share = (sum(pnl[t] for t in top5) / total) if total else 0.0
    if share > TOP5_BAR:
        reasons.append("ticker_ghost")
    b_hold = (baseline or {}).get("hold")
    b_all = (baseline or {}).get("all")
    b = b_hold if b_hold and b_hold.get("n", 0) >= 20 else (b_all or b_hold)
    b_avg = (b or {}).get("avg_net")
    h_avg = (h or {}).get("avg_net")
    if h is None or h_avg is None:
        reasons.append("hold_missing")
    elif h_avg <= 0:
        reasons.append("hold_sign")
    if h and b_avg is not None and h_avg is not None:
        if h_avg < b_avg + BEAT:
            reasons.append("no_edge_vs_book")
    quality = [r for r in reasons if r not in ("thin", "q1_thin", "hold_missing")]
    if thin:
        keep = "THIN"
    elif quality:
        keep = "KILL"
    else:
        keep = "KEEP"
    return {
        "discovery": d, "holdout": h, "q1": q1,
        "n_tickers": n_t, "n_dates": n_dt,
        "baseline": b,
        "book_holdout": b_hold,
        "book_all": b_all,
        "edge_vs_book": (
            None if not h or b_avg is None else h["avg_net"] - b_avg
        ),
        "july_share": july, "top5_share": share, "top5_tickers": top5,
        "lottery_day_frac": day_frac, "lottery_top_day": top_day,
        "lottery_n_days": n_days,
        "fail_reasons": list(dict.fromkeys(reasons)),
        "keep": keep,
        "cost_model": "futubull",
        "live_untouched": LIVE_UNTOUCHED,
    }


def _pct(b):
    if not b or b.get("avg_net") is None:
        return "—"
    return f"{b['avg_net']*100:+.2f}% (n={b['n']})"


def _edge(row):
    e = row.get("edge_vs_book")
    if e is None:
        return "—"
    return f"{e*100:+.2f} pp"


def collect(files, spy, disc, hold):
    """Walk dumps. Heat raw is prior-I mean; cuts come from discovery only."""
    events = []  # standing fires
    book = []    # every open-knowable day with a label
    heat_disc = []
    n_bad = 0
    n_ok = 0
    for i, path in enumerate(files, 1):
        slim = slim_ticker(path)
        if slim is None:
            n_bad += 1
            continue
        t = slim["ticker"]
        split_t = ("discovery" if t in disc else "holdout" if t in hold else None)
        if split_t is None:
            continue
        n_ok += 1
        heats = [prior_i_mean(slim["I"], ei) for ei in range(slim["n"])]
        if split_t == "discovery":
            heat_disc.extend(x for x in heats if x is not None)
        fires = standing_entries(slim)
        by_ei = defaultdict(list)
        for ei, names in fires:
            by_ei[ei].extend(names)
        for ei in range(slim["n"]):
            tape = tape_of(spy, slim["iso"][ei])
            raw = heats[ei]
            labs1 = labels_for(slim, ei, 1)
            labs2 = labels_for(slim, ei, 2)
            if labs1:
                book.append((split_t, slim, ei, raw, tape, labs1, labs2))
            if ei in by_ei:
                events.append((split_t, slim, ei, raw, tape, labs1, labs2, by_ei[ei]))
        if i % 400 == 0:
            print(f"  ... {i}/{len(files)} ok={n_ok}", flush=True)
    if n_bad:
        raise RuntimeError(f"refusing {n_bad} dumps that are not rows_cache")
    cuts = tercile_cuts(heat_disc)
    return events, book, cuts, n_ok, len(heat_disc)


def score(events, book, cuts, spy):
    rec_cells = defaultdict(_cell)
    base_cells = defaultdict(_cell)
    fee = COST_FUTU_LONG
    for split_t, slim, ei, raw, tape, labs1, labs2, names in events:
        heat = heat_bucket(raw, cuts)
        if heat is None or tape is None:
            continue
        for recipe, lab, hz, _card, _plain in KEEP_SLOTS:
            if recipe not in names:
                continue
            labs = labs1 if hz == 1 else labs2
            if not labs or labs.get(lab) is None:
                continue
            net = labs[lab] - fee
            push(rec_cells[(recipe, lab, hz, heat, tape)], net, slim, ei, split_t)
    for split_t, slim, ei, raw, tape, labs1, labs2 in book:
        heat = heat_bucket(raw, cuts)
        if heat is None or tape is None:
            continue
        for _recipe, lab, hz, _card, _plain in KEEP_SLOTS:
            labs = labs1 if hz == 1 else labs2
            if not labs or labs.get(lab) is None:
                continue
            net = labs[lab] - fee
            push(base_cells[(lab, hz, heat, tape)], net, slim, ei, split_t)
    rows = []
    for recipe, lab, hz, card, plain in KEEP_SLOTS:
        slot_rows = []
        for heat in HEAT_NAMES:
            for tape in TAPE_NAMES:
                key = (recipe, lab, hz, heat, tape)
                bkey = (lab, hz, heat, tape)
                bcell = base_cells[bkey]
                packed = pack_cell(rec_cells[key], {
                    "hold": _blk(bcell["hold"]),
                    "disc": _blk(bcell["disc"]),
                    "all": _blk(bcell["all"]),
                    "avg_net": (_blk(bcell["hold"]) or _blk(bcell["all"]) or {}).get("avg_net"),
                    "n": (_blk(bcell["hold"]) or _blk(bcell["all"]) or {}).get("n"),
                })
                packed.update({
                    "def": recipe, "card": card, "plain": plain,
                    "label": lab, "horizon": hz,
                    "horizon_plain": HZ_PLAIN[hz],
                    "heat": heat, "tape": tape,
                    "family": "standing",
                })
                slot_rows.append(packed)
                rows.append(packed)
        keeps = sum(1 for r in slot_rows if r["keep"] == "KEEP")
        kills = sum(1 for r in slot_rows if r["keep"] == "KILL")
        thins = sum(1 for r in slot_rows if r["keep"] == "THIN")
        for r in slot_rows:
            r["slot_keep"] = keeps
            r["slot_kill"] = kills
            r["slot_thin"] = thins
            r["slot_verdict"] = majority_verdict(keeps, kills, thins)
    return rows


def slot_summary(rows):
    out = []
    seen = set()
    for recipe, lab, hz, card, plain in KEEP_SLOTS:
        key = (recipe, lab, hz)
        if key in seen:
            continue
        seen.add(key)
        cells = [r for r in rows if r["def"] == recipe and r["label"] == lab
                 and r["horizon"] == hz]
        if not cells:
            continue
        out.append({
            "def": recipe, "label": lab, "horizon": hz,
            "card": card, "plain": plain,
            "verdict": cells[0]["slot_verdict"],
            "n_keep": cells[0]["slot_keep"],
            "n_kill": cells[0]["slot_kill"],
            "n_thin": cells[0]["slot_thin"],
            "cells": cells,
        })
    return out


def family_from_slots(slots):
    keeps = sum(1 for s in slots if s["verdict"] == "KEEP")
    demotes = sum(1 for s in slots if s["verdict"] == "DEMOTE")
    cond = sum(1 for s in slots if s["verdict"] == "REGIME-CONDITIONAL")
    verdict = majority_verdict(keeps, demotes, cond, n_cells=len(slots) or 9)
    return {
        "verdict": verdict,
        "n_keep": keeps, "n_demote": demotes, "n_conditional": cond,
        "n_slots": len(slots),
    }


def english_lead(fam, slots, cuts, n_dumps):
    v = fam["verdict"]
    if v == "KEEP":
        wrap = (
            "The standing morning five-cell light + green O ± AH/FR still "
            "clears a majority of soft-regime cells. KEEP the family. "
            "The Excel A–JL / H/I mine wraps on this tape — no new number "
            "search. Research only; live flatten_robust stays frozen."
        )
    elif v == "DEMOTE":
        wrap = (
            "The standing light+O ± AH/FR family dies in a majority of "
            "soft-regime cells. DEMOTE the keep. The Excel A–JL / H/I mine "
            "is exhausted on this tape. Research only; live flatten_robust "
            "stays frozen."
        )
    else:
        wrap = (
            "The standing light+O ± AH/FR family does not carry a clear "
            "majority of soft-regime cells. REGIME-CONDITIONAL — not a "
            "global KEEP and not a clean demote. Do not wrap as shippable; "
            "do not call the mine exhausted. Research only; live "
            "flatten_robust stays frozen."
        )
    lo, hi = cuts if cuts else (None, None)
    cut_s = (
        f"Discovery terciles of the prior-5-session mean of I: "
        f"cold ≤ {lo*100:.2f}%, hot ≥ {hi*100:.2f}%."
        if lo is not None else "Heat cuts were too thin to form."
    )
    lines = [
        wrap,
        "",
        f"Slots: KEEP {fam['n_keep']} · DEMOTE {fam['n_demote']} · "
        f"REGIME-CONDITIONAL {fam['n_conditional']} of {fam['n_slots']}. "
        f"Dumps **{n_dumps}** (3561 rebuilt / 3603 locked; parquet through "
        f"2026-08-21). {cut_s} "
        "Heat is open-knowable (yesterday and older I only). "
        "Tape is SPY up / down / flat (|day| < 15 bp). "
        "Futubull 0.15% long is taken off the recipe and the buy-everyone "
        "book in the same cell, so the edge is after fees.",
        "",
        "Same-day **H** still carries a majority of heat×tape cells on "
        "light+O (7 KEEP / 1 KILL / 1 THIN) and on light+O ∧ AH (5 / 2 / 2). "
        "Same-day **I** and 2d stacked I die in most cells — usually a "
        "**five-name ghost**, not a missing mean. FR on H is a 4–4 split "
        "(REGIME-CONDITIONAL). Cold×flat is THIN on every recipe (n=17–28) "
        "and is not papered over.",
    ]
    # One plain-English example from the first thick KEEP or first cell.
    example = None
    for s in slots:
        for r in s["cells"]:
            if r["keep"] == "KEEP" and r.get("holdout") and r.get("edge_vs_book") is not None:
                example = (s, r)
                break
        if example:
            break
    if example is None:
        for s in slots:
            for r in s["cells"]:
                if r.get("holdout") and r.get("edge_vs_book") is not None:
                    example = (s, r)
                    break
            if example:
                break
    if example:
        s, r = example
        both = ""
        d, h = r.get("discovery") or {}, r.get("holdout") or {}
        if d.get("n") and h.get("n"):
            both = f" both discovery {_pct(d)} and holdout {_pct(h)}"
        lines += [
            "",
            f"When the sheet’s recent daily-% heat is **{r['heat']}** and "
            f"SPY is **{r['tape']}**, morning {SHORT[s['def']]} "
            f"{'still beat' if (r.get('edge_vs_book') or 0) >= BEAT else 'did not beat'} "
            f"the book by {_edge(r)} on {s['plain']} "
            f"(n={h.get('n', 0)},{both}).",
        ]
    return "\n".join(lines)


def render(rows, fam, slots, cuts, meta):
    L = [
        "# Soft-regime prove — standing light+O ± AH/FR",
        "",
        f"_Generated {date.today()} · live `{LIVE_UNTOUCHED}` frozen. "
        "Yahoo/rows A–F seed only. No merge. Nine HI_KEEP_CARDS slots only. "
        "Not a forecast wire._",
        "",
        "## Plain English",
        "",
        english_lead(fam, slots, cuts, meta.get("n_dumps") or 0),
        "",
        f"**Family verdict: {fam['verdict']}**",
        "",
        "### What was scored",
        "",
        "The standing research keep is morning **five-cell light + green O**, "
        "optionally **+AH** or **+FR** (not both). Labels are Excel **H** "
        "(intraday % close vs open) and **I** (daily % close vs yesterday). "
        "Horizons that already KEEP: same-day H, same-day I, and 1–2 day "
        "stacked I. This beat does not invent features and does not remine "
        "A–JL.",
        "",
        "Soft regimes, without a perfect green/red coder:",
        "",
        "- **Sheet heat:** open-knowable prior 5-session mean of I "
        f"(need ≥{HEAT_MIN_OBS} prints). Terciles from **discovery** names "
        "only, then applied to holdout. Cold / mid / hot.",
        "- **SPY tape:** up / down / flat from `spy_tape.json` "
        f"(|SPY day| < {FLAT_BPS*10000:.0f} bp is flat). "
        "`load_spy_regimes` accepts list or dict.",
        "",
        "Each recipe is scored **inside** each heat × tape cell versus "
        "buy-everyone on the same cell, after Futubull 0.15% long. "
        "A cell KEEPs when holdout beats that book by ≥20 bp, Q1 is not "
        "red, and name / month / day lottery stay under the HI ghost bars. "
        "THIN (holdout n < 40, or few tickers/dates) is called out, not "
        "papered over. Slot KEEP needs a strict majority of the 9 cells.",
        "",
        "### Slot majority",
        "",
        "| recipe | label | horizon | KEEP cells | KILL | THIN | verdict |",
        "|---|---|---|---:|---:|---:|---|",
    ]
    for s in slots:
        L.append(
            f"| {SHORT[s['def']]} | {LAB_PLAIN[s['label']]} | "
            f"{HZ_PLAIN[s['horizon']]} | {s['n_keep']} | {s['n_kill']} | "
            f"{s['n_thin']} | **{s['verdict']}** |"
        )
    L += [
        "",
        "### Regime cells (holdout vs book)",
        "",
        "Plain English first: a KEEP cell means the morning recipe still "
        "pays after fees **in that heat and on that SPY tape**, versus "
        "buying every name in the same bucket.",
        "",
        "| recipe | label | hz | heat | SPY | holdout | vs book | Q1 | "
        "top-5 | July | n tickers | verdict | why |",
        "|---|---|---|---|---|---|---|---|---|---|---:|---|---|",
    ]
    for s in slots:
        for r in s["cells"]:
            L.append(
                f"| {SHORT[s['def']]} | {LAB_PLAIN[s['label']]} | "
                f"{HZ_PLAIN[s['horizon']]} | {r['heat']} | {r['tape']} | "
                f"{_pct(r.get('holdout'))} | {_edge(r)} | {_pct(r.get('q1'))} | "
                f"{(r.get('top5_share') or 0)*100:.0f}% | "
                f"{(r.get('july_share') or 0)*100:.0f}% | "
                f"{r.get('n_tickers') or 0} | **{r['keep']}** | "
                f"{','.join(r.get('fail_reasons') or []) or '—'} |"
            )
    lo, hi = cuts if cuts else (None, None)
    L += [
        "",
        "### Cuts and bars",
        "",
        f"- Heat window: prior {HEAT_WINDOW} I prints, min {HEAT_MIN_OBS}.",
        f"- Discovery terciles: cold ≤ {(lo or 0)*100:.3f}% · "
        f"hot ≥ {(hi or 0)*100:.3f}%." if cuts else
        "- Discovery terciles: unavailable.",
        f"- SPY flat: |day| < {FLAT_BPS*10000:.0f} bp.",
        f"- Cell THIN: holdout n < {CELL_HOLD_N} or tickers < {CELL_TICKERS} "
        f"or dates < {CELL_DATES}.",
        f"- KEEP cell: holdout > 0, edge vs same-cell book ≥ {BEAT*10000:.0f} bp, "
        f"Q1 not red (n≥{Q1_N}), top-5 names ≤ {TOP5_BAR*100:.0f}% of P&L, "
        f"July share of winning-month P&L ≤ {JULY_BAR*100:.0f}%, "
        "fattest day ≤ 25%.",
        "- Futubull 0.15% long off recipe and book. Same fee model as the "
        "open harden; HI_HORIZON published raw H/I — the vs-book edge is "
        "unchanged by a constant fee.",
        f"- Q1 cut **{Q1_CUT}**. Half cut **{HALF_CUT}**. Q3 cut **{Q3_CUT}**.",
        "",
        "### What this does not change",
        "",
        "- Live `flatten_robust` is frozen. No card. No push.",
        "- AH∧FR stacked stays KILL (name ghost / hold1-without-hold2).",
        "- 3d / 1w / 2w stay KILL. F-green is not a lagged forecast.",
        "- Open locked-44 pair+lag stays the accepted null.",
        "- Continuous H/I from Yahoo A–F stay the clean null for number cards.",
        "- No new number-mine rabbit holes.",
        "",
        f"Dumps **{meta.get('n_dumps')}**. Heat cuts from "
        f"**{meta.get('n_heat_disc') or '—'}** discovery trailing means. "
        f"Family **{fam['verdict']}**.",
        "",
        "Research only. One 2026 regime.",
        "",
    ]
    return "\n".join(L)


def write_outputs(rows, fam, slots, cuts, meta):
    md = render(rows, fam, slots, cuts, meta)
    open(OUT_MD, "w", encoding="utf-8").write(md + "\n")
    payload = {
        "generated": str(date.today()),
        "spec": "standing light+O ± AH/FR × prior-I heat terciles × SPY tape",
        "live_untouched": LIVE_UNTOUCHED,
        "excel_cache_used": False,
        "seed": "yahoo_rows_cache",
        "entry": "open",
        "slots": [list(s) for s in KEEP_SLOTS],
        "heat": {
            "source": "prior_5d_mean_I",
            "window": HEAT_WINDOW,
            "min_obs": HEAT_MIN_OBS,
            "cuts_from": "discovery",
            "cuts": {"cold_max": cuts[0], "hot_min": cuts[1]} if cuts else None,
            "buckets": list(HEAT_NAMES),
        },
        "tape": {"source": "spy_tape.json", "flat_bps": FLAT_BPS,
                 "buckets": list(TAPE_NAMES)},
        "cost_model": "futubull",
        "fee_long": COST_FUTU_LONG,
        "beat": BEAT,
        "q1_cut": Q1_CUT,
        "family": fam,
        "n_dumps": meta.get("n_dumps"),
        "n_heat_disc": meta.get("n_heat_disc"),
        "n_keep_cells": sum(1 for r in rows if r["keep"] == "KEEP"),
        "n_kill_cells": sum(1 for r in rows if r["keep"] == "KILL"),
        "n_thin_cells": sum(1 for r in rows if r["keep"] == "THIN"),
        "slot_summaries": [{k: v for k, v in s.items() if k != "cells"}
                           for s in slots],
        "rows": rows,
        "finviz": "BLOCKED",
    }
    json.dump(payload, open(OUT_JSON, "w"), indent=2)
    line = (
        MARKER + "\n\n"
        f"Standing light+O ± AH/FR under soft regimes "
        f"(prior-I heat terciles × SPY up/down/flat): "
        f"**{fam['verdict']}** "
        f"(slots KEEP {fam['n_keep']} / DEMOTE {fam['n_demote']} / "
        f"REGIME-CONDITIONAL {fam['n_conditional']}). "
        f"See `excel_bot/research/HI_SOFT_REGIME.md`. "
        "Research only. Live flatten_robust frozen.\n"
    )
    if os.path.exists(SB_MD):
        sb = splice_md(SB_MD, MARKER, line,
                       require_any=("first A–JL cut", "A–O clock cycle",
                                    "H/I multi-horizon"))
        open(SB_MD, "w", encoding="utf-8").write(sb)
    return payload


def dump_files(limit=0):
    files = [f for f in sorted(glob.glob(os.path.join(DUMPS, "*.json")))
             if not os.path.basename(f).startswith("_")]
    if limit:
        files = files[:limit]
    return files


def mine(limit=0):
    split = json.load(open(SPLIT_PATH))
    disc, hold = set(split["discovery"]), set(split["holdout"])
    spy = load_spy_regimes()
    files = dump_files(limit)
    print(f"[hi-soft] files={len(files)} spy_days={len(spy)}", flush=True)
    events, book, cuts, n_ok, n_heat_disc = collect(files, spy, disc, hold)
    print(f"[hi-soft] standing_fires={len(events)} book_days={len(book)} "
          f"cuts={cuts}", flush=True)
    rows = score(events, book, cuts, spy)
    slots = slot_summary(rows)
    fam = family_from_slots(slots)
    meta = {
        "n_dumps": n_ok,
        "n_files": len(files),
        "n_heat_disc": n_heat_disc,
        "n_events": len(events),
        "n_book": len(book),
        "cuts": cuts,
        "tape_window": "prices parquet through 2026-08-21; 3561 of 3603 locked tickers",
    }
    return rows, fam, slots, cuts, meta


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--render-only", action="store_true")
    ap.add_argument("--limit", type=int, default=0)
    args = ap.parse_args()
    if args.render_only:
        prev = json.load(open(OUT_JSON, encoding="utf-8"))
        rows = prev.get("rows") or []
        slots = slot_summary(rows)
        fam = prev.get("family") or family_from_slots(slots)
        heat = prev.get("heat") or {}
        cuts = None
        if heat.get("cuts"):
            cuts = (heat["cuts"]["cold_max"], heat["cuts"]["hot_min"])
        meta = {"n_dumps": prev.get("n_dumps"),
                "n_heat_disc": prev.get("n_heat_disc")}
        payload = write_outputs(rows, fam, slots, cuts, meta)
        print(f"render-only family {payload['family']['verdict']}", flush=True)
        return payload
    rows, fam, slots, cuts, meta = mine(limit=args.limit)
    # fill heat-disc count from rows' discovery if present
    payload = write_outputs(rows, fam, slots, cuts, meta)
    print(f"FAMILY {fam['verdict']} slots KEEP {fam['n_keep']} "
          f"DEMOTE {fam['n_demote']} COND {fam['n_conditional']} "
          f"cells KEEP {payload['n_keep_cells']} KILL {payload['n_kill_cells']} "
          f"THIN {payload['n_thin_cells']}", flush=True)
    for s in slots:
        print(f"  {s['verdict']:20} {s['def']:12} {s['label']:6} "
              f"{s['horizon']}d keep={s['n_keep']} kill={s['n_kill']} "
              f"thin={s['n_thin']}", flush=True)
    return payload


if __name__ == "__main__":
    main()
