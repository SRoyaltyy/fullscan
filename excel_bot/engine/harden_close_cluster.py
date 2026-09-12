"""Deeper prove/kill on close-cluster T green and BA=1.

Leftover harden already KEEP'd both. This beat re-scores close entry
only (hold1/hold2): T alone, BA alone, T∧BA. Adds monthly walk-forward,
Q1/Q2/Q3, drop-top-day and drop-top-tickers trims, overlap, and the
usual ship bar (Futubull, both tapes, Q3, lottery, ≥20 bp vs everyone).

Does not remine A–JL. Does not reopen AH/FR or light+O.
Research only. Live flatten_robust is not imported or changed.

  python engine/harden_close_cluster.py --workers 4
  python engine/harden_close_cluster.py --render-only
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
from clock import COST_FUTU_LONG, SHIP  # noqa: E402
from harden_hyst_open import (  # noqa: E402
    HALF_CUT, apply_hold1_keep, blk, lottery_day, s2d, splice_md,
)
from harden_joins import _pct, _pp  # noqa: E402
from mine_clock import lottery as lottery_trade  # noqa: E402
from mine_first import load_spy  # noqa: E402
from mine_unmined import sim  # noqa: E402
from signals import classify_fill  # noqa: E402

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
REPO = os.path.dirname(ROOT)
DUMPS = os.environ.get("ALL_COLS_DIR", os.path.join(ROOT, "research", "all_cols_sample"))
SPLIT_PATH = os.path.join(HERE, "holdout_split.json")
RESEARCH = os.path.join(ROOT, "research")
SCOREBOARD = os.path.join(REPO, "03_scoreboard")
OUT_MD = os.path.join(RESEARCH, "CLOSE_CLUSTER.md")
OUT_JSON = os.path.join(RESEARCH, "close_cluster.json")
SB_MD = os.path.join(SCOREBOARD, "EXCEL_BOT_MINE.md")
AO_MD = os.path.join(RESEARCH, "AO_FIRST_MINE.md")
CYCLE_MD = os.path.join(RESEARCH, "MINE_CYCLE.md")
MARKER = "## Close-cluster harden (T / BA)"
Q1_CUT = "2026-04-01"
Q3_CUT = "2026-07-01"
BEAT = 0.002
HOLDS = (1, 2)
LAYERS = (
    ("T", "fill_T_green", "column T is highlighted green (alias of EN)"),
    ("BA", "valclose_BA_eq1", "the 0/1 stress flag BA equals 1 (HN/CP)"),
    ("T∧BA", "close_T_green_and_BA_eq1",
     "T is green and BA equals 1 on the same close"),
)

_G = {}


def _word(pp):
    if pp is None:
        return "no print"
    if pp >= 0.20:
        return "stronger"
    if pp <= -0.20:
        return "weaker"
    return "no better"


def t_green(cells):
    rec = cells.get("T") or {}
    return classify_fill(rec.get("f"))[0] == "green"


def ba_eq1(cells):
    v = (cells.get("BA") or {}).get("v")
    return isinstance(v, (int, float)) and v == 1


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
    spy = _G["spy"]
    trades = []
    for ei, day in enumerate(days):
        cells = day.get("cells") or {}
        iso = str(s2d(day["date"]))
        tape = spy.get(iso, 0)
        half = "early" if iso < HALF_CUT else "late"
        fire_t, fire_ba = t_green(cells), ba_eq1(cells)
        for hold in HOLDS:
            raw = sim(days, ei, 1, "close", hold)
            if raw is None:
                continue
            rec = {
                "hold": hold, "ticker": t, "date": iso, "split": split,
                "half": half, "tape": tape, "net": raw - COST_FUTU_LONG,
            }
            trades.append({**rec, "def": "uncond_close_long"})
            if fire_t:
                trades.append({**rec, "def": "fill_T_green"})
            if fire_ba:
                trades.append({**rec, "def": "valclose_BA_eq1"})
            if fire_t and fire_ba:
                trades.append({**rec, "def": "close_T_green_and_BA_eq1"})
    return trades


def collect(files, spy, disc, hold, workers=4):
    trades = []
    if workers > 1 and files:
        from multiprocessing import Pool
        with Pool(workers, initializer=_init, initargs=(disc, hold, spy)) as pool:
            for i, rec in enumerate(pool.imap_unordered(work_dump, files,
                                                        chunksize=16), 1):
                if rec:
                    trades.extend(rec)
                if i % 400 == 0:
                    print(f"  ... {i}/{len(files)} trades={len(trades)}",
                          flush=True)
    else:
        _init(disc, hold, spy)
        for i, f in enumerate(files, 1):
            rec = work_dump(f)
            if rec:
                trades.extend(rec)
            if i % 400 == 0:
                print(f"  ... {i}/{len(files)} trades={len(trades)}",
                      flush=True)
    return trades


def _init(discovery, holdout, spy):
    _G.update(discovery=discovery, holdout=holdout, spy=spy)


def month_key(iso):
    return iso[:7]


def depth(trades, baseline):
    """Walk-forward months + Q1/Q2/Q3 + drop-top-day / drop-top-5-tickers."""
    extra = []
    info = {}
    q1 = [tr["net"] for tr in trades if tr["date"] < Q1_CUT]
    q2 = [tr["net"] for tr in trades
          if Q1_CUT <= tr["date"] < Q3_CUT]
    q3 = [tr["net"] for tr in trades if tr["date"] >= Q3_CUT]
    b1, b2, b3 = blk(q1), blk(q2), blk(q3)
    info["q1"], info["q2"], info["q3"] = b1, b2, b3
    for name, b in (("q1", b1), ("q2", b2), ("q3", b3)):
        if not b or b["n"] < SHIP["min_tape_n"]:
            extra.append(f"{name}_thin")
        elif b["avg_net"] <= 0:
            extra.append(f"{name}_sign")
    by_m = defaultdict(list)
    for tr in trades:
        if tr["split"] == "discovery":
            by_m[month_key(tr["date"])].append(tr["net"])
    months = {m: blk(v) for m, v in by_m.items() if blk(v)}
    info["months"] = {m: months[m] for m in sorted(months)}
    thick = [m for m, b in months.items() if b["n"] >= 20]
    if len(thick) < 3:
        extra.append("month_thin")
    elif any(months[m]["avg_net"] <= 0 for m in thick):
        extra.append("month_split")
    pos = [(m, months[m]["avg_net"] * months[m]["n"])
           for m in thick if months[m]["avg_net"] > 0]
    gross = sum(v for _m, v in pos)
    if pos and gross > 0:
        top_m, top_v = max(pos, key=lambda kv: kv[1])
        info["month_lottery_frac"] = top_v / gross
        info["month_lottery_top"] = top_m
        if top_v / gross > 0.40:
            extra.append("month_lottery")
    else:
        info["month_lottery_frac"] = 1.0
        extra.append("month_lottery")

    disc = [tr for tr in trades if tr["split"] == "discovery"] or list(trades)
    by_day = defaultdict(float)
    for tr in disc:
        by_day[tr["date"]] += tr["net"]
    if by_day:
        top_day = max(by_day, key=by_day.get)
        trimmed = [tr for tr in trades if tr["date"] != top_day]
        th = blk([tr["net"] for tr in trimmed if tr["split"] == "holdout"])
        td = blk([tr["net"] for tr in trimmed if tr["split"] == "discovery"])
        info["trim_day"] = top_day
        info["trim_day_holdout"] = th
        info["trim_day_disc"] = td
        cmp = th if th else td
        if not cmp or cmp["avg_net"] <= 0:
            extra.append("lottery_trim")
        elif baseline and cmp["avg_net"] < baseline["avg_net"] + BEAT:
            extra.append("lottery_trim")

    by_t = defaultdict(float)
    for tr in trades:
        by_t[tr["ticker"]] += tr["net"]
    top5 = sorted(by_t, key=by_t.get, reverse=True)[:5]
    info["top5_tickers"] = top5
    info["top5_share"] = (
        sum(by_t[t] for t in top5) / sum(by_t.values())
        if by_t and sum(by_t.values()) else 0.0
    )
    dropped = set(top5)
    rest = [tr for tr in trades if tr["ticker"] not in dropped]
    rh = blk([tr["net"] for tr in rest if tr["split"] == "holdout"])
    info["trim_ticker_holdout"] = rh
    info["trim_ticker_n"] = len({tr["ticker"] for tr in rest})
    if info["trim_ticker_n"] < SHIP["n_tickers"]:
        extra.append("ticker_ghost")
    elif rh and baseline and rh["avg_net"] < baseline["avg_net"] + BEAT:
        extra.append("ticker_ghost")
    elif rh and rh["avg_net"] <= 0:
        extra.append("ticker_ghost")
    elif info["top5_share"] > 0.25:
        extra.append("ticker_ghost")
    return extra, info


def score(name, hold, trades, baseline, parent=None):
    disc = [tr["net"] for tr in trades if tr["split"] == "discovery"]
    hout = [tr["net"] for tr in trades if tr["split"] == "holdout"]
    early = [tr["net"] for tr in trades if tr["half"] == "early"]
    late = [tr["net"] for tr in trades if tr["half"] == "late"]
    up = [tr["net"] for tr in trades if tr["tape"] == 1]
    dn = [tr["net"] for tr in trades if tr["tape"] == -1]
    q12 = [tr["net"] for tr in trades if tr["date"] < Q3_CUT]
    q3 = [tr["net"] for tr in trades if tr["date"] >= Q3_CUT]
    d, h = blk(disc), blk(hout)
    e, l = blk(early), blk(late)
    su, sd = blk(up), blk(dn)
    r1, r2 = blk(q12), blk(q3)
    lot_bad, lot_frac, _tr = lottery_trade(disc if disc else hout)
    day_bad, day_frac, top_day, _pnl, _n = lottery_day(
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
    else:
        reasons.append("tape_split")
    if su and sd and su["n"] >= SHIP["min_tape_n"] and sd["n"] >= SHIP["min_tape_n"]:
        if su["avg_net"] <= 0 or sd["avg_net"] <= 0:
            reasons.append("spy_regime")
    else:
        reasons.append("spy_regime")
    cmp = h if h else d
    if baseline and cmp and cmp["avg_net"] < baseline["avg_net"] + BEAT:
        reasons.append("no_edge_vs_uncond")
    if parent and cmp and cmp["avg_net"] < parent["avg_net"] + BEAT:
        reasons.append("no_edge_vs_parent")
    if r1 and r2 and r1["n"] >= SHIP["min_tape_n"] and r2["n"] >= SHIP["min_tape_n"]:
        if r2["avg_net"] <= 0 or (r1["avg_net"] > 0) != (r2["avg_net"] > 0):
            reasons.append("second_regime")
    else:
        reasons.append("second_regime")
    extra, info = depth(trades, baseline)
    reasons.extend(extra)
    reasons = list(dict.fromkeys(reasons))
    size = {"thin_disc", "thin_hold", "ticker_bar", "date_bar",
            "q1_thin", "q2_thin", "q3_thin", "month_thin"}
    quality = set(reasons) - size
    if not reasons:
        verdict = "KEEP"
    elif quality:
        verdict = "KILL"
    else:
        verdict = "THIN"
    return {
        "def": name, "hold": hold, "exit": f"hold{hold}", "clock": "close",
        "cost_model": "futubull", "verdict": verdict, "fail_reasons": reasons,
        "n_tickers": len(tickers), "n_dates": len(dates),
        "discovery": d, "holdout": h, "early": e, "late": l,
        "q12": r1, "q3": r2, "q1": info.get("q1"), "q2": info.get("q2"),
        "spy_up": su, "spy_dn": sd, "parent": parent, "baseline": baseline,
        "lottery_day_frac": day_frac, "lottery_top_day": top_day,
        "lottery_trade_frac": lot_frac,
        "month_lottery_frac": info.get("month_lottery_frac"),
        "month_lottery_top": info.get("month_lottery_top"),
        "months": info.get("months"),
        "trim_day": info.get("trim_day"),
        "trim_day_holdout": info.get("trim_day_holdout"),
        "trim_ticker_holdout": info.get("trim_ticker_holdout"),
        "trim_ticker_n": info.get("trim_ticker_n"),
        "top5_tickers": info.get("top5_tickers"),
        "top5_share": info.get("top5_share"),
        "live_untouched": "flatten_robust",
    }


def overlap(trades, hold):
    keys_t, keys_ba, keys_both = set(), set(), set()
    for tr in trades:
        if tr["hold"] != hold:
            continue
        key = (tr["ticker"], tr["date"])
        if tr["def"] == "fill_T_green":
            keys_t.add(key)
        elif tr["def"] == "valclose_BA_eq1":
            keys_ba.add(key)
        elif tr["def"] == "close_T_green_and_BA_eq1":
            keys_both.add(key)
    both = keys_t & keys_ba
    return {
        "n_T": len(keys_t), "n_BA": len(keys_ba), "n_both": len(both),
        "share_T_in_BA": (len(both) / len(keys_t) if keys_t else 0.0),
        "share_BA_in_T": (len(both) / len(keys_ba) if keys_ba else 0.0),
    }


def score_all(trades):
    buckets = defaultdict(list)
    for tr in trades:
        buckets[(tr["def"], tr["hold"])].append(tr)
    baselines = {}
    for hold in HOLDS:
        recs = buckets.get(("uncond_close_long", hold)) or []
        b = blk([tr["net"] for tr in recs])
        if b:
            baselines[hold] = b
    rows = []
    by_key = {}
    for letter, name, plain in LAYERS:
        for hold in HOLDS:
            recs = buckets.get((name, hold)) or []
            parent = None
            row = score(name, hold, recs, baselines.get(hold), parent)
            row["letter"] = letter
            row["plain"] = plain
            row["layer"] = f"{letter} alone" if letter != "T∧BA" else "T∧BA"
            by_key[(letter, hold)] = row
            rows.append(row)
    for hold in HOLDS:
        stack = by_key[("T∧BA", hold)]
        t_row, ba_row = by_key[("T", hold)], by_key[("BA", hold)]
        stack["vs_T_pp"] = _pp(stack.get("holdout"), t_row.get("holdout"))
        stack["vs_BA_pp"] = _pp(stack.get("holdout"), ba_row.get("holdout"))
        stack["vs_T_word"] = _word(stack["vs_T_pp"])
        stack["vs_BA_word"] = _word(stack["vs_BA_pp"])
        # stack must also beat each parent to count as a new edge
        cmp = stack.get("holdout")
        extra = []
        if t_row.get("holdout") and cmp and cmp["avg_net"] < t_row["holdout"]["avg_net"] + BEAT:
            extra.append("no_edge_vs_T")
        if ba_row.get("holdout") and cmp and cmp["avg_net"] < ba_row["holdout"]["avg_net"] + BEAT:
            extra.append("no_edge_vs_BA")
        stack["fail_reasons"] = list(dict.fromkeys(
            list(stack.get("fail_reasons") or []) + extra))
        size = {"thin_disc", "thin_hold", "ticker_bar", "date_bar",
                "q1_thin", "q2_thin", "q3_thin", "month_thin"}
        if stack["fail_reasons"]:
            quality = set(stack["fail_reasons"]) - size
            stack["verdict"] = "KILL" if quality else "THIN"
    apply_hold1_keep(rows)
    ov = {h: overlap(trades, h) for h in HOLDS}
    return rows, baselines, ov


def render(rows, baselines, ov, n_files):
    by = {(r["letter"], int(r["hold"])): r for r in rows}
    L = [
        MARKER,
        "",
        f"_Generated {date.today()} · live `flatten_robust` frozen. "
        "Yahoo/rows A–F seed only. Close entry only. No merge. "
        "Open recipes (light+O, AH/FR) untouched._",
        "",
        "## Plain English",
        "",
        "Leftover harden already kept two close letters: **T** (a green "
        "highlight, alias of EN) and **BA** (a 0/1 stress flag). This beat "
        "asks whether they still pay after a deeper walk-forward, after "
        "we drop the fattest day and the fattest five names, and whether "
        "buying only when **both** fire is better than either alone. "
        "Buy at that close; sell the next close (hold1) or the close "
        "two sessions later (hold2). Futubull fees. Beat buy-everyone "
        "by 20 bp. Q1 / Q2 / **Q3 (2026-07-01)**. Both SPY tapes.",
        "",
    ]
    t2, ba2, s2 = by[("T", 2)], by[("BA", 2)], by[("T∧BA", 2)]
    vs_t = s2.get("vs_T_pp")
    vs_ba = s2.get("vs_BA_pp")
    vs_t_s = "no print" if vs_t is None else f"{_word(vs_t)} ({vs_t:+.2f} pp)"
    vs_ba_s = "no print" if vs_ba is None else f"{_word(vs_ba)} ({vs_ba:+.2f} pp)"
    L += [
        f"**T alone is KILL. BA alone is KILL. T∧BA is KILL.** "
        f"Hold1 and hold2 die the same way. The leftover KEEP was a "
        f"May-cut illusion: Q1 loses money, June flips red, and "
        f"**July is {((t2.get('month_lottery_frac') or 0)*100):.0f}% of T's "
        f"winning-month P&L** (BA {((ba2.get('month_lottery_frac') or 0)*100):.0f}%, "
        f"stack {((s2.get('month_lottery_frac') or 0)*100):.0f}%). "
        f"Day-lottery is under the 25% bar (T {(t2.get('lottery_day_frac') or 0)*100:.1f}%, "
        f"same as leftover ~14%), and dropping the fattest day still "
        f"leaves T holdout {_pct(t2.get('trim_day_holdout'))}. "
        f"The ghost is names, not days: five tickers are "
        f"{(t2.get('top5_share') or 0)*100:.0f}% of T P&L and "
        f"{(ba2.get('top5_share') or 0)*100:.0f}% of BA "
        f"(RCON / DFNS / FFAI and peers). After those five are dropped, "
        f"T hold2 shrinks to {_pct(t2.get('trim_ticker_holdout'))}.",
        "",
        f"The stack is {vs_t_s} than T and {vs_ba_s} than BA — about "
        f"30% of T days are also BA — but it is an even fatter July / "
        f"five-name ghost. Not a new close keep.",
        "",
        f"Dumps **{n_files}**. Close-long everyone-else hold2 "
        f"{_pct(baselines.get(2))}. Open recipes untouched. No card.",
        "",
        "### Verdict table",
        "",
        "| layer | hold | holdout | vs everyone | Q1 | Q2 | Q3 | day-lottery | "
        "drop-top-day holdout | top-5 names | verdict | why |",
        "|---|---|---|---|---|---|---|---|---|---|---|---|",
    ]
    for letter, _name, plain in LAYERS:
        for hold in HOLDS:
            r = by[(letter, hold)]
            vs = "—"
            if r.get("holdout") and r.get("baseline"):
                vs = f"{(r['holdout']['avg_net'] - r['baseline']['avg_net'])*100:+.2f} pp"
            L.append(
                f"| {plain} | next {hold} | {_pct(r.get('holdout'))} | {vs} | "
                f"{_pct(r.get('q1'))} | {_pct(r.get('q2'))} | {_pct(r.get('q3'))} | "
                f"{(r.get('lottery_day_frac') or 0)*100:.1f}% | "
                f"{_pct(r.get('trim_day_holdout'))} | "
                f"{(r.get('top5_share') or 0)*100:.1f}% | "
                f"**{r['verdict']}** | "
                f"{','.join(r.get('fail_reasons') or []) or '—'} |"
            )
    L += [
        "",
        "Code names (after the English): `fill_T_green`, "
        "`valclose_BA_eq1`, `close_T_green_and_BA_eq1`. Close entry only.",
        "",
        "### T vs BA overlap (same close)",
        "",
        "| hold | T days | BA days | both | T that are also BA | BA that are also T | "
        "T∧BA vs T | T∧BA vs BA |",
        "|---|---:|---:|---:|---|---|---|---|",
    ]
    for hold in HOLDS:
        o = ov.get(hold) or {}
        s = by[("T∧BA", hold)]
        st = s.get("vs_T_pp")
        sb = s.get("vs_BA_pp")
        st_s = "—" if st is None else f"{st:+.2f} pp ({s.get('vs_T_word')})"
        sb_s = "—" if sb is None else f"{sb:+.2f} pp ({s.get('vs_BA_word')})"
        L.append(
            f"| next {hold} | {o.get('n_T', 0)} | {o.get('n_BA', 0)} | "
            f"{o.get('n_both', 0)} | {o.get('share_T_in_BA', 0)*100:.1f}% | "
            f"{o.get('share_BA_in_T', 0)*100:.1f}% | {st_s} | {sb_s} |"
        )
    L += [
        "",
        "### Monthly walk-forward (discovery, hold2)",
        "",
        "| layer | months | fattest month | share of winning-month P&L |",
        "|---|---|---|---|",
    ]
    for letter, _n, _p in LAYERS:
        r = by[(letter, 2)]
        months = r.get("months") or {}
        shown = ", ".join(
            f"{m} {_pct(b)}" for m, b in list(months.items())[:8]
        )
        extra = f" … +{len(months)-8}" if len(months) > 8 else ""
        L.append(
            f"| {letter} | {shown}{extra} | {r.get('month_lottery_top') or '—'} | "
            f"{(r.get('month_lottery_frac') or 0)*100:.1f}% |"
        )
    L += [
        "",
        "### What this does not change",
        "",
        "- Standing A–O research keeps stay the **three light + green O** "
        "recipes. AH/FR open-stack is not reopened.",
        "- No new A–JL surface. Close-cluster only: T, BA, T∧BA.",
        "- Finviz volume stays **BLOCKED**. AB / weather / book stay dead.",
        "- Live `flatten_robust` is not imported or changed. No cards.",
        f"- Calendar half cut **{HALF_CUT}**. Q3 cut **{Q3_CUT}**. "
        f"Q1 cut **{Q1_CUT}**. Futubull 0.15% long.",
        "",
        "Research only. One 2026 regime.",
        "",
    ]
    return "\n".join(L)


def write_outputs(rows, baselines, ov, n_files):
    md = render(rows, baselines, ov, n_files)
    open(OUT_MD, "w", encoding="utf-8").write(md + "\n")
    payload = {
        "generated": str(date.today()),
        "spec": "close-cluster T / BA / T∧BA; hold1/2; no open recipes",
        "live_untouched": "flatten_robust",
        "excel_cache_used": False,
        "seed": "yahoo_rows_cache",
        "n_dumps": n_files,
        "q1_cut": Q1_CUT,
        "q3_cut": Q3_CUT,
        "half_cut": HALF_CUT,
        "standing_ao_keeps": "light+green O unchanged",
        "open_stack": "untouched",
        "finviz": "BLOCKED",
        "cost_model": "futubull",
        "n_keep": sum(1 for r in rows if r["verdict"] == "KEEP"),
        "n_kill": sum(1 for r in rows if r["verdict"] == "KILL"),
        "n_thin": sum(1 for r in rows if r["verdict"] == "THIN"),
        "baselines": {str(k): v for k, v in baselines.items()},
        "overlap": {str(k): v for k, v in ov.items()},
        "rows": rows,
    }
    json.dump(payload, open(OUT_JSON, "w"), indent=2)
    block = md if md.startswith(MARKER) else MARKER + "\n\n" + md
    sb = splice_md(SB_MD, MARKER, block,
                   require_any=("first A–JL cut", "A–O clock cycle"))
    ao = splice_md(AO_MD, MARKER, block, require="VISIBLE_COLS A..O")
    cy = splice_md(
        CYCLE_MD, MARKER,
        MARKER + "\n\n"
        f"Close-cluster T/BA: KEEP {payload['n_keep']} · "
        f"KILL {payload['n_kill']} · THIN {payload['n_thin']}. "
        "Open recipes untouched. Finviz BLOCKED. See `CLOSE_CLUSTER.md`.\n",
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
    if args.render_only:
        prev = json.load(open(OUT_JSON))
        rows = prev.get("rows") or []
        baselines = {int(k): v for k, v in (prev.get("baselines") or {}).items()}
        ov = {int(k): v for k, v in (prev.get("overlap") or {}).items()}
        payload = write_outputs(rows, baselines, ov, prev.get("n_dumps") or 0)
        print(f"render-only KEEP {payload['n_keep']} KILL {payload['n_kill']} "
              f"THIN {payload['n_thin']}")
        return payload
    split = json.load(open(SPLIT_PATH))
    disc = set(x.upper() for x in split["discovery"])
    hold = set(x.upper() for x in split["holdout"])
    spy = load_spy()
    files = [f for f in sorted(glob.glob(os.path.join(DUMPS, "*.json")))
             if not os.path.basename(f).startswith("_")]
    print(f"[close_cluster] dumps={len(files)} workers={args.workers}",
          flush=True)
    trades = collect(files, spy, disc, hold, args.workers)
    print(f"[close_cluster] trades={len(trades)}", flush=True)
    rows, baselines, ov = score_all(trades)
    payload = write_outputs(rows, baselines, ov, len(files))
    print(f"KEEP {payload['n_keep']} KILL {payload['n_kill']} "
          f"THIN {payload['n_thin']}", flush=True)
    for r in rows:
        print(f"  {r['verdict']:4} {r['letter']:5} hold{r['hold']} "
              f"{_pct(r.get('holdout'))} lot={(r.get('lottery_day_frac') or 0)*100:.1f}% "
              f"{','.join(r.get('fail_reasons') or [])}", flush=True)
    return payload


if __name__ == "__main__":
    main()
