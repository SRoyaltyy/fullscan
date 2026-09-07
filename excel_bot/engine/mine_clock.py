"""Clock-aware Excel-grid miner. Research only — does not emit live cards
and does not touch flatten_robust.

Universe : discovery / holdout from engine/holdout_split.json
Patterns : patterns.pattern_matrix() (card defs + new color/value/lag)
Holds    : sleeve-native 1/2/3/5/8 (clock.py hold_exit_idx)
Costs    : labeled `mcap_bps` (0.1%/0.3%) AND `futubull` (1.5%/2.0%)
Clock    : every cell carries clock=open|close
Tape     : early/late date split + SPY cc up/down when SPY rows exist
Ship bar : clock.SHIP (n, t, tickers, dates, lottery, both-tape)

  python engine/mine_clock.py --min-n 80 --workers 6
"""
from __future__ import annotations

import argparse
import glob
import json
import math
import os
import sys
from collections import defaultdict
from datetime import date, datetime, timedelta

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from clock import (  # noqa: E402
    SHIP, SLEEVE_HOLDS, annotate_days, futu_cost, mcap_cost, simulate_clock,
)
from cohort_analysis import cohorts_of, load_finviz  # noqa: E402
from patterns import detect_pattern, pattern_matrix  # noqa: E402

GRIDS_DIR = os.environ.get("GRIDS_DIR", "grids")
ROWS_DIR = os.environ.get("ROWS_DIR", "data/rows")
RESEARCH = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                        "..", "research")
SCOREBOARD = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                          "..", "..", "03_scoreboard")


def s2d(n):
    return (datetime(1899, 12, 30) + timedelta(days=float(n))).date()


def tstat(vals):
    n = len(vals)
    if n < 2:
        return float("nan")
    m = sum(vals) / n
    v = sum((x - m) ** 2 for x in vals) / (n - 1)
    return m / math.sqrt(v / n) if v > 0 else float("nan")


def _try_fetch_spy():
    """Best-effort SPY tape for regime split. Never fails the mine."""
    try:
        from fastfetch import fetch_daily_fast
        rows = fetch_daily_fast("SPY", date(2025, 12, 1), date.today())
        out = {}
        prev = None
        for r in rows:
            iso = r["date"].isoformat()
            if prev is not None:
                out[iso] = 1 if r["close"] > prev else -1
            prev = r["close"]
        os.makedirs(RESEARCH, exist_ok=True)
        json.dump([{"date": r["date"].isoformat(), "close": r["close"]}
                   for r in rows],
                  open(os.path.join(RESEARCH, "spy_tape.json"), "w"))
        return out
    except Exception as e:  # noqa: BLE001
        print(f"[spy] fetch skipped: {e}", flush=True)
        return {}


def load_spy_tape():
    """date iso -> +1 if SPY close>prior close else -1. Empty if unavailable."""
    for path in (os.path.join(ROWS_DIR, "SPY.json"),
                 os.path.join(GRIDS_DIR, "SPY.json"),
                 os.path.join(RESEARCH, "spy_tape.json")):
        if not os.path.exists(path):
            continue
        raw = json.load(open(path))
        rows = raw.get("days") or raw
        out = {}
        prev = None
        for r in rows:
            if isinstance(r.get("date"), (int, float)):
                d = str(s2d(r["date"]))
            else:
                d = str(r["date"])[:10]
            c = r.get("close")
            if c is None:
                continue
            if prev is not None:
                out[d] = 1 if c > prev else -1
            prev = c
        if out:
            return out
    return {}


def _init(discovery, holdout, fz, pats, spy, holds):
    _G["discovery"] = discovery
    _G["holdout"] = holdout
    _G["fz"] = fz
    _G["pats"] = pats
    _G["spy"] = spy
    _G["holds"] = holds


_G = {}


def work_grid(path):
    t = os.path.basename(path)[:-5]
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
    rec = _G["fz"].get(t)
    cohorts = ["ALL"] + (cohorts_of(rec) if rec else [])
    mc = rec["mcap"] if rec else None
    cost_m = mcap_cost(mc)
    trades = []
    for pat in _G["pats"]:
        clock = pat["clock"]
        try:
            clusters = detect_pattern(days, pat)
        except Exception:
            continue
        for c in clusters:
            for hold in _G["holds"]:
                rule = f"hold{hold}"
                raw = simulate_clock(days, c, clock, rule)
                if raw is None:
                    continue
                ei = c["entry_idx"]
                iso = str(s2d(days[ei]["date"]))
                tape = _G["spy"].get(iso, 0)
                half = "early" if iso < "2026-05-01" else "late"
                side = "long" if c["side"] == 1 else "short"
                trades.append({
                    "def": pat["name"], "clock": clock, "side": side,
                    "exit": rule, "split": split, "raw": raw,
                    "cost_mcap": cost_m, "cost_futu": futu_cost(c["side"]),
                    "ticker": t, "date": iso, "tape": tape, "half": half,
                    "cohorts": cohorts, "family": pat["kind"],
                })
    return trades


def _accum():
    # key -> lists
    return defaultdict(lambda: {
        "raw": [], "tickers": set(), "dates": set(),
        "early": [], "late": [], "spy_up": [], "spy_dn": [],
        "disc": [], "hold": [],
    })


def add_trade(cells, tr, cost_name, cost, cohort):
    key = (tr["def"], tr["clock"], tr["side"], tr["exit"], cohort, cost_name)
    cell = cells[key]
    net = tr["raw"] - cost
    cell["raw"].append(net)
    cell["tickers"].add(tr["ticker"])
    cell["dates"].add(tr["date"])
    cell[tr["half"]].append(net)
    if tr["tape"] == 1:
        cell["spy_up"].append(net)
    elif tr["tape"] == -1:
        cell["spy_dn"].append(net)
    if tr["split"] == "discovery":
        cell["disc"].append(net)
    else:
        cell["hold"].append(net)
    return cell


def lottery(vals):
    if not vals:
        return True, 1.0, float("nan")
    pos = [v for v in vals if v > 0]
    gross = sum(pos) if pos else 0.0
    mx = max(vals)
    frac = (mx / gross) if (gross > 0 and mx > 0) else 0.0
    # drop the single best trade
    if len(vals) >= 3:
        rest = list(vals)
        rest.remove(mx)
        trimmed = sum(rest) / len(rest)
    else:
        trimmed = float("nan")
    bad = frac > SHIP["max_trade_frac"] or (trimmed == trimmed and trimmed <= 0)
    return bad, frac, trimmed


def pack_cell(key, cell):
    dn, clock, side, rule, cohort, cost = key
    disc, hold = cell["disc"], cell["hold"]
    allv = cell["raw"]

    def blk(vals):
        if len(vals) < 2:
            return None
        m = sum(vals) / len(vals)
        return {"n": len(vals), "avg_net": m, "t": tstat(vals),
                "win": sum(1 for v in vals if v > 0) / len(vals)}

    d, h = blk(disc), blk(hold)
    lot_bad, lot_frac, trimmed = lottery(disc if disc else allv)
    early, late = blk(cell["early"]), blk(cell["late"])
    up, dn = blk(cell["spy_up"]), blk(cell["spy_dn"])
    tape_ok = True
    if early and late and early["n"] >= SHIP["min_tape_n"] and late["n"] >= SHIP["min_tape_n"]:
        tape_ok = (early["avg_net"] > 0) == (late["avg_net"] > 0) and late["avg_net"] > 0
    spy_ok = True
    if up and dn and up["n"] >= SHIP["min_tape_n"] and dn["n"] >= SHIP["min_tape_n"]:
        spy_ok = (up["avg_net"] > 0) and (dn["avg_net"] > 0)

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
    if len(cell["tickers"]) < SHIP["n_tickers"]:
        reasons.append("ticker_bar")
    if len(cell["dates"]) < SHIP["n_dates"]:
        reasons.append("date_bar")
    if lot_bad:
        reasons.append("lottery")
    if not tape_ok:
        reasons.append("tape_split")
    if not spy_ok:
        reasons.append("spy_regime")
    # Size-bar only → THIN. Any quality miss → FAIL. No reasons → PASS.
    SIZE = {"thin_disc", "thin_hold", "ticker_bar", "date_bar"}
    if not reasons:
        verdict = "PASS"
    elif set(reasons) <= SIZE:
        verdict = "THIN"
    else:
        verdict = "FAIL"
    return {
        "def": dn, "clock": clock, "side": side, "exit": rule,
        "cohort": cohort, "cost_model": cost,
        "n_tickers": len(cell["tickers"]), "n_dates": len(cell["dates"]),
        "discovery": d, "holdout": h,
        "early": early, "late": late, "spy_up": up, "spy_dn": dn,
        "lottery_frac": lot_frac, "trimmed_avg": trimmed,
        "verdict": verdict, "fail_reasons": reasons,
        "live_untouched": "flatten_robust",
    }


def apply_hold1_sibling(rows):
    """A hold1 PASS that has no hold2 PASS on the same cell is a one-day lottery."""
    passed = {(r["def"], r["clock"], r["side"], r["cohort"], r["cost_model"],
               r["exit"]) for r in rows if r["verdict"] == "PASS"}
    for r in rows:
        if r["verdict"] != "PASS" or r["exit"] != "hold1":
            continue
        sib = (r["def"], r["clock"], r["side"], r["cohort"], r["cost_model"],
               "hold2")
        if sib not in passed:
            r["verdict"] = "FAIL"
            r["fail_reasons"] = list(r["fail_reasons"]) + ["hold1_without_hold2"]
    return rows


def _row_line(r):
    d, h = r.get("discovery") or {}, r.get("holdout") or {}

    def fmt(b):
        if not b:
            return "—"
        return f"{b['n']}/{b['avg_net']*100:+.2f}%/{b['t']:.1f}"

    return (
        f"| {r['verdict']} | `{r['def']}` | {r['clock']} | {r['side']} | "
        f"{r['exit']} | {r['cohort']} | {r['cost_model']} | "
        f"{fmt(d)} | {fmt(h)} | {r['n_tickers']} | "
        f"{','.join(r.get('fail_reasons') or []) or '—'} |"
    )


def render_md(rows, n_grids, n_pats, spy_n, path):
    keep = [r for r in rows if r["verdict"] == "PASS"]
    fail = [r for r in rows if r["verdict"] == "FAIL"]
    thin = [r for r in rows if r["verdict"] == "THIN"]
    keep_all = [r for r in keep if r["cohort"] == "ALL"]
    L = [
        "# Excel emulator mine — clock-aware A–O cycle",
        "",
        f"_Generated {date.today()} · live `flatten_robust` is not changed. "
        f"No merge without Cyrus._",
        "",
        "## Ship bar",
        "",
        "PASS needs **all** of: discovery n≥300 t≥3 avg>0; holdout n≥100 "
        "t≥2 avg>0 same sign; ≥50 tickers; ≥20 entry dates; no single trade "
        ">25% of gross wins; trimmed mean (drop best trade) >0; early and "
        "late tape same sign when both n≥40; SPY-up and SPY-down both "
        "positive when both n≥40. Clock labeled on every row. Open entry "
        "only when the feature reads no close-knowable fill. hold1 only "
        "if hold2 also PASSes.",
        "",
        "THIN = size-bar only (n / tickers / dates). FAIL = any quality miss "
        "(sign, t, lottery, tape, SPY regime, hold1-without-hold2).",
        "",
        f"A–O grids mined: **{n_grids}**. Patterns: **{n_pats}**. "
        f"SPY tape days: **{spy_n}**. Cells scored (disc n≥80): **{len(rows)}**. "
        f"**PASS {len(keep)}** (ALL-cohort **{len(keep_all)}**) · "
        f"**FAIL {len(fail)}** · **THIN {len(thin)}**.",
        "",
        "All-cols N=25/35 sample stays research-only in `ALL_COLS_MINE.md`. "
        "Do **not** promote `IZ_eq1` / `deeper_g5` / `AD_ge1`.",
        "",
        "## PASS (keepers)",
        "",
    ]
    hdr = ("| verdict | def | clock | side | exit | cohort | cost | "
           "disc n/avg/t | hold n/avg/t | tickers | why |")
    sep = "|---|---|---|---|---|---|---|---|---|---:|---|"
    show = keep_all[:40] if keep_all else keep[:40]
    if not keep:
        L += ["*(none cleared the ship bar)*", ""]
    else:
        if keep_all and len(keep) > len(keep_all):
            L += [f"ALL-cohort keepers shown first ({len(keep_all)} of "
                  f"{len(keep)} PASS cells; rest are cohort slices).", ""]
        L += [hdr, sep]
        for r in show:
            L.append(_row_line(r))
        L.append("")
    near = [r for r in fail
            if r.get("discovery") and r["discovery"]["n"] >= 200
            and r.get("holdout") and r["holdout"].get("t") == r["holdout"].get("t")
            and r["holdout"]["t"] >= 1.5
            and r["discovery"]["avg_net"] > 0
            and r["cohort"] == "ALL"]
    near.sort(key=lambda r: -((r["holdout"] or {}).get("t") or 0))
    L += ["## FAIL (ALL-cohort, holdout t≥1.5, disc avg>0)", ""]
    if not near:
        L += ["*(none)*", ""]
    else:
        L += [hdr, sep]
        for r in near[:25]:
            L.append(_row_line(r))
        L.append("")
    L += ["## THIN (size-bar only, ALL-cohort, top by holdout t)", ""]
    thin_all = [r for r in thin if r["cohort"] == "ALL"]
    thin_all.sort(key=lambda r: -((r["holdout"] or {}).get("t") or -9))
    if not thin_all:
        L += ["*(none at ALL cohort)*", ""]
    else:
        L += [hdr, sep]
        for r in thin_all[:20]:
            L.append(_row_line(r))
        L.append("")
    L += [
        "## What this cycle mined",
        "",
        "- A–O fill grids rebuilt from excel-state Yahoo rows "
        "(`rebuild_grids.py`).",
        "- Existing card defs, A-keyed at **open** and again at **close**.",
        "- New `open_score` / `open_core` (no D,E,F,H,I,N fills).",
        "- Color combos and majority of open-knowable fills.",
        "- Lag combos: yesterday H/I/N + today's A (open clock).",
        "- Formula-state gates from stored OHLCV: gap, J, H.",
        "- Sleeve holds 1/2/3/5/8. Costs: `mcap_bps` and `futubull`.",
        "",
        "Deeper A–JL values are **not** in these grids. The timed "
        "`--all-cols` sample is accepted THIN in `ALL_COLS_MINE.md` "
        "and is not re-run here.",
        "",
        "Research only. No merge without Cyrus. Live flatten_robust untouched.",
        "",
    ]
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as fh:
        fh.write("\n".join(L) + "\n")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--min-n", type=int, default=80)
    ap.add_argument("--workers", type=int, default=min(6, os.cpu_count() or 2))
    ap.add_argument("--new-only", action="store_true")
    ap.add_argument("--holds", default="1,2,3,5,8")
    args = ap.parse_args()
    holds = tuple(int(x) for x in args.holds.split(","))
    assert all(h in SLEEVE_HOLDS for h in holds)

    split = json.load(open(os.path.join(
        os.path.dirname(os.path.abspath(__file__)), "holdout_split.json")))
    discovery, holdout = set(split["discovery"]), set(split["holdout"])
    fz = load_finviz()
    pats = pattern_matrix(include_existing=not args.new_only)
    spy = load_spy_tape()
    if not spy:
        spy = _try_fetch_spy()
    files = [f for f in sorted(glob.glob(os.path.join(GRIDS_DIR, "*.json")))
             if not os.path.basename(f).startswith("_")]
    print(f"[mine_clock] grids={len(files)} pats={len(pats)} "
          f"holds={holds} spy_days={len(spy)}", flush=True)

    cells = _accum()
    seen = 0
    if args.workers > 1 and files:
        from multiprocessing import Pool
        with Pool(args.workers, initializer=_init,
                  initargs=(discovery, holdout, fz, pats, spy, holds)) as pool:
            for i, trades in enumerate(pool.imap_unordered(work_grid, files,
                                                           chunksize=8), 1):
                if not trades:
                    continue
                seen += 1
                for tr in trades:
                    for cost_name, cost in (("mcap_bps", tr["cost_mcap"]),
                                            ("futubull", tr["cost_futu"])):
                        add_trade(cells, tr, cost_name, cost, "ALL")
                        for co in tr["cohorts"]:
                            if co == "ALL":
                                continue
                            add_trade(cells, tr, cost_name, cost, co)
                if i % 200 == 0:
                    print(f"  ... {i}/{len(files)}", flush=True)
    else:
        _init(discovery, holdout, fz, pats, spy, holds)
        for f in files:
            trades = work_grid(f)
            if not trades:
                continue
            seen += 1
            for tr in trades:
                for cost_name, cost in (("mcap_bps", tr["cost_mcap"]),
                                        ("futubull", tr["cost_futu"])):
                    add_trade(cells, tr, cost_name, cost, "ALL")
                    for co in tr["cohorts"]:
                        if co != "ALL":
                            add_trade(cells, tr, cost_name, cost, co)

    rows = []
    for key, cell in cells.items():
        if len(cell["disc"]) < args.min_n:
            continue
        rows.append(pack_cell(key, cell))
    rows = apply_hold1_sibling(rows)
    rank = {"PASS": 0, "FAIL": 1, "THIN": 2}
    rows.sort(key=lambda r: (
        rank.get(r["verdict"], 9),
        0 if r["cohort"] == "ALL" else 1,
        -((r["holdout"] or {}).get("t") or -9),
    ))

    os.makedirs(RESEARCH, exist_ok=True)
    n_pass = sum(1 for r in rows if r["verdict"] == "PASS")
    n_fail = sum(1 for r in rows if r["verdict"] == "FAIL")
    n_thin = sum(1 for r in rows if r["verdict"] == "THIN")
    payload = {
        "generated": str(date.today()),
        "spec": "clock-aware A-O excel_bot mine; live flatten_robust untouched",
        "ship": SHIP,
        "grids": seen,
        "patterns": len(pats),
        "spy_days": len(spy),
        "n_cells": len(rows),
        "n_pass": n_pass,
        "n_fail": n_fail,
        "n_thin": n_thin,
        "n_pass_all": sum(1 for r in rows if r["verdict"] == "PASS"
                          and r["cohort"] == "ALL"),
        "live_untouched": "flatten_robust",
        "all_cols_not_promoted": ["IZ_eq1", "deeper_g5", "AD_ge1"],
        "cells": rows,
    }
    jpath = os.path.join(RESEARCH, "mine_clock_results.json")
    json.dump(payload, open(jpath, "w"), indent=1)
    compact = dict(payload)
    compact["cells"] = [r for r in rows if r["verdict"] == "PASS"
                        or (r["cohort"] == "ALL" and r["verdict"] == "FAIL")]
    json.dump(compact, open(os.path.join(RESEARCH, "mine_clock_summary.json"),
                            "w"), indent=1)
    md = os.path.join(RESEARCH, "MINE_CYCLE.md")
    render_md(rows, seen, len(pats), len(spy), md)
    sb = os.path.join(SCOREBOARD, "EXCEL_BOT_MINE.md")
    render_md(rows, seen, len(pats), len(spy), sb)
    print(f"[done] cells={len(rows)} PASS={n_pass} FAIL={n_fail} "
          f"THIN={n_thin} ALL-PASS={payload['n_pass_all']} -> {jpath}")
    print(f"       {md}")
    print(f"       {sb}")


if __name__ == "__main__":
    main()
