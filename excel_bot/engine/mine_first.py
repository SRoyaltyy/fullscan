"""Recommended first mines on stored A–O grids (inventory 2026-09-07).

Standing order (no live wire):
  1. Open-knowable / a_score defs — never core_score at open.
  2. Close-entry: L3-like + non-TP low-vol holds; then S1/S2. L4/L5 deferred.
  3. A–JL full-col mine is phase 2 (expensive --all-cols rebuild). Pilot
     already THIN — see ALL_COLS_MINE.md.

Ship bar + beat unconditional same-clock/hold baseline by ≥20 bp.
hold3/5/8 also need hold2 short-horizon edge (not a bull-tape ride).
"""
from __future__ import annotations

import glob
import json
import math
import os
import sys
from collections import defaultdict
from datetime import date, datetime, timedelta

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from clock import (  # noqa: E402
    COST_FUTU_LONG, COST_FUTU_SHORT, SHIP, annotate_days, assert_clock_legal,
    feature_clock, simulate_clock,
)
from cohort_analysis import cohorts_of, load_finviz  # noqa: E402
from mine_clock import apply_horizon_sibling  # noqa: E402
from patterns import (  # noqa: E402
    detect_pattern, new_combo_defs, new_lag_defs, new_score_defs,
    new_value_defs,
)
from sweep import detect_def  # noqa: E402

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
REPO = os.path.dirname(ROOT)
GRIDS = os.environ.get("GRIDS_DIR", os.path.join(ROOT, "grids"))
SPLIT_PATH = os.path.join(HERE, "holdout_split.json")
HOLDS = (1, 2, 3, 5, 8)
KEEP = {
    "ALL",
    "mid(1-10B):all",
    "volM:low(<3%)",
    "volM:high(>8%)",
    "opt:Yes",
}

# (name, defn, clock, sides)
A_DEFS = [
    ("strict_A_ml1", {"kind": "strict", "key": "a", "min_len": 1}, "open", (1, -1)),
    ("strict_A_ml2", {"kind": "strict", "key": "a", "min_len": 2}, "open", (1, -1)),
    ("strict_A_ml3", {"kind": "strict", "key": "a", "min_len": 3}, "open", (1, -1)),
    ("tol2_A_ml2", {"kind": "tolerant", "key": "a", "tol": 2, "min_len": 2}, "open", (1, -1)),
    ("tol2_A_ml3", {"kind": "tolerant", "key": "a", "tol": 2, "min_len": 3}, "open", (1, -1)),
    ("tol3_A_ml3", {"kind": "tolerant", "key": "a", "tol": 3, "min_len": 3}, "open", (1, -1)),
]
A_CLOSE = [(n + "_closeclk", d, "close", s) for n, d, _, s in A_DEFS]
CLOSE_DEFS = [
    ("tol2_core_score_ml3",
     {"kind": "tolerant", "key": "core_score", "thresh": 0.5, "tol": 2, "min_len": 3},
     "close", (1,)),
    ("tol2_core_score_ml2",
     {"kind": "tolerant", "key": "core_score", "thresh": 0.5, "tol": 2, "min_len": 2},
     "close", (1,)),
    ("strict_A_ml1_sr", {"kind": "strict", "key": "a", "min_len": 1}, "close", (-1,)),
    ("tol2_core_score_ml3_sr",
     {"kind": "tolerant", "key": "core_score", "thresh": 0.5, "tol": 2, "min_len": 3},
     "close", (-1,)),
]
FOCUS = {
    ("tol2_core_score_ml3", "long", "hold2", "mid(1-10B):all"),
    ("tol2_core_score_ml3", "long", "hold2", "volM:low(<3%)"),
    ("tol2_core_score_ml3", "long", "hold3", "volM:low(<3%)"),
    ("tol2_core_score_ml3", "long", "hold5", "volM:low(<3%)"),
    ("tol2_core_score_ml3", "long", "hold8", "volM:low(<3%)"),
    ("tol2_core_score_ml2", "long", "hold2", "volM:low(<3%)"),
    ("strict_A_ml1_sr", "short", "hold1", "opt:Yes"),
    ("strict_A_ml1_sr", "short", "hold1", "volM:high(>8%)"),
    ("tol2_core_score_ml3_sr", "short", "hold1", "opt:Yes"),
    ("tol2_core_score_ml3_sr", "short", "hold1", "volM:high(>8%)"),
}


def first_mine_pats():
    """Open a_score surface + close L3/S1. Never core_score at open."""
    pats = []
    for name, defn, clock, sides in A_DEFS + A_CLOSE + CLOSE_DEFS:
        pats.append({"name": name, "defn": defn, "clock": clock, "sides": sides})
    seen = {p["name"] for p in pats}
    extra = []
    extra.extend(new_score_defs())
    extra.extend(new_combo_defs())
    extra.extend(new_lag_defs())
    extra.extend(new_value_defs())
    for p in extra:
        if p.get("clock") != "open":
            continue
        if p["name"] in seen:
            continue
        assert_clock_legal(p.get("feature_cols") or (), "open")
        kind = p.get("kind")
        if kind in ("strict", "tolerant", "hyst"):
            sides = (1, -1)
        else:
            hint = p.get("side_hint") or (1 if p.get("family") == "green" else -1)
            sides = (hint,)
        pats.append({"name": p["name"], "defn": p, "clock": "open", "sides": sides})
    for p in pats:
        if p["clock"] == "open" and p["defn"].get("key") == "core_score":
            raise ValueError(f"illegal open core_score: {p['name']}")
        if p["clock"] == "open":
            cols = p["defn"].get("feature_cols")
            if cols is None and p["defn"].get("key") in ("a", None, "open_score", "open_core"):
                cols = (0,) if p["defn"].get("key") == "a" else cols
            if cols:
                assert feature_clock(cols) == "open", p["name"]
    return pats


def s2d(n):
    return (datetime(1899, 12, 30) + timedelta(days=float(n))).date()


def load_spy():
    p = os.path.join(ROOT, "research", "spy_tape.json")
    if not os.path.exists(p):
        return {}
    raw = json.load(open(p))
    rows = raw if isinstance(raw, list) else raw.get("days") or []
    out, prev = {}, None
    for r in rows:
        iso = str(r["date"])[:10]
        c = r.get("close")
        if c is None:
            continue
        if prev is not None:
            out[iso] = 1 if c > prev else -1
        prev = c
    return out


def _slot():
    return [0, 0.0, 0.0, 0, -1e9, 0.0]


def _push(slot, net):
    slot[0] += 1
    slot[1] += net
    slot[2] += net * net
    if net > 0:
        slot[3] += 1
        slot[5] += net
    if net > slot[4]:
        slot[4] = net


def _blk(slot):
    n, s, sq, w, mx, pos = slot
    if n < 2:
        return None
    m = s / n
    var = max(sq - s * s / n, 0.0) / (n - 1)
    t = m / math.sqrt(var / n) if var > 0 else 0.0
    return {"n": n, "avg_net": m, "t": t, "win": w / n}


def lottery_stats(n, s, mx, pos):
    if n < 3:
        return True, 1.0, float("nan")
    frac = (mx / pos) if (pos > 0 and mx > 0) else 0.0
    trimmed = (s - mx) / (n - 1)
    return frac > SHIP["max_trade_frac"] or trimmed <= 0, frac, trimmed


def _clusters(days, defn):
    kind = defn.get("kind")
    if kind in ("combo", "combo_mix", "lag_combo", "value_gate"):
        return detect_pattern(days, defn)
    return detect_def(days, defn)


def score_rows(pats, files, disc, hold, fz, spy):
    cells = defaultdict(lambda: {
        "disc": _slot(), "hold": _slot(),
        "early": _slot(), "late": _slot(),
        "spy_up": _slot(), "spy_dn": _slot(),
        "tickers": set(), "dates": set(),
    })
    base = defaultdict(lambda: _slot())

    for i, path in enumerate(files, 1):
        t = os.path.basename(path)[:-5]
        split_t = ("discovery" if t in disc else "holdout" if t in hold else None)
        if split_t is None:
            continue
        try:
            days = annotate_days(json.load(open(path))["days"])
        except Exception:
            continue
        if len(days) < 30:
            continue
        rec = fz.get(t)
        cohorts = [c for c in (["ALL"] + (cohorts_of(rec) if rec else []))
                   if c in KEEP]
        last = len(days) - 1
        for ei, day in enumerate(days):
            iso = str(s2d(day["date"]))
            for clock, side in (("open", 1), ("close", 1),
                                ("open", -1), ("close", -1)):
                dummy = {"side": side, "entry_idx": ei, "exit_idx": last}
                for h in HOLDS:
                    raw = simulate_clock(days, dummy, clock, f"hold{h}")
                    if raw is None:
                        continue
                    cost = COST_FUTU_LONG if side == 1 else COST_FUTU_SHORT
                    _push(base[(clock, "long" if side == 1 else "short", h)],
                          raw - cost)
        for pat in pats:
            try:
                clusters = _clusters(days, pat["defn"])
            except Exception:
                continue
            clock, sides = pat["clock"], pat["sides"]
            for c in clusters:
                if c["side"] not in sides:
                    continue
                side_s = "long" if c["side"] == 1 else "short"
                for h in HOLDS:
                    raw = simulate_clock(days, c, clock, f"hold{h}")
                    if raw is None:
                        continue
                    cost = COST_FUTU_LONG if c["side"] == 1 else COST_FUTU_SHORT
                    net = raw - cost
                    ei = c["entry_idx"]
                    iso = str(s2d(days[ei]["date"]))
                    tape = spy.get(iso, 0)
                    half = "early" if iso < "2026-05-01" else "late"
                    for co in cohorts:
                        key = (pat["name"], clock, side_s, f"hold{h}", co)
                        cell = cells[key]
                        _push(cell["disc"] if split_t == "discovery" else cell["hold"], net)
                        _push(cell[half], net)
                        if tape == 1:
                            _push(cell["spy_up"], net)
                        elif tape == -1:
                            _push(cell["spy_dn"], net)
                        cell["tickers"].add(t)
                        cell["dates"].add(iso)
        if i % 500 == 0:
            print(f"  ... {i}/{len(files)}", flush=True)
    return cells, base


def verdict_row(name, clock, side, rule, co, cell, baselines):
    d, h = _blk(cell["disc"]), _blk(cell["hold"])
    if not d or d["n"] < 80:
        return None
    early, late = _blk(cell["early"]), _blk(cell["late"])
    up, dn = _blk(cell["spy_up"]), _blk(cell["spy_dn"])
    hn = int(rule[4:])
    b = baselines.get(f"{clock}_{side}_h{hn}")
    sl = cell["disc"]
    lot_bad, lot_frac, trimmed = lottery_stats(sl[0], sl[1], sl[4], sl[5])
    reasons = []
    if d["n"] < SHIP["disc_n"]:
        reasons.append("thin_disc")
    if not h or h["n"] < SHIP["hold_n"]:
        reasons.append("thin_hold")
    if d["t"] < SHIP["disc_t"]:
        reasons.append("disc_t")
    if h and h["t"] < SHIP["hold_t"]:
        reasons.append("hold_t")
    if d["avg_net"] <= 0:
        reasons.append("disc_sign")
    if h and h["avg_net"] <= 0:
        reasons.append("hold_sign")
    if len(cell["tickers"]) < SHIP["n_tickers"]:
        reasons.append("ticker_bar")
    if len(cell["dates"]) < SHIP["n_dates"]:
        reasons.append("date_bar")
    if lot_bad:
        reasons.append("lottery")
    if early and late and early["n"] >= 40 and late["n"] >= 40:
        if late["avg_net"] <= 0 or (early["avg_net"] > 0) != (late["avg_net"] > 0):
            reasons.append("tape_split")
    if up and dn and up["n"] >= 40 and dn["n"] >= 40:
        if up["avg_net"] <= 0 or dn["avg_net"] <= 0:
            reasons.append("spy_regime")
    cmp = h if h else d
    if b and cmp and cmp["avg_net"] < b["avg_net"] + 0.002:
        reasons.append("no_edge_vs_uncond")
    n_t = len(cell["tickers"])
    if n_t < 50 or (d and d["n"] < 80):
        verdict = "THIN"
    elif not reasons:
        verdict = "PASS"
    else:
        verdict = "FAIL"
    return {
        "def": name, "clock": clock, "side": side, "exit": rule,
        "cohort": co, "cost_model": "futubull",
        "n_tickers": n_t, "n_dates": len(cell["dates"]),
        "discovery": d, "holdout": h, "early": early, "late": late,
        "spy_up": up, "spy_dn": dn,
        "baseline": b, "lottery_frac": lot_frac, "trimmed_avg": trimmed,
        "verdict": verdict, "fail_reasons": reasons,
        "focus": (name, side, rule, co) in FOCUS,
        "live_untouched": "flatten_robust",
    }


def main():
    split = json.load(open(SPLIT_PATH))
    disc, hold = set(split["discovery"]), set(split["holdout"])
    fz = load_finviz()
    spy = load_spy()
    pats = first_mine_pats()
    files = [f for f in sorted(glob.glob(os.path.join(GRIDS, "*.json")))
             if not os.path.basename(f).startswith("_")]
    print(f"[mine_first] grids={len(files)} pats={len(pats)} spy={len(spy)}",
          flush=True)
    cells, base = score_rows(pats, files, disc, hold, fz, spy)
    baselines = {}
    for k, sl in base.items():
        b = _blk(sl)
        if b:
            baselines[f"{k[0]}_{k[1]}_h{k[2]}"] = b
    rows = []
    for (name, clock, side, rule, co), cell in cells.items():
        row = verdict_row(name, clock, side, rule, co, cell, baselines)
        if row:
            rows.append(row)
    apply_horizon_sibling(rows)
    rows.sort(key=lambda r: (
        0 if r["verdict"] == "PASS" else 1 if r["verdict"] == "FAIL" else 2,
        0 if r["focus"] else 1,
        -((r["holdout"] or {}).get("t") or -9),
    ))
    return rows, baselines, len(files), len(pats)


def fmt_blk(b):
    if not b:
        return "—"
    return f"{b['n']}/{b['avg_net']*100:+.2f}%/t={b['t']:.1f}"


def _table(rows, cols_hdr, cols_sep, line_fn, empty="*(none)*"):
    if not rows:
        return [empty, ""]
    out = [cols_hdr, cols_sep]
    for r in rows:
        out.append(line_fn(r))
    out.append("")
    return out


def render(rows, baselines, n_grids, n_pats):
    n_pass = sum(1 for r in rows if r["verdict"] == "PASS")
    n_fail = sum(1 for r in rows if r["verdict"] == "FAIL")
    n_thin = sum(1 for r in rows if r["verdict"] == "THIN")
    L = [
        "# Excel A–O first mine (inventory plan)",
        "",
        f"_Generated {date.today()} · live `flatten_robust` is not changed. "
        f"No merge without Cyrus._",
        "",
        "## Inventory (folded)",
        "",
        "- **VISIBLE_COLS A..O (15)** = what daily/backtest grids store "
        "(~139 days × OHLCV + 15 fills). Rebuilt this cycle: "
        f"**{n_grids}** from excel-state `state/rows.tar.gz`. "
        "Daily `done_grids` is ~3445; excel-state itself has **no** grid JSON.",
        "- **ALL_COLS A..JL (275)** via `run.py --all-cols` exists (#139) but "
        "is **not** used by the daily bot. `signal_colors` in suggestions = "
        "A→O strip only.",
        "- PIT: SOD/open A,B,C,G,J,K,L,M,O · close D,E,F,H,I,N. "
        "`core_score` needs **CLOSE** entry. G/K/M *values* are close-knowable "
        "even when the fill clock is open.",
        "- Cards L1–L5 / S1–S2 holdout PASS historically. Live 2026-09-05: "
        "L1/L2 −0.55%, L3 +0.35%, L5 −4%. **L4/L5 deferred.**",
        "- Phase-2 A–JL: 35-ticker lean pilot **0 PASS / 0 FAIL / 90 THIN** "
        "(`research/ALL_COLS_MINE.md`). Full 3603 rebuild stays scheduled "
        "after this A–O surface is exhausted.",
        "",
        "## This cycle (recommended order)",
        "",
        "1. Open-knowable **a_score / open_score / open_core / combos / "
        "lags / gap-J** (never `core_score` at open).",
        "2. Close-entry **L3-like** + non-TP low-vol holds; then **S1/S2**.",
        "3. Beat unconditional same-clock/hold baseline by **≥20 bps** "
        "or FAIL `no_edge_vs_uncond`.",
        "4. hold3/5/8 also need hold2 short-horizon edge or FAIL "
        "`long_hold_without_hold2`.",
        "",
        f"Grids **{n_grids}**. Patterns **{n_pats}**. Cells **{len(rows)}**. "
        f"**PASS {n_pass}** · **FAIL {n_fail}** · **THIN {n_thin}**. "
        "Cost = futubull 0.15%/0.20%.",
        "",
        "### Unconditional baseline (futubull)",
        "",
        "| clock | side | hold | n | avg net | t | win |",
        "|---|---|---:|---:|---:|---:|---:|",
    ]
    for clock in ("open", "close"):
        for side in ("long", "short"):
            for h in HOLDS:
                b = baselines.get(f"{clock}_{side}_h{h}")
                if not b:
                    continue
                L.append(f"| {clock} | {side} | {h} | {b['n']} | "
                         f"{b['avg_net']*100:+.2f}% | {b['t']:.1f} | "
                         f"{b['win']:.0%} |")
    L += ["", "### Focus refresh (L3 / low-vol holds / S1–S2)", ""]
    focus = [r for r in rows if r["focus"]]
    L += [
        "| verdict | def | clock | side | exit | cohort | disc | hold | "
        "base | tickers | why |",
        "|---|---|---|---|---|---|---|---|---|---:|---|",
    ]
    if not focus:
        L.append("| — | *(none scored)* | | | | | | | | | |")
    for r in focus:
        L.append(
            f"| {r['verdict']} | `{r['def']}` | {r['clock']} | {r['side']} | "
            f"{r['exit']} | {r['cohort']} | {fmt_blk(r['discovery'])} | "
            f"{fmt_blk(r['holdout'])} | {fmt_blk(r['baseline'])} | "
            f"{r['n_tickers']} | {','.join(r['fail_reasons']) or '—'} |"
        )
    keep = [r for r in rows if r["verdict"] == "PASS"]
    primary = [r for r in keep
               if r["cohort"] == "ALL" and r["exit"] in ("hold1", "hold2")]
    L += [
        "",
        "### Primary (ALL × hold1/2 only — not cohort/hold8 slices)",
        "",
        f"**{len(keep)}** PASS cells include overlapping cohort × hold "
        "slices of the same defs. The honest sleeve-shaped set is "
        f"**{len(primary)}** ALL-cohort hold1/2 rows. hold8 / opt / "
        "hi-vol slices are not independent edges. Research candidates "
        "only — one Jan–Sep 2026 regime, overlapping cluster days, "
        "no cards.",
        "",
    ]
    L += _table(
        primary,
        "| def | clock | side | exit | hold | base | tickers |",
        "|---|---|---|---|---|---|---:|",
        lambda r: (
            f"| `{r['def']}` | {r['clock']} | {r['side']} | {r['exit']} | "
            f"{fmt_blk(r['holdout'])} | {fmt_blk(r['baseline'])} | "
            f"{r['n_tickers']} |"
        ),
    )
    L += ["", "### Keepers (ship bar + hold2 sibling + beat baseline)", ""]
    L += _table(
        keep[:25],
        "| def | clock | side | exit | cohort | disc | hold | base | tickers |",
        "|---|---|---|---|---|---|---|---|---:|",
        lambda r: (
            f"| `{r['def']}` | {r['clock']} | {r['side']} | {r['exit']} | "
            f"{r['cohort']} | {fmt_blk(r['discovery'])} | "
            f"{fmt_blk(r['holdout'])} | {fmt_blk(r['baseline'])} | "
            f"{r['n_tickers']} |"
        ),
    )
    near = [r for r in rows if r["verdict"] == "FAIL"
            and r.get("holdout") and r["holdout"]["t"] >= 2
            and r["discovery"]["avg_net"] > 0
            and "no_edge_vs_uncond" not in r["fail_reasons"]
            and "long_hold_without_hold2" not in r["fail_reasons"]]
    L += ["### Near-miss (holdout t≥2, still FAIL, not baseline / tape-ride)", ""]
    L += _table(
        near[:15],
        "| def | clock | side | exit | cohort | hold t | why |",
        "|---|---|---|---|---|---:|---|",
        lambda r: (
            f"| `{r['def']}` | {r['clock']} | {r['side']} | {r['exit']} | "
            f"{r['cohort']} | {r['holdout']['t']:.1f} | "
            f"{','.join(r['fail_reasons'])} |"
        ),
    )
    demoted = [r for r in rows
               if "long_hold_without_hold2" in (r.get("fail_reasons") or [])]
    L += [
        f"hold5/8 demoted as tape-rides (`long_hold_without_hold2`): "
        f"**{len(demoted)}** cells. Not keepers.",
        "",
        "A–JL full rebuild stays phase 2. Live frozen. No strategy cards emitted.",
        "",
    ]
    slim_row = lambda r: {k: r[k] for k in (
        "def", "clock", "side", "exit", "cohort", "n_tickers", "n_dates",
        "discovery", "holdout", "baseline", "verdict", "fail_reasons",
        "focus", "live_untouched",
    ) if k in r}
    return "\n".join(L) + "\n", {
        "generated": str(date.today()),
        "n_grids": n_grids,
        "n_pats": n_pats,
        "n_pass": n_pass, "n_fail": n_fail, "n_thin": n_thin,
        "n_demoted_long_hold": len(demoted),
        "baselines": baselines,
        "focus": [slim_row(r) for r in focus],
        "keepers": [slim_row(r) for r in keep],
        "primary": [slim_row(r) for r in primary],
        "near": [slim_row(r) for r in near[:40]],
        "live_untouched": "flatten_robust",
    }


def render_cycle(rows, baselines, n_grids, n_pats, ao_md):
    n_pass = sum(1 for r in rows if r["verdict"] == "PASS")
    n_fail = sum(1 for r in rows if r["verdict"] == "FAIL")
    n_thin = sum(1 for r in rows if r["verdict"] == "THIN")
    return "\n".join([
        "# Excel emulator mine — standing cycle",
        "",
        f"_Generated {date.today()} · live `flatten_robust` is not changed. "
        f"No merge without Cyrus._",
        "",
        "## Inventory → mine plan",
        "",
        "| surface | what | this cycle |",
        "|---|---|---|",
        "| VISIBLE A..O (15) | daily/backtest grids, `signal_colors`, "
        f"~139d OHLCV+fills; excel-state = rows only | **mined** "
        f"({n_grids} rebuilt; done_grids ~3445) |",
        "| ALL_COLS A..JL (275) | `run.py --all-cols`, not daily | "
        "phase 2 — 35-ticker pilot **THIN 90** |",
        "| PIT | open A,B,C,G,J,K,L,M,O · close D,E,F,H,I,N · "
        "`core_score` = close | enforced |",
        "| live 2026-09-05 | L1/L2 −0.55%, L3 +0.35%, L5 −4% | "
        "refresh L3 then S1/S2; defer L4/L5 |",
        "",
        "## Ship bar",
        "",
        "PASS needs **all** of: discovery n≥300 t≥3 avg>0; holdout n≥100 "
        "t≥2 avg>0 same sign; ≥50 tickers; ≥20 entry dates; no single trade "
        ">25% of gross wins; trimmed mean (drop best trade) >0; early and "
        "late tape same sign when both n≥40; SPY-up and SPY-down both "
        "positive when both n≥40; hold1 must also PASS hold2; hold3/5/8 "
        "need hold2 short-horizon edge vs uncond; beat uncond by ≥20 bp. "
        "Open entry only when the feature reads no close-knowable fill.",
        "",
        f"A–O first mine: patterns **{n_pats}** · cells **{len(rows)}** · "
        f"**PASS {n_pass}** · **FAIL {n_fail}** · **THIN {n_thin}**.",
        "",
        "Full table: `AO_FIRST_MINE.md` / `03_scoreboard/EXCEL_BOT_MINE.md`. "
        "All-cols pilot: `ALL_COLS_MINE.md`.",
        "",
        "Research only. No merge without Cyrus. Live flatten_robust untouched.",
        "",
    ]) + "\n"


if __name__ == "__main__":
    os.chdir(ROOT)
    rows, baselines, n, n_pats = main()
    md, payload = render(rows, baselines, n, n_pats)
    cycle = render_cycle(rows, baselines, n, n_pats, md)
    paths = [
        (os.path.join(ROOT, "research", "AO_FIRST_MINE.md"), md),
        (os.path.join(REPO, "03_scoreboard", "EXCEL_BOT_MINE.md"), md),
        (os.path.join(ROOT, "research", "MINE_CYCLE.md"), cycle),
    ]
    for p, text in paths:
        os.makedirs(os.path.dirname(p), exist_ok=True)
        open(p, "w").write(text)
    json.dump(payload, open(os.path.join(ROOT, "research",
                                         "ao_first_mine.json"), "w"), indent=1)
    print(f"PASS={payload['n_pass']} FAIL={payload['n_fail']} "
          f"THIN={payload['n_thin']} demoted={payload['n_demoted_long_hold']} "
          f"-> research/AO_FIRST_MINE.md")
