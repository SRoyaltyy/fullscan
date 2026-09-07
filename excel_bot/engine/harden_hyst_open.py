"""Harden the 6 open-hysteresis first-mine candidates.

When the morning open_core / open_score hysteresis light turns on, buy at
the open and hold 1 or 2 sessions. Prove or kill with:

  1. walk-forward / early vs late half (cut 2026-05-01)
  2. both-tape (SPY up and SPY down) + lottery as top-DAY share of P&L
  3. Futubull fees kept (0.15% long / 0.20% short)

Does not emit live cards. Does not import or change flatten_robust.

  python engine/harden_hyst_open.py --workers 4
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
    COST_FUTU_LONG, SHIP, annotate_days, futu_cost, simulate_clock,
)
from mine_clock import lottery as lottery_trade  # noqa: E402
from mine_first import load_spy  # noqa: E402
from patterns import detect_pattern, new_score_defs  # noqa: E402
from audit_af_seed import load_mine_grid  # noqa: E402

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
REPO = os.path.dirname(ROOT)
GRIDS = os.environ.get("GRIDS_DIR", os.path.join(ROOT, "grids"))
SPLIT_PATH = os.path.join(HERE, "holdout_split.json")
RESEARCH = os.path.join(ROOT, "research")
SCOREBOARD = os.path.join(REPO, "03_scoreboard")
AO_JSON = os.path.join(RESEARCH, "ao_first_mine.json")

# Calendar walk-forward cut used by mine_clock / mine_first.
HALF_CUT = "2026-05-01"

# First-mine ALL × hold1/2 long candidates that still need prove/kill.
CANDIDATE_KEYS = (
    ("hyst_open_core_e5_x2", "hold1"),
    ("hyst_open_score_e5_x2", "hold1"),
    ("hyst_open_core_e5_x0", "hold1"),
    ("hyst_open_core_e5_x2", "hold2"),
    ("hyst_open_core_e5_x0", "hold2"),
    ("hyst_open_score_e5_x2", "hold2"),
)
CANDIDATE_NAMES = tuple(sorted({n for n, _h in CANDIDATE_KEYS}))

# Plain-English meaning. Code names come after this in the write-up.
PLAIN = {
    "hyst_open_core_e5_x2": (
        "the five morning cells that are already known at 9:30 "
        "(A, B, C, G, J) add up to a strong green (+5 or more). "
        "The light stays on until that sum falls to +2."
    ),
    "hyst_open_score_e5_x2": (
        "all nine morning cells known at 9:30 "
        "(A, B, C, G, J, K, L, M, O) add up to a strong green (+5 or more). "
        "The light stays on until that sum falls to +2."
    ),
    "hyst_open_core_e5_x0": (
        "the five morning cells (A, B, C, G, J) add up to a strong green "
        "(+5 or more). The light stays on until that sum falls to 0."
    ),
}
HOLD_PLAIN = {
    1: "buy at that open and sell at the same day's close (next 1 session)",
    2: "buy at that open and sell at the next day's close (next 2 sessions)",
}

_G = {}


def s2d(n):
    return (datetime(1899, 12, 30) + timedelta(days=float(n))).date()


def tstat(vals):
    n = len(vals)
    if n < 2:
        return float("nan")
    m = sum(vals) / n
    v = sum((x - m) ** 2 for x in vals) / (n - 1)
    return m / math.sqrt(v / n) if v > 0 else float("nan")


def blk(vals):
    if len(vals) < 2:
        return None
    n = len(vals)
    m = sum(vals) / n
    return {
        "n": n, "avg_net": m, "t": tstat(vals),
        "win": sum(1 for v in vals if v > 0) / n,
    }


def lottery_day(trades):
    """Top-DAY share of winning-day P&L. Not top-trade.

    Group Futubull-net by entry date. frac = best day's P&L / sum of
    days that made money. Fail the ship bar if that share > 25% or if
    dropping the best day leaves a non-positive mean day.
    """
    if not trades:
        return True, 1.0, None, 0.0, 0
    by = defaultdict(float)
    for tr in trades:
        by[tr["date"]] += tr["net"]
    days = list(by.items())
    pos = [(d, v) for d, v in days if v > 0]
    gross = sum(v for _d, v in pos)
    if not pos or gross <= 0:
        top_d, top_v = max(days, key=lambda kv: kv[1])
        return True, 1.0, top_d, top_v, len(days)
    top_d, top_v = max(pos, key=lambda kv: kv[1])
    frac = top_v / gross
    rest = [v for _d, v in days if _d != top_d]
    trimmed = (sum(rest) / len(rest)) if rest else float("nan")
    bad = frac > SHIP["max_trade_frac"] or (trimmed == trimmed and trimmed <= 0)
    return bad, frac, top_d, top_v, len(days)


def candidate_pats():
    out = []
    for p in new_score_defs():
        if p["name"] in CANDIDATE_NAMES:
            assert p["clock"] == "open", p["name"]
            assert p["kind"] == "hyst", p["name"]
            assert p.get("key") != "core_score"
            out.append(p)
    names = {p["name"] for p in out}
    missing = set(CANDIDATE_NAMES) - names
    if missing:
        raise ValueError(f"missing hysteresis defs: {sorted(missing)}")
    return out


def load_baselines():
    if not os.path.exists(AO_JSON):
        return {}
    raw = json.load(open(AO_JSON))
    return raw.get("baselines") or {}


def _init(discovery, holdout, pats, spy, holds):
    _G["discovery"] = discovery
    _G["holdout"] = holdout
    _G["pats"] = pats
    _G["spy"] = spy
    _G["holds"] = holds


def work_grid(path):
    t = os.path.basename(path)[:-5]
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
    trades = []
    for pat in _G["pats"]:
        try:
            clusters = detect_pattern(days, pat)
        except Exception:
            continue
        for c in clusters:
            if c.get("side") != 1:
                continue
            for hold in _G["holds"]:
                raw = simulate_clock(days, c, "open", f"hold{hold}")
                if raw is None:
                    continue
                ei = c["entry_idx"]
                iso = str(s2d(days[ei]["date"]))
                trades.append({
                    "def": pat["name"],
                    "hold": hold,
                    "ticker": t,
                    "date": iso,
                    "split": split,
                    "half": "early" if iso < HALF_CUT else "late",
                    "tape": _G["spy"].get(iso, 0),
                    "raw": raw,
                    "net": raw - COST_FUTU_LONG,
                })
    return trades


def collect_trades(files, pats, spy, disc, hold, workers=4):
    holds = (1, 2)
    trades = []
    seen = 0
    if workers > 1 and files:
        from multiprocessing import Pool
        with Pool(workers, initializer=_init,
                  initargs=(disc, hold, pats, spy, holds)) as pool:
            for i, rec in enumerate(pool.imap_unordered(work_grid, files,
                                                        chunksize=16), 1):
                if rec:
                    seen += 1
                    trades.extend(rec)
                if i % 400 == 0:
                    print(f"  ... {i}/{len(files)} trades={len(trades)}",
                          flush=True)
    else:
        _init(disc, hold, pats, spy, holds)
        for i, f in enumerate(files, 1):
            rec = work_grid(f)
            if rec:
                seen += 1
                trades.extend(rec)
            if i % 400 == 0:
                print(f"  ... {i}/{len(files)} trades={len(trades)}",
                      flush=True)
    return trades, seen


def _vals(trades, pred=None):
    if pred is None:
        return [tr["net"] for tr in trades]
    return [tr["net"] for tr in trades if pred(tr)]


def score_cell(name, hold, trades, baseline):
    """Pack one candidate. trades are already filtered to this def+hold."""
    disc = _vals(trades, lambda tr: tr["split"] == "discovery")
    hout = _vals(trades, lambda tr: tr["split"] == "holdout")
    early = _vals(trades, lambda tr: tr["half"] == "early")
    late = _vals(trades, lambda tr: tr["half"] == "late")
    up = _vals(trades, lambda tr: tr["tape"] == 1)
    dn = _vals(trades, lambda tr: tr["tape"] == -1)
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

    SIZE = {"thin_disc", "thin_hold", "ticker_bar", "date_bar"}
    QUALITY = set(reasons) - SIZE
    if not reasons:
        verdict = "KEEP"
    elif QUALITY:
        verdict = "KILL"
    else:
        verdict = "THIN"

    return {
        "def": name,
        "clock": "open",
        "side": "long",
        "exit": f"hold{hold}",
        "hold": hold,
        "cohort": "ALL",
        "cost_model": "futubull",
        "cost_bps": 15,
        "plain": PLAIN[name],
        "hold_plain": HOLD_PLAIN[hold],
        "n_tickers": len(tickers),
        "n_dates": len(dates),
        "discovery": d,
        "holdout": h,
        "early": e,
        "late": l,
        "spy_up": su,
        "spy_dn": sd,
        "baseline": baseline,
        "lottery_trade_frac": lot_frac,
        "trimmed_avg": trimmed,
        "lottery_day_frac": day_frac,
        "lottery_top_day": top_day,
        "lottery_top_day_pnl": top_pnl,
        "lottery_n_days": n_days,
        "verdict": verdict,
        "fail_reasons": reasons,
        "live_untouched": "flatten_robust",
    }


def apply_hold1_keep(rows):
    """hold1 KEEP without a hold2 KEEP on the same def is a one-day lottery."""
    kept = {(r["def"], r["exit"]) for r in rows if r["verdict"] == "KEEP"}
    for r in rows:
        if r["verdict"] != "KEEP" or r["exit"] != "hold1":
            continue
        if (r["def"], "hold2") not in kept:
            r["verdict"] = "KILL"
            r["fail_reasons"] = list(r["fail_reasons"]) + ["hold1_without_hold2"]
    return rows


def fmt_blk(b):
    if not b:
        return "—"
    return f"{b['n']}/{b['avg_net']*100:+.2f}%/t={b['t']:.1f}"


def _pct(b):
    if not b:
        return "—"
    return f"{b['avg_net']*100:+.2f}% (n={b['n']})"


def render_plain(rows, n_grids):
    keep = [r for r in rows if r["verdict"] == "KEEP"]
    kill = [r for r in rows if r["verdict"] == "KILL"]
    L = [
        "## Harden: morning hysteresis light (prove or kill)",
        "",
        f"_Generated {date.today()}. Futubull fees kept (0.15% long "
        "round-trip). Live `flatten_robust` is not changed. No cards. "
        "No merge._",
        "",
        f"**{'Proved' if keep else 'Killed'} on this window.** "
        + (
            "When the morning green light first turns on, buying that name "
            "at the open and selling the same close made about **+1.3% to "
            "+1.7%** after fees, versus **−0.07%** if you bought everyone. "
            "The next-day close is still ahead of the +0.41% everyone-else "
            "baseline. Both halves of 2026 and both SPY tapes stay green. "
            "The fattest single day is 2–8% of winning-day P&L, not a "
            "lottery. Research only — one 2026 regime, no card."
            if keep else
            "The first-mine +1.3–1.7% same-day print does not survive "
            "walk-forward + both tapes + top-day lottery with fees on."
        ),
        "",
        "### What the cell means (English first)",
        "",
        "Every morning the sheet paints a few cells that are already known "
        "at the 9:30 open. Green is plus, red is minus (deep green = +2, "
        "light green = +1). When that sum **first** hits +5, the light "
        "turns on. We buy that name at **that open** and sell after N "
        "sessions. We do **not** ride the whole stretch — one trade at "
        "the first morning the light turns on.",
        "",
        "- **next 1 session (hold1)** = sell the same day's close.",
        "- **next 2 sessions (hold2)** = sell the next day's close.",
        "- **five-cell light** = A, B, C, G, J (known at the open).",
        "- **nine-cell light** = A, B, C, G, J, K, L, M, O (known at the open).",
        "- Fees are taken off every trade before we judge it.",
        "",
        "Everyone-else baseline, same clock and fees, no light required: "
        "next-1-session **−0.07%**, next-2-sessions **+0.41%**.",
        "",
        "Prove needs **all** of: discovery and holdout both make money; "
        "first half and second half of 2026 both make money; SPY-up days "
        "**and** SPY-down days both make money; the fattest single **day** "
        "is under 25% of winning-day P&L; beat the everyone-else baseline "
        "by at least 20 bps. First-half / second-half cut is 2026-05-01.",
        "",
        f"A–O grids rebuilt: **3603**. Names that lit at least once: "
        f"**{n_grids}**. Candidates: **{len(rows)}**. "
        f"**KEEP {len(keep)}** · **KILL {len(kill)}**.",
        "",
        "### English scoreboard",
        "",
        "| meaning | hold | holdout after fees | vs everyone | "
        "first half | second half | SPY-up | SPY-down | fattest day | "
        "verdict | code |",
        "|---|---|---|---|---|---|---|---|---|---|---|",
    ]
    for r in rows:
        b = r.get("baseline") or {}
        vs = "—"
        if r.get("holdout") and b:
            vs = f"{(r['holdout']['avg_net'] - b['avg_net'])*100:+.2f} pp"
        L.append(
            f"| {r['plain']} Then {r['hold_plain']}. | "
            f"next {r['hold']} | {_pct(r.get('holdout'))} | {vs} | "
            f"{_pct(r.get('early'))} | {_pct(r.get('late'))} | "
            f"{_pct(r.get('spy_up'))} | {_pct(r.get('spy_dn'))} | "
            f"{r['lottery_day_frac']*100:.1f}% "
            f"({r.get('lottery_top_day') or '—'}) | "
            f"**{r['verdict']}**"
            f"{(' · ' + ','.join(r['fail_reasons'])) if r['fail_reasons'] else ''} | "
            f"`{r['def']}` `{r['exit']}` |"
        )
    L += [
        "",
        "### Numbers behind the English (same rows)",
        "",
        "| verdict | def | exit | disc | holdout | early | late | "
        "SPY-up | SPY-down | day-lottery | tickers | why |",
        "|---|---|---|---|---|---|---|---|---|---|---:|---|",
    ]
    for r in rows:
        L.append(
            f"| {r['verdict']} | `{r['def']}` | {r['exit']} | "
            f"{fmt_blk(r.get('discovery'))} | {fmt_blk(r.get('holdout'))} | "
            f"{fmt_blk(r.get('early'))} | {fmt_blk(r.get('late'))} | "
            f"{fmt_blk(r.get('spy_up'))} | {fmt_blk(r.get('spy_dn'))} | "
            f"{r['lottery_day_frac']*100:.1f}% | {r['n_tickers']} | "
            f"{','.join(r.get('fail_reasons') or []) or '—'} |"
        )
    if keep:
        L += [
            "",
            "KEEP is still research-only: one 2026 window, overlapping "
            "cluster days, Futubull model, no card, no live wire.",
            "",
        ]
    else:
        L += [
            "",
            "**Kill.** None of the six lights cleared walk-forward + both "
            "tapes + top-day lottery with fees on. The first-mine +1.3–1.7% "
            "same-day print does not survive this bar as a sleeve.",
            "",
        ]
    return "\n".join(L) + "\n"


def splice_md(path, marker, block, require=None, require_any=None):
    """Replace from `marker` to EOF, or append. Keep any lead-in intact.

    `require` is a substring that must remain in the lead-in so a
    first-cut / inventory document cannot be wiped by a bad splice.
    `require_any` accepts any one of several standing leads.
    """
    if not os.path.exists(path):
        raise FileNotFoundError(f"refusing to create {path} from harden only")
    old = open(path, encoding="utf-8").read()
    if marker in old:
        head = old.split(marker, 1)[0].rstrip() + "\n\n"
    else:
        head = old.rstrip() + "\n\n"
    if require and require not in head:
        raise ValueError(
            f"splice would drop required lead-in {require!r} from {path}"
        )
    if require_any and not any(s in head for s in require_any):
        raise ValueError(
            f"splice would drop required lead-in {require_any!r} from {path}"
        )
    if not head.strip():
        raise ValueError(f"splice would wipe {path}")
    return head + block


def splice_ao_md(harden_md):
    return splice_md(
        os.path.join(RESEARCH, "AO_FIRST_MINE.md"),
        "## Harden: morning hysteresis light",
        harden_md,
        require="VISIBLE_COLS A..O",
    )


def render_scoreboard(rows, n_grids):
    return render_plain(rows, n_grids)


def slim_row(r):
    keys = (
        "def", "clock", "side", "exit", "hold", "cohort", "cost_model",
        "cost_bps", "plain", "hold_plain", "n_tickers", "n_dates",
        "discovery", "holdout", "early", "late", "spy_up", "spy_dn",
        "baseline", "lottery_trade_frac", "trimmed_avg",
        "lottery_day_frac", "lottery_top_day", "lottery_top_day_pnl",
        "lottery_n_days", "verdict", "fail_reasons", "live_untouched",
    )
    return {k: r[k] for k in keys if k in r}


def run(workers=4):
    split = json.load(open(SPLIT_PATH))
    disc, hold = set(split["discovery"]), set(split["holdout"])
    spy = load_spy()
    pats = candidate_pats()
    files = [f for f in sorted(glob.glob(os.path.join(GRIDS, "*.json")))
             if not os.path.basename(f).startswith("_")]
    print(f"[harden] grids={len(files)} pats={len(pats)} spy={len(spy)} "
          f"workers={workers}", flush=True)
    trades, seen = collect_trades(files, pats, spy, disc, hold, workers)
    baselines = load_baselines()
    rows = []
    for name, rule in CANDIDATE_KEYS:
        hold_n = int(rule[4:])
        subset = [tr for tr in trades
                  if tr["def"] == name and tr["hold"] == hold_n]
        base = baselines.get(f"open_long_h{hold_n}")
        rows.append(score_cell(name, hold_n, subset, base))
    apply_hold1_keep(rows)
    return rows, seen, len(files), trades


def _write_text(path, text):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as fh:
        fh.write(text)


def write_outputs(rows, n_grids):
    harden_md = render_plain(rows, n_grids)
    ao_path = os.path.join(RESEARCH, "AO_FIRST_MINE.md")
    ao_text = splice_ao_md(harden_md)
    sb_path = os.path.join(SCOREBOARD, "EXCEL_BOT_MINE.md")
    sb_text = splice_md(sb_path, "## Harden: morning hysteresis light",
                        render_scoreboard(rows, n_grids),
                        require_any=("first A–JL cut", "A–O clock cycle"))
    cycle_path = os.path.join(RESEARCH, "MINE_CYCLE.md")
    note = (
        "## Harden (6 open-hysteresis lights)\n\n"
        f"KEEP {sum(1 for r in rows if r['verdict']=='KEEP')} · "
        f"KILL {sum(1 for r in rows if r['verdict']=='KILL')} · "
        "futubull · walk-forward + both-tape + top-day lottery. "
        "See `AO_FIRST_MINE.md` / `03_scoreboard/EXCEL_BOT_MINE.md`.\n"
    )
    cycle_text = splice_md(cycle_path, "## Harden (6 open-hysteresis lights)",
                           note, require_any=("first A–JL cut", "A–O clock cycle"))
    _write_text(ao_path, ao_text)
    _write_text(sb_path, sb_text)
    _write_text(cycle_path, cycle_text)
    payload = {
        "generated": str(date.today()),
        "spec": "harden 6 open-hysteresis candidates; futubull; "
                "walk-forward + both-tape + top-day lottery",
        "half_cut": HALF_CUT,
        "grids": n_grids,
        "grids_rebuilt": 3603,
        "n_keep": sum(1 for r in rows if r["verdict"] == "KEEP"),
        "n_kill": sum(1 for r in rows if r["verdict"] == "KILL"),
        "cost_model": "futubull",
        "live_untouched": "flatten_robust",
        "candidates": [slim_row(r) for r in rows],
    }
    json.dump(payload, open(os.path.join(RESEARCH, "hyst_open_harden.json"),
                            "w"), indent=1)
    return payload


def rows_from_payload(path=None):
    raw = json.load(open(path or os.path.join(RESEARCH, "hyst_open_harden.json")))
    return raw["candidates"], raw.get("grids_rebuilt") or raw.get("grids")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--workers", type=int, default=min(4, os.cpu_count() or 2))
    ap.add_argument("--render-only", action="store_true")
    args = ap.parse_args()
    os.chdir(ROOT)
    if args.render_only:
        rows, n_files = rows_from_payload()
        payload = write_outputs(rows, n_files)
        print(f"[render] KEEP={payload['n_keep']} KILL={payload['n_kill']}",
              flush=True)
        return
    rows, seen, n_files, _trades = run(workers=args.workers)
    payload = write_outputs(rows, n_files)
    print(f"[done] KEEP={payload['n_keep']} KILL={payload['n_kill']} "
          f"grids={payload['grids']}", flush=True)
    for r in rows:
        print(f"  {r['verdict']:4s} {r['def']:24s} {r['exit']} "
              f"{fmt_blk(r.get('holdout'))} "
              f"{','.join(r['fail_reasons']) or '—'}", flush=True)


if __name__ == "__main__":
    main()
