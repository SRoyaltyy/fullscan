"""Strict walk-forward strategy-discovery leaderboard for factor mine.

The published FACTOR_MINE board searched ~235 recipes + ~74 combos on the
same ~21 overlapping sessions it then ranked. That is multiple-testing
contaminated — hot names like ``short_news_r_h3``, ``union_hot_n4_h1``,
and ``combo_sh_5050_shared`` (+35.9% Book%) were chosen after seeing
every day they were graded on.

This harness does not remine cameras or rewrite recipes. It:

  1. Cuts the leak-free 09:30 panel at a series of dates
  2. Scores / selects recipes using only sessions ≤ cutoff
  3. Freezes the selected recipes (and any IS-built 50/50 combo)
  4. Scores them on a hidden forward window (fresh $10k cash book)
  5. Always scores controls: (A) hot-4 long (B) news-red short
     (C) 50/50 combo (D) random L/S matched (E) market-neutralized
     residuals (median same-day open→close of the panel names)

Every money number goes through the existing audited ``simulate_book``
/ ``simulate_shared`` cash ledger (Futubull fees, whole shares, leftover
split, sell first, hard-red sit). Research only — does not change live
``flatten_robust`` or the published cash book.

CLI: python -m src.walkforward_factor_mine --write
"""
from __future__ import annotations

import argparse
import json
import random
from datetime import datetime
from pathlib import Path
from statistics import median

from . import book_era
from . import factor_mine as fm
from . import factor_mine_book as fmb
from . import factor_mine_combo as fmc
from . import paper_trade as pt
from . import ticker_lookback as tl

ROOT = Path(__file__).resolve().parent.parent
OUT_DIR = ROOT / "03_scoreboard" / "walkforward_factor_mine"
OUT_MD = OUT_DIR / "WALKFORWARD.md"
OUT_JSON = OUT_DIR / "walkforward.json"

HOT4 = "union_hot_n4_h1"
NEWS_RED = "short_news_r_h3"
COMBO_SH = "combo_sh_5050_shared"
PIN_LONG = "wf_pin_long"
PIN_SHORT = "wf_pin_short"

DEFAULT_FIRST_CUTOFF = "2026-08-20"
DEFAULT_STEP = 4
DEFAULT_FORWARD = 4
MIN_IS = 5
MIN_OOS = 3
MIN_SELECT_DAYS = 3
CAPITAL = fm.CAPITAL


# ---------------------------------------------------------------------------
# Panel / fold plumbing
# ---------------------------------------------------------------------------

def slice_panel(panel: dict, start: str | None = None,
                end: str | None = None) -> dict:
    """Rows and calendar inside ``[start, end]``. No later session leaks in."""
    cal = [d for d in (panel.get("session_dates") or [])
           if (not start or d >= start) and (not end or d <= end)]
    keep = set(cal)
    rows = [r for r in (panel.get("rows") or []) if r.get("date") in keep]
    by_date: dict[str, list] = {}
    for r in rows:
        by_date.setdefault(r["date"], []).append(r)
    out = {k: v for k, v in panel.items()
           if k not in ("session_dates", "rows", "by_date")}
    out.update({
        "session_dates": cal,
        "rows": rows,
        "by_date": by_date,
        "from_date": cal[0] if cal else start,
        "to_date": cal[-1] if cal else end,
        "n_sessions": len(cal),
        "n_rows": len(rows),
    })
    return out


def make_folds(cal: list[str], *, first_cutoff: str = DEFAULT_FIRST_CUTOFF,
               step: int = DEFAULT_STEP, forward: int = DEFAULT_FORWARD,
               min_is: int = MIN_IS, min_oos: int = MIN_OOS) -> list[dict]:
    """Cutoffs at ``first_cutoff``, then every ``step`` sessions.

    IS = every session ≤ cutoff. OOS = the next ``forward`` sessions
    (last fold may be shorter, down to ``min_oos``).
    """
    cal = [d for d in cal if d]
    if not cal:
        return []
    if first_cutoff not in cal:
        later = [d for d in cal if d >= first_cutoff]
        if not later:
            return []
        first_cutoff = later[0]
    i = cal.index(first_cutoff)
    folds = []
    while i < len(cal) - min_oos:
        is_dates = cal[: i + 1]
        oos_dates = cal[i + 1: i + 1 + forward]
        if len(is_dates) >= min_is and len(oos_dates) >= min_oos:
            folds.append({
                "cutoff": cal[i],
                "is_dates": list(is_dates),
                "oos_dates": list(oos_dates),
            })
        i += max(1, int(step))
    return folds


def recipe_book(name: str, recipes: list[dict] | None = None) -> dict:
    recipes = list(recipes or fm.build_recipes())
    for rec in recipes:
        if rec.get("name") == name:
            return rec
    raise KeyError(f"unknown recipe {name}")


# ---------------------------------------------------------------------------
# Cash-book scoring (no cash-start replay — one fresh $10k path)
# ---------------------------------------------------------------------------

def _max_dd(equity: list) -> float | None:
    peak = None
    dd = 0.0
    n = 0
    for x in equity or []:
        try:
            v = float(x)
        except (TypeError, ValueError):
            continue
        n += 1
        peak = v if peak is None else max(peak, v)
        if peak and peak > 0:
            dd = max(dd, (peak - v) / peak)
    if not n:
        return None
    return round(100.0 * dd, 2)


def slim_book(book: dict, *, name: str, side: str, hold,
              role: str, selected: bool = False,
              members: list[str] | None = None) -> dict:
    days = [d for d in (book.get("daily") or []) if d.get("mean") is not None]
    n_hit = sum(1 for d in days if d.get("made_money"))
    aud = book.get("audit") or {}
    return {
        "name": name,
        "side": side,
        "hold": hold,
        "role": role,
        "selected": bool(selected),
        "members": list(members or []),
        "n_sessions": len(days),
        "n_hit": n_hit,
        "hit_rate": None if not days else round(n_hit / len(days), 4),
        "win_rate": book.get("win_rate"),
        "n_trades": book.get("n_trades") or 0,
        "n_closed": book.get("n_closed") or 0,
        "total_ret_pct": book.get("total_ret_pct"),
        "realized": book.get("realized"),
        "final_equity": book.get("final_equity"),
        "max_dd_pct": _max_dd(book.get("equity")),
        "audit_ok": bool(aud.get("ok", True)),
        "daily": [
            {
                "date": d.get("date"),
                "mean": d.get("mean"),
                "equity": d.get("equity"),
                "stock": d.get("stock"),
                "made_money": d.get("made_money"),
            }
            for d in days
        ],
    }


def score_recipe_book(panel: dict, rec: dict, *, bars=None, fees=None,
                      regime=None, start: str | None = None,
                      role: str = "recipe", selected: bool = False) -> dict:
    book = fmb.simulate_book(
        panel, rec, bars=bars, fees=fees, regime=regime, start=start)
    return slim_book(
        book, name=rec["name"], side=rec.get("side") or "long",
        hold=rec.get("hold"), role=role, selected=selected)


def score_combo_book(panel: dict, recs: list[dict], weights: list[float],
                     *, name: str, bars=None, fees=None, regime=None,
                     start: str | None = None, role: str = "combo",
                     selected: bool = False) -> dict:
    book = fmc.simulate_shared(
        panel, recs, weights, bars=bars, fees=fees, regime=regime,
        start=start, name=name)
    return slim_book(
        book, name=name, side="mix", hold="mix", role=role,
        selected=selected, members=[r["name"] for r in recs])


def select_is(scored: list[dict], *, min_days: int = MIN_SELECT_DAYS) -> dict:
    """Pick best long / short / overall from IS cash books only."""
    usable = [
        s for s in scored
        if s.get("audit_ok") and (s.get("n_sessions") or 0) >= min_days
    ]

    def key(s: dict) -> tuple:
        return (
            float(s.get("total_ret_pct") if s.get("total_ret_pct") is not None else -999),
            float(s.get("hit_rate") or 0),
            int(s.get("n_trades") or 0),
        )

    longs = [s for s in usable if (s.get("side") or "long") == "long"]
    shorts = [s for s in usable if s.get("side") == "short"]
    best = max(usable, key=key) if usable else None
    best_long = max(longs, key=key) if longs else None
    best_short = max(shorts, key=key) if shorts else None
    return {
        "top": None if not best else best["name"],
        "best_long": None if not best_long else best_long["name"],
        "best_short": None if not best_short else best_short["name"],
        "top_book_pct": None if not best else best.get("total_ret_pct"),
        "best_long_book_pct": None if not best_long else best_long.get("total_ret_pct"),
        "best_short_book_pct": None if not best_short else best_short.get("total_ret_pct"),
        "n_scored": len(scored),
        "n_usable": len(usable),
    }


# ---------------------------------------------------------------------------
# Random L/S matched control + cheap market neutralization
# ---------------------------------------------------------------------------

def _uniq(tickers: list[str]) -> list[str]:
    seen: set[str] = set()
    out = []
    for t in tickers:
        name = fm._tick(t)
        if not name or name in seen:
            continue
        seen.add(name)
        out.append(name)
    return out


def random_matched_groups(panel: dict, long_rec: dict, short_rec: dict,
                          *, seed: int) -> tuple[dict, dict]:
    """Same daily head-count as the two members; names drawn from the rest."""
    rng = random.Random(int(seed))
    long_picks: dict[str, list[str]] = {}
    short_picks: dict[str, list[str]] = {}
    for date, rows in (panel.get("by_date") or {}).items():
        actual_l = {r["ticker"] for r in fm.pick_day(rows, long_rec)}
        actual_s = {r["ticker"] for r in fm.pick_day(rows, short_rec)}
        pool = _uniq([r.get("ticker") for r in rows])
        avail = [t for t in pool if t not in actual_l and t not in actual_s]
        if len(avail) < len(actual_l) + len(actual_s):
            avail = avail + [t for t in pool if t not in avail]
        rng.shuffle(avail)
        n_l, n_s = len(actual_l), len(actual_s)
        taken_l = avail[:n_l]
        taken_s = [t for t in avail[n_l:n_l + n_s] if t not in set(taken_l)]
        long_picks[date] = taken_l
        short_picks[date] = taken_s
    return long_picks, short_picks


def apply_pins(panel: dict, groups: dict[str, dict[str, list[str]]]) -> dict:
    """Stamp synthetic universe tags onto pinned names. Other rows stay."""
    rows = []
    for r in panel.get("rows") or []:
        rr = dict(r)
        srcs = list(rr.get("sources") or [])
        date = r.get("date")
        t = r.get("ticker")
        for tag, by_date in groups.items():
            if t in set(by_date.get(date) or []) and tag not in srcs:
                srcs.append(tag)
        rr["sources"] = srcs
        rows.append(rr)
    out = dict(panel)
    out["rows"] = rows
    by_date: dict[str, list] = {}
    for r in rows:
        by_date.setdefault(r["date"], []).append(r)
    out["by_date"] = by_date
    return out


def pin_recipe(name: str, tag: str, rec: dict) -> dict:
    return fm.make_recipe(
        name, universe=tag, hold=int(rec.get("hold") or 1),
        side=rec.get("side") or "long",
        top_n=max(32, int(rec.get("top_n") or 8)),
        note=f"walk-forward pin matching {rec.get('name')}",
    )


def market_session_rets(panel: dict, bars=None) -> dict[str, float]:
    """Median same-day open→close of names printed on that 09:30 panel."""
    out: dict[str, float] = {}
    for date, rows in (panel.get("by_date") or {}).items():
        xs = []
        seen: set[str] = set()
        for r in rows:
            t = fm._tick(r.get("ticker"))
            if not t or t in seen:
                continue
            seen.add(t)
            bar = fm._bar(t, date, bars)
            o = fm._finite(bar.get("open"))
            c = fm._finite(bar.get("close"))
            if o and c and o > 0:
                xs.append(100.0 * (c / o - 1.0))
        if xs:
            out[date] = round(float(median(xs)), 4)
    return out


def neutralize_metrics(row: dict, mkt: dict[str, float],
                       *, name: str | None = None) -> dict:
    """Compound daily book% minus (stock/equity) × market open→close.

    Cheap residual — not a second simulated hedge book. Long books have
    +exposure, shorts −exposure, the 50/50 combo is already netted.
    """
    eq0 = float(CAPITAL)
    eq = eq0
    n = 0
    n_hit = 0
    for d in row.get("daily") or []:
        mean = d.get("mean")
        if mean is None:
            continue
        m = mkt.get(d["date"])
        stock = float(d.get("stock") or 0.0)
        e = float(d.get("equity") or eq)
        exp = (stock / e) if e else 0.0
        resid = float(mean) if m is None else float(mean) - exp * float(m)
        eq *= (1.0 + resid / 100.0)
        n += 1
        if resid > 0:
            n_hit += 1
    out = dict(row)
    out["name"] = name or (str(row.get("name") or "") + "_mktneut")
    out["role"] = "neutralized"
    out["selected"] = False
    out["n_sessions"] = n
    out["n_hit"] = n_hit
    out["hit_rate"] = None if not n else round(n_hit / n, 4)
    out["total_ret_pct"] = round(100.0 * (eq / eq0 - 1.0), 3)
    out["final_equity"] = round(eq, 2)
    out["daily"] = []
    return out


# ---------------------------------------------------------------------------
# One fold
# ---------------------------------------------------------------------------

def _seed_for(cutoff: str) -> int:
    return 1 + sum((i + 1) * ord(ch) for i, ch in enumerate(cutoff))


def run_fold(panel: dict, fold: dict, recipes: list[dict], *,
             bars=None, fees=None, regime=None,
             mine: bool = True) -> dict:
    rec_by = {r["name"]: r for r in recipes}
    hot = rec_by[HOT4]
    news = rec_by[NEWS_RED]
    is_panel = slice_panel(panel, end=fold["cutoff"])
    oos_panel = slice_panel(
        panel, start=fold["oos_dates"][0], end=fold["oos_dates"][-1])
    oos_start = fold["oos_dates"][0]

    selection = {
        "top": None, "best_long": None, "best_short": None,
        "n_scored": 0, "n_usable": 0,
    }
    is_rows: list[dict] = []
    if mine:
        for rec in recipes:
            try:
                is_rows.append(score_recipe_book(
                    is_panel, rec, bars=bars, fees=fees, regime=regime,
                    role="is_recipe"))
            except Exception as e:
                print(f"[wf] IS skip {rec.get('name')}: {e}", flush=True)
        selection = select_is(is_rows)

    frozen = []
    for key, role in (("top", "discovered_top"),
                      ("best_long", "discovered_long"),
                      ("best_short", "discovered_short")):
        name = selection.get(key)
        if name and name in rec_by:
            frozen.append((rec_by[name], role, True))

    oos_rows: list[dict] = []

    def add_single(rec, role, selected=False, start=None, use=None):
        oos_rows.append(score_recipe_book(
            use or oos_panel, rec, bars=bars, fees=fees, regime=regime,
            start=start or oos_start, role=role, selected=selected))

    def add_combo(recs, weights, name, role, selected=False, use=None):
        oos_rows.append(score_combo_book(
            use or oos_panel, recs, weights, name=name, bars=bars,
            fees=fees, regime=regime, start=oos_start, role=role,
            selected=selected))

    # Frozen IS picks on the hidden window (fresh $10k).
    seen_frozen: set[str] = set()
    for rec, role, selected in frozen:
        if rec["name"] in seen_frozen:
            continue
        seen_frozen.add(rec["name"])
        add_single(rec, role, selected=selected)

    disc_long = rec_by.get(selection.get("best_long") or "")
    disc_short = rec_by.get(selection.get("best_short") or "")
    if disc_long and disc_short:
        add_combo(
            [disc_short, disc_long], [1, 1],
            f"discovered_5050_{disc_short['name']}+{disc_long['name']}",
            "discovered_5050", selected=True)

    # Controls — always scored, never selected from the OOS tape.
    add_single(hot, "control_hot4")
    add_single(news, "control_news_red")
    add_combo([news, hot], [1, 1], COMBO_SH, "control_combo_sh")

    long_p, short_p = random_matched_groups(
        oos_panel, hot, news, seed=_seed_for(fold["cutoff"]))
    pinned = apply_pins(oos_panel, {PIN_LONG: long_p, PIN_SHORT: short_p})
    rand_l = pin_recipe("random_long_matched", PIN_LONG, hot)
    rand_s = pin_recipe("random_short_matched", PIN_SHORT, news)
    add_combo([rand_s, rand_l], [1, 1], "random_ls_matched",
              "control_random", use=pinned)

    mkt = market_session_rets(oos_panel, bars)
    neut_src = [r for r in oos_rows if r["role"] in (
        "control_hot4", "control_news_red", "control_combo_sh",
        "discovered_5050")]
    for row in neut_src:
        oos_rows.append(neutralize_metrics(row, mkt))

    # IS print of the published combo (context only — not a selector).
    is_combo = score_combo_book(
        is_panel, [news, hot], [1, 1], name=COMBO_SH,
        bars=bars, fees=fees, regime=regime, role="is_control_combo_sh")

    return {
        "cutoff": fold["cutoff"],
        "is_dates": list(fold["is_dates"]),
        "oos_dates": list(fold["oos_dates"]),
        "n_is": len(fold["is_dates"]),
        "n_oos": len(fold["oos_dates"]),
        "selection": selection,
        "is_combo_sh": is_combo,
        "oos": oos_rows,
        "market": mkt,
    }


# ---------------------------------------------------------------------------
# Pool + verdict
# ---------------------------------------------------------------------------

def _rows_with_role(folds: list[dict], role: str) -> list[dict]:
    out = []
    for f in folds:
        for r in f.get("oos") or []:
            if r.get("role") == role:
                out.append(r)
    return out


def pool_rows(rows: list[dict]) -> dict:
    rets = [float(r["total_ret_pct"]) for r in rows
            if r.get("total_ret_pct") is not None]
    n_sess = sum(int(r.get("n_sessions") or 0) for r in rows)
    n_hit = sum(int(r.get("n_hit") or 0) for r in rows)
    pnl = 0.0
    for r in rows:
        eq = r.get("final_equity")
        if eq is not None:
            pnl += float(eq) - CAPITAL
    return {
        "n_folds": len(rets),
        "n_green_folds": sum(1 for x in rets if x > 0),
        "mean_book_pct": None if not rets else round(sum(rets) / len(rets), 3),
        "n_sessions": n_sess,
        "n_hit": n_hit,
        "hit_rate": None if not n_sess else round(n_hit / n_sess, 4),
        "pnl": round(pnl, 2),
        "fold_book_pct": [round(x, 3) for x in rets],
    }


def decide_verdict(combo: dict, random_c: dict, process: dict) -> dict:
    """Plain-language KEEP/KILL on the published +35.9 discovery.

    KILL unless the frozen 50/50 is +EV after fees on the hidden windows
    *and* it beats the matched random L/S control on mean Book%.
    The IS discovery *process* (re-pick best long/short each fold) is
    a second line — it can fail even if the frozen combo is lucky.
    """
    c_mean = combo.get("mean_book_pct")
    r_mean = random_c.get("mean_book_pct")
    n = int(combo.get("n_folds") or 0)
    n_green = int(combo.get("n_green_folds") or 0)
    plus = c_mean is not None and c_mean > 0
    beats_rand = (
        c_mean is not None and r_mean is not None and c_mean > r_mean
    )
    majority = n > 0 and n_green > n / 2
    keep_combo = bool(plus and beats_rand and majority)
    p_mean = process.get("mean_book_pct")
    process_plus = p_mean is not None and p_mean > 0
    if n == 0:
        label = "THIN"
        why = "No completed walk-forward folds — not enough sessions."
    elif keep_combo:
        label = "KEEP"
        why = (
            "Frozen combo_sh_5050_shared stayed +EV after fees on hidden "
            "windows, beat the matched random L/S control, and was green "
            "on a majority of folds."
        )
    elif plus and not beats_rand:
        label = "KILL"
        why = (
            "Frozen combo_sh_5050_shared was green OOS but did not beat "
            "the matched random L/S control — the 35.9% print is not "
            "distinguishable from a same-count coin flip."
        )
    elif plus and not majority:
        label = "KILL"
        why = (
            "Mean OOS Book% is positive but a majority of hidden windows "
            "are not — the full-sample +35.9% does not survive the cut."
        )
    else:
        label = "KILL"
        why = (
            "Frozen combo_sh_5050_shared is not +EV after fees on the "
            "hidden windows. The published +35.9% does not survive "
            "walk-forward."
        )
    return {
        "label": label,
        "keep_combo": keep_combo,
        "process_plus_ev": process_plus,
        "why": why,
        "combo": combo,
        "random": random_c,
        "process": process,
    }


# ---------------------------------------------------------------------------
# Board
# ---------------------------------------------------------------------------

def _pct(v) -> str:
    return "—" if v is None else f"{100 * float(v):.0f}%"


def _n(v) -> str:
    return "—" if v is None else f"{float(v):+.2f}"


def _row_on(fold: dict, role: str) -> dict | None:
    for r in fold.get("oos") or []:
        if r.get("role") == role:
            return r
    return None


def render_md(payload: dict) -> str:
    v = payload.get("verdict") or {}
    combo = v.get("combo") or {}
    rand = v.get("random") or {}
    proc = v.get("process") or {}
    folds = payload.get("folds") or []
    lines = [
        "# Walk-forward factor-mine discovery",
        "",
        f"**{v.get('label') or 'THIN'}** the published "
        f"`combo_sh_5050_shared` +35.9% discovery.",
        "",
        str(v.get("why") or ""),
        "",
        f"IS discovery process (re-pick best long + best short each "
        f"cutoff, freeze a 50/50) OOS mean Book% "
        f"{_n(proc.get('mean_book_pct'))}% — "
        + ("still +EV OOS." if v.get("process_plus_ev")
           else "does **not** find +EV OOS."),
        "",
        "## Method",
        "",
        "The published [FACTOR_MINE.md](../FACTOR_MINE.md) board searched "
        f"**{payload.get('n_recipes')}** leak-free 09:30 recipes plus "
        "combination books on the **same** sessions it then ranked. "
        "That is multiple-testing contaminated.",
        "",
        "This board:",
        "",
        "1. Uses the existing leak-free 09:30 panel "
        f"`{payload.get('from_date')} → {payload.get('to_date')}` "
        f"({payload.get('n_sessions')} sessions).",
        "2. At each cutoff, scores the recipe grid with the audited "
        "`simulate_book` cash ledger on sessions **≤ cutoff only**.",
        "3. Freezes the best-IS long, best-IS short, and a 50/50 shared "
        "combo of those two.",
        "4. Wakes a **fresh $10k** book on the hidden window after the "
        "cutoff (never used in selection). Open lots at the cutoff do "
        "not carry forward.",
        "5. Always scores controls: (A) `union_hot_n4_h1` (B) "
        "`short_news_r_h3` (C) `combo_sh_5050_shared` (D) random L/S "
        "matched to C's daily head-count (E) market-neutralized "
        "residuals (daily Book% − exposure × median panel open→close).",
        "",
        "Fills stay the published cash rules: whole shares, Futubull "
        "fees, leftover split, sell first, min-hold, 09:30 open, "
        "hard-red S≤−3 sit. Research only — does **not** change live "
        "`flatten_robust` or the cash book.",
        "",
        f"Cutoffs start `{payload.get('first_cutoff')}`, step "
        f"{payload.get('step')} sessions, hidden window "
        f"{payload.get('forward')} sessions (last fold may be shorter).",
        "",
        "`docs/HOW_IT_WORKS*` was not in the repo; method follows "
        "`src/factor_mine.py` + `src/factor_mine_book.py`.",
        "",
        "## Pooled hidden windows",
        "",
        "| Sleeve | Role | Folds green | Mean Book% | Hit rate | n sess | $ P&L |",
        "|---|---|---:|---:|---:|---:|---:|",
    ]
    pools = payload.get("pools") or {}
    order = [
        ("control_combo_sh", "frozen 50/50 (the +35.9 claim)"),
        ("control_hot4", "A · hot-4 long"),
        ("control_news_red", "B · news-red short"),
        ("control_random", "D · random L/S matched"),
        ("discovered_5050", "IS-picked 50/50 (process)"),
        ("discovered_top", "IS-picked top single"),
        ("control_combo_sh_mktneut", "E · 50/50 mkt-neutralized"),
        ("control_hot4_mktneut", "E · hot-4 mkt-neutralized"),
        ("control_news_red_mktneut", "E · news-red mkt-neutralized"),
        ("discovered_5050_mktneut", "E · IS 50/50 mkt-neutralized"),
    ]
    # Neutralized rows share role "neutralized"; reconstruct from pools keys.
    for key, label in order:
        p = pools.get(key)
        if not p:
            continue
        lines.append(
            f"| `{key}` | {label} | "
            f"{p.get('n_green_folds') or 0}/{p.get('n_folds') or 0} | "
            f"{_n(p.get('mean_book_pct'))} | {_pct(p.get('hit_rate'))} | "
            f"{p.get('n_sessions') or 0} | {_n(p.get('pnl'))} |"
        )
    lines += [
        "",
        f"Frozen `combo_sh_5050_shared` mean OOS Book% "
        f"{_n(combo.get('mean_book_pct'))}% vs random "
        f"{_n(rand.get('mean_book_pct'))}%.",
        "",
        "## Folds",
        "",
        "| Cutoff | IS n | Hidden window | IS combo Book% | "
        "OOS combo | OOS hot-4 | OOS news-red | OOS random | "
        "IS pick (L / S) | OOS IS-50/50 |",
        "|---|---:|---|---:|---:|---:|---:|---:|---|---:|",
    ]
    for f in folds:
        c = _row_on(f, "control_combo_sh") or {}
        h = _row_on(f, "control_hot4") or {}
        nws = _row_on(f, "control_news_red") or {}
        rnd = _row_on(f, "control_random") or {}
        disc = _row_on(f, "discovered_5050") or {}
        sel = f.get("selection") or {}
        is_c = f.get("is_combo_sh") or {}
        hidden = f"{(f.get('oos_dates') or [''])[0]} → {(f.get('oos_dates') or [''])[-1]}"
        pick = f"{sel.get('best_long') or '—'} / {sel.get('best_short') or '—'}"
        lines.append(
            f"| `{f.get('cutoff')}` | {f.get('n_is') or 0} | {hidden} | "
            f"{_n(is_c.get('total_ret_pct'))} | "
            f"{_n(c.get('total_ret_pct'))} | {_n(h.get('total_ret_pct'))} | "
            f"{_n(nws.get('total_ret_pct'))} | {_n(rnd.get('total_ret_pct'))} | "
            f"{pick} | {_n(disc.get('total_ret_pct'))} |"
        )
    lines += [
        "",
        "## Per-fold OOS detail",
        "",
    ]
    for f in folds:
        sel = f.get("selection") or {}
        lines += [
            f"### Cutoff `{f.get('cutoff')}` "
            f"(IS {f.get('n_is')} · hidden {f.get('n_oos')})",
            "",
            f"IS selected **top** `{sel.get('top') or '—'}` "
            f"({_n(sel.get('top_book_pct'))}%), "
            f"**long** `{sel.get('best_long') or '—'}` "
            f"({_n(sel.get('best_long_book_pct'))}%), "
            f"**short** `{sel.get('best_short') or '—'}` "
            f"({_n(sel.get('best_short_book_pct'))}%). "
            f"Scored {sel.get('n_scored') or 0} recipes, "
            f"{sel.get('n_usable') or 0} usable.",
            "",
            "| Name | Role | Book% | Hit | n | Trades | Win% | Audit |",
            "|---|---|---:|---:|---:|---:|---:|---|",
        ]
        for r in f.get("oos") or []:
            aud = "PASS" if r.get("audit_ok") else "FAIL"
            lines.append(
                f"| `{r.get('name')}` | {r.get('role')} | "
                f"{_n(r.get('total_ret_pct'))} | {_pct(r.get('hit_rate'))} | "
                f"{r.get('n_sessions') or 0} | {r.get('n_trades') or 0} | "
                f"{_pct(r.get('win_rate'))} | {aud} |"
            )
        lines.append("")
    lines += [
        "## KEEP / KILL",
        "",
        f"**{v.get('label')}** — {v.get('why')}",
        "",
        "This is a research scoreboard. It does not wire anything into "
        "`flatten_robust` and it does not change the live cash book.",
        "",
    ]
    return "\n".join(lines)


def _pool_payload(folds: list[dict]) -> dict:
    pools = {
        "control_combo_sh": pool_rows(_rows_with_role(folds, "control_combo_sh")),
        "control_hot4": pool_rows(_rows_with_role(folds, "control_hot4")),
        "control_news_red": pool_rows(_rows_with_role(folds, "control_news_red")),
        "control_random": pool_rows(_rows_with_role(folds, "control_random")),
        "discovered_5050": pool_rows(_rows_with_role(folds, "discovered_5050")),
        "discovered_top": pool_rows(_rows_with_role(folds, "discovered_top")),
    }
    # Neutralized rows keep the source name + _mktneut.
    by_neut: dict[str, list] = {}
    for f in folds:
        for r in f.get("oos") or []:
            if r.get("role") != "neutralized":
                continue
            nm = str(r.get("name") or "")
            if nm.endswith("_mktneut"):
                key = nm
            else:
                key = nm + "_mktneut"
            # Map known controls onto stable pool keys.
            src = nm[:-8] if nm.endswith("_mktneut") else nm
            alias = {
                COMBO_SH: "control_combo_sh_mktneut",
                HOT4: "control_hot4_mktneut",
                NEWS_RED: "control_news_red_mktneut",
            }.get(src)
            if alias is None and str(r.get("name") or "").startswith("discovered_5050"):
                alias = "discovered_5050_mktneut"
            if alias:
                by_neut.setdefault(alias, []).append(r)
            else:
                by_neut.setdefault(key, []).append(r)
    for k, rows in by_neut.items():
        pools[k] = pool_rows(rows)
    return pools


def write_outputs(payload: dict) -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    slim = dict(payload)
    # Daily marks are enough for the board; drop nothing essential.
    OUT_JSON.write_text(json.dumps(slim, indent=2), encoding="utf-8")
    OUT_MD.write_text(render_md(payload), encoding="utf-8")


# ---------------------------------------------------------------------------
# Bars + run
# ---------------------------------------------------------------------------

def preload_bars(panel: dict) -> dict:
    """Official 09:30 / 16:00 prints for every panel name-day."""
    tickers = {fm._tick(r.get("ticker")) for r in (panel.get("rows") or [])}
    tickers.discard("")
    dates = set(panel.get("session_dates") or [])
    bars: dict = {}
    df = tl._ohlc_bars()
    if df is None or getattr(df, "empty", True):
        return bars
    try:
        idx_dates = df.index.get_level_values(0)
        idx_tix = df.index.get_level_values(1)
        mask = idx_dates.isin(dates) & idx_tix.isin(tickers)
        sub = df.loc[mask]
    except Exception:
        sub = df
    try:
        records = sub.reset_index().to_dict(orient="records")
    except Exception:
        return bars
    for rec in records:
        t = fm._tick(rec.get("ticker"))
        d = str(rec.get("date") or "")[:10]
        if not t or d not in dates:
            continue
        o = fm._finite(rec.get("open"))
        c = fm._finite(rec.get("close"))
        bars[(t, d)] = {
            "open": o,
            "high": fm._finite(rec.get("high")),
            "low": fm._finite(rec.get("low")),
            "close": c,
        }
    return bars


def run(*, from_date: str = book_era.DASHBOARD_START,
        to_date: str | None = None, first_cutoff: str = DEFAULT_FIRST_CUTOFF,
        step: int = DEFAULT_STEP, forward: int = DEFAULT_FORWARD,
        write: bool = False, mine: bool = True,
        recipes: list[dict] | None = None,
        panel: dict | None = None, bars=None,
        fees=None, regime=None) -> dict:
    recipes = list(recipes or fm.build_recipes())
    panel = panel if panel is not None else fm.load_or_build_panel(
        from_date, to_date)
    cal = list(panel.get("session_dates") or [])
    folds_spec = make_folds(
        cal, first_cutoff=first_cutoff, step=step, forward=forward)
    if bars is None:
        print("[wf] preloading official bars", flush=True)
        bars = preload_bars(panel)
        print(f"[wf] bars={len(bars)}", flush=True)
    fees = fees if fees is not None else pt.load_fees()
    if regime is None:
        try:
            regime = fmb.load_regime()
        except Exception:
            regime = {}
    need = {HOT4, NEWS_RED}
    rec_by = {r["name"]: r for r in recipes}
    missing = [n for n in need if n not in rec_by]
    if missing:
        raise KeyError(f"control recipes missing: {missing}")

    folds = []
    for spec in folds_spec:
        print(f"[wf] fold cutoff={spec['cutoff']} "
              f"IS={len(spec['is_dates'])} OOS={spec['oos_dates']}",
              flush=True)
        folds.append(run_fold(
            panel, spec, recipes, bars=bars, fees=fees, regime=regime,
            mine=mine))

    pools = _pool_payload(folds)
    verdict = decide_verdict(
        pools.get("control_combo_sh") or {},
        pools.get("control_random") or {},
        pools.get("discovered_5050") or {},
    )
    payload = {
        "generated_at": datetime.now(tl.ET).isoformat(),
        "live_untouched": "flatten_robust",
        "fill": (
            "09:30 open, whole shares, Futubull fees, leftover split, "
            "sell first, hard-red sit, fresh $10k per window"
        ),
        "from_date": panel.get("from_date"),
        "to_date": panel.get("to_date"),
        "n_sessions": panel.get("n_sessions"),
        "n_recipes": len(recipes),
        "n_folds": len(folds),
        "first_cutoff": first_cutoff,
        "step": step,
        "forward": forward,
        "capital": CAPITAL,
        "controls": [HOT4, NEWS_RED, COMBO_SH, "random_ls_matched"],
        "folds": folds,
        "pools": pools,
        "verdict": verdict,
    }
    if write:
        write_outputs(payload)
        print(f"[wf] wrote {OUT_MD}  verdict={verdict.get('label')}",
              flush=True)
    return payload


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(
        description="Walk-forward factor-mine discovery leaderboard "
                    "(research only; does not change flatten_robust).")
    ap.add_argument("--from-date", default=book_era.DASHBOARD_START)
    ap.add_argument("--to-date", default="")
    ap.add_argument("--first-cutoff", default=DEFAULT_FIRST_CUTOFF)
    ap.add_argument("--step", type=int, default=DEFAULT_STEP)
    ap.add_argument("--forward", type=int, default=DEFAULT_FORWARD)
    ap.add_argument("--write", action="store_true")
    ap.add_argument("--no-mine", action="store_true",
                    help="skip IS recipe search; score controls only")
    args = ap.parse_args(argv)
    payload = run(
        from_date=args.from_date,
        to_date=args.to_date or None,
        first_cutoff=args.first_cutoff,
        step=args.step,
        forward=args.forward,
        write=args.write,
        mine=not args.no_mine,
    )
    v = payload.get("verdict") or {}
    print(f"[wf] folds={payload.get('n_folds')} "
          f"verdict={v.get('label')} "
          f"combo_oos={_n((v.get('combo') or {}).get('mean_book_pct'))}% "
          f"random={_n((v.get('random') or {}).get('mean_book_pct'))}%")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
