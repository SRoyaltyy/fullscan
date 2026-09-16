"""Mine factor/combo sleeves that still work on hard-red mornings.

The dashboard combo recipes came from the leak-free 09:30 catalog
(``build_recipes`` + ``combo_specs``). This mine asks the same catalog
a different question: which sleeves still make money after Futubull
fees when the *entry* morning is hard-red (S≤−3), at every hold
(1 / 2 / 3 / 5)?

Calendar time-split (HOLD_FRAC=0.30), same contract as the Excel remine:

  * discovery fire: entry date < cutoff AND the full horizon close
    is strictly before cutoff
  * holdout fire: entry date ≥ cutoff
  * a missing open or exit fails closed (ungraded)

Live sit is untouched. Research mode ``allow`` takes the official
09:30 open on a red morning — no dip-scoop rewrite. Default
``simulate_book`` / ``simulate_shared`` / Webull tickets stay sit.

CLI: python -m src.hard_red_strategy_mine --write
"""
from __future__ import annotations

import argparse
import json
from datetime import datetime
from pathlib import Path

from . import book_era
from . import factor_mine as fm
from . import factor_mine_book as fmb
from . import factor_mine_combo as fmc
from . import hard_red_sit_research as hrs
from . import paper_trade as pt
from . import ticker_lookback as tl
from . import walkforward_factor_mine as wf

ROOT = Path(__file__).resolve().parent.parent
OUT_DIR = ROOT / "03_scoreboard" / "hard_red_mine"
OUT_MD = ROOT / "03_scoreboard" / "HARD_RED_MINE.md"
OUT_JSON = OUT_DIR / "hard_red_mine.json"

HOLD_FRAC = 0.30
HORIZONS = (1, 2, 3, 5)
# Published KEEP — same bar as HARD_RED_SIT. Thin n is KILL.
KEEP_MIN_FIRES = 30
KEEP_WIN = 0.55
# Research floor for "holdout survivor" reporting. Still not a wire.
RESEARCH_MIN_FIRES = 8
RESEARCH_WIN = 0.55
TOP_DISC = 12
TOP_COMBOS = 8
LIVE_COMBO = "combo_sh_macd_5050_shared"
CAPITAL = fm.CAPITAL


def time_split_cutoff(cal: list[str], hold_frac: float = HOLD_FRAC,
                      locked: str | None = None) -> str | None:
    """First holdout session date. Locked cutoff is reused if passed."""
    if locked:
        return locked
    dates = [d for d in cal if d]
    if not dates:
        return None
    if len(dates) == 1:
        return dates[0]
    idx = int(round(len(dates) * (1.0 - hold_frac)))
    idx = min(max(idx, 1), len(dates) - 1)
    return dates[idx]


def disc_label_ok(entry: str | None, exit_d: str | None,
                  cutoff: str | None) -> bool:
    """Discovery only if the entry *and* the full label window precede cutoff."""
    if not cutoff or not entry:
        return False
    if entry >= cutoff:
        return False
    if not exit_d or exit_d >= cutoff:
        return False
    return True


def split_fires(fires: list[dict], cutoff: str | None) -> dict:
    disc, hold = [], []
    for f in fires or []:
        entry = f.get("date")
        exit_d = f.get("exit_date")
        if disc_label_ok(entry, exit_d, cutoff):
            disc.append(f)
        elif cutoff and entry and entry >= cutoff:
            hold.append(f)
    return {"disc": disc, "hold": hold}


def name_day_fires(panel: dict, rec: dict, red_dates: list[str],
                   cal: list[str], *, bars=None, fees=None,
                   hold: int) -> list[dict]:
    """1-share after-fee P&L: 09:30 open → horizon close. Close grades only."""
    fees = fees if fees is not None else pt.load_fees()
    by_date = panel.get("by_date") or {}
    side = rec.get("side") or "long"
    out = []
    for date in red_dates:
        rows = by_date.get(date) or []
        for r in fm.pick_day(rows, rec):
            t = fm._tick(r.get("ticker"))
            if not t:
                continue
            o = fmb._px(t, date, "open", bars)
            if o is None:
                out.append({
                    "date": date, "ticker": t, "side": side,
                    "hold": hold, "entry": None, "exit": None,
                    "exit_date": None, "exit_how": "no_open",
                    "pnl": None, "win": None, "recipe": rec.get("name"),
                })
                continue
            exit_px, exit_d, how = hrs.horizon_exit(
                cal, date, hold, t, bars)
            if exit_px is None and exit_d:
                # Exit session may not be a panel name-day. Official
                # parquet print still grades; missing print fails closed.
                store = hrs.clock_bar(t, exit_d, None)
                exit_px = store.get("close")
                if exit_px is None:
                    exit_px = store.get("open")
                    how = "horizon_open_store" if exit_px is not None else how
                else:
                    how = "horizon_close_store"
            pnl = None
            if exit_px is not None:
                pnl = round(hrs.after_fee_pnl(
                    1, float(o), float(exit_px), side=side, fees=fees), 4)
            out.append({
                "date": date, "ticker": t, "side": side,
                "hold": hold,
                "entry": round(float(o), 4),
                "exit": None if exit_px is None else round(float(exit_px), 4),
                "exit_date": exit_d, "exit_how": how,
                "pnl": pnl,
                "win": None if pnl is None else bool(pnl > 0),
                "recipe": rec.get("name"),
            })
    return out


def combo_name_day_fires(panel: dict, recs: list[dict], red_dates: list[str],
                         cal: list[str], *, bars=None, fees=None,
                         hold: int | None = None) -> list[dict]:
    """Union of kid picks. Each fire uses ``hold`` or that kid's baked hold."""
    seen: set[tuple] = set()
    out = []
    for rec in recs:
        h = int(hold if hold is not None else rec.get("hold") or 1)
        for f in name_day_fires(
                panel, rec, red_dates, cal, bars=bars, fees=fees, hold=h):
            key = (f.get("date"), f.get("ticker"), f.get("side"))
            if key in seen:
                continue
            seen.add(key)
            f = dict(f)
            f["owner"] = rec.get("name")
            out.append(f)
    return out


def slim_stats(st: dict) -> dict:
    return {k: st.get(k) for k in (
        "n_fires", "n_graded", "n_wins", "win_rate", "pnl")}


def _rank_key(st: dict) -> tuple:
    wr = st.get("win_rate")
    return (
        wr if wr is not None else -1.0,
        int(st.get("n_graded") or 0),
        float(st.get("pnl") or 0),
    )


def research_survivor(*, disc: dict, hold: dict) -> bool:
    d_n = int(disc.get("n_graded") or 0)
    h_n = int(hold.get("n_graded") or 0)
    d_wr = disc.get("win_rate")
    h_wr = hold.get("win_rate")
    return (
        d_n >= RESEARCH_MIN_FIRES and d_wr is not None and d_wr > RESEARCH_WIN
        and h_n >= RESEARCH_MIN_FIRES and h_wr is not None and h_wr > RESEARCH_WIN
    )


def live_keep(*, n_fires: int, win_rate) -> dict:
    """Same published bar as HARD_RED_SIT. Does not change live sit."""
    return hrs.decide_verdict(
        n_fires=n_fires, win_rate=win_rate,
        tapes_n={"name_day": {"n_fires": n_fires, "win_rate": win_rate}},
        wf_ok=None, label="hard-red name-day (any horizon)",
    )


def score_horizons(panel: dict, rec: dict, red_dates: list[str],
                   cal: list[str], cutoff: str | None, *,
                   bars=None, fees=None,
                   holds: tuple[int, ...] = HORIZONS) -> list[dict]:
    rows = []
    for hold in holds:
        fires = name_day_fires(
            panel, rec, red_dates, cal, bars=bars, fees=fees, hold=hold)
        parts = split_fires(fires, cutoff)
        disc = hrs.fire_stats(parts["disc"])
        hold_st = hrs.fire_stats(parts["hold"])
        all_st = hrs.fire_stats(fires)
        rows.append({
            "name": rec.get("name"),
            "side": rec.get("side") or "long",
            "hold": hold,
            "baked_hold": rec.get("hold"),
            "kind": "recipe",
            "note": rec.get("note"),
            "disc": slim_stats(disc),
            "holdout": slim_stats(hold_st),
            "all_red": slim_stats(all_st),
            "research_ok": research_survivor(disc=disc, hold=hold_st),
            "live_keep": live_keep(
                n_fires=all_st.get("n_fires") or 0,
                win_rate=all_st.get("win_rate")),
        })
    return rows


def score_combo_horizons(panel: dict, spec: dict, recs: list[dict],
                         red_dates: list[str], cal: list[str],
                         cutoff: str | None, *, bars=None, fees=None,
                         holds: tuple[int, ...] = HORIZONS) -> list[dict]:
    rows = []
    baked = combo_name_day_fires(
        panel, recs, red_dates, cal, bars=bars, fees=fees, hold=None)
    parts = split_fires(baked, cutoff)
    disc = hrs.fire_stats(parts["disc"])
    hold_st = hrs.fire_stats(parts["hold"])
    all_st = hrs.fire_stats(baked)
    rows.append({
        "name": spec["name"],
        "side": "mix",
        "hold": "baked",
        "kind": "combo",
        "members": list(spec.get("members") or []),
        "disc": slim_stats(disc),
        "holdout": slim_stats(hold_st),
        "all_red": slim_stats(all_st),
        "research_ok": research_survivor(disc=disc, hold=hold_st),
        "live_keep": live_keep(
            n_fires=all_st.get("n_fires") or 0,
            win_rate=all_st.get("win_rate")),
    })
    for hold in holds:
        fires = combo_name_day_fires(
            panel, recs, red_dates, cal, bars=bars, fees=fees, hold=hold)
        parts = split_fires(fires, cutoff)
        disc = hrs.fire_stats(parts["disc"])
        hold_st = hrs.fire_stats(parts["hold"])
        all_st = hrs.fire_stats(fires)
        rows.append({
            "name": f"{spec['name']}_h{hold}",
            "side": "mix",
            "hold": hold,
            "kind": "combo_horizon",
            "members": list(spec.get("members") or []),
            "parent": spec["name"],
            "disc": slim_stats(disc),
            "holdout": slim_stats(hold_st),
            "all_red": slim_stats(all_st),
            "research_ok": research_survivor(disc=disc, hold=hold_st),
            "live_keep": live_keep(
                n_fires=all_st.get("n_fires") or 0,
                win_rate=all_st.get("win_rate")),
        })
    return rows


def _book_card(book: dict, *, bars=None, fees=None) -> dict:
    red = hrs.fire_stats(hrs.hard_red_fires(book, bars=bars, fees=fees))
    return {
        "name": book.get("name"),
        "book_pct": book.get("total_ret_pct"),
        "win_rate": book.get("win_rate"),
        "n_trades": book.get("n_trades"),
        "n_closed": book.get("n_closed"),
        "final_equity": book.get("final_equity"),
        "audit_ok": bool((book.get("audit") or {}).get("ok")),
        "hard_red": slim_stats(red),
    }


def cash_compare(panel: dict, rec: dict, *, bars=None, fees=None,
                 regime=None) -> dict:
    """Sit (published / all-day) vs allow-through on the same $10k book."""
    sit = fmb.simulate_book(
        panel, rec, bars=bars, fees=fees, regime=regime)
    allow = fmb.simulate_book(
        panel, rec, bars=bars, fees=fees, regime=regime,
        rules={"hard_red_no_new": False})
    return {
        "sit": _book_card(sit, bars=bars, fees=fees),
        "allow": _book_card(allow, bars=bars, fees=fees),
    }


def cash_compare_combo(panel: dict, recs: list[dict], spec: dict, *,
                       bars=None, fees=None, regime=None) -> dict:
    sit = fmc.simulate_shared(
        panel, recs, spec["weights"], bars=bars, fees=fees, regime=regime,
        net=spec.get("net") or "priority", name=spec["name"],
        hard_red_mode=fmc.HARD_RED_SIT)
    allow = fmc.simulate_shared(
        panel, recs, spec["weights"], bars=bars, fees=fees, regime=regime,
        net=spec.get("net") or "priority", name=f"{spec['name']}:allow",
        hard_red_mode=fmc.HARD_RED_ALLOW)
    return {
        "sit": _book_card(sit, bars=bars, fees=fees),
        "allow": _book_card(allow, bars=bars, fees=fees),
    }


def fill_horizon_bars(panel: dict, bars: dict, cal: list[str],
                      red_dates: list[str]) -> dict:
    """Yahoo regular-session prints for red-day names whose exit is off parquet.

    Research overlay only. Does not write ``data/prices/ohlc.parquet``.
    Existing official opens are not overwritten.
    """
    bars = dict(bars or {})
    by_date = panel.get("by_date") or {}
    names = set()
    for d in red_dates:
        for r in by_date.get(d) or []:
            t = fm._tick(r.get("ticker"))
            if t:
                names.add(t)
    if not names or not cal:
        return bars
    need_dates = []
    for d in cal:
        n_close = sum(1 for t in names
                      if fm._finite((bars.get((t, d)) or {}).get("close"))
                      is not None)
        if n_close < max(3, len(names) // 8):
            need_dates.append(d)
    overlay_n = 0
    for d in need_dates:
        extra = hrs.yahoo_session_overlay(sorted(names), d)
        for key, bar in (extra or {}).items():
            cur = bars.get(key) or {}
            if fm._finite(cur.get("open")) is None or fm._finite(cur.get("close")) is None:
                merged = dict(cur)
                for k, v in bar.items():
                    if merged.get(k) is None:
                        merged[k] = v
                bars[key] = merged
                overlay_n += 1
    bars["_yahoo_overlay_n"] = overlay_n
    bars["_yahoo_overlay_dates"] = list(need_dates)
    return bars


def pick_disc_leaders(rows: list[dict], *, side: str | None,
                      limit: int = TOP_DISC) -> list[dict]:
    usable = []
    for r in rows:
        if side and (r.get("side") or "long") != side:
            continue
        if int((r.get("disc") or {}).get("n_graded") or 0) < 1:
            continue
        usable.append(r)
    # Prefer rows whose holdout actually graded — a hold-5 tease with
    # a blank hidden window is not a leader.
    graded = [r for r in usable
              if int((r.get("holdout") or {}).get("n_graded") or 0) >= 1]
    pool = graded or usable
    pool.sort(key=lambda r: _rank_key(r.get("disc") or {}), reverse=True)
    # One row per recipe name (best hold).
    seen = set()
    out = []
    for r in pool:
        base = r.get("parent") or r.get("name")
        if base in seen:
            continue
        seen.add(base)
        out.append(r)
        if len(out) >= limit:
            break
    return out


def pick_holdout_leaders(rows: list[dict], *,
                         limit: int = TOP_DISC) -> list[dict]:
    """Best hidden-window win% among rows that actually graded holdout."""
    usable = [
        r for r in rows
        if int((r.get("holdout") or {}).get("n_graded") or 0) >= 1
    ]
    usable.sort(key=lambda r: (
        1 if int((r.get("holdout") or {}).get("n_graded") or 0)
        >= RESEARCH_MIN_FIRES else 0,
        _rank_key(r.get("holdout") or {}),
    ), reverse=True)
    seen = set()
    out = []
    for r in usable:
        base = r.get("parent") or r.get("name")
        if base in seen:
            continue
        seen.add(base)
        out.append(r)
        if len(out) >= limit:
            break
    return out


def _pct(v) -> str:
    return "—" if v is None else f"{100.0 * float(v):.1f}%"


def _n(v) -> str:
    return "—" if v is None else f"{float(v):+.2f}"


def _row_md(r: dict) -> str:
    d = r.get("disc") or {}
    h = r.get("holdout") or {}
    a = r.get("all_red") or {}
    flag = "YES" if r.get("research_ok") else "no"
    keep = (r.get("live_keep") or {}).get("label") or "KILL"
    return (
        f"| `{r.get('name')}` | {r.get('side')} | {r.get('hold')} | "
        f"{d.get('n_graded') or 0}/{_pct(d.get('win_rate'))} | "
        f"{h.get('n_graded') or 0}/{_pct(h.get('win_rate'))} | "
        f"{a.get('n_graded') or 0}/{_pct(a.get('win_rate'))} | "
        f"{_n(a.get('pnl'))} | {flag} | {keep} |"
    )


def render_md(payload: dict) -> str:
    red = payload.get("hard_red_days") or []
    survivors = payload.get("survivors") or []
    leaders = payload.get("disc_leaders") or []
    books = payload.get("cash_books") or []
    live = payload.get("live_combo") or {}
    lines = [
        "# Hard-red strategy mine",
        "",
        "Research only. Same leak-free 09:30 catalog that produced the "
        "factor-mine combo sleeves. Live `flatten_robust` and Webull "
        f"`{LIVE_COMBO}` stay on full hard-red sit.",
        "",
        f"Window `{payload.get('from_date')} → {payload.get('to_date')}` "
        f"({payload.get('n_sessions')} sessions). Hard-red mornings "
        f"(S≤{fmb.HARD_RED:g}): **{len(red)}** — "
        + (", ".join(f"`{r['date']}` S={r['s']}" for r in red) or "none")
        + ".",
        "",
        f"Calendar time-split: cutoff `{payload.get('cutoff')}` "
        f"(hold_frac={payload.get('hold_frac')}). Discovery fires need "
        "the entry **and** the horizon close strictly before cutoff. "
        "Holdout = entry on/after cutoff. Missing open/exit fail closed."
        + (
            f" Yahoo regular-session overlay filled `{payload.get('yahoo_overlay_n') or 0}` "
            f"holes on {payload.get('yahoo_overlay_dates') or []} "
            "(research only — parquet was not written)."
            if payload.get("yahoo_overlay_n") else ""
        ),
        "",
        "## What this is grading",
        "",
        "Each recipe's 09:30 list on a hard-red morning, filled at the "
        "official open, exited at the hold-th session close (1 / 2 / 3 / 5). "
        "1 share, Futubull round-trip. Close grades — it does not trigger. "
        "A fat all-day Book% that sits every red morning is **not** a "
        "hard-red edge.",
        "",
        f"**Published KEEP** (same as HARD_RED_SIT): ≥{KEEP_MIN_FIRES} "
        f"fires and >{100 * KEEP_WIN:.0f}% after fees. Thin n is KILL. "
        "This board does not wire a live policy change.",
        "",
        f"**Research survivor** (reporting only): discovery and holdout "
        f"each ≥{RESEARCH_MIN_FIRES} graded fires and "
        f">{100 * RESEARCH_WIN:.0f}% after fees.",
        "",
        f"Recipes scored: **{payload.get('n_recipes')}**. "
        f"Combo specs: **{payload.get('n_combos')}**. "
        f"Name-day rows: **{payload.get('n_rows')}**.",
        "",
        "## Holdout survivors",
        "",
    ]
    if not survivors:
        lines += [
            "None. No recipe or combo cleared the research survivor bar "
            "on the hidden hard-red window. Live sit stays.",
            "",
        ]
    else:
        lines += [
            "| Sleeve | Side | Hold | Disc n/win | Holdout n/win | "
            "All-red n/win | All-red $ | Research | Live KEEP |",
            "|---|---|---:|---:|---:|---:|---:|---|---|",
        ]
        for r in survivors:
            lines.append(_row_md(r))
        lines.append("")
    lines += [
        "## Holdout leaders (hidden window, not a KEEP)",
        "",
        "Best after-fee win% on hard-red entries on/after cutoff. "
        "A holdout tease that missed discovery is still KILL — we "
        "would not have picked it with the earlier tape only.",
        "",
        "| Sleeve | Side | Hold | Disc n/win | Holdout n/win | "
        "All-red n/win | All-red $ | Research | Live KEEP |",
        "|---|---|---:|---:|---:|---:|---:|---|---|",
    ]
    for r in payload.get("holdout_leaders") or []:
        lines.append(_row_md(r))
    lines += [
        "",
        "## Discovery leaders (not confirmed)",
        "",
        "Best discovery win% at each recipe's best hold. Holdout is "
        "shown so a full-sample tease that dies hidden is visible.",
        "",
        "| Sleeve | Side | Hold | Disc n/win | Holdout n/win | "
        "All-red n/win | All-red $ | Research | Live KEEP |",
        "|---|---|---:|---:|---:|---:|---:|---|---|",
    ]
    for r in leaders:
        lines.append(_row_md(r))
    lines += [
        "",
        "## Live combo vs all-day sit book",
        "",
        f"`{LIVE_COMBO}` is the Webull paper sleeve. Sit is the "
        "published all-day cash book (red mornings take no new lots). "
        "Allow is the research counterfactual: same leftover book, "
        "09:30 fills on hard-red too.",
        "",
    ]
    sit = live.get("sit") or {}
    allow = live.get("allow") or {}
    lines += [
        "| Book | Book% | Book win | Hard-red fires | Hard-red win | Hard-red $ | Audit |",
        "|---|---:|---:|---:|---:|---:|---|",
        f"| sit (live / all-day) | {_n(sit.get('book_pct'))} | "
        f"{_pct(sit.get('win_rate'))} | "
        f"{(sit.get('hard_red') or {}).get('n_fires') or 0} | "
        f"{_pct((sit.get('hard_red') or {}).get('win_rate'))} | "
        f"{_n((sit.get('hard_red') or {}).get('pnl'))} | "
        f"{'PASS' if sit.get('audit_ok') else 'FAIL'} |",
        f"| allow (research) | {_n(allow.get('book_pct'))} | "
        f"{_pct(allow.get('win_rate'))} | "
        f"{(allow.get('hard_red') or {}).get('n_fires') or 0} | "
        f"{_pct((allow.get('hard_red') or {}).get('win_rate'))} | "
        f"{_n((allow.get('hard_red') or {}).get('pnl'))} | "
        f"{'PASS' if allow.get('audit_ok') else 'FAIL'} |",
        "",
        "## Cash books for research survivors / leaders",
        "",
        "Sit = published all-day path. Allow = trade through red at "
        "the 09:30 open. Hard-red $ is only the red-morning fills.",
        "",
        "| Sleeve | Sit Book% | Sit hard-red n/win | Allow Book% | "
        "Allow hard-red n/win | Allow hard-red $ |",
        "|---|---:|---:|---:|---:|---:|",
    ]
    if not books:
        lines.append("| — | — | — | — | — | — |")
    for b in books:
        sit = b.get("sit") or {}
        allow = b.get("allow") or {}
        lines.append(
            f"| `{b.get('name')}` | {_n(sit.get('book_pct'))} | "
            f"{(sit.get('hard_red') or {}).get('n_fires') or 0}/"
            f"{_pct((sit.get('hard_red') or {}).get('win_rate'))} | "
            f"{_n(allow.get('book_pct'))} | "
            f"{(allow.get('hard_red') or {}).get('n_fires') or 0}/"
            f"{_pct((allow.get('hard_red') or {}).get('win_rate'))} | "
            f"{_n((allow.get('hard_red') or {}).get('pnl'))} |"
        )
    lines += [
        "",
        "## Gate (do not change live)",
        "",
        "Hard-red S≤−3 still blocks new lots in "
        "`combo_broker.size_combo_tickets`, `simulate_shared` (default "
        "sit), and `simulate_book` / `flatten_robust`. This mine adds "
        "opt-in `hard_red_mode=allow` for research books only.",
        "",
        f"**Survivors: {len(survivors)}.** "
        + (payload.get("verdict_why") or "Live sit stands."),
        "",
    ]
    return "\n".join(lines)


def _best_hold_rows(rows: list[dict]) -> list[dict]:
    """One row per base name, best discovery win then holdout win."""
    by: dict[str, dict] = {}
    for r in rows:
        key = r.get("parent") or r.get("name")
        cur = by.get(key)
        if cur is None:
            by[key] = r
            continue
        a = (_rank_key(r.get("disc") or {}), _rank_key(r.get("holdout") or {}))
        b = (_rank_key(cur.get("disc") or {}), _rank_key(cur.get("holdout") or {}))
        if a > b:
            by[key] = r
    return list(by.values())


def run(*, from_date: str = book_era.DASHBOARD_START,
        to_date: str | None = None, write: bool = False,
        panel: dict | None = None, bars=None, fees=None,
        regime=None, recipes: list[dict] | None = None,
        combo_specs: list[dict] | None = None,
        cash: bool = True) -> dict:
    recipes = list(recipes or fm.build_recipes())
    rec_by = {r["name"]: r for r in recipes}
    specs = list(combo_specs if combo_specs is not None else fmc.combo_specs())
    if panel is None:
        panel = fm.load_or_build_panel(from_date, to_date)
    cal = [d for d in (panel.get("session_dates") or [])
           if d >= from_date and (not to_date or d <= to_date)]
    built_bars = bars is None
    if bars is None:
        bars = wf.preload_bars(panel)
    overlay_n = 0
    overlay_dates = []
    fees = fees if fees is not None else pt.load_fees()
    if regime is None:
        try:
            regime = fmb.load_regime()
        except Exception:
            regime = {}
    red = hrs.hard_red_dates(cal, regime)
    if built_bars:
        bars = fill_horizon_bars(
            panel, bars, cal, [r["date"] for r in red])
        overlay_n = int((bars or {}).pop("_yahoo_overlay_n", 0) or 0)
        overlay_dates = list((bars or {}).pop("_yahoo_overlay_dates", []) or [])
    red_dates = [r["date"] for r in red]
    cutoff = time_split_cutoff(cal)
    rows: list[dict] = []
    for rec in recipes:
        rows.extend(score_horizons(
            panel, rec, red_dates, cal, cutoff,
            bars=bars, fees=fees))
    for spec in specs:
        try:
            recs = fmc.recs_for_spec(spec, rec_by)
        except Exception:
            continue
        rows.extend(score_combo_horizons(
            panel, spec, recs, red_dates, cal, cutoff,
            bars=bars, fees=fees))

    survivors = [r for r in rows if r.get("research_ok")]
    survivors.sort(key=lambda r: (
        _rank_key(r.get("holdout") or {}),
        _rank_key(r.get("disc") or {}),
    ), reverse=True)
    leaders = pick_disc_leaders(rows, side=None, limit=TOP_DISC)
    hold_leaders = pick_holdout_leaders(rows, limit=TOP_DISC)
    live_books = {}
    cash_books = []
    if cash:
        live_spec = next((s for s in specs if s["name"] == LIVE_COMBO), None)
        if live_spec is not None:
            try:
                live_recs = fmc.recs_for_spec(live_spec, rec_by)
                live_books = cash_compare_combo(
                    panel, live_recs, live_spec,
                    bars=bars, fees=fees, regime=regime)
            except Exception as e:
                live_books = {"error": str(e)}
        want = []
        seen = set()
        for r in survivors[:TOP_COMBOS] + leaders[:6] + hold_leaders[:4]:
            name = r.get("parent") or r.get("name")
            if name in seen:
                continue
            seen.add(name)
            want.append(name)
        for name in want:
            if name in rec_by:
                try:
                    card = cash_compare(
                        panel, rec_by[name], bars=bars, fees=fees,
                        regime=regime)
                    card["name"] = name
                    cash_books.append(card)
                except Exception as e:
                    cash_books.append({"name": name, "error": str(e)})
            else:
                spec = next((s for s in specs if s["name"] == name), None)
                if spec is None:
                    continue
                try:
                    recs = fmc.recs_for_spec(spec, rec_by)
                    card = cash_compare_combo(
                        panel, recs, spec, bars=bars, fees=fees,
                        regime=regime)
                    card["name"] = name
                    cash_books.append(card)
                except Exception as e:
                    cash_books.append({"name": name, "error": str(e)})

    n_keep = sum(1 for r in rows if (r.get("live_keep") or {}).get("keep"))
    if survivors:
        why = (
            f"{len(survivors)} research survivor(s) on the hidden "
            f"hard-red window. Published KEEP still "
            f"{'hit' if n_keep else 'missed'} "
            f"(≥{KEEP_MIN_FIRES} fires, >{100 * KEEP_WIN:.0f}%). "
            "Do not change live sit from this board."
        )
    else:
        why = (
            "No holdout survivor. Discovery teases that miss the hidden "
            "red window stay KILL. Live sit stands."
        )
    payload = {
        "generated_at": datetime.now(tl.ET).isoformat(),
        "live_untouched": ["flatten_robust", LIVE_COMBO],
        "from_date": cal[0] if cal else from_date,
        "to_date": cal[-1] if cal else to_date,
        "n_sessions": len(cal),
        "hard_red_n": len(red),
        "hard_red_days": red,
        "cutoff": cutoff,
        "hold_frac": HOLD_FRAC,
        "yahoo_overlay_n": overlay_n,
        "yahoo_overlay_dates": overlay_dates,
        "horizons": list(HORIZONS),
        "keep_bar": {"min_fires": KEEP_MIN_FIRES, "win": KEEP_WIN},
        "research_bar": {
            "min_fires": RESEARCH_MIN_FIRES, "win": RESEARCH_WIN,
        },
        "n_recipes": len(recipes),
        "n_combos": len(specs),
        "n_rows": len(rows),
        "n_research_ok": len(survivors),
        "n_live_keep": n_keep,
        "survivors": survivors,
        "disc_leaders": leaders,
        "holdout_leaders": hold_leaders,
        "live_combo": live_books,
        "cash_books": cash_books,
        "verdict_why": why,
        "top_all_red": sorted(
            _best_hold_rows(rows),
            key=lambda r: _rank_key(r.get("all_red") or {}),
            reverse=True,
        )[:20],
    }
    if write:
        OUT_DIR.mkdir(parents=True, exist_ok=True)
        OUT_JSON.write_text(json.dumps(payload, indent=2), encoding="utf-8")
        OUT_MD.write_text(render_md(payload), encoding="utf-8")
        (ROOT / "03_scoreboard" / "HARD_RED_MINE.md").write_text(
            render_md(payload), encoding="utf-8")
    return payload


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(
        description="Mine 09:30 recipes that still work on hard-red days "
                    "(research only — does not change live sit).")
    ap.add_argument("--from-date", default=book_era.DASHBOARD_START)
    ap.add_argument("--to-date", default=None)
    ap.add_argument("--write", action="store_true")
    ap.add_argument("--no-cash", action="store_true",
                    help="skip $10k sit/allow cash books")
    args = ap.parse_args(argv)
    payload = run(
        from_date=args.from_date, to_date=args.to_date or None,
        write=args.write, cash=not args.no_cash)
    print(
        f"[hard-red-mine] sessions={payload.get('n_sessions')} "
        f"hard-red={payload.get('hard_red_n')} "
        f"cutoff={payload.get('cutoff')} "
        f"rows={payload.get('n_rows')} "
        f"survivors={payload.get('n_research_ok')} "
        f"live_keep={payload.get('n_live_keep')}",
        flush=True,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
