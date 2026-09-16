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
from statistics import median

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
X_GRID = fmc.DIP_GRID
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


def _ohlc_open_ext(ticker: str, date: str, bars) -> tuple:
    """Official open / high / low. Close is never returned."""
    o = fmb._px(ticker, date, "open", bars)
    high = fmb._px(ticker, date, "high", bars)
    low = fmb._px(ticker, date, "low", bars)
    if o is None or high is None or low is None:
        store = hrs.clock_bar(ticker, date, None)
        if o is None:
            o = store.get("open")
        if high is None:
            high = store.get("high")
        if low is None:
            low = store.get("low")
    return o, high, low


def _horizon_px(cal: list[str], date: str, hold: int, ticker: str, bars):
    exit_px, exit_d, how = hrs.horizon_exit(cal, date, hold, ticker, bars)
    if exit_px is None and exit_d:
        store = hrs.clock_bar(ticker, exit_d, None)
        exit_px = store.get("close")
        if exit_px is None:
            exit_px = store.get("open")
            how = "horizon_open_store" if exit_px is not None else how
        else:
            how = "horizon_close_store"
    return exit_px, exit_d, how


def intraday_fires(panel: dict, rec: dict, red_dates: list[str],
                   cal: list[str], *, bars=None, fees=None,
                   hold: int, x_pct: float, trigger: str) -> list[dict]:
    """Limit entry after 09:30. ``scoop`` = long open−X%; ``fade`` = short open+X%.

    Trigger uses session low / high only. Close grades. A miss is not a fire.
    Daily OHLC is a first-hit proxy — slightly optimistic vs a live monitor.
    """
    fees = fees if fees is not None else pt.load_fees()
    trigger = "fade" if trigger == "fade" else "scoop"
    side = "short" if trigger == "fade" else "long"
    by_date = panel.get("by_date") or {}
    out = []
    for date in red_dates:
        for r in fm.pick_day(by_date.get(date) or [], rec):
            t = fm._tick(r.get("ticker"))
            if not t:
                continue
            o, high, low = _ohlc_open_ext(t, date, bars)
            if trigger == "scoop":
                fill, kind = fmc.dip_limit_px(o, low, x_pct)
            else:
                fill, kind = fmc.rally_limit_px(o, high, x_pct)
            if fill is None:
                continue
            exit_px, exit_d, how = _horizon_px(cal, date, hold, t, bars)
            pnl = None
            if exit_px is not None:
                pnl = round(hrs.after_fee_pnl(
                    1, float(fill), float(exit_px), side=side, fees=fees), 4)
            out.append({
                "date": date, "ticker": t, "side": side,
                "hold": hold, "x_pct": x_pct, "trigger": trigger,
                "entry": round(float(fill), 4),
                "open": None if o is None else round(float(o), 4),
                "exit": None if exit_px is None else round(float(exit_px), 4),
                "exit_date": exit_d, "exit_how": how,
                "kind": kind,
                "pnl": pnl,
                "win": None if pnl is None else bool(pnl > 0),
                "recipe": rec.get("name"),
            })
    return out


def score_intraday(panel: dict, rec: dict, red_dates: list[str],
                   cal: list[str], cutoff: str | None, *,
                   bars=None, fees=None,
                   holds: tuple[int, ...] = HORIZONS,
                   xs: tuple = X_GRID) -> list[dict]:
    rows = []
    for trigger in ("scoop", "fade"):
        side = "short" if trigger == "fade" else "long"
        for x in xs:
            for hold in holds:
                fires = intraday_fires(
                    panel, rec, red_dates, cal, bars=bars, fees=fees,
                    hold=hold, x_pct=x, trigger=trigger)
                rows.append(score_flip_row(
                    fires, cutoff,
                    name=f"{rec.get('name')}_{trigger}_x{x:g}_h{hold}",
                    hold=hold, side=side, kind=trigger))
                rows[-1]["x_pct"] = x
                rows[-1]["trigger"] = trigger
                rows[-1]["parent"] = f"{rec.get('name')}:{trigger}"
                rows[-1]["list"] = rec.get("name")
    return rows


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


def flip_recipe(rec: dict) -> dict:
    """Same 09:30 list, opposite side. Research only — not a live recipe."""
    base = dict(rec)
    side = str(base.get("side") or "long")
    base["side"] = "short" if side != "short" else "long"
    name = str(base.get("name") or "rec")
    if not name.endswith("_flip"):
        base["name"] = f"{name}_flip"
    base["note"] = f"polarity flip of {rec.get('name')} (research)"
    base["flipped_from"] = rec.get("name")
    return base


def flip_fires(fires: list[dict], *, fees=None) -> list[dict]:
    """Re-grade the same name-days on the opposite side. Fees re-applied."""
    fees = fees if fees is not None else pt.load_fees()
    out = []
    for f in fires or []:
        rec = dict(f)
        side = "short" if (f.get("side") or "long") != "short" else "long"
        rec["side"] = side
        rec["recipe"] = f"{f.get('recipe') or ''}_flip"
        entry, exit_px = f.get("entry"), f.get("exit")
        pnl = None
        if entry is not None and exit_px is not None:
            pnl = round(hrs.after_fee_pnl(
                1, float(entry), float(exit_px), side=side, fees=fees), 4)
        rec["pnl"] = pnl
        rec["win"] = None if pnl is None else bool(pnl > 0)
        out.append(rec)
    return out


def both_lose(entry, exit_px, *, fees) -> bool | None:
    """True when the move is inside the Futubull round-trip — both sides lose."""
    if entry is None or exit_px is None:
        return None
    long_pnl = hrs.after_fee_pnl(
        1, float(entry), float(exit_px), side="long", fees=fees)
    short_pnl = hrs.after_fee_pnl(
        1, float(entry), float(exit_px), side="short", fees=fees)
    return bool(long_pnl <= 0 and short_pnl <= 0)


def session_tape(panel: dict, date: str, *, bars=None, fees=None) -> dict:
    """Median 09:30→16:00 of names on that morning's panel, plus fee dead-zone."""
    fees = fees if fees is not None else pt.load_fees()
    rows = (panel.get("by_date") or {}).get(date) or []
    xs, longs, shorts, dead, graded = [], 0, 0, 0, 0
    seen: set[str] = set()
    for r in rows:
        t = fm._tick(r.get("ticker"))
        if not t or t in seen:
            continue
        seen.add(t)
        o = fmb._px(t, date, "open", bars)
        c = fmb._px(t, date, "close", bars)
        if o is None or c is None or o <= 0:
            if o is None or c is None:
                store = hrs.clock_bar(t, date, None)
                o = o if o is not None else store.get("open")
                c = c if c is not None else store.get("close")
            if o is None or c is None or o <= 0:
                continue
        xs.append(100.0 * (float(c) / float(o) - 1.0))
        lp = hrs.after_fee_pnl(1, float(o), float(c), side="long", fees=fees)
        sp = hrs.after_fee_pnl(1, float(o), float(c), side="short", fees=fees)
        graded += 1
        if lp > 0:
            longs += 1
        if sp > 0:
            shorts += 1
        if lp <= 0 and sp <= 0:
            dead += 1
    return {
        "date": date,
        "n": len(xs),
        "median_oc": None if not xs else round(float(median(xs)), 4),
        "mean_oc": None if not xs else round(sum(xs) / len(xs), 4),
        "pct_up": None if not xs else round(
            sum(1 for x in xs if x > 0) / len(xs), 4),
        "n_graded": graded,
        "long_win": None if not graded else round(longs / graded, 4),
        "short_win": None if not graded else round(shorts / graded, 4),
        "dead_zone": None if not graded else round(dead / graded, 4),
        "n_dead": dead,
    }


def tape_split(panel: dict, cal: list[str], red_dates: list[str],
               cutoff: str | None, *, bars=None, fees=None) -> dict:
    """Hard-red vs other mornings, discovery vs holdout."""
    red_set = set(red_dates)
    days = [session_tape(panel, d, bars=bars, fees=fees) for d in cal]
    def _pool(dates: list[str]) -> dict:
        recs = [d for d in days if d["date"] in dates and d.get("n")]
        meds = [d["median_oc"] for d in recs if d.get("median_oc") is not None]
        ups = [d["pct_up"] for d in recs if d.get("pct_up") is not None]
        dead = [d["dead_zone"] for d in recs if d.get("dead_zone") is not None]
        lw = [d["long_win"] for d in recs if d.get("long_win") is not None]
        sw = [d["short_win"] for d in recs if d.get("short_win") is not None]
        return {
            "n_days": len(recs),
            "median_of_medians": None if not meds else round(float(median(meds)), 4),
            "mean_pct_up": None if not ups else round(sum(ups) / len(ups), 4),
            "mean_long_win": None if not lw else round(sum(lw) / len(lw), 4),
            "mean_short_win": None if not sw else round(sum(sw) / len(sw), 4),
            "mean_dead_zone": None if not dead else round(sum(dead) / len(dead), 4),
            "days_up": sum(1 for m in meds if m > 0),
            "days_down": sum(1 for m in meds if m < 0),
        }
    other = [d for d in cal if d not in red_set]
    disc_red = [d for d in red_dates if cutoff and d < cutoff]
    hold_red = [d for d in red_dates if cutoff and d >= cutoff]
    return {
        "hard_red": _pool(red_dates),
        "other": _pool(other),
        "disc_red": _pool(disc_red),
        "holdout_red": _pool(hold_red),
        "days": days,
    }


def panel_intraday_tape(panel: dict, red_dates: list[str], cal: list[str],
                        cutoff: str | None, *, bars=None, fees=None,
                        xs: tuple = X_GRID) -> dict:
    """Whole-panel scoop / fade, same-day close. Not a recipe list."""
    fees = fees if fees is not None else pt.load_fees()
    by_date = panel.get("by_date") or {}
    out = {}
    for x in xs:
        scoop, fade = [], []
        for date in red_dates:
            seen: set[str] = set()
            for r in by_date.get(date) or []:
                t = fm._tick(r.get("ticker"))
                if not t or t in seen:
                    continue
                seen.add(t)
                o, high, low = _ohlc_open_ext(t, date, bars)
                c = fmb._px(t, date, "close", bars)
                if c is None:
                    c = hrs.clock_bar(t, date, None).get("close")
                for trigger, fill, side, bucket in (
                    ("scoop", fmc.dip_limit_px(o, low, x)[0], "long", scoop),
                    ("fade", fmc.rally_limit_px(o, high, x)[0], "short", fade),
                ):
                    if fill is None or c is None:
                        continue
                    pnl = round(hrs.after_fee_pnl(
                        1, float(fill), float(c), side=side, fees=fees), 4)
                    bucket.append({
                        "date": date, "ticker": t, "side": side,
                        "hold": 1, "entry": fill, "exit": c,
                        "exit_date": date, "pnl": pnl,
                        "win": pnl > 0,
                    })
        parts_s = split_fires(scoop, cutoff)
        parts_f = split_fires(fade, cutoff)
        out[str(x)] = {
            "x_pct": x,
            "scoop": {
                "all": slim_stats(hrs.fire_stats(scoop)),
                "disc": slim_stats(hrs.fire_stats(parts_s["disc"])),
                "holdout": slim_stats(hrs.fire_stats(parts_s["hold"])),
            },
            "fade": {
                "all": slim_stats(hrs.fire_stats(fade)),
                "disc": slim_stats(hrs.fire_stats(parts_f["disc"])),
                "holdout": slim_stats(hrs.fire_stats(parts_f["hold"])),
            },
        }
    return out


def score_flip_row(fires: list[dict], cutoff: str | None, *,
                   name: str, hold, side: str, kind: str = "flip") -> dict:
    parts = split_fires(fires, cutoff)
    disc = hrs.fire_stats(parts["disc"])
    hold_st = hrs.fire_stats(parts["hold"])
    all_st = hrs.fire_stats(fires)
    return {
        "name": name,
        "side": side,
        "hold": hold,
        "kind": kind,
        "disc": slim_stats(disc),
        "holdout": slim_stats(hold_st),
        "all_red": slim_stats(all_st),
        "research_ok": research_survivor(disc=disc, hold=hold_st),
        "live_keep": live_keep(
            n_fires=all_st.get("n_fires") or 0,
            win_rate=all_st.get("win_rate")),
    }


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
        flipped = flip_fires(fires, fees=fees)
        flip_side = "short" if (rec.get("side") or "long") != "short" else "long"
        rows.append(score_flip_row(
            flipped, cutoff,
            name=f"{rec.get('name')}_flip",
            hold=hold, side=flip_side, kind="flip"))
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
    rows.append(score_flip_row(
        flip_fires(baked, fees=fees), cutoff,
        name=f"{spec['name']}_flip", hold="baked",
        side="mix", kind="combo_flip"))
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
        rows.append(score_flip_row(
            flip_fires(fires, fees=fees), cutoff,
            name=f"{spec['name']}_h{hold}_flip", hold=hold,
            side="mix", kind="combo_flip"))
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


def _render_tape(tape: dict, red: list[dict]) -> list[str]:
    hr = tape.get("hard_red") or {}
    ot = tape.get("other") or {}
    dr = tape.get("disc_red") or {}
    ho = tape.get("holdout_red") or {}
    by_day = {d.get("date"): d for d in (tape.get("days") or [])}
    lines = [
        "| Window | Days | Median OC | Days up/down | Long win | Short win | Both lose (fees) |",
        "|---|---:|---:|---:|---:|---:|---:|",
        f"| Hard-red | {hr.get('n_days') or 0} | {_n(hr.get('median_of_medians'))}% | "
        f"{hr.get('days_up') or 0}/{hr.get('days_down') or 0} | "
        f"{_pct(hr.get('mean_long_win'))} | {_pct(hr.get('mean_short_win'))} | "
        f"{_pct(hr.get('mean_dead_zone'))} |",
        f"| Discovery red | {dr.get('n_days') or 0} | {_n(dr.get('median_of_medians'))}% | "
        f"{dr.get('days_up') or 0}/{dr.get('days_down') or 0} | "
        f"{_pct(dr.get('mean_long_win'))} | {_pct(dr.get('mean_short_win'))} | "
        f"{_pct(dr.get('mean_dead_zone'))} |",
        f"| Holdout red | {ho.get('n_days') or 0} | {_n(ho.get('median_of_medians'))}% | "
        f"{ho.get('days_up') or 0}/{ho.get('days_down') or 0} | "
        f"{_pct(ho.get('mean_long_win'))} | {_pct(ho.get('mean_short_win'))} | "
        f"{_pct(ho.get('mean_dead_zone'))} |",
        f"| Other mornings | {ot.get('n_days') or 0} | {_n(ot.get('median_of_medians'))}% | "
        f"{ot.get('days_up') or 0}/{ot.get('days_down') or 0} | "
        f"{_pct(ot.get('mean_long_win'))} | {_pct(ot.get('mean_short_win'))} | "
        f"{_pct(ot.get('mean_dead_zone'))} |",
        "",
        "Per hard-red morning (panel names, 1-share after fees, same-day close):",
        "",
        "| Date | S | Median OC | % up | Long win | Short win | Both lose | n |",
        "|---|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for r in red:
        d = by_day.get(r.get("date")) or {}
        lines.append(
            f"| `{r.get('date')}` | {r.get('s')} | {_n(d.get('median_oc'))}% | "
            f"{_pct(d.get('pct_up'))} | {_pct(d.get('long_win'))} | "
            f"{_pct(d.get('short_win'))} | {_pct(d.get('dead_zone'))} | "
            f"{d.get('n') or 0} |"
        )
    lines += ["", ""]
    return lines


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
        f"Name-day rows: **{payload.get('n_rows')}** "
        f"(includes polarity flips).",
        "",
        "## Why the lists lose",
        "",
        "Morning S is already known at 09:30. The gap has printed. "
        "These books grade the **residual** (official open → horizon close), "
        "not the overnight. That residual is not one-way: some hard-red "
        "mornings bounce after the open, some keep dumping. The 09:30 "
        "lists are yesterday-hot / camera-green names — buying yesterday's "
        "winners into a red weather print. Fees eat the small moves: "
        "when the open→close is inside the Futubull round-trip, "
        "**long and short both lose**.",
        "",
        *_render_tape(payload.get("tape") or {}, red),
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
        "## Polarity flips (fade the same 09:30 list)",
        "",
        "Same names, opposite side, same fees and time-split. "
        "A long that lost 60% of the time is **not** automatically a "
        "40% short — the fee dead-zone makes both sides lose on small "
        "moves. Flip survivors still need discovery **and** holdout "
        f">{100 * RESEARCH_WIN:.0f}% on ≥{RESEARCH_MIN_FIRES} fires.",
        "",
    ]
    flips = payload.get("flip_survivors") or []
    if not flips:
        lines += [
            f"**Flip survivors: {payload.get('n_flip_ok') or 0}.** "
            "No polarity flip cleared both windows.",
            "",
        ]
    else:
        lines += [
            f"**Flip survivors: {len(flips)}.**",
            "",
            "| Sleeve | Side | Hold | Disc n/win | Holdout n/win | "
            "All-red n/win | All-red $ | Research | Live KEEP |",
            "|---|---|---:|---:|---:|---:|---:|---|---|",
        ]
        for r in flips:
            lines.append(_row_md(r))
        lines.append("")
    lines += [
        "Flip discovery leaders (not confirmed):",
        "",
        "| Sleeve | Side | Hold | Disc n/win | Holdout n/win | "
        "All-red n/win | All-red $ | Research | Live KEEP |",
        "|---|---|---:|---:|---:|---:|---:|---|---|",
    ]
    for r in payload.get("flip_disc_leaders") or []:
        lines.append(_row_md(r))
    lines += [
        "",
        "Flip holdout leaders (hidden window, not a KEEP):",
        "",
        "| Sleeve | Side | Hold | Disc n/win | Holdout n/win | "
        "All-red n/win | All-red $ | Research | Live KEEP |",
        "|---|---|---:|---:|---:|---:|---:|---|---|",
    ]
    for r in payload.get("flip_holdout_leaders") or []:
        lines.append(_row_md(r))
    lines += [
        "",
        "## After the open: dip-scoop and rally-fade",
        "",
        "Assume we watch the official print in real time. "
        "**Scoop** = long the first touch of open−X% (session low as "
        "the daily first-hit proxy). **Fade** = short the first touch "
        "of open+X% (session high). Close / last never trigger; they "
        "only grade. Daily OHLC cannot prove the print was after 09:30, "
        "so these fills are slightly optimistic vs a live monitor. "
        "A name that never tags X% is a skip, not a fire.",
        "",
        "Whole-panel same-day (every 09:30 name, not a recipe filter):",
        "",
        "| X | Scoop n/win disc | Scoop n/win holdout | "
        "Fade n/win disc | Fade n/win holdout |",
        "|---:|---:|---:|---:|---:|",
    ]
    for x in X_GRID:
        rec = (payload.get("intraday_tape") or {}).get(str(x)) or {}
        sc, fd = rec.get("scoop") or {}, rec.get("fade") or {}
        lines.append(
            f"| {x:g}% | "
            f"{(sc.get('disc') or {}).get('n_graded') or 0}/"
            f"{_pct((sc.get('disc') or {}).get('win_rate'))} | "
            f"{(sc.get('holdout') or {}).get('n_graded') or 0}/"
            f"{_pct((sc.get('holdout') or {}).get('win_rate'))} | "
            f"{(fd.get('disc') or {}).get('n_graded') or 0}/"
            f"{_pct((fd.get('disc') or {}).get('win_rate'))} | "
            f"{(fd.get('holdout') or {}).get('n_graded') or 0}/"
            f"{_pct((fd.get('holdout') or {}).get('win_rate'))} |"
        )
    n_in = payload.get("n_intraday_ok") or 0
    lines += [
        "",
        f"**Intraday recipe survivors: {n_in}.** "
        "Same KEEP / research bars. Recipe lists are the 09:30 names; "
        "the trigger is the limit after the open.",
        "",
    ]
    if payload.get("intraday_survivors"):
        lines += [
            "| Sleeve | Side | Hold | Disc n/win | Holdout n/win | "
            "All-red n/win | All-red $ | Research | Live KEEP |",
            "|---|---|---:|---:|---:|---:|---:|---|---|",
        ]
        for r in payload.get("intraday_survivors") or []:
            lines.append(_row_md(r))
        lines.append("")
    lines += [
        "Scoop discovery leaders:",
        "",
        "| Sleeve | Side | Hold | Disc n/win | Holdout n/win | "
        "All-red n/win | All-red $ | Research | Live KEEP |",
        "|---|---|---:|---:|---:|---:|---:|---|---|",
    ]
    for r in payload.get("scoop_disc_leaders") or []:
        lines.append(_row_md(r))
    lines += [
        "",
        "Scoop holdout leaders:",
        "",
        "| Sleeve | Side | Hold | Disc n/win | Holdout n/win | "
        "All-red n/win | All-red $ | Research | Live KEEP |",
        "|---|---|---:|---:|---:|---:|---:|---|---|",
    ]
    for r in payload.get("scoop_holdout_leaders") or []:
        lines.append(_row_md(r))
    lines += [
        "",
        "Fade discovery leaders:",
        "",
        "| Sleeve | Side | Hold | Disc n/win | Holdout n/win | "
        "All-red n/win | All-red $ | Research | Live KEEP |",
        "|---|---|---:|---:|---:|---:|---:|---|---|",
    ]
    for r in payload.get("fade_disc_leaders") or []:
        lines.append(_row_md(r))
    lines += [
        "",
        "Fade holdout leaders:",
        "",
        "| Sleeve | Side | Hold | Disc n/win | Holdout n/win | "
        "All-red n/win | All-red $ | Research | Live KEEP |",
        "|---|---|---:|---:|---:|---:|---:|---|---|",
    ]
    for r in payload.get("fade_holdout_leaders") or []:
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

    intra: list[dict] = []
    for rec in recipes:
        intra.extend(score_intraday(
            panel, rec, red_dates, cal, cutoff,
            bars=bars, fees=fees))
    intra_tape = panel_intraday_tape(
        panel, red_dates, cal, cutoff, bars=bars, fees=fees)
    intra_survivors = [r for r in intra if r.get("research_ok")]
    intra_survivors.sort(key=lambda r: (
        _rank_key(r.get("holdout") or {}),
        _rank_key(r.get("disc") or {}),
    ), reverse=True)
    scoop_rows = [r for r in intra if r.get("trigger") == "scoop"]
    fade_rows = [r for r in intra if r.get("trigger") == "fade"]
    scoop_disc = pick_disc_leaders(scoop_rows, side=None, limit=TOP_DISC)
    scoop_hold = pick_holdout_leaders(scoop_rows, limit=TOP_DISC)
    fade_disc = pick_disc_leaders(fade_rows, side=None, limit=TOP_DISC)
    fade_hold = pick_holdout_leaders(fade_rows, limit=TOP_DISC)

    tape = tape_split(
        panel, cal, red_dates, cutoff, bars=bars, fees=fees)
    flip_rows = [
        r for r in rows
        if str(r.get("kind") or "") in ("flip", "combo_flip")
        or str(r.get("name") or "").endswith("_flip")
    ]
    flip_survivors = [r for r in flip_rows if r.get("research_ok")]
    flip_leaders = pick_disc_leaders(flip_rows, side=None, limit=TOP_DISC)
    flip_hold_leaders = pick_holdout_leaders(flip_rows, limit=TOP_DISC)
    survivors = [r for r in list(rows) + intra if r.get("research_ok")]
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
            "No holdout survivor — including polarity flips, existing "
            "shorts, dip-scoops, and rally-fades. Discovery teases that "
            "miss the hidden red window stay KILL. Live sit stands."
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
        "n_flip_ok": len(flip_survivors),
        "n_live_keep": n_keep,
        "survivors": survivors,
        "flip_survivors": flip_survivors,
        "flip_disc_leaders": flip_leaders,
        "flip_holdout_leaders": flip_hold_leaders,
        "tape": tape,
        "intraday_n": len(intra),
        "intraday_survivors": intra_survivors,
        "n_intraday_ok": len(intra_survivors),
        "scoop_disc_leaders": scoop_disc,
        "scoop_holdout_leaders": scoop_hold,
        "fade_disc_leaders": fade_disc,
        "fade_holdout_leaders": fade_hold,
        "intraday_tape": intra_tape,
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
        f"live_keep={payload.get('n_live_keep')} "
        f"intraday_ok={payload.get('n_intraday_ok')}",
        flush=True,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
