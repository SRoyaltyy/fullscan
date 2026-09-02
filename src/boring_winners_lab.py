"""Scenario lab for the book × mine overlay.

Replays the same panel as boring_winners_backtest, then runs named
sleeves that change one knob at a time:

  source     book | overlay | mine
  seats      10 / 25 / 50
  hold       1 / 2 / 3 / 5 trading sessions (locked names keep their seat)
  color      all | blue | green (panel cond==good)
  hard_red   none | stand_down | haircut_5 | limit_5

Hold-N: a name bought on D cannot be sold until it has aged N sessions.
New candidates only fill empty seats. If the book is full of locked
names, today's buys are skipped (n_skip).

hard_red (lattice live from 2026-08-31):
  stand_down  no new buys
  haircut_5   new buys assume a 5% cheaper fill (+5 on that day's 1d)
  limit_5     new buy only if that name's 1d is ≤ −5 (limit actually filled)

Does not rebuild the mine parquet. Does not overwrite the paper-trading
dashboard — writes dashboard/boring-winners/index.html.
"""
from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import pandas as pd

from src import bt_report
from src.boring_winners_backtest import (
    CLIP,
    MAX_EXTRAS,
    SEATS,
    _pack_seat,
    fill_overlay,
    fill_returns_from_finviz,
    fill_seats,
    load_book_buys,
    load_book_universe,
    load_finviz_px,
    load_panel,
)
from src.gainer_asof import load_day_context

ROOT = Path(__file__).resolve().parent.parent
OUT_JSON = ROOT / "03_scoreboard" / "boring_winners_lab.json"
OUT_MD = ROOT / "03_scoreboard" / "BORING_WINNERS_LAB.md"
OUT_CSV = ROOT / "03_scoreboard" / "boring_winners_lab_daily.csv"
DASH_DIR = ROOT / "dashboard" / "boring-winners"
TEMPLATE = Path(__file__).with_name("boring_winners_dash.html")
CAPITAL = 10_000.0


@dataclass(frozen=True)
class Spec:
    id: str
    label: str
    source: str = "overlay"       # book | overlay | mine
    seats: int = 25
    hold: int = 1
    color: str = "all"            # all | blue | green
    hard_red: str = "none"        # none | stand_down | haircut_5 | limit_5
    fade_veto: bool = True


# Named grid — not a full cartesian. Each row changes one idea.
SCENARIOS = (
    Spec("book_25_h1", "book 25 · daily", source="book", seats=25, hold=1),
    Spec("overlay_25_h1", "overlay 25 · daily (live)", source="overlay", seats=25, hold=1),
    Spec("mine_25_h1", "mine 25 · daily", source="mine", seats=25, hold=1),
    Spec("overlay_10_h1", "overlay 10 · daily", source="overlay", seats=10, hold=1),
    Spec("overlay_50_h1", "overlay 50 · daily", source="overlay", seats=50, hold=1),
    Spec("overlay_25_h2", "overlay 25 · hold 2d", source="overlay", seats=25, hold=2),
    Spec("overlay_25_h3", "overlay 25 · hold 3d", source="overlay", seats=25, hold=3),
    Spec("overlay_25_h5", "overlay 25 · hold 1w", source="overlay", seats=25, hold=5),
    Spec("book_25_h3", "book 25 · hold 3d", source="book", seats=25, hold=3),
    Spec("overlay_10_h3", "overlay 10 · hold 3d", source="overlay", seats=10, hold=3),
    Spec("overlay_25_h1_blue", "overlay 25 · blue only", source="overlay", seats=25, hold=1, color="blue"),
    Spec("overlay_25_h1_green", "overlay 25 · green cond", source="overlay", seats=25, hold=1, color="green"),
    Spec("overlay_10_h1_green", "overlay 10 · green cond", source="overlay", seats=10, hold=1, color="green"),
    Spec("overlay_25_h3_blue", "overlay 25 · hold 3d · blue", source="overlay", seats=25, hold=3, color="blue"),
    Spec("overlay_25_h1_stand", "overlay 25 · HARD_RED stand-down", source="overlay", seats=25, hold=1, hard_red="stand_down"),
    Spec("overlay_25_h1_cut5", "overlay 25 · HARD_RED −5% fill", source="overlay", seats=25, hold=1, hard_red="haircut_5"),
    Spec("overlay_25_h1_lim5", "overlay 25 · HARD_RED limit −5%", source="overlay", seats=25, hold=1, hard_red="limit_5"),
    Spec("overlay_50_h1_cut5", "overlay 50 · HARD_RED −5% fill", source="overlay", seats=50, hold=1, hard_red="haircut_5"),
)


def extras_for(seats: int) -> int:
    return max(MAX_EXTRAS, seats // 5)


def market_is_hard(date: str, ctx_cache: dict | None = None) -> bool:
    cache = ctx_cache if ctx_cache is not None else {}
    if date not in cache:
        cache[date] = load_day_context(date)
    return str((cache[date] or {}).get("market_state") or "").lower() == "hard_red"


def filter_color(packs: list[dict], color: str) -> list[dict]:
    if color == "all":
        return packs
    out = []
    for p in packs:
        r = p.get("row")
        if r is None:
            continue
        if color == "blue" and bool(r.get("blue")):
            out.append(p)
        elif color == "green" and str(r.get("cond") or "") == "good":
            out.append(p)
    return out


def candidates(date: str, g: pd.DataFrame, spec: Spec) -> list[dict]:
    buys = load_book_buys(date, top=max(spec.seats, 25))
    universe = load_book_universe(date)
    by_t = {str(r["Ticker"]).upper(): r for _, r in g.iterrows()} if len(g) else {}
    if spec.source == "book":
        packs = []
        for t in buys[: spec.seats]:
            p = _pack_seat(t, "book", by_t.get(t), universe.get(t))
            if spec.fade_veto and p.get("fade"):
                continue
            packs.append(p)
    elif spec.source == "mine":
        seats, _ = fill_seats(g, seats=spec.seats)
        packs = []
        if len(seats):
            for _, r in seats.iterrows():
                t = str(r["Ticker"]).upper()
                packs.append(_pack_seat(t, "mine", r, universe.get(t)))
    else:
        packs, _, _ = fill_overlay(
            g, buys, universe, seats=spec.seats, max_extras=extras_for(spec.seats),
        )
    return filter_color(packs, spec.color)


def day_rets(g: pd.DataFrame) -> dict[str, float]:
    out = {}
    if not len(g) or "ret_1d" not in g.columns:
        return out
    for _, r in g.iterrows():
        v = r.get("ret_1d")
        if v is None or pd.isna(v):
            continue
        out[str(r["Ticker"]).upper()] = float(v)
    return out


def step_hold(
    held: dict[str, dict],
    cand: list[dict],
    *,
    hold: int,
    seats: int,
    hard_red: str,
    market_hard: bool,
    rets: dict[str, float],
) -> dict:
    """One morning: age → sell unlocked off-list → fill empty seats → mark."""
    for pos in held.values():
        pos["age"] = int(pos.get("age") or 0) + 1

    cand_ids = [p["ticker"] for p in cand]
    cand_set = set(cand_ids)
    sold = []
    for t in list(held):
        if int(held[t]["age"]) >= hold and t not in cand_set:
            sold.append(t)
            del held[t]

    bought, skipped = [], []
    allow_new = not (market_hard and hard_red == "stand_down")
    if allow_new:
        for p in cand:
            t = p["ticker"]
            if t in held:
                continue
            if len(held) >= seats:
                skipped.append({"ticker": t, "why": "no_seat"})
                continue
            r = rets.get(t)
            if market_hard and hard_red == "limit_5":
                if r is None or r > -5.0:
                    skipped.append({"ticker": t, "why": "limit_miss"})
                    continue
            held[t] = {
                "age": 0,
                "src": p.get("source"),
                "stack": p.get("stack"),
                "entry": p.get("date"),
            }
            bought.append(t)
    elif market_hard:
        for p in cand:
            if p["ticker"] not in held:
                skipped.append({"ticker": p["ticker"], "why": "stand_down"})

    pnls = []
    named = []
    for t, pos in held.items():
        r = rets.get(t)
        if r is None:
            continue
        adj = float(r)
        if int(pos.get("age") or 0) == 0 and market_hard and hard_red == "haircut_5":
            adj = adj + 5.0
        adj = max(-CLIP, min(CLIP, adj))
        pnls.append(adj)
        named.append({"ticker": t, "ret_1d": adj, "age": pos["age"], "src": pos.get("src"), "stack": pos.get("stack")})
    day_pnl = round(float(sum(pnls) / len(pnls)), 3) if pnls else None
    return {
        "day_pnl": day_pnl,
        "n_held": len(held),
        "n_priced": len(pnls),
        "n_buy": len(bought),
        "n_sell": len(sold),
        "n_skip": len(skipped),
        "bought": bought,
        "sold": sold,
        "skipped": skipped,
        "held": list(held),
        "names": named,
    }


def spy_series(dates: list[str]) -> list[float | None]:
    out = []
    for d in dates:
        row = (load_finviz_px(d) or {}).get("SPY") or {}
        chg = row.get("change")
        out.append(None if chg is None else round(float(chg), 3))
    return out


def run_spec(spec: Spec, days: list[tuple[str, pd.DataFrame]], ctx_cache: dict) -> dict:
    held: dict[str, dict] = {}
    curve = []
    equity = CAPITAL
    daily = []
    for date, g in days:
        cand = candidates(date, g, spec)
        for p in cand:
            p["date"] = date
        hard = market_is_hard(date, ctx_cache)
        rec = step_hold(
            held, cand,
            hold=spec.hold, seats=spec.seats,
            hard_red=spec.hard_red, market_hard=hard,
            rets=day_rets(g),
        )
        rec["date"] = date
        rec["hard_red"] = hard
        rec["n_cand"] = len(cand)
        if rec["day_pnl"] is not None:
            equity = round(equity * (1.0 + rec["day_pnl"] / 100.0), 2)
        rec["equity"] = equity
        daily.append(rec)
        curve.append(equity)
    pnls = [d["day_pnl"] for d in daily if d["day_pnl"] is not None]
    return {
        "spec": asdict(spec),
        "totals": bt_report.day_book_stats(pnls),
        "equity": curve,
        "final": curve[-1] if curve else CAPITAL,
        "daily": daily,
    }


def run_lab(path=None) -> dict:
    from src.boring_winners_backtest import PANEL
    df = fill_returns_from_finviz(load_panel(path or PANEL))
    days = [(str(d), g) for d, g in df.groupby("date", sort=True)]
    dates = [d for d, _ in days]
    ctx_cache: dict = {}
    sleeves = {spec.id: run_spec(spec, days, ctx_cache) for spec in SCENARIOS}
    spy = spy_series(dates)
    spy_eq = []
    eq = CAPITAL
    for x in spy:
        if x is not None:
            eq = round(eq * (1.0 + x / 100.0), 2)
        spy_eq.append(eq)
    return {
        "generated": datetime.now(ZoneInfo("America/New_York")).isoformat(),
        "capital": CAPITAL,
        "dates": dates,
        "scenarios": [asdict(s) for s in SCENARIOS],
        "sleeves": sleeves,
        "spy": {"day": spy, "equity": spy_eq},
        "rules": {
            "hold_unit": "trading_sessions",
            "clip": CLIP,
            "fill": "signal-day close; HARD_RED haircut_5 adds +5 to that day's 1d on new buys only",
            "hold": "locked names keep their seat; new buys only fill empties",
        },
    }


def _ret(v) -> str:
    if v is None:
        return "—"
    return f"{v:+.2f}"


def render_md(lab: dict) -> str:
    lines = [
        "# Boring winners — scenario lab",
        "",
        "Same overlay engine, different knobs. Hold-N locks a seat for N sessions; today's new buys only fill empties. HARD_RED rules only fire when the lattice prints `hard_red` (from 2026-08-31).",
        "",
        "Dashboard: `dashboard/boring-winners/index.html` → https://sroyaltyy.github.io/fullscan/dashboard/boring-winners/",
        "",
        "## Leaderboard",
        "",
        "| sleeve | source | n | hold | color | hard_red | mean day | cum | p(loss day) | final $10k |",
        "|---|---|---:|---:|---|---|---:|---:|---:|---:|",
    ]
    rows = []
    for sid, sl in lab["sleeves"].items():
        spec = sl["spec"]
        tot = sl.get("totals") or {}
        rows.append((tot.get("mean_day") or -999, sid, spec, tot, sl.get("final")))
    rows.sort(key=lambda x: x[0], reverse=True)
    for _, sid, spec, tot, final in rows:
        lines.append(
            f"| `{sid}` | {spec['source']} | {spec['seats']} | {spec['hold']} "
            f"| {spec['color']} | {spec['hard_red']} "
            f"| {_ret(tot.get('mean_day'))} | {_ret(tot.get('cum_sum'))} "
            f"| {bt_report.fmt_pct(tot.get('p_loss_day'))} "
            f"| {final:,.0f} |"
        )
    lines += ["", "## Daily overlay 25 · daily (live)", ""]
    live = (lab["sleeves"].get("overlay_25_h1") or {}).get("daily") or []
    lines += ["| date | n held | buy | skip | sell | 1d | equity |", "|---|---:|---:|---:|---:|---:|---:|"]
    for d in live:
        lines.append(
            f"| {d['date']} | {d['n_held']} | {d['n_buy']} | {d['n_skip']} | {d['n_sell']} "
            f"| {_ret(d['day_pnl'])} | {d['equity']:,.0f} |"
        )
    lines += [
        "",
        "Live book is still `overlay_25_h1`. The lab is how we pick the next default.",
        "",
        "Notes: `haircut_5` assumes we actually got 5% cheaper on every new HARD_RED buy — that is an entry model, not a print. `hold 3d` is the more honest lift (less churn in the late-August grind). `blue only` lost because this window's 🔵 names were the energy tape.",
        "",
    ]
    return "\n".join(lines) + "\n"


def write_dashboard(lab: dict) -> None:
    DASH_DIR.mkdir(parents=True, exist_ok=True)
    series = {sid: sl["equity"] for sid, sl in lab["sleeves"].items()}
    series["SPY"] = lab["spy"]["equity"]
    stats = []
    for sid, sl in lab["sleeves"].items():
        tot = sl.get("totals") or {}
        spec = sl["spec"]
        stats.append({
            "sleeve": sid,
            "label": spec.get("label") or sid,
            "source": spec["source"],
            "seats": spec["seats"],
            "hold": spec["hold"],
            "color": spec["color"],
            "hard_red": spec["hard_red"],
            "mean_day": tot.get("mean_day"),
            "cum": tot.get("cum_sum"),
            "p_loss_day": tot.get("p_loss_day"),
            "n_days": tot.get("n_days"),
            "final": sl.get("final"),
        })
    slim_daily = {}
    for sid, sl in lab["sleeves"].items():
        slim_daily[sid] = [
            {
                "date": d["date"],
                "n_held": d["n_held"],
                "n_buy": d["n_buy"],
                "n_sell": d["n_sell"],
                "n_skip": d["n_skip"],
                "n_cand": d.get("n_cand"),
                "day_pnl": d["day_pnl"],
                "equity": d["equity"],
                "hard_red": d.get("hard_red"),
                "held": d.get("held") or [],
                "bought": d.get("bought") or [],
                "sold": d.get("sold") or [],
                "skipped": d.get("skipped") or [],
            }
            for d in sl.get("daily") or []
        ]
    payload = {
        "generated": lab["generated"],
        "capital": lab["capital"],
        "dates": lab["dates"],
        "series": series,
        "stats": stats,
        "daily": slim_daily,
        "rules": lab["rules"],
        "paper": "../",
    }
    shell = TEMPLATE.read_text(encoding="utf-8")
    html = shell.replace("__DATA__", json.dumps(payload))
    (DASH_DIR / "index.html").write_text(html, encoding="utf-8")


def daily_rows(lab: dict) -> list[dict]:
    rows = []
    for sid, sl in lab["sleeves"].items():
        spec = sl["spec"]
        for d in sl.get("daily") or []:
            rows.append({
                "sleeve": sid,
                "source": spec["source"],
                "seats": spec["seats"],
                "hold": spec["hold"],
                "color": spec["color"],
                "hard_red": spec["hard_red"],
                "date": d["date"],
                "n_held": d["n_held"],
                "n_buy": d["n_buy"],
                "n_skip": d["n_skip"],
                "n_sell": d["n_sell"],
                "day_pnl": d["day_pnl"],
                "equity": d["equity"],
            })
    return rows


def write_outputs(lab: dict) -> None:
    OUT_JSON.parent.mkdir(parents=True, exist_ok=True)
    slim = {
        "generated": lab["generated"],
        "capital": lab["capital"],
        "dates": lab["dates"],
        "scenarios": lab["scenarios"],
        "rules": lab["rules"],
        "spy": lab["spy"],
        "sleeves": {
            sid: {
                "spec": sl["spec"],
                "totals": sl["totals"],
                "equity": sl["equity"],
                "final": sl["final"],
                "daily": [
                    {k: d[k] for k in (
                        "date", "day_pnl", "equity", "n_held", "n_buy",
                        "n_sell", "n_skip", "n_cand", "hard_red",
                        "bought", "sold", "held",
                    ) if k in d}
                    for d in sl.get("daily") or []
                ],
            }
            for sid, sl in lab["sleeves"].items()
        },
    }
    OUT_JSON.write_text(json.dumps(slim, indent=2), encoding="utf-8")
    OUT_MD.write_text(render_md(lab), encoding="utf-8")
    pd.DataFrame(daily_rows(lab)).to_csv(OUT_CSV, index=False)
    write_dashboard(lab)


def main() -> None:
    lab = run_lab()
    write_outputs(lab)
    print(
        f"[bw-lab] sleeves={len(lab['sleeves'])} days={len(lab['dates'])} "
        f"dash={DASH_DIR / 'index.html'} md={OUT_MD}",
        flush=True,
    )


if __name__ == "__main__":
    main()
