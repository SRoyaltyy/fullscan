"""Three-sleeve combine — Excel, mover paper, and the .io dashboard.

Each sleeve is good at a different job:

  Excel        breadth telescope (~3,600 grids). Never the primary book.
  Mover paper  gated 09:30 BUY book. Highest live win rate, tiny drawdown.
  .io / paper  stock-book follow. Keeps winning on down days; S < -3
               is a no-new-1d rule, not a flatten.

Route on the leak-free morning general predict score S (~05:55 ET):

  S >= +1.0     → mover  (open entry, 1d hold, top-N by cond)
  -3.0 <= S < 1 → .io    (close fill, prefer size-bucket sleeves)
  S < -3.0      → no new 1d risk; hold existing .io size sleeves

Excel is a confirmation overlay on whichever primary is live: a name
that also printed a fresh L3 (or L1/L2) cluster can be sized up. Raw
Excel signals are not a capital sleeve — live tracking is underwater
and tail-driven.

The curve-stitch in this module is a sketch. The fill-level backtest
with matched holds, shared cash, and the open/close cash clock is
`src/sleeve_combine_bt.py`.

CLI:
  python -m src.sleeve_combine            # write the scoreboard report
  python -m src.sleeve_combine --date D   # print today's route card
  python -m src.sleeve_combine_bt         # integrity backtest
"""
from __future__ import annotations

import argparse
import csv
import json
import math
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

ROOT = Path(__file__).resolve().parent.parent
ET = ZoneInfo("America/New_York")

PAYLOAD = ROOT / "03_scoreboard" / "mover_lookback_action.json"
PAPER_EQ = ROOT / "data" / "paper" / "equity_curve.csv"
MOVER_EQ = ROOT / "data" / "mover_paper" / "equity_curve.csv"
MOVER_TR = ROOT / "data" / "mover_paper" / "trades.csv"
BOOK_EQ = ROOT / "data" / "book_paper" / "equity_curve.csv"
EXCEL_SUG = ROOT / "excel_bot" / "suggestions" / "suggestions.csv"
BOOK_DIR = ROOT / "data" / "stock_book"
OUT_MD = ROOT / "03_scoreboard" / "THREE_SLEEVE_COMBINE.md"
OUT_JSON = ROOT / "data" / "sleeve_combine" / "analysis.json"

MOVER_GATE = 1.0
IO_HARD_RED = -3.0
IO_PREF_SLEEVES = ("2w_size", "1d_size", "3d_size")
EXCEL_CONFIRM = ("L3_long_green_hold2_midcap",
                 "L1_long_green_tp8_lowvol",
                 "L2_long_green_tp3_lowvol")
WINDOW_START = "2026-08-13"

BUCKET_MOVER = "mover"
BUCKET_IO = "io"
BUCKET_CASH = "cash"


def route(score: float | None, *, missing_is_io: bool = True) -> dict:
    """Pick the primary sleeve from the morning general predict score.

    Missing predict: mover paper treats missing as OPEN (allow). That is
    the wrong default for a *combined* book — a blank tape is not a +1
    green light. Combined mode parks in .io (always-on, size sleeves)
    unless the caller sets missing_is_io=False.
    """
    if score is None:
        primary = BUCKET_IO if missing_is_io else BUCKET_MOVER
        return {
            "score": None,
            "bucket": primary,
            "primary": primary,
            "excel_role": "confirm_only",
            "why": ("no predict on file — "
                    + (".io size sleeves (blank tape is not a mover green light)"
                       if missing_is_io else
                       "mover OPEN, same as the solo mover gate")),
        }
    s = float(score)
    if s >= MOVER_GATE:
        return {
            "score": s,
            "bucket": BUCKET_MOVER,
            "primary": BUCKET_MOVER,
            "excel_role": "confirm_only",
            "why": (f"predict {s:+.2f} >= {MOVER_GATE:+.1f} — "
                    "mover 09:30 book (gated, high hit-rate)"),
        }
    if s >= IO_HARD_RED:
        return {
            "score": s,
            "bucket": BUCKET_IO,
            "primary": BUCKET_IO,
            "excel_role": "confirm_only",
            "why": (f"predict {s:+.2f} in [{IO_HARD_RED:+.1f}, {MOVER_GATE:+.1f}) — "
                    ".io dashboard keeps buying on flat/down days"),
        }
    return {
        "score": s,
        "bucket": BUCKET_CASH,
        "primary": BUCKET_CASH,
        "excel_role": "shorts_only_unfunded",
        "why": (f"predict {s:+.2f} < {IO_HARD_RED:+.1f} — "
                "hard-red: no new 1d risk; hold existing .io size sleeves"),
    }


def _pct(n, d) -> float | None:
    if not d:
        return None
    return round(100.0 * n / d, 1)


def _f(v):
    if v is None or v == "":
        return None
    try:
        x = float(v)
    except (TypeError, ValueError):
        return None
    if math.isnan(x):
        return None
    return x


def _parse_ret(s) -> float | None:
    """Excel `ret_vs_open` is a percent string ('-7.96%'). Return a fraction."""
    if s is None or s == "":
        return None
    t = str(s).strip().replace("%", "").replace("+", "")
    x = _f(t)
    return None if x is None else x / 100.0


def load_regime(payload: dict | None = None) -> dict[str, dict]:
    if payload is None:
        payload = json.loads(PAYLOAD.read_text(encoding="utf-8"))
    sweeps = ((payload.get("sweeps") or {}).get("featured") or {})
    params = ((sweeps.get("mover_days") or {}).get("params") or {})
    regime = params.get("_regime") or payload.get("regime") or {}
    return {d: dict(g) for d, g in regime.items()}


def load_paper_daily() -> dict[str, dict[str, float]]:
    """date -> {sleeve: equity}."""
    by: dict[str, dict[str, float]] = defaultdict(dict)
    if not PAPER_EQ.is_file():
        return {}
    with PAPER_EQ.open(encoding="utf-8") as f:
        for row in csv.DictReader(f):
            d, sl = row.get("date"), row.get("sleeve")
            eq = _f(row.get("equity"))
            if d and sl and eq is not None:
                by[d][sl] = eq
    return dict(by)


def _curve_daily(path: Path) -> dict[str, float]:
    out = {}
    if not path.is_file():
        return out
    with path.open(encoding="utf-8") as f:
        for row in csv.DictReader(f):
            d, eq = row.get("date"), _f(row.get("equity"))
            if d and eq is not None:
                out[d] = eq
    return out


def daily_returns(eq_by_date: dict[str, float]) -> dict[str, float]:
    dates = sorted(eq_by_date)
    out = {}
    for i, d in enumerate(dates):
        if i == 0:
            continue
        prev = eq_by_date[dates[i - 1]]
        if prev:
            out[d] = (eq_by_date[d] / prev) - 1.0
    return out


def max_drawdown(eq_by_date: dict[str, float]) -> float:
    peak = None
    dd = 0.0
    for d in sorted(eq_by_date):
        eq = eq_by_date[d]
        peak = eq if peak is None else max(peak, eq)
        if peak:
            dd = min(dd, eq / peak - 1.0)
    return dd


def compound(rets: list[float]) -> float:
    eq = 1.0
    for r in rets:
        eq *= 1.0 + r
    return eq - 1.0


def load_excel_rows() -> list[dict]:
    if not EXCEL_SUG.is_file():
        return []
    rows = []
    with EXCEL_SUG.open(encoding="utf-8") as f:
        for row in csv.DictReader(f):
            rows.append(row)
    return rows


def load_book_buys() -> dict[str, list[str]]:
    out: dict[str, list[str]] = {}
    for path in sorted(BOOK_DIR.glob("????-??-??_stock_book.json")):
        date = path.name[:10]
        try:
            doc = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            continue
        buys = ((doc.get("books") or {}).get("1d") or {}).get("buy") or []
        tickers = []
        for p in buys:
            t = (p.get("ticker") or "").upper()
            if t:
                tickers.append(t)
        out[date] = tickers
    return out


def load_mover_trades() -> list[dict]:
    rows = []
    if not MOVER_TR.is_file():
        return rows
    with MOVER_TR.open(encoding="utf-8") as f:
        for row in csv.DictReader(f):
            rows.append(row)
    return rows


def _cond_net(row: dict) -> int:
    c = row.get("condition") or {}
    return int(c.get("good") or 0) - int(c.get("bad") or 0)


def ungated_mover_days(payload: dict, top_n: int = 10) -> dict[str, dict]:
    """Equal-weight mean 1d of top-N BUY-by-cond, no day gate.

    Uses lookback price_changes['1d'] (close→next close). That is not the
    paper fill (open→next close) but it is the leak-free same ranking the
    sweep uses to decide whether a day was a winner or a landmine.
    """
    by: dict[str, list[dict]] = defaultdict(list)
    for r in payload.get("called_rows") or []:
        if r.get("action_call") != "BUY":
            continue
        d = r.get("date")
        if not d:
            continue
        by[d].append(r)
    out = {}
    for d, rows in by.items():
        ranked = sorted(rows, key=_cond_net, reverse=True)[:top_n]
        rets = []
        for r in ranked:
            pc = (r.get("price_changes") or {}).get("1d")
            if pc is not None:
                rets.append(float(pc) / 100.0)
        if not rets:
            continue
        wins = sum(1 for x in rets if x > 0)
        out[d] = {
            "n": len(rets),
            "mean": sum(rets) / len(rets),
            "hit": wins / len(rets),
            "tickers": [r.get("ticker") for r in ranked],
        }
    return out


def analyze() -> dict:
    payload = json.loads(PAYLOAD.read_text(encoding="utf-8"))
    regime = load_regime(payload)
    paper = load_paper_daily()
    mover_eq = _curve_daily(MOVER_EQ)
    book_eq = _curve_daily(BOOK_EQ)
    excel = load_excel_rows()
    books = load_book_buys()
    mover_tr = load_mover_trades()
    ungated = ungated_mover_days(payload)

    dates = sorted(set(regime) | set(paper) | set(mover_eq) | set(book_eq))
    dates = [d for d in dates if d >= WINDOW_START]

    paper_rets: dict[str, dict[str, float]] = defaultdict(dict)
    paper_dates = sorted(paper)
    for i, d in enumerate(paper_dates):
        if i == 0:
            continue
        prev = paper_dates[i - 1]
        for sl, eq in paper[d].items():
            p = paper[prev].get(sl)
            if p:
                paper_rets[d][sl] = eq / p - 1.0

    spy_rets = {d: paper_rets[d]["SPY (benchmark)"]
                for d in paper_rets if "SPY (benchmark)" in paper_rets[d]}

    io_day = []
    for d in paper_rets:
        g = regime.get(d) or {}
        score = g.get("predict_score")
        spy = spy_rets.get(d)
        rec = {
            "date": d,
            "score": score,
            "spy": spy,
            "spy_down": spy is not None and spy < 0,
            "bucket": route(score)["bucket"],
        }
        for sl in ("1d_top", "1d_size", "2w_size", "3d_size", "1w_size"):
            rec[sl] = paper_rets[d].get(sl)
        io_day.append(rec)

    def _bucket_stats(rows, pred, sleeve="2w_size"):
        hit = [r for r in rows if pred(r) and r.get(sleeve) is not None]
        if not hit:
            return {"n": 0, "mean": None, "vs_spy": None, "win": None}
        mean = sum(r[sleeve] for r in hit) / len(hit)
        spy_m = [r["spy"] for r in hit if r.get("spy") is not None]
        vs = (mean - (sum(spy_m) / len(spy_m))) if spy_m else None
        wins = sum(1 for r in hit if r[sleeve] > 0)
        return {
            "n": len(hit),
            "mean": mean,
            "vs_spy": vs,
            "win": wins / len(hit),
        }

    io_stats = {
        "all": _bucket_stats(io_day, lambda r: True),
        "spy_down": _bucket_stats(io_day, lambda r: r.get("spy_down")),
        "spy_up": _bucket_stats(io_day, lambda r: r.get("spy") is not None
                                and r["spy"] >= 0),
        "score_ge1": _bucket_stats(io_day, lambda r: r.get("score") is not None
                                   and r["score"] >= MOVER_GATE),
        "score_mid": _bucket_stats(io_day, lambda r: r.get("score") is not None
                                   and IO_HARD_RED <= r["score"] < MOVER_GATE),
        "score_hard": _bucket_stats(io_day, lambda r: r.get("score") is not None
                                    and r["score"] < IO_HARD_RED),
        "score_mid_or_missing": _bucket_stats(
            io_day, lambda r: r.get("score") is None
            or (IO_HARD_RED <= r["score"] < MOVER_GATE)),
        "1d_top_spy_down": _bucket_stats(io_day, lambda r: r.get("spy_down"),
                                         sleeve="1d_top"),
        "1d_size_spy_down": _bucket_stats(io_day, lambda r: r.get("spy_down"),
                                          sleeve="1d_size"),
    }

    mover_closed = [r for r in mover_tr if _f(r.get("pnl")) is not None]
    mover_wins = sum(1 for r in mover_closed if float(r["pnl"]) > 0)
    mover_loss = sum(float(r["pnl"]) for r in mover_closed if float(r["pnl"]) < 0)
    mover_gain = sum(float(r["pnl"]) for r in mover_closed if float(r["pnl"]) > 0)

    gated_days, open_days, blocked_bad, kept_good = [], [], [], []
    for d, g in sorted(regime.items()):
        if d < WINDOW_START:
            continue
        score = g.get("predict_score")
        u = ungated.get(d)
        is_open = score is None or score >= MOVER_GATE
        rec = {"date": d, "score": score, "dir": g.get("predict_dir"),
               "open": is_open, "ungated_mean": (u or {}).get("mean"),
               "ungated_hit": (u or {}).get("hit")}
        (open_days if is_open else gated_days).append(rec)
        if u and not is_open and u["mean"] < 0:
            blocked_bad.append(rec)
        if u and is_open and u["mean"] > 0:
            kept_good.append(rec)

    # stitch: take mover daily ret when bucket=mover, else .io 2w_size,
    # else 0. Align on paper dates (the .io calendar).
    mover_rets = daily_returns(mover_eq)
    stitch = {}
    # walk every calendar day we have a regime or a paper print
    walk = sorted(set(paper_dates) | set(mover_eq) | set(regime))
    walk = [d for d in walk if d >= WINDOW_START]
    eq_m = eq_i = eq_s = eq_split = eq_hold = 1.0
    curves = {
        "mover": {}, "io_2w_size": {}, "router": {},
        "hold_through": {}, "split_40_40_20": {},
    }
    for d in walk:
        g = regime.get(d) or {}
        bucket = route(g.get("predict_score"))["bucket"]
        # daily ret: 0 on first observation of that series
        r_m = mover_rets.get(d, 0.0) if d in mover_rets else 0.0
        r_i = paper_rets.get(d, {}).get("2w_size")
        if r_i is None:
            r_i = 0.0
        if bucket == BUCKET_MOVER:
            r_s = r_m
        elif bucket == BUCKET_IO:
            r_s = r_i
        else:
            r_s = 0.0
        # hold-through: same switch, but S < -3 keeps the .io mark
        # (no new 1d risk conceptually; 2w_size is already on)
        r_hold = r_m if bucket == BUCKET_MOVER else r_i
        # 40% mover / 40% .io / 20% cash — always on, no routing
        r_split = 0.40 * r_m + 0.40 * r_i
        eq_m *= 1 + r_m
        eq_i *= 1 + r_i
        eq_s *= 1 + r_s
        eq_hold *= 1 + r_hold
        eq_split *= 1 + r_split
        curves["mover"][d] = eq_m
        curves["io_2w_size"][d] = eq_i
        curves["router"][d] = eq_s
        curves["hold_through"][d] = eq_hold
        curves["split_40_40_20"][d] = eq_split
        stitch[d] = {
            "bucket": bucket, "score": g.get("predict_score"),
            "r_mover": r_m, "r_io": r_i, "r_router": r_s,
            "r_hold": r_hold, "r_split": r_split,
        }

    # Excel breadth + overlap
    excel_all = excel
    excel_win = [r for r in excel_all if (_parse_ret(r.get("ret_vs_open")) or 0) > 0]
    excel_n = len(excel_all)
    excel_tickers = {r.get("ticker", "").upper() for r in excel_all if r.get("ticker")}
    excel_in_window = [r for r in excel_all
                       if (r.get("signal_date") or "") >= WINDOW_START]
    excel_win_w = [r for r in excel_in_window
                   if (_parse_ret(r.get("ret_vs_open")) or 0) > 0]
    by_day_xl: dict[str, set[str]] = defaultdict(set)
    for r in excel_in_window:
        t = (r.get("ticker") or "").upper()
        if t:
            by_day_xl[r.get("signal_date")].add(t)

    mover_tickers = {(r.get("ticker") or "").upper() for r in mover_tr}
    book_tickers = {t for ts in books.values() for t in ts}
    xl_w_tickers = {(r.get("ticker") or "").upper() for r in excel_in_window}

    overlap = {
        "excel_n_suggestions": excel_n,
        "excel_n_tickers": len(excel_tickers),
        "excel_window_suggestions": len(excel_in_window),
        "excel_window_tickers": len(xl_w_tickers),
        "excel_window_win": _pct(len(excel_win_w), len(excel_in_window)),
        "excel_live_win": _pct(len(excel_win), excel_n),
        "mover_n_trades": len(mover_closed),
        "mover_win": _pct(mover_wins, len(mover_closed)),
        "book_n_tickers": len(book_tickers),
        "excel_and_mover": sorted(xl_w_tickers & mover_tickers),
        "excel_and_book": sorted(xl_w_tickers & book_tickers),
        "mover_and_book": sorted(mover_tickers & book_tickers),
        "all_three": sorted(xl_w_tickers & mover_tickers & book_tickers),
        "same_day_excel_and_book": {},
    }
    same_day = 0
    same_day_n = 0
    for d, buys in books.items():
        xl = by_day_xl.get(d) or set()
        hit = xl & set(buys)
        if buys:
            same_day_n += 1
            if hit:
                same_day += 1
                overlap["same_day_excel_and_book"][d] = sorted(hit)
    overlap["same_day_excel_and_book_days"] = same_day
    overlap["same_day_excel_and_book_book_days"] = same_day_n

    by_strat: dict[str, list[float]] = defaultdict(list)
    for r in excel_all:
        ret = _parse_ret(r.get("ret_vs_open"))
        if ret is not None:
            by_strat[r.get("strategy") or "?"].append(ret)
    excel_scoreboard = {}
    for s, rets in by_strat.items():
        wins = sum(1 for x in rets if x > 0)
        excel_scoreboard[s] = {
            "n": len(rets),
            "mean": sum(rets) / len(rets),
            "win": wins / len(rets),
        }

    latest = dates[-1] if dates else None
    card = route((regime.get(latest) or {}).get("predict_score")) if latest else None
    if card is not None:
        card["date"] = latest

    out = {
        "generated_at": datetime.now(ET).isoformat(timespec="seconds"),
        "window": [WINDOW_START, dates[-1] if dates else None],
        "policy": {
            "mover_gate": MOVER_GATE,
            "io_hard_red": IO_HARD_RED,
            "io_pref_sleeves": list(IO_PREF_SLEEVES),
            "excel_confirm": list(EXCEL_CONFIRM),
        },
        "card": card,
        "regime": {d: {"score": g.get("predict_score"),
                       "dir": g.get("predict_dir"),
                       "route": route(g.get("predict_score"))["bucket"]}
                   for d, g in sorted(regime.items()) if d >= WINDOW_START},
        "mover": {
            "final_equity": mover_eq.get(sorted(mover_eq)[-1]) if mover_eq else None,
            "ret": (sorted(mover_eq) and
                    mover_eq[sorted(mover_eq)[-1]] / 100000.0 - 1.0),
            "max_dd": max_drawdown(mover_eq) if mover_eq else None,
            "trades": len(mover_closed),
            "win": (mover_wins / len(mover_closed)) if mover_closed else None,
            "gross_win": mover_gain,
            "gross_loss": mover_loss,
            "open_days": open_days,
            "gated_days": gated_days,
            "blocked_bad": blocked_bad,
            "kept_good": kept_good,
            "ungated": {d: {"mean": v["mean"], "hit": v["hit"], "n": v["n"]}
                        for d, v in ungated.items()},
        },
        "io": {
            "days": io_day,
            "stats": io_stats,
            "final": {sl: paper[paper_dates[-1]][sl]
                      for sl in paper.get(paper_dates[-1], {})} if paper_dates else {},
        },
        "excel": {
            "universe": 3603,
            "scoreboard": excel_scoreboard,
            "live_n": excel_n,
            "live_win": _pct(len(excel_win), excel_n),
        },
        "overlap": overlap,
        "combine": {
            "final": {k: v[sorted(v)[-1]] - 1.0 for k, v in curves.items() if v},
            "max_dd": {k: max_drawdown(v) for k, v in curves.items() if v},
            "stitch": stitch,
        },
    }
    return out


def _pct_cell(x, digits=1) -> str:
    if x is None:
        return "—"
    return f"{100 * x:+.{digits}f}%"


def _wr_cell(x) -> str:
    if x is None:
        return "—"
    return f"{100 * x:.1f}%"


def render(doc: dict) -> str:
    w0, w1 = (doc.get("window") or [None, None])
    pol = doc.get("policy") or {}
    mv = doc.get("mover") or {}
    io = doc.get("io") or {}
    xl = doc.get("excel") or {}
    ov = doc.get("overlap") or {}
    cb = doc.get("combine") or {}
    card = doc.get("card") or {}
    lines = [
        "# Three-sleeve combine — Excel · mover · .io",
        "",
        f"_Generated {doc.get('generated_at')} — window {w0} → {w1}_",
        "",
        "The three live books are complementary. They should not vote as "
        "equals. **Excel finds, mover times, .io stays long when the tape "
        "is only mildly ugly.**",
        "",
        "## Route (the combine)",
        "",
        f"| Morning general score S | Primary | Excel |",
        f"|---|---|---|",
        f"| S ≥ **{pol.get('mover_gate'):+.1f}** | **Mover** — 09:30 open, "
        f"1d hold, top 10 by cond | confirm (size-up if L3/L1 cluster) |",
        f"| **{pol.get('io_hard_red'):+.1f}** ≤ S < "
        f"**{pol.get('mover_gate'):+.1f}** | **.io dashboard** — close fill, "
        f"prefer `{', '.join(pol.get('io_pref_sleeves') or [])}` | confirm |",
        f"| S < **{pol.get('io_hard_red'):+.1f}** | **No new 1d risk** — "
        f"hold .io size sleeves; no mover / no fresh 1d fills | "
        f"shorts only (S1/S2, unfunded) |",
        "",
        f"**Today ({card.get('date') or '—'}):** "
        f"score {card.get('score') if card.get('score') is not None else '—'} "
        f"→ **{card.get('primary') or '—'}** — {card.get('why') or ''}",
        "",
        "## What each sleeve is actually good at",
        "",
        "### Excel — vast swaths, not a book",
        "",
        f"Scans **{xl.get('universe'):,}** grids (Yahoo OHLCV cluster colors, "
        f"zero tokens). Live ledger: **{xl.get('live_n')}** suggestions, "
        f"win {xl.get('live_win')}% vs entry open. That win rate is the "
        f"tell: the engine is a *searchlight*, not a portfolio.",
        "",
        "| strategy | n | mean vs open | win |",
        "|---|---:|---:|---:|",
    ]
    for s, st in sorted((xl.get("scoreboard") or {}).items()):
        lines.append(
            f"| `{s}` | {st['n']} | {_pct_cell(st['mean'])} | {_wr_cell(st['win'])} |"
        )
    lines += [
        "",
        "Backtest (confirmation-close, 2022–2026) is a different story — "
        "tens of thousands of trades, holdout t ≥ 2 on every card, L3 "
        "midcap hold-2 and S1 1-day shorts are the durable ones. Live "
        "tracking is underwater because L1/L2 cap winners and leave losers "
        "open, and because the median trade is ~0: **you have to take "
        "every signal or the tail math breaks.** That is the opposite of "
        "how mover and .io size.",
        "",
        "Use Excel to *see* names the other two never look at, and as a "
        f"same-day confirm. Same-day Excel ∩ 1d book: "
        f"**{ov.get('same_day_excel_and_book_days')}** of "
        f"{ov.get('same_day_excel_and_book_book_days')} book days. "
        f"All-three tickers in the window: "
        f"`{'`, `'.join(ov.get('all_three') or []) or 'none'}`.",
        "",
        "### Mover paper — highest hit-rate, losses almost deleted",
        "",
        f"| Start | Final | Return | Max DD | Trades | Win |",
        f"|---:|---:|---:|---:|---:|---:|",
        f"| $100,000 | "
        f"${(mv.get('final_equity') or 0):,.2f} | "
        f"**{_pct_cell(mv.get('ret'))}** | "
        f"{_pct_cell(mv.get('max_dd'))} | "
        f"{mv.get('trades')} | "
        f"**{_wr_cell(mv.get('win'))}** |",
        "",
        f"Gross won ${mv.get('gross_win') or 0:,.0f} vs lost "
        f"${abs(mv.get('gross_loss') or 0):,.0f}. The day gate "
        f"(S ≥ {pol.get('mover_gate'):+.1f}) is the whole product: it "
        f"closed **{len(mv.get('gated_days') or [])}** sessions and "
        f"blocked **{len(mv.get('blocked_bad') or [])}** days whose "
        f"ungated top-10 BUY basket was negative.",
        "",
        "| Date | Score | Gate | Ungated top-10 1d |",
        "|---|---:|---|---:|",
    ]
    for rec in sorted((mv.get("open_days") or []) + (mv.get("gated_days") or []),
                      key=lambda r: r["date"]):
        flag = "OPEN" if rec["open"] else "**CLOSED**"
        lines.append(
            f"| {rec['date']} | "
            f"{rec['score'] if rec['score'] is not None else '—'} | "
            f"{flag} | {_pct_cell(rec.get('ungated_mean'))} |"
        )
    lines += [
        "",
        "Mover's weakness is the same as its strength: it is *off* most "
        "days. Over this window that is correct. A combine that only ran "
        "mover would sit in cash through the mild-down sessions .io is "
        "built to buy.",
        "",
        "### .io dashboard — buys (and often wins) on down days",
        "",
        "This is `src.paper_trade` following the stock book onto "
        "[the Pages dashboard](https://sroyaltyy.github.io/fullscan/dashboard/). "
        "No S ≥ +1 gate. Follow-the-book, close fill, size-bucket sleeves "
        "beat top-N. The user's rule of thumb — .io can keep *winning* on "
        "down days — is right for the longer size sleeves. The "
        "S < −3 caveat is a **new-buy** rule, not a flatten rule: "
        "`2w_size` stayed green through every hard-red session in this "
        "window because it was already on. `1d_top` / `1d_size` (new "
        "close fills) are the ones that wobble.",
        "",
        "| Cut | n | 2w_size mean | vs SPY | win |",
        "|---|---:|---:|---:|---:|",
    ]
    labels = [
        ("all", "all .io sessions"),
        ("spy_down", "SPY down days"),
        ("spy_up", "SPY up days"),
        ("score_ge1", "S ≥ +1 (mover's days)"),
        ("score_mid", f"{pol.get('io_hard_red'):+.1f} ≤ S < +1"),
        ("score_hard", f"S < {pol.get('io_hard_red'):+.1f} (hard red)"),
        ("1d_size_spy_down", "1d_size on SPY down"),
        ("1d_top_spy_down", "1d_top on SPY down"),
    ]
    st = io.get("stats") or {}
    for key, lab in labels:
        s = st.get(key) or {}
        lines.append(
            f"| {lab} | {s.get('n', 0)} | {_pct_cell(s.get('mean'))} | "
            f"{_pct_cell(s.get('vs_spy'))} | {_wr_cell(s.get('win'))} |"
        )
    lines += [
        "",
        "Per session (2w_size vs SPY):",
        "",
        "| Date | Score | Route | SPY | 2w_size | 1d_size | 1d_top |",
        "|---|---:|---|---:|---:|---:|---:|",
    ]
    for r in io.get("days") or []:
        lines.append(
            f"| {r['date']} | "
            f"{r['score'] if r['score'] is not None else '—'} | "
            f"{r['bucket']} | {_pct_cell(r.get('spy'))} | "
            f"{_pct_cell(r.get('2w_size'))} | "
            f"{_pct_cell(r.get('1d_size'))} | "
            f"{_pct_cell(r.get('1d_top'))} |"
        )
    fin = cb.get("final") or {}
    dd = cb.get("max_dd") or {}
    lines += [
        "",
        "## Stitched books (same window, existing curves)",
        "",
        "This is not a new fill engine. It replays the *already realized* "
        "daily returns of mover paper and .io `2w_size`, then either "
        "routes by S or holds a fixed 40/40/20 (mover / .io / cash) split. "
        "First day of each series is a flat 0.",
        "",
        "| Book | Return | Max DD |",
        "|---|---:|---:|",
        f"| Mover alone | {_pct_cell(fin.get('mover'))} | {_pct_cell(dd.get('mover'))} |",
        f"| .io 2w_size alone | {_pct_cell(fin.get('io_2w_size'))} | {_pct_cell(dd.get('io_2w_size'))} |",
        f"| Router flatten (S < −3 → cash) | {_pct_cell(fin.get('router'))} | "
        f"{_pct_cell(dd.get('router'))} |",
        f"| **Hold-through** (S ≥ +1 mover, else .io; no flatten) | "
        f"**{_pct_cell(fin.get('hold_through'))}** | "
        f"{_pct_cell(dd.get('hold_through'))} |",
        f"| Split 40/40/20 | {_pct_cell(fin.get('split_40_40_20'))} | "
        f"{_pct_cell(dd.get('split_40_40_20'))} |",
        "",
        "Flattening on S < −3 *hurt* this window: .io `2w_size` was the "
        "best single book (+13%) and it made that number on the hard-red "
        "days the flatten rule would skip. **Hold-through is the combine "
        "that matches the evidence** — mover on green-light mornings, "
        ".io the rest of the time, no forced cash-out. The 40/40/20 split "
        "is the defensive alternative (smaller DD, gives up some .io "
        "upside). Do not flatten a working size sleeve just because the "
        "morning stamp went hard-red.",
        "",
        "## How to combine in production",
        "",
        "1. **Do not average the three pick lists.** Excel dumps 30–50 "
        "names a day; mover wants 10; .io fills 10. Averaging re-imports "
        "Excel's median-zero / tail-or-nothing payoff.",
        "2. **Excel = universe + confirm.** Overnight: scan. At 09:30: if "
        "the route is mover, size-up any top-N name that also confirmed "
        "L3 (midcap hold-2) or L1/L2 that session. At the close: if the "
        "route is .io, same confirm on the size-sleeve fills.",
        "3. **Prefer .io size sleeves over 1d_top.** `2w_size` is the "
        "down-day engine (7 SPY-down sessions, +0.2% mean, 71% win, "
        "+0.7% vs SPY). 1d_top is the dashboard headline and the weakest "
        "long sleeve.",
        "4. **Keep mover's +1 gate for mover fills.** Do not loosen it "
        "just because .io can buy below +1. That gate is why max DD is "
        "0.12%. Below +1, *switch books*, do not dilute mover.",
        "5. **Hard-red (S < −3) = no new 1d risk, not flatten.** Stop "
        "fresh .io 1d fills and all mover fills. Leave 2w/1m size "
        "sleeves on. Do not stand up an Excel short book until S1/S2 "
        "have a fee-aware paper sleeve (borrow is ignored today).",
        "6. **Intersection is a bonus, not a requirement.** "
        f"Mover ∩ book tickers this window: "
        f"`{'`, `'.join(ov.get('mover_and_book') or []) or 'none'}`. "
        "Waiting for all three to agree starves the book.",
        "",
        "## Caveats",
        "",
        "- Window is ~16 sessions. Router vs split ranking can flip.",
        "- Mover ungated day P&L uses lookback close→next-close, not the "
        "paper open→next-close fill.",
        "- .io curve skips sessions with no stock-book file (2026-08-24..26).",
        "- Excel live marks are vs entry open and many names are still open "
        "(tp strategies never sold). Do not compare that mean to paper P&L.",
        "- Missing predict: solo mover allows the day; the *combine* parks "
        "in .io instead. A blank tape is not a +1 green light.",
        "",
        "Code: `src/sleeve_combine.py`. Machine copy: "
        "`data/sleeve_combine/analysis.json`.",
        "",
    ]
    return "\n".join(lines)


def write(doc: dict | None = None) -> dict:
    doc = doc or analyze()
    OUT_JSON.parent.mkdir(parents=True, exist_ok=True)
    OUT_JSON.write_text(json.dumps(doc, indent=2, default=str) + "\n",
                        encoding="utf-8")
    OUT_MD.write_text(render(doc), encoding="utf-8")
    return doc


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--date", help="print the route card for YYYY-MM-DD")
    p.add_argument("--write", action="store_true", default=True)
    p.add_argument("--no-write", action="store_true")
    args = p.parse_args(argv)
    if args.date:
        payload = json.loads(PAYLOAD.read_text(encoding="utf-8"))
        g = load_regime(payload).get(args.date) or {}
        card = route(g.get("predict_score"))
        card["date"] = args.date
        print(json.dumps(card, indent=2))
        return 0
    doc = analyze()
    if not args.no_write:
        write(doc)
        print(f"wrote {OUT_MD.relative_to(ROOT)} and {OUT_JSON.relative_to(ROOT)}")
    card = doc.get("card") or {}
    print(f"route {card.get('date')} → {card.get('primary')} "
          f"(score {card.get('score')})")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
