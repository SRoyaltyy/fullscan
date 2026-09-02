"""Shared backtest report pack.

Every strat board going forward prints these fields. Returns are percent
close-to-close from the signal session (same hops as FEATURE_MINE).

  p_loss          P(name ret < 0) among priced names
  p_loss_day      P(equal-weight book ret < 0) among priced sessions
  avg_loss        mean of losing names only (always <= 0)
  avg_win         mean of winning names only
  avg_loss_day    mean of losing book-days only
  book_pnl        equal-weight mean of that day's priced seats
"""
from __future__ import annotations

from typing import Iterable

import pandas as pd

CLIP = 30.0


def _s(series) -> pd.Series:
    s = pd.to_numeric(pd.Series(series), errors="coerce")
    return s.replace([float("inf"), float("-inf")], pd.NA).dropna()


def name_stats(rets: Iterable, clip: float = CLIP) -> dict:
    r = _s(rets)
    if r.empty:
        return {"n": 0}
    wins = r[r > 0]
    loss = r[r < 0]
    flat = r[r == 0]
    clip_r = r.clip(-clip, clip)
    out = {
        "n": int(len(r)),
        "n_win": int(len(wins)),
        "n_loss": int(len(loss)),
        "n_flat": int(len(flat)),
        "p_win": round(float((r > 0).mean()), 4),
        "p_loss": round(float((r < 0).mean()), 4),
        "p_flat": round(float((r == 0).mean()), 4),
        "mean": round(float(r.mean()), 3),
        "mean_clip30": round(float(clip_r.mean()), 3),
        "median": round(float(r.median()), 3),
        "avg_win": None if wins.empty else round(float(wins.mean()), 3),
        "avg_loss": None if loss.empty else round(float(loss.mean()), 3),
        "payoff": None,
    }
    if out["avg_win"] is not None and out["avg_loss"] not in (None, 0):
        out["payoff"] = round(out["avg_win"] / abs(out["avg_loss"]), 3)
    return out


def day_book_stats(daily_pnls: Iterable) -> dict:
    r = _s(daily_pnls)
    if r.empty:
        return {"n_days": 0}
    wins = r[r > 0]
    loss = r[r < 0]
    return {
        "n_days": int(len(r)),
        "n_win_days": int(len(wins)),
        "n_loss_days": int(len(loss)),
        "p_loss_day": round(float((r < 0).mean()), 4),
        "p_win_day": round(float((r > 0).mean()), 4),
        "mean_day": round(float(r.mean()), 3),
        "avg_win_day": None if wins.empty else round(float(wins.mean()), 3),
        "avg_loss_day": None if loss.empty else round(float(loss.mean()), 3),
        "cum_sum": round(float(r.sum()), 3),
    }


def fmt_pct(x) -> str:
    if x is None:
        return "—"
    return f"{100 * x:.1f}%"


def fmt_num(x, signed=True) -> str:
    if x is None:
        return "—"
    return f"{x:+.2f}" if signed else f"{x:.2f}"


def fmt_stats_row(st: dict) -> str:
    if not st or not st.get("n"):
        return "n=0"
    bits = [
        f"n={st['n']}",
        f"p_win={fmt_pct(st.get('p_win'))}",
        f"p_loss={fmt_pct(st.get('p_loss'))}",
        f"avg_win={fmt_num(st.get('avg_win'))}",
        f"avg_loss={fmt_num(st.get('avg_loss'))}",
        f"mean={fmt_num(st.get('mean'))}",
        f"clip30={fmt_num(st.get('mean_clip30'))}",
    ]
    if st.get("payoff") is not None:
        bits.append(f"payoff={st['payoff']:.2f}")
    return " · ".join(bits)
