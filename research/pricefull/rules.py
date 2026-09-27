"""Pass rules for the two price-only variants.

The arithmetic is section 9 of research/pricefull/PREREG.md. This module
does not choose a gate or a threshold.
"""
from __future__ import annotations

import math
from collections import defaultdict

import numpy as np
from scipy import stats

CAPITAL = 10000.0
BP15 = 0.0015
YEARS = tuple(range(2019, 2027))
SIX = tuple(range(2019, 2025))
PERIOD_START = "2025-01-01"
PERIOD_END = "2026-08-12"


def holm(pvals: list[float]) -> list[float]:
    """Holm step-down. Rank i starts at 1. Ties keep the earlier index first."""
    m = len(pvals)
    order = sorted(range(m), key=lambda i: (pvals[i], i))
    adj = [1.0] * m
    prev = 0.0
    for rank, i in enumerate(order, start=1):
        raw = min(1.0, (m - rank + 1) * pvals[i])
        prev = max(raw, prev)
        adj[i] = prev
    return adj


def cluster_test(returns_by_day: list[float]) -> dict:
    """One-sided t on entry-day mean returns. Fewer than 2 days has p = 1."""
    series = np.array(returns_by_day, dtype=float)
    n = int(series.size)
    if n < 2:
        return {"n": n, "mean": None, "t": None, "p": 1.0, "std": None}
    mean = float(series.mean())
    std = float(series.std(ddof=1))
    if std == 0.0 and mean > 0.0:
        return {"n": n, "mean": mean, "t": None, "p": 0.0, "std": 0.0}
    if std == 0.0:
        return {"n": n, "mean": mean, "t": 0.0, "p": 1.0, "std": 0.0}
    result = stats.ttest_1samp(series, 0.0, alternative="greater")
    p = float(result.pvalue)
    tstat = float(result.statistic)
    if not math.isfinite(p):
        p = 1.0
    return {"n": n, "mean": mean, "t": tstat, "p": p, "std": std}


def empty_ledger(name: str, sessions: list[str]) -> dict:
    """A book that buys nobody. Cash stays at the starting capital."""
    daily = [
        {"date": day, "cash": CAPITAL, "equity": CAPITAL, "buys": 0, "positions": 0}
        for day in sessions
    ]
    return {
        "rule": name,
        "capital": CAPITAL,
        "trades": [],
        "buy_fills": [],
        "daily": daily,
        "final_equity": CAPITAL,
        "open_lots_at_end": 0,
    }


def _year_of(trades: list[dict], field: str) -> dict[str, dict]:
    out = {
        str(year): {"wins": 0, "losses": 0, "flat": 0, "pnl": 0.0, "trades": 0}
        for year in YEARS
    }
    for trade in trades:
        year = str(trade[field])[:4]
        if year not in out:
            out[year] = {"wins": 0, "losses": 0, "flat": 0, "pnl": 0.0, "trades": 0}
        pnl = float(trade["pnl"])
        out[year]["trades"] += 1
        out[year]["pnl"] += pnl
        if pnl > 0:
            out[year]["wins"] += 1
        elif pnl < 0:
            out[year]["losses"] += 1
        else:
            out[year]["flat"] += 1
    return out


def ticker_pnl(trades: list[dict]) -> list[tuple[str, float]]:
    """Largest closed P&L first. A tie sorts the ticker A→Z."""
    totals: dict[str, float] = defaultdict(float)
    for trade in trades:
        totals[str(trade["ticker"])] += float(trade["pnl"])
    return sorted(totals.items(), key=lambda item: (-item[1], item[0]))


def best_share(trades: list[dict]) -> dict:
    """Best ticker's closed P&L divided by the sum of closed P&L.

    The share is defined only when that sum is strictly positive.
    """
    ranked = ticker_pnl(trades)
    total = float(sum(pnl for _, pnl in ranked))
    if not ranked or not total > 0:
        return {
            "best_ticker": ranked[0][0] if ranked else None,
            "best_pnl": ranked[0][1] if ranked else None,
            "closed_pnl": total,
            "share": None,
        }
    return {
        "best_ticker": ranked[0][0],
        "best_pnl": ranked[0][1],
        "closed_pnl": total,
        "share": ranked[0][1] / total,
    }


def removed_pnl(trades: list[dict], banned: set[str]) -> float:
    """Closed P&L of the same trades with banned tickers left out.

    The original book bought nobody, so a rerun that still buys nobody has
    the same empty sum. When the book did trade, the caller passes the
    rerun's own trades. This helper is the empty and the post-hoc sum.
    """
    return float(sum(float(t["pnl"]) for t in trades if t["ticker"] not in banned))


def summarize(name: str, ledger: dict, sessions: list[str], p_holm: float,
              removed_best: dict, removed_top3: dict) -> dict:
    trades = ledger["trades"]
    buys = ledger["buy_fills"]
    n_sessions = len(sessions)
    by_entry: dict[str, list[float]] = defaultdict(list)
    for trade in trades:
        by_entry[trade["entry_date"]].append(float(trade["ret"]))
    series = [float(np.mean(by_entry[day])) for day in sorted(by_entry)]
    test = cluster_test(series)
    closed = float(sum(float(t["pnl"]) for t in trades))
    n_closed = len(trades)
    wins = sum(1 for t in trades if float(t["pnl"]) > 0)
    win_rate = (wins / n_closed) if n_closed else None
    rate = (len(buys) / (n_sessions / 252.0)) if n_sessions else 0.0
    by_exit = _year_of(trades, "exit_date")
    positive_six = sum(1 for year in SIX if by_exit[str(year)]["trades"] > 0 and by_exit[str(year)]["pnl"] > 0)
    period = float(sum(
        float(t["pnl"]) for t in trades
        if PERIOD_START <= t["exit_date"] <= PERIOD_END
    ))
    mean = test["mean"]
    pass_mean = mean is not None and mean > 0 and p_holm < 0.05
    pass_years = positive_six >= 4
    pass_drop = float(removed_best["closed_pnl"]) > 0
    pass_rate = rate >= 30
    pass_period = period > 0
    study_pass = pass_mean and pass_years and pass_drop and pass_rate and pass_period
    keep = len(buys) >= 30 and win_rate is not None and win_rate > 0.55
    pnl_15 = float(sum(
        t["shares"] * (t["exit_px"] - t["entry_px"]) - BP15 * t["shares"] * t["entry_px"]
        for t in trades
    ))
    share = best_share(trades)
    trades_per_session = (n_closed / n_sessions) if n_sessions else 0.0
    buys_per_session = (len(buys) / n_sessions) if n_sessions else 0.0
    if study_pass and keep:
        label = "study_pass"
    elif study_pass:
        label = "study_pass_ironclad_keep_bar_fail"
    else:
        label = "fail"
    return {
        "rule": name,
        "label": label,
        "study_pass": study_pass,
        "ironclad_keep_bar": keep,
        "pass_9_1_mean": pass_mean,
        "pass_9_2_years": pass_years,
        "pass_9_3_best_removed": pass_drop,
        "pass_9_4_rate": pass_rate,
        "pass_9_5_period": pass_period,
        "positive_years_2019_2024": positive_six,
        "cluster_n": test["n"],
        "cluster_mean": test["mean"],
        "t": test["t"],
        "p": test["p"],
        "p_holm": p_holm,
        "closed_trades": n_closed,
        "buy_fills": len(buys),
        "trades_per_session": trades_per_session,
        "buy_fills_per_session": buys_per_session,
        "fires_per_year": rate,
        "win_rate": win_rate,
        "wins_losses_by_exit_year": by_exit,
        "closed_pnl": closed,
        "return_on_10000": ledger["final_equity"] / CAPITAL - 1.0,
        "ending_equity": ledger["final_equity"],
        "pnl_15bp_same_shares": pnl_15,
        "return_15bp_on_10000": pnl_15 / CAPITAL,
        "best_ticker": share["best_ticker"],
        "best_pnl": share["best_pnl"],
        "best_share_of_closed_pnl": share["share"],
        "removed_best_ticker": removed_best["banned"],
        "return_without_best": removed_best["return_on_10000"],
        "pnl_without_best": removed_best["closed_pnl"],
        "removed_top3": removed_top3["banned"],
        "return_without_top3": removed_top3["return_on_10000"],
        "pnl_without_top3": removed_top3["closed_pnl"],
        "pnl_2025_01_01_through_2026_08_12": period,
        "buy_fills_by_year": _buy_years(buys),
    }


def _buy_years(buys: list[dict]) -> dict[str, int]:
    counts = {str(year): 0 for year in YEARS}
    for buy in buys:
        year = str(buy["date"])[:4]
        counts[year] = counts.get(year, 0) + 1
    return counts


def rerun_empty(banned: list[str]) -> dict:
    """The empty book rerun. No name was bought, so the banned set changes nothing."""
    return {
        "banned": list(banned),
        "closed_pnl": 0.0,
        "final_equity": CAPITAL,
        "return_on_10000": 0.0,
        "closed_trades": 0,
    }
