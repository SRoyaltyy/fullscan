"""Breadth metrics and the luck test for breadth_mine_v1e.

The t-test is the same one-sided Student tail as src.lever_search_score.
Raw p is that tail. Adjusted p multiplies by the luck denominator and
caps at 1. Fewer than two check days cannot reject the null.
"""
from __future__ import annotations

import math


def compound(returns: list[float]) -> float:
    acc = 1.0
    for value in returns:
        acc *= 1.0 + float(value)
    return acc - 1.0


def _log_gamma(value: float) -> float:
    return math.lgamma(value)


def _betacf(a: float, b: float, x: float) -> float:
    max_iter = 200
    eps = 3e-12
    a0 = 1.0
    b0 = 1.0
    a1 = 1.0
    b1 = 1.0 - (a + b) * x / (a + 1.0)
    if abs(b1) < 1e-30:
        b1 = 1e-30
    frac = a1 / b1
    for m in range(1, max_iter + 1):
        em = float(m)
        tem = em + em
        d = em * (b - em) * x / ((a + tem - 1.0) * (a + tem))
        ap = a1 + d * a0
        bp = b1 + d * b0
        d = -(a + em) * (a + b + em) * x / ((a + tem) * (a + tem + 1.0))
        app = ap + d * a1
        bpp = bp + d * b1
        a0, b0, a1, b1 = ap, bp, app, bpp
        if abs(bpp) < 1e-30:
            bpp = 1e-30
        frac_next = app / bpp
        if abs(frac_next - frac) < eps * abs(frac_next):
            return frac_next
        frac = frac_next
    return frac


def regularized_incomplete_beta(a: float, b: float, x: float) -> float:
    if x <= 0.0:
        return 0.0
    if x >= 1.0:
        return 1.0
    log_beta = _log_gamma(a) + _log_gamma(b) - _log_gamma(a + b)
    front = math.exp(a * math.log(x) + b * math.log(1.0 - x) - log_beta) / a
    if x < (a + 1.0) / (a + b + 2.0):
        return front * _betacf(a, b, x)
    return 1.0 - math.exp(
        b * math.log(1.0 - x) + a * math.log(x) - log_beta
    ) / b * _betacf(b, a, 1.0 - x)


def student_t_sf(stat: float, df: int) -> float:
    if df < 1:
        return 1.0
    if stat == 0.0:
        return 0.5
    x = df / (df + stat * stat)
    prob = 0.5 * regularized_incomplete_beta(df / 2.0, 0.5, x)
    if stat > 0:
        return prob
    return 1.0 - prob


def raw_luck_p(returns: list[float]) -> float:
    """One-sided p that the mean return is above zero. No multiplicity."""
    clean = [float(value) for value in returns if math.isfinite(value)]
    n = len(clean)
    if n < 2:
        return 1.0
    mean = sum(clean) / n
    var = sum((value - mean) ** 2 for value in clean) / (n - 1)
    if var <= 0.0:
        return 0.0 if mean > 0.0 else 1.0
    stat = mean / math.sqrt(var / n)
    return student_t_sf(stat, n - 1)


def luck_p(returns: list[float], n_tries: int) -> float:
    return min(1.0, raw_luck_p(returns) * n_tries)


def median(values: list[float]) -> float | None:
    clean = sorted(float(value) for value in values if math.isfinite(value))
    if not clean:
        return None
    mid = len(clean) // 2
    if len(clean) % 2:
        return clean[mid]
    return (clean[mid - 1] + clean[mid]) / 2.0


def ticker_shares(totals: dict[str, float]) -> tuple[float | None, float | None, str | None]:
    """Top-1 share, top-3 share, and the best ticker.

    Share is the ticker's attributed P&L divided by the sum of ticker P&L.
    A non-positive sum has no profit to split, so the shares are None.
    Ties take the ticker that sorts first.
    """
    if not totals:
        return None, None, None
    ordered = sorted(totals.items(), key=lambda item: (-item[1], item[0]))
    total = sum(totals.values())
    best = ordered[0][0]
    if total <= 0.0:
        return None, None, best
    top1 = ordered[0][1] / total
    top3 = sum(value for _ticker, value in ordered[:3]) / total
    return top1, top3, best


def ex_best_compound(
    sessions: list[str],
    check: list[str],
    returns: list[float],
    pnl_by_day: dict[str, dict[str, float]],
    best: str | None,
) -> float | None:
    """Compound check-day returns after removing the best ticker's dollars.

    Equity is the continuous path of every session return, starting at
    10,000. Only check days enter the product. The best ticker's
    attributed dollars on that check day are taken out of the end equity.
    """
    if not check or best is None or len(returns) != len(sessions):
        return None
    equity = 10_000.0
    by_session = dict(zip(sessions, returns))
    out: list[float] = []
    check_set = set(check)
    for session in sessions:
        start = equity
        ret = float(by_session[session])
        end = start * (1.0 + ret)
        if session in check_set and start != 0.0:
            removed = float(pnl_by_day.get(session, {}).get(best) or 0.0)
            out.append((end - removed) / start - 1.0)
        equity = end
    if not out:
        return None
    return compound(out)
