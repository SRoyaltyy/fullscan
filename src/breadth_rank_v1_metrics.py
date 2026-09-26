"""Compound, luck, and ex-best-ticker math for breadth_rank_v1.

The t-test matches the one-sided Student tail used by the earlier
breadth studies. Luck is reported. It is not a rank input.
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


def day_counts(returns: list[float]) -> tuple[int, int, int]:
    up = down = flat = 0
    for value in returns:
        if value > 0:
            up += 1
        elif value < 0:
            down += 1
        else:
            flat += 1
    return up, down, flat


def start_day_win_rate(returns: list[float]) -> float | None:
    """Fraction of start sessions whose remaining compound is strictly up."""
    if not returns:
        return None
    wins = 0
    for i in range(len(returns)):
        if compound(returns[i:]) > 0:
            wins += 1
    return wins / len(returns)


def asymmetric_payoff(trades: list[float]) -> float | None:
    """Average winning closed-trade return divided by the average losing one."""
    wins = [value for value in trades if value > 0]
    losses = [value for value in trades if value < 0]
    if not wins or not losses:
        return None
    return abs((sum(wins) / len(wins)) / (sum(losses) / len(losses)))


def ex_best_compound(
    sessions: list[str],
    check: list[str],
    returns: list[float],
    pnl_by_day: dict[str, dict[str, float]],
    best: str | None,
) -> float | None:
    """Compound check-session returns after removing the best ticker's dollars.

    Equity follows every session from 10,000. Only the check sessions
    enter the product. On a check session the best ticker's attributed
    dollars are taken out of that session's end equity.
    """
    if not check or best is None or len(returns) != len(sessions):
        return None
    equity = 10_000.0
    by_session = dict(zip(sessions, returns))
    out: list[float] = []
    check_set = set(check)
    for session in sessions:
        start = equity
        end = start * (1.0 + float(by_session[session]))
        if session in check_set and start != 0.0:
            removed = float(pnl_by_day.get(session, {}).get(best) or 0.0)
            out.append((end - removed) / start - 1.0)
        equity = end
    if not out:
        return None
    return compound(out)


def best_ticker(totals: dict[str, float], first: dict[str, str]) -> str | None:
    if not totals:
        return None
    return min(
        totals,
        key=lambda ticker: (-totals[ticker], first.get(ticker, "9999-99-99"), ticker),
    )


def top_share(totals: dict[str, float], first: dict[str, str]) -> tuple[float | None, str | None]:
    """Best ticker's dollars over the sum of ticker dollars, when that sum is positive."""
    if not totals:
        return None, None
    best = best_ticker(totals, first)
    total = sum(totals.values())
    if best is None or total <= 0.0:
        return None, best
    return totals[best] / total, best
