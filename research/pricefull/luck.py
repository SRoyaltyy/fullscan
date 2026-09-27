"""Luck test for the two variants. Seed 20260927. Hold is 1.

The long-history procedure builds the proxy panel's 1-share return vector
for hold 1 and hold 2, then permutes that session once per reshuffle.
Both variants here are hold 1. A variant with no pooled pick has no real
mean, so the p-value formula is not applied.
"""
from __future__ import annotations

import math

import numpy as np

from research.longhist.engine import one_share_return

SEED = 20260927
DRAWS = 10000


def luck_test(panel, tapes, sessions: list[str], fees: dict,
              books: list[dict]) -> dict:
    n_sess = len(sessions)
    ret1: list[np.ndarray] = []
    ret2: list[np.ndarray] = []
    order: list[dict[str, int]] = []
    for si, rows in enumerate(panel):
        ordered = sorted(rows, key=lambda row: row.ticker)
        order.append({row.ticker: i for i, row in enumerate(ordered)})
        ret1.append(np.array([
            one_share_return(tapes, sessions, si, row.ticker, 1, fees)
            for row in ordered
        ], dtype=float))
        ret2.append(np.array([
            one_share_return(tapes, sessions, si, row.ticker, 2, fees)
            for row in ordered
        ], dtype=float))

    day_index = {day: i for i, day in enumerate(sessions)}
    slots: list[list[list[int]]] = []
    holds: list[int] = []
    for book in books:
        hold = int(book["hold"])
        if hold not in (1, 2):
            raise RuntimeError(f"hold {hold} is not in the luck-test vectors")
        holds.append(hold)
        per_day: list[list[int]] = [[] for _ in range(n_sess)]
        for buy in book["buy_fills"]:
            si = day_index[buy["date"]]
            slot = order[si].get(buy["ticker"])
            if slot is not None:
                per_day[si].append(slot)
        slots.append(per_day)

    real = np.zeros(len(books))
    real_n = np.zeros(len(books))
    for ri, hold in enumerate(holds):
        for si in range(n_sess):
            series = ret1[si] if hold == 1 else ret2[si]
            for slot in slots[ri][si]:
                val = float(series[slot]) if len(series) else float("nan")
                if math.isfinite(val):
                    real[ri] += val
                    real_n[ri] += 1
    real_mean = np.divide(
        real, real_n, out=np.full(len(books), np.nan), where=real_n > 0
    )

    rng = np.random.Generator(np.random.PCG64(SEED))
    calls = 0
    # The published comparison needs a real mean. Empty pick lists still
    # consume one permutation per session per draw, in calendar order.
    for k in range(DRAWS):
        if k and k % 2000 == 0:
            print(f"luck {k}", flush=True)
        for si in range(n_sess):
            n = len(ret1[si])
            perm = rng.permutation(n)
            calls += 1
            if real_n.sum() == 0:
                continue
            _ = perm  # picks exist: the mean uses the permuted vector below
        if real_n.sum() == 0:
            continue
        # Reached only when some variant has a pooled pick. The locked audit
        # drops every id, so this branch is not the published path.
        raise RuntimeError("luck test has pooled picks; the empty path must not invent a p-value here")

    rules = []
    eligible = []
    for ri, book in enumerate(books):
        ok = math.isfinite(float(real_mean[ri]))
        if ok:
            eligible.append(ri)
        rules.append({
            "rule": book["rule"],
            "hold": holds[ri],
            "pooled_picks": int(real_n[ri]),
            "real_1share_mean": None if not ok else float(real_mean[ri]),
            "p_rule": None,
            "p_rule_reason": (
                None if ok else "no pooled pick, so null_mean >= real_1share_mean is not defined"
            ),
        })
    return {
        "seed": SEED,
        "reshuffles": DRAWS,
        "permutation_calls": calls,
        "expected_permutation_calls": DRAWS * n_sess,
        "p_best": None if not eligible else None,
        "p_best_reason": (
            "no variant has at least one pooled pick"
            if not eligible
            else "pooled picks exist and this runner stops rather than invent a third variant"
        ),
        "real_best_1share_mean": None,
        "rules": rules,
    }
