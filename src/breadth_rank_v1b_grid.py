"""Forty rank rules for breadth_rank_v1b.

Same baskets as breadth_rank_v1. The pick is the best long rule by
Futubull compound. N is 40. The luck denominator adds these 40 tries
to the 239,830 already counted by breadth_mine_v1 through v1d and
breadth_rank_v1.
"""
from __future__ import annotations

ATOMS: tuple[str, ...] = (
    "a09_sma50",
    "ab_good",
    "actions_good",
    "book_buy",
    "catalyst_on",
    "cd_engulf_bull",
    "fv_week_pos",
    "hard_red",
    "heat_up",
    "hot_pos",
    "judge_up",
    "last_green",
    "predict_up",
    "s_gt_0",
    "sector_up",
)

BASKETS: tuple[tuple[str, tuple[str, ...]], ...] = (
    ("eq15", ATOMS),
    ("price", ("a09_sma50", "cd_engulf_bull", "hot_pos", "last_green")),
    ("flow", ("ab_good", "actions_good", "fv_week_pos", "heat_up")),
    ("story", ("book_buy", "catalyst_on", "judge_up", "sector_up")),
    ("hot", ("hot_pos",)),
    ("heat", ("heat_up",)),
    ("ab", ("ab_good",)),
    ("judge", ("judge_up",)),
    ("week", ("fv_week_pos",)),
    ("sma", ("a09_sma50",)),
)

SIDES: tuple[str, ...] = ("long", "short")
TOP_N: tuple[int, ...] = (4, 8)
EXIT_NAME = "time"
HOLD = 1
MIN_HOLD = 1
PRIOR_BASE = 239830
MIN_CLOSED = 30
WIN_MIN = 0.55
RANDOM4_SEED = 20260813
RANDOM4_DRAWS = 1000
RANDOM4_N = 4


def rule_id(basket: str, side: str, top_n: int) -> str:
    return f"{basket}|{side}|{EXIT_NAME}|h{HOLD}m{MIN_HOLD}|n{top_n}"


def iter_rules():
    """Yield (index, id, basket, atoms, side, top_n)."""
    index = 0
    for basket, atoms in BASKETS:
        for side in SIDES:
            for top_n in TOP_N:
                yield index, rule_id(basket, side, top_n), basket, atoms, side, top_n
                index += 1


def grid_counts() -> dict[str, int]:
    n = len(BASKETS) * len(SIDES) * len(TOP_N)
    return {
        "baskets": len(BASKETS),
        "sides": len(SIDES),
        "top_n": len(TOP_N),
        "n": n,
        "luck": PRIOR_BASE + n,
    }


N = grid_counts()["n"]
LUCK_DENOMINATOR = grid_counts()["luck"]
