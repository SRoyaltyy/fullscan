"""Bounded search space for breadth_mine_v1c.

Same 57,600 combinations as breadth_mine_v1 and breadth_mine_v1b.
Those tries stay in the luck tally. The denominator here is
124,590 + 57,600 = 182,190.
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

# Files that must be present or a rule that reads the atom sits out.
# Price, candle, AB-price, and hot score read the pinned bars only.
ATOM_ROLES: dict[str, tuple[str, ...]] = {
    "a09_sma50": (),
    "ab_good": ("ab",),
    "actions_good": ("actions",),
    "book_buy": ("stock_book",),
    "catalyst_on": ("catalyst",),
    "cd_engulf_bull": (),
    "fv_week_pos": ("export",),
    "hard_red": ("predict",),
    "heat_up": ("heat",),
    "hot_pos": (),
    "judge_up": ("judge",),
    "last_green": (),
    "predict_up": ("predict",),
    "s_gt_0": ("predict",),
    "sector_up": ("sector",),
}

SIDES: tuple[str, ...] = ("long", "short")
EXITS: tuple[str, ...] = ("time", "list", "cut_loser", "trail")
HOLDS: tuple[tuple[int, int], ...] = (
    (1, 1),
    (2, 1), (2, 2),
    (3, 1), (3, 2), (3, 3),
    (4, 1), (4, 2), (4, 3), (4, 4),
    (5, 1), (5, 2), (5, 3), (5, 4), (5, 5),
)
TOP_N: tuple[int, ...] = (1, 4, 8, 12)

HOT_POS_MIN = 1.0
CUT_LOSS = 0.03
TRAIL_GAP = 0.05
CANDIDATE_CAP = 50
MIN_FIRES = 30
WIN_MIN = 0.55
RANDOM4_SEED = 20260813
RANDOM4_DRAWS = 1000
RANDOM4_N = 4
# v1 and v1b already spent 57,600 each. Prior denominator is 124,590.
PRIOR_BASE = 124590


def signal_pairs() -> tuple[tuple[str, str | None], ...]:
    """Singles, then unordered pairs with the left id first in byte order."""
    out: list[tuple[str, str | None]] = [(atom, None) for atom in ATOMS]
    for i, left in enumerate(ATOMS):
        for right in ATOMS[i + 1 :]:
            out.append((left, right))
    return tuple(out)


def signal_name(left: str, right: str | None) -> str:
    if right is None:
        return f"f:{left}"
    a, b = (left, right) if left <= right else (right, left)
    return f"p:{a}+{b}"


def rule_id(signal: str, side: str, exit_name: str, hold: int, min_hold: int, top_n: int) -> str:
    return f"{signal}|{side}|{exit_name}|h{hold}m{min_hold}|n{top_n}"


def iter_rules():
    """Yield (index, id, signal, side, exit, hold, min_hold, top_n, roles)."""
    index = 0
    for left, right in signal_pairs():
        signal = signal_name(left, right)
        roles = tuple(sorted(set(ATOM_ROLES[left]) | (set(ATOM_ROLES[right]) if right else set())))
        for side in SIDES:
            for exit_name in EXITS:
                for hold, min_hold in HOLDS:
                    for top_n in TOP_N:
                        yield (
                            index,
                            rule_id(signal, side, exit_name, hold, min_hold, top_n),
                            signal,
                            side,
                            exit_name,
                            hold,
                            min_hold,
                            top_n,
                            roles,
                        )
                        index += 1


def grid_counts() -> dict[str, int]:
    n_atoms = len(ATOMS)
    n_pairs = n_atoms * (n_atoms - 1) // 2
    n_signals = n_atoms + n_pairs
    n_cross = len(SIDES) * len(EXITS) * len(HOLDS) * len(TOP_N)
    n = n_signals * n_cross
    return {
        "atoms": n_atoms,
        "singles": n_atoms,
        "pairs": n_pairs,
        "signals": n_signals,
        "sides": len(SIDES),
        "exits": len(EXITS),
        "holds": len(HOLDS),
        "top_n": len(TOP_N),
        "cross": n_cross,
        "n": n,
        "luck": PRIOR_BASE + n,
    }


N = grid_counts()["n"]
LUCK_DENOMINATOR = grid_counts()["luck"]
