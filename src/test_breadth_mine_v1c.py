"""Checks for the fingerprinted breadth_mine_v1c protocol."""
from __future__ import annotations

import numpy as np

from src.breadth_mine_v1c_bars import assert_split_consistent, unexplained
from src.breadth_mine_v1c_grid import (
    CANDIDATE_CAP,
    LUCK_DENOMINATOR,
    MIN_FIRES,
    N,
    RANDOM4_DRAWS,
    RANDOM4_N,
    RANDOM4_SEED,
    WIN_MIN,
)
from src.breadth_mine_v1c_protocol import FINGERPRINT, fingerprint_sha256
from src.breadth_mine_v1c_score import cap_day, random4_names


def test_fingerprint_matches() -> None:
    assert fingerprint_sha256() == FINGERPRINT


def test_counts() -> None:
    assert N == 57600
    assert LUCK_DENOMINATOR == 182190
    assert CANDIDATE_CAP == 50
    assert MIN_FIRES == 30
    assert WIN_MIN == 0.55
    assert RANDOM4_SEED == 20260813
    assert RANDOM4_DRAWS == 1000
    assert RANDOM4_N == 4


def test_candidate_cap() -> None:
    names = [f"T{i:03d}" for i in range(80)]
    hot = np.array([float(i) for i in range(80)])
    atoms = np.ones((80, 2), dtype=bool)
    day = cap_day({
        "names": names,
        "hot": hot,
        "atoms": atoms,
        "open_of": {name: 1.0 for name in names},
        "close_of": {name: 1.0 for name in names},
        "universe": set(names),
    })
    assert day["candidate_n"] == 50
    assert day["names"][0] == "T079"
    assert day["names"][-1] == "T030"
    assert len(day["universe"]) == 50


def test_random4_seed_is_stable() -> None:
    pool = [f"T{i:02d}" for i in range(10)]
    first = random4_names(pool, 0)
    assert first == random4_names(pool, 0)
    assert len(first) == 4
    assert first != random4_names(pool, 1)


def test_rule26_refuses_unexplained_jumps() -> None:
    bad = unexplained()
    assert len(bad) == 57
    try:
        assert_split_consistent()
    except SystemExit:
        return
    raise AssertionError("rule 26 should refuse this snapshot")


if __name__ == "__main__":
    test_fingerprint_matches()
    test_counts()
    test_candidate_cap()
    test_random4_seed_is_stable()
    test_rule26_refuses_unexplained_jumps()
    print("ok")
