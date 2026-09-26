"""Checks for the fingerprinted breadth_mine_v1e verdict rule."""
from __future__ import annotations

import numpy as np

from src.breadth_mine_v1e_bars import assert_split_consistent, unexplained
from src.breadth_mine_v1e_grid import LUCK_DENOMINATOR, N, PRIMARY_MIN
from src.breadth_mine_v1e_protocol import FINGERPRINT, FORMATION_END, fingerprint_sha256
from src.breadth_mine_v1e_score import apply_objective, cap_day, select_pick, study_verdict


def test_fingerprint_matches() -> None:
    assert fingerprint_sha256() == FINGERPRINT
    assert FORMATION_END == "2026-09-13"


def test_counts() -> None:
    assert N == 57600
    assert LUCK_DENOMINATOR == 297390
    assert PRIMARY_MIN == 0.30


def test_split_gate_passes() -> None:
    assert unexplained() == []
    assert_split_consistent()


def test_candidate_cap() -> None:
    names = [f"T{i:03d}" for i in range(80)]
    hot = np.array([float(i) for i in range(80)])
    day = cap_day({
        "names": names,
        "hot": hot,
        "atoms": np.ones((80, 2), dtype=bool),
        "open_of": {name: 1.0 for name in names},
        "close_of": {name: 1.0 for name in names},
        "universe": set(names),
    })
    assert day["candidate_n"] == 50
    assert day["names"][0] == "T079"


def _row(**overrides) -> dict:
    row = {
        "id": "b",
        "label": "",
        "full": 0.40,
        "fires": 40,
        "win": 0.60,
        "top1": 0.20,
        "n_check": 16,
        "random4_mean": 0.10,
        "iwm": 0.05,
        "luck_p": 0.90,
        "raw_p": 0.20,
    }
    row.update(overrides)
    return row


def test_luck_does_not_choose_the_pick() -> None:
    better = _row(id="better", full=0.50, luck_p=0.99)
    worse = _row(id="worse", full=0.31, luck_p=0.00)
    for row in (better, worse):
        apply_objective(row, True)
    pick = select_pick([worse, better])
    assert pick is not None
    assert pick["id"] == "better"


def test_forward_loss_is_not_a_win() -> None:
    row = _row()
    apply_objective(row, True)
    assert study_verdict(row, {"compound": -0.01}) == "no win"
    assert study_verdict(row, {"compound": 0.02}) == "win"
    assert study_verdict(None, None) == "nothing selected"


def test_guards_exclude_a_thin_book() -> None:
    thin = _row(fires=9, win=0.80)
    apply_objective(thin, True)
    assert thin["selection_pass"] is False
    assert "fires" in thin["failed"]
    assert "luck" not in thin["failed"]


if __name__ == "__main__":
    test_fingerprint_matches()
    test_counts()
    test_split_gate_passes()
    test_candidate_cap()
    test_luck_does_not_choose_the_pick()
    test_forward_loss_is_not_a_win()
    test_guards_exclude_a_thin_book()
    print("ok")
