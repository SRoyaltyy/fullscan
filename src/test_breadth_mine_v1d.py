"""Checks for the fingerprinted breadth_mine_v1d protocol."""
from __future__ import annotations

import numpy as np

from src.breadth_mine_v1d_bars import assert_split_consistent, unexplained
from src.breadth_mine_v1d_grid import (
    CANDIDATE_CAP,
    LUCK_DENOMINATOR,
    MIN_FIRES,
    N,
    RANDOM4_DRAWS,
    RANDOM4_N,
    PRIMARY_MIN,
    RANDOM4_SEED,
    WIN_MIN,
)
from src.breadth_mine_v1d_protocol import FINGERPRINT, fingerprint_sha256
from src.breadth_mine_v1d_score import apply_objective, cap_day, random4_names, render_report


def test_fingerprint_matches() -> None:
    assert fingerprint_sha256() == FINGERPRINT


def test_counts() -> None:
    assert N == 57600
    assert LUCK_DENOMINATOR == 239790
    assert PRIMARY_MIN == 0.30
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


def _stat(**overrides) -> dict:
    row = {
        "id": "a",
        "label": "",
        "full": 0.40,
        "ex_best": 0.20,
        "top1": 0.20,
        "fires": 40,
        "win": 0.60,
        "from_0914": 0.01,
        "luck_p": 0.01,
        "random4_mean": 0.10,
        "iwm": 0.05,
        "n_check": 16,
        "side": "long",
        "best": "ZZ",
        "median": 0.01,
        "top3": 0.4,
        "trades_per_day": 2.0,
        "random4_beaten": 0.8,
        "under_3": 0.0,
        "full_15": 0.3,
        "raw_p": 0.01,
    }
    row.update(overrides)
    return row


def test_primary_is_the_compound_and_guards_are_separate() -> None:
    clear = _stat()
    apply_objective(clear, True)
    assert clear["label"] == "designed_after"
    assert clear["primary_hit"] is True
    assert clear["failed"] == []
    short = _stat(full=0.20)
    apply_objective(short, True)
    assert short["label"] == "guards pass"
    assert short["primary_hit"] is False
    missed = _stat(id="high", full=0.90, fires=1, win=0.40, ex_best=0.01)
    apply_objective(missed, True)
    assert missed["failed"] == ["ex-best", "fires", "win"]
    flat = _stat(top1=None)
    apply_objective(flat, True)
    assert "top-1" not in flat["failed"]
    quiet = _stat(label="untestable")
    apply_objective(quiet, True)
    assert quiet["label"] == "untestable"


def test_report_ranks_passers_by_compound_and_lists_failures() -> None:
    passing = _stat(id="pass", full=0.31)
    apply_objective(passing, True)
    bigger = _stat(id="high", full=0.90, fires=1)
    apply_objective(bigger, True)
    text = render_report([bigger, passing], "nothing proven yet")
    first, second = text.split("## Top 20 by compound, guards shown")
    assert "`pass`" in first
    assert "`high`" not in first
    assert second.index("`high`") < second.index("`pass`")
    assert "fires" in second


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
    test_primary_is_the_compound_and_guards_are_separate()
    test_report_ranks_passers_by_compound_and_lists_failures()
    test_rule26_refuses_unexplained_jumps()
    print("ok")
