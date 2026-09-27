"""Pass-rule arithmetic. No market score."""
from __future__ import annotations

from research.pricefull.rules import (
    CAPITAL,
    cluster_test,
    empty_ledger,
    holm,
    rerun_empty,
    summarize,
)


def test_holm_matches_the_longhist_check():
    adj = holm([0.01, 0.04, 0.03])
    assert abs(adj[0] - 0.03) < 1e-12
    assert abs(adj[1] - 0.06) < 1e-12
    assert abs(adj[2] - 0.06) < 1e-12


def test_two_empty_pvalues_stay_at_one():
    adj = holm([1.0, 1.0])
    assert adj == [1.0, 1.0]


def test_flat_positive_cluster_is_zero_p():
    got = cluster_test([0.1, 0.1])
    assert got["p"] == 0.0
    assert got["mean"] == 0.1


def test_empty_book_fails_every_pass_rule():
    sessions = ["2019-01-02", "2019-01-03"]
    ledger = empty_ledger("pricefull_w1d", sessions)
    row = summarize(
        "pricefull_w1d",
        ledger,
        sessions,
        1.0,
        rerun_empty([]),
        rerun_empty([]),
    )
    assert row["closed_trades"] == 0
    assert row["trades_per_session"] == 0.0
    assert row["cluster_mean"] is None
    assert row["p"] == 1.0
    assert row["pass_9_1_mean"] is False
    assert row["pass_9_2_years"] is False
    assert row["pass_9_3_best_removed"] is False
    assert row["pass_9_4_rate"] is False
    assert row["pass_9_5_period"] is False
    assert row["study_pass"] is False
    assert row["ironclad_keep_bar"] is False
    assert row["best_share_of_closed_pnl"] is None
    assert row["return_without_best"] == 0.0
    assert row["return_without_top3"] == 0.0
    assert row["ending_equity"] == CAPITAL
    assert row["wins_losses_by_exit_year"]["2019"]["wins"] == 0
    assert row["wins_losses_by_exit_year"]["2019"]["losses"] == 0
