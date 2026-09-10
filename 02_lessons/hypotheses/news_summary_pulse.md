---
kind: win
scope: news
date: news
status: open
---

# Hypothesis — news / WIN news

## WHEN
[news] summary={"n_suggestions": 950, "ever_profitable": {"n": 950, "wins": 941, "win_rate": 99.1}, "close_1d": {"n": 914, "wins": 507, "win_rate": 55.5, "avg": 0.21}, "close_3d": {"n": 912, "wins": 563, "win_rate": 61.7, "avg": 0.96}, "close_5d": {"n": 867, "wins": 533, "win_rate": 61.5, "avg": 1.33}, "close_10d"

## ASK (counterfactual)
Which event families drive ever-profitable vs 1d close?

## EXPERIMENT
[news] Track event-level 1d close win rate daily in learn_cycle.

## DO INSTEAD (policy candidate)
[news] Rank event families by 1d close, not ever-touch MFE.

## WRONG IF (falsifier)
[news] Wrong if ever-touch is the better trading objective for you.
