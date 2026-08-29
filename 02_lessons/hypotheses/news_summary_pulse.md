---
kind: win
scope: news
date: news
status: open
---

# Hypothesis — news / WIN news

## WHEN
[news] summary={"n_suggestions": 566, "ever_profitable": {"n": 566, "wins": 555, "win_rate": 98.1}, "close_1d": {"n": 488, "wins": 281, "win_rate": 57.6, "avg": 0.12}, "close_3d": {"n": 312, "wins": 187, "win_rate": 59.9, "avg": 0.79}, "close_5d": {"n": 247, "wins": 145, "win_rate": 58.7, "avg": 1.44}, "close_10d"

## ASK (counterfactual)
Which event families drive ever-profitable vs 1d close?

## EXPERIMENT
[news] Track event-level 1d close win rate daily in learn_cycle.

## DO INSTEAD (policy candidate)
[news] Rank event families by 1d close, not ever-touch MFE.

## WRONG IF (falsifier)
[news] Wrong if ever-touch is the better trading objective for you.
