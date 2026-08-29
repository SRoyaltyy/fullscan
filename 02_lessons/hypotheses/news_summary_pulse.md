---
kind: win
scope: news
date: news
status: open
---

# Hypothesis — news / WIN news

## WHEN
[news] summary={"n_suggestions": 651, "ever_profitable": {"n": 651, "wins": 639, "win_rate": 98.2}, "close_1d": {"n": 568, "wins": 315, "win_rate": 55.5, "avg": -0.08}, "close_3d": {"n": 438, "wins": 263, "win_rate": 60.0, "avg": 0.52}, "close_5d": {"n": 286, "wins": 159, "win_rate": 55.6, "avg": 0.79}, "close_10d

## ASK (counterfactual)
Which event families drive ever-profitable vs 1d close?

## EXPERIMENT
[news] Track event-level 1d close win rate daily in learn_cycle.

## DO INSTEAD (policy candidate)
[news] Rank event families by 1d close, not ever-touch MFE.

## WRONG IF (falsifier)
[news] Wrong if ever-touch is the better trading objective for you.
