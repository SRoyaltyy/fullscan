---
kind: win
scope: news
date: news
status: open
---

# Hypothesis — news / WIN news

## WHEN
[news] summary={"n_suggestions": 494, "ever_profitable": {"n": 494, "wins": 488, "win_rate": 98.8}, "close_1d": {"n": 440, "wins": 248, "win_rate": 56.4, "avg": 0.08}, "close_3d": {"n": 286, "wins": 173, "win_rate": 60.5, "avg": 0.91}, "close_5d": {"n": 247, "wins": 145, "win_rate": 58.7, "avg": 1.3}, "close_10d":

## ASK (counterfactual)
Which event families drive ever-profitable vs 1d close?

## EXPERIMENT
[news] Track event-level 1d close win rate daily in learn_cycle.

## DO INSTEAD (policy candidate)
[news] Rank event families by 1d close, not ever-touch MFE.

## WRONG IF (falsifier)
[news] Wrong if ever-touch is the better trading objective for you.
