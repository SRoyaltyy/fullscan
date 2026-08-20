---
kind: win
scope: news
date: news
status: open
---

# Hypothesis — news / WIN news

## WHEN
[news] summary={"n_suggestions": 247, "ever_profitable": {"n": 247, "wins": 241, "win_rate": 97.6}, "close_1d": {"n": 216, "wins": 131, "win_rate": 60.6, "avg": 0.43}, "close_3d": {"n": 170, "wins": 120, "win_rate": 70.6, "avg": 1.78}, "close_5d": {"n": 123, "wins": 87, "win_rate": 70.7, "avg": 2.26}, "close_10d":

## ASK (counterfactual)
Which event families drive ever-profitable vs 1d close?

## EXPERIMENT
[news] Track event-level 1d close win rate daily in learn_cycle.

## DO INSTEAD (policy candidate)
[news] Rank event families by 1d close, not ever-touch MFE.

## WRONG IF (falsifier)
[news] Wrong if ever-touch is the better trading objective for you.
