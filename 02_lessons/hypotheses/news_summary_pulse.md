---
kind: win
scope: news
date: news
status: open
---

# Hypothesis — news / WIN news

## WHEN
[news] summary={"n_suggestions": 188, "ever_profitable": {"n": 188, "wins": 187, "win_rate": 99.5}, "close_1d": {"n": 170, "wins": 105, "win_rate": 61.8, "avg": 0.55}, "close_3d": {"n": 123, "wins": 87, "win_rate": 70.7, "avg": 1.27}, "close_5d": {"n": 47, "wins": 36, "win_rate": 76.6, "avg": 3.47}, "close_10d": n

## ASK (counterfactual)
Which event families drive ever-profitable vs 1d close?

## EXPERIMENT
[news] Track event-level 1d close win rate daily in learn_cycle.

## DO INSTEAD (policy candidate)
[news] Rank event families by 1d close, not ever-touch MFE.

## WRONG IF (falsifier)
[news] Wrong if ever-touch is the better trading objective for you.
