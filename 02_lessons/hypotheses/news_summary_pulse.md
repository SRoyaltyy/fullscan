---
kind: win
scope: news
date: news
status: open
---

# Hypothesis — news / WIN news

## WHEN
[news] summary={"n_suggestions": 252, "ever_profitable": {"n": 252, "wins": 248, "win_rate": 98.4}, "close_1d": {"n": 247, "wins": 148, "win_rate": 59.9, "avg": 0.35}, "close_3d": {"n": 188, "wins": 133, "win_rate": 70.7, "avg": 1.99}, "close_5d": {"n": 157, "wins": 100, "win_rate": 63.7, "avg": 1.83}, "close_10d"

## ASK (counterfactual)
Which event families drive ever-profitable vs 1d close?

## EXPERIMENT
[news] Track event-level 1d close win rate daily in learn_cycle.

## DO INSTEAD (policy candidate)
[news] Rank event families by 1d close, not ever-touch MFE.

## WRONG IF (falsifier)
[news] Wrong if ever-touch is the better trading objective for you.
