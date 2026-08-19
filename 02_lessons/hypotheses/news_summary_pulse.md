---
kind: win
scope: news
date: news
status: open
---

# Hypothesis — news / WIN news

## WHEN
[news] summary={"n_suggestions": 216, "ever_profitable": {"n": 216, "wins": 214, "win_rate": 99.1}, "close_1d": {"n": 188, "wins": 118, "win_rate": 62.8, "avg": 0.59}, "close_3d": {"n": 157, "wins": 107, "win_rate": 68.2, "avg": 1.38}, "close_5d": {"n": 87, "wins": 65, "win_rate": 74.7, "avg": 2.85}, "close_10d": 

## ASK (counterfactual)
Which event families drive ever-profitable vs 1d close?

## EXPERIMENT
[news] Track event-level 1d close win rate daily in learn_cycle.

## DO INSTEAD (policy candidate)
[news] Rank event families by 1d close, not ever-touch MFE.

## WRONG IF (falsifier)
[news] Wrong if ever-touch is the better trading objective for you.
