---
kind: win
scope: news
date: news
status: open
---

# Hypothesis — news / WIN news

## WHEN
[news] summary={"n_suggestions": 170, "ever_profitable": {"n": 170, "wins": 168, "win_rate": 98.8}, "close_1d": {"n": 157, "wins": 91, "win_rate": 58.0, "avg": 0.32}, "close_3d": {"n": 87, "wins": 59, "win_rate": 67.8, "avg": 0.97}, "close_5d": null, "close_10d": null, "close_14d": null, "side_buy": {"n": 97, "eve

## ASK (counterfactual)
Which event families drive ever-profitable vs 1d close?

## EXPERIMENT
[news] Track event-level 1d close win rate daily in learn_cycle.

## DO INSTEAD (policy candidate)
[news] Rank event families by 1d close, not ever-touch MFE.

## WRONG IF (falsifier)
[news] Wrong if ever-touch is the better trading objective for you.
