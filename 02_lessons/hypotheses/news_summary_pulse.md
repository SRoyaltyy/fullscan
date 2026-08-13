---
kind: win
scope: news
date: news
status: open
---

# Hypothesis — news / WIN news

## WHEN
[news] summary={"n_suggestions": 123, "ever_profitable": {"n": 123, "wins": 121, "win_rate": 98.4}, "close_1d": {"n": 86, "wins": 56, "win_rate": 65.1, "avg": 0.88}, "close_3d": null, "close_5d": null, "close_10d": null, "close_14d": null, "side_buy": {"n": 72, "ever_profitable": {"n": 71, "wins": 71, "win_rate": 

## ASK (counterfactual)
Which event families drive ever-profitable vs 1d close?

## EXPERIMENT
[news] Track event-level 1d close win rate daily in learn_cycle.

## DO INSTEAD (policy candidate)
[news] Rank event families by 1d close, not ever-touch MFE.

## WRONG IF (falsifier)
[news] Wrong if ever-touch is the better trading objective for you.
