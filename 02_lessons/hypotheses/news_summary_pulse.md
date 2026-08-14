---
kind: win
scope: news
date: news
status: open
---

# Hypothesis — news / WIN news

## WHEN
[news] summary={"n_suggestions": 157, "ever_profitable": {"n": 157, "wins": 154, "win_rate": 98.1}, "close_1d": {"n": 123, "wins": 69, "win_rate": 56.1, "avg": 0.32}, "close_3d": {"n": 44, "wins": 28, "win_rate": 63.6, "avg": 0.94}, "close_5d": null, "close_10d": null, "close_14d": null, "side_buy": {"n": 89, "eve

## ASK (counterfactual)
Which event families drive ever-profitable vs 1d close?

## EXPERIMENT
[news] Track event-level 1d close win rate daily in learn_cycle.

## DO INSTEAD (policy candidate)
[news] Rank event families by 1d close, not ever-touch MFE.

## WRONG IF (falsifier)
[news] Wrong if ever-touch is the better trading objective for you.
