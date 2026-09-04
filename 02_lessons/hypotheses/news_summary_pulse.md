---
kind: win
scope: news
date: news
status: open
---

# Hypothesis — news / WIN news

## WHEN
[news] summary={"n_suggestions": 912, "ever_profitable": {"n": 912, "wins": 895, "win_rate": 98.1}, "close_1d": {"n": 890, "wins": 493, "win_rate": 55.4, "avg": 0.23}, "close_3d": {"n": 795, "wins": 500, "win_rate": 62.9, "avg": 0.93}, "close_5d": {"n": 656, "wins": 417, "win_rate": 63.6, "avg": 1.52}, "close_10d"

## ASK (counterfactual)
Which event families drive ever-profitable vs 1d close?

## EXPERIMENT
[news] Track event-level 1d close win rate daily in learn_cycle.

## DO INSTEAD (policy candidate)
[news] Rank event families by 1d close, not ever-touch MFE.

## WRONG IF (falsifier)
[news] Wrong if ever-touch is the better trading objective for you.
