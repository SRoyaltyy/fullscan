---
kind: win
scope: news
date: news
status: open
---

# Hypothesis — news / WIN news

## WHEN
[news] summary={"n_suggestions": 914, "ever_profitable": {"n": 914, "wins": 903, "win_rate": 98.8}, "close_1d": {"n": 912, "wins": 506, "win_rate": 55.5, "avg": 0.21}, "close_3d": {"n": 890, "wins": 548, "win_rate": 61.6, "avg": 0.98}, "close_5d": {"n": 795, "wins": 499, "win_rate": 62.8, "avg": 1.41}, "close_10d"

## ASK (counterfactual)
Which event families drive ever-profitable vs 1d close?

## EXPERIMENT
[news] Track event-level 1d close win rate daily in learn_cycle.

## DO INSTEAD (policy candidate)
[news] Rank event families by 1d close, not ever-touch MFE.

## WRONG IF (falsifier)
[news] Wrong if ever-touch is the better trading objective for you.
