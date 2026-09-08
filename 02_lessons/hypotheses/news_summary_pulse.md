---
kind: win
scope: news
date: news
status: open
---

# Hypothesis — news / WIN news

## WHEN
[news] summary={"n_suggestions": 912, "ever_profitable": {"n": 912, "wins": 900, "win_rate": 98.7}, "close_1d": {"n": 912, "wins": 506, "win_rate": 55.5, "avg": 0.21}, "close_3d": {"n": 867, "wins": 538, "win_rate": 62.1, "avg": 1.01}, "close_5d": {"n": 656, "wins": 421, "win_rate": 64.2, "avg": 1.54}, "close_10d"

## ASK (counterfactual)
Which event families drive ever-profitable vs 1d close?

## EXPERIMENT
[news] Track event-level 1d close win rate daily in learn_cycle.

## DO INSTEAD (policy candidate)
[news] Rank event families by 1d close, not ever-touch MFE.

## WRONG IF (falsifier)
[news] Wrong if ever-touch is the better trading objective for you.
