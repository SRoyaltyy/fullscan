---
kind: loss
scope: news
date: news
status: open
---

# Hypothesis — news / LOSS news

## WHEN
[news] Global 1d close win rate 54.1% (n=950).

## ASK (counterfactual)
Entry timing, side mix, or event taxonomy noise?

## EXPERIMENT
[news] Raise min net weight to map a ticker; drop weak edges.

## DO INSTEAD (policy candidate)
[news] Only emit actions with |net| above a higher floor.

## WRONG IF (falsifier)
[news] Wrong if higher floor reduces 1d win rate further.
