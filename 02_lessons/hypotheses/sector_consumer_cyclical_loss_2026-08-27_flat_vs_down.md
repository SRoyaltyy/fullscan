---
kind: loss
scope: sector_consumer_cyclical
date: 2026-08-27
status: open
---

# Hypothesis — sector_consumer_cyclical / LOSS 2026-08-27

## WHEN
[sector_consumer_cyclical] Predicted flat but went down (pct=-1.0925284812920988, score=0.5, sector=Consumer Cyclical).

## ASK (counterfactual)
Dominant factor family? Regime misread vs sector-specific shock? Shared macro S0 wrong or sector factors S1 wrong?

## EXPERIMENT
[sector_consumer_cyclical] Require one extra confirming source in the dominant bucket before full weight when score sign matches this fail pattern.

## DO INSTEAD (policy candidate)
[sector_consumer_cyclical] When score sign conflicts with sector ETF tape / breadth, cut conviction; prefer flat/mild.

## WRONG IF (falsifier)
[sector_consumer_cyclical] Wrong if this hedge reduces direction accuracy over 10 runs.
