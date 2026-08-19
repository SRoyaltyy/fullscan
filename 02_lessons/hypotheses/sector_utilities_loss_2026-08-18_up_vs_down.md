---
kind: loss
scope: sector_utilities
date: 2026-08-18
status: open
---

# Hypothesis — sector_utilities / LOSS 2026-08-18

## WHEN
[sector_utilities] Predicted up but went down (pct=-0.36215447330670836, score=9.0, sector=Utilities).

## ASK (counterfactual)
Dominant factor family? Regime misread vs sector-specific shock? Shared macro S0 wrong or sector factors S1 wrong?

## EXPERIMENT
[sector_utilities] Require one extra confirming source in the dominant bucket before full weight when score sign matches this fail pattern.

## DO INSTEAD (policy candidate)
[sector_utilities] When score sign conflicts with sector ETF tape / breadth, cut conviction; prefer flat/mild.

## WRONG IF (falsifier)
[sector_utilities] Wrong if this hedge reduces direction accuracy over 10 runs.
