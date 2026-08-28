---
kind: loss
scope: sector_real_estate
date: 2026-08-27
status: open
---

# Hypothesis — sector_real_estate / LOSS 2026-08-27

## WHEN
[sector_real_estate] Predicted up but went down (pct=-0.9536489326250397, score=4.5, sector=Real Estate).

## ASK (counterfactual)
Dominant factor family? Regime misread vs sector-specific shock? Shared macro S0 wrong or sector factors S1 wrong?

## EXPERIMENT
[sector_real_estate] Require one extra confirming source in the dominant bucket before full weight when score sign matches this fail pattern.

## DO INSTEAD (policy candidate)
[sector_real_estate] When score sign conflicts with sector ETF tape / breadth, cut conviction; prefer flat/mild.

## WRONG IF (falsifier)
[sector_real_estate] Wrong if this hedge reduces direction accuracy over 10 runs.
