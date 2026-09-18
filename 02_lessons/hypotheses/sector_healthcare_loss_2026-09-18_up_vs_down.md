---
kind: loss
scope: sector_healthcare
date: 2026-09-18
status: open
---

# Hypothesis — sector_healthcare / LOSS 2026-09-18

## WHEN
[sector_healthcare] Predicted up but went down (pct=-0.24879934542948456, score=2.404, sector=Healthcare).

## ASK (counterfactual)
Dominant factor family? Regime misread vs sector-specific shock? Shared macro S0 wrong or sector factors S1 wrong?

## EXPERIMENT
[sector_healthcare] Require one extra confirming source in the dominant bucket before full weight when score sign matches this fail pattern.

## DO INSTEAD (policy candidate)
[sector_healthcare] When score sign conflicts with sector ETF tape / breadth, cut conviction; prefer flat/mild.

## WRONG IF (falsifier)
[sector_healthcare] Wrong if this hedge reduces direction accuracy over 10 runs.
