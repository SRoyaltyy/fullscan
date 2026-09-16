---
kind: loss
scope: sector_financial
date: 2026-09-16
status: open
---

# Hypothesis — sector_financial / LOSS 2026-09-16

## WHEN
[sector_financial] Predicted flat but went down (pct=-1.6182905780799728, score=2.998, sector=Financial).

## ASK (counterfactual)
Dominant factor family? Regime misread vs sector-specific shock? Shared macro S0 wrong or sector factors S1 wrong?

## EXPERIMENT
[sector_financial] Require one extra confirming source in the dominant bucket before full weight when score sign matches this fail pattern.

## DO INSTEAD (policy candidate)
[sector_financial] When score sign conflicts with sector ETF tape / breadth, cut conviction; prefer flat/mild.

## WRONG IF (falsifier)
[sector_financial] Wrong if this hedge reduces direction accuracy over 10 runs.
