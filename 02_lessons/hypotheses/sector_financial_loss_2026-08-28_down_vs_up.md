---
kind: loss
scope: sector_financial
date: 2026-08-28
status: open
---

# Hypothesis — sector_financial / LOSS 2026-08-28

## WHEN
[sector_financial] Predicted down but went up (pct=0.3800922632101411, score=-2.925, sector=Financial).

## ASK (counterfactual)
Dominant factor family? Regime misread vs sector-specific shock? Shared macro S0 wrong or sector factors S1 wrong?

## EXPERIMENT
[sector_financial] Require one extra confirming source in the dominant bucket before full weight when score sign matches this fail pattern.

## DO INSTEAD (policy candidate)
[sector_financial] When score sign conflicts with sector ETF tape / breadth, cut conviction; prefer flat/mild.

## WRONG IF (falsifier)
[sector_financial] Wrong if this hedge reduces direction accuracy over 10 runs.
