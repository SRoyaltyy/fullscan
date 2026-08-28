---
kind: loss
scope: sector_industrials
date: 2026-08-27
status: open
---

# Hypothesis — sector_industrials / LOSS 2026-08-27

## WHEN
[sector_industrials] Predicted up but went down (pct=-0.8539388474021248, score=7.65, sector=Industrials).

## ASK (counterfactual)
Dominant factor family? Regime misread vs sector-specific shock? Shared macro S0 wrong or sector factors S1 wrong?

## EXPERIMENT
[sector_industrials] Require one extra confirming source in the dominant bucket before full weight when score sign matches this fail pattern.

## DO INSTEAD (policy candidate)
[sector_industrials] When score sign conflicts with sector ETF tape / breadth, cut conviction; prefer flat/mild.

## WRONG IF (falsifier)
[sector_industrials] Wrong if this hedge reduces direction accuracy over 10 runs.
