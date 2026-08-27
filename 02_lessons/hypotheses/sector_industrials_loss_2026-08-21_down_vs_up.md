---
kind: loss
scope: sector_industrials
date: 2026-08-21
status: open
---

# Hypothesis — sector_industrials / LOSS 2026-08-21

## WHEN
[sector_industrials] Predicted down but went up (pct=0.2670054603834737, score=-3.15, sector=Industrials).

## ASK (counterfactual)
Dominant factor family? Regime misread vs sector-specific shock? Shared macro S0 wrong or sector factors S1 wrong?

## EXPERIMENT
[sector_industrials] Require one extra confirming source in the dominant bucket before full weight when score sign matches this fail pattern.

## DO INSTEAD (policy candidate)
[sector_industrials] When score sign conflicts with sector ETF tape / breadth, cut conviction; prefer flat/mild.

## WRONG IF (falsifier)
[sector_industrials] Wrong if this hedge reduces direction accuracy over 10 runs.
