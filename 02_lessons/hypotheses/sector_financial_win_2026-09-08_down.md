---
kind: win
scope: sector_financial
date: 2026-09-08
status: open
---

# Hypothesis — sector_financial / WIN 2026-09-08

## WHEN
[sector_financial] Predicted down, market/sector went down (pct=-1.3769350397089597, score=-2.925, sector=Financial).

## ASK (counterfactual)
Could magnitude/conviction have been better? Double-count in factors? Missing confirming source?

## EXPERIMENT
[sector_financial] On similar setups, test milder bands when |score|<4; log whether lagging tape factors overrode leading ones.

## DO INSTEAD (policy candidate)
[sector_financial] Keep direction; shrink confidence on modest |score| when magnitude historically misses.

## WRONG IF (falsifier)
[sector_financial] Wrong if milder bands hurt direction accuracy over 10 runs.
