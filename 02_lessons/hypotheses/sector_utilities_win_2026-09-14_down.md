---
kind: win
scope: sector_utilities
date: 2026-09-14
status: open
---

# Hypothesis — sector_utilities / WIN 2026-09-14

## WHEN
[sector_utilities] Predicted down, market/sector went down (pct=-1.3446560581065081, score=-7.559, sector=Utilities).

## ASK (counterfactual)
Could magnitude/conviction have been better? Double-count in factors? Missing confirming source?

## EXPERIMENT
[sector_utilities] On similar setups, test milder bands when |score|<4; log whether lagging tape factors overrode leading ones.

## DO INSTEAD (policy candidate)
[sector_utilities] Keep direction; shrink confidence on modest |score| when magnitude historically misses.

## WRONG IF (falsifier)
[sector_utilities] Wrong if milder bands hurt direction accuracy over 10 runs.
