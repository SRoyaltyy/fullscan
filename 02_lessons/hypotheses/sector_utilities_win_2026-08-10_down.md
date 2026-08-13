---
kind: win
scope: sector_utilities
date: 2026-08-10
status: open
---

# Hypothesis — sector_utilities / WIN 2026-08-10

## WHEN
[sector_utilities] Predicted down, market/sector went down (pct=-1.1006639200146995, score=-8.8, sector=Utilities).

## ASK (counterfactual)
Could magnitude/conviction have been better? Double-count in factors? Missing confirming source?

## EXPERIMENT
[sector_utilities] On similar setups, test milder bands when |score|<4; log whether lagging tape factors overrode leading ones.

## DO INSTEAD (policy candidate)
[sector_utilities] Keep direction; shrink confidence on modest |score| when magnitude historically misses.

## WRONG IF (falsifier)
[sector_utilities] Wrong if milder bands hurt direction accuracy over 10 runs.
