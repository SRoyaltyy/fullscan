---
kind: win
scope: sector_real_estate
date: 2026-08-10
status: open
---

# Hypothesis — sector_real_estate / WIN 2026-08-10

## WHEN
[sector_real_estate] Predicted down, market/sector went down (pct=-1.2894575861718272, score=-6.75, sector=Real Estate).

## ASK (counterfactual)
Could magnitude/conviction have been better? Double-count in factors? Missing confirming source?

## EXPERIMENT
[sector_real_estate] On similar setups, test milder bands when |score|<4; log whether lagging tape factors overrode leading ones.

## DO INSTEAD (policy candidate)
[sector_real_estate] Keep direction; shrink confidence on modest |score| when magnitude historically misses.

## WRONG IF (falsifier)
[sector_real_estate] Wrong if milder bands hurt direction accuracy over 10 runs.
