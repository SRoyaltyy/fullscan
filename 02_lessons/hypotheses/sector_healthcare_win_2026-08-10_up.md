---
kind: win
scope: sector_healthcare
date: 2026-08-10
status: open
---

# Hypothesis — sector_healthcare / WIN 2026-08-10

## WHEN
[sector_healthcare] Predicted up, market/sector went up (pct=1.6658678703747043, score=14.7, sector=Healthcare).

## ASK (counterfactual)
Could magnitude/conviction have been better? Double-count in factors? Missing confirming source?

## EXPERIMENT
[sector_healthcare] On similar setups, test milder bands when |score|<4; log whether lagging tape factors overrode leading ones.

## DO INSTEAD (policy candidate)
[sector_healthcare] Keep direction; shrink confidence on modest |score| when magnitude historically misses.

## WRONG IF (falsifier)
[sector_healthcare] Wrong if milder bands hurt direction accuracy over 10 runs.
