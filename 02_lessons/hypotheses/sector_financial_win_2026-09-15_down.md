---
kind: win
scope: sector_financial
date: 2026-09-15
status: open
---

# Hypothesis — sector_financial / WIN 2026-09-15

## WHEN
[sector_financial] Predicted down, market/sector went down (pct=-0.3156238979986181, score=-4.063, sector=Financial).

## ASK (counterfactual)
Could magnitude/conviction have been better? Double-count in factors? Missing confirming source?

## EXPERIMENT
[sector_financial] On similar setups, test milder bands when |score|<4; log whether lagging tape factors overrode leading ones.

## DO INSTEAD (policy candidate)
[sector_financial] Keep direction; shrink confidence on modest |score| when magnitude historically misses.

## WRONG IF (falsifier)
[sector_financial] Wrong if milder bands hurt direction accuracy over 10 runs.
