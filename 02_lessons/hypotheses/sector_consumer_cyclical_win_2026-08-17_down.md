---
kind: win
scope: sector_consumer_cyclical
date: 2026-08-17
status: open
---

# Hypothesis — sector_consumer_cyclical / WIN 2026-08-17

## WHEN
[sector_consumer_cyclical] Predicted down, market/sector went down (pct=-1.2267317983748471, score=-12.0, sector=Consumer Cyclical).

## ASK (counterfactual)
Could magnitude/conviction have been better? Double-count in factors? Missing confirming source?

## EXPERIMENT
[sector_consumer_cyclical] On similar setups, test milder bands when |score|<4; log whether lagging tape factors overrode leading ones.

## DO INSTEAD (policy candidate)
[sector_consumer_cyclical] Keep direction; shrink confidence on modest |score| when magnitude historically misses.

## WRONG IF (falsifier)
[sector_consumer_cyclical] Wrong if milder bands hurt direction accuracy over 10 runs.
