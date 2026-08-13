---
kind: win
scope: sector_energy
date: 2026-08-12
status: open
---

# Hypothesis — sector_energy / WIN 2026-08-12

## WHEN
[sector_energy] Predicted up, market/sector went up (pct=0.16412025869068092, score=12.0, sector=Energy).

## ASK (counterfactual)
Could magnitude/conviction have been better? Double-count in factors? Missing confirming source?

## EXPERIMENT
[sector_energy] On similar setups, test milder bands when |score|<4; log whether lagging tape factors overrode leading ones.

## DO INSTEAD (policy candidate)
[sector_energy] Keep direction; shrink confidence on modest |score| when magnitude historically misses.

## WRONG IF (falsifier)
[sector_energy] Wrong if milder bands hurt direction accuracy over 10 runs.
