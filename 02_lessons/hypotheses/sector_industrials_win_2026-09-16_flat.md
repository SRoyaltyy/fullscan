---
kind: win
scope: sector_industrials
date: 2026-09-16
status: open
---

# Hypothesis — sector_industrials / WIN 2026-09-16

## WHEN
[sector_industrials] Predicted flat, market/sector went flat (pct=-0.08291346436943847, score=7.288, sector=Industrials).

## ASK (counterfactual)
Could magnitude/conviction have been better? Double-count in factors? Missing confirming source?

## EXPERIMENT
[sector_industrials] On similar setups, test milder bands when |score|<4; log whether lagging tape factors overrode leading ones.

## DO INSTEAD (policy candidate)
[sector_industrials] Keep direction; shrink confidence on modest |score| when magnitude historically misses.

## WRONG IF (falsifier)
[sector_industrials] Wrong if milder bands hurt direction accuracy over 10 runs.
