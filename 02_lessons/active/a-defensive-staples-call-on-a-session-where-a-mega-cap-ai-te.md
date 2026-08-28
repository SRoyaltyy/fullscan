---
trigger_pattern: "A defensive/staples call on a session where a mega-cap AI/tech earnings print is already public from the prior after-hours, Nasdaq futures lead ES (risk-on, NQ ≥ +0.5%), and the sector is already a 1d/1m relative laggard — but the model misdates the print as pending after today's close, parks it as unscored event risk, keeps S0 at 0, and applies the no-fresh-catalyst mild/flat cap."
corrected_behavior: "Before labeling mega-cap earnings “pending,” verify the actual report timestamp. If results/guide are already public overnight, treat them as a live shared-macro anti-FTS shock for staples (S0 negative), count NVDA + NQ>ES + XLP lag once, and do not fire the no-fresh-catalyst mild cap. With NQ ≥ +0.5% and a confirming negative 1d/1m relative tape, allow down/notable. Do not restack PCE, session oil-rebound, or sub-1% names (HRL) as extra hits."
falsifier: "If this setup recurs (prior-AH mega-cap AI beat already public, NQ ≥ +0.5% leading ES, XLP already a 1d/1m laggard) and XLP still prints only a mild absolute decline without a notable relative fade, lifting the mild cap / S0-negative is wrong and must be narrowed."
current_behavior: "Scored S0=0 (PCE digested; NVDA “pending, do not one-way score”), S1/S2/S4=-1, multiplier 0.9, emitted down/mild (narrative even flat-to-mild). Direction matched the anti-FTS tape; magnitude was capped because the already-out NVDA print was treated as tonight’s binary and the 08-10/08-14 mild caps were applied despite NQ +0.55%."
evidence_cited: "2026-08-27 predicted down/mild (S0=0, S1=-1, S2=-1, S3=0, S4=-1, mult 0.9, total -4.95). Actual XLP -1.38% vs SPY +0.66% (rel -2.03%), notable. NVDA printed AH 08-26 ($96.2B, Q3 $108B ±2%); cash NVDA +8.74%, Nasdaq +1.57%, staples worst. Morning wrote “Nvidia earnings due after the close today.”"
error_category: "B"
scope: "general"
date: "2026-08-27"
status: "active"
occurrences: "1"
promoted_on: "2026-08-28"
sources: "['2026-08-27_sector_consumer_defensive_lesson.md']"
schema_ok: "true"
---

## RULE
Before labeling mega-cap earnings “pending,” verify the actual report timestamp. If results/guide are already public overnight, treat them as a live shared-macro anti-FTS shock for staples (S0 negative), count NVDA + NQ>ES + XLP lag once, and do not fire the no-fresh-catalyst mild cap. With NQ ≥ +0.5% and a confirming negative 1d/1m relative tape, allow down/notable. Do not restack PCE, session oil-rebound, or sub-1% names (HRL) as extra hits.

## WHEN IT FIRES
A defensive/staples call on a session where a mega-cap AI/tech earnings print is already public from the prior after-hours, Nasdaq futures lead ES (risk-on, NQ ≥ +0.5%), and the sector is already a 1d/1m relative laggard — but the model misdates the print as pending after today's close, parks it as unscored event risk, keeps S0 at 0, and applies the no-fresh-catalyst mild/flat cap.

## WRONG IF
If this setup recurs (prior-AH mega-cap AI beat already public, NQ ≥ +0.5% leading ES, XLP already a 1d/1m laggard) and XLP still prints only a mild absolute decline without a notable relative fade, lifting the mild cap / S0-negative is wrong and must be narrowed.

## EVIDENCE
2026-08-27 predicted down/mild (S0=0, S1=-1, S2=-1, S3=0, S4=-1, mult 0.9, total -4.95). Actual XLP -1.38% vs SPY +0.66% (rel -2.03%), notable. NVDA printed AH 08-26 ($96.2B, Q3 $108B ±2%); cash NVDA +8.74%, Nasdaq +1.57%, staples worst. Morning wrote “Nvidia earnings due after the close today.”

(learn_cycle promote)
