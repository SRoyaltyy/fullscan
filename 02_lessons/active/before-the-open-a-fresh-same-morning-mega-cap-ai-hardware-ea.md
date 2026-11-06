---
trigger_pattern: "Before the open, a fresh same-morning mega-cap/AI-hardware earnings miss is present while NQ confirms ≤ −2% and ES confirms ≥0.5% down, with no unsigned CPI/NFP/FOMC binary."
corrected_behavior: "When this trigger matches, set B1_CATALYSTS = −3, keep B6 = −0.5 as confirmation-only, and force predicted_magnitude_band = notable with multiplier ≥1.2; do not let Apple/Meta green prints, offered oil (B7 +0.5), or a missing B2 bond print cap the band below notable."
falsifier: "If this trigger recurs and SPX closes inside the mild band (≤1.0%) on 2 of the next 3 such days, the B1=−3/notable gate is wrong and must be revised."
current_behavior: "Scored B1_CATALYSTS at −2 and emitted down/mild despite AMZN/INTC fresh misses, NQ −2.39%, ES −1.06%, and Kospi −3.48%; B7 +0.5 and Apple/Meta offsets diluted the tech-rout severity, and B2 was left 0 despite the bond rally."
evidence_cited: "2026-10-30 predicted down/mild (total −12.921, B1 −2, B6 −0.5, NQ −2.39%, ES −1.06%) vs actual SPX −1.82% down/notable, NDX −2.86%; factor mismatch was B1 −2 vs a severe fresh AI-hardware rout."
error_category: "B"
scope: "general"
date: "2026-10-30"
status: "active"
occurrences: "2"
promoted_on: "2026-11-05"
sources: "['2026-10-30_lesson.md', '2026-11-05_lesson.md']"
schema_ok: "true"
---

## RULE
When this trigger matches, set B1_CATALYSTS = −3, keep B6 = −0.5 as confirmation-only, and force predicted_magnitude_band = notable with multiplier ≥1.2; do not let Apple/Meta green prints, offered oil (B7 +0.5), or a missing B2 bond print cap the band below notable.

## WHEN IT FIRES
Before the open, a fresh same-morning mega-cap/AI-hardware earnings miss is present while NQ confirms ≤ −2% and ES confirms ≥0.5% down, with no unsigned CPI/NFP/FOMC binary.

## WRONG IF
If this trigger recurs and SPX closes inside the mild band (≤1.0%) on 2 of the next 3 such days, the B1=−3/notable gate is wrong and must be revised.

## EVIDENCE
2026-10-30 predicted down/mild (total −12.921, B1 −2, B6 −0.5, NQ −2.39%, ES −1.06%) vs actual SPX −1.82% down/notable, NDX −2.86%; factor mismatch was B1 −2 vs a severe fresh AI-hardware rout.

(learn_cycle promote)
