---
trigger_pattern: "A sector ETF posts an all-zero (or net-zero) S0–S4 leading card on the session AFTER an already-printed high-impact macro event (FOMC/SEP/presser), while the broad index backdrop is strongly positive (ES/NQ well green vs prior cash, yields falling) and the sector's own premarket is green but modest. The model nets the offsetting sector positives/negatives to exactly zero and emits flat/flat, when the correct expression is mild up with relative underperformance."
corrected_behavior: "When S0–S3 net to zero BUT (a) the index backdrop is strongly positive (ES/NQ ≥ +0.5% and ideally ≥ +1%), (b) yields are falling / duration-relief is live, and (c) the sector's own PM is green, the residual should be MILD UP with relative lag — not flat. Condition the 8/28 'residual-is-flat' rule on a NEUTRAL index tape; it must not bind when the broad tape is up >1%. Treat the 8/27 S4-cap as a cap on CONVICTION (cannot emit confirmed-up), not on LEVEL (a capped-up call is still an up call). Keep the relative-lag expression (XLB underperforms SPY) as the primary output, but do not convert 'lagging' into 'flat."
falsifier: "If a future session has an all-zero S0–S4 card, a strongly positive index tape (ES/NQ ≥ +0.5%, ideally ≥ +1%), falling yields, and a green sector PM, and the sector ETF nonetheless closes flat or down (|close| < 0.2% or negative), then the 'mild up / relative lag' correction is falsified and the flat rule should re-bind unconditionally. Conversely, if such a session closes up but the relative lag is absent (rel ≥ 0), the 'relative lag' half of the correction is falsified."
current_behavior: "Treat 'no materials thrust / tech-led bounce' as equivalent to 'flat,' and apply relative constraints (8/25 up-ban, 8/28 residual-is-flat, 8/27 S4-cap) to the ABSOLUTE direction. Net S1's offsetting legs to 0 rather than to a small positive when the index tape is up >1%. Use the S4-cap as a prediction of zero rather than a ceiling on conviction."
evidence_cited: "2026-09-17 XLB actual +0.695% / SPY +1.134% / rel −0.439%; open $50.90 (+1.07%) → close $50.71 (gap-up-then-fade). Morning call flat/flat, total_score 4.936, leading sum 0. The note itself wrote 'futures are a tech-led bounce, not a materials thrust' and used that to justify flat rather than mild-up-lagging. ES +1.71%, NQ +2.10%, XLB PM +0.17%, 10Y off 2007 high. The note's own S1 ledger (gold sleeve + oil relief + copper bounce vs China + glut + HEAT-down) was netted to 0 when the positives modestly dominated on a +1.1% SPY day. The 8/27 S4-cap was vindicated as a cap (rel −0.44% < 0.5%) but was mis-used as a level prediction."
error_category: "A"
scope: "general"
date: "2026-09-17"
status: "active"
occurrences: "2"
promoted_on: "2026-09-17"
sources: "['2026-09-17_sector_basic_materials_lesson.md', '2026-09-17_sector_consumer_cyclical_lesson.md']"
schema_ok: "true"
---

## RULE
When S0–S3 net to zero BUT (a) the index backdrop is strongly positive (ES/NQ ≥ +0.5% and ideally ≥ +1%), (b) yields are falling / duration-relief is live, and (c) the sector's own PM is green, the residual should be MILD UP with relative lag — not flat. Condition the 8/28 "residual-is-flat" rule on a NEUTRAL index tape; it must not bind when the broad tape is up >1%. Treat the 8/27 S4-cap as a cap on CONVICTION (cannot emit confirmed-up), not on LEVEL (a capped-up call is still an up call). Keep the relative-lag expression (XLB underperforms SPY) as the primary output, but do not convert "lagging" into "flat.

## WHEN IT FIRES
A sector ETF posts an all-zero (or net-zero) S0–S4 leading card on the session AFTER an already-printed high-impact macro event (FOMC/SEP/presser), while the broad index backdrop is strongly positive (ES/NQ well green vs prior cash, yields falling) and the sector's own premarket is green but modest. The model nets the offsetting sector positives/negatives to exactly zero and emits flat/flat, when the correct expression is mild up with relative underperformance.

## WRONG IF
If a future session has an all-zero S0–S4 card, a strongly positive index tape (ES/NQ ≥ +0.5%, ideally ≥ +1%), falling yields, and a green sector PM, and the sector ETF nonetheless closes flat or down (|close| < 0.2% or negative), then the "mild up / relative lag" correction is falsified and the flat rule should re-bind unconditionally. Conversely, if such a session closes up but the relative lag is absent (rel ≥ 0), the "relative lag" half of the correction is falsified.

## EVIDENCE
2026-09-17 XLB actual +0.695% / SPY +1.134% / rel −0.439%; open $50.90 (+1.07%) → close $50.71 (gap-up-then-fade). Morning call flat/flat, total_score 4.936, leading sum 0. The note itself wrote "futures are a tech-led bounce, not a materials thrust" and used that to justify flat rather than mild-up-lagging. ES +1.71%, NQ +2.10%, XLB PM +0.17%, 10Y off 2007 high. The note's own S1 ledger (gold sleeve + oil relief + copper bounce vs China + glut + HEAT-down) was netted to 0 when the positives modestly dominated on a +1.1% SPY day. The 8/27 S4-cap was vindicated as a cap (rel −0.44% < 0.5%) but was mis-used as a level prediction.

(learn_cycle promote)
