---
trigger_pattern: "A correct-direction Financial sector call on a constructive tape (positive futures, easing long-end yields, positive S4 relative strength) where the deterministic pipeline total maps to “notable,” while the narrative, applying rolling magnitude-accuracy discipline and residual macro overhangs, independently caps the band at “mild.” Both bands are left in the emitted output, the scorecard snapshots the higher official pipeline band, and the magnitude is marked a miss despite the direction being correct."
corrected_behavior: "Before emitting the final SECTOR_SCORES, reconcile to one unambiguous total and magnitude band. If the narrative applies a magnitude temper — here, rolling mag accuracy 0.0 plus lingering Fed-minutes/consumer-credit/CRE overhangs — that temper must be propagated into the official pipeline total/band. Conversely, if the official pipeline band is intentionally left unchanged, the narrative must not state a different final band. For Financials specifically, do not let a single positive XLF day mechanically lift the official band to “notable” when the existing magnitude record strongly favors capping at mild."
falsifier: "This lesson is falsified if, in the next several Financial sector calls, applying this reconciliation and capping the official band at mild consistently under-calls actuals that the pipeline’s notable band would have hit — e.g., XLF closes at/above the rubric’s notable threshold on constructive macro and the scoreboard shows magnitude_hit True for the tempered output. If the tempered band repeatedly misses under-call while the unreconciled pipeline band would have hit, then the tempering rule is too aggressive and should be revised."
current_behavior: "The system produces two conflicting final magnitudes: the pipeline-computed decision says `up/notable` (total_score 11.0), while the narrative section says `up/mild` (total_score 5.0). No reconciliation step forces them to agree. When the scorecard later records `predicted up/notable vs actual 0.93%`, the conservative narrative band is ignored and the pipeline over-call is scored as a magnitude miss."
evidence_cited: "2026-08-21 XLF +0.93%, SPY +0.41%, relative +0.52%. Scoreboard: `direction_hit: True | magnitude_hit: False | predicted up/notable vs actual 0.9306%`. The prediction narrative ended at `TOTAL_SCORE: 5.0 / PREDICTED_MAGNITUDE_BAND: mild`, while the pipeline-computed decision emitted `total_score: 11.0 / predicted_magnitude_band: notable`. The post-session narrative itself labels the actual move “notable,” but the scoreboard treats the pipeline band as a miss; the scoreboard is authoritative. Direction was right; the scored error was magnitude over-call caused by an unreconciled final output."
error_category: "B"
scope: "general"
date: "2026-08-21"
status: "active"
occurrences: "1"
promoted_on: "2026-08-27"
sources: "['2026-08-21_sector_financial_lesson.md']"
schema_ok: "true"
---

## RULE
Before emitting the final SECTOR_SCORES, reconcile to one unambiguous total and magnitude band. If the narrative applies a magnitude temper — here, rolling mag accuracy 0.0 plus lingering Fed-minutes/consumer-credit/CRE overhangs — that temper must be propagated into the official pipeline total/band. Conversely, if the official pipeline band is intentionally left unchanged, the narrative must not state a different final band. For Financials specifically, do not let a single positive XLF day mechanically lift the official band to “notable” when the existing magnitude record strongly favors capping at mild.

## WHEN IT FIRES
A correct-direction Financial sector call on a constructive tape (positive futures, easing long-end yields, positive S4 relative strength) where the deterministic pipeline total maps to “notable,” while the narrative, applying rolling magnitude-accuracy discipline and residual macro overhangs, independently caps the band at “mild.” Both bands are left in the emitted output, the scorecard snapshots the higher official pipeline band, and the magnitude is marked a miss despite the direction being correct.

## WRONG IF
This lesson is falsified if, in the next several Financial sector calls, applying this reconciliation and capping the official band at mild consistently under-calls actuals that the pipeline’s notable band would have hit — e.g., XLF closes at/above the rubric’s notable threshold on constructive macro and the scoreboard shows magnitude_hit True for the tempered output. If the tempered band repeatedly misses under-call while the unreconciled pipeline band would have hit, then the tempering rule is too aggressive and should be revised.

## EVIDENCE
2026-08-21 XLF +0.93%, SPY +0.41%, relative +0.52%. Scoreboard: `direction_hit: True | magnitude_hit: False | predicted up/notable vs actual 0.9306%`. The prediction narrative ended at `TOTAL_SCORE: 5.0 / PREDICTED_MAGNITUDE_BAND: mild`, while the pipeline-computed decision emitted `total_score: 11.0 / predicted_magnitude_band: notable`. The post-session narrative itself labels the actual move “notable,” but the scoreboard treats the pipeline band as a miss; the scoreboard is authoritative. Direction was right; the scored error was magnitude over-call caused by an unreconciled final output.

(learn_cycle promote)
