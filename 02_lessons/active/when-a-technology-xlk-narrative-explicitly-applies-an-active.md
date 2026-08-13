---
trigger_pattern: "When a Technology/XLK narrative explicitly applies an active risk-off reflect lesson and says “flat” or “flat/down,” but the deterministic pipeline still emits “up” because the signed component effects were lost in aggregation (e.g., leading_sum is computed from absolute magnitudes rather than the signed S0+S1+S2+S3 sum), the final graded prediction must be reconciled. Relative tape strength vs SPY must not be converted into absolute up direction; XLK can fall while outperforming SPY."
corrected_behavior: "When the narrative override and the deterministic output disagree, resolve the conflict before finalizing. Use signed component scores rather than absolute magnitudes when computing the leading sum and direction. If a fresh macro shock + crowded tech + stale catalysts is knowable at open, emit flat/down — not up. Also, treat relative outperformance vs SPY as a relative note, not as evidence for an absolute up move. If the fresh catalyst is after-hours, do not count it as same-session upward support."
falsifier: "This lesson fails if a similar setup — narrative flat/down, pipeline up, negative macro, crowded tech, stale catalysts — is overridden to flat/down but XLK closes solidly up on a genuinely fresh same-session catalyst or a macro reversal. The override should be conditional: it must be based on signed component scores, not automatic narrative preference."
current_behavior: "On 2026-08-11, the narrative correctly identified the Hormuz inflation/geopolitical shock, scored S0_SHARED_MACRO -1 and S3_FLOWS_POSITIONING -1, invoked the active 2026-08-10 reflect lesson, and wrote “Direction: flat” and “prefer flat/down.” However, the deterministic pipeline emitted predicted_direction “up” with total_score 2.0 and a leading_sum of 4.0, which does not match the signed component sum of 0 (-1 + 2 + 0 - 1 = 0). The final graded call was therefore up/flat instead of flat/down."
evidence_cited: "2026-08-11 XLK actual: ETF_PCT -0.12%, SPY_PCT -0.32%, REL_PCT +0.20%. The morning tape read S4=0 with 1d rel +0.17%; actual rel +0.20% confirmed relative outperformance, but absolute XLK closed down. Hormuz/oil/CPI risk-off was knowable at open and negative futures (NQ -0.26%) confirmed weakness. Supermicro earnings after-hours were not a regular-session driver. Pipeline emitted up despite narrative flat, causing direction_hit False; magnitude_hit True."
error_category: "C"
scope: "general"
date: "2026-08-11"
status: "active"
occurrences: "1"
promoted_on: "2026-08-13"
sources: "['2026-08-11_sector_technology_lesson.md']"
schema_ok: "true"
---

## RULE
When the narrative override and the deterministic output disagree, resolve the conflict before finalizing. Use signed component scores rather than absolute magnitudes when computing the leading sum and direction. If a fresh macro shock + crowded tech + stale catalysts is knowable at open, emit flat/down — not up. Also, treat relative outperformance vs SPY as a relative note, not as evidence for an absolute up move. If the fresh catalyst is after-hours, do not count it as same-session upward support.

## WHEN IT FIRES
When a Technology/XLK narrative explicitly applies an active risk-off reflect lesson and says “flat” or “flat/down,” but the deterministic pipeline still emits “up” because the signed component effects were lost in aggregation (e.g., leading_sum is computed from absolute magnitudes rather than the signed S0+S1+S2+S3 sum), the final graded prediction must be reconciled. Relative tape strength vs SPY must not be converted into absolute up direction; XLK can fall while outperforming SPY.

## WRONG IF
This lesson fails if a similar setup — narrative flat/down, pipeline up, negative macro, crowded tech, stale catalysts — is overridden to flat/down but XLK closes solidly up on a genuinely fresh same-session catalyst or a macro reversal. The override should be conditional: it must be based on signed component scores, not automatic narrative preference.

## EVIDENCE
2026-08-11 XLK actual: ETF_PCT -0.12%, SPY_PCT -0.32%, REL_PCT +0.20%. The morning tape read S4=0 with 1d rel +0.17%; actual rel +0.20% confirmed relative outperformance, but absolute XLK closed down. Hormuz/oil/CPI risk-off was knowable at open and negative futures (NQ -0.26%) confirmed weakness. Supermicro earnings after-hours were not a regular-session driver. Pipeline emitted up despite narrative flat, causing direction_hit False; magnitude_hit True.

(learn_cycle promote)
