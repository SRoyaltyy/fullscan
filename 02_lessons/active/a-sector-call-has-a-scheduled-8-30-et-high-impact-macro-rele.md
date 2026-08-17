---
trigger_pattern: "A sector call has a scheduled 8:30 ET high-impact macro release (including retail sales, PPI, CPI, jobless claims) but the narrative claims “no scheduled high-impact macro print today.” At the same time, S4 is flat/zero, divergence_flagged is True, and the narrative caps magnitude at MILD while the deterministic pipeline prints a different total_score/band from the same components, flipping the official band to NOTABLE. The scoreboard grades the pipeline output, producing avoidable magnitude misses and masking the correct mild/flat read."
corrected_behavior: "Before finalizing, reconcile the deterministic total with the narrative component arithmetic. If they disagree, treat the narrative component-derived score as authoritative or block the prediction until the mismatch is fixed. Always check the 8:30 ET data calendar; if a high-impact release is scheduled, encode it in S0 and direction risk. When divergence_flagged is True and S4 is flat, do not allow the pipeline to flip the band to NOTABLE. If a consumer-data release is pending and consumer-credit stress is already flagged, allow a flat/down direction outcome rather than unconditional up."
falsifier: "The lesson fails if, after reconciling pipeline/narrative and correctly ingesting the macro calendar, a flat-S4 diverged Financial call still misses a >1% XLF move caused by a fresh same-day Financial catalyst. The scoreboard portion is retired once no future pipeline/narrative mismatches occur for 20 consecutive sector calls."
current_behavior: "The model emits two conflicting outputs for the same call: narrative SECTOR_SCORES say up/mild from component arithmetic (4.0 × 0.9 = 3.6), while the pipeline output says up/notable with total 8.775. The scoreboard uses the pipeline. Separately, the macro-calendar layer misses scheduled 8:30 ET releases, allowing S0 to stay positive into a weak-data surprise. On 2026-08-14, retail sales came in at −0.6% vs +0.1% expected, pushing XLF to −0.17%, while the official graded call was up/notable."
evidence_cited: "2026-08-14 Financial: XLF −0.17%, SPY −0.20%, REL +0.03%. Actual driver was July retail sales −0.6% vs +0.1% expected, released at 8:30 ET. Narrative components: 1.0 + 2.0 + 0.5 + 0.5 + 0.0 = 4.0 × 0.9 = 3.6 → up/mild. Pipeline: total 8.775 → up/notable. Scoreboard: predicted up/notable, magnitude_hit False. The narrative’s up/mild would have been a magnitude HIT against actual flat magnitude. Rolling mag accuracy remains 0.0 (n=5), driven partly by this pipeline/scoreboard inconsistency."
error_category: "B"
scope: "general"
date: "2026-08-14"
status: "active"
occurrences: "1"
promoted_on: "2026-08-17"
sources: "['2026-08-14_sector_financial_lesson.md']"
schema_ok: "true"
---

## RULE
Before finalizing, reconcile the deterministic total with the narrative component arithmetic. If they disagree, treat the narrative component-derived score as authoritative or block the prediction until the mismatch is fixed. Always check the 8:30 ET data calendar; if a high-impact release is scheduled, encode it in S0 and direction risk. When divergence_flagged is True and S4 is flat, do not allow the pipeline to flip the band to NOTABLE. If a consumer-data release is pending and consumer-credit stress is already flagged, allow a flat/down direction outcome rather than unconditional up.

## WHEN IT FIRES
A sector call has a scheduled 8:30 ET high-impact macro release (including retail sales, PPI, CPI, jobless claims) but the narrative claims “no scheduled high-impact macro print today.” At the same time, S4 is flat/zero, divergence_flagged is True, and the narrative caps magnitude at MILD while the deterministic pipeline prints a different total_score/band from the same components, flipping the official band to NOTABLE. The scoreboard grades the pipeline output, producing avoidable magnitude misses and masking the correct mild/flat read.

## WRONG IF
The lesson fails if, after reconciling pipeline/narrative and correctly ingesting the macro calendar, a flat-S4 diverged Financial call still misses a >1% XLF move caused by a fresh same-day Financial catalyst. The scoreboard portion is retired once no future pipeline/narrative mismatches occur for 20 consecutive sector calls.

## EVIDENCE
2026-08-14 Financial: XLF −0.17%, SPY −0.20%, REL +0.03%. Actual driver was July retail sales −0.6% vs +0.1% expected, released at 8:30 ET. Narrative components: 1.0 + 2.0 + 0.5 + 0.5 + 0.0 = 4.0 × 0.9 = 3.6 → up/mild. Pipeline: total 8.775 → up/notable. Scoreboard: predicted up/notable, magnitude_hit False. The narrative’s up/mild would have been a magnitude HIT against actual flat magnitude. Rolling mag accuracy remains 0.0 (n=5), driven partly by this pipeline/scoreboard inconsistency.

(learn_cycle promote)
