---
trigger_pattern: "When a sector has a scheduled mega-cap catalyst event (e.g., Apple product unveiling) on the same day as a live macro risk-off shock (oil >$100), and the sector's dominant hardware/mega-cap complex has demonstrated multi-day relative outperformance, the correct prediction is flat — the scheduled catalyst offsets the macro drag. The model should explicitly name the scheduled event in the morning analysis even when the macro shock dominates the narrative."
corrected_behavior: "When a scheduled mega-cap catalyst (Apple event, Nvidia GTC, etc.) falls on the prediction date, the model must explicitly name it in the morning analysis and score its offsetting potential against any live macro headwinds — even when the macro shock is the dominant narrative. A scheduled catalyst for the sector's top holding is a knowable factor that should be part of the S1/S4 reasoning, not an after-the-fact explanation."
falsifier: "A future session where a scheduled mega-cap catalyst is explicitly named in the morning analysis, scored as an offset, and the sector still moves against the catalyst (e.g., oil shock dominates despite Apple event) would falsify the implied strength of scheduled catalysts. Also, a session where the model names a scheduled catalyst but the sector moves flat/down without it would not falsify — the lesson only requires naming, not assigning excessive weight."
current_behavior: "The model correctly identified the macro down-pressure (oil shock) and the hardware spine resilience, but did not mention the Apple foldable iPhone event (September 9 was a known scheduled date) in the morning analysis. The flat call was correct, but the reasoning omitted the single largest sector-specific catalyst that made the flat outcome knowable."
evidence_cited: "Morning analysis for 2026-09-09 Technology/XLK made no mention of Apple's September 9 foldable iPhone event — Apple is XLK's largest holding. The outcome review states: 'The morning analysis did not mention Apple's event at all, which is a notable omission given Apple is XLK's largest holding.' The flat/flat prediction hit both direction and magnitude, but the reasoning was incomplete: the flat outcome required the Apple catalyst to fully offset the oil shock, which was a closer call than the confidence (0.55) suggested."
error_category: "NONE"
scope: "general"
date: "2026-09-09"
status: "active"
occurrences: "1"
promoted_on: "2026-09-09"
sources: "['2026-09-09_sector_technology_lesson.md']"
schema_ok: "true"
---

## RULE
When a scheduled mega-cap catalyst (Apple event, Nvidia GTC, etc.) falls on the prediction date, the model must explicitly name it in the morning analysis and score its offsetting potential against any live macro headwinds — even when the macro shock is the dominant narrative. A scheduled catalyst for the sector's top holding is a knowable factor that should be part of the S1/S4 reasoning, not an after-the-fact explanation.

## WHEN IT FIRES
When a sector has a scheduled mega-cap catalyst event (e.g., Apple product unveiling) on the same day as a live macro risk-off shock (oil >$100), and the sector's dominant hardware/mega-cap complex has demonstrated multi-day relative outperformance, the correct prediction is flat — the scheduled catalyst offsets the macro drag. The model should explicitly name the scheduled event in the morning analysis even when the macro shock dominates the narrative.

## WRONG IF
A future session where a scheduled mega-cap catalyst is explicitly named in the morning analysis, scored as an offset, and the sector still moves against the catalyst (e.g., oil shock dominates despite Apple event) would falsify the implied strength of scheduled catalysts. Also, a session where the model names a scheduled catalyst but the sector moves flat/down without it would not falsify — the lesson only requires naming, not assigning excessive weight.

## EVIDENCE
Morning analysis for 2026-09-09 Technology/XLK made no mention of Apple's September 9 foldable iPhone event — Apple is XLK's largest holding. The outcome review states: "The morning analysis did not mention Apple's event at all, which is a notable omission given Apple is XLK's largest holding." The flat/flat prediction hit both direction and magnitude, but the reasoning was incomplete: the flat outcome required the Apple catalyst to fully offset the oil shock, which was a closer call than the confidence (0.55) suggested.

(learn_cycle promote)
