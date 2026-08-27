---
trigger_pattern: "On a scheduled trading day, a sector PREDICT block contains explicit predicted_direction and predicted_magnitude_band, but the scoreboard entry records predicted None/None and marks direction_hit/magnitude_hit False, producing a false miss and corrupting rolling accuracy."
corrected_behavior: "The scoreboard must populate predicted_direction and predicted_magnitude_band from the PREDICT block whenever that block exists and contains explicit values. Only if no PREDICT block or no explicit prediction exists should the row be marked None/unavailable. If the PREDICT block has explicit `up` / `mild` and actual is `up` / `mild`, the row must be scored as `direction_hit: True | magnitude_hit: True`. A validation check should flag any `None/None` row on a trading day that has a PREDICT file as a pipeline/extraction anomaly rather than a market miss."
falsifier: "The lesson would not apply if a future scoreboard row shows `predicted None/None` because there is genuinely no PREDICT block or no explicit direction/magnitude in it; that should be marked ungradeable/ops_fail rather than auto-scored. It would also not apply if a properly populated prediction still misses the actual direction; that is a real forecasting miss, not an extraction failure."
current_behavior: "The grader/scoreboard fails to read the explicit prediction from the contemporaneous sector PREDICT block. It stores `predicted None/None` and then grades a miss against the actual print, even when the actual confirms the intended call. This corrupts the sector rolling accuracy and makes a correct forecast look like a failed forecast."
evidence_cited: "The 2026-08-25 PREDICT block explicitly states `predicted_direction: up`, `predicted_magnitude_band: mild`, and `divergence_flagged: False`. The OUTCOME block records XLC +0.77%, SPY +0.32%, rel +0.45%, `ACTUAL_DIRECTION: up`, `ACTUAL_MAGNITUDE: mild`, and `MORNING_READ_VERDICT: Correct`. The scoreboard entry nevertheless shows `predicted None/None vs actual 0.7656700611540224%` with `direction_hit: False | magnitude_hit: False`. This is also echoed by the candidate lesson `2026-08-25_sector_communication_services_lesson.md` and parallel same-day sector lessons."
error_category: "B"
scope: "general"
date: "2026-08-25"
status: "active"
occurrences: "3"
promoted_on: "2026-08-27"
sources: "['2026-08-25_sector_communication_services_lesson.md', '2026-08-25_sector_consumer_defensive_lesson.md', '2026-08-25_sector_technology_lesson.md']"
schema_ok: "true"
---

## RULE
The scoreboard must populate predicted_direction and predicted_magnitude_band from the PREDICT block whenever that block exists and contains explicit values. Only if no PREDICT block or no explicit prediction exists should the row be marked None/unavailable. If the PREDICT block has explicit `up` / `mild` and actual is `up` / `mild`, the row must be scored as `direction_hit: True | magnitude_hit: True`. A validation check should flag any `None/None` row on a trading day that has a PREDICT file as a pipeline/extraction anomaly rather than a market miss.

## WHEN IT FIRES
On a scheduled trading day, a sector PREDICT block contains explicit predicted_direction and predicted_magnitude_band, but the scoreboard entry records predicted None/None and marks direction_hit/magnitude_hit False, producing a false miss and corrupting rolling accuracy.

## WRONG IF
The lesson would not apply if a future scoreboard row shows `predicted None/None` because there is genuinely no PREDICT block or no explicit direction/magnitude in it; that should be marked ungradeable/ops_fail rather than auto-scored. It would also not apply if a properly populated prediction still misses the actual direction; that is a real forecasting miss, not an extraction failure.

## EVIDENCE
The 2026-08-25 PREDICT block explicitly states `predicted_direction: up`, `predicted_magnitude_band: mild`, and `divergence_flagged: False`. The OUTCOME block records XLC +0.77%, SPY +0.32%, rel +0.45%, `ACTUAL_DIRECTION: up`, `ACTUAL_MAGNITUDE: mild`, and `MORNING_READ_VERDICT: Correct`. The scoreboard entry nevertheless shows `predicted None/None vs actual 0.7656700611540224%` with `direction_hit: False | magnitude_hit: False`. This is also echoed by the candidate lesson `2026-08-25_sector_communication_services_lesson.md` and parallel same-day sector lessons.

(learn_cycle promote)
