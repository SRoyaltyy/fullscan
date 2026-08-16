---
trigger_pattern: "A sector prediction’s stated magnitude band equals the outcome’s stated actual magnitude (e.g., predicted down/mild, actual magnitude = mild), but the scoreboard line records magnitude_hit False. This scoreboard/accounting inconsistency repeats across runs and can cause a false magnitude lesson to be learned from a correct call."
corrected_behavior: "Before writing a magnitude/reasoning lesson, cross-check the scoreboard magnitude flag against the OUTCOME block’s ACTUAL_MAGNITUDE and the predicted magnitude band. If predicted band == actual magnitude, treat the scoreboard False as a scoreboard/accounting flag error, flag the line for correction, and do not create a magnitude-threshold reasoning lesson."
falsifier: "If a published magnitude rubric is audited and shows that -0.211% is outside the “mild” band (e.g., mild requires |pct| ≥ 0.5%), then the scoreboard False is correct and this lesson is invalid. Also, if the OUTCOME ACTUAL_MAGNITUDE field is shown to be derived from the prediction rather than independently classified, the scoreboard should be treated as authoritative instead."
current_behavior: "The scoreboard marks magnitude_hit False for Consumer Cyclical even though the OUTCOME block explicitly says ACTUAL_MAGNITUDE: mild and the predicted band was mild. Taken at face value, this would teach the model a false magnitude miss and understate rolling magnitude accuracy."
evidence_cited: "2026-08-14 XLY predicted down/mild; OUTCOME says ACTUAL_MAGNITUDE: mild; scoreboard says direction_hit True, magnitude_hit False. This is the same pattern already captured in the 2026-08-13 sector:Consumer Cyclical candidate lesson: predicted up/flat, actual +0.475% classified flat, post-session review said both HIT, but the scoreboard recorded magnitude_hit False. The current run is a second confirmation that the False flag is a scoreboard accounting problem, not a sector reasoning miss."
error_category: "D"
scope: "ops"
date: "2026-08-14"
status: "active"
occurrences: "1"
promoted_on: "2026-08-16"
sources: "['2026-08-14_sector_consumer_cyclical_lesson.md']"
schema_ok: "true"
---

## RULE
Before writing a magnitude/reasoning lesson, cross-check the scoreboard magnitude flag against the OUTCOME block’s ACTUAL_MAGNITUDE and the predicted magnitude band. If predicted band == actual magnitude, treat the scoreboard False as a scoreboard/accounting flag error, flag the line for correction, and do not create a magnitude-threshold reasoning lesson.

## WHEN IT FIRES
A sector prediction’s stated magnitude band equals the outcome’s stated actual magnitude (e.g., predicted down/mild, actual magnitude = mild), but the scoreboard line records magnitude_hit False. This scoreboard/accounting inconsistency repeats across runs and can cause a false magnitude lesson to be learned from a correct call.

## WRONG IF
If a published magnitude rubric is audited and shows that -0.211% is outside the “mild” band (e.g., mild requires |pct| ≥ 0.5%), then the scoreboard False is correct and this lesson is invalid. Also, if the OUTCOME ACTUAL_MAGNITUDE field is shown to be derived from the prediction rather than independently classified, the scoreboard should be treated as authoritative instead.

## EVIDENCE
2026-08-14 XLY predicted down/mild; OUTCOME says ACTUAL_MAGNITUDE: mild; scoreboard says direction_hit True, magnitude_hit False. This is the same pattern already captured in the 2026-08-13 sector:Consumer Cyclical candidate lesson: predicted up/flat, actual +0.475% classified flat, post-session review said both HIT, but the scoreboard recorded magnitude_hit False. The current run is a second confirmation that the False flag is a scoreboard accounting problem, not a sector reasoning miss.


