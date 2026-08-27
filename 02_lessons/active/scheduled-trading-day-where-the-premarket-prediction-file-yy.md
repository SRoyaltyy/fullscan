---
trigger_pattern: "Scheduled trading day where the premarket prediction file (YYYY-MM-DD_predict.md) is absent, empty, or unavailable to the grader at grading time — recurring pipeline failure upstream of reasoning; no baseline exists to grade."
corrected_behavior: "Deploy the hard pre-open gate before 09:30 ET: verify YYYY-MM-DD_predict.md exists, is non-empty, and contains the SCORES_BEGIN block at the canonical path the grader reads; retry generation once; alert loudly on the ops channel on failure. At grading, always mark ops_fail=True with direction_hit/magnitude_hit None — never a default market miss, never a hit/miss against a missing baseline. Consolidate the three standing ops lessons into one D-category rule with occurrences=7 (08-02, 08-08, 08-09, 08-15, 08-16, 08-22, 08-25) and add a post-hoc audit check: if a predict file appears in later context but was graded null, flag a timing/path mismatch for the ops log and verify the grader consumed the correct artifact."
falsifier: "If the watchdog is deployed and a scheduled trading day still reaches grading with no baseline and no loud alert, the deployment is broken — fix tooling, not prompt; if a present valid predict.md is ever marked ops_fail or a missing file is graded as a hit/miss, the grader is broken."
current_behavior: "On 2026-08-25 the grader again found no baseline (predicted None/None, ops_fail=True, both hits null) despite a fully populated predict.md existing in later context — indicating the file was generated late, written to a path the grader does not read, or otherwise unavailable at grade time. The grader correctly applied the standing ops lesson (nulls preserved), but the pre-open watchdog that would have caught this before 09:30 ET remains undeployed, and the grader path/timing mismatch is newly evidenced by the file's later existence."
evidence_cited: "2026-08-25 — grader recorded predicted None/None vs actual SPX +0.32% (up/mild), ops_fail=True, direction_hit=None, magnitude_hit=None; a complete predict.md (UP/FLAT, total 1.8, full SCORES_BEGIN/SCORES_END) is present in the injected context, so the failure is baseline availability/timing at grade time, not generation or reasoning."
error_category: "D"
scope: "ops"
date: "2026-08-25"
status: "active"
occurrences: "1"
promoted_on: "2026-08-27"
sources: "['2026-08-25_lesson.md']"
schema_ok: "true"
---

## RULE
Deploy the hard pre-open gate before 09:30 ET: verify YYYY-MM-DD_predict.md exists, is non-empty, and contains the SCORES_BEGIN block at the canonical path the grader reads; retry generation once; alert loudly on the ops channel on failure. At grading, always mark ops_fail=True with direction_hit/magnitude_hit None — never a default market miss, never a hit/miss against a missing baseline. Consolidate the three standing ops lessons into one D-category rule with occurrences=7 (08-02, 08-08, 08-09, 08-15, 08-16, 08-22, 08-25) and add a post-hoc audit check: if a predict file appears in later context but was graded null, flag a timing/path mismatch for the ops log and verify the grader consumed the correct artifact.

## WHEN IT FIRES
Scheduled trading day where the premarket prediction file (YYYY-MM-DD_predict.md) is absent, empty, or unavailable to the grader at grading time — recurring pipeline failure upstream of reasoning; no baseline exists to grade.

## WRONG IF
If the watchdog is deployed and a scheduled trading day still reaches grading with no baseline and no loud alert, the deployment is broken — fix tooling, not prompt; if a present valid predict.md is ever marked ops_fail or a missing file is graded as a hit/miss, the grader is broken.

## EVIDENCE
2026-08-25 — grader recorded predicted None/None vs actual SPX +0.32% (up/mild), ops_fail=True, direction_hit=None, magnitude_hit=None; a complete predict.md (UP/FLAT, total 1.8, full SCORES_BEGIN/SCORES_END) is present in the injected context, so the failure is baseline availability/timing at grade time, not generation or reasoning.

(learn_cycle promote)
