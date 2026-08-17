---
trigger_pattern: "Scheduled trading day where the premarket prediction file (YYYY-MM-DD_predict.md) is absent or empty at open/grading time — a pipeline failure upstream of reasoning, recurring despite active ops lessons; no baseline exists to grade."
corrected_behavior: "No new lesson — this is a recurrence record only. Per the standing 08-15 instruction, consolidate the three existing ops lessons into one and increment occurrences to 5; deploy the hard pre-open gate before 09:30 ET that verifies predict.md exists, is non-empty, and contains the SCORES_BEGIN block, retries generation once, alerts loudly on the ops channel, and at grading marks ops_fail=True with null axes — never a default market miss."
falsifier: "If the watchdog is deployed and a scheduled trading day still reaches grading with no predict file and no loud alert — or the grader records false direction/magnitude against a missing baseline — the deployment is broken; a new lesson is only justified if a consolidated, enforced gate still fails to prevent recurrence."
current_behavior: "The pre-open generation path still has no file-existence watchdog/alert, so the missing predict.md failure recurred for the 5th time (08-02, 08-08, 08-09, 08-15, 08-16). The grading side correctly applied the active ops lesson (ops_fail=True, direction_hit/magnitude_hit=None), but prevention is still not deployed."
evidence_cited: "2026-08-16: predict file missing; actual SPX -0.17% (down/flat); scoreboard ops_fail=True, direction_hit=None, magnitude_hit=None. Same root cause as 08-02, 08-08, 08-09, and 08-15."
error_category: "D"
scope: "ops"
date: "2026-08-16"
status: "active"
occurrences: "1"
promoted_on: "2026-08-17"
sources: "['2026-08-16_lesson.md']"
schema_ok: "true"
---

## RULE
No new lesson — this is a recurrence record only. Per the standing 08-15 instruction, consolidate the three existing ops lessons into one and increment occurrences to 5; deploy the hard pre-open gate before 09:30 ET that verifies predict.md exists, is non-empty, and contains the SCORES_BEGIN block, retries generation once, alerts loudly on the ops channel, and at grading marks ops_fail=True with null axes — never a default market miss.

## WHEN IT FIRES
Scheduled trading day where the premarket prediction file (YYYY-MM-DD_predict.md) is absent or empty at open/grading time — a pipeline failure upstream of reasoning, recurring despite active ops lessons; no baseline exists to grade.

## WRONG IF
If the watchdog is deployed and a scheduled trading day still reaches grading with no predict file and no loud alert — or the grader records false direction/magnitude against a missing baseline — the deployment is broken; a new lesson is only justified if a consolidated, enforced gate still fails to prevent recurrence.

## EVIDENCE
2026-08-16: predict file missing; actual SPX -0.17% (down/flat); scoreboard ops_fail=True, direction_hit=None, magnitude_hit=None. Same root cause as 08-02, 08-08, 08-09, and 08-15.

(learn_cycle promote)
