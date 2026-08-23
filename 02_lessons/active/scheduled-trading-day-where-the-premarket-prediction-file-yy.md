---
trigger_pattern: "Scheduled trading day where the premarket prediction file (YYYY-MM-DD_predict.md) is absent or empty at open/grading time — recurring pipeline failure upstream of reasoning; no baseline exists to grade."
corrected_behavior: "No new lesson — recurrence record only. Per the standing 08-15/08-16 instruction, consolidate the existing ops lessons into one D-category rule and increment occurrences to 6. Deploy the hard pre-open gate before 09:30 ET: verify predict.md exists, is non-empty, and contains the SCORES_BEGIN block; retry generation once; alert loudly on the ops channel on failure. At grading, always mark ops_fail=True with direction_hit/magnitude_hit None — never a default market miss. Do not write any reasoning lesson from this day."
falsifier: "If the watchdog is deployed and a scheduled trading day still reaches grading with no predict file and no loud alert — or the grader records false direction/magnitude against a missing baseline — the deployment is broken and must be fixed at the tooling level, not by another lesson."
current_behavior: "2026-08-22 predict.md was missing; the grader correctly applied the active ops lesson (ops_fail=True, direction_hit/magnitude_hit=None), but no pre-open file-existence watchdog is deployed, so the same operational failure recurs for the sixth time despite standing lessons demanding a hard gate."
evidence_cited: "2026-08-22: no predict file; actual SPX +0.43% (up/mild); scoreboard ops_fail=True, direction_hit=None, magnitude_hit=None. Same root cause as 08-02, 08-08, 08-09, 08-15, and 08-16."
error_category: "D"
scope: "ops"
date: "2026-08-22"
status: "active"
occurrences: "1"
promoted_on: "2026-08-23"
sources: "['2026-08-22_lesson.md']"
schema_ok: "true"
---

## RULE
No new lesson — recurrence record only. Per the standing 08-15/08-16 instruction, consolidate the existing ops lessons into one D-category rule and increment occurrences to 6. Deploy the hard pre-open gate before 09:30 ET: verify predict.md exists, is non-empty, and contains the SCORES_BEGIN block; retry generation once; alert loudly on the ops channel on failure. At grading, always mark ops_fail=True with direction_hit/magnitude_hit None — never a default market miss. Do not write any reasoning lesson from this day.

## WHEN IT FIRES
Scheduled trading day where the premarket prediction file (YYYY-MM-DD_predict.md) is absent or empty at open/grading time — recurring pipeline failure upstream of reasoning; no baseline exists to grade.

## WRONG IF
If the watchdog is deployed and a scheduled trading day still reaches grading with no predict file and no loud alert — or the grader records false direction/magnitude against a missing baseline — the deployment is broken and must be fixed at the tooling level, not by another lesson.

## EVIDENCE
2026-08-22: no predict file; actual SPX +0.43% (up/mild); scoreboard ops_fail=True, direction_hit=None, magnitude_hit=None. Same root cause as 08-02, 08-08, 08-09, 08-15, and 08-16.


