---
trigger_pattern: "Scheduled trading day where the premarket prediction file (YYYY-MM-DD_predict.md) is absent or empty at open/grading time — a pipeline failure upstream of reasoning, recurring despite active ops lessons; no baseline exists to grade."
corrected_behavior: "Deploy a hard pre-open gate before 09:30 ET: verify YYYY-MM-DD_predict.md exists, is non-empty, and contains the SCORES_BEGIN block; if missing, retry generation once, alert loudly on the ops channel, and if still missing at grading mark 'no baseline — ungraded' with ops_fail=True and direction_hit/magnitude_hit=null — never a default market miss. Do not write another lesson for this pattern; consolidate the three existing ops lessons into one and increment occurrences."
falsifier: "If the watchdog is deployed and a scheduled trading day still opens with no predict file and no loud alert — or the grader records false direction/magnitude against a missing baseline — the deployment is broken; if the guard rejects legitimately late baselines, add an override."
current_behavior: "2026-08-15 generated no predict file; the grader correctly emitted ops_fail=True with direction_hit/magnitude_hit=None, but the pre-open generation path still has no file-existence watchdog/alert, so the same missing-file failure recurred for the 4th time (08-02, 08-08, 08-09, 08-15) instead of being prevented."
evidence_cited: "2026-08-15: predict file missing; actual SPX -0.17% (down/flat); scoreboard ops_fail=True, direction_hit=None, magnitude_hit=None. Same root cause previously polluted the scoreboard on 2026-08-02, 2026-08-08, and 2026-08-09."
error_category: "D"
scope: "ops"
date: "2026-08-15"
status: "active"
occurrences: "1"
promoted_on: "2026-08-16"
sources: "['2026-08-15_lesson.md']"
schema_ok: "true"
---

## RULE
Deploy a hard pre-open gate before 09:30 ET: verify YYYY-MM-DD_predict.md exists, is non-empty, and contains the SCORES_BEGIN block; if missing, retry generation once, alert loudly on the ops channel, and if still missing at grading mark 'no baseline — ungraded' with ops_fail=True and direction_hit/magnitude_hit=null — never a default market miss. Do not write another lesson for this pattern; consolidate the three existing ops lessons into one and increment occurrences.

## WHEN IT FIRES
Scheduled trading day where the premarket prediction file (YYYY-MM-DD_predict.md) is absent or empty at open/grading time — a pipeline failure upstream of reasoning, recurring despite active ops lessons; no baseline exists to grade.

## WRONG IF
If the watchdog is deployed and a scheduled trading day still opens with no predict file and no loud alert — or the grader records false direction/magnitude against a missing baseline — the deployment is broken; if the guard rejects legitimately late baselines, add an override.

## EVIDENCE
2026-08-15: predict file missing; actual SPX -0.17% (down/flat); scoreboard ops_fail=True, direction_hit=None, magnitude_hit=None. Same root cause previously polluted the scoreboard on 2026-08-02, 2026-08-08, and 2026-08-09.


