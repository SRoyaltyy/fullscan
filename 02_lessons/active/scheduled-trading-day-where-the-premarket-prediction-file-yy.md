---
trigger_pattern: "Scheduled trading day where the premarket prediction file (YYYY-MM-DD_predict.md) is absent or empty at grading time — or, better, at market open — so no scored baseline exists and the run is graded as a miss by default."
corrected_behavior: "Add a premarket pipeline guard before 09:30 ET: verify YYYY-MM-DD_predict.md exists and is non-empty with complete B0-B7/scores. If missing, fail loudly, retry generation, and alert. If still missing at grading time, mark the run as 'unscorable' rather than a direction/magnitude miss."
falsifier: "If the file-existence guard is implemented and a scheduled trading day still opens with no prediction file — or scoring still records false/false against None — this lesson must be revised to diagnose the scheduler/fetch root cause rather than only enforcing file presence."
current_behavior: "Pipeline executed scoring against a missing prediction file; no prediction was generated for 2026-08-08, direction/magnitude were recorded as misses against None, and the failure was treated equivalently to a wrong forecast."
evidence_cited: "2026-08-08 — prediction file missing; ES premarket +0.58%, NQ +1.18%; actual SPX +0.62% (up/mild); scoreboard recorded direction_hit False and magnitude_hit False solely because the baseline was None. Candidate 2026-08-02_lesson documents the same missing-file failure."
error_category: "D"
scope: "ops"
date: "2026-08-08"
status: "active"
occurrences: "1"
promoted_on: "2026-08-13"
sources: "['2026-08-08_lesson.md']"
schema_ok: "true"
---

## RULE
Add a premarket pipeline guard before 09:30 ET: verify YYYY-MM-DD_predict.md exists and is non-empty with complete B0-B7/scores. If missing, fail loudly, retry generation, and alert. If still missing at grading time, mark the run as "unscorable" rather than a direction/magnitude miss.

## WHEN IT FIRES
Scheduled trading day where the premarket prediction file (YYYY-MM-DD_predict.md) is absent or empty at grading time — or, better, at market open — so no scored baseline exists and the run is graded as a miss by default.

## WRONG IF
If the file-existence guard is implemented and a scheduled trading day still opens with no prediction file — or scoring still records false/false against None — this lesson must be revised to diagnose the scheduler/fetch root cause rather than only enforcing file presence.

## EVIDENCE
2026-08-08 — prediction file missing; ES premarket +0.58%, NQ +1.18%; actual SPX +0.62% (up/mild); scoreboard recorded direction_hit False and magnitude_hit False solely because the baseline was None. Candidate 2026-08-02_lesson documents the same missing-file failure.

(learn_cycle promote)
