---
trigger_pattern: "Scheduled trading day opens with the premarket prediction file (YYYY-MM-DD_predict.md) missing or empty at open/grading time — a pipeline failure upstream of reasoning, not a market condition."
corrected_behavior: "Deploy a pre-open watchdog: verify YYYY-MM-DD_predict.md exists and is non-empty before market open; if missing, retry generation, alert loudly, and mark the run 'no baseline — ungraded' instead of grading a default miss. At grading time, a missing baseline is always D-category pipeline error, never a reasoning miss. Consolidate with candidate 2026-08-08_lesson.md and promote; do not create a duplicate lesson."
falsifier: "If the watchdog is live and a scheduled trading day still reaches grading with no predict file and no loud alert, the deployment is broken — fix the tooling, not the lesson. If the watchdog blocks valid but late-correct baselines, refine to allow flagged late baselines."
current_behavior: "2026-08-09: no prediction file existed at grading time; the grader recorded a default direction/magnitude double miss against actual SPX +0.62% (up/mild), artificially deflating the scoreboard."
evidence_cited: "2026-08-09: no prediction file; actual SPX +0.62% up/mild; direction and magnitude both graded False solely due to missing baseline. Same root cause on 2026-08-08 (None/None default miss) and 2026-08-02 (no scored baseline). Scoreboard integrity corrupted: duplicate None/None entries with identical +0.62% actual on two dates; n=9 accuracy stats polluted by default misses."
error_category: "D"
scope: "ops"
date: "2026-08-09"
status: "active"
occurrences: "1"
promoted_on: "2026-08-13"
sources: "['2026-08-09_lesson.md']"
schema_ok: "true"
---

## RULE
Deploy a pre-open watchdog: verify YYYY-MM-DD_predict.md exists and is non-empty before market open; if missing, retry generation, alert loudly, and mark the run "no baseline — ungraded" instead of grading a default miss. At grading time, a missing baseline is always D-category pipeline error, never a reasoning miss. Consolidate with candidate 2026-08-08_lesson.md and promote; do not create a duplicate lesson.

## WHEN IT FIRES
Scheduled trading day opens with the premarket prediction file (YYYY-MM-DD_predict.md) missing or empty at open/grading time — a pipeline failure upstream of reasoning, not a market condition.

## WRONG IF
If the watchdog is live and a scheduled trading day still reaches grading with no predict file and no loud alert, the deployment is broken — fix the tooling, not the lesson. If the watchdog blocks valid but late-correct baselines, refine to allow flagged late baselines.

## EVIDENCE
2026-08-09: no prediction file; actual SPX +0.62% up/mild; direction and magnitude both graded False solely due to missing baseline. Same root cause on 2026-08-08 (None/None default miss) and 2026-08-02 (no scored baseline). Scoreboard integrity corrupted: duplicate None/None entries with identical +0.62% actual on two dates; n=9 accuracy stats polluted by default misses.

(learn_cycle promote)
