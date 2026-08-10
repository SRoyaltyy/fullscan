---
trigger_pattern: "Scheduled trading day where premarket YYYY-MM-DD_predict.md is absent or empty at open/grading time."
corrected_behavior: "OPS: verify predict.md exists and contains SCORES_BEGIN before 09:30 ET; if missing retry generation and alert. At grading time mark ops_fail=true and leave direction_hit/magnitude_hit null — never score as a market miss."
falsifier: "Wrong if a day with a present non-empty predict.md is still marked ops_fail, or a missing file is still counted direction_hit=false."
current_behavior: "Outcome grades null prediction as flat/miss."
evidence_cited: "2026-08-02 and 2026-08-08 missing predict files graded as direction_hit false."
error_category: "D"
scope: "ops"
date: "2026-08-08"
status: "active"
occurrences: "2"
promoted_on: "2026-08-10"
sources: "['2026-08-02_lesson.md', '2026-08-08_lesson.md']"
schema_ok: "true"
---

## RULE
OPS: verify predict.md exists and contains SCORES_BEGIN before 09:30 ET; if missing retry generation and alert. At grading time mark ops_fail=true and leave direction_hit/magnitude_hit null — never score as a market miss.

## WHEN IT FIRES
Scheduled trading day where premarket YYYY-MM-DD_predict.md is absent or empty at open/grading time.

## WRONG IF
Wrong if a day with a present non-empty predict.md is still marked ops_fail, or a missing file is still counted direction_hit=false.

## EVIDENCE
2026-08-02 and 2026-08-08 missing predict files graded as direction_hit false.
