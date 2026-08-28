---
trigger_pattern: "Scheduled US cash session where premarket YYYY-MM-DD_predict.md is absent, empty, or missing SCORES_BEGIN at open/grading — pipeline failure upstream of reasoning, not a market condition."
corrected_behavior: "OPS gate before 09:30 ET: verify predict.md exists, is non-empty, and contains SCORES_BEGIN; retry generation once; alert ops on failure; do not score B0–B7, direction, or magnitude from a null baseline. At grade time keep ops_fail=True and hits None. Consolidate duplicate D-ops actives; increment occurrences; write no reasoning lesson."
falsifier: "If the 09:30 ET watchdog is live and a scheduled session still grades a silent missing predict.md, or a valid predict.md is marked ops_fail / direction_hit=false, fix tooling rather than defend or add this lesson"
current_behavior: "2026-08-27 predict file never landed; grader correctly set ops_fail=True with direction_hit/magnitude_hit None, but no pre-open existence/SCORES_BEGIN watchdog ran, so the null baseline recurred. Snapshot copy still painted ❌ WRONG on a null call."
evidence_cited: "2026-08-27 predicted None/None vs actual SPX +0.72% (up/mild), NDX +1.57%; ops_fail True; same missing-file pattern as 08-02/08-08/08-09/08-15/08-16/08-22/08-25."
error_category: "D"
scope: "ops"
date: "2026-08-27"
status: "active"
occurrences: "1"
promoted_on: "2026-08-28"
sources: "['2026-08-27_lesson.md']"
schema_ok: "true"
---

## RULE
OPS gate before 09:30 ET: verify predict.md exists, is non-empty, and contains SCORES_BEGIN; retry generation once; alert ops on failure; do not score B0–B7, direction, or magnitude from a null baseline. At grade time keep ops_fail=True and hits None. Consolidate duplicate D-ops actives; increment occurrences; write no reasoning lesson.

## WHEN IT FIRES
Scheduled US cash session where premarket YYYY-MM-DD_predict.md is absent, empty, or missing SCORES_BEGIN at open/grading — pipeline failure upstream of reasoning, not a market condition.

## WRONG IF
If the 09:30 ET watchdog is live and a scheduled session still grades a silent missing predict.md, or a valid predict.md is marked ops_fail / direction_hit=false, fix tooling rather than defend or add this lesson

## EVIDENCE
2026-08-27 predicted None/None vs actual SPX +0.72% (up/mild), NDX +1.57%; ops_fail True; same missing-file pattern as 08-02/08-08/08-09/08-15/08-16/08-22/08-25.

(learn_cycle promote)
