---
trigger_pattern: "Scheduled US cash session where canonical premarket predict.md is absent, empty, or missing SCORES_BEGIN at open/grade after generation fail-closed, while Channel 1 tape exists."
corrected_behavior: "Enforce the pre-09:30 ET ops gate: verify canonical predict.md exists, is non-empty, and contains SCORES_BEGIN; retry generation once; alert ops on failure. At grade, scan the run packet for a valid SCORES_BEGIN block before writing ops_fail=True; if none exists, keep ops_fail=True and hits None, increment the existing D-ops occurrence, and do not emit a parallel A/B/C reasoning lesson."
falsifier: "If a valid SCORES_BEGIN prediction artifact exists for a scheduled cash session and the grader still writes ops_fail=True with null hits, this D-ops lesson must be revised/narrowed."
current_behavior: "No premarket prediction artifact was produced/consumed; scoreboard recorded ops_fail=True with direction_hit and magnitude_hit null, so no B0-B7 or market-direction reasoning was gradeable."
evidence_cited: "2026-11-06 predicted None/None vs actual SPX +0.31% (up/mild); ops_fail True; canonical premarket predict.md missing/null; no valid SCORES_BEGIN baseline."
error_category: "D"
scope: "ops"
date: "2026-11-06"
status: "active"
occurrences: "1"
promoted_on: "2026-11-06"
sources: "['2026-11-06_lesson.md']"
schema_ok: "true"
---

## RULE
Enforce the pre-09:30 ET ops gate: verify canonical predict.md exists, is non-empty, and contains SCORES_BEGIN; retry generation once; alert ops on failure. At grade, scan the run packet for a valid SCORES_BEGIN block before writing ops_fail=True; if none exists, keep ops_fail=True and hits None, increment the existing D-ops occurrence, and do not emit a parallel A/B/C reasoning lesson.

## WHEN IT FIRES
Scheduled US cash session where canonical premarket predict.md is absent, empty, or missing SCORES_BEGIN at open/grade after generation fail-closed, while Channel 1 tape exists.

## WRONG IF
If a valid SCORES_BEGIN prediction artifact exists for a scheduled cash session and the grader still writes ops_fail=True with null hits, this D-ops lesson must be revised/narrowed.

## EVIDENCE
2026-11-06 predicted None/None vs actual SPX +0.31% (up/mild); ops_fail True; canonical premarket predict.md missing/null; no valid SCORES_BEGIN baseline.

(learn_cycle promote)
