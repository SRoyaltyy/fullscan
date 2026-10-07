---
trigger_pattern: "A scheduled US cash session where the run packet already contains a valid premarket prediction artifact (SCORES_BEGIN/SCORES_END, non-null predicted_direction and predicted_magnitude_band), but the grader still writes ops_fail=True with direction_hit/magnitude_hit null — artifact generated, not consumed."
corrected_behavior: "Ops/grade step: before any ops_fail=True, scan the run packet for SCORES_BEGIN…SCORES_END with non-null direction and magnitude; if found, pair and grade hits against that artifact, log a timing/path mismatch, do not paint WRONG. Only mark ops_fail when no valid block exists. Increment existing D-ops occurrence (do not add a parallel reasoning lesson)."
falsifier: "If packet-scan is deployed and a cash session with a valid SCORES_BEGIN block still grades ops_fail=True with null hits, the consumption check is the wrong layer and the pairing path must be revised, not copied again."
current_behavior: "Grader treated 2026-10-07 as morning predict missing/null, painted predicted None/None and snapshot WRONG, and skipped pairing despite a full down/mild SCORES block (total −5.211) in the same packet."
evidence_cited: "2026-10-07 packet predicted down/mild (total −5.211, B6=0, B1=−1, B2=−0.5) vs scoreboard ops_fail True / None/None vs SPX −0.22% (down/flat); same unpaired-artifact pattern as 2026-10-06 and 2026-09-30."
error_category: "D"
scope: "ops"
date: "2026-10-07"
status: "active"
occurrences: "1"
promoted_on: "2026-10-07"
sources: "['2026-10-07_lesson.md']"
schema_ok: "true"
---

## RULE
Ops/grade step: before any ops_fail=True, scan the run packet for SCORES_BEGIN…SCORES_END with non-null direction and magnitude; if found, pair and grade hits against that artifact, log a timing/path mismatch, do not paint WRONG. Only mark ops_fail when no valid block exists. Increment existing D-ops occurrence (do not add a parallel reasoning lesson).

## WHEN IT FIRES
A scheduled US cash session where the run packet already contains a valid premarket prediction artifact (SCORES_BEGIN/SCORES_END, non-null predicted_direction and predicted_magnitude_band), but the grader still writes ops_fail=True with direction_hit/magnitude_hit null — artifact generated, not consumed.

## WRONG IF
If packet-scan is deployed and a cash session with a valid SCORES_BEGIN block still grades ops_fail=True with null hits, the consumption check is the wrong layer and the pairing path must be revised, not copied again.

## EVIDENCE
2026-10-07 packet predicted down/mild (total −5.211, B6=0, B1=−1, B2=−0.5) vs scoreboard ops_fail True / None/None vs SPX −0.22% (down/flat); same unpaired-artifact pattern as 2026-10-06 and 2026-09-30.

(learn_cycle promote)
