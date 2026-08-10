---
trigger_pattern: "Scheduled trading day where premarket YYYY-MM-DD_predict.md is absent or empty at open/grading time."
corrected_behavior: "OPS: verify predict.md exists with SCORES_BEGIN before 09:30 ET; retry if missing. At grading mark ops_fail=true and leave direction_hit/magnitude_hit null — never score as a market miss."
falsifier: "Wrong if a present predict.md is marked ops_fail, or a missing file is still counted direction_hit=false."
current_behavior: "Outcome grades null prediction as miss."
evidence_cited: "2026-08-02 and 2026-08-08 missing predict files."
error_category: "D"
scope: "ops"
date: "2026-08-08"
status: "active"
schema_ok: "true"
---

## RULE
OPS: verify predict.md exists with SCORES_BEGIN before 09:30 ET; retry if missing. At grading mark ops_fail=true and leave direction_hit/magnitude_hit null — never score as a market miss.

## WHEN IT FIRES
Scheduled trading day where premarket predict.md is absent or empty.

## WRONG IF
Wrong if a present predict.md is marked ops_fail, or a missing file is still counted direction_hit=false.

## EVIDENCE
2026-08-02 and 2026-08-08 missing predict files.
