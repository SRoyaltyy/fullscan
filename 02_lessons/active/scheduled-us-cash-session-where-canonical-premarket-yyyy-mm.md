---
trigger_pattern: "Scheduled US cash session where canonical premarket YYYY-MM-DD_predict.md is absent, empty, or missing SCORES_BEGIN at open or grade after generate fail-closed (gateway timeout and/or paid LLM 402), while Channel 1 tape still exists."
corrected_behavior: "Ops step — keep the pre-09:30 ET gate (verify non-empty predict.md with SCORES_BEGIN, retry once, alert ops); at grade keep ops_fail=True and hits None; never score B0–B7, direction, or magnitude from Channel 1 tape; packet-scan only if a real SCORES_BEGIN exists; increment existing missing-predict D-ops occurrence; do not emit a parallel A/B/C lesson; fix OpenClaw hang/402 fallback rather than prompt weights."
falsifier: "If a valid SCORES_BEGIN predict.md exists for the cash session and grade still writes ops_fail=True / null hits, this is unpaired-artifact D not missing-file D; if a missing file is counted direction_hit=false, the grade path must be revised not defended"
current_behavior: "Predict job retried twice, wrote no stub, left general_predict missing; grader correctly set ops_fail=True with direction_hit/magnitude_hit None, but snapshot still painted Predicted None as WRONG vs the close and the autopsy invited B0–B7 scoring off Channel 1."
evidence_cited: "2026-10-15 predicted None/None vs actual SPX +0.33% (up/mild), Dow +1.08%, NDX +0.08%; ops_fail True; no 01_daily/general/2026-10-15_predict.md; heal log FAIL-CLOSED after OpenClaw 75s timeout and DeepSeek 402 x2; preopen missing general_predict."
error_category: "D"
scope: "ops"
date: "2026-10-15"
status: "active"
occurrences: "1"
promoted_on: "2026-10-15"
sources: "['2026-10-15_lesson.md']"
schema_ok: "true"
---

## RULE
Ops step — keep the pre-09:30 ET gate (verify non-empty predict.md with SCORES_BEGIN, retry once, alert ops); at grade keep ops_fail=True and hits None; never score B0–B7, direction, or magnitude from Channel 1 tape; packet-scan only if a real SCORES_BEGIN exists; increment existing missing-predict D-ops occurrence; do not emit a parallel A/B/C lesson; fix OpenClaw hang/402 fallback rather than prompt weights.

## WHEN IT FIRES
Scheduled US cash session where canonical premarket YYYY-MM-DD_predict.md is absent, empty, or missing SCORES_BEGIN at open or grade after generate fail-closed (gateway timeout and/or paid LLM 402), while Channel 1 tape still exists.

## WRONG IF
If a valid SCORES_BEGIN predict.md exists for the cash session and grade still writes ops_fail=True / null hits, this is unpaired-artifact D not missing-file D; if a missing file is counted direction_hit=false, the grade path must be revised not defended

## EVIDENCE
2026-10-15 predicted None/None vs actual SPX +0.33% (up/mild), Dow +1.08%, NDX +0.08%; ops_fail True; no 01_daily/general/2026-10-15_predict.md; heal log FAIL-CLOSED after OpenClaw 75s timeout and DeepSeek 402 x2; preopen missing general_predict.

(learn_cycle promote)
