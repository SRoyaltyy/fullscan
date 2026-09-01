---
trigger_pattern: "Envelope date is a US cash-closed day (weekend/holiday) and/or premarket YYYY-MM-DD_predict.md is absent/empty/missing SCORES_BEGIN at open or grade time, while Channel 1 still injects a prior cash session’s OHLC."
corrected_behavior: "OPS calendar+file gate before grade: if the envelope date is not a US cash session OR predict.md lacks SCORES_BEGIN, keep ops_fail=true, hits None, skip B0–B7, do not pair a weekend job to a prior Thursday/Friday tape as a market miss; on the next cash open verify/retry predict.md before 09:30 ET and alert ops; snapshot must not paint WRONG when ops_fail; increment existing D-ops occurrences, write no reasoning lesson."
falsifier: "If a present valid predict.md with SCORES_BEGIN on a real cash session is marked ops_fail, or a weekend file written as a same-session Friday forecast is auto-ungraded and hides a real reasoning miss, this gate must be narrowed"
current_behavior: "Grader correctly sets ops_fail=true and leaves direction_hit/magnitude_hit null, but still runs a full prior-session autopsy (here Thu 08-27 NVDA digestion), and snapshot copy paints ❌ WRONG on a null call; the 09:30 existence/SCORES_BEGIN watchdog remains undeployed."
evidence_cited: "2026-08-29 (Saturday) predicted None/None vs injected Thu 08-27 SPX +0.72% (up/mild), NDX +1.57%; ops_fail True; same missing-file chain as 08-02/08-08/08-09/08-15/08-16/08-22/08-25/08-27 plus 08-23 weekend pairing."
error_category: "D"
scope: "ops"
date: "2026-08-29"
status: "active"
occurrences: "2"
promoted_on: "2026-08-31"
sources: "['2026-08-29_lesson.md', '2026-08-30_lesson.md']"
schema_ok: "true"
---

## RULE
OPS calendar+file gate before grade: if the envelope date is not a US cash session OR predict.md lacks SCORES_BEGIN, keep ops_fail=true, hits None, skip B0–B7, do not pair a weekend job to a prior Thursday/Friday tape as a market miss; on the next cash open verify/retry predict.md before 09:30 ET and alert ops; snapshot must not paint WRONG when ops_fail; increment existing D-ops occurrences, write no reasoning lesson.

## WHEN IT FIRES
Envelope date is a US cash-closed day (weekend/holiday) and/or premarket YYYY-MM-DD_predict.md is absent/empty/missing SCORES_BEGIN at open or grade time, while Channel 1 still injects a prior cash session’s OHLC.

## WRONG IF
If a present valid predict.md with SCORES_BEGIN on a real cash session is marked ops_fail, or a weekend file written as a same-session Friday forecast is auto-ungraded and hides a real reasoning miss, this gate must be narrowed

## EVIDENCE
2026-08-29 (Saturday) predicted None/None vs injected Thu 08-27 SPX +0.72% (up/mild), NDX +1.57%; ops_fail True; same missing-file chain as 08-02/08-08/08-09/08-15/08-16/08-22/08-25/08-27 plus 08-23 weekend pairing.

(learn_cycle promote)
