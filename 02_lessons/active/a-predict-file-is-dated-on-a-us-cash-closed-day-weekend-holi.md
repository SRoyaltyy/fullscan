---
trigger_pattern: "A predict file is dated on a US cash-closed day (weekend/holiday) and the note already treats the prior Friday close as fact while forecasting the next cash session, but the grader injects that already-printed Friday OHLC and records a direction/magnitude miss with ops_fail false."
corrected_behavior: "OPS gate before scoring: if predict.md’s calendar date is not a US cash session, or the note already cites the injected SPX close as prior-session fact and forecasts the next cash day, set ops_fail=true and leave direction_hit/magnitude_hit null; pair the file to the next cash session or leave ungraded. Do not rewrite B0–B7 or flip direction off the prior Friday print."
falsifier: "If a weekend-dated file is written as a same-session Friday forecast (does not treat Friday as already closed) and that Friday call is wrong on reasoning, auto-ungrading would hide a real miss and this pairing rule must be narrowed."
current_behavior: "Sunday 2026-08-23 predict (Monday follow-through, flat/flat, B6=0, leftover B1 −0.5) was graded against Friday 08-21 SPX +0.43% — a close the note itself already stated — and marked both axes false."
evidence_cited: "2026-08-23 predicted flat/flat vs injected Friday SPX +0.43% (up/mild); morning text: “Friday already closed SPX +0.43%”; 08-23 was Sunday; primary Friday driver (flash PMI 56.8/56.0) was mid-morning 08-21, not a Sunday/Monday premarket input."
error_category: "D"
scope: "ops"
date: "2026-08-23"
status: "active"
occurrences: "1"
promoted_on: "2026-08-27"
sources: "['2026-08-23_lesson.md']"
schema_ok: "true"
---

## RULE
OPS gate before scoring: if predict.md’s calendar date is not a US cash session, or the note already cites the injected SPX close as prior-session fact and forecasts the next cash day, set ops_fail=true and leave direction_hit/magnitude_hit null; pair the file to the next cash session or leave ungraded. Do not rewrite B0–B7 or flip direction off the prior Friday print.

## WHEN IT FIRES
A predict file is dated on a US cash-closed day (weekend/holiday) and the note already treats the prior Friday close as fact while forecasting the next cash session, but the grader injects that already-printed Friday OHLC and records a direction/magnitude miss with ops_fail false.

## WRONG IF
If a weekend-dated file is written as a same-session Friday forecast (does not treat Friday as already closed) and that Friday call is wrong on reasoning, auto-ungrading would hide a real miss and this pairing rule must be narrowed.

## EVIDENCE
2026-08-23 predicted flat/flat vs injected Friday SPX +0.43% (up/mild); morning text: “Friday already closed SPX +0.43%”; 08-23 was Sunday; primary Friday driver (flash PMI 56.8/56.0) was mid-morning 08-21, not a Sunday/Monday premarket input.

(learn_cycle promote)
