---
trigger_pattern: "Scheduled US cash equities session (including bond-holiday/equity-open days) where premarket YYYY-MM-DD_predict.md is absent, empty, or missing SCORES_BEGIN at open or grade time."
corrected_behavior: "OPS gate before 09:30 ET: verify canonical predict.md exists, is non-empty, and contains SCORES_BEGIN; retry generation once; alert ops on failure. At grade keep ops_fail=True and direction_hit/magnitude_hit None — never score B0–B7, direction, or magnitude from a null baseline or from Channel 1 tape alone. Increment existing D-ops occurrence; do not add a parallel A/B/C lesson."
falsifier: "If the 09:30 ET watchdog is live and an equity-open session still grades silent missing predict.md with no alert, or a valid SCORES_BEGIN file is marked ops_fail / direction_hit=false, fix tooling rather than defend or duplicate this lesson."
current_behavior: "Predict artifact never generated; grader correctly set ops_fail=True with null hits, but the standing pre-open exist/SCORES_BEGIN watchdog (retry once, alert ops, do not score B0–B7) did not fire or did not recover the file."
evidence_cited: "2026-10-12 predicted None/None vs actual SPX −0.06% (flat/flat), Dow +0.38%, NDX −0.44%; ops_fail True; morning file missing; Channel 1 ES −0.07%/NQ −0.67%/XLE +1.01%/XLK −0.27% not a substitute baseline."
error_category: "D"
scope: "ops"
date: "2026-10-12"
status: "active"
occurrences: "1"
promoted_on: "2026-10-12"
sources: "['2026-10-12_lesson.md']"
schema_ok: "true"
---

## RULE
OPS gate before 09:30 ET: verify canonical predict.md exists, is non-empty, and contains SCORES_BEGIN; retry generation once; alert ops on failure. At grade keep ops_fail=True and direction_hit/magnitude_hit None — never score B0–B7, direction, or magnitude from a null baseline or from Channel 1 tape alone. Increment existing D-ops occurrence; do not add a parallel A/B/C lesson.

## WHEN IT FIRES
Scheduled US cash equities session (including bond-holiday/equity-open days) where premarket YYYY-MM-DD_predict.md is absent, empty, or missing SCORES_BEGIN at open or grade time.

## WRONG IF
If the 09:30 ET watchdog is live and an equity-open session still grades silent missing predict.md with no alert, or a valid SCORES_BEGIN file is marked ops_fail / direction_hit=false, fix tooling rather than defend or duplicate this lesson.

## EVIDENCE
2026-10-12 predicted None/None vs actual SPX −0.06% (flat/flat), Dow +0.38%, NDX −0.44%; ops_fail True; morning file missing; Channel 1 ES −0.07%/NQ −0.67%/XLE +1.01%/XLK −0.27% not a substitute baseline.

(learn_cycle promote)
