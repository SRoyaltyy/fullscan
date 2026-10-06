---
trigger_pattern: "A scheduled US cash session where the run packet contains a valid premarket prediction artifact (well-formed SCORES_BEGIN/SCORES_END block, non-null predicted_direction and predicted_magnitude_band), but the grader records ops_fail=True with direction_hit/magnitude_hit null at grade time — the artifact was generated but not consumed."
corrected_behavior: "Before finalizing any ops_fail=True grade, the grader MUST execute a packet-scan step: search the run packet for a valid SCORES_BEGIN...SCORES_END block with non-null predicted_direction and predicted_magnitude_band. If found, do NOT mark ops_fail — instead pair the artifact to the session, grade direction_hit/magnitude_hit against it, and log a timing/path mismatch to the ops channel. Only mark ops_fail=True when no valid SCORES_BEGIN block exists anywhere in the packet. This is the enforcement of existing active lesson `a-scheduled-us-cash-session-where-a-valid-premarket-predicti` (promoted 2026-09-30); increment that lesson's occurrence count rather than creating a duplicate."
falsifier: "If the grader is patched to scan for SCORES_BEGIN before writing ops_fail, and a scheduled session with a valid SCORES_BEGIN block still grades null/ops_fail=True, this lesson must be revised — the fix is deeper in the pipeline (artifact path or timing mismatch), not at the consumption check."
current_behavior: "The grader wrote ops_fail=True and null hits for 2026-10-06 despite a complete prediction (total 2.623, predicted up/mild, conf 0.605, full B0–B7 and SCORES_BEGIN/SCORES_END) being present in the packet; the outcome header itself says 'OPS_FAIL: morning predict missing/null — not a market miss,' contradicting the packet contents."
evidence_cited: "2026-10-06 — packet contained a full premarket prediction (total 2.623, predicted up/mild, confidence 0.605, complete SCORES_BEGIN/SCORES_END) yet scoreboard recorded ops_fail=True, direction_hit=None, magnitude_hit=None; actual SPX +0.58% (up/mild) would have graded direction HIT, magnitude HIT. Identical failure on 2026-09-30 (total 2.009, up/mild, conf 0.58, ops_fail=True, hits None)."
error_category: "D"
scope: "ops"
date: "2026-10-06"
status: "active"
occurrences: "1"
promoted_on: "2026-10-06"
sources: "['2026-10-06_lesson.md']"
schema_ok: "true"
---

## RULE
Before finalizing any ops_fail=True grade, the grader MUST execute a packet-scan step: search the run packet for a valid SCORES_BEGIN...SCORES_END block with non-null predicted_direction and predicted_magnitude_band. If found, do NOT mark ops_fail — instead pair the artifact to the session, grade direction_hit/magnitude_hit against it, and log a timing/path mismatch to the ops channel. Only mark ops_fail=True when no valid SCORES_BEGIN block exists anywhere in the packet. This is the enforcement of existing active lesson `a-scheduled-us-cash-session-where-a-valid-premarket-predicti` (promoted 2026-09-30); increment that lesson's occurrence count rather than creating a duplicate.

## WHEN IT FIRES
A scheduled US cash session where the run packet contains a valid premarket prediction artifact (well-formed SCORES_BEGIN/SCORES_END block, non-null predicted_direction and predicted_magnitude_band), but the grader records ops_fail=True with direction_hit/magnitude_hit null at grade time — the artifact was generated but not consumed.

## WRONG IF
If the grader is patched to scan for SCORES_BEGIN before writing ops_fail, and a scheduled session with a valid SCORES_BEGIN block still grades null/ops_fail=True, this lesson must be revised — the fix is deeper in the pipeline (artifact path or timing mismatch), not at the consumption check.

## EVIDENCE
2026-10-06 — packet contained a full premarket prediction (total 2.623, predicted up/mild, confidence 0.605, complete SCORES_BEGIN/SCORES_END) yet scoreboard recorded ops_fail=True, direction_hit=None, magnitude_hit=None; actual SPX +0.58% (up/mild) would have graded direction HIT, magnitude HIT. Identical failure on 2026-09-30 (total 2.009, up/mild, conf 0.58, ops_fail=True, hits None).

(learn_cycle promote)
