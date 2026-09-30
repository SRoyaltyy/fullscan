---
trigger_pattern: "A scheduled US cash session where a valid premarket prediction artifact (with SCORES_BEGIN) exists in the run packet, but the grader records ops_fail=True with direction_hit/magnitude_hit null at grade time — i.e., the artifact was generated but not consumed."
corrected_behavior: "Before finalizing any ops_fail=True grade, the grader MUST scan the run packet for a valid SCORES_BEGIN block. If one exists, do NOT mark ops_fail — instead flag a timing/path mismatch to the ops log, attempt to pair the artifact to the session, and grade against it. Only mark ops_fail=True when no valid SCORES_BEGIN block exists anywhere in the packet. This is a grader-boundary check, not a generation check."
falsifier: "If the grader is patched to consume an existing artifact and a scheduled session still grades null while a valid SCORES_BEGIN block is present in the packet, this lesson must be revised — the fix is deeper in the pipeline, not at the consumption check."
current_behavior: "The grader marked 2026-09-30 ops_fail=True and left hits null despite a complete, valid SCORES_BEGIN block (total 2.009, predicted up/mild) being present in the injected packet. The standing ops lesson's own post-hoc audit clause ('if a predict file appears in later context but was graded null, flag a timing/path mismatch') was not executed."
evidence_cited: "2026-09-30 — packet contained a full premarket prediction (total 2.009, predicted up/mild, confidence 0.58, complete SCORES_BEGIN/SCORES_END) yet scoreboard recorded ops_fail=True, direction_hit=None, magnitude_hit=None. Same missing-consumption chain as the 08-02/08-08/08-09/08-15/08-16/08-22/08-25/08-27/08-29 ops-fail cluster, but this is the first instance where the artifact demonstrably existed in-packet."
error_category: "D"
scope: "ops"
date: "2026-09-30"
status: "active"
occurrences: "1"
promoted_on: "2026-09-30"
sources: "['2026-09-30_lesson.md']"
schema_ok: "true"
---

## RULE
Before finalizing any ops_fail=True grade, the grader MUST scan the run packet for a valid SCORES_BEGIN block. If one exists, do NOT mark ops_fail — instead flag a timing/path mismatch to the ops log, attempt to pair the artifact to the session, and grade against it. Only mark ops_fail=True when no valid SCORES_BEGIN block exists anywhere in the packet. This is a grader-boundary check, not a generation check.

## WHEN IT FIRES
A scheduled US cash session where a valid premarket prediction artifact (with SCORES_BEGIN) exists in the run packet, but the grader records ops_fail=True with direction_hit/magnitude_hit null at grade time — i.e., the artifact was generated but not consumed.

## WRONG IF
If the grader is patched to consume an existing artifact and a scheduled session still grades null while a valid SCORES_BEGIN block is present in the packet, this lesson must be revised — the fix is deeper in the pipeline, not at the consumption check.

## EVIDENCE
2026-09-30 — packet contained a full premarket prediction (total 2.009, predicted up/mild, confidence 0.58, complete SCORES_BEGIN/SCORES_END) yet scoreboard recorded ops_fail=True, direction_hit=None, magnitude_hit=None. Same missing-consumption chain as the 08-02/08-08/08-09/08-15/08-16/08-22/08-25/08-27/08-29 ops-fail cluster, but this is the first instance where the artifact demonstrably existed in-packet.

(learn_cycle promote)
