---
trigger_pattern: "When the events addon already has earnings inside the 1w horizon, micros with constructive AB/peer are still join-floor gated_out and then gap on the print."
corrected_behavior: "Add an events-addon exception to the join eligibility gate — if days-to-earnings is 0–7 and s_ab>0, keep the name eligible instead of gated_out (no family-weight change)."
falsifier: "Names admitted only by that events exception underperform the 1w top-10, or CHPT-like prints still lose after they are allowed through."
current_behavior: "Join (and related) eligibility floors drop the name before six-family scoring; events addon is health-ok (49 events) but is not a ranker overlay, so CHPT-style prints never compete."
evidence_cited: "CHPT 2026-08-27 gated_out (join -0.315, AB 0.635, news 0.0, sector -0.8) then +55.75% into the Sep 2 print; CHGG/BRR same date also join-floor with AB>0 (CHGG +22.35%, BRR +21.76%)."
error_category: "A"
scope: "book"
date: "2026-09-03"
status: "active"
occurrences: "1"
promoted_on: "2026-09-04"
sources: "['2026-09-03_book_lesson.md']"
schema_ok: "true"
---

## RULE
Add an events-addon exception to the join eligibility gate — if days-to-earnings is 0–7 and s_ab>0, keep the name eligible instead of gated_out (no family-weight change).

## WHEN IT FIRES
When the events addon already has earnings inside the 1w horizon, micros with constructive AB/peer are still join-floor gated_out and then gap on the print.

## WRONG IF
Names admitted only by that events exception underperform the 1w top-10, or CHPT-like prints still lose after they are allowed through.

## EVIDENCE
CHPT 2026-08-27 gated_out (join -0.315, AB 0.635, news 0.0, sector -0.8) then +55.75% into the Sep 2 print; CHGG/BRR same date also join-floor with AB>0 (CHGG +22.35%, BRR +21.76%).

(learn_cycle promote)
