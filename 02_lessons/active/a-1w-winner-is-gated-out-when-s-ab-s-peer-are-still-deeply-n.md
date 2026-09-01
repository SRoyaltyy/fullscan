---
trigger_pattern: "A 1w winner is gated_out when s_ab/s_peer are still deeply negative after a crash even though s_join and s_sector are already constructive, or when a micro name with maxed peer/AB fails a size or join floor."
corrected_behavior: "Add an eligibility override gate: do not hard-exclude when (s_join>0.5 and s_sector>0.5) even if s_ab and s_peer are both <-0.5; and do not size-gate micros when s_peer>=0.99 and s_ab>0.5."
falsifier: "Wrong if admitting CAPR/CYPH-style overrides into the 1w top-10 lowers excess vs the universe median over the next ≥5 fully-realized books."
current_behavior: "Eligibility treats weak AB/peer (and micro size) as hard exclusions, so CAPR never enters despite s_join=0.611 and s_sector=0.6, and CYPH never enters despite s_ab=0.704 and s_peer=1.0."
evidence_cited: "CAPR 2026-08-21 gated_out +52.46% 1w (join 0.611, sector 0.6, AB -0.762, peer -0.732); CYPH gated_out +20.42% (AB 0.704, peer 1.0, micro); CAN gated_out +7.04% (peer 1.0, micro, join -0.792)."
error_category: "A"
scope: "book"
date: "2026-09-01"
status: "active"
occurrences: "1"
promoted_on: "2026-09-01"
sources: "['2026-09-01_book_lesson.md']"
schema_ok: "true"
---

## RULE
Add an eligibility override gate: do not hard-exclude when (s_join>0.5 and s_sector>0.5) even if s_ab and s_peer are both <-0.5; and do not size-gate micros when s_peer>=0.99 and s_ab>0.5.

## WHEN IT FIRES
A 1w winner is gated_out when s_ab/s_peer are still deeply negative after a crash even though s_join and s_sector are already constructive, or when a micro name with maxed peer/AB fails a size or join floor.

## WRONG IF
Wrong if admitting CAPR/CYPH-style overrides into the 1w top-10 lowers excess vs the universe median over the next ≥5 fully-realized books.

## EVIDENCE
CAPR 2026-08-21 gated_out +52.46% 1w (join 0.611, sector 0.6, AB -0.762, peer -0.732); CYPH gated_out +20.42% (AB 0.704, peer 1.0, micro); CAN gated_out +7.04% (peer 1.0, micro, join -0.792).

(learn_cycle promote)
