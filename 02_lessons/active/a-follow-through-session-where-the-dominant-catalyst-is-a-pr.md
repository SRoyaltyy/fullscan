---
trigger_pattern: "A follow-through session where the dominant catalyst is a prior-day macro print (e.g., benign CPI) that already drove the prior session's rally, overnight catalysts are positive but US index futures (B6) are flat within ±0.5% (zero futures confirmation), and a scheduled 8:30 ET data release (PPI/jobless claims) is pending — the pipeline emits NOTABLE from the carried catalyst alone, but the already-traded setup plus flat futures cap the day at MILD."
corrected_behavior: "When the dominant catalyst is a prior-day macro print and B6 is flat (within ±0.5%), treat the catalyst as partially priced: cap the final magnitude band at MILD and use multiplier ≤1.0 unless (a) a fresh same-day catalyst is known premarket (e.g., an 8:30 release direction, a dominant |B1|≥2 event), or (b) US index futures independently confirm a ≥0.5% move. Operationalize: leading_sum > +3.0 with B6=0 is a MAGNITUDE divergence — the flat futures cap the band; do not let a strong leading sum alone emit NOTABLE. On post-macro-print follow-through days with pending 8:30 data, hold the band at MILD pre-release."
falsifier: "If the same trigger recurs (prior-day macro setup, flat B6, non-confirming catalysts) and SPX closes ≥1.0% with no fresh catalyst and no futures confirmation, the MILD cap is wrong; also if it under-calls 2 of the next 3 such days, narrow or discard."
current_behavior: "Scored B1=+1 on yesterday's CPI plus fresh-but-non-confirming overnight catalysts (Supermicro +10%, Kospi +3.56%, oil -2%), kept B6=0 (ES +0.19%), applied multiplier 1.1 → total 8.525 → UP/NOTABLE. Actual SPX +0.65% (up/mild): direction HIT, magnitude MISS. The flat futures correctly signaled a contained follow-through; the carried CPI could not by itself drive a ≥1.0% second-day move."
evidence_cited: "2026-08-13 predicted up/notable (total 8.525, mult 1.1, B1=+1, B6=0); actual SPX +0.65% (up/mild) — direction HIT, magnitude MISS. Day driver was flat PPI (8:30 surprise, un-knowable at prediction time), but the notable band was unsupported by flat futures and leaned on the already-traded 08-12 CPI. Outcome review: every B0–B7 component scored RIGHT — pure band/confidence error."
error_category: "C"
scope: "general"
date: "2026-08-13"
status: "active"
occurrences: "1"
promoted_on: "2026-08-13"
sources: "['2026-08-13_lesson.md']"
schema_ok: "true"
---

## RULE
When the dominant catalyst is a prior-day macro print and B6 is flat (within ±0.5%), treat the catalyst as partially priced: cap the final magnitude band at MILD and use multiplier ≤1.0 unless (a) a fresh same-day catalyst is known premarket (e.g., an 8:30 release direction, a dominant |B1|≥2 event), or (b) US index futures independently confirm a ≥0.5% move. Operationalize: leading_sum > +3.0 with B6=0 is a MAGNITUDE divergence — the flat futures cap the band; do not let a strong leading sum alone emit NOTABLE. On post-macro-print follow-through days with pending 8:30 data, hold the band at MILD pre-release.

## WHEN IT FIRES
A follow-through session where the dominant catalyst is a prior-day macro print (e.g., benign CPI) that already drove the prior session's rally, overnight catalysts are positive but US index futures (B6) are flat within ±0.5% (zero futures confirmation), and a scheduled 8:30 ET data release (PPI/jobless claims) is pending — the pipeline emits NOTABLE from the carried catalyst alone, but the already-traded setup plus flat futures cap the day at MILD.

## WRONG IF
If the same trigger recurs (prior-day macro setup, flat B6, non-confirming catalysts) and SPX closes ≥1.0% with no fresh catalyst and no futures confirmation, the MILD cap is wrong; also if it under-calls 2 of the next 3 such days, narrow or discard.

## EVIDENCE
2026-08-13 predicted up/notable (total 8.525, mult 1.1, B1=+1, B6=0); actual SPX +0.65% (up/mild) — direction HIT, magnitude MISS. Day driver was flat PPI (8:30 surprise, un-knowable at prediction time), but the notable band was unsupported by flat futures and leaned on the already-traded 08-12 CPI. Outcome review: every B0–B7 component scored RIGHT — pure band/confidence error.

(learn_cycle promote)
