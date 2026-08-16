---
trigger_pattern: "A sector call’s narrative and SECTOR_SCORES block cap magnitude at mild (component sum × multiplier = 4.0), but the deterministic pipeline prints a different `leading_sum`/`total_score` from the identical components and emits the official band as NOTABLE. The scoreboard grades the pipeline output rather than the narrative, converting a correct mild call into a magnitude miss."
corrected_behavior: "Before accepting the pipeline output, validate `leading_sum` and `total_score` against the SECTOR_SCORES components. If Σ(S0..S4) × multiplier equals 4.0 and the pipeline emits NOTABLE, treat the pipeline total as erroneous and emit up/mild until the pipeline logic is fixed. Additionally, on a follow-through session with flat futures and no fresh same-day scheduled catalyst, cap magnitude at mild unless a new catalyst or futures confirmation ≥0.5% is present."
falsifier: "This lesson would be falsified if a future run with identical shape — component sum 4, multiplier 1.0, no fresh catalyst, flat futures — emits pipeline total 7 and the actual XLP move is genuinely notable (>1%), while the narrative’s mild call is the wrong one. That would suggest the nonlinear band mapping is intentional. Current evidence — actual mild and narrative consistent — strongly supports the tool-failure diagnosis."
current_behavior: "The official prediction is taken from the pipeline block even when it contradicts the explicitly computed component sum. Here, S0=1, S1=1, S2=0, S3=1, S4=1, multiplier=1.0 → 4.0 → up/mild in the narrative, but the pipeline printed `leading_sum: 5.0` and `total_score: 7.0` → up/notable. Scoreboard then records magnitude_hit=False against an actual up/mild day."
evidence_cited: "2026-08-14 XLP: components S0=1, S1=1, S2=0, S3=1, S4=1 with multiplier 1.0 ⇒ total 4.0. Narrative explicitly concluded “total +4.0 → up/mild.” Pipeline block listed identical components but produced `leading_sum=5.0` / `total_score=7.0` → up/notable. Actual XLP was +0.10%, SPY -0.20%, rel +0.30% → actual direction up/mild. If the narrative’s 4.0 had been emitted, magnitude would have been HIT. This is the same pattern flagged in the 2026-08-13 financial sector lesson candidate."
error_category: "B"
scope: "general"
date: "2026-08-14"
status: "active"
occurrences: "2"
promoted_on: "2026-08-16"
sources: "['2026-08-14_sector_consumer_defensive_lesson.md', '2026-08-14_sector_real_estate_lesson.md']"
schema_ok: "true"
---

## RULE
Before accepting the pipeline output, validate `leading_sum` and `total_score` against the SECTOR_SCORES components. If Σ(S0..S4) × multiplier equals 4.0 and the pipeline emits NOTABLE, treat the pipeline total as erroneous and emit up/mild until the pipeline logic is fixed. Additionally, on a follow-through session with flat futures and no fresh same-day scheduled catalyst, cap magnitude at mild unless a new catalyst or futures confirmation ≥0.5% is present.

## WHEN IT FIRES
A sector call’s narrative and SECTOR_SCORES block cap magnitude at mild (component sum × multiplier = 4.0), but the deterministic pipeline prints a different `leading_sum`/`total_score` from the identical components and emits the official band as NOTABLE. The scoreboard grades the pipeline output rather than the narrative, converting a correct mild call into a magnitude miss.

## WRONG IF
This lesson would be falsified if a future run with identical shape — component sum 4, multiplier 1.0, no fresh catalyst, flat futures — emits pipeline total 7 and the actual XLP move is genuinely notable (>1%), while the narrative’s mild call is the wrong one. That would suggest the nonlinear band mapping is intentional. Current evidence — actual mild and narrative consistent — strongly supports the tool-failure diagnosis.

## EVIDENCE
2026-08-14 XLP: components S0=1, S1=1, S2=0, S3=1, S4=1 with multiplier 1.0 ⇒ total 4.0. Narrative explicitly concluded “total +4.0 → up/mild.” Pipeline block listed identical components but produced `leading_sum=5.0` / `total_score=7.0` → up/notable. Actual XLP was +0.10%, SPY -0.20%, rel +0.30% → actual direction up/mild. If the narrative’s 4.0 had been emitted, magnitude would have been HIT. This is the same pattern flagged in the 2026-08-13 financial sector lesson candidate.


