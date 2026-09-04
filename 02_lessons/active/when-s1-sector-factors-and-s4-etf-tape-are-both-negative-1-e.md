---
trigger_pattern: "When S1 (sector factors) and S4 (ETF tape) are both negative (−1 each) and the 1d relative tape is decisively red (rel ≤ −1.5%), the total score should map to a mild DOWN direction, not 'flat' — the flat band should be reserved for cases where negative components are offset by positive ones or where the negative evidence is sub-threshold (single −1 with no tape confirmation)."
corrected_behavior: "When S1 and S4 are both negative AND the 1d relative tape is decisively red (rel ≤ −1.5%), the predicted direction must be 'down' with mild magnitude — the DO-INSTEAD flat/mild preference should cap magnitude (notable → mild) but must not override direction when two independent negative channels (sector factor + tape confirmation) align. The flat band requires either (a) mixed signs across components, (b) a single negative component without tape confirmation, or (c) sub-threshold negative evidence (rel between −0.5% and −1.5%)."
falsifier: "A future session where S1=−1 (live oil-offered) and S4=−1 (1d rel ≤ −1.5%) but XLE closes flat or positive (abs ≥ +0.3%) would falsify this lesson. Also falsified if mapping to 'down' in this configuration produces a worse direction hit-rate than 'flat' over the next 10 Energy predictions."
current_behavior: "The model scored S1=−1 (oil offered) and S4=−1 (1d rel −1.78% decisive fade), applied DO-INSTEAD to cap conviction at flat/mild, and the pipeline mapped total_score −3.15 to predicted_direction 'flat' — even though both negative components pointed the same direction and the outcome was knowable at open (XLE premarket gap-down ~−1.6%)."
evidence_cited: "2026-09-04 Energy: S1=−1 (WTI −0.53% offered), S4=−1 (Sep 3 rel −1.78%), total −3.15 → predicted flat/flat; actual XLE −0.87% (down/mild). The outcome review confirms: 'The direction was knowable; the scoring rubric's flat/down boundary was the issue.' Also 2026-09-03 Energy: S1=−1 (oil offered), S4=0 (no tape confirmation), total −2.7 → predicted flat/flat; actual XLE −0.74% vs SPY +1.05% (rel −1.78%) — same pattern where negative S1 alone with a red premarket barrel should have signaled down when the sector had an extended relative run."
error_category: "B"
scope: "general"
date: "2026-09-04"
status: "active"
occurrences: "1"
promoted_on: "2026-09-04"
sources: "['2026-09-04_sector_energy_lesson.md']"
schema_ok: "true"
---

## RULE
When S1 and S4 are both negative AND the 1d relative tape is decisively red (rel ≤ −1.5%), the predicted direction must be "down" with mild magnitude — the DO-INSTEAD flat/mild preference should cap magnitude (notable → mild) but must not override direction when two independent negative channels (sector factor + tape confirmation) align. The flat band requires either (a) mixed signs across components, (b) a single negative component without tape confirmation, or (c) sub-threshold negative evidence (rel between −0.5% and −1.5%).

## WHEN IT FIRES
When S1 (sector factors) and S4 (ETF tape) are both negative (−1 each) and the 1d relative tape is decisively red (rel ≤ −1.5%), the total score should map to a mild DOWN direction, not "flat" — the flat band should be reserved for cases where negative components are offset by positive ones or where the negative evidence is sub-threshold (single −1 with no tape confirmation).

## WRONG IF
A future session where S1=−1 (live oil-offered) and S4=−1 (1d rel ≤ −1.5%) but XLE closes flat or positive (abs ≥ +0.3%) would falsify this lesson. Also falsified if mapping to "down" in this configuration produces a worse direction hit-rate than "flat" over the next 10 Energy predictions.

## EVIDENCE
2026-09-04 Energy: S1=−1 (WTI −0.53% offered), S4=−1 (Sep 3 rel −1.78%), total −3.15 → predicted flat/flat; actual XLE −0.87% (down/mild). The outcome review confirms: "The direction was knowable; the scoring rubric's flat/down boundary was the issue." Also 2026-09-03 Energy: S1=−1 (oil offered), S4=0 (no tape confirmation), total −2.7 → predicted flat/flat; actual XLE −0.74% vs SPY +1.05% (rel −1.78%) — same pattern where negative S1 alone with a red premarket barrel should have signaled down when the sector had an extended relative run.

(learn_cycle promote)
