---
trigger_pattern: "When a sector-specific negative cluster (packaged-food margin/dividend stress) has demonstrated dominance over a flight-to-safety bid on multiple prior sessions (09-03, 09-08), and a fresh input-cost shock (oil >$100) compounds rather than offsets the sector drag, the model correctly scores S0=0 (FTS bid neutralized), S1 negative (food-crash + input-cost squeeze), and predicts down/mild — which is validated when the sector underperforms SPY on a risk-off day."
corrected_behavior: "Continue this pattern: when a sector-specific negative cluster has demonstrated dominance over the FTS bid on consecutive sessions, score S0=0 or negative even under the strongest FTS trigger (oil >$100); treat the oil spike as an input-cost negative for staples (S1), not a defensive bid (S0); maintain the down/mild call when the food-crash drag is fresh and the tape confirms multi-horizon lagging."
falsifier: "A future instance where the food-crash cluster has demonstrated dominance on 2+ sessions, oil is >$100, but XLP rallies on a genuine FTS bid (rel > +0.5%) would falsify the 'food-crash always dominates' inference. Also, if the food-crash cluster fades (no new dividend cuts, earnings stabilize) and the model still scores S0=0 on a fresh oil shock, that would be over-applying this lesson."
current_behavior: "Model correctly applied the 09-08 lesson (food-crash dominance → S0=0 or negative), treated oil-at-$100 as an input-cost negative rather than a defensive bid, avoided double-counting the oil spike across S0 and S1, and predicted down/mild with all five S-scores validated by the outcome."
evidence_cited: "Predicted down/mild (total −1.35, S0=0, S1=−1, S4=−0.5). Actual XLP −1.15% vs SPY −0.46% (rel −0.69%). Direction HIT, magnitude HIT (within mild band). All five S-scores validated as HIT in the outcome audit. The 09-08 lesson was correctly applied — the food-crash dominance extended to a third session (09-09)."
error_category: "NONE"
scope: "general"
date: "2026-09-09"
status: "active"
occurrences: "1"
promoted_on: "2026-09-09"
sources: "['2026-09-09_sector_consumer_defensive_lesson.md']"
schema_ok: "true"
---

## RULE
Continue this pattern: when a sector-specific negative cluster has demonstrated dominance over the FTS bid on consecutive sessions, score S0=0 or negative even under the strongest FTS trigger (oil >$100); treat the oil spike as an input-cost negative for staples (S1), not a defensive bid (S0); maintain the down/mild call when the food-crash drag is fresh and the tape confirms multi-horizon lagging.

## WHEN IT FIRES
When a sector-specific negative cluster (packaged-food margin/dividend stress) has demonstrated dominance over a flight-to-safety bid on multiple prior sessions (09-03, 09-08), and a fresh input-cost shock (oil >$100) compounds rather than offsets the sector drag, the model correctly scores S0=0 (FTS bid neutralized), S1 negative (food-crash + input-cost squeeze), and predicts down/mild — which is validated when the sector underperforms SPY on a risk-off day.

## WRONG IF
A future instance where the food-crash cluster has demonstrated dominance on 2+ sessions, oil is >$100, but XLP rallies on a genuine FTS bid (rel > +0.5%) would falsify the "food-crash always dominates" inference. Also, if the food-crash cluster fades (no new dividend cuts, earnings stabilize) and the model still scores S0=0 on a fresh oil shock, that would be over-applying this lesson.

## EVIDENCE
Predicted down/mild (total −1.35, S0=0, S1=−1, S4=−0.5). Actual XLP −1.15% vs SPY −0.46% (rel −0.69%). Direction HIT, magnitude HIT (within mild band). All five S-scores validated as HIT in the outcome audit. The 09-08 lesson was correctly applied — the food-crash dominance extended to a third session (09-09).

(learn_cycle promote)
