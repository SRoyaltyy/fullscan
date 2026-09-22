---
trigger_pattern: "All-zero leading S0–S4 on a T+1 digestion session where ES and NQ are both inside ±0.5% vs prior close (no cross-asset melt-up certificate), and v2 still mints a signed official direction from leftover tape_anchor plus index_carry."
corrected_behavior: "When leading_sum=0 and both ES and NQ are inside ±0.5% vs prior close with no 09-21-style cross-asset confirmation, suppress tape_anchor and index_carry from flipping the official call off the unsigned factor-card band (flat). Apply symmetrically to leftover-green and leftover-red anchors. Do not write a new XLY prompt rule — enforce 09-16 in the engine."
falsifier: "Same unsigned T+1 mixed setup with |ES|,|NQ|<0.5% vs prior close where XLY still closes ≤ −0.3% with no fresh consumer shock — then leftover futures were informative and 09-16 must be revised"
current_behavior: "LLM kept S0–S4=0, overlay 0.0, honest flat, and named 09-16 discarded-anchor; engine still published down/mild from tape_anchor −0.324 (ES −0.07%, NQ −0.07%) + index_carry −0.124 against leading_sum 0."
evidence_cited: "2026-09-22 predicted down/mild (engine total −0.448) vs XLY +0.089% / SPY −0.016% / rel +0.105% (flat/flat). Morning S0–S4 all 0 audited correct; AMZN −1.12% vs HD ~+2.8%/TSLA +0.82% cancelled; oil/yields slightly offered; Williams was clearing/reserves not a path binary."
error_category: "D"
scope: "ops"
date: "2026-09-22"
status: "active"
occurrences: "1"
promoted_on: "2026-09-22"
sources: "['2026-09-22_sector_consumer_cyclical_lesson.md']"
schema_ok: "true"
---

## RULE
When leading_sum=0 and both ES and NQ are inside ±0.5% vs prior close with no 09-21-style cross-asset confirmation, suppress tape_anchor and index_carry from flipping the official call off the unsigned factor-card band (flat). Apply symmetrically to leftover-green and leftover-red anchors. Do not write a new XLY prompt rule — enforce 09-16 in the engine.

## WHEN IT FIRES
All-zero leading S0–S4 on a T+1 digestion session where ES and NQ are both inside ±0.5% vs prior close (no cross-asset melt-up certificate), and v2 still mints a signed official direction from leftover tape_anchor plus index_carry.

## WRONG IF
Same unsigned T+1 mixed setup with |ES|,|NQ|<0.5% vs prior close where XLY still closes ≤ −0.3% with no fresh consumer shock — then leftover futures were informative and 09-16 must be revised

## EVIDENCE
2026-09-22 predicted down/mild (engine total −0.448) vs XLY +0.089% / SPY −0.016% / rel +0.105% (flat/flat). Morning S0–S4 all 0 audited correct; AMZN −1.12% vs HD ~+2.8%/TSLA +0.82% cancelled; oil/yields slightly offered; Williams was clearing/reserves not a path binary.

(learn_cycle promote)
