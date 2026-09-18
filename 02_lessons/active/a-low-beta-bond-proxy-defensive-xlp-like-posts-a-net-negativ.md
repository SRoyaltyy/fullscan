---
trigger_pattern: "A low-beta bond-proxy defensive (XLP-like) posts a net-negative leading S0–S4 card and a non-haven premarket print already inside the flat band (mid/bottom of a green book, PM ≲ 0.3%) on a post-event risk-on rebound (index futures ≥ +0.5%, growth/cyclicals lead, oil offered). The v2 engine still emits official up/mild from tape_anchor (ES rip + small green PM) plus index_carry, even though overlay/factors describe leftover beta and relative lag, not a sector-up day."
corrected_behavior: "When leading S0–S4 is net-negative and sector PM is non-haven and already in the flat band, do not accept v2 up/mild from ES tape_anchor + index_carry. Official call is up/flat (PM beta) or flat/flat — never widen magnitude to mild off index beta. Size_gate + non-haven PM caps the band at flat. Trust factors/overlay over leftover RS and over index_carry."
falsifier: "Same trigger recurs and XLP still closes ≥0.3% (mild or larger) so tape_anchor up/mild matches cash better than a flat cap — then the cap is wrong and must be revised"
current_behavior: "Narrative scored S0=-1, S1=-0.5, S2=S3=S4=0, overlay -3.2, leftover 3d/1w RS paid, PM +0.18% named as non-haven beta. Engine still printed up/mild (total 0.765) from tape_anchor 2.119 + index_carry 1.846."
evidence_cited: "2026-09-17 XLP +0.192% vs SPY +1.134% (rel -0.942%); predicted up/mild vs actual up/flat; PM +0.18% ≈ cash; S0–S4 held; leftover 3d/1w RS did not continue."
error_category: "B"
scope: "general"
date: "2026-09-17"
status: "active"
occurrences: "2"
promoted_on: "2026-09-18"
sources: "['2026-09-17_sector_consumer_defensive_lesson.md', '2026-09-18_sector_consumer_defensive_lesson.md']"
schema_ok: "true"
---

## RULE
When leading S0–S4 is net-negative and sector PM is non-haven and already in the flat band, do not accept v2 up/mild from ES tape_anchor + index_carry. Official call is up/flat (PM beta) or flat/flat — never widen magnitude to mild off index beta. Size_gate + non-haven PM caps the band at flat. Trust factors/overlay over leftover RS and over index_carry.

## WHEN IT FIRES
A low-beta bond-proxy defensive (XLP-like) posts a net-negative leading S0–S4 card and a non-haven premarket print already inside the flat band (mid/bottom of a green book, PM ≲ 0.3%) on a post-event risk-on rebound (index futures ≥ +0.5%, growth/cyclicals lead, oil offered). The v2 engine still emits official up/mild from tape_anchor (ES rip + small green PM) plus index_carry, even though overlay/factors describe leftover beta and relative lag, not a sector-up day.

## WRONG IF
Same trigger recurs and XLP still closes ≥0.3% (mild or larger) so tape_anchor up/mild matches cash better than a flat cap — then the cap is wrong and must be revised

## EVIDENCE
2026-09-17 XLP +0.192% vs SPY +1.134% (rel -0.942%); predicted up/mild vs actual up/flat; PM +0.18% ≈ cash; S0–S4 held; leftover 3d/1w RS did not continue.

(learn_cycle promote)
