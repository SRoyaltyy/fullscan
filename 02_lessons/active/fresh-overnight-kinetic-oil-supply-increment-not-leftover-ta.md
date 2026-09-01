---
trigger_pattern: "Fresh overnight kinetic/oil-supply increment (not leftover-tape) with independently confirming NQ ≤ −0.5%, Europe already red, paid hawkish Fed text scored only in B3, BN=GN confined to data, no same-morning mega-cap or hard-data miss."
corrected_behavior: "No score change. Keep B1 at −1 (not −2 crash, not leftover 0) when the kinetic increment is fresh; keep B7 at −0.5 for the same oil shock; allow B6 −0.5 when NQ independently confirms ≤ −0.5%; leave B3 as the only Warsh sleeve; emit DOWN/MILD; do not apply 08-28 leftover-tape flatten or mega-cap down-forbid when futures already confirm weakness."
falsifier: "If this trigger recurs and SPX closes ≥ +0.3% or ≥ 1.0% down on 2 of the next 3 such days, down/mild must be revised, not defended."
current_behavior: "Scored B1 −1 on the Hormuz tanker/strike increment, B7 −0.5 as the oil-transmission sleeve (no −1/−1 double-count), B6 −0.5 on NQ −1.04%, B3 −0.5 for paid Warsh/hike odds, B2 −0.5 on live backup without promoting the 1d FRED tick, multiplier 0.9 → down/mild; 08-28 leftover flatten and mega-cap down-forbid correctly not fired."
evidence_cited: "2026-09-01 predicted down/mild (total −6.3, conf 0.55) vs SPX −0.71% / NDX −1.03% (down/mild); direction_hit True, magnitude_hit True; KNOWABLE_AT_9AM yes; PATH never green."
error_category: "NONE"
scope: "general"
date: "2026-09-01"
status: "active"
occurrences: "1"
promoted_on: "2026-09-01"
sources: "['2026-09-01_lesson.md']"
schema_ok: "true"
---

## RULE
No score change. Keep B1 at −1 (not −2 crash, not leftover 0) when the kinetic increment is fresh; keep B7 at −0.5 for the same oil shock; allow B6 −0.5 when NQ independently confirms ≤ −0.5%; leave B3 as the only Warsh sleeve; emit DOWN/MILD; do not apply 08-28 leftover-tape flatten or mega-cap down-forbid when futures already confirm weakness.

## WHEN IT FIRES
Fresh overnight kinetic/oil-supply increment (not leftover-tape) with independently confirming NQ ≤ −0.5%, Europe already red, paid hawkish Fed text scored only in B3, BN=GN confined to data, no same-morning mega-cap or hard-data miss.

## WRONG IF
If this trigger recurs and SPX closes ≥ +0.3% or ≥ 1.0% down on 2 of the next 3 such days, down/mild must be revised, not defended.

## EVIDENCE
2026-09-01 predicted down/mild (total −6.3, conf 0.55) vs SPX −0.71% / NDX −1.03% (down/mild); direction_hit True, magnitude_hit True; KNOWABLE_AT_9AM yes; PATH never green.

(learn_cycle promote)
