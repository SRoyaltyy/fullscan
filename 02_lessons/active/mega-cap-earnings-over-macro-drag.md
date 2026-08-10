---
trigger_pattern: "Strong positive mega-cap earnings/AI momentum coincides with negative macro/geopolitical headlines (oil spike, China PMI miss, hawkish Fed) — market follows earnings unless futures independently confirm weakness."
corrected_behavior: "When Channel 2 has an index-relevant positive mega-cap earnings catalyst and B6 futures are not negative: set B1 at least 0 (do not net to -1 on oil/China alone); cap B2/B3/B7 combined drag; FORBID predicted_direction=down unless futures or leading internals independently confirm weakness."
falsifier: "Wrong if mega-cap earnings green but SPX still falls that day while futures were non-negative at the open — then revisit whether earnings were truly index-relevant."
current_behavior: "Over-weight oil/China/Fed negatives vs Amazon/MSFT-type prints; call down."
evidence_cited: "2026-07-31 predicted down/mild; actual SPX +0.70%; Amazon +10% / Mag7 +3.2% led; oil/China did not dominate."
error_category: "B"
scope: "general"
date: "2026-07-31"
status: "active"
occurrences: "1"
promoted_on: "2026-08-10"
sources: "['2026-07-31_lesson.md']"
schema_ok: "true"
---

## RULE
When Channel 2 has an index-relevant positive mega-cap earnings catalyst and B6 futures are not negative: set B1 at least 0 (do not net to -1 on oil/China alone); cap B2/B3/B7 combined drag; FORBID predicted_direction=down unless futures or leading internals independently confirm weakness.

## WHEN IT FIRES
Strong positive mega-cap earnings/AI momentum coincides with negative macro/geopolitical headlines (oil spike, China PMI miss, hawkish Fed) — market follows earnings unless futures independently confirm weakness.

## WRONG IF
Wrong if mega-cap earnings green but SPX still falls that day while futures were non-negative at the open — then revisit whether earnings were truly index-relevant.

## EVIDENCE
2026-07-31 predicted down/mild; actual SPX +0.70%; Amazon +10% / Mag7 +3.2% led; oil/China did not dominate.
