---
trigger_pattern: "When a defensive-sector prediction is directionally negative but the premarket/global tape shows no directional confirmation (flat US futures, flat global sessions) and the analysis text itself flags an offsetting/dampening signal (e.g., negative 10Y-SPX correlation, cooling risk appetite, nascent defensive bid), the deterministic score still emits a high-magnitude band solely from structural factor scores."
corrected_behavior: "When the analysis text identifies a dampening factor that reduces conviction, the final magnitude band should be capped at mild/flat unless the tape independently confirms notable movement. Structural negatives can still justify a negative direction, but a high negative total like -9.6 requires confirmation from futures/tape, not just S0–S4 factor scores."
falsifier: "If a future Consumer Defensive prediction with flat futures/no tape confirmation nonetheless produces a notable move (e.g., ≥0.75% in the predicted direction), the “cap magnitude at mild when tape shows no confirmation” rule would be weakened. One occurrence would not fully falsify it, but repeated notable moves under flat tape would invalidate the rule."
current_behavior: "The component scores are driven by structural rotation and mega-cap drag (Walmart) without adequately damping magnitude when the analysis narrative acknowledges that the tape is flat and a defensive bid may be emerging. Result: predicted down/notable (-9.6) while actual was down/flat (-0.20%, rel -0.17%)."
evidence_cited: "XLP actual -0.20% (rel -0.17%) vs predicted -9.6 / notable. Morning analysis itself noted: “negative 10Y-SPX correlation (-0.842) hints rising yields may eventually favor defensives relatively” and “futures flat… some defensive bid emerging.” Actual day was essentially flat, consistent with that dampening signal, not with the notable score."
error_category: "C"
scope: "general"
date: "2026-08-10"
status: "active"
occurrences: "1"
promoted_on: "2026-08-13"
sources: "['2026-08-10_sector_consumer_defensive_lesson.md']"
schema_ok: "true"
---

## RULE
When the analysis text identifies a dampening factor that reduces conviction, the final magnitude band should be capped at mild/flat unless the tape independently confirms notable movement. Structural negatives can still justify a negative direction, but a high negative total like -9.6 requires confirmation from futures/tape, not just S0–S4 factor scores.

## WHEN IT FIRES
When a defensive-sector prediction is directionally negative but the premarket/global tape shows no directional confirmation (flat US futures, flat global sessions) and the analysis text itself flags an offsetting/dampening signal (e.g., negative 10Y-SPX correlation, cooling risk appetite, nascent defensive bid), the deterministic score still emits a high-magnitude band solely from structural factor scores.

## WRONG IF
If a future Consumer Defensive prediction with flat futures/no tape confirmation nonetheless produces a notable move (e.g., ≥0.75% in the predicted direction), the “cap magnitude at mild when tape shows no confirmation” rule would be weakened. One occurrence would not fully falsify it, but repeated notable moves under flat tape would invalidate the rule.

## EVIDENCE
XLP actual -0.20% (rel -0.17%) vs predicted -9.6 / notable. Morning analysis itself noted: “negative 10Y-SPX correlation (-0.842) hints rising yields may eventually favor defensives relatively” and “futures flat… some defensive bid emerging.” Actual day was essentially flat, consistent with that dampening signal, not with the notable score.

(learn_cycle promote)
