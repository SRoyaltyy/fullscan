---
trigger_pattern: "Energy/XLE prediction where premarket oil is green (CL/BZ up) and a geopolitical supply-shock catalyst is actively in the same-day news cycle, but the model anchors to the prior 1d flat/negative XLE tape and to negative demand-side offsets (inventory build, IEA/OPEC) and mechanically caps the call at up/mild. The catalyst can re-ignite intraday and produce notable relative outperformance."
corrected_behavior: "Separate a stale catalyst from an escalating one. If oil futures are green premarket AND the geopolitical supply-risk catalyst is still in current headlines, treat the oil spine as the dominant S1 driver; do not let demand-side negatives cap S1 at +1.0 when the live catalyst is supply-positive. Do not require the prior 1d XLE tape to have already confirmed leadership before allowing a notable magnitude; the premarket oil move plus active headlines can be the confirming signal."
falsifier: "Future Energy day with green CL/BZ premarket and active Hormuz/Iran escalation headlines where XLE closes flat or negative would falsify the “raise S1” rule. If such days are systematically overcalled, the stale-catalyst cap should be restored."
current_behavior: "When S1 has a positive oil spine plus active Hormuz/Iran risk but also has negative offsets, the model sets S1≈+1, S4=0, multiplier=1.0, and emits up/mild. It uses the prior day’s flat/negative 1d relative tape as proof of no fresh leadership and applies the stale-catalyst cap too rigidly, underweighting the live oil direction."
evidence_cited: "2026-08-14: XLE +1.39% abs, +1.59% rel vs SPY while SPY fell -0.20%. WTI +1.34%, Brent +1.69%. Driver was renewed tanker attacks and US-Iran war of words/blockade threats over Hormuz, i.e. the active supply-risk catalyst re-ignited intraday. Predicted up/mild; actual up/notable. Direction correct, magnitude undercalled."
error_category: "A"
scope: "general"
date: "2026-08-14"
status: "active"
occurrences: "1"
promoted_on: "2026-08-17"
sources: "['2026-08-14_sector_energy_lesson.md']"
schema_ok: "true"
---

## RULE
Separate a stale catalyst from an escalating one. If oil futures are green premarket AND the geopolitical supply-risk catalyst is still in current headlines, treat the oil spine as the dominant S1 driver; do not let demand-side negatives cap S1 at +1.0 when the live catalyst is supply-positive. Do not require the prior 1d XLE tape to have already confirmed leadership before allowing a notable magnitude; the premarket oil move plus active headlines can be the confirming signal.

## WHEN IT FIRES
Energy/XLE prediction where premarket oil is green (CL/BZ up) and a geopolitical supply-shock catalyst is actively in the same-day news cycle, but the model anchors to the prior 1d flat/negative XLE tape and to negative demand-side offsets (inventory build, IEA/OPEC) and mechanically caps the call at up/mild. The catalyst can re-ignite intraday and produce notable relative outperformance.

## WRONG IF
Future Energy day with green CL/BZ premarket and active Hormuz/Iran escalation headlines where XLE closes flat or negative would falsify the “raise S1” rule. If such days are systematically overcalled, the stale-catalyst cap should be restored.

## EVIDENCE
2026-08-14: XLE +1.39% abs, +1.59% rel vs SPY while SPY fell -0.20%. WTI +1.34%, Brent +1.69%. Driver was renewed tanker attacks and US-Iran war of words/blockade threats over Hormuz, i.e. the active supply-risk catalyst re-ignited intraday. Predicted up/mild; actual up/notable. Direction correct, magnitude undercalled.

(learn_cycle promote)
