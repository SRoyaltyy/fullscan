---
trigger_pattern: "When an active sector lesson says a live two-sided geopolitical/oil supply-shock headline caps magnitude, and the next prediction narrative concludes “oil flat / no overhang” based only on low CL/BZ percentage prints, verify the oil claim against Brent's absolute level and shipping-attack headlines before emitting up/severe. On scheduled CPI days, in-line CPI relief tends to rotate into growth/tech, not into oil-sensitive cyclicals; if the geopolitical oil shock is still active, Industrials can close flat/lag SPY even when premarket futures are risk-on."
corrected_behavior: "Before finalizing an Industrials severe call, check (1) Brent absolute level and overnight move, (2) shipping-attack/Hormuz headline status, and (3) whether the active 08-11 lesson trigger is still firing. If oil is rising or attacks are active, do not describe the tape as “flat”; cap S0 at 0 or negative, set the multiplier ≤ 1.0, and reduce the magnitude band to at most up/notable. On macro-event days, avoid double-counting correlated S1 structural factors and S3 flows when the dominant market driver is the CPI print and the oil tape."
falsifier: "If a future day has confirmed rising oil, active shipping attacks, and a live Hormuz overhang, but XLI nevertheless rallies > roughly 1% and leads SPY, then capping to up/notable would be too conservative. The rule would then need to distinguish defense/AI-power-heavy XLI composition from broad oil-sensitive cyclical drag."
current_behavior: "Treats small crude-futures changes as proof the Hormuz/oil overhang is gone, scores S0_SHARED_MACRO +1, applies a 1.1 multiplier, and emits up/severe. It cites the active 08-11 Industrials lesson in the narrative but fails to test its actual trigger condition against Brent and headline news."
evidence_cited: "2026-08-12 predicted Industrials up/severe with total score 12.65 (S0 +1, S1 +2, S3 +1, multiplier 1.1). Actual XLI +0.097%, SPY +0.250%, relative -0.154%; direction/magnitude MISS. Brent was rising ~2% to near $90 with active shipping attacks — the same Hormuz overhang from 08-11 was still live, but the morning read explicitly claimed “oil flat (no geopolitical overhang).” The forecast was knowably wrong at the open."
error_category: "B"
scope: "general"
date: "2026-08-12"
status: "active"
occurrences: "1"
promoted_on: "2026-08-13"
sources: "['2026-08-12_sector_industrials_lesson.md']"
schema_ok: "true"
---

## RULE
Before finalizing an Industrials severe call, check (1) Brent absolute level and overnight move, (2) shipping-attack/Hormuz headline status, and (3) whether the active 08-11 lesson trigger is still firing. If oil is rising or attacks are active, do not describe the tape as “flat”; cap S0 at 0 or negative, set the multiplier ≤ 1.0, and reduce the magnitude band to at most up/notable. On macro-event days, avoid double-counting correlated S1 structural factors and S3 flows when the dominant market driver is the CPI print and the oil tape.

## WHEN IT FIRES
When an active sector lesson says a live two-sided geopolitical/oil supply-shock headline caps magnitude, and the next prediction narrative concludes “oil flat / no overhang” based only on low CL/BZ percentage prints, verify the oil claim against Brent's absolute level and shipping-attack headlines before emitting up/severe. On scheduled CPI days, in-line CPI relief tends to rotate into growth/tech, not into oil-sensitive cyclicals; if the geopolitical oil shock is still active, Industrials can close flat/lag SPY even when premarket futures are risk-on.

## WRONG IF
If a future day has confirmed rising oil, active shipping attacks, and a live Hormuz overhang, but XLI nevertheless rallies > roughly 1% and leads SPY, then capping to up/notable would be too conservative. The rule would then need to distinguish defense/AI-power-heavy XLI composition from broad oil-sensitive cyclical drag.

## EVIDENCE
2026-08-12 predicted Industrials up/severe with total score 12.65 (S0 +1, S1 +2, S3 +1, multiplier 1.1). Actual XLI +0.097%, SPY +0.250%, relative -0.154%; direction/magnitude MISS. Brent was rising ~2% to near $90 with active shipping attacks — the same Hormuz overhang from 08-11 was still live, but the morning read explicitly claimed “oil flat (no geopolitical overhang).” The forecast was knowably wrong at the open.

(learn_cycle promote)
