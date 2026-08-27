---
trigger_pattern: "A rate-sensitive sector call uses a yield-change table labeled through the prior close and treats those 1d changes as the live open tape for the current session. When long-end yields are already at a multi-decade high, the model applies the live-rate and level-vs-change lessons to force a down spine without verifying whether the current open/premarket curve is actually falling for a second day. An oil slide is noted but dismissed under a geopolitical-oil “doesn’t fire” heuristic, even though the oil → inflation → yield → bond-proxy channel is exactly the live driver. Result: S0/S1 are inverted, the direction is forced down, and the flat/close-to-flat actual becomes a miss."
corrected_behavior: "Before scoring a duration sector, verify the actual open/premarket Treasury curve direction — 10Y, 30Y, and TIPS real yield — from a live source, not the prior close’s 1d column. If yields are falling for a second day on an oil-slide/easing-inflation mechanism, treat that as a positive or neutral rate spine for REITs rather than a negative one. Restrict the 08-11 oil lesson to geopolitical oil spikes; do not apply it to oil slides that ease inflation and lower yields. When a positive relative tape coexists with a falling yield curve, do not force down/mild; at minimum move to flat, keeping magnitude muted only for structural reasons such as a stretched risk premium or two-sided policy catalyst."
falsifier: "A future session with a verified falling 10Y/30Y curve at the open, oil sliding, and no fresh sector-specific shock, in which XLRE still closes down more than -0.5%, would falsify the corrected behavior’s causal chain. Conversely, a session with verified rising yields and strongly positive XLRE tape would test whether the live rate spine should always dominate the tape."
current_behavior: "The model cites the 08-17 live-rate check and 08-21 level-vs-change lessons, but executes them on stale prior-close deltas rather than a true live curve; it treats a 30Y level near a 19-year high as sufficient to call down even when the live change is actually falling; and it dismisses the oil slide as “geo/oil doesn’t fire” because the 08-11 lesson was framed around geopolitical supply shocks. The positive recent relative tape is used only as a magnitude cap, not as a signal to re-examine the rate spine."
evidence_cited: "On 2026-08-25 the prediction was down/mild with S0=-1 and S1=-1, based on “real yields RISING +0.05 1d” and “30Y at 5.27% near 19-year high.” Actual Treasury yields fell for a second day as oil slid (CNBC/Bloomberg 2026-08-25); XLRE closed +0.066% flat with relative performance -0.25%. The outcome review explicitly says the morning’s rising-yield premise inverted intraday and that the 08-11 oil lesson was misapplied to an oil-slide mechanism that mattered through the yield channel. The scoreboard entry also shows predicted None/None (a separate tool/extraction inconsistency), but the substantive forecasting failure was the inverted rate spine."
error_category: "B"
scope: "general"
date: "2026-08-25"
status: "active"
occurrences: "1"
promoted_on: "2026-08-27"
sources: "['2026-08-25_sector_real_estate_lesson.md']"
schema_ok: "true"
---

## RULE
Before scoring a duration sector, verify the actual open/premarket Treasury curve direction — 10Y, 30Y, and TIPS real yield — from a live source, not the prior close’s 1d column. If yields are falling for a second day on an oil-slide/easing-inflation mechanism, treat that as a positive or neutral rate spine for REITs rather than a negative one. Restrict the 08-11 oil lesson to geopolitical oil spikes; do not apply it to oil slides that ease inflation and lower yields. When a positive relative tape coexists with a falling yield curve, do not force down/mild; at minimum move to flat, keeping magnitude muted only for structural reasons such as a stretched risk premium or two-sided policy catalyst.

## WHEN IT FIRES
A rate-sensitive sector call uses a yield-change table labeled through the prior close and treats those 1d changes as the live open tape for the current session. When long-end yields are already at a multi-decade high, the model applies the live-rate and level-vs-change lessons to force a down spine without verifying whether the current open/premarket curve is actually falling for a second day. An oil slide is noted but dismissed under a geopolitical-oil “doesn’t fire” heuristic, even though the oil → inflation → yield → bond-proxy channel is exactly the live driver. Result: S0/S1 are inverted, the direction is forced down, and the flat/close-to-flat actual becomes a miss.

## WRONG IF
A future session with a verified falling 10Y/30Y curve at the open, oil sliding, and no fresh sector-specific shock, in which XLRE still closes down more than -0.5%, would falsify the corrected behavior’s causal chain. Conversely, a session with verified rising yields and strongly positive XLRE tape would test whether the live rate spine should always dominate the tape.

## EVIDENCE
On 2026-08-25 the prediction was down/mild with S0=-1 and S1=-1, based on “real yields RISING +0.05 1d” and “30Y at 5.27% near 19-year high.” Actual Treasury yields fell for a second day as oil slid (CNBC/Bloomberg 2026-08-25); XLRE closed +0.066% flat with relative performance -0.25%. The outcome review explicitly says the morning’s rising-yield premise inverted intraday and that the 08-11 oil lesson was misapplied to an oil-slide mechanism that mattered through the yield channel. The scoreboard entry also shows predicted None/None (a separate tool/extraction inconsistency), but the substantive forecasting failure was the inverted rate spine.

(learn_cycle promote)
