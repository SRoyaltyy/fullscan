---
trigger_pattern: "When the pre-fetched commodity tape conflicts with live sources, resolve with live verification. When the oil spine is up and the geopolitical supply-risk catalyst is actively escalating (ceasefire lapsed, chokepoint closed, oil at multi-year highs), treat it as the dominant S1 driver even if demand-side negatives exist. If the sector has already run hard over 1w and is overbought, cap magnitude at notable rather than severe, but do not downgrade the positive direction."
corrected_behavior: "No correction required. Maintain the pattern: live-verified oil up + actively escalating supply-risk catalyst → positive oil spine; cap only the magnitude band at notable when the run is extended, unless the tape supports severe."
falsifier: "This pattern would be falsified if live oil were rising with an actively escalating geopolitical supply-risk catalyst, yet XLE closed down or materially underperformed SPY. The “cap at notable” part would be falsified if XLE delivered a severe absolute gain (>3%) under the same extended/overbought conditions. Neither occurred today."
current_behavior: "The model correctly verified a live oil sign (pre-fetched CL=F -0.46% vs live WTI +2.28%), treated the fresh Iran/Hormuz escalation as dominant S1 (+2.0), kept S0 muted under sector_shock framing, and capped magnitude at notable due to XLE 1w rel +6.02% and RSI 73. Direction and magnitude both hit."
evidence_cited: "XLE +1.76%, SPY -0.68%, rel +2.43%; scoreboard entry direction_hit=True, magnitude_hit=True. Brent held above $90 and WTI above $84 on fading Hormuz peace prospects. The risk-off tape and oil surge were the same catalyst; energy was the isolated winner, confirming sector_shock."
error_category: "NONE"
scope: "general"
date: "2026-08-18"
status: "active"
occurrences: "1"
promoted_on: "2026-08-19"
sources: "['2026-08-18_sector_energy_lesson.md']"
schema_ok: "true"
---

## RULE
No correction required. Maintain the pattern: live-verified oil up + actively escalating supply-risk catalyst → positive oil spine; cap only the magnitude band at notable when the run is extended, unless the tape supports severe.

## WHEN IT FIRES
When the pre-fetched commodity tape conflicts with live sources, resolve with live verification. When the oil spine is up and the geopolitical supply-risk catalyst is actively escalating (ceasefire lapsed, chokepoint closed, oil at multi-year highs), treat it as the dominant S1 driver even if demand-side negatives exist. If the sector has already run hard over 1w and is overbought, cap magnitude at notable rather than severe, but do not downgrade the positive direction.

## WRONG IF
This pattern would be falsified if live oil were rising with an actively escalating geopolitical supply-risk catalyst, yet XLE closed down or materially underperformed SPY. The “cap at notable” part would be falsified if XLE delivered a severe absolute gain (>3%) under the same extended/overbought conditions. Neither occurred today.

## EVIDENCE
XLE +1.76%, SPY -0.68%, rel +2.43%; scoreboard entry direction_hit=True, magnitude_hit=True. Brent held above $90 and WTI above $84 on fading Hormuz peace prospects. The risk-off tape and oil surge were the same catalyst; energy was the isolated winner, confirming sector_shock.

(learn_cycle promote)
