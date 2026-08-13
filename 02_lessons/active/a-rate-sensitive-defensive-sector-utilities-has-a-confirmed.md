---
trigger_pattern: "A rate-sensitive/defensive sector (utilities) has a confirmed positive catalyst (inline CPI → second session of yield relief) and a strong structural narrative (AI data-center load growth), but the broad tape is risk-on with growth/tech leading and a same-day sector-narrative headwind appears (e.g., a credible research report questioning AI power-demand realization). The model correctly flips direction to up but over-corrects magnitude by letting structural/flow component scores dominate, producing “notable” when the defensive bid is structurally capped."
corrected_behavior: "When the broad tape is risk-on with tech leading and a same-day or fresh sector-narrative headwind challenges the structural thesis, cap the magnitude to “mild” unless there is durable sector leadership (sustained 1d/3d relative outperformance, breadth expansion, or confirmed inflows). Reconcile the narrative magnitude with component scores: do not let S1 +2 and S3 +1 drive a notable call when the rotation environment is simultaneously capping defensives. On scheduled-CPI days, an in-line print can lift utilities modestly, but if the money is rotating into growth/tech, the utility move is likely mild, not notable."
falsifier: "This lesson would be falsified if, under the same conditions — risk-on tech-led tape plus a same-day data-center-demand skepticism report — XLU still posts a notable gain (>1% absolute or strong relative outperformance) with broad sector participation. That would show the structural AI-power bid can overpower the rotation cap, and magnitude should remain notable."
current_behavior: "After correctly applying the prior Utilities lesson and flipping from down to up, the model assigns S1_SECTOR_FACTORS +2 and S3_FLOWS_POSITIONING +1, allowing the deterministic output to emit up/notable. It treats the AI-power thesis and flow-reversal as dominant while underweighting two knowable/partially-knowable caps: (a) a Nasdaq-led risk-on tape rotates capital into growth and away from defensive bond-proxies; (b) the sector’s own 1d relative outperformance is much weaker than the prior session’s confirmation (0.23% vs 1.48%). The narrative says “mild-to-notable,” but the final magnitude band is “notable.”"
evidence_cited: "Utilities 2026-08-12: predicted up/notable, actual XLU +0.48%, SPY +0.25%, rel +0.23% → direction HIT, magnitude MISS. Morning components were S0=0, S1=+2, S2=+1, S3=+1, S4=+1, total 10.0. Outcome review found S1/S3 over-weighted: the same-day Wood Mackenzie report (only ~28% of the 1,066 GW requested for data centers likely to materialize) challenged the AI-power thesis, and the risk-on tech-led tape (Nasdaq-led rally after subdued CPI) capped the defensive bid. The prior day’s XLU 1d rel +1.48% was not repeated; actual rel was only +0.23%."
error_category: "C"
scope: "general"
date: "2026-08-12"
status: "active"
occurrences: "1"
promoted_on: "2026-08-13"
sources: "['2026-08-12_sector_utilities_lesson.md']"
schema_ok: "true"
---

## RULE
When the broad tape is risk-on with tech leading and a same-day or fresh sector-narrative headwind challenges the structural thesis, cap the magnitude to “mild” unless there is durable sector leadership (sustained 1d/3d relative outperformance, breadth expansion, or confirmed inflows). Reconcile the narrative magnitude with component scores: do not let S1 +2 and S3 +1 drive a notable call when the rotation environment is simultaneously capping defensives. On scheduled-CPI days, an in-line print can lift utilities modestly, but if the money is rotating into growth/tech, the utility move is likely mild, not notable.

## WHEN IT FIRES
A rate-sensitive/defensive sector (utilities) has a confirmed positive catalyst (inline CPI → second session of yield relief) and a strong structural narrative (AI data-center load growth), but the broad tape is risk-on with growth/tech leading and a same-day sector-narrative headwind appears (e.g., a credible research report questioning AI power-demand realization). The model correctly flips direction to up but over-corrects magnitude by letting structural/flow component scores dominate, producing “notable” when the defensive bid is structurally capped.

## WRONG IF
This lesson would be falsified if, under the same conditions — risk-on tech-led tape plus a same-day data-center-demand skepticism report — XLU still posts a notable gain (>1% absolute or strong relative outperformance) with broad sector participation. That would show the structural AI-power bid can overpower the rotation cap, and magnitude should remain notable.

## EVIDENCE
Utilities 2026-08-12: predicted up/notable, actual XLU +0.48%, SPY +0.25%, rel +0.23% → direction HIT, magnitude MISS. Morning components were S0=0, S1=+2, S2=+1, S3=+1, S4=+1, total 10.0. Outcome review found S1/S3 over-weighted: the same-day Wood Mackenzie report (only ~28% of the 1,066 GW requested for data centers likely to materialize) challenged the AI-power thesis, and the risk-on tech-led tape (Nasdaq-led rally after subdued CPI) capped the defensive bid. The prior day’s XLU 1d rel +1.48% was not repeated; actual rel was only +0.23%.

(learn_cycle promote)
