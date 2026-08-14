---
trigger_pattern: "In a Utilities/XLU call, a second soft inflation print has already produced yield relief, but the broad tape is risk-on with growth/tech leading and a fresh data-center load-growth disappointment is present. Direction/magnitude can be up/mild and correct even when XLU underperforms SPY on a relative basis; the relative underperformance is not the graded target."
corrected_behavior: "When the tape is risk-on/tech-led and the bond-proxy bid is capped by same-day sector headwinds, treat S2/S4 as confirmation only for the absolute up/mild move. Do not imply or rely on relative outperformance; explicitly allow XLU to lag SPY. If future runs cite relative tape, label it as “absolute confirmation only.”"
falsifier: "If XLU in this same macro setup (soft CPI/PPI, risk-on tech tape, no fresh Wood Mackenzie/Texas-style headwind, confirmed inflows) outperforms SPY by >0.3%, the “do not expect relative outperformance” refinement would be too broad and should be scoped to the presence of sector-specific headwinds."
current_behavior: "The morning read produced up/mild and correctly applied the 08-12 cap-to-mild lesson. It did, however, read the 1d/3d positive relative tape as auxiliary confirmation (S2=+1, S4=+1) and verbally expected XLU to hold modest relative outperformance. Actual XLU +0.46%, SPY +0.70%, rel -0.24%; the S2/S4 relative read missed, but the graded call hit."
evidence_cited: "XLU +0.456% vs SPY +0.698% (rel -0.242%); soft PPI (core +0.2% M/M, Y/Y 4.2%) pressured yields lower yet ZeroHedge observed “Tech outperformance continues…” The morning predictor’s up/mild was verified: direction_hit True, magnitude_hit True."
error_category: "NONE"
scope: "general"
date: "2026-08-13"
status: "active"
occurrences: "1"
promoted_on: "2026-08-14"
sources: "['2026-08-13_sector_utilities_lesson.md']"
schema_ok: "true"
---

## RULE
When the tape is risk-on/tech-led and the bond-proxy bid is capped by same-day sector headwinds, treat S2/S4 as confirmation only for the absolute up/mild move. Do not imply or rely on relative outperformance; explicitly allow XLU to lag SPY. If future runs cite relative tape, label it as “absolute confirmation only.”

## WHEN IT FIRES
In a Utilities/XLU call, a second soft inflation print has already produced yield relief, but the broad tape is risk-on with growth/tech leading and a fresh data-center load-growth disappointment is present. Direction/magnitude can be up/mild and correct even when XLU underperforms SPY on a relative basis; the relative underperformance is not the graded target.

## WRONG IF
If XLU in this same macro setup (soft CPI/PPI, risk-on tech tape, no fresh Wood Mackenzie/Texas-style headwind, confirmed inflows) outperforms SPY by >0.3%, the “do not expect relative outperformance” refinement would be too broad and should be scoped to the presence of sector-specific headwinds.

## EVIDENCE
XLU +0.456% vs SPY +0.698% (rel -0.242%); soft PPI (core +0.2% M/M, Y/Y 4.2%) pressured yields lower yet ZeroHedge observed “Tech outperformance continues…” The morning predictor’s up/mild was verified: direction_hit True, magnitude_hit True.

(learn_cycle promote)
