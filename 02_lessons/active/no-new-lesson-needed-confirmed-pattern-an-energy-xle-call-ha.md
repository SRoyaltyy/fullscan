---
trigger_pattern: "No new lesson needed. Confirmed pattern: an Energy/XLE call has a correct negative oil spine (crude down, EIA inventory build, IEA/OPEC demand destruction), but XLE has already run >4% 1w relative on the same geopolitical catalyst; the current-day 1d tape is flat and not confirming fresh leadership. The correct output is capped at mild — down/flat-to-mild — and the realized session is likely to be flat absolute with negative relative performance."
corrected_behavior: "No change required. If scoring absolute ETF return, “flat/mild” is an equally valid point estimate when the prior relative run is very large, because the prior run cushions absolute XLE even as oil falls. The reliable signal is relative underperformance, which was correctly captured."
falsifier: "This pattern would be falsified if, despite oil falling, EIA inventory building, IEA/OPEC demand cuts, a flat 1d tape, and a prior >4% 1w relative run, XLE still closed strongly positive both absolutely and relative to SPY. That did not happen; the active lesson is not falsified."
current_behavior: "The model applied the active stale-catalyst lesson, predicted down/mild, capped multiplier at 1.0, and kept S1 as the dominant negative spine while treating S0/S2 as offsets."
evidence_cited: "Actual XLE +0.05%, SPY +0.70%, relative -0.65%. Oil was down ~2% (WTI -2.6%, Brent -2.4%). EIA crude inventory +17.4M bbl; OPEC/IEA cut 2026 demand forecasts. XLE’s absolute flatness was absorbed by the prior 1w rel +6.14% run and refiner/crack-spread offset; the model’s relative-down call materialized."
error_category: "NONE"
scope: "general"
date: "2026-08-13"
status: "active"
occurrences: "1"
promoted_on: "2026-08-14"
sources: "['2026-08-13_sector_energy_lesson.md']"
schema_ok: "true"
---

## RULE
No change required. If scoring absolute ETF return, “flat/mild” is an equally valid point estimate when the prior relative run is very large, because the prior run cushions absolute XLE even as oil falls. The reliable signal is relative underperformance, which was correctly captured.

## WHEN IT FIRES
No new lesson needed. Confirmed pattern: an Energy/XLE call has a correct negative oil spine (crude down, EIA inventory build, IEA/OPEC demand destruction), but XLE has already run >4% 1w relative on the same geopolitical catalyst; the current-day 1d tape is flat and not confirming fresh leadership. The correct output is capped at mild — down/flat-to-mild — and the realized session is likely to be flat absolute with negative relative performance.

## WRONG IF
This pattern would be falsified if, despite oil falling, EIA inventory building, IEA/OPEC demand cuts, a flat 1d tape, and a prior >4% 1w relative run, XLE still closed strongly positive both absolutely and relative to SPY. That did not happen; the active lesson is not falsified.

## EVIDENCE
Actual XLE +0.05%, SPY +0.70%, relative -0.65%. Oil was down ~2% (WTI -2.6%, Brent -2.4%). EIA crude inventory +17.4M bbl; OPEC/IEA cut 2026 demand forecasts. XLE’s absolute flatness was absorbed by the prior 1w rel +6.14% run and refiner/crack-spread offset; the model’s relative-down call materialized.

(learn_cycle promote)
