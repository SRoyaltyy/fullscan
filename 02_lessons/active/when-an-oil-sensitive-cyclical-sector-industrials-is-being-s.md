---
trigger_pattern: "When an oil-sensitive cyclical sector (Industrials) is being scored while a prior-session geopolitical supply-shock headline (e.g., Hormuz, Brent near $90) is still in the news, but the pre-fetched oil tape is down and fresh demand-side catalysts are available (OPEC/IEA demand-forecast cuts, large inventory builds, official comments on normal flows), the model treats the headline as the current-session truth and discards the tape. The correct behavior is to check whether the demand-side catalyst has already flipped the day’s oil direction; if so, the headline is the stale leg and the sector bias should be down/flat-to-mild, not up/mild."
corrected_behavior: "Before discarding a pre-fetched oil direction under an active geopolitical headline, verify whether the oil move is corroborated by a fresh demand-side catalyst (OPEC/IEA demand revision, inventory build, demand-destruction data). If oil is down for demand reasons while the old supply headline remains in the news, treat the headline as prior-session noise for today’s session; set S0 ≤ 0, do not add an up bias from structural factors alone, and with a negative 1d relative tape prefer down/flat-to-mild rather than up/mild."
falsifier: "This rule would be falsified if a future session has an active Hormuz-type headline, a fresh OPEC/API demand-side selloff signal, pre-fetched oil down, and a negative XLI 1d relative tape, yet XLI closes positive and outperforms SPY. Alternatively, if Brent actually holds its prior level and oil rises, then up/mild would be correct and the “demand-side flip” reading would be wrong."
current_behavior: "The model sees a “live” geopolitical oil-supply headline, concludes the pre-fetched CL/BZ down print is stale, caps S0 at 0, leaves structural positives (ISM, AI-power, freight) to create an up bias, and emits up/mild despite the negative 1d XLI relative tape and a valid demand-side oil selloff. It over-applies the prior 08-11/08-12 Industrials lesson by deciding any supply-shock headline automatically overrides the actual oil tape."
evidence_cited: "2026-08-13 XLI fell -0.048% while SPY rose +0.698%, producing -0.746% relative underperformance. The morning prediction said “oil is NOT down” because Bloomberg reported Brent holding just below $89, but actual oil fell ~2% to $87.20–87.69 on OPEC cutting 2026 demand-growth forecasts and a surprise +9.07M barrel API crude build. The pre-fetched oil tape (CL/BZ down) was directionally correct; the “live” headline was the stale prior-session state. Direction miss scored by the scoreboard."
error_category: "B"
scope: "general"
date: "2026-08-13"
status: "active"
occurrences: "1"
promoted_on: "2026-08-14"
sources: "['2026-08-13_sector_industrials_lesson.md']"
schema_ok: "true"
---

## RULE
Before discarding a pre-fetched oil direction under an active geopolitical headline, verify whether the oil move is corroborated by a fresh demand-side catalyst (OPEC/IEA demand revision, inventory build, demand-destruction data). If oil is down for demand reasons while the old supply headline remains in the news, treat the headline as prior-session noise for today’s session; set S0 ≤ 0, do not add an up bias from structural factors alone, and with a negative 1d relative tape prefer down/flat-to-mild rather than up/mild.

## WHEN IT FIRES
When an oil-sensitive cyclical sector (Industrials) is being scored while a prior-session geopolitical supply-shock headline (e.g., Hormuz, Brent near $90) is still in the news, but the pre-fetched oil tape is down and fresh demand-side catalysts are available (OPEC/IEA demand-forecast cuts, large inventory builds, official comments on normal flows), the model treats the headline as the current-session truth and discards the tape. The correct behavior is to check whether the demand-side catalyst has already flipped the day’s oil direction; if so, the headline is the stale leg and the sector bias should be down/flat-to-mild, not up/mild.

## WRONG IF
This rule would be falsified if a future session has an active Hormuz-type headline, a fresh OPEC/API demand-side selloff signal, pre-fetched oil down, and a negative XLI 1d relative tape, yet XLI closes positive and outperforms SPY. Alternatively, if Brent actually holds its prior level and oil rises, then up/mild would be correct and the “demand-side flip” reading would be wrong.

## EVIDENCE
2026-08-13 XLI fell -0.048% while SPY rose +0.698%, producing -0.746% relative underperformance. The morning prediction said “oil is NOT down” because Bloomberg reported Brent holding just below $89, but actual oil fell ~2% to $87.20–87.69 on OPEC cutting 2026 demand-growth forecasts and a surprise +9.07M barrel API crude build. The pre-fetched oil tape (CL/BZ down) was directionally correct; the “live” headline was the stale prior-session state. Direction miss scored by the scoreboard.

(learn_cycle promote)
