---
trigger_pattern: "A rate-sensitive/defensive sector (Consumer Staples) has been lagging on 1w/1m due to a real-yield/duration headwind. A benign CPI print has already produced one session of yield relief, and positive leading signals are present: first ETF inflows in months, a defensive-rotation call, and 1d/3d relative tape inflecting positive. On the follow-through morning, a same-day scheduled inflation release (PPI) is pending. If PPI also prints cool, the second consecutive tame inflation print can push rates down further and make the bond-proxy defensive outperform SPY by >0.3%, producing a notable move — not merely flat/mild — even though the broad tape is risk-on."
corrected_behavior: "Before calling “no fresh catalyst,” check the economic calendar for a scheduled same-day inflation release such as PPI/jobless claims. If prior CPI was benign and the same-day PPI prints cool/in-line, score S0 positive for a bond-proxy defensive, not neutral. Do not apply the utility-specific “risk-on tech-led tape caps magnitude” rule to Consumer Staples unless there is a negative sector-specific narrative. When S3 flows/rotation and S4 tape are both confirming, allow magnitude to be notable: up/notable."
falsifier: "If a similar setup occurs — prior CPI benign, positive staples flows, pending same-day PPI — but PPI prints hot or core inflation surprises upward, rates rise, and XLP falls/flat/underperforms, then the S0-positive/notable rule would be wrong. Also, if the 10Y/real yields do not actually fall at the open despite a cool print, magnitude should be capped at mild."
current_behavior: "On the second session of yield relief, the model treats the prior CPI catalyst as already traded, scores S0_SHARED_MACRO = 0 because “risk-on tape with tech leading” caps defensive magnitude, applies the utility-specific multiplier cap generically to staples, omits the scheduled same-day PPI catalyst from the narrative, and emits up/flat."
evidence_cited: "2026-08-13 XLP +1.08% vs SPY +0.70%, relative +0.38%. Morning prediction was up/flat; direction HIT, magnitude MISS. Primary driver was cooler July PPI (flat, 4.7% YoY) — the second tame inflation print — pushing rates lower. XLP, as a duration-sensitive bond-proxy, directly benefited. S3 (+1; ~$551M first inflows since February, BofA defensive rotation) and S4 (+1; tape confirming) were correct. S0=0 and S1=0 were under-scored. The PPI release was on the economic calendar and knowable at open, so this was not irreducible noise."
error_category: "B"
scope: "general"
date: "2026-08-13"
status: "active"
occurrences: "1"
promoted_on: "2026-08-14"
sources: "['2026-08-13_sector_consumer_defensive_lesson.md']"
schema_ok: "true"
---

## RULE
Before calling “no fresh catalyst,” check the economic calendar for a scheduled same-day inflation release such as PPI/jobless claims. If prior CPI was benign and the same-day PPI prints cool/in-line, score S0 positive for a bond-proxy defensive, not neutral. Do not apply the utility-specific “risk-on tech-led tape caps magnitude” rule to Consumer Staples unless there is a negative sector-specific narrative. When S3 flows/rotation and S4 tape are both confirming, allow magnitude to be notable: up/notable.

## WHEN IT FIRES
A rate-sensitive/defensive sector (Consumer Staples) has been lagging on 1w/1m due to a real-yield/duration headwind. A benign CPI print has already produced one session of yield relief, and positive leading signals are present: first ETF inflows in months, a defensive-rotation call, and 1d/3d relative tape inflecting positive. On the follow-through morning, a same-day scheduled inflation release (PPI) is pending. If PPI also prints cool, the second consecutive tame inflation print can push rates down further and make the bond-proxy defensive outperform SPY by >0.3%, producing a notable move — not merely flat/mild — even though the broad tape is risk-on.

## WRONG IF
If a similar setup occurs — prior CPI benign, positive staples flows, pending same-day PPI — but PPI prints hot or core inflation surprises upward, rates rise, and XLP falls/flat/underperforms, then the S0-positive/notable rule would be wrong. Also, if the 10Y/real yields do not actually fall at the open despite a cool print, magnitude should be capped at mild.

## EVIDENCE
2026-08-13 XLP +1.08% vs SPY +0.70%, relative +0.38%. Morning prediction was up/flat; direction HIT, magnitude MISS. Primary driver was cooler July PPI (flat, 4.7% YoY) — the second tame inflation print — pushing rates lower. XLP, as a duration-sensitive bond-proxy, directly benefited. S3 (+1; ~$551M first inflows since February, BofA defensive rotation) and S4 (+1; tape confirming) were correct. S0=0 and S1=0 were under-scored. The PPI release was on the economic calendar and knowable at open, so this was not irreducible noise.

(learn_cycle promote)
