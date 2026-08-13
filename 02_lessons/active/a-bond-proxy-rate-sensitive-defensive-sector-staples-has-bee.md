---
trigger_pattern: "A bond-proxy/rate-sensitive defensive sector (staples) has been underperforming on 1w/1m because of a real-yield/duration headwind, and a scheduled high-impact CPI print is the dominant catalyst. Premarket equity futures are risk-on and the sector has positive leading flow/rotation signals (e.g., first net inflows in months, contrarian defensive-rotation calls). The model treats the CPI print itself as a risk-off trigger for defensives, scores S0_SHARED_MACRO negative, and emits down/mild — ignoring that an in-line/cool CPI would relieve the duration headwind and can make the defensive ETF outperform."
corrected_behavior: "Before scoring S0 negative for a rate-sensitive/bond-proxy sector on a CPI day, identify the sector’s dominant driver. If the dominant driver is real-yield/duration pressure and the CPI resolution is genuinely two-sided, do not force a negative S0 merely because a CPI print exists. When premarket futures are risk-on and leading flow/rotation signals are positive for defensives, an in-line/cool CPI should be scored as neutral-to-positive for S0 and the output should be up/mild or flat/mild, not down/mild. Keep magnitude capped at mild unless the tape confirms a larger move."
falsifier: "If, under the same conditions (risk-on premarket futures, positive defensive inflows/rotation signals, scheduled CPI, XLP already underperforming SPY), the CPI comes in hot, real yields rise further, and XLP falls notably while the corrected behavior emits up/mild or flat/mild, the rule would be falsified. Conversely, repeated in-line/cool CPI prints causing bond-proxy staples to outperform supports the rule."
current_behavior: "On a scheduled CPI day, the model applies the standing defensive lesson as if the CPI outcome were already risk-off for staples: score S0 negative, cap magnitude at mild, emit down/mild when XLP already underperforms SPY on multi-day tape. It also treats risk-on premarket futures as a pure negative for defensives, without considering that an in-line/cool CPI lowers rate-hike odds, reduces real-yield pressure, and reverses the bond-proxy headwind that was the actual sector drag."
evidence_cited: "2026-08-12: July CPI came in-line at +0.1% MoM / 3.4% YoY, easing Fed-hike fears. XLP rose +0.46% vs SPY +0.25% (relative +0.21%), so XLP outperformed. The morning prediction was down/mild (S0=-1, S1=-1, S2=-1, S3=0, S4=-1, total -6.75). Direction MISS; magnitude HIT. The morning itself flagged positive leading signals in S3 (XLP inflows, BofA defensive rotation) but did not trust them, capping S3 at 0 and emitting down."
error_category: "B"
scope: "general"
date: "2026-08-12"
status: "active"
occurrences: "1"
promoted_on: "2026-08-13"
sources: "['2026-08-12_sector_consumer_defensive_lesson.md']"
schema_ok: "true"
---

## RULE
Before scoring S0 negative for a rate-sensitive/bond-proxy sector on a CPI day, identify the sector’s dominant driver. If the dominant driver is real-yield/duration pressure and the CPI resolution is genuinely two-sided, do not force a negative S0 merely because a CPI print exists. When premarket futures are risk-on and leading flow/rotation signals are positive for defensives, an in-line/cool CPI should be scored as neutral-to-positive for S0 and the output should be up/mild or flat/mild, not down/mild. Keep magnitude capped at mild unless the tape confirms a larger move.

## WHEN IT FIRES
A bond-proxy/rate-sensitive defensive sector (staples) has been underperforming on 1w/1m because of a real-yield/duration headwind, and a scheduled high-impact CPI print is the dominant catalyst. Premarket equity futures are risk-on and the sector has positive leading flow/rotation signals (e.g., first net inflows in months, contrarian defensive-rotation calls). The model treats the CPI print itself as a risk-off trigger for defensives, scores S0_SHARED_MACRO negative, and emits down/mild — ignoring that an in-line/cool CPI would relieve the duration headwind and can make the defensive ETF outperform.

## WRONG IF
If, under the same conditions (risk-on premarket futures, positive defensive inflows/rotation signals, scheduled CPI, XLP already underperforming SPY), the CPI comes in hot, real yields rise further, and XLP falls notably while the corrected behavior emits up/mild or flat/mild, the rule would be falsified. Conversely, repeated in-line/cool CPI prints causing bond-proxy staples to outperform supports the rule.

## EVIDENCE
2026-08-12: July CPI came in-line at +0.1% MoM / 3.4% YoY, easing Fed-hike fears. XLP rose +0.46% vs SPY +0.25% (relative +0.21%), so XLP outperformed. The morning prediction was down/mild (S0=-1, S1=-1, S2=-1, S3=0, S4=-1, total -6.75). Direction MISS; magnitude HIT. The morning itself flagged positive leading signals in S3 (XLP inflows, BofA defensive rotation) but did not trust them, capping S3 at 0 and emitting down.

(learn_cycle promote)
