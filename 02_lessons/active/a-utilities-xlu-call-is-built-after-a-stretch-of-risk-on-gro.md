---
trigger_pattern: "A Utilities/XLU call is built after a stretch of risk-on, growth/tech-led tape (low VIX, Greed, strong Asia tech), but the same session has a scheduled 8:30 ET high-impact consumer/macro release (retail sales, sentiment) that can miss consensus. The model anchors S0 to the prior session’s risk-on rotation and treats that rotation as the permanent cap on the defensive/bond-proxy bid, without stress-testing the scheduled macro calendar for a regime-flip catalyst."
corrected_behavior: "Before finalizing a Utilities call, explicitly scan the day’s economic calendar for 8:30 ET high-impact releases. If a downside miss would plausibly flip a growth-led tape into a defensive rotation, do not let the prior day’s risk-on tape keep S0 at 0; score the bond-proxy/defensive bid as a live positive input. The scheduled release is knowable as a risk even when its outcome is not yet known at compilation time."
falsifier: "On a future Utilities call, if S0 is upgraded from 0 to +1 because a scheduled consumer-data miss could trigger a defensive rotation, and the data does miss, SPY fades, but XLU fails to outperform SPY on a relative basis, then the S0-positive defensive-rotation mechanism is not reliable and this lesson should be revised."
current_behavior: "S0 is scored from the prior day’s tape and stated regime, so a risk-on tech-led backdrop yields S0=0 or negative for utilities even when a weak scheduled consumer print would flip the tape defensive. The narrative emphasizes “risk-on caps the defensive bid” and misses the defensive-rotation setup that would actually lift XLU relative to SPY."
evidence_cited: "Morning XLU call used S0=0 with regime “mixed” and narrative “risk-on tech-led tape.” Actual day: retail sales -0.6% vs +0.1% consensus (first decline in 9 months) and consumer sentiment 51 vs 55.2; SPY fell -0.20% while XLU rose +0.61%, delivering rel +0.81%. Outcome review itself labeled S0 the weak link: the defensive rotation was the dominant driver, and the S0 read underweighted it. The direction/magnitude call still hit, but for the right final band and partly luck of the 3d/1w positive inflection."
error_category: "A"
scope: "general"
date: "2026-08-14"
status: "active"
occurrences: "1"
promoted_on: "2026-08-17"
sources: "['2026-08-14_sector_utilities_lesson.md']"
schema_ok: "true"
---

## RULE
Before finalizing a Utilities call, explicitly scan the day’s economic calendar for 8:30 ET high-impact releases. If a downside miss would plausibly flip a growth-led tape into a defensive rotation, do not let the prior day’s risk-on tape keep S0 at 0; score the bond-proxy/defensive bid as a live positive input. The scheduled release is knowable as a risk even when its outcome is not yet known at compilation time.

## WHEN IT FIRES
A Utilities/XLU call is built after a stretch of risk-on, growth/tech-led tape (low VIX, Greed, strong Asia tech), but the same session has a scheduled 8:30 ET high-impact consumer/macro release (retail sales, sentiment) that can miss consensus. The model anchors S0 to the prior session’s risk-on rotation and treats that rotation as the permanent cap on the defensive/bond-proxy bid, without stress-testing the scheduled macro calendar for a regime-flip catalyst.

## WRONG IF
On a future Utilities call, if S0 is upgraded from 0 to +1 because a scheduled consumer-data miss could trigger a defensive rotation, and the data does miss, SPY fades, but XLU fails to outperform SPY on a relative basis, then the S0-positive defensive-rotation mechanism is not reliable and this lesson should be revised.

## EVIDENCE
Morning XLU call used S0=0 with regime “mixed” and narrative “risk-on tech-led tape.” Actual day: retail sales -0.6% vs +0.1% consensus (first decline in 9 months) and consumer sentiment 51 vs 55.2; SPY fell -0.20% while XLU rose +0.61%, delivering rel +0.81%. Outcome review itself labeled S0 the weak link: the defensive rotation was the dominant driver, and the S0 read underweighted it. The direction/magnitude call still hit, but for the right final band and partly luck of the 3d/1w positive inflection.

(learn_cycle promote)
