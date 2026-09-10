---
trigger_pattern: "A sector ETF has demonstrated multi-timeframe relative outperformance (1d/3d/1w/1m rel all positive) going into a session where a live, high-conviction macro overlay is present (oil supply shock >$100, long-end yield backup, 5-day yield–equity correlation strongly negative ≤ −0.9, VIX term structure in backwardation), and the model uses the trailing relative strength as a reason to CAP the macro negative (declining S0 = −2 in favor of −1) rather than treating the trailing strength as the crowded-long fuel that makes the macro overlay more dangerous."
corrected_behavior: "When a live macro overlay is present with (a) oil supply shock, (b) long-end yield backup, (c) 5-day yield–equity correlation ≤ −0.9, and (d) VIX term structure in backwardation, the sector's trailing relative outperformance must be scored as CROWDED-LONG FUEL (a multiplier on the macro negative), not as a resilience shield. S0 should be scored at full weight (−2), and the DO-INSTEAD divergence rule must be applied as a conviction damper on the SIGNAL, not as a sign-flipper that neutralizes a correct bearish lean. Specifically: when leading_sum is negative and the tape is positive on a risk-off regime day with a −0.9 yield–equity correlation, the correct output is down/mild, not flat."
falsifier: "This lesson is falsified if a future session presents the same configuration (live oil supply shock >$100, 10Y ≥ 4.75, 5-day yield–equity correlation ≤ −0.9, VIX/VIX3M > 1.0 backwardation, sector 1m rel ≥ +2% crowded-long) and the sector ETF closes flat or up on the session. In that case the trailing relative strength genuinely functioned as a shield and the corrected behavior (down/mild) would be wrong."
current_behavior: "The model wrote 'S0 = −1: fresh macro risk-off overlay for XLK, but not −2 given XLK's demonstrated relative resilience' — i.e., it used lagging relative outperformance as a shield to under-weight a live macro shock. It then computed leading_sum = −1, flagged divergence against the positive tape, and invoked DO-INSTEAD to flatten a correct bearish lean to flat/flat. Result: direction MISS and magnitude MISS on a −1.41% / −0.81% rel session."
evidence_cited: "Morning tape: XLK 1d rel +0.46%, 3d +2.41%, 1w +2.22%, 1m +2.21%; Brent $102.08, 10Y 4.80, 5d 10Y–SPX corr −0.969, VIX/VIX3M 1.079 backwardation, JPMorgan semis crowding ~99%. Realized: XLK −1.41%, SPY −0.60%, rel −0.81%, open 185.24 → close 185.22 (flat-open, close-at-lows = intraday distribution, the fingerprint of crowded-long unwind). The morning's own S3 = −1 (crowded long) was the correct signal; it was cancelled against S1 = +1 rather than compounded with S0."
error_category: "A"
scope: "general"
date: "2026-09-10"
status: "active"
occurrences: "1"
promoted_on: "2026-09-10"
sources: "['2026-09-10_sector_technology_lesson.md']"
schema_ok: "true"
---

## RULE
When a live macro overlay is present with (a) oil supply shock, (b) long-end yield backup, (c) 5-day yield–equity correlation ≤ −0.9, and (d) VIX term structure in backwardation, the sector's trailing relative outperformance must be scored as CROWDED-LONG FUEL (a multiplier on the macro negative), not as a resilience shield. S0 should be scored at full weight (−2), and the DO-INSTEAD divergence rule must be applied as a conviction damper on the SIGNAL, not as a sign-flipper that neutralizes a correct bearish lean. Specifically: when leading_sum is negative and the tape is positive on a risk-off regime day with a −0.9 yield–equity correlation, the correct output is down/mild, not flat.

## WHEN IT FIRES
A sector ETF has demonstrated multi-timeframe relative outperformance (1d/3d/1w/1m rel all positive) going into a session where a live, high-conviction macro overlay is present (oil supply shock >$100, long-end yield backup, 5-day yield–equity correlation strongly negative ≤ −0.9, VIX term structure in backwardation), and the model uses the trailing relative strength as a reason to CAP the macro negative (declining S0 = −2 in favor of −1) rather than treating the trailing strength as the crowded-long fuel that makes the macro overlay more dangerous.

## WRONG IF
This lesson is falsified if a future session presents the same configuration (live oil supply shock >$100, 10Y ≥ 4.75, 5-day yield–equity correlation ≤ −0.9, VIX/VIX3M > 1.0 backwardation, sector 1m rel ≥ +2% crowded-long) and the sector ETF closes flat or up on the session. In that case the trailing relative strength genuinely functioned as a shield and the corrected behavior (down/mild) would be wrong.

## EVIDENCE
Morning tape: XLK 1d rel +0.46%, 3d +2.41%, 1w +2.22%, 1m +2.21%; Brent $102.08, 10Y 4.80, 5d 10Y–SPX corr −0.969, VIX/VIX3M 1.079 backwardation, JPMorgan semis crowding ~99%. Realized: XLK −1.41%, SPY −0.60%, rel −0.81%, open 185.24 → close 185.22 (flat-open, close-at-lows = intraday distribution, the fingerprint of crowded-long unwind). The morning's own S3 = −1 (crowded long) was the correct signal; it was cancelled against S1 = +1 rather than compounded with S0.

(learn_cycle promote)
