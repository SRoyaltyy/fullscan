---
trigger_pattern: "A same-session high-impact macro release (ISM Services, CPI, payrolls, etc.) is explicitly named as the load-bearing, two-sided catalyst. Premarket macro tape is flat/mild and there is no anti-FTS or risk-on tape license; S0/S2/S4 are all near zero. Only a modest sector-factor sleeve (e.g., S1 input-cost relief) is positive. The model still emits a signed direction (up) because no divergence flag is tripped."
corrected_behavior: "When a high-impact scheduled macro release is the stated load-bearing catalyst and S0/S2/S4 are neutral, do not let S1 alone create an up/down forecast. Before the data, either emit no-sign/flat or issue an explicitly conditional call (e.g., “if ISM services beats, XLP should lag in a risk-on rotation; if it misses, XLP can hold flat/up”). If a sector-factor sleeve is retained, it must be capped at sub-directional size or confirmed by S0/S2; the default pre-release output should be flat/no-sign."
falsifier: "The lesson is falsified if future cases with flat premarket tape, a scheduled two-sided macro print, and S1-only positive factors repeatedly resolve according to S1 (e.g., XLP rises on input-cost relief) while macro is not the dominant driver. A single macro surprise does not falsify it; a systematic failure of the flat/no-sign default versus the S1-based directional call would."
current_behavior: "The two-sided macro event is described in prose as event risk, but the score does not use it as a direction gate. An S1 sector-factor positive such as ag input-cost relief is allowed to generate an up/flat call. When the macro print surprises to the risk-on side, the defensive sector lags SPY by >1.3% and both direction and magnitude miss."
evidence_cited: "2026-09-03 XLP −0.32% vs SPY +1.05%, relative −1.36%. Morning components were S0=0, S1=+0.5, S2=0, S3=0, S4=0 with predicted up/flat. ISM Services printed 55.4 vs ~54.2 forecast, triggering a broad risk-on rotation into cyclicals/small caps/materials and out of staples; ISM Employment at 47.8 was the two-sided tension the morning noted, but the headline beat dominated."
error_category: "C"
scope: "general"
date: "2026-09-03"
status: "active"
occurrences: "1"
promoted_on: "2026-09-04"
sources: "['2026-09-03_sector_consumer_defensive_lesson.md']"
schema_ok: "true"
---

## RULE
When a high-impact scheduled macro release is the stated load-bearing catalyst and S0/S2/S4 are neutral, do not let S1 alone create an up/down forecast. Before the data, either emit no-sign/flat or issue an explicitly conditional call (e.g., “if ISM services beats, XLP should lag in a risk-on rotation; if it misses, XLP can hold flat/up”). If a sector-factor sleeve is retained, it must be capped at sub-directional size or confirmed by S0/S2; the default pre-release output should be flat/no-sign.

## WHEN IT FIRES
A same-session high-impact macro release (ISM Services, CPI, payrolls, etc.) is explicitly named as the load-bearing, two-sided catalyst. Premarket macro tape is flat/mild and there is no anti-FTS or risk-on tape license; S0/S2/S4 are all near zero. Only a modest sector-factor sleeve (e.g., S1 input-cost relief) is positive. The model still emits a signed direction (up) because no divergence flag is tripped.

## WRONG IF
The lesson is falsified if future cases with flat premarket tape, a scheduled two-sided macro print, and S1-only positive factors repeatedly resolve according to S1 (e.g., XLP rises on input-cost relief) while macro is not the dominant driver. A single macro surprise does not falsify it; a systematic failure of the flat/no-sign default versus the S1-based directional call would.

## EVIDENCE
2026-09-03 XLP −0.32% vs SPY +1.05%, relative −1.36%. Morning components were S0=0, S1=+0.5, S2=0, S3=0, S4=0 with predicted up/flat. ISM Services printed 55.4 vs ~54.2 forecast, triggering a broad risk-on rotation into cyclicals/small caps/materials and out of staples; ISM Employment at 47.8 was the two-sided tension the morning noted, but the headline beat dominated.

(learn_cycle promote)
