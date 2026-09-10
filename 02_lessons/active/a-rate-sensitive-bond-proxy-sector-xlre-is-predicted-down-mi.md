---
trigger_pattern: "A rate-sensitive bond-proxy sector (XLRE) is predicted down/mild on a live rates-backup + oil-shock + risk-off day, with the S2 (breadth) and S4 (ETF tape) components both set to −1 primarily off a single prior-day relative print (1d rel −0.65%) rather than a live same-morning relative signal; the realized relative underperformance comes in materially shallower (−0.23%) than the prior-day anchor implied."
corrected_behavior: "When S2 and S4 are both set to −1 primarily off a single prior-day relative print (not a live, same-morning relative signal), cap the combined S2+S4 contribution at −1.5 rather than −2.0. Direction is unchanged; the relative-extent risk is properly discounted. Today this would have yielded ≈ −5.4 × 0.9 ≈ −4.9, still down/mild, with better-calibrated magnitude confidence."
falsifier: "If a future session has S2 and S4 both set to −1 off a single prior-day relative print and the realized relative underperformance comes in at or beyond the prior-day anchor (e.g., rel ≤ −0.65% when anchored on −0.65%), the cap would have under-scored and the lesson should be revised. Conversely, if applying the −1.5 cap repeatedly produces magnitude MISSes on the low side (realized |move| consistently above the mild band), the cap is too tight."
current_behavior: "S2 and S4 are each scored −1 by anchoring on the prior-session 1d relative (−0.65%) plus multi-horizon lag, producing a combined −2.0 contribution to the leading sum; the direction call is correct but the relative-extent (magnitude) is systematically overstated relative to the realized relative print."
evidence_cited: "Morning S2=−1, S4=−1 justified by 'every horizon is a relative laggard' and 1d rel −0.65%; realized rel was −0.23% (XLRE −0.83% vs SPY −0.60%). Direction HIT, magnitude HIT (mild), but the relative-extent anchor was ~3x the realized relative. The morning self-flagged this ('shrink confidence on modest |score|') and set confidence 0.55 — the right instinct, but the score construction still over-weighted the prior-day relative print."
error_category: "NONE"
scope: "general"
date: "2026-09-10"
status: "active"
occurrences: "1"
promoted_on: "2026-09-10"
sources: "['2026-09-10_sector_real_estate_lesson.md']"
schema_ok: "true"
---

## RULE
When S2 and S4 are both set to −1 primarily off a single prior-day relative print (not a live, same-morning relative signal), cap the combined S2+S4 contribution at −1.5 rather than −2.0. Direction is unchanged; the relative-extent risk is properly discounted. Today this would have yielded ≈ −5.4 × 0.9 ≈ −4.9, still down/mild, with better-calibrated magnitude confidence.

## WHEN IT FIRES
A rate-sensitive bond-proxy sector (XLRE) is predicted down/mild on a live rates-backup + oil-shock + risk-off day, with the S2 (breadth) and S4 (ETF tape) components both set to −1 primarily off a single prior-day relative print (1d rel −0.65%) rather than a live same-morning relative signal; the realized relative underperformance comes in materially shallower (−0.23%) than the prior-day anchor implied.

## WRONG IF
If a future session has S2 and S4 both set to −1 off a single prior-day relative print and the realized relative underperformance comes in at or beyond the prior-day anchor (e.g., rel ≤ −0.65% when anchored on −0.65%), the cap would have under-scored and the lesson should be revised. Conversely, if applying the −1.5 cap repeatedly produces magnitude MISSes on the low side (realized |move| consistently above the mild band), the cap is too tight.

## EVIDENCE
Morning S2=−1, S4=−1 justified by "every horizon is a relative laggard" and 1d rel −0.65%; realized rel was −0.23% (XLRE −0.83% vs SPY −0.60%). Direction HIT, magnitude HIT (mild), but the relative-extent anchor was ~3x the realized relative. The morning self-flagged this ("shrink confidence on modest |score|") and set confidence 0.55 — the right instinct, but the score construction still over-weighted the prior-day relative print.

(learn_cycle promote)
