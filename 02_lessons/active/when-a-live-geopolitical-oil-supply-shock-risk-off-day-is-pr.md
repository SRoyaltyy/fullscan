---
trigger_pattern: "When a live geopolitical/oil supply-shock risk-off day is present (Brent >$100, long-end yields in stress zone) and the sector is Financials (XLF), the correct treatment is to score S0 strongly negative (−2), score S1 negative (−0.5) via the oil→inflation→long-end yield→rate-sensitive financials channel, apply a dampening multiplier (≤1.0), and predict down/mild — NOT down/flat or down/notable. The tight credit backdrop (HY ~2.68) tempers magnitude from notable to mild, and the flat 1d relative confirms no sector-specific underperformance beyond the macro drag."
corrected_behavior: "When a live oil supply shock (Brent >$100) with long-end yields in stress zone is present, score S0=−2 (oil→inflation→long-end yield channel is a direct negative for rate-sensitive financials, NOT a value shield), score S1=−0.5 (rate-sensitivity channel distinct from S0 macro drag), apply multiplier ≤1.0 (geo/oil live + flat S4 → no absolute up), and predict down/mild — the tight credit backdrop (HY ~2.68) tempers magnitude from notable to mild. Do NOT assume a 'value shield' on oil-shock days."
falsifier: "If on a subsequent oil-shock day with Brent >$100 and long-end yields in stress zone, XLF falls ≥1% (notable) despite tight credit (HY <2.75), then the 'tight credit tempers to mild' assumption is falsified. Conversely, if XLF falls <0.2% (flat) on such a day, the S0=−2 / S1=−0.5 scoring is too negative. The lesson would also be falsified if XLF rises (up) on an oil-shock day with long-end stress, which would indicate the 'value shield' dynamic can re-emerge under certain conditions not yet identified."
current_behavior: "On 09-08, the model predicted down/flat and the actual was XLF −1.38% (dir HIT, mag MISS — actual notable). The model had under-scored the oil shock's impact on financials by assuming a 'value shield' and scoring S0=−2 but S1=−0.5 with insufficient magnitude conviction. On 09-09, the model correctly applied the 09-08 lesson: S0=−2, S1=−0.5, multiplier 0.9, predicted down/mild."
evidence_cited: "09-09 prediction down/mild vs actual XLF −0.42% (dir HIT, mag HIT). All five component scores validated: S0=−2 (HIT), S1=−0.5 (HIT), S2=−0.5 (HIT), S3=0 (HIT), S4=−0.5 (HIT), multiplier 0.9 (HIT). The 09-08 lesson (down/flat vs actual −1.38%, mag MISS) was correctly applied and produced a full hit on 09-09. The escalation from $94 to >$100 Brent did not push magnitude to notable because credit remained tight (HY 2.68, creeping but not blowing out) and futures were muted-negative, not a crash tape."
error_category: "NONE"
scope: "general"
date: "2026-09-09"
status: "active"
occurrences: "1"
promoted_on: "2026-09-09"
sources: "['2026-09-09_sector_financial_lesson.md']"
schema_ok: "true"
---

## RULE
When a live oil supply shock (Brent >$100) with long-end yields in stress zone is present, score S0=−2 (oil→inflation→long-end yield channel is a direct negative for rate-sensitive financials, NOT a value shield), score S1=−0.5 (rate-sensitivity channel distinct from S0 macro drag), apply multiplier ≤1.0 (geo/oil live + flat S4 → no absolute up), and predict down/mild — the tight credit backdrop (HY ~2.68) tempers magnitude from notable to mild. Do NOT assume a "value shield" on oil-shock days.

## WHEN IT FIRES
When a live geopolitical/oil supply-shock risk-off day is present (Brent >$100, long-end yields in stress zone) and the sector is Financials (XLF), the correct treatment is to score S0 strongly negative (−2), score S1 negative (−0.5) via the oil→inflation→long-end yield→rate-sensitive financials channel, apply a dampening multiplier (≤1.0), and predict down/mild — NOT down/flat or down/notable. The tight credit backdrop (HY ~2.68) tempers magnitude from notable to mild, and the flat 1d relative confirms no sector-specific underperformance beyond the macro drag.

## WRONG IF
If on a subsequent oil-shock day with Brent >$100 and long-end yields in stress zone, XLF falls ≥1% (notable) despite tight credit (HY <2.75), then the "tight credit tempers to mild" assumption is falsified. Conversely, if XLF falls <0.2% (flat) on such a day, the S0=−2 / S1=−0.5 scoring is too negative. The lesson would also be falsified if XLF rises (up) on an oil-shock day with long-end stress, which would indicate the "value shield" dynamic can re-emerge under certain conditions not yet identified.

## EVIDENCE
09-09 prediction down/mild vs actual XLF −0.42% (dir HIT, mag HIT). All five component scores validated: S0=−2 (HIT), S1=−0.5 (HIT), S2=−0.5 (HIT), S3=0 (HIT), S4=−0.5 (HIT), multiplier 0.9 (HIT). The 09-08 lesson (down/flat vs actual −1.38%, mag MISS) was correctly applied and produced a full hit on 09-09. The escalation from $94 to >$100 Brent did not push magnitude to notable because credit remained tight (HY 2.68, creeping but not blowing out) and futures were muted-negative, not a crash tape.

(learn_cycle promote)
