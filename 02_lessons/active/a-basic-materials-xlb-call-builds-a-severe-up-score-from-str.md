---
trigger_pattern: "A Basic Materials/XLB call builds a severe-up score from strong structural supply/monetary-metal factors (record copper, DRC/Congo export ban, gold/silver surge, ultra-low inventories), but the current 1d XLB relative return is modest (<0.5%), a China-demand/PMI contraction is explicitly present as an offset, an active geopolitics/oil headline can flip the broad equity tape risk-off, and the analysis text says “temper.” The deterministic output still emits severe because component scores and multiplier are not adjusted to match the tempered conclusion."
corrected_behavior: "When the text concludes “temper” and the 1d XLB relative return is <0.5% with an active geopolitics/oil risk-off headline and an explicit China/PMI offset, score S0 as 0 or negative for the equity tape, cap S1 at 2 to reflect the offset, set S4 to 0 if the 1d tape is only modestly positive, and reduce the multiplier/band so the output is up/notable or up/mild, not up/severe. Direction may remain up only while the metals supply/monetary bid is intact."
falsifier: "The lesson is falsified if, with the same trigger conditions (1d rel <0.5%, China PMI contraction, active Iran/Hormuz-style risk-off headline), XLB still produces a severe absolute gain on the same structural factors, e.g. ≥1.5%. In that case tempering would be wrong and severe would remain justified."
current_behavior: "S0 is scored +1 risk_on because premarket futures are initially flat and the geopolitical headline is read mainly as a precious-metals positive; S1 remains 3, S4 remains 1, multiplier remains 1.4, and the total_score emits up/severe (18.2). The 1d rel (+0.27%) and China PMI drag are noted in the text but not converted into a lower S1/S4 or a lower multiplier."
evidence_cited: "2026-08-11 predicted up/severe; actual XLB +0.113% (flat), SPY -0.320%, rel +0.432%. Direction hit; magnitude missed badly. Morning tape showed 1d rel +0.27%; the Iran/Hormuz risk-off tape and pre-CPI caution capped absolute upside while supply/monetary metals kept XLB green and outperforming."
error_category: "C"
scope: "general"
date: "2026-08-11"
status: "active"
occurrences: "2"
promoted_on: "2026-08-13"
sources: "['2026-08-11_sector_basic_materials_lesson.md', '2026-08-12_sector_basic_materials_lesson.md']"
schema_ok: "true"
---

## RULE
When the text concludes “temper” and the 1d XLB relative return is <0.5% with an active geopolitics/oil risk-off headline and an explicit China/PMI offset, score S0 as 0 or negative for the equity tape, cap S1 at 2 to reflect the offset, set S4 to 0 if the 1d tape is only modestly positive, and reduce the multiplier/band so the output is up/notable or up/mild, not up/severe. Direction may remain up only while the metals supply/monetary bid is intact.

## WHEN IT FIRES
A Basic Materials/XLB call builds a severe-up score from strong structural supply/monetary-metal factors (record copper, DRC/Congo export ban, gold/silver surge, ultra-low inventories), but the current 1d XLB relative return is modest (<0.5%), a China-demand/PMI contraction is explicitly present as an offset, an active geopolitics/oil headline can flip the broad equity tape risk-off, and the analysis text says “temper.” The deterministic output still emits severe because component scores and multiplier are not adjusted to match the tempered conclusion.

## WRONG IF
The lesson is falsified if, with the same trigger conditions (1d rel <0.5%, China PMI contraction, active Iran/Hormuz-style risk-off headline), XLB still produces a severe absolute gain on the same structural factors, e.g. ≥1.5%. In that case tempering would be wrong and severe would remain justified.

## EVIDENCE
2026-08-11 predicted up/severe; actual XLB +0.113% (flat), SPY -0.320%, rel +0.432%. Direction hit; magnitude missed badly. Morning tape showed 1d rel +0.27%; the Iran/Hormuz risk-off tape and pre-CPI caution capped absolute upside while supply/monetary metals kept XLB green and outperforming.

(learn_cycle promote)
