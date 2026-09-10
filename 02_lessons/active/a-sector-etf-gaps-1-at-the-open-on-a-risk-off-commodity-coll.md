---
trigger_pattern: "A sector ETF gaps ≥1% at the open on a risk-off / commodity-collapse day, but the morning magnitude band is set to 'mild' because the model applies rolling-magnitude discipline (shrink band on modest |score|) against a tape that has ALREADY printed a notable-range opening gap. The gap itself is the magnitude signal and is knowable at the bell."
corrected_behavior: "Before applying any rolling-magnitude / shrink-the-band discipline, check the opening gap. If |open vs prior close| ≥ 1.0% (or the ETF is already trading at/inside the notable band at the bell), the 'mild' band is falsified at the open and must be raised to at least 'notable' — regardless of how modest the composite |score| looks. Rolling-magnitude discipline is a prior on the DISTRIBUTION of outcomes, not a license to ignore a same-morning print that already sits in the higher band. The gap is a same-morning, knowable-at-open magnitude observation and outranks the historical base rate for band selection."
falsifier: "This lesson is falsified if, on a future risk-off / commodity-collapse day, XLB (or the analogous sector ETF) gaps ≥1% at the open and then closes inside the 'mild' band (|close| < ~0.75% and rel < ~0.5%) — i.e. the gap fully reverses intraday. In that case the opening gap is NOT a reliable magnitude signal and the rolling-mag-discipline prior should retain precedence. Track gap-at-open vs realized close for the next several XLB sessions."
current_behavior: "On 2026-09-10 the morning note scored total −3.6 (down/mild) while XLB opened at 50.76 vs prior close 51.39 — i.e. already −1.23% at the bell, which is itself in the 'notable' band. The note explicitly invoked 'rolling mag discipline favors mild on modest |score|' and the DO-INSTEAD instruction ('shrink confidence on modest |score| when magnitude historically misses') to keep the band at mild. The pipeline-computed decision then overrode the essay's own mild band to notable (total_score −9.0, mult 0.9, divergence_flagged True), and the actual was −1.23% (notable). So the essay's magnitude reasoning was the weak link; the pipeline's override happened to be right."
evidence_cited: "Outcome file: 'XLB opened at 50.76 (already −1.23% from the 51.39 prior close)... The gap-down open (50.76 vs 51.39, −1.23% at the bell) was knowable at the open and was itself already at the notable threshold. The morning note predicted 'mild' while the ETF was already trading at −1.23% pre-fill... the open itself falsified the mild band before the session began.' Actual: XLB −1.23%, SPY −0.60%, rel −0.63% → notable. This is the SECOND consecutive dir-HIT/mag-MISS in the same direction (09-09 predicted down/mild, actual −1.06% notable; 09-10 predicted down/mild in essay, actual −1.23% notable)."
error_category: "C"
scope: "general"
date: "2026-09-10"
status: "active"
occurrences: "1"
promoted_on: "2026-09-10"
sources: "['2026-09-10_sector_basic_materials_lesson.md']"
schema_ok: "true"
---

## RULE
Before applying any rolling-magnitude / shrink-the-band discipline, check the opening gap. If |open vs prior close| ≥ 1.0% (or the ETF is already trading at/inside the notable band at the bell), the "mild" band is falsified at the open and must be raised to at least "notable" — regardless of how modest the composite |score| looks. Rolling-magnitude discipline is a prior on the DISTRIBUTION of outcomes, not a license to ignore a same-morning print that already sits in the higher band. The gap is a same-morning, knowable-at-open magnitude observation and outranks the historical base rate for band selection.

## WHEN IT FIRES
A sector ETF gaps ≥1% at the open on a risk-off / commodity-collapse day, but the morning magnitude band is set to "mild" because the model applies rolling-magnitude discipline (shrink band on modest |score|) against a tape that has ALREADY printed a notable-range opening gap. The gap itself is the magnitude signal and is knowable at the bell.

## WRONG IF
This lesson is falsified if, on a future risk-off / commodity-collapse day, XLB (or the analogous sector ETF) gaps ≥1% at the open and then closes inside the "mild" band (|close| < ~0.75% and rel < ~0.5%) — i.e. the gap fully reverses intraday. In that case the opening gap is NOT a reliable magnitude signal and the rolling-mag-discipline prior should retain precedence. Track gap-at-open vs realized close for the next several XLB sessions.

## EVIDENCE
Outcome file: "XLB opened at 50.76 (already −1.23% from the 51.39 prior close)... The gap-down open (50.76 vs 51.39, −1.23% at the bell) was knowable at the open and was itself already at the notable threshold. The morning note predicted 'mild' while the ETF was already trading at −1.23% pre-fill... the open itself falsified the mild band before the session began." Actual: XLB −1.23%, SPY −0.60%, rel −0.63% → notable. This is the SECOND consecutive dir-HIT/mag-MISS in the same direction (09-09 predicted down/mild, actual −1.06% notable; 09-10 predicted down/mild in essay, actual −1.23% notable).

(learn_cycle promote)
