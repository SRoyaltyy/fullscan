---
trigger_pattern: "A sector ETF gaps down ≥1% at the open on a risk-off / commodity-collapse day, but the model applies rolling-magnitude discipline ('shrink the band on modest |score|') and predicts mild — even though the opening print itself is already inside the notable band."
corrected_behavior: "When the sector ETF's opening print is already at or beyond the notable threshold (|gap| ≥ ~1.0%) on a risk-off / metals-collapse day, the magnitude band must be set to **notable** at the open, independent of the rolling-magnitude prior. Rolling-magnitude discipline is a *tie-breaker for modest |score| with a flat/unknown open* — it is not a license to shrink the band against a tape that has already printed the larger move. The gap is the magnitude signal; the intraday acceleration (copper −2.89% → −5.36%) is confirmation, not the trigger."
falsifier: "This lesson is falsified if a future session shows XLB (or a comparable sector ETF) gapping down ≥1.0% at the open on a risk-off/commodity-collapse day and then closing **inside the mild band** (|close| < ~0.75% or rel not notable) — i.e., a gap-down that fully reverses intraday. In that case the gap is not a reliable magnitude signal and the rolling-magnitude prior should retain priority."
current_behavior: "On 2026-09-10 the morning note scored S0=−1, S1=−2, S2=−1, S3=0, S4=0 → total −3.6, and explicitly chose magnitude **mild** on the reasoning 'rolling mag discipline favors mild on modest |score|' plus 'ES is only +0.11%, no same-morning China print, CPI is tomorrow.' XLB opened at 50.76 vs a 51.39 prior close — a −1.23% gap at the bell — and closed at 50.76 (−1.23%, rel −0.63%). The gap-down open had already falsified the mild band before the session began, yet the band was not upgraded."
evidence_cited: "Outcome file: 'XLB opened at 50.76 (already −1.23% from the 51.39 prior close)... The gap-down open... was itself already at the *notable* threshold. The morning note predicted 'mild' while the ETF was *already trading at −1.23% pre-fill*... **the open itself falsified the mild band before the session began.**' Actual: XLB −1.23%, rel −0.63%, full-session trend-down path (high $51.35 early, low $50.58, close at the open). Second consecutive dir-HIT/mag-MISS in the same direction (09-09 predicted mild, actual notable)."
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
When the sector ETF's opening print is already at or beyond the notable threshold (|gap| ≥ ~1.0%) on a risk-off / metals-collapse day, the magnitude band must be set to **notable** at the open, independent of the rolling-magnitude prior. Rolling-magnitude discipline is a *tie-breaker for modest |score| with a flat/unknown open* — it is not a license to shrink the band against a tape that has already printed the larger move. The gap is the magnitude signal; the intraday acceleration (copper −2.89% → −5.36%) is confirmation, not the trigger.

## WHEN IT FIRES
A sector ETF gaps down ≥1% at the open on a risk-off / commodity-collapse day, but the model applies rolling-magnitude discipline ("shrink the band on modest |score|") and predicts mild — even though the opening print itself is already inside the notable band.

## WRONG IF
This lesson is falsified if a future session shows XLB (or a comparable sector ETF) gapping down ≥1.0% at the open on a risk-off/commodity-collapse day and then closing **inside the mild band** (|close| < ~0.75% or rel not notable) — i.e., a gap-down that fully reverses intraday. In that case the gap is not a reliable magnitude signal and the rolling-magnitude prior should retain priority.

## EVIDENCE
Outcome file: "XLB opened at 50.76 (already −1.23% from the 51.39 prior close)... The gap-down open... was itself already at the *notable* threshold. The morning note predicted 'mild' while the ETF was *already trading at −1.23% pre-fill*... **the open itself falsified the mild band before the session began.**" Actual: XLB −1.23%, rel −0.63%, full-session trend-down path (high $51.35 early, low $50.58, close at the open). Second consecutive dir-HIT/mag-MISS in the same direction (09-09 predicted mild, actual notable).

(learn_cycle promote)
