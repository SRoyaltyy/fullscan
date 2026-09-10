---
trigger_pattern: "A sector ETF has printed ≥2 consecutive sessions of relative stabilization (|1d rel| ≤ ~0.15%, sign flipping around zero) after a multi-day relative lag, AND the leading negative sum is built from S0/S1/S2 components that all trace to the SAME root cause (one macro shock → unwind → rotation-out → breadth failure). The model treats the stabilization as a magnitude cap (S4 only) while leaving S0/S1/S2 at full negative weight, so the compounded sum overstates the expected move and the mechanism (relative underperformance) never fires."
corrected_behavior: "(1) When 1d rel has stabilized for ≥2 consecutive sessions after a multi-day lag, decay S0/S1/S2 toward zero — cap the combined S0+S1+S2 at ≈ −1.5, not −3.0. (2) Broaden the double-count check: if S0, S1, and S2 all trace to one root cause, they are not independent and the sum must be discounted, not merely checked pairwise (oil-vs-rotation). (3) S2 (breadth) must score only sector-internal breadth (constituent dispersion, sub-industry divergence); cross-asset co-moves (metals complex) belong in S0 or nowhere — scoring them in S2 is a category error. (4) Treat a third consecutive relative stabilization as an exhaustion signal that pulls the leading sum toward zero, not just a magnitude cap on S4."
falsifier: "If on a future day with ≥2 consecutive 1d rel stabilizations after a multi-day lag, the sector ETF subsequently prints rel ≤ −0.5% (clear relative underperformance) with S0/S1/S2 all tracing to one root cause, then the 'decay the leading sum' correction is wrong and the full-weight stacking was justified. Conversely, if the sector prints rel ≥ +0.3% (relative outperformance) on such a day, the correction is confirmed."
current_behavior: "On 2026-09-10 the morning read saw the 1d rel +0.14% ('second consecutive modest stabilization') but scored S0 = −1.0, S1 = −1.0, S2 = −1.0, S4 = −0.5 (leading_sum −7.0, total −6.525). The oil-shock → crowded-long-unwind → rotation-out → breadth-failure chain was scored as four-to-five independent negatives. Outcome: XLV −0.552% vs SPY −0.599%, rel +0.047% — direction and magnitude HIT (down/mild) but the mechanism MISS: no relative underperformance materialized, and each of S0/S1/S2 was overscored by ~0.5."
evidence_cited: "Morning: S0 −1.0 / S1 −1.0 / S2 −1.0 / S3 0.0 / S4 −0.5, mult 0.9, total −6.525, predicted down/mild. Actual: XLV −0.552%, SPY −0.599%, rel +0.047%. Direction HIT, magnitude HIT, mechanism MISS. Rates flat-to-marginally-higher (DGS10 +0.02, DFII10 0.00) — the 'duration drag' was a pre-existing condition, not a fresh catalyst. Metals co-move (Gold −0.56%, Silver −2.43%, Copper −2.89%) is market-wide, not healthcare-breadth. The morning's own S4 read conceded the 1d rel was stabilizing, yet S0/S1/S2 stayed at full negative weight."
error_category: "C"
scope: "general"
date: "2026-09-10"
status: "active"
occurrences: "1"
promoted_on: "2026-09-10"
sources: "['2026-09-10_sector_healthcare_lesson.md']"
schema_ok: "true"
---

## RULE
(1) When 1d rel has stabilized for ≥2 consecutive sessions after a multi-day lag, decay S0/S1/S2 toward zero — cap the combined S0+S1+S2 at ≈ −1.5, not −3.0. (2) Broaden the double-count check: if S0, S1, and S2 all trace to one root cause, they are not independent and the sum must be discounted, not merely checked pairwise (oil-vs-rotation). (3) S2 (breadth) must score only sector-internal breadth (constituent dispersion, sub-industry divergence); cross-asset co-moves (metals complex) belong in S0 or nowhere — scoring them in S2 is a category error. (4) Treat a third consecutive relative stabilization as an exhaustion signal that pulls the leading sum toward zero, not just a magnitude cap on S4.

## WHEN IT FIRES
A sector ETF has printed ≥2 consecutive sessions of relative stabilization (|1d rel| ≤ ~0.15%, sign flipping around zero) after a multi-day relative lag, AND the leading negative sum is built from S0/S1/S2 components that all trace to the SAME root cause (one macro shock → unwind → rotation-out → breadth failure). The model treats the stabilization as a magnitude cap (S4 only) while leaving S0/S1/S2 at full negative weight, so the compounded sum overstates the expected move and the mechanism (relative underperformance) never fires.

## WRONG IF
If on a future day with ≥2 consecutive 1d rel stabilizations after a multi-day lag, the sector ETF subsequently prints rel ≤ −0.5% (clear relative underperformance) with S0/S1/S2 all tracing to one root cause, then the "decay the leading sum" correction is wrong and the full-weight stacking was justified. Conversely, if the sector prints rel ≥ +0.3% (relative outperformance) on such a day, the correction is confirmed.

## EVIDENCE
Morning: S0 −1.0 / S1 −1.0 / S2 −1.0 / S3 0.0 / S4 −0.5, mult 0.9, total −6.525, predicted down/mild. Actual: XLV −0.552%, SPY −0.599%, rel +0.047%. Direction HIT, magnitude HIT, mechanism MISS. Rates flat-to-marginally-higher (DGS10 +0.02, DFII10 0.00) — the "duration drag" was a pre-existing condition, not a fresh catalyst. Metals co-move (Gold −0.56%, Silver −2.43%, Copper −2.89%) is market-wide, not healthcare-breadth. The morning's own S4 read conceded the 1d rel was stabilizing, yet S0/S1/S2 stayed at full negative weight.

(learn_cycle promote)
