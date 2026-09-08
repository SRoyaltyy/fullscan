---
trigger_pattern: "When a live oil/geopolitical supply shock is present at the open (WTI +3% toward $94-100) AND the sector ETF is a crowded-long, high-duration defensive book (XLV with 1m rel +1.71%, biotech sleeve with duration sensitivity), the model applies the 08-17 lesson (oil/geo risk-off → XLV defensive relative bid) without checking whether the oil price level and the sector's positioning regime make the shock an inflation/stagflation negative rather than a flight-to-safety bid. The model must distinguish between (a) a fresh oil shock from low levels that triggers a defensive rotation INTO healthcare, versus (b) an incremental shock at already-elevated oil prices ($94-100) that reads as an inflation scare, hitting the crowded-long, duration-sensitive sector as an unwind candidate."
corrected_behavior: "When oil is already elevated ($90+ WTI) and the sector is crowded-long with a persistent multi-day relative lag, score S0 = −0.5 to −1.0 (oil spike = inflation/duration negative for healthcare's biotech sleeve, not a defensive bid), score S3 = −0.5 (crowded-long is an accelerant to the downside in risk-off, not a mild dampener), and score S4 = −0.5 to −1.0 (the multi-day lag is momentum/ongoing rotation-out, not a stale print — the 08-28 'don't copy lag' lesson only applies when a fresh offsetting bid is actually present and validated, not when the offsetting thesis is speculative). The 08-17 lesson must be conditioned on oil price level and sector positioning: it fires only when the shock is from low levels and the sector is not already extended."
falsifier: "A future session where oil spikes from low levels (<$80 WTI) with XLV not crowded-long (1m rel < +1%) and XLV outperforms SPY — this would confirm the 08-17 lesson still fires in the right regime and this lesson is properly scoped to elevated-oil/crowded-long conditions."
current_behavior: "Scores S0 = +0.5 for oil/geo risk-off as a defensive relative bid for XLV, treats the 1d/3d/1w relative lag as 'completed' (08-28 lesson), scores S2/S3/S4 = 0, and predicts flat/flat with 0.9 multiplier — missing that the oil spike at elevated levels + crowded-long positioning + persistent multi-day lag = accelerant to the downside."
evidence_cited: "XLV fell −2.52% vs SPY −0.55% (rel −1.97%) on 2026-09-08 despite the morning's S0 = +0.5 defensive bid thesis. The 1d/3d/1w lag (rel −2.08%, −3.59%, −1.97%) continued and accelerated — the rotation-out was live momentum, not a completed print. The oil spike at $94.4 WTI / $99.1 Brent was an inflation/stagflation signal that hit the crowded-long (1m rel +1.71%) duration-sensitive sector as an unwind, not a safe-haven bid."
error_category: "A"
scope: "general"
date: "2026-09-08"
status: "active"
occurrences: "1"
promoted_on: "2026-09-08"
sources: "['2026-09-08_sector_healthcare_lesson.md']"
schema_ok: "true"
---

## RULE
When oil is already elevated ($90+ WTI) and the sector is crowded-long with a persistent multi-day relative lag, score S0 = −0.5 to −1.0 (oil spike = inflation/duration negative for healthcare's biotech sleeve, not a defensive bid), score S3 = −0.5 (crowded-long is an accelerant to the downside in risk-off, not a mild dampener), and score S4 = −0.5 to −1.0 (the multi-day lag is momentum/ongoing rotation-out, not a stale print — the 08-28 "don't copy lag" lesson only applies when a fresh offsetting bid is actually present and validated, not when the offsetting thesis is speculative). The 08-17 lesson must be conditioned on oil price level and sector positioning: it fires only when the shock is from low levels and the sector is not already extended.

## WHEN IT FIRES
When a live oil/geopolitical supply shock is present at the open (WTI +3% toward $94-100) AND the sector ETF is a crowded-long, high-duration defensive book (XLV with 1m rel +1.71%, biotech sleeve with duration sensitivity), the model applies the 08-17 lesson (oil/geo risk-off → XLV defensive relative bid) without checking whether the oil price level and the sector's positioning regime make the shock an inflation/stagflation negative rather than a flight-to-safety bid. The model must distinguish between (a) a fresh oil shock from low levels that triggers a defensive rotation INTO healthcare, versus (b) an incremental shock at already-elevated oil prices ($94-100) that reads as an inflation scare, hitting the crowded-long, duration-sensitive sector as an unwind candidate.

## WRONG IF
A future session where oil spikes from low levels (<$80 WTI) with XLV not crowded-long (1m rel < +1%) and XLV outperforms SPY — this would confirm the 08-17 lesson still fires in the right regime and this lesson is properly scoped to elevated-oil/crowded-long conditions.

## EVIDENCE
XLV fell −2.52% vs SPY −0.55% (rel −1.97%) on 2026-09-08 despite the morning's S0 = +0.5 defensive bid thesis. The 1d/3d/1w lag (rel −2.08%, −3.59%, −1.97%) continued and accelerated — the rotation-out was live momentum, not a completed print. The oil spike at $94.4 WTI / $99.1 Brent was an inflation/stagflation signal that hit the crowded-long (1m rel +1.71%) duration-sensitive sector as an unwind, not a safe-haven bid.

(learn_cycle promote)
