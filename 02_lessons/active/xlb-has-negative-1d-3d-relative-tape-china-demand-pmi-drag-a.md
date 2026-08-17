---
trigger_pattern: "XLB has negative 1d/3d relative tape, China demand/PMI drag, and copper is off its recent peak, but gold/silver are green, USD is weak, and the broad equity index is extended near record highs after a long run. The model treats the monetary-metals/commodity bid as a mere dampener, scores the industrial-demand drag multiple times, and emits a confident down/notable call by extrapolating the prior day’s underperformance."
corrected_behavior: "If gold/silver futures are green and USD is weakening, do not downgrade the monetary-metals bid to “dampener only.” Score the firm metals bid as a positive/neutral S1 offset, and do not build all five components negative from the prior day’s relative tape. When the broad index is at/near record highs and futures are flat or only mildly positive, treat a tech-led risk-off rotation into commodity-linked materials as a live scenario; cap the call at flat/up/mild or low-conviction down/mild, not down/notable."
falsifier: "The rule is falsified if, in a future setup with negative XLB relative tape, China demand contraction, copper off highs, but gold positive and SPY near record highs, XLB still falls or underperforms SPY by more than ~1%. That would show the risk-off rotation mechanism is not reliable enough to override the down-side industrial signals."
current_behavior: "When the industrial-metals spine is negative (China PMI contraction, copper fading, prior negative XLB relative tape), the model systematically refuses to let the gold/silver bid offset it. It sets S0/S1/S2/S3/S4 all negative, double-counts the same China/copper-fade thesis in multiple components, and produces down/notable even when the same-day metals tape is firm."
evidence_cited: "2026-08-14 predicted down/notable (total -9.0); actual XLB +0.44% vs SPY -0.20%, relative +0.64%. Gold was +0.67%, silver >$65, copper near records; money rotated out of tech and into materials. The morning had gold green and DXY -0.23%, so a firm metals tape was knowable at the open. The outcome was a direction miss and a magnitude miss."
error_category: "A"
scope: "general"
date: "2026-08-14"
status: "active"
occurrences: "1"
promoted_on: "2026-08-17"
sources: "['2026-08-14_sector_basic_materials_lesson.md']"
schema_ok: "true"
---

## RULE
If gold/silver futures are green and USD is weakening, do not downgrade the monetary-metals bid to “dampener only.” Score the firm metals bid as a positive/neutral S1 offset, and do not build all five components negative from the prior day’s relative tape. When the broad index is at/near record highs and futures are flat or only mildly positive, treat a tech-led risk-off rotation into commodity-linked materials as a live scenario; cap the call at flat/up/mild or low-conviction down/mild, not down/notable.

## WHEN IT FIRES
XLB has negative 1d/3d relative tape, China demand/PMI drag, and copper is off its recent peak, but gold/silver are green, USD is weak, and the broad equity index is extended near record highs after a long run. The model treats the monetary-metals/commodity bid as a mere dampener, scores the industrial-demand drag multiple times, and emits a confident down/notable call by extrapolating the prior day’s underperformance.

## WRONG IF
The rule is falsified if, in a future setup with negative XLB relative tape, China demand contraction, copper off highs, but gold positive and SPY near record highs, XLB still falls or underperforms SPY by more than ~1%. That would show the risk-off rotation mechanism is not reliable enough to override the down-side industrial signals.

## EVIDENCE
2026-08-14 predicted down/notable (total -9.0); actual XLB +0.44% vs SPY -0.20%, relative +0.64%. Gold was +0.67%, silver >$65, copper near records; money rotated out of tech and into materials. The morning had gold green and DXY -0.23%, so a firm metals tape was knowable at the open. The outcome was a direction miss and a magnitude miss.

(learn_cycle promote)
