---
trigger_pattern: "A Consumer Cyclical down call is driven by a genuinely negative macro spine (geopolitical/oil risk-off, negative futures, weak retail/sales/sentiment data) but the model fails to scan for knowable sector-level offsets: a positive pre-market catalyst in a major holding (Home Depot Q2 beat), defensive rotation into discretionary on a tech-led selloff, and the concentrated ETF’s largest weights (AMZN/TSLA) trading flat/mildly down. The model converts strong negative S0/S1 into severe without checking whether the selloff’s composition actually hits XLY’s dominant holdings."
corrected_behavior: "Before selecting severe for XLY, decompose the risk-off by leadership (tech/semis vs broad) and inventory knowable same-morning sector positives among top holdings. If the selloff is led by tech/semis, a top-holding catalyst is positive pre-market (HD beat, McDonald’s strength), and AMZN/TSLA are roughly flat, cap the band to down/notable or down/mild. Severe requires the negative catalyst to hit the ETF’s dominant weights broadly, or the sector’s own leaders to break down."
falsifier: "If, on a future trigger day with a positive pre-market top-holding beat (HD/WMT/TGT), a tech-led selloff, and AMZN/TSLA flat at the open, XLY still closes at or below roughly -1.2% (severe), the offset/composition check would be falsified. Conversely, if XLY only reaches severe when the selloff is broad-based and its top holdings are also down, the rule is confirmed."
current_behavior: "After applying the active Consumer Cyclical oil-shock lesson, the model sets S0 = -2 and S1 = -2, double-counts the oil shock, and on convergence with negative futures escalates the official magnitude to severe. It underweights the Home Depot beat as a “mild offset” and treats XLY’s mega-cap concentration as a pure breadth negative, ignoring that flat AMZN/TSLA can buffer the ETF."
evidence_cited: "Predicted down/severe; actual XLY -0.33%, SPY -0.68%, XLY relative +0.34%. Home Depot beat Q2 pre-market (EPS $4.92 vs $4.73, comps +1.7%) and shares rose >2.5%. The selloff was tech/semis-led (Nvidia -5.07%), while Amazon was roughly flat and Tesla only -0.89%, cushioning XLY’s ~42% AMZN/TSLA weight. The HD beat and defensive-rotation dynamic were knowable at the open."
error_category: "B"
scope: "general"
date: "2026-08-18"
status: "active"
occurrences: "1"
promoted_on: "2026-08-19"
sources: "['2026-08-18_sector_consumer_cyclical_lesson.md']"
schema_ok: "true"
---

## RULE
Before selecting severe for XLY, decompose the risk-off by leadership (tech/semis vs broad) and inventory knowable same-morning sector positives among top holdings. If the selloff is led by tech/semis, a top-holding catalyst is positive pre-market (HD beat, McDonald’s strength), and AMZN/TSLA are roughly flat, cap the band to down/notable or down/mild. Severe requires the negative catalyst to hit the ETF’s dominant weights broadly, or the sector’s own leaders to break down.

## WHEN IT FIRES
A Consumer Cyclical down call is driven by a genuinely negative macro spine (geopolitical/oil risk-off, negative futures, weak retail/sales/sentiment data) but the model fails to scan for knowable sector-level offsets: a positive pre-market catalyst in a major holding (Home Depot Q2 beat), defensive rotation into discretionary on a tech-led selloff, and the concentrated ETF’s largest weights (AMZN/TSLA) trading flat/mildly down. The model converts strong negative S0/S1 into severe without checking whether the selloff’s composition actually hits XLY’s dominant holdings.

## WRONG IF
If, on a future trigger day with a positive pre-market top-holding beat (HD/WMT/TGT), a tech-led selloff, and AMZN/TSLA flat at the open, XLY still closes at or below roughly -1.2% (severe), the offset/composition check would be falsified. Conversely, if XLY only reaches severe when the selloff is broad-based and its top holdings are also down, the rule is confirmed.

## EVIDENCE
Predicted down/severe; actual XLY -0.33%, SPY -0.68%, XLY relative +0.34%. Home Depot beat Q2 pre-market (EPS $4.92 vs $4.73, comps +1.7%) and shares rose >2.5%. The selloff was tech/semis-led (Nvidia -5.07%), while Amazon was roughly flat and Tesla only -0.89%, cushioning XLY’s ~42% AMZN/TSLA weight. The HD beat and defensive-rotation dynamic were knowable at the open.

(learn_cycle promote)
