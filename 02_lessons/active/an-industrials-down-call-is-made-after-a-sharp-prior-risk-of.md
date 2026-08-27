---
trigger_pattern: "An Industrials down call is made after a sharp prior risk-off day (e.g., a large Dow decline) when the same-morning reversal checklist is positive — US index futures ≥ +0.3%, no fresh same-day negative hard-data/earnings catalyst, and the macro negatives (hawkish Fed minutes, elevated yields) are one to two sessions old and already absorbed in the prior tape — while the medium-term sector tape is negative. The model weights the continuing macro overlay and negative 1w/1m relative tape over the knowable futures bounce and single-name positives, producing an absolute down call when the sector closes up but underperforms SPY."
corrected_behavior: "When the reversal checklist is positive in a cyclical sector (ES/NQ ≥ +0.3%, no fresh same-day negative catalyst), do not let stale macro headlines set absolute direction. Treat the macro drag as a relative-performance signal, not an absolute-down signal. The direction should be non-negative — flat/up with mild magnitude — especially when knowable sector-specific positives exist. To use the 08-18 hard-data-miss/down-tape lesson, require both a fresh hard-data miss and a down/negative-futures tape on the decision day; otherwise do not invoke its down-bias branch."
falsifier: "If, after a sharp risk-off day, with ES/NQ ≥ +0.3%, no fresh same-day negative catalyst, and XLI still closes down while SPY closes up (absolute loss, not just relative underperformance), then the 'positive reversal dominates absolute direction' rule is falsified for that instance. If that happens repeatedly in the next 10 Industrials runs, the lesson should be downgraded to relative-only."
current_behavior: "Emits down/mild on a -3.15 score. S0 is held at -2 on stale hawkish-Fed/rising-yield commentary; the 08-18 hard-data-miss lesson is applied to cap S1 at +1; the positive futures bounce is labelled a 'recovery attempt, not confirmed risk-on'; single-name positives (CNH +8.3% guidance, GE Aerospace PT raise) are noted but do not move direction. Result: absolute down call misses XLI +0.27%, though it correctly expects relative underperformance (-0.14% vs SPY)."
evidence_cited: "2026-08-21: XLI +0.267%, SPY +0.409%, rel -0.142%; futures ES +0.35%, NQ +0.49%; no fresh hard-data/earnings miss; CNH guided 2026 outlook to high end (+8.3%); JPM raised GE Aerospace PT to $400; XLI gapped up and faded but closed green. Scoreboard: direction_hit False, magnitude_hit False, predicted down/mild vs actual +0.267%."
error_category: "A"
scope: "general"
date: "2026-08-21"
status: "active"
occurrences: "1"
promoted_on: "2026-08-27"
sources: "['2026-08-21_sector_industrials_lesson.md']"
schema_ok: "true"
---

## RULE
When the reversal checklist is positive in a cyclical sector (ES/NQ ≥ +0.3%, no fresh same-day negative catalyst), do not let stale macro headlines set absolute direction. Treat the macro drag as a relative-performance signal, not an absolute-down signal. The direction should be non-negative — flat/up with mild magnitude — especially when knowable sector-specific positives exist. To use the 08-18 hard-data-miss/down-tape lesson, require both a fresh hard-data miss and a down/negative-futures tape on the decision day; otherwise do not invoke its down-bias branch.

## WHEN IT FIRES
An Industrials down call is made after a sharp prior risk-off day (e.g., a large Dow decline) when the same-morning reversal checklist is positive — US index futures ≥ +0.3%, no fresh same-day negative hard-data/earnings catalyst, and the macro negatives (hawkish Fed minutes, elevated yields) are one to two sessions old and already absorbed in the prior tape — while the medium-term sector tape is negative. The model weights the continuing macro overlay and negative 1w/1m relative tape over the knowable futures bounce and single-name positives, producing an absolute down call when the sector closes up but underperforms SPY.

## WRONG IF
If, after a sharp risk-off day, with ES/NQ ≥ +0.3%, no fresh same-day negative catalyst, and XLI still closes down while SPY closes up (absolute loss, not just relative underperformance), then the "positive reversal dominates absolute direction" rule is falsified for that instance. If that happens repeatedly in the next 10 Industrials runs, the lesson should be downgraded to relative-only.

## EVIDENCE
2026-08-21: XLI +0.267%, SPY +0.409%, rel -0.142%; futures ES +0.35%, NQ +0.49%; no fresh hard-data/earnings miss; CNH guided 2026 outlook to high end (+8.3%); JPM raised GE Aerospace PT to $400; XLI gapped up and faded but closed green. Scoreboard: direction_hit False, magnitude_hit False, predicted down/mild vs actual +0.267%.

(learn_cycle promote)
