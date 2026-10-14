---
trigger_pattern: "Consumer Cyclical / XLY on a same-session unsigned CPI (or NFP/FOMC) open, leading S0–S4 all-zero, ES/NQ both inside ±0.5%, XLY PM dead-flat, leftover-anchor flatten binding in the card, while v2 still publishes official down/mild from index_carry; after the print, a hawkish core miss sells AMZN/TSLA duration and the carry label grades as a hit."
corrected_behavior: "Do not mint a new lesson from this hit. Keep the morning process: unsigned CPI in S0, live oil is not the CPI-gasoline clock, no AMZN/TSLA-as-wallet call, no leftover-anchor direction. Credit the hawkish print’s realization, not index_carry. Do not weaken 09-21 flatten or 10-02 “don’t pre-score CPI,” and do not add S1 gasoline on top of S0 (double-count)."
falsifier: "If someone proposed “on this pattern, official label should follow index_carry down/mild,” it is already falsified by 10-08 and 10-13 (carry-down vs small up). If someone proposed “always flatten the official label to flat here,” today is the falsifier (hawkish CPI → XLY −0.77% mild down). That two-way kill is why this is D / no new lesson."
current_behavior: "Channel 2 correctly refuses to pre-score the binary and writes flat; the graded object is still engine down/mild from general index_carry. Post-close it is tempting to treat that carry hit as a sector signal (or to rewrite S1 from live WTI relief into CPI gasoline)."
evidence_cited: "Predicted down/mild vs XLY −0.77% / SPY −1.08% / rel +0.32% (dir+mag HIT). LLM leading_sum 0.0, overlay 0.0, flatten bound; engine total −0.187 from index_carry −0.411 (general −1.645) + tape_anchor +0.224. CPI 8:30 ET: core 0.3/2.6 vs 0.2/2.5, gasoline +9.1% m/m. AMZN −1.7%, TSLA −2.2%, HD +0.54%, XLK −2.98%, XLP +2.22%. Path was gap-red after CPI then grind — not a pre-open discretionary wipeout. Rolling dir=0.3 / mag=0.2 (n=10) still a down/mild bias; 10-08 and 10-13 were carry-style down/mild vs small green."
error_category: "D"
scope: "ops"
date: "2026-10-14"
status: "active"
occurrences: "1"
promoted_on: "2026-10-14"
sources: "['2026-10-14_sector_consumer_cyclical_lesson.md']"
schema_ok: "true"
---

## RULE
Do not mint a new lesson from this hit. Keep the morning process: unsigned CPI in S0, live oil is not the CPI-gasoline clock, no AMZN/TSLA-as-wallet call, no leftover-anchor direction. Credit the hawkish print’s realization, not index_carry. Do not weaken 09-21 flatten or 10-02 “don’t pre-score CPI,” and do not add S1 gasoline on top of S0 (double-count).

## WHEN IT FIRES
Consumer Cyclical / XLY on a same-session unsigned CPI (or NFP/FOMC) open, leading S0–S4 all-zero, ES/NQ both inside ±0.5%, XLY PM dead-flat, leftover-anchor flatten binding in the card, while v2 still publishes official down/mild from index_carry; after the print, a hawkish core miss sells AMZN/TSLA duration and the carry label grades as a hit.

## WRONG IF
If someone proposed “on this pattern, official label should follow index_carry down/mild,” it is already falsified by 10-08 and 10-13 (carry-down vs small up). If someone proposed “always flatten the official label to flat here,” today is the falsifier (hawkish CPI → XLY −0.77% mild down). That two-way kill is why this is D / no new lesson.

## EVIDENCE
Predicted down/mild vs XLY −0.77% / SPY −1.08% / rel +0.32% (dir+mag HIT). LLM leading_sum 0.0, overlay 0.0, flatten bound; engine total −0.187 from index_carry −0.411 (general −1.645) + tape_anchor +0.224. CPI 8:30 ET: core 0.3/2.6 vs 0.2/2.5, gasoline +9.1% m/m. AMZN −1.7%, TSLA −2.2%, HD +0.54%, XLK −2.98%, XLP +2.22%. Path was gap-red after CPI then grind — not a pre-open discretionary wipeout. Rolling dir=0.3 / mag=0.2 (n=10) still a down/mild bias; 10-08 and 10-13 were carry-style down/mild vs small green.

(learn_cycle promote)
