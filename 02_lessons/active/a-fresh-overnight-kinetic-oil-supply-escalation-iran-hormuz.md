---
trigger_pattern: "A fresh overnight kinetic/oil-supply escalation (Iran/Hormuz tanker attacks, Brent >$100) is present at the open with B1 scored at −3, but US index futures (B6) are flat within ±0.5% and do not independently confirm a ≥0.5% down move; the leading_sum is strongly negative (≤ −8) and divergence is flagged."
corrected_behavior: "When B1 is scored at −3 for a fresh kinetic/oil shock but B6 (futures) is flat within ±0.5% and does not confirm ≥0.5% down, FORCE the final magnitude band to MILD regardless of leading_sum magnitude. Operationalize as a hard gate: if |B6| < 0.5% and no same-day hard-data or mega-cap miss confirms, cap predicted_magnitude_band at mild and set multiplier ≤ 0.9. Keep B1=−3 for direction and conviction; the band is set separately by futures confirmation. Reconcile the narrative band with the pipeline band before emit — if the narrative says mild and the pipeline says severe, the pipeline band must be corrected to mild."
falsifier: "see above."
current_behavior: "The model scored B1=−3 (correct) but let the total reach −13.275 → severe band, treating the divergence flag as license for a severe magnitude. It named the 09-08/09-09 down/mild lessons in RULES_APPLIED but did not enforce their mild cap, and left the narrative ('DOWN / MILD') unreconciled with the pipeline (severe)."
evidence_cited: "2026-09-10 predicted down/severe (total −13.275, B1=−3, B6=0.0, divergence flagged); actual SPX −0.58% (down/mild). Direction HIT, magnitude MISS. The 09-08 (−0.58% mild), 09-09 (−0.48% mild), and 09-01 (−0.71% mild) fresh-kinetic days all closed mild with the same B6-flat signature — today is the outlier where the model escalated to severe."
error_category: "B"
scope: "general"
date: "2026-09-10"
status: "active"
occurrences: "1"
promoted_on: "2026-09-10"
sources: "['2026-09-10_lesson.md']"
schema_ok: "true"
---

## RULE
When B1 is scored at −3 for a fresh kinetic/oil shock but B6 (futures) is flat within ±0.5% and does not confirm ≥0.5% down, FORCE the final magnitude band to MILD regardless of leading_sum magnitude. Operationalize as a hard gate: if |B6| < 0.5% and no same-day hard-data or mega-cap miss confirms, cap predicted_magnitude_band at mild and set multiplier ≤ 0.9. Keep B1=−3 for direction and conviction; the band is set separately by futures confirmation. Reconcile the narrative band with the pipeline band before emit — if the narrative says mild and the pipeline says severe, the pipeline band must be corrected to mild.

## WHEN IT FIRES
A fresh overnight kinetic/oil-supply escalation (Iran/Hormuz tanker attacks, Brent >$100) is present at the open with B1 scored at −3, but US index futures (B6) are flat within ±0.5% and do not independently confirm a ≥0.5% down move; the leading_sum is strongly negative (≤ −8) and divergence is flagged.

## WRONG IF
see above.

## EVIDENCE
2026-09-10 predicted down/severe (total −13.275, B1=−3, B6=0.0, divergence flagged); actual SPX −0.58% (down/mild). Direction HIT, magnitude MISS. The 09-08 (−0.58% mild), 09-09 (−0.48% mild), and 09-01 (−0.71% mild) fresh-kinetic days all closed mild with the same B6-flat signature — today is the outlier where the model escalated to severe.

(learn_cycle promote)
