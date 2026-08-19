---
trigger_pattern: "A Technology/XLK down call has strongly negative leading components (S0/S1 at -2), broad semiconductor/foundry weakness visible premarket, sharply negative NQ futures, and the deterministic pipeline emits a severe band while the narrative text drifts to “notable.” When the official pipeline band is the scored output and is confirmed by the actual tape, the narrative undercall is an interpretability issue, not a scorecard error."
corrected_behavior: "When S0 and S1 are both strongly negative, NQ futures are independently confirming downside, and a broad high-beta sector driver (semiconductor/foundry/AI-capex fear) is active, the narrative should adopt the pipeline’s severe band unless a concrete offsetting sector composition or beta argument exists. Reconcile any narrative-vs-pipeline band mismatch explicitly before final output."
falsifier: "A future Technology case with the same setup — S0/S1 strongly negative, NQ futures ≤ -1.5%, broad foundry/semi selloff, pipeline severe — would be falsified if XLK closed at or above -1% or materially outperformed SPY on the day."
current_behavior: "The narrative can state a weaker magnitude band than the pipeline-computed deterministic output without explicit reconciliation. In this case the narrative said “notable” while the official pipeline said “severe”; the actual -2.47% XLK close proved the pipeline correct."
evidence_cited: "2026-08-18 predicted down/severe; actual XLK -2.47%, SPY -0.68%, rel -1.79%. Direction and magnitude both HIT. Key drivers: foundry/semi selloff (UMC -7%, Tower -10%, GFS -7%), Nvidia $105B OpenAI circular-financing concerns, bond yields at multi-year peaks, and Iran ceasefire expiry. The pipeline’s severe band was correct; the narrative’s notable undercall did not affect the scored outcome."
error_category: "NONE"
scope: "general"
date: "2026-08-18"
status: "active"
occurrences: "1"
promoted_on: "2026-08-19"
sources: "['2026-08-18_sector_technology_lesson.md']"
schema_ok: "true"
---

## RULE
When S0 and S1 are both strongly negative, NQ futures are independently confirming downside, and a broad high-beta sector driver (semiconductor/foundry/AI-capex fear) is active, the narrative should adopt the pipeline’s severe band unless a concrete offsetting sector composition or beta argument exists. Reconcile any narrative-vs-pipeline band mismatch explicitly before final output.

## WHEN IT FIRES
A Technology/XLK down call has strongly negative leading components (S0/S1 at -2), broad semiconductor/foundry weakness visible premarket, sharply negative NQ futures, and the deterministic pipeline emits a severe band while the narrative text drifts to “notable.” When the official pipeline band is the scored output and is confirmed by the actual tape, the narrative undercall is an interpretability issue, not a scorecard error.

## WRONG IF
A future Technology case with the same setup — S0/S1 strongly negative, NQ futures ≤ -1.5%, broad foundry/semi selloff, pipeline severe — would be falsified if XLK closed at or above -1% or materially outperformed SPY on the day.

## EVIDENCE
2026-08-18 predicted down/severe; actual XLK -2.47%, SPY -0.68%, rel -1.79%. Direction and magnitude both HIT. Key drivers: foundry/semi selloff (UMC -7%, Tower -10%, GFS -7%), Nvidia $105B OpenAI circular-financing concerns, bond yields at multi-year peaks, and Iran ceasefire expiry. The pipeline’s severe band was correct; the narrative’s notable undercall did not affect the scored outcome.

(learn_cycle promote)
