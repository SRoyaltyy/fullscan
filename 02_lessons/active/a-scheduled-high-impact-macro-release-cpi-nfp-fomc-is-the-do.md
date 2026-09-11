---
trigger_pattern: "A scheduled high-impact macro release (CPI/NFP/FOMC) is the dominant catalyst and is still pending at the open, while US index futures independently confirm a directional move of ≥+0.5% (ES and/or NQ), and the LLM narrative lean agrees with the futures direction but the deterministic pipeline total maps to flat."
corrected_behavior: "Add a hard direction gate at emit time: if a scheduled high-impact macro binary is pending AND |B6| ≥ +0.5% (ES or NQ independently confirming), the emitted predicted_direction MUST match the sign of B6, with magnitude capped at mild unless a fresh same-day catalyst or a |B6| ≥ 1.0% move justifies notable. The pipeline may not emit flat against confirming futures. Additionally, re-score B4: VIX term-structure backwardation is a stress *level* signal, not a directional *day* signal — do not score it negative when the same-morning tape shows volatility already collapsing; score B4 = 0 unless VIX is rising intraday."
falsifier: "see above."
current_behavior: "The pipeline emitted predicted_direction=flat / magnitude=flat (total_score 0.5) despite ES +0.63% / NQ +0.66% independently confirming up and the narrative leaning UP/MILD; the aggregation discarded the futures confirmation and the narrative lean. Separately, B4 scored VIX backwardation (1.115) as −0.5 when it resolved into a −12.22% volatility collapse and a contango flip (0.852) — a positive."
evidence_cited: "2026-09-11 predicted flat/flat (total 0.5, B6 +0.5, leading_sum +1.0, divergence_flagged False) vs actual SPX +0.86% (up/mild) — direction MISS, magnitude MISS. Morning narrative explicitly leaned UP/MILD; pipeline emitted flat. B4 scored −0.5 on backwardation 1.115; actual VIX closed 15.84 (−12.22%) with VIX/VIX3M flipping to 0.852 contango. Backward test: the same gate would have HIT on 09-03 (flat→up/mild, actual +1.06%) and been silent on 09-04 (futures weakening, gate not fired, actual −0.38%)."
error_category: "B"
scope: "general"
date: "2026-09-11"
status: "active"
occurrences: "1"
promoted_on: "2026-09-11"
sources: "['2026-09-11_lesson.md']"
schema_ok: "true"
---

## RULE
Add a hard direction gate at emit time: if a scheduled high-impact macro binary is pending AND |B6| ≥ +0.5% (ES or NQ independently confirming), the emitted predicted_direction MUST match the sign of B6, with magnitude capped at mild unless a fresh same-day catalyst or a |B6| ≥ 1.0% move justifies notable. The pipeline may not emit flat against confirming futures. Additionally, re-score B4: VIX term-structure backwardation is a stress *level* signal, not a directional *day* signal — do not score it negative when the same-morning tape shows volatility already collapsing; score B4 = 0 unless VIX is rising intraday.

## WHEN IT FIRES
A scheduled high-impact macro release (CPI/NFP/FOMC) is the dominant catalyst and is still pending at the open, while US index futures independently confirm a directional move of ≥+0.5% (ES and/or NQ), and the LLM narrative lean agrees with the futures direction but the deterministic pipeline total maps to flat.

## WRONG IF
see above.

## EVIDENCE
2026-09-11 predicted flat/flat (total 0.5, B6 +0.5, leading_sum +1.0, divergence_flagged False) vs actual SPX +0.86% (up/mild) — direction MISS, magnitude MISS. Morning narrative explicitly leaned UP/MILD; pipeline emitted flat. B4 scored −0.5 on backwardation 1.115; actual VIX closed 15.84 (−12.22%) with VIX/VIX3M flipping to 0.852 contango. Backward test: the same gate would have HIT on 09-03 (flat→up/mild, actual +1.06%) and been silent on 09-04 (futures weakening, gate not fired, actual −0.38%).

(learn_cycle promote)
