---
trigger_pattern: "A Technology/XLK session is scored as a pending binary de-risk day (scheduled 8:30 macro still due, top-weight mega-cap earnings still after the close) so the 08-12 up/notable gate is marked not met and magnitude is capped at mild, without verifying primary-source timestamps or the cash gap, when those events already printed prior session/AHR and the confirmed beat is the live AI-infra spine."
corrected_behavior: "Before applying the pending-binary / no-confirmed-beat mild cap, verify company IR/BEA timestamps and open vs prior close. If the top-weight print is already public and market-confirmed and NQ is green, fire 08-12: one AI-infra cluster in S1 at +2/+3, do not score S3 as unresolved event-risk, and allow notable. A gap already through the mild band is a magnitude floor. Do not upgrade to severe from post-hoc concentration. Sticky inflation that caps SPY does not veto XLK once the mega-cap beat is confirmed."
falsifier: "If top-weight AHR beat is already public, NQ ≥ ~+0.5%, ETF gapped past mild, and XLK still closes mild/flat on ≥2 of the next 3 such days, the notable requirement is too strong and must be revised. Also revise if timestamp verification still cannot surface the PR and notable would have been wrong."
current_behavior: "Treats the cash session as pre-PCE and pre-NVDA; parks NVDA as de-risk; scores S1 on pre-print tape (CoreWeave/Berkshire/ARM/upgrades) at +1; scores S3 crowding as event-risk supply (−1); applies 08-12 as not met; emits up/mild (mult 1.0)."
evidence_cited: "2026-08-27 predicted up/mild; XLK +3.16% vs SPY +0.66% (rel +2.50%), gap-and-go 186.47→188.61. NVDA reported 2026-08-26 AHR ($96.2B / $108B Q3 / ~70% FY28); Thursday NVDA ~+8.7%. PCE printed Wed 08:30 ET, not Thursday. Direction HIT, magnitude MISS. 08-12 was live at 9:30; the pre-print template was one session late."
error_category: "A"
scope: "general"
date: "2026-08-27"
status: "active"
occurrences: "1"
promoted_on: "2026-08-28"
sources: "['2026-08-27_sector_technology_lesson.md']"
schema_ok: "true"
---

## RULE
Before applying the pending-binary / no-confirmed-beat mild cap, verify company IR/BEA timestamps and open vs prior close. If the top-weight print is already public and market-confirmed and NQ is green, fire 08-12: one AI-infra cluster in S1 at +2/+3, do not score S3 as unresolved event-risk, and allow notable. A gap already through the mild band is a magnitude floor. Do not upgrade to severe from post-hoc concentration. Sticky inflation that caps SPY does not veto XLK once the mega-cap beat is confirmed.

## WHEN IT FIRES
A Technology/XLK session is scored as a pending binary de-risk day (scheduled 8:30 macro still due, top-weight mega-cap earnings still after the close) so the 08-12 up/notable gate is marked not met and magnitude is capped at mild, without verifying primary-source timestamps or the cash gap, when those events already printed prior session/AHR and the confirmed beat is the live AI-infra spine.

## WRONG IF
If top-weight AHR beat is already public, NQ ≥ ~+0.5%, ETF gapped past mild, and XLK still closes mild/flat on ≥2 of the next 3 such days, the notable requirement is too strong and must be revised. Also revise if timestamp verification still cannot surface the PR and notable would have been wrong.

## EVIDENCE
2026-08-27 predicted up/mild; XLK +3.16% vs SPY +0.66% (rel +2.50%), gap-and-go 186.47→188.61. NVDA reported 2026-08-26 AHR ($96.2B / $108B Q3 / ~70% FY28); Thursday NVDA ~+8.7%. PCE printed Wed 08:30 ET, not Thursday. Direction HIT, magnitude MISS. 08-12 was live at 9:30; the pre-print template was one session late.

(learn_cycle promote)
