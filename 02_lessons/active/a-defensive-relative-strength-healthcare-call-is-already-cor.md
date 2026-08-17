---
trigger_pattern: "A defensive/relative-strength Healthcare call is already correctly read as a reversal—negative 1d/3d relative tape plus a growth/tech-led tape unwinding the prior defensive bid—but S1 is left at 0 because the policy category was checked too superficially as “nothing material,” while a same-day drug-pricing/policy overhang is actually live in sector media. The result is a directionally correct but magnitude-understated down/flat call where the actual is down/mild."
corrected_behavior: "When a reversal call is already confirmed by S2/S4 tape, run a same-day audit of policy/regulatory headlines before scoring S1=0. If a live negative policy/pricing narrative targets mega-cap pharma/insurers (drug-cost executive orders, record price-drop claims, combination-shot cost warnings, etc.), score S1 at least -0.5 and select the mild band rather than flat. Use flat only if the policy audit is genuinely neutral."
falsifier: "If a future Healthcare call in the exact confirmed-reversal + live drug-pricing-overhang setup closes within ±0.3% (flat) or higher, the “policy overhang ⇒ mild-down” rule will be disconfirmed and should be softened back to flat-centering."
current_behavior: "When the reversal direction is clear, S1 is defaulted to 0 if no obvious catalyst was pre-fetched; the predicted band then settles on flat, producing direction_hit True but magnitude_hit False."
evidence_cited: "2026-08-14: predicted down/flat; actual XLV -0.60% vs SPY -0.20% (rel -0.40). Direction hit, magnitude missed. S1 was 0 despite same-day pharma-trade reporting that the combination-shot separation EO “will drive up drugmaker costs” and that drug prices recorded their biggest YoY drop; CNBC’s wrap also named healthcare as an explicit laggard. A -0.5 S1 would have shifted the output to the mild band and matched the actual."
error_category: "B"
scope: "general"
date: "2026-08-14"
status: "active"
occurrences: "1"
promoted_on: "2026-08-17"
sources: "['2026-08-14_sector_healthcare_lesson.md']"
schema_ok: "true"
---

## RULE
When a reversal call is already confirmed by S2/S4 tape, run a same-day audit of policy/regulatory headlines before scoring S1=0. If a live negative policy/pricing narrative targets mega-cap pharma/insurers (drug-cost executive orders, record price-drop claims, combination-shot cost warnings, etc.), score S1 at least -0.5 and select the mild band rather than flat. Use flat only if the policy audit is genuinely neutral.

## WHEN IT FIRES
A defensive/relative-strength Healthcare call is already correctly read as a reversal—negative 1d/3d relative tape plus a growth/tech-led tape unwinding the prior defensive bid—but S1 is left at 0 because the policy category was checked too superficially as “nothing material,” while a same-day drug-pricing/policy overhang is actually live in sector media. The result is a directionally correct but magnitude-understated down/flat call where the actual is down/mild.

## WRONG IF
If a future Healthcare call in the exact confirmed-reversal + live drug-pricing-overhang setup closes within ±0.3% (flat) or higher, the “policy overhang ⇒ mild-down” rule will be disconfirmed and should be softened back to flat-centering.

## EVIDENCE
2026-08-14: predicted down/flat; actual XLV -0.60% vs SPY -0.20% (rel -0.40). Direction hit, magnitude missed. S1 was 0 despite same-day pharma-trade reporting that the combination-shot separation EO “will drive up drugmaker costs” and that drug prices recorded their biggest YoY drop; CNBC’s wrap also named healthcare as an explicit laggard. A -0.5 S1 would have shifted the output to the mild band and matched the actual.

(learn_cycle promote)
