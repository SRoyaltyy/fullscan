---
trigger_pattern: "A macro headwind (hawkish Fed minutes, rate-hike risk) was released 1–2 sessions earlier and is already reflected in the prior week tape, while the same-morning reversal checklist is positive: US index futures ≥ +0.3%, real yields easing, oil off highs. The model runs the reversal checklist but still lets the stale macro headline keep S0 negative, producing down/flat and missing a rebound."
corrected_behavior: "When a macro headwind is already priced over the prior 1–2 sessions AND the morning reversal checklist is positive, score S0 at 0 or positive, not -1, unless a fresh same-morning shock has appeared. A non-fresh legal/regulatory overhang should keep S1 at 0 and must not convert the call to down. The output should be flat/up, not down/flat."
falsifier: "If the same setup occurs — positive futures, easing real yields, priced macro headwind — but a fresh same-morning negative data point (hot CPI, negative mega-cap premarket move, new adverse ruling) drives the sector down, the corrected behavior would fail. Also, repeated positive-reversal mornings that resolve lower without a fresh shock would invalidate the rule."
current_behavior: "The reversal signals are acknowledged in the narrative but treated only as magnitude caps, not as direction-flip evidence. The deterministic pipeline then emits a down direction because S0 remains negative from a macro catalyst that is no longer fresh."
evidence_cited: "2026-08-21 Communication Services: official pipeline predicted down/flat; actual XLC +0.65% vs SPY +0.41%, rel +0.24%. The morning itself cited ES +0.35%, NQ +0.49%, DFII10 -0.06 1d, CL=F -1.15%, and “partially priced” hawkish minutes, yet scored S0 = -1. Outcome was a broad rebound; Meta rose as investors looked past the trial overhang."
error_category: "A"
scope: "general"
date: "2026-08-21"
status: "active"
occurrences: "1"
promoted_on: "2026-08-27"
sources: "['2026-08-21_sector_communication_services_lesson.md']"
schema_ok: "true"
---

## RULE
When a macro headwind is already priced over the prior 1–2 sessions AND the morning reversal checklist is positive, score S0 at 0 or positive, not -1, unless a fresh same-morning shock has appeared. A non-fresh legal/regulatory overhang should keep S1 at 0 and must not convert the call to down. The output should be flat/up, not down/flat.

## WHEN IT FIRES
A macro headwind (hawkish Fed minutes, rate-hike risk) was released 1–2 sessions earlier and is already reflected in the prior week tape, while the same-morning reversal checklist is positive: US index futures ≥ +0.3%, real yields easing, oil off highs. The model runs the reversal checklist but still lets the stale macro headline keep S0 negative, producing down/flat and missing a rebound.

## WRONG IF
If the same setup occurs — positive futures, easing real yields, priced macro headwind — but a fresh same-morning negative data point (hot CPI, negative mega-cap premarket move, new adverse ruling) drives the sector down, the corrected behavior would fail. Also, repeated positive-reversal mornings that resolve lower without a fresh shock would invalidate the rule.

## EVIDENCE
2026-08-21 Communication Services: official pipeline predicted down/flat; actual XLC +0.65% vs SPY +0.41%, rel +0.24%. The morning itself cited ES +0.35%, NQ +0.49%, DFII10 -0.06 1d, CL=F -1.15%, and “partially priced” hawkish minutes, yet scored S0 = -1. Outcome was a broad rebound; Meta rose as investors looked past the trial overhang.

(learn_cycle promote)
