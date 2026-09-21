---
trigger_pattern: "A sector card whose leading S0–S4 components are unanimously and correctly signed (all positive, no divergence), with an independently confirming index future (NQ ≥ +0.5% vs prior cash), a green sector premarket print, a uniformly positive 4-horizon relative tape, AND a live same-session sector catalyst — yet the published call is flat/flat because (a) the deterministic pipeline applies a `sector_rs_veto` built on stale relative-tape numbers that contradict the injected Channel 1 tape, and (b) a gap-day band rule (09-14 'PM gap is direction, not a notable extrapolant') is applied to a trend day."
corrected_behavior: "(1) When the injected Channel 1 relative tape and the pipeline's `sector_rs_tape` disagree in sign, the injected tape wins and the RS veto must be suppressed — a veto built on numbers that contradict the trusted feed is not a veto. (2) Scope the 09-14 'PM gap is direction, not a notable extrapolant' rule to gap days only; on a trend day (PM green, NQ ≥ +0.5%, 4-horizon rel uniformly green, live same-session sector catalyst), the band must be permitted to reach notable. (3) When the LLM overlay is unanimously signed and the narrative states a direction, the deterministic engine must not publish the opposite/neutral direction without an explicit, logged override reason."
falsifier: "If a future session shows the injected Channel 1 relative tape and the pipeline `sector_rs_tape` agreeing in sign, AND the card is a genuine gap day (PM green but NQ < +0.5% or 4-horizon rel mixed), AND the published flat/flat call is correct — then this lesson's mechanism is not the binding constraint and should be re-examined."
current_behavior: "The engine's `sector_rs_tape {d1: -2.05, w1: -2.07}` (contradicting injected 1d rel +0.69% / 1w rel +1.12%) triggers `sector_rs_veto_applied: True`, and `calendar_size_gate_applied: True`; the LLM overlay (raw 6.075, all channels positive) is arithmetically flattened to `predicted_direction: flat, predicted_magnitude_band: flat` despite the narrative explicitly stating 'Direction: up ... Magnitude: mild' and invoking the 09-16/09-17/09-18 lessons that forbid publishing flat against confirming NQ."
evidence_cited: "Pipeline JSON: `sector_rs_veto_applied: True, sector_rs_tape: {d1: -2.05, w1: -2.07}` vs injected Channel 1 `1d rel +0.69%, 1w rel +1.12%`; `predicted_direction: flat` vs narrative 'Direction: up ... Magnitude: mild'; actual XLK +2.764% / SPY +1.552% / rel +1.212%; 4th consecutive flat/flat miss in this scope (09-16, 09-17, 09-18, 09-21), all same-direction."
error_category: "D"
scope: "ops"
date: "2026-09-21"
status: "active"
occurrences: "1"
promoted_on: "2026-09-21"
sources: "['2026-09-21_sector_technology_lesson.md']"
schema_ok: "true"
---

## RULE
(1) When the injected Channel 1 relative tape and the pipeline's `sector_rs_tape` disagree in sign, the injected tape wins and the RS veto must be suppressed — a veto built on numbers that contradict the trusted feed is not a veto. (2) Scope the 09-14 "PM gap is direction, not a notable extrapolant" rule to gap days only; on a trend day (PM green, NQ ≥ +0.5%, 4-horizon rel uniformly green, live same-session sector catalyst), the band must be permitted to reach notable. (3) When the LLM overlay is unanimously signed and the narrative states a direction, the deterministic engine must not publish the opposite/neutral direction without an explicit, logged override reason.

## WHEN IT FIRES
A sector card whose leading S0–S4 components are unanimously and correctly signed (all positive, no divergence), with an independently confirming index future (NQ ≥ +0.5% vs prior cash), a green sector premarket print, a uniformly positive 4-horizon relative tape, AND a live same-session sector catalyst — yet the published call is flat/flat because (a) the deterministic pipeline applies a `sector_rs_veto` built on stale relative-tape numbers that contradict the injected Channel 1 tape, and (b) a gap-day band rule (09-14 "PM gap is direction, not a notable extrapolant") is applied to a trend day.

## WRONG IF
If a future session shows the injected Channel 1 relative tape and the pipeline `sector_rs_tape` agreeing in sign, AND the card is a genuine gap day (PM green but NQ < +0.5% or 4-horizon rel mixed), AND the published flat/flat call is correct — then this lesson's mechanism is not the binding constraint and should be re-examined.

## EVIDENCE
Pipeline JSON: `sector_rs_veto_applied: True, sector_rs_tape: {d1: -2.05, w1: -2.07}` vs injected Channel 1 `1d rel +0.69%, 1w rel +1.12%`; `predicted_direction: flat` vs narrative "Direction: up ... Magnitude: mild"; actual XLK +2.764% / SPY +1.552% / rel +1.212%; 4th consecutive flat/flat miss in this scope (09-16, 09-17, 09-18, 09-21), all same-direction.

(learn_cycle promote)
