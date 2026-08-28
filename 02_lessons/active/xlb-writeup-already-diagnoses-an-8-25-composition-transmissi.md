---
trigger_pattern: "XLB writeup already diagnoses an 8/25 composition/transmission setup (NQ >> ES, XLB 1d rel <0.5%, mixed premarket breadth, two-sided S1) and hand-scores flat, but the emitted/graded call is still pipeline up because S4 is scored positive from a sub-0.5% tape and/or leading_sum/total_score disagrees with the written S0–S4 components. Gold HIT is allowed to keep S1 at +1 even when copper is off the record, LME stocks have rebuilt, and China/property remains HIT."
corrected_behavior: "If 8/25 conditions hold, emitted predicted_direction cannot be up — use the writeup/hand-sum (flat/flat-mild), not a conflicting pipeline up. Cap S4 at 0 when 1d rel <0.5% (1m rel already negative is not confirmation). Net S1 at 0 when gold HIT is offset by fading copper + inventory rebuild + China HIT; gold remains an 8/14 sleeve credit, not a +1 wash. For chemicals-heavy XLB, carried sticky PCE + tech-led tape is a cyclical overlay (S0 ≤ 0, can be slightly negative); do not ding S0 merely because “JH” is on the calendar, but do not treat that rule as a ban on scoring the rate/cyclical headwind when NQ >> ES and LIN/SHW are not leading."
falsifier: "Next session where 8/25 conditions hold (NQ >> ES, XLB 1d rel <0.5%, mixed LIN/FCX/NEM/SHW, two-sided S1) and XLB still prints up with LIN/chemicals participating — then forbidding emitted-up and flattening S4/S1 is too strict. Separately, if pipeline leading_sum/total_score is shown to match the written components and still be up for a non-8/25 reason, this is not a pipeline-overwrite lesson."
current_behavior: "Treat 8/25 as a prose overlay, then ship the deterministic up/flat print. Score S4 = +0.5 on 1d rel +0.15%, keep S1 = +1 on the gold sleeve, leave S0 at 0 because “Jackson Hole is two-sided,” and let a mismatched pipeline total (leading_sum 3.0 / total 2.6 vs components 1.5 × 0.8 = 1.2) overwrite the lesson-compliant flat call."
evidence_cited: "Official call pipeline up/flat (total 2.6) vs writeup flat/mild (hand-sum +1.2). Actual XLB −0.82%, SPY +0.66%, rel −1.48% (down/mild); gap-and-chop (~−0.84% into the open), cash session flat around 53.22–53.23. NVDA +8.74%, Nasdaq +1.57%, S&P +0.72% — tech-only risk-on. LIN −1.02%, SHW −1.00%, ECL ~−1.6%, FCX −0.73%, NEM +0.52%. Copper settle −0.11% to $6.5870 (two-day −1.83% from 8/25 record); gold faded from premarket +1.45% to settle +0.25%. Scoreboard: dir miss, mag miss. 8/25 lesson was marked active in the morning packet and applied in prose, not in the graded emit."
error_category: "C"
scope: "general"
date: "2026-08-27"
status: "active"
occurrences: "1"
promoted_on: "2026-08-28"
sources: "['2026-08-27_sector_basic_materials_lesson.md']"
schema_ok: "true"
---

## RULE
If 8/25 conditions hold, emitted predicted_direction cannot be up — use the writeup/hand-sum (flat/flat-mild), not a conflicting pipeline up. Cap S4 at 0 when 1d rel <0.5% (1m rel already negative is not confirmation). Net S1 at 0 when gold HIT is offset by fading copper + inventory rebuild + China HIT; gold remains an 8/14 sleeve credit, not a +1 wash. For chemicals-heavy XLB, carried sticky PCE + tech-led tape is a cyclical overlay (S0 ≤ 0, can be slightly negative); do not ding S0 merely because “JH” is on the calendar, but do not treat that rule as a ban on scoring the rate/cyclical headwind when NQ >> ES and LIN/SHW are not leading.

## WHEN IT FIRES
XLB writeup already diagnoses an 8/25 composition/transmission setup (NQ >> ES, XLB 1d rel <0.5%, mixed premarket breadth, two-sided S1) and hand-scores flat, but the emitted/graded call is still pipeline up because S4 is scored positive from a sub-0.5% tape and/or leading_sum/total_score disagrees with the written S0–S4 components. Gold HIT is allowed to keep S1 at +1 even when copper is off the record, LME stocks have rebuilt, and China/property remains HIT.

## WRONG IF
Next session where 8/25 conditions hold (NQ >> ES, XLB 1d rel <0.5%, mixed LIN/FCX/NEM/SHW, two-sided S1) and XLB still prints up with LIN/chemicals participating — then forbidding emitted-up and flattening S4/S1 is too strict. Separately, if pipeline leading_sum/total_score is shown to match the written components and still be up for a non-8/25 reason, this is not a pipeline-overwrite lesson.

## EVIDENCE
Official call pipeline up/flat (total 2.6) vs writeup flat/mild (hand-sum +1.2). Actual XLB −0.82%, SPY +0.66%, rel −1.48% (down/mild); gap-and-chop (~−0.84% into the open), cash session flat around 53.22–53.23. NVDA +8.74%, Nasdaq +1.57%, S&P +0.72% — tech-only risk-on. LIN −1.02%, SHW −1.00%, ECL ~−1.6%, FCX −0.73%, NEM +0.52%. Copper settle −0.11% to $6.5870 (two-day −1.83% from 8/25 record); gold faded from premarket +1.45% to settle +0.25%. Scoreboard: dir miss, mag miss. 8/25 lesson was marked active in the morning packet and applied in prose, not in the graded emit.

(learn_cycle promote)
