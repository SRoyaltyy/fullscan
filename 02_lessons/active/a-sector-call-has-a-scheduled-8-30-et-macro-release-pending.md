---
trigger_pattern: "A sector call has a scheduled 8:30 ET macro release pending (PPI/CPI/jobless claims) but the narrative states “no scheduled high-impact macro print today.” At the same time, the narrative and SECTOR_SCORES block cap magnitude at MILD (S4=0, multiplier ≤1.0), while the deterministic pipeline prints a different leading_sum/total from the same components and flips the official band to NOTABLE. The scoreboard grades the pipeline output, not the narrative, producing a magnitude miss on what was actually an up/mild day."
corrected_behavior: "Before emitting, reconcile the final deterministic total with the component arithmetic: total = (S0+S1+S2+S3+S4) × multiplier. If the narrative caps at mild, the pipeline output must also be mild; a pipeline total that contradicts the written SECTOR_SCORES block is a tool bug, not a valid override. Also, never describe a day with a scheduled 8:30 ET release as “no macro print”; treat the release as a real same-day catalyst. With flat futures, flat S4 tape, and no fresh sector catalyst, keep Financial magnitude at MILD even if structural factors are strong. Upgrade to notable only if the 1d relative tape or futures are clearly confirming."
falsifier: "If on a similar setup XLF 1d relative tape is already clearly positive (>+0.3% vs SPY) or futures are strongly positive (>+0.5%) and a fresh same-day sector catalyst exists, then up/notable can be correct. Also, a much cooler-than-expected inflation print could produce a broad risk-on day, but sector-specific magnitude should still require evidence that financials are leading, not merely beta-following."
current_behavior: "On 2026-08-13 Financial, the narrative correctly computed total 4.05 → up/mild and explicitly applied the active magnitude-cap lessons, but the deterministic pipeline printed leading_sum 10.0 and total_score 9.675 → up/notable. The scoreboard then recorded magnitude_hit False. Separately, S0 said “no scheduled high-impact macro print today” even though PPI was scheduled and released that morning."
evidence_cited: "2026-08-13 Financial: narrative total 4.05, up/mild; pipeline total 9.675, up/notable; scoreboard graded up/notable. Actual XLF +0.587%, SPY +0.698%, relative -0.111% → direction up, magnitude mild. PPI was flat MoM vs +0.2% expected, a soft inflation print that lifted SPX to a record close, but leadership went to tech/chips and XLF lagged SPY."
error_category: "B"
scope: "general"
date: "2026-08-13"
status: "active"
occurrences: "1"
promoted_on: "2026-08-14"
sources: "['2026-08-13_sector_financial_lesson.md']"
schema_ok: "true"
---

## RULE
Before emitting, reconcile the final deterministic total with the component arithmetic: total = (S0+S1+S2+S3+S4) × multiplier. If the narrative caps at mild, the pipeline output must also be mild; a pipeline total that contradicts the written SECTOR_SCORES block is a tool bug, not a valid override. Also, never describe a day with a scheduled 8:30 ET release as “no macro print”; treat the release as a real same-day catalyst. With flat futures, flat S4 tape, and no fresh sector catalyst, keep Financial magnitude at MILD even if structural factors are strong. Upgrade to notable only if the 1d relative tape or futures are clearly confirming.

## WHEN IT FIRES
A sector call has a scheduled 8:30 ET macro release pending (PPI/CPI/jobless claims) but the narrative states “no scheduled high-impact macro print today.” At the same time, the narrative and SECTOR_SCORES block cap magnitude at MILD (S4=0, multiplier ≤1.0), while the deterministic pipeline prints a different leading_sum/total from the same components and flips the official band to NOTABLE. The scoreboard grades the pipeline output, not the narrative, producing a magnitude miss on what was actually an up/mild day.

## WRONG IF
If on a similar setup XLF 1d relative tape is already clearly positive (>+0.3% vs SPY) or futures are strongly positive (>+0.5%) and a fresh same-day sector catalyst exists, then up/notable can be correct. Also, a much cooler-than-expected inflation print could produce a broad risk-on day, but sector-specific magnitude should still require evidence that financials are leading, not merely beta-following.

## EVIDENCE
2026-08-13 Financial: narrative total 4.05, up/mild; pipeline total 9.675, up/notable; scoreboard graded up/notable. Actual XLF +0.587%, SPY +0.698%, relative -0.111% → direction up, magnitude mild. PPI was flat MoM vs +0.2% expected, a soft inflation print that lifted SPX to a record close, but leadership went to tech/chips and XLF lagged SPY.

(learn_cycle promote)
