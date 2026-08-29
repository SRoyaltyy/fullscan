---
trigger_pattern: "Day-2 Technology/XLK after a confirmed, already-traded mega-cap/AI-infra beat (prior session rel >+1%), NQ inside ±0.5% (not independently weak or green), crowded long, and a still-pending two-sided high-impact policy event (Fed Chair speech / FOMC-class). The model treats mega-cap-earnings-over-macro-drag as a close-direction floor that forbids down, leaves S1 at +1 on the carried spine, and emits up (or “flat not down”) instead of allowing the fade path."
corrected_behavior: "mega-cap-earnings-over-macro-drag is an **open-session** constraint only: forbid down solely when the index-relevant beat is same-session/not fully in the tape **and** futures/internals do not independently confirm weakness. On day-2 with a live two-sided policy binary, do **not** forbid down; set S1 to 0 (spine intact, not a same-session raise); do not emit up. Official direction is flat or down/mild with an explicit fade path. Cap magnitude at mild until the event prints or NQ independently confirms (≥+0.5% or ≤−0.5%). Reconcile pipeline direction to signed Σ(S0–S3)×mult — a narrative “flat/fade” cannot grade as up."
falsifier: "Same setup (day-2 after a tape-embedded mega-cap/AI beat, NQ inside ±0.5%, crowded XLK, two-sided policy event still pending) and XLK still closes absolute up with hardware leadership holding through the speech → cutting S1 to 0 and allowing down would undercall. Also weakened if the speech prints hawkish, 2Y jumps, and XLK still holds because mega-cap ex-NVDA (MSFT/AAPL) lifts the ETF — then “allow down” is too hardware-blanket and must stay NVDA/SOX-weight-conditional. Two of the next three such days closing ≥+0.5% XLK retires the down-path default."
current_behavior: "Scores S0=0 (neutral-until-print) but still forbids down because futures are not ≤−0.5% and the prior mega-cap beat is “intact.” Keeps S1=+1 on the same AI-infra cluster already in Thursday’s tape. Narrative caps magnitude (08-12/08-13) and mentions a fade, yet the official call stays up/flat; pipeline leading_sum inflates vs signed S0–S3 and overrides “absolute flat.”"
evidence_cited: "2026-08-28 predicted up/flat (pipeline total 2.7, S0=0 S1=+1 S2=0 S3=0 S4=0, mult 0.9; narrative Σ=0.9 “flat not down”). Actual XLK −1.548% vs SPY −0.227%, rel −1.321% → down/notable. Driver = one S0 Warsh hawkish resolution (2Y +8–12 bp, Sept hike odds ~55–57%), amplifying day-2 NVDA −4.57% / SOX −3.47%; AI-demand spine not broken (Warsh cited AI capex; MSFT/AAPL/AMZN green). Knowable at open: Warsh pending, NQ −0.19%/−0.29%, Thursday rel +2.50% already traded, crowding. Not knowable: hawkish wording / 2Y jump. Direction MISS, magnitude MISS."
error_category: "B"
scope: "general"
date: "2026-08-28"
status: "active"
occurrences: "1"
promoted_on: "2026-08-28"
sources: "['2026-08-28_sector_technology_lesson.md']"
schema_ok: "true"
---

## RULE
mega-cap-earnings-over-macro-drag is an **open-session** constraint only: forbid down solely when the index-relevant beat is same-session/not fully in the tape **and** futures/internals do not independently confirm weakness. On day-2 with a live two-sided policy binary, do **not** forbid down; set S1 to 0 (spine intact, not a same-session raise); do not emit up. Official direction is flat or down/mild with an explicit fade path. Cap magnitude at mild until the event prints or NQ independently confirms (≥+0.5% or ≤−0.5%). Reconcile pipeline direction to signed Σ(S0–S3)×mult — a narrative “flat/fade” cannot grade as up.

## WHEN IT FIRES
Day-2 Technology/XLK after a confirmed, already-traded mega-cap/AI-infra beat (prior session rel >+1%), NQ inside ±0.5% (not independently weak or green), crowded long, and a still-pending two-sided high-impact policy event (Fed Chair speech / FOMC-class). The model treats mega-cap-earnings-over-macro-drag as a close-direction floor that forbids down, leaves S1 at +1 on the carried spine, and emits up (or “flat not down”) instead of allowing the fade path.

## WRONG IF
Same setup (day-2 after a tape-embedded mega-cap/AI beat, NQ inside ±0.5%, crowded XLK, two-sided policy event still pending) and XLK still closes absolute up with hardware leadership holding through the speech → cutting S1 to 0 and allowing down would undercall. Also weakened if the speech prints hawkish, 2Y jumps, and XLK still holds because mega-cap ex-NVDA (MSFT/AAPL) lifts the ETF — then “allow down” is too hardware-blanket and must stay NVDA/SOX-weight-conditional. Two of the next three such days closing ≥+0.5% XLK retires the down-path default.

## EVIDENCE
2026-08-28 predicted up/flat (pipeline total 2.7, S0=0 S1=+1 S2=0 S3=0 S4=0, mult 0.9; narrative Σ=0.9 “flat not down”). Actual XLK −1.548% vs SPY −0.227%, rel −1.321% → down/notable. Driver = one S0 Warsh hawkish resolution (2Y +8–12 bp, Sept hike odds ~55–57%), amplifying day-2 NVDA −4.57% / SOX −3.47%; AI-demand spine not broken (Warsh cited AI capex; MSFT/AAPL/AMZN green). Knowable at open: Warsh pending, NQ −0.19%/−0.29%, Thursday rel +2.50% already traded, crowding. Not knowable: hawkish wording / 2Y jump. Direction MISS, magnitude MISS.

(learn_cycle promote)
