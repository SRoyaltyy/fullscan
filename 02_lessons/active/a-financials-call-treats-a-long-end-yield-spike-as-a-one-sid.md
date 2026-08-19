---
trigger_pattern: "A financials call treats a long-end yield spike as a one-sided headwind and applies 'relative outperformance does not make an absolute up call' mechanically, even when the risk-off tape is tech/growth-specific, the long-end selloff is the rotation catalyst out of tech, and XLF has strong sustained relative strength (1d rel ≥ +0.4%, positive 3d/1w, bank index in multi-session uptrend)."
corrected_behavior: "On tech-specific yield-driven risk-off days, treat the long-end move as two-sided for financials: (1) a NIM/rate headwind, but (2) a rotation tailwind out of high-multiple growth into value/financials. If XLF premarket relative strength is strong (≥ +0.4% 1d rel), 3d/1w relative tape is positive, the bank index is in a multi-day uptrend, and credit spreads are not blowing out, allow S2/S3/S4 to offset S0/S1 and set direction at least neutral-with-up-bias, ideally up/mild. The 'relative ≠ absolute' cap should apply only to weak/marginal relative strength or credit-stress-driven risk-off."
falsifier: "If XLF shows the full trigger set — strong premarket relative strength, positive 3d/1w, KBW multi-day uptrend, tech-specific yield-driven risk-off, tight credit spreads — and repeatedly closes flat-to-down in absolute terms, this correction should be reverted. The rule also fails if credit stress is systemic rather than a tech-specific rotation."
current_behavior: "Long-end steepening is scored as a headwind in S0/S1; S2/S4 relative strength is capped at +0.5; narrative says relative strength cannot justify an absolute up call on risk-off. Result: flat/down even when financials are the defensive rotation destination."
evidence_cited: "2026-08-18 actual XLF +0.45% vs SPY -0.68%, relative +1.13%. Morning had XLF 1d rel +0.47%, 3d rel +0.39%, 1w rel +0.65%. KBW Bank Index had advanced 4 straight sessions and was +18% YTD; Goldman IB fees +55%, equities trading +72%; 30y Treasury reached ~5.34%, highest since 2007, which crushed tech but drove defensive/value rotation into financials."
error_category: "B"
scope: "general"
date: "2026-08-18"
status: "active"
occurrences: "1"
promoted_on: "2026-08-19"
sources: "['2026-08-18_sector_financial_lesson.md']"
schema_ok: "true"
---

## RULE
On tech-specific yield-driven risk-off days, treat the long-end move as two-sided for financials: (1) a NIM/rate headwind, but (2) a rotation tailwind out of high-multiple growth into value/financials. If XLF premarket relative strength is strong (≥ +0.4% 1d rel), 3d/1w relative tape is positive, the bank index is in a multi-day uptrend, and credit spreads are not blowing out, allow S2/S3/S4 to offset S0/S1 and set direction at least neutral-with-up-bias, ideally up/mild. The "relative ≠ absolute" cap should apply only to weak/marginal relative strength or credit-stress-driven risk-off.

## WHEN IT FIRES
A financials call treats a long-end yield spike as a one-sided headwind and applies "relative outperformance does not make an absolute up call" mechanically, even when the risk-off tape is tech/growth-specific, the long-end selloff is the rotation catalyst out of tech, and XLF has strong sustained relative strength (1d rel ≥ +0.4%, positive 3d/1w, bank index in multi-session uptrend).

## WRONG IF
If XLF shows the full trigger set — strong premarket relative strength, positive 3d/1w, KBW multi-day uptrend, tech-specific yield-driven risk-off, tight credit spreads — and repeatedly closes flat-to-down in absolute terms, this correction should be reverted. The rule also fails if credit stress is systemic rather than a tech-specific rotation.

## EVIDENCE
2026-08-18 actual XLF +0.45% vs SPY -0.68%, relative +1.13%. Morning had XLF 1d rel +0.47%, 3d rel +0.39%, 1w rel +0.65%. KBW Bank Index had advanced 4 straight sessions and was +18% YTD; Goldman IB fees +55%, equities trading +72%; 30y Treasury reached ~5.34%, highest since 2007, which crushed tech but drove defensive/value rotation into financials.

(learn_cycle promote)
