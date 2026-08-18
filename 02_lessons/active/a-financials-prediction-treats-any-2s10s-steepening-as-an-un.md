---
trigger_pattern: "A Financials prediction treats any 2s10s steepening as an unconditional bank NIM tailwind and scores it positively in S0/S1, even when the steepening is actually a bearish long-end Treasury selloff — 30y auction at ~5.22% (highest since 2001), 10y at multi-decade highs, oil/geopolitical inflation fears live. The model records the steepening as a positive, double-counts it, and leaves direction up, while the flat S4 tape is used only to cap magnitude in prose and the pipeline still emits a higher official band. The sector then underperforms because rate-sensitive financials are hurt by long-end yields and term-premium repricing."
corrected_behavior: "Before scoring S0/S1, decompose the steepening. If it is long-end-driven — 30y/10y at multi-decade highs, term premium expanding from deficits, war, or AI spending — treat it as a headwind for Financials, not a NIM tailwind. Check the latest 30y auction print and oil/geopolitical headlines at open. Do not double-count the same steepening in S0 and S1. If long-end yields are at extreme levels and oil is spiking on chokepoint risk, set direction at least neutral/down for rate-sensitive financials unless there is a strong offsetting same-day sector catalyst. Ensure the written total_score and the pipeline total_score agree before the official band is graded."
falsifier: "The lesson is false if a Financials session with 30y at 5.2%+ and rising oil still produces positive XLF relative returns because NIM expansion and IB revenue dominate. It is also false if empirical data shows long-end-driven steepening is net-positive for XLF, e.g., banks fully pass through funding costs and insurers are duration-matched."
current_behavior: "Reads 'curve steepening' as a benign NIM tailwind whenever present, without decomposing whether the move is driven by the short end falling (benign) or the long end selling off (bearish for duration-sensitive financials). Records 30y at 5.21–5.22% as 'curve steepening intact' rather than flagging a 25-year-high long-end auction. Scores the same steepening as positive in both S0 and S1, adds 'credit tight' and 'IB surge' as further positives, and does not integrate oil/Hormuz or term-premium stress into the rate-sensitive read. The written conclusion caps magnitude at mild, but the pipeline output remains up/notable, and the scoreboard grades the pipeline band."
evidence_cited: "XLF -0.997% vs SPY -0.473%, rel -0.525%. The 30-year auction cleared at 5.22%, the highest since 2001; the 10-year reached ~4.72% intraday, the highest since 2007; oil rose to ~$88-90 on US-Iran/Hormuz tensions. The strong Empire State beat (20.6 vs 11.0) was not financial-specific and did not offset the rate pressure. Scoreboard: direction_hit False, magnitude_hit False against predicted up/notable."
error_category: "A"
scope: "general"
date: "2026-08-17"
status: "active"
occurrences: "1"
promoted_on: "2026-08-18"
sources: "['2026-08-17_sector_financial_lesson.md']"
schema_ok: "true"
---

## RULE
Before scoring S0/S1, decompose the steepening. If it is long-end-driven — 30y/10y at multi-decade highs, term premium expanding from deficits, war, or AI spending — treat it as a headwind for Financials, not a NIM tailwind. Check the latest 30y auction print and oil/geopolitical headlines at open. Do not double-count the same steepening in S0 and S1. If long-end yields are at extreme levels and oil is spiking on chokepoint risk, set direction at least neutral/down for rate-sensitive financials unless there is a strong offsetting same-day sector catalyst. Ensure the written total_score and the pipeline total_score agree before the official band is graded.

## WHEN IT FIRES
A Financials prediction treats any 2s10s steepening as an unconditional bank NIM tailwind and scores it positively in S0/S1, even when the steepening is actually a bearish long-end Treasury selloff — 30y auction at ~5.22% (highest since 2001), 10y at multi-decade highs, oil/geopolitical inflation fears live. The model records the steepening as a positive, double-counts it, and leaves direction up, while the flat S4 tape is used only to cap magnitude in prose and the pipeline still emits a higher official band. The sector then underperforms because rate-sensitive financials are hurt by long-end yields and term-premium repricing.

## WRONG IF
The lesson is false if a Financials session with 30y at 5.2%+ and rising oil still produces positive XLF relative returns because NIM expansion and IB revenue dominate. It is also false if empirical data shows long-end-driven steepening is net-positive for XLF, e.g., banks fully pass through funding costs and insurers are duration-matched.

## EVIDENCE
XLF -0.997% vs SPY -0.473%, rel -0.525%. The 30-year auction cleared at 5.22%, the highest since 2001; the 10-year reached ~4.72% intraday, the highest since 2007; oil rose to ~$88-90 on US-Iran/Hormuz tensions. The strong Empire State beat (20.6 vs 11.0) was not financial-specific and did not offset the rate pressure. Scoreboard: direction_hit False, magnitude_hit False against predicted up/notable.

(learn_cycle promote)
