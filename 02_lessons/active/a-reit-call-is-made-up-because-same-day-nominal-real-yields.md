---
trigger_pattern: "A REIT call is made 'up' because same-day nominal/real yields are easing (DFII10, 10Y, and 30Y down on the day), while the 30Y remains at or near a multi-decade high and bond-market strain is the persistent structural backdrop. The same easing factor is scored positively in S0, S1, and S4, producing an up call that misses a flat or underperforming sector close."
corrected_behavior: "Before scoring rate easing as positive for REITs, check the absolute yield level and prior-session context. If 30Y/10Y remain near multi-decade highs and bond-market strain is persistent, treat a small same-day decline as stabilization/noise, not relief. Cap S0/S1 at 0 or negative, avoid placing the same easing factor in S4, default to flat/underperform relative to SPY, and reconcile any pipeline-vs-narrative band mismatch before emission."
falsifier: "If 30Y remains ≥5.25%, DFII10 closes down ≥10bp, and XLRE closes ≥ +0.5% relative on the same day, the level-sensitivity rule is too rigid. To be robust, require this outcome to recur in at least 3 of 5 similar future cases before rejecting the lesson."
current_behavior: "Treats a one-day yield decline as durable duration relief; ignores yield level; over-weights and double-counts the same easing factor across shared macro, sector factors, and ETF tape; fails to reconcile the narrative band with the deterministic pipeline band before finalizing."
evidence_cited: "2026-08-21: predicted up/notable (pipeline) / up/mild (narrative); actual XLRE 0.0%, SPY +0.41%, rel -0.41%. Morning cited DFII10 -0.06 1d, 10Y -0.06 1d, 30Y -0.09 1d as duration relief, but the actual backdrop was persistent 30Y strain near a 19-year high ~5.3% (Reuters/Kitco). The same 'real yields falling' signal was used in S0, S1, and S4, causing single-factor over-concentration."
error_category: "B"
scope: "general"
date: "2026-08-21"
status: "active"
occurrences: "1"
promoted_on: "2026-08-27"
sources: "['2026-08-21_sector_real_estate_lesson.md']"
schema_ok: "true"
---

## RULE
Before scoring rate easing as positive for REITs, check the absolute yield level and prior-session context. If 30Y/10Y remain near multi-decade highs and bond-market strain is persistent, treat a small same-day decline as stabilization/noise, not relief. Cap S0/S1 at 0 or negative, avoid placing the same easing factor in S4, default to flat/underperform relative to SPY, and reconcile any pipeline-vs-narrative band mismatch before emission.

## WHEN IT FIRES
A REIT call is made "up" because same-day nominal/real yields are easing (DFII10, 10Y, and 30Y down on the day), while the 30Y remains at or near a multi-decade high and bond-market strain is the persistent structural backdrop. The same easing factor is scored positively in S0, S1, and S4, producing an up call that misses a flat or underperforming sector close.

## WRONG IF
If 30Y remains ≥5.25%, DFII10 closes down ≥10bp, and XLRE closes ≥ +0.5% relative on the same day, the level-sensitivity rule is too rigid. To be robust, require this outcome to recur in at least 3 of 5 similar future cases before rejecting the lesson.

## EVIDENCE
2026-08-21: predicted up/notable (pipeline) / up/mild (narrative); actual XLRE 0.0%, SPY +0.41%, rel -0.41%. Morning cited DFII10 -0.06 1d, 10Y -0.06 1d, 30Y -0.09 1d as duration relief, but the actual backdrop was persistent 30Y strain near a 19-year high ~5.3% (Reuters/Kitco). The same "real yields falling" signal was used in S0, S1, and S4, causing single-factor over-concentration.

(learn_cycle promote)
