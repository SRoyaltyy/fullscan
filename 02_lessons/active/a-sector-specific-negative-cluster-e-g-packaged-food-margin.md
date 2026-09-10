---
trigger_pattern: "A sector-specific negative cluster (e.g., packaged-food margin/dividend stress) has produced a multi-session relative-lag streak, and the model converts that streak into a standing 'dominance rule' that zeroes or negates a simultaneously maximal, live, market-wide flight-to-safety regime signal (VIX backwardation + oil >$100 + NQ lagging + Asia red + strongly negative 10Y–SPX correlation) for a defensive sector."
corrected_behavior: "When the regime panel shows a maximal FTS setup, a defensive sector's trailing relative lag is a setup for mean-reversion/FTS catch-up, not a bearish confirmation. Cap the sector-specific-drag override at one session; require a fresh negative print (new guidance cut, new data) to keep the drag at full weight; and when de-duplicating a two-sided shock (oil = input-cost negative AND haven bid), count it once at the correct net weight rather than keeping only the negative leg. Apply the 08-18 template ('rising 10Y + risk-off → relative outperformance / flat-to-negative absolute') to the sign call."
falsifier: "If on a future session with a maximal FTS setup (VIX backwardation + oil >$100 + NQ lagging + Asia red + 10Y–SPX corr ≤ −0.9) a defensive sector with a multi-session relative lag still closes down in absolute terms AND underperforms SPY, the corrected rule is falsified and the dominance rule should be restored to full weight."
current_behavior: "The model identifies the FTS bid in its own text ('risk-off + NQ lag + oil >$100 = theoretical relative FTS bid'), then zeroes S0 via the streak rule, keeps S1 at full negative weight on a carried (not fresh) drag, and uses a paid trailing relative lag as a bearish S4 confirmation — producing a down/flat call against a maximal FTS setup."
evidence_cited: "2026-09-10 XLP +0.048% vs SPY −0.599% (rel +0.648%) — dir MISS on absolute sign, mag HIT (flat), rel thesis HIT. Morning memo wrote the correct 08-18 template verbatim and then scored −2.925 against it. S0=0 should have been +0.5 to +1.0; S1=−1 should have been −0.5; S4=−0.5 was a sign error (should be 0 to +0.25). Corrected total ≈ 0 to +0.5 → flat/up."
error_category: "A"
scope: "general"
date: "2026-09-10"
status: "active"
occurrences: "1"
promoted_on: "2026-09-10"
sources: "['2026-09-10_sector_consumer_defensive_lesson.md']"
schema_ok: "true"
---

## RULE
When the regime panel shows a maximal FTS setup, a defensive sector's trailing relative lag is a setup for mean-reversion/FTS catch-up, not a bearish confirmation. Cap the sector-specific-drag override at one session; require a fresh negative print (new guidance cut, new data) to keep the drag at full weight; and when de-duplicating a two-sided shock (oil = input-cost negative AND haven bid), count it once at the correct net weight rather than keeping only the negative leg. Apply the 08-18 template ("rising 10Y + risk-off → relative outperformance / flat-to-negative absolute") to the sign call.

## WHEN IT FIRES
A sector-specific negative cluster (e.g., packaged-food margin/dividend stress) has produced a multi-session relative-lag streak, and the model converts that streak into a standing "dominance rule" that zeroes or negates a simultaneously maximal, live, market-wide flight-to-safety regime signal (VIX backwardation + oil >$100 + NQ lagging + Asia red + strongly negative 10Y–SPX correlation) for a defensive sector.

## WRONG IF
If on a future session with a maximal FTS setup (VIX backwardation + oil >$100 + NQ lagging + Asia red + 10Y–SPX corr ≤ −0.9) a defensive sector with a multi-session relative lag still closes down in absolute terms AND underperforms SPY, the corrected rule is falsified and the dominance rule should be restored to full weight.

## EVIDENCE
2026-09-10 XLP +0.048% vs SPY −0.599% (rel +0.648%) — dir MISS on absolute sign, mag HIT (flat), rel thesis HIT. Morning memo wrote the correct 08-18 template verbatim and then scored −2.925 against it. S0=0 should have been +0.5 to +1.0; S1=−1 should have been −0.5; S4=−0.5 was a sign error (should be 0 to +0.25). Corrected total ≈ 0 to +0.5 → flat/up.

(learn_cycle promote)
