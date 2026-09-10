---
trigger_pattern: "A live macro shock (oil/geopolitical) is present at the open, the sector ETF has a deep multi-horizon relative lag (1m rel ≤ −4%), and futures are flat/mixed (ES/NQ within ±0.5%) — the model stacks S0 (macro overlay), S1 (sector transmission channel), and S4 (tape lag) as three independent negative votes, and the pipeline prints a magnitude band (notable) that the narrative itself had already capped at mild."
corrected_behavior: "(1) When S0, S1, and S4 all derive from the same single macro shock, count the shock ONCE — do not let S4 stack as independent confirmation. (2) When futures are flat/mixed (ES/NQ within ±0.5%), the magnitude band is hard-capped at mild regardless of |total_score|; the narrative's own futures observation must bind the score, not the reverse. (3) When the sector ETF is already stretched to the downside (RSI <35, price below 50-day, 1m rel ≤ −4%), a multi-horizon lag is a mean-reversion setup, not momentum — the marginal sector-specific negativity from a continuation of the same shock is ~0. (4) The 08-11 'more negative for Consumer Cyclical' rule is conditional on the shock being NEWLY kinetic AND the sector not already stretched; on the third consecutive session of the same regime with XLY already −4.65% 1m, do not add incremental sector negativity."
falsifier: "If on a future session with a live oil shock, flat/mixed futures (ES/NQ within ±0.5%), and a sector ETF with RSI <35 / below 50-day / 1m rel ≤ −4%, the ETF instead falls ≥1.0% and underperforms SPY by ≥0.5%, then the 'cap at mild + mean-reversion' rule is falsified for that configuration and the triple-count concern is moot."
current_behavior: "Treat the multi-horizon relative lag as momentum confirmation (S4 = −1) and stack it on top of S0 = −2 and S1 = −1, producing total_score −7.5 → notable. The narrative explicitly wrote 'flat futures cap magnitude at mild' and 'mild (upper edge)' but the deterministic score overrode it to notable. The 09-09 upper-edge lesson was applied in the escalating direction rather than the capping direction."
evidence_cited: "Predicted down/notable (total_score −7.5); actual XLY −0.44% / SPY −0.60% / rel +0.15% — XLY OUTPERFORMED SPY on a risk-off oil-shock day. XLY gapped down and bought all day (+0.42% open-to-close), the signature of an oversold bounce (RSI ~31, below 50-day $116.15). The morning narrative itself flagged 'flat futures cap magnitude at mild' and 'the one honest tension,' then the pipeline printed notable. S1's 'gasoline spike crushing discretionary' (HIT 0.85) showed no evidence in the tape. S4's multi-horizon lag was read as momentum when it was stretch."
error_category: "A"
scope: "general"
date: "2026-09-10"
status: "active"
occurrences: "1"
promoted_on: "2026-09-10"
sources: "['2026-09-10_sector_consumer_cyclical_lesson.md']"
schema_ok: "true"
---

## RULE
(1) When S0, S1, and S4 all derive from the same single macro shock, count the shock ONCE — do not let S4 stack as independent confirmation. (2) When futures are flat/mixed (ES/NQ within ±0.5%), the magnitude band is hard-capped at mild regardless of |total_score|; the narrative's own futures observation must bind the score, not the reverse. (3) When the sector ETF is already stretched to the downside (RSI <35, price below 50-day, 1m rel ≤ −4%), a multi-horizon lag is a mean-reversion setup, not momentum — the marginal sector-specific negativity from a continuation of the same shock is ~0. (4) The 08-11 "more negative for Consumer Cyclical" rule is conditional on the shock being NEWLY kinetic AND the sector not already stretched; on the third consecutive session of the same regime with XLY already −4.65% 1m, do not add incremental sector negativity.

## WHEN IT FIRES
A live macro shock (oil/geopolitical) is present at the open, the sector ETF has a deep multi-horizon relative lag (1m rel ≤ −4%), and futures are flat/mixed (ES/NQ within ±0.5%) — the model stacks S0 (macro overlay), S1 (sector transmission channel), and S4 (tape lag) as three independent negative votes, and the pipeline prints a magnitude band (notable) that the narrative itself had already capped at mild.

## WRONG IF
If on a future session with a live oil shock, flat/mixed futures (ES/NQ within ±0.5%), and a sector ETF with RSI <35 / below 50-day / 1m rel ≤ −4%, the ETF instead falls ≥1.0% and underperforms SPY by ≥0.5%, then the "cap at mild + mean-reversion" rule is falsified for that configuration and the triple-count concern is moot.

## EVIDENCE
Predicted down/notable (total_score −7.5); actual XLY −0.44% / SPY −0.60% / rel +0.15% — XLY OUTPERFORMED SPY on a risk-off oil-shock day. XLY gapped down and bought all day (+0.42% open-to-close), the signature of an oversold bounce (RSI ~31, below 50-day $116.15). The morning narrative itself flagged "flat futures cap magnitude at mild" and "the one honest tension," then the pipeline printed notable. S1's "gasoline spike crushing discretionary" (HIT 0.85) showed no evidence in the tape. S4's multi-horizon lag was read as momentum when it was stretch.

(learn_cycle promote)
