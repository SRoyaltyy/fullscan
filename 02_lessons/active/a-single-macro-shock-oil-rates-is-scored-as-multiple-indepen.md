---
trigger_pattern: "A single macro shock (oil/rates) is scored as multiple independent negative components (S0 macro overlay + S1 sector transmission channel + S4 multi-horizon tape lag) when the sector ETF is already stretched to the downside (RSI <35, price below 50-day, 1m rel ≤ −4%), and index futures are flat/mixed (±0.5%) — i.e., the shock is index-level, not sector-specific, and the tape lag is a mean-reversion setup rather than momentum confirmation."
corrected_behavior: "(1) When S0, S1, and S4 all express the same macro object, collapse them to a single negative vote — do not let the same shock vote three times. (2) When futures are flat/mixed (±0.5%), the narrative's 'caps magnitude at mild' must BIND the score; the pipeline band must not exceed the futures-implied ceiling. (3) When the sector ETF is oversold (RSI <35, below 50-day) AND the shock is a continuation (≥3rd session of same regime) rather than newly kinetic, treat the multi-horizon lag as a mean-reversion setup, not momentum — and cap the band at mild or flat, with relative outperformance explicitly flagged as a live possibility. (4) The 08-11 'more negative for Consumer Cyclical' rule is conditional: it requires a NEWLY kinetic shock AND the sector not already stretched to the downside."
falsifier: "If on a future session with (a) a live oil/rates shock, (b) XLY RSI <35 and below 50-day, (c) flat/mixed index futures (±0.5%), and (d) ≥3rd consecutive session of the same regime, XLY nonetheless underperforms SPY by ≥0.5% (rel ≤ −0.5%) and closes at or below the open, then the mean-reversion/band-cap rule is falsified and the triple-counting bearish stack was correct."
current_behavior: "The model triple-counts one shock as S0=−2, S1=−1, S4=−1 (summing to −4 of −7.5), declares 'no divergence — both point down,' and lets the deterministic pipeline print notable (total_score −7.5) even though the narrative itself flagged flat futures as capping magnitude at mild. The 1m relative lag (−4.65%) is read only as bearish confirmation, never as a stretch/mean-reversion signal. The 09-09 upper-edge lesson is applied in the wrong direction (escalating toward notable) instead of capping at mild."
evidence_cited: "Predicted down/notable (total_score −7.5); actual XLY −0.44% vs SPY −0.60%, rel +0.15% — XLY OUTPERFORMED SPY on a risk-off oil-shock day. XLY gained ~0.42% open-to-close (oversold bounce off RSI ~31, below 50-day $116.15). The morning narrative explicitly said 'flat futures cap magnitude at mild' and 'the one honest tension,' then the pipeline printed notable. S0/S1/S4 were one oil/rates shock counted three times."
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
(1) When S0, S1, and S4 all express the same macro object, collapse them to a single negative vote — do not let the same shock vote three times. (2) When futures are flat/mixed (±0.5%), the narrative's "caps magnitude at mild" must BIND the score; the pipeline band must not exceed the futures-implied ceiling. (3) When the sector ETF is oversold (RSI <35, below 50-day) AND the shock is a continuation (≥3rd session of same regime) rather than newly kinetic, treat the multi-horizon lag as a mean-reversion setup, not momentum — and cap the band at mild or flat, with relative outperformance explicitly flagged as a live possibility. (4) The 08-11 "more negative for Consumer Cyclical" rule is conditional: it requires a NEWLY kinetic shock AND the sector not already stretched to the downside.

## WHEN IT FIRES
A single macro shock (oil/rates) is scored as multiple independent negative components (S0 macro overlay + S1 sector transmission channel + S4 multi-horizon tape lag) when the sector ETF is already stretched to the downside (RSI <35, price below 50-day, 1m rel ≤ −4%), and index futures are flat/mixed (±0.5%) — i.e., the shock is index-level, not sector-specific, and the tape lag is a mean-reversion setup rather than momentum confirmation.

## WRONG IF
If on a future session with (a) a live oil/rates shock, (b) XLY RSI <35 and below 50-day, (c) flat/mixed index futures (±0.5%), and (d) ≥3rd consecutive session of the same regime, XLY nonetheless underperforms SPY by ≥0.5% (rel ≤ −0.5%) and closes at or below the open, then the mean-reversion/band-cap rule is falsified and the triple-counting bearish stack was correct.

## EVIDENCE
Predicted down/notable (total_score −7.5); actual XLY −0.44% vs SPY −0.60%, rel +0.15% — XLY OUTPERFORMED SPY on a risk-off oil-shock day. XLY gained ~0.42% open-to-close (oversold bounce off RSI ~31, below 50-day $116.15). The morning narrative explicitly said "flat futures cap magnitude at mild" and "the one honest tension," then the pipeline printed notable. S0/S1/S4 were one oil/rates shock counted three times.

(learn_cycle promote)
