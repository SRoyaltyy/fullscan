---
trigger_pattern: "A sector is a deep multi-horizon relative laggard (1m rel ≤ −5%) with a negative 1d relative tape, on a live escalating macro shock day (oil supply shock / rates pressure), and the model emits the directional call (down/mild) rather than flattening — the call resolves as a direction HIT and magnitude HIT, but the realized relative underperformance is far smaller than the prior-day 1d rel signal used to justify the S4 = −1 weighting (signal decay: −1.04% prior-day rel → −0.12pp realized rel)."
corrected_behavior: "When the sector is deeply oversold (RSI < 30) AND the 1m relative lag is extreme (≤ −5%), treat the prior-day 1d relative tape as a decaying signal, not a level signal: keep the directional call (down/mild) — do NOT flatten — but recognize that the S4 confirming-tape weight overstates the expected relative underperformance. The direction call is validated; the relative-magnitude expectation should be tempered. Do not convert this into a flat call (the 09-09 correction still governs direction), but do not read the prior-day rel as evidence of continued relative acceleration."
falsifier: "If on a future deep-laggard (1m rel ≤ −5%, RSI < 30) + live macro shock day the sector's realized 1d relative underperformance is ≥ 1.0pp (i.e., the prior-day rel signal does NOT decay but extends), then the 'decaying signal' framing is wrong and the S4 = −1 level-weighting should be retained at full strength."
current_behavior: "The model treats the prior-day 1d relative tape (−1.04%) as a strong, level-based confirming signal and assigns S4 = −1 on that basis, without discounting for the fact that a stretched, oversold laggard (RSI ~25, 1m rel −5.56%) is a mean-reversion setup whose relative beta to the shock is decaying rather than accelerating."
evidence_cited: "2026-09-10 Industrials: predicted down/mild, actual XLI −0.72% vs SPY −0.60%, rel −0.12pp. Direction HIT, magnitude HIT. Morning S4 = −1 was built on 1d rel −1.04%; realized rel was −0.12pp. The sector did not lead the decline despite Brent surging 6% to ~$107.63 intraday. Post-session review explicitly flags: 'the S4 confirming-tape signal (−1.04% 1d rel) decayed to −0.12pp realized, so the sector's relative beta to the oil shock was lower than the tape implied."
error_category: "NONE"
scope: "general"
date: "2026-09-10"
status: "active"
occurrences: "1"
promoted_on: "2026-09-10"
sources: "['2026-09-10_sector_industrials_lesson.md']"
schema_ok: "true"
---

## RULE
When the sector is deeply oversold (RSI < 30) AND the 1m relative lag is extreme (≤ −5%), treat the prior-day 1d relative tape as a decaying signal, not a level signal: keep the directional call (down/mild) — do NOT flatten — but recognize that the S4 confirming-tape weight overstates the expected relative underperformance. The direction call is validated; the relative-magnitude expectation should be tempered. Do not convert this into a flat call (the 09-09 correction still governs direction), but do not read the prior-day rel as evidence of continued relative acceleration.

## WHEN IT FIRES
A sector is a deep multi-horizon relative laggard (1m rel ≤ −5%) with a negative 1d relative tape, on a live escalating macro shock day (oil supply shock / rates pressure), and the model emits the directional call (down/mild) rather than flattening — the call resolves as a direction HIT and magnitude HIT, but the realized relative underperformance is far smaller than the prior-day 1d rel signal used to justify the S4 = −1 weighting (signal decay: −1.04% prior-day rel → −0.12pp realized rel).

## WRONG IF
If on a future deep-laggard (1m rel ≤ −5%, RSI < 30) + live macro shock day the sector's realized 1d relative underperformance is ≥ 1.0pp (i.e., the prior-day rel signal does NOT decay but extends), then the "decaying signal" framing is wrong and the S4 = −1 level-weighting should be retained at full strength.

## EVIDENCE
2026-09-10 Industrials: predicted down/mild, actual XLI −0.72% vs SPY −0.60%, rel −0.12pp. Direction HIT, magnitude HIT. Morning S4 = −1 was built on 1d rel −1.04%; realized rel was −0.12pp. The sector did not lead the decline despite Brent surging 6% to ~$107.63 intraday. Post-session review explicitly flags: "the S4 confirming-tape signal (−1.04% 1d rel) decayed to −0.12pp realized, so the sector's relative beta to the oil shock was lower than the tape implied.

(learn_cycle promote)
