---
trigger_pattern: "Sector prediction calls flat/flat when the sector's own factor (oil) is offered premarket (WTI −1.19%, Brent −1.07%) but the model suppresses the signed lean because (a) the 1d relative tape from the prior session is flat (+0.07%), (b) the sector already ran 1w rel +4.40% / 1m rel +12.04%, and (c) a DO-INSTEAD rule prefers flat/mild when factors and tape conflict. The actual session delivers a −1.78% relative reversal day — the offered barrel was the correct leading signal, and the flat 1d tape was a stall, not a neutralization."
corrected_behavior: "When S1 is a live, verified sector-factor print (oil offered −1.19% WTI / −1.07% Brent) and S4 is 0 only because the prior session's 1d relative move was flat (not because the current session's premarket tape is confirming strength), the model should emit a signed direction (down/mild or down/notable depending on magnitude rules) rather than flat. A flat prior-day tape is not a counter-signal to a live offered barrel; it is a neutral starting point. The DO-INSTEAD rule should only fire when the live tape is actually flat/mixed at the decision time (e.g., premarket XLE flat, majors mixed), not when the prior session's close was flat. Additionally, the crowded-long condition (1w rel +4.40%, 1m rel +12.04%) combined with a live offered barrel is a fade setup, not a reason to downgrade to flat — extension increases downside risk on a factor reversal, it does not neutralize it."
falsifier: "A future session where WTI/Brent are offered premarket (≥−0.75%) AND the prior session's 1d relative tape was flat (|rel| < 0.3%) AND the model emits down/mild or down/notable AND the actual session is flat (|rel| < 0.5%) — that would falsify this lesson. Conversely, if the model emits flat and the actual is a >1% relative down day again, the lesson is confirmed."
current_behavior: "When S1 (sector factors) is −1 but S4 (ETF tape) is 0 because the prior session's 1d relative move was flat, the model treats the flat tape as a veto on converting the factor lean into a signed direction. It applies DO-INSTEAD (prefer flat/mild) and emits flat/flat with a 0.9 multiplier, even though the live oil print is unambiguously down and the sector is extended (crowded long). The model conflates 'tape stalled yesterday' with 'tape will neutralize the factor today."
evidence_cited: "Prediction: flat/flat, S1=−1, S4=0, total −2.7, multiplier 0.9. Actual: XLE −0.737%, SPY +1.047%, rel −1.784% — a >1.5% relative down day. The live oil print (WTI −1.19%, Brent −1.07%) was correct in sign; the model's own S1 captured it. The error was entirely in the flat-tape veto logic: S4=0 (prior 1d rel +0.07%) was used to suppress the S1 lean into flat, but the actual session saw XLE underperform SPY by −1.78% — exactly the direction S1 pointed. The crowded-long condition (HIT in the grid, 1m rel +12.04%) should have amplified the downside lean, not capped it to flat."
error_category: "A"
scope: "general"
date: "2026-09-03"
status: "active"
occurrences: "1"
promoted_on: "2026-09-04"
sources: "['2026-09-03_sector_energy_lesson.md']"
schema_ok: "true"
---

## RULE
When S1 is a live, verified sector-factor print (oil offered −1.19% WTI / −1.07% Brent) and S4 is 0 only because the prior session's 1d relative move was flat (not because the current session's premarket tape is confirming strength), the model should emit a signed direction (down/mild or down/notable depending on magnitude rules) rather than flat. A flat prior-day tape is not a counter-signal to a live offered barrel; it is a neutral starting point. The DO-INSTEAD rule should only fire when the live tape is actually flat/mixed at the decision time (e.g., premarket XLE flat, majors mixed), not when the prior session's close was flat. Additionally, the crowded-long condition (1w rel +4.40%, 1m rel +12.04%) combined with a live offered barrel is a fade setup, not a reason to downgrade to flat — extension increases downside risk on a factor reversal, it does not neutralize it.

## WHEN IT FIRES
Sector prediction calls flat/flat when the sector's own factor (oil) is offered premarket (WTI −1.19%, Brent −1.07%) but the model suppresses the signed lean because (a) the 1d relative tape from the prior session is flat (+0.07%), (b) the sector already ran 1w rel +4.40% / 1m rel +12.04%, and (c) a DO-INSTEAD rule prefers flat/mild when factors and tape conflict. The actual session delivers a −1.78% relative reversal day — the offered barrel was the correct leading signal, and the flat 1d tape was a stall, not a neutralization.

## WRONG IF
A future session where WTI/Brent are offered premarket (≥−0.75%) AND the prior session's 1d relative tape was flat (|rel| < 0.3%) AND the model emits down/mild or down/notable AND the actual session is flat (|rel| < 0.5%) — that would falsify this lesson. Conversely, if the model emits flat and the actual is a >1% relative down day again, the lesson is confirmed.

## EVIDENCE
Prediction: flat/flat, S1=−1, S4=0, total −2.7, multiplier 0.9. Actual: XLE −0.737%, SPY +1.047%, rel −1.784% — a >1.5% relative down day. The live oil print (WTI −1.19%, Brent −1.07%) was correct in sign; the model's own S1 captured it. The error was entirely in the flat-tape veto logic: S4=0 (prior 1d rel +0.07%) was used to suppress the S1 lean into flat, but the actual session saw XLE underperform SPY by −1.78% — exactly the direction S1 pointed. The crowded-long condition (HIT in the grid, 1m rel +12.04%) should have amplified the downside lean, not capped it to flat.

(learn_cycle promote)
