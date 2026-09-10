---
trigger_pattern: "A sector ETF is a deep multi-horizon relative laggard (1m rel ≤ −5%) and the prior-session 1d relative tape is negative, on a day with a live macro shock (oil/rates risk-off) — the model scores the laggard condition in S2 (breadth) AND re-scores the same relative-underperformance signal in S4 (ETF tape confirmation), citing a prior 'tape confirms → don't flatten' correction as license to add the confirmation point."
corrected_behavior: "(1) Treat S2 and S4 as the SAME signal (relative underperformance) measured over different windows — score it ONCE. If the lag is scored in S2, S4 must be 0 unless there is an independent, same-session tape fact (e.g., a fresh gap, a volume spike, an intraday reversal) not already captured by the lag. (2) The 09-09 correction is a NON-VETO rule (don't let sector_rs_veto/calendar_size_gate flatten a confirmed directional call) — it is NOT a license to ADD a confirmation point. Apply it to the direction decision, not to the score sum. (3) A persistent laggard (1m rel ≤ −5%) is a CONDITION, not a same-day forecast; require a FRESH catalyst to score it as a forward factor, and recognize that in a uniform risk-off tape dispersion compresses and laggards converge. (4) On an oil-spike day, score oil's sign for the SPECIFIC sector composition (XLI contains energy-capex revenue winners and defense bid, not just transport fuel-cost losers) — a one-sided cost-headwind mapping overstates the drag."
falsifier: "If on a future deep-laggard (1m rel ≤ −5%) + negative-1d-rel + live-macro-shock day, XLI's actual relative return is ≤ −0.5% (i.e., the laggard genuinely continues to lag), then scoring the lag once (S2 only, S4 = 0) would under-predict and this lesson would be falsified for that configuration. The lesson holds only when the broad tape is uniformly risk-off (dispersion compresses, laggards converge)."
current_behavior: "The 09-10 Industrials note scored S2 = −1 (1m rel −5.56% laggard) and S4 = −1 (1d rel −1.04% 'confirming tape'), explicitly invoking the 09-09 correction ('when the tape CONFIRMS the negative score, do NOT flatten; emit down/mild') to justify the S4 point. The note claimed it was scoring the lag 'once here (09-04: do not double-count in S4)' but then scored S4 anyway. Result: leading_sum −4.0, total −4.05, down/mild with 0.55 confidence — while actual rel was only −0.12% (XLI tracked SPY almost tick-for-tick). The laggard converged rather than continued."
evidence_cited: "Outcome: XLI −0.72%, SPY −0.60%, rel −0.12% (essentially flat vs SPY). Post-session review: 'S2 (1m laggard) and S4 (1d tape confirmation) were the same relative-underperformance signal scored twice; the 09-09 'tape confirms' correction was misused as a license to double-count.' Honest post-session score: S0 −0.5, S1 0, S2 −0.5, S3 0, S4 0 → sum −1.0 × 0.9 = −0.9 → flat/mild, which matches the actual −0.72% / rel −0.12% outcome. The pipeline's −4.05 was ~3 points too negative. Direction HIT (down) but relative-return thesis MISS."
error_category: "B"
scope: "general"
date: "2026-09-10"
status: "active"
occurrences: "1"
promoted_on: "2026-09-10"
sources: "['2026-09-10_sector_industrials_lesson.md']"
schema_ok: "true"
---

## RULE
(1) Treat S2 and S4 as the SAME signal (relative underperformance) measured over different windows — score it ONCE. If the lag is scored in S2, S4 must be 0 unless there is an independent, same-session tape fact (e.g., a fresh gap, a volume spike, an intraday reversal) not already captured by the lag. (2) The 09-09 correction is a NON-VETO rule (don't let sector_rs_veto/calendar_size_gate flatten a confirmed directional call) — it is NOT a license to ADD a confirmation point. Apply it to the direction decision, not to the score sum. (3) A persistent laggard (1m rel ≤ −5%) is a CONDITION, not a same-day forecast; require a FRESH catalyst to score it as a forward factor, and recognize that in a uniform risk-off tape dispersion compresses and laggards converge. (4) On an oil-spike day, score oil's sign for the SPECIFIC sector composition (XLI contains energy-capex revenue winners and defense bid, not just transport fuel-cost losers) — a one-sided cost-headwind mapping overstates the drag.

## WHEN IT FIRES
A sector ETF is a deep multi-horizon relative laggard (1m rel ≤ −5%) and the prior-session 1d relative tape is negative, on a day with a live macro shock (oil/rates risk-off) — the model scores the laggard condition in S2 (breadth) AND re-scores the same relative-underperformance signal in S4 (ETF tape confirmation), citing a prior "tape confirms → don't flatten" correction as license to add the confirmation point.

## WRONG IF
If on a future deep-laggard (1m rel ≤ −5%) + negative-1d-rel + live-macro-shock day, XLI's actual relative return is ≤ −0.5% (i.e., the laggard genuinely continues to lag), then scoring the lag once (S2 only, S4 = 0) would under-predict and this lesson would be falsified for that configuration. The lesson holds only when the broad tape is uniformly risk-off (dispersion compresses, laggards converge).

## EVIDENCE
Outcome: XLI −0.72%, SPY −0.60%, rel −0.12% (essentially flat vs SPY). Post-session review: "S2 (1m laggard) and S4 (1d tape confirmation) were the same relative-underperformance signal scored twice; the 09-09 'tape confirms' correction was misused as a license to double-count." Honest post-session score: S0 −0.5, S1 0, S2 −0.5, S3 0, S4 0 → sum −1.0 × 0.9 = −0.9 → flat/mild, which matches the actual −0.72% / rel −0.12% outcome. The pipeline's −4.05 was ~3 points too negative. Direction HIT (down) but relative-return thesis MISS.

(learn_cycle promote)
