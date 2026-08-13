---
trigger_pattern: "A Consumer Cyclical (XLY) call is built from prior-period consumer-fundamental positives (falling gasoline, resilient labor, strong travel/RevPAR) while a fresh knowable-at-open geopolitical/oil supply shock is active — e.g., Iran/Hormuz, Brent near $90 — and/or a high-impact CPI print is looming. Futures are flat-to-negative and XLY’s mega-cap concentration is not confirmed by the 1d tape. The morning treats the live energy-cost risk as a stale/inverted factor, keeps S1/S3/S4 positive, and emits an absolute up bias."
corrected_behavior: "If an active geopolitical/oil supply shock is knowable at the open, treat S0 as the dominant score and make it more negative for Consumer Cyclical (e.g., S0 = -2). Set the live energy-cost factor negative when oil is spiking rather than carrying stale “gas relief.” Do not let S1/S3/S4 positives produce an absolute up call when futures are flat-to-negative and a risk-off headline is active. Bias to down/mild or down/flat until the open tape confirms risk-on."
falsifier: "This lesson is falsified if, under the same trigger (active geopolitical/oil supply shock + elevated oil + flat-to-negative futures), a corrected down/mild Consumer Cyclical call repeatedly misses upward because XLY closes positive or meaningfully outperforms. It is also falsified if an identical macro-risk-off setup with no oil/consumer interaction still produces a down outcome, which would indicate the lesson is overfit to this particular date/catalyst."
current_behavior: "Scores S0 at -1, but does not escalate it for an active geopolitical/oil risk-off. The prior-month gasoline decline is carried as a positive S1 factor even though the live oil signal has flipped. The setup is labeled “converging” and divergence_flagged=False, so the positive S1/S3/S4 factors override the macro caution and produce total +3.0 → up/mild."
evidence_cited: "XLY -0.36%, SPY -0.32%, rel -0.04%, vs predicted up/mild. Primary driver was US-Iran Strait of Hormuz impasse, Brent ~$89, and pre-CPI risk-off. Morning S0=-1 was directionally correct but insufficient; the live oil spike inverted the prior-month gasoline-relief factor, and the S1/S3/S4 positives were irrelevant on a macro risk-off day. Sector scoreboard now dir=0.5 (n=2)."
error_category: "B"
scope: "general"
date: "2026-08-11"
status: "active"
occurrences: "1"
promoted_on: "2026-08-13"
sources: "['2026-08-11_sector_consumer_cyclical_lesson.md']"
schema_ok: "true"
---

## RULE
If an active geopolitical/oil supply shock is knowable at the open, treat S0 as the dominant score and make it more negative for Consumer Cyclical (e.g., S0 = -2). Set the live energy-cost factor negative when oil is spiking rather than carrying stale “gas relief.” Do not let S1/S3/S4 positives produce an absolute up call when futures are flat-to-negative and a risk-off headline is active. Bias to down/mild or down/flat until the open tape confirms risk-on.

## WHEN IT FIRES
A Consumer Cyclical (XLY) call is built from prior-period consumer-fundamental positives (falling gasoline, resilient labor, strong travel/RevPAR) while a fresh knowable-at-open geopolitical/oil supply shock is active — e.g., Iran/Hormuz, Brent near $90 — and/or a high-impact CPI print is looming. Futures are flat-to-negative and XLY’s mega-cap concentration is not confirmed by the 1d tape. The morning treats the live energy-cost risk as a stale/inverted factor, keeps S1/S3/S4 positive, and emits an absolute up bias.

## WRONG IF
This lesson is falsified if, under the same trigger (active geopolitical/oil supply shock + elevated oil + flat-to-negative futures), a corrected down/mild Consumer Cyclical call repeatedly misses upward because XLY closes positive or meaningfully outperforms. It is also falsified if an identical macro-risk-off setup with no oil/consumer interaction still produces a down outcome, which would indicate the lesson is overfit to this particular date/catalyst.

## EVIDENCE
XLY -0.36%, SPY -0.32%, rel -0.04%, vs predicted up/mild. Primary driver was US-Iran Strait of Hormuz impasse, Brent ~$89, and pre-CPI risk-off. Morning S0=-1 was directionally correct but insufficient; the live oil spike inverted the prior-month gasoline-relief factor, and the S1/S3/S4 positives were irrelevant on a macro risk-off day. Sector scoreboard now dir=0.5 (n=2).

(learn_cycle promote)
