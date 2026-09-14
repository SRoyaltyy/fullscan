---
status: living_policy
updated: 2026-09-14
source: src/learn_cycle.py
covers: general, sectors, news
note: Injected into general + sector PREDICT. Core output formats unchanged.
see_also: 03_scoreboard/LEARNINGS.md
---

# Mutable policy (all workflows)

Last learn_cycle: **2026-09-14**. Promoted: 0. Retired: 10. Active lessons: 191. Human digest: `03_scoreboard/LEARNINGS.md`.

## Accuracy by topic (graded window)

- **general**: 40% (6/15)
- **sector:Basic Materials**: 47% (7/15)
- **sector:Communication Services**: 33% (5/15)
- **sector:Consumer Cyclical**: 67% (10/15)
- **sector:Consumer Defensive**: 53% (8/15)
- **sector:Energy**: 40% (6/15)
- **sector:Financial**: 40% (6/15)
- **sector:Healthcare**: 53% (8/15)
- **sector:Industrials**: 33% (5/15)
- **sector:Real Estate**: 47% (7/15)
- **sector:Technology**: 53% (8/15)
- **sector:Utilities**: 47% (7/15)

## Numeric factor weights in force (engine_policy.json — applied by code, not by you)

General (B0–B7 LLM components; multiplier applied by compute_scores):
- B0_ASIA: n=13 sign-hit=0.39 → ×0.0
- B0_EUROPE: n=8 sign-hit=1.00 → ×1.25
- B1_CATALYSTS: n=21 sign-hit=0.76 → ×1.25
- B2_BONDS: n=24 sign-hit=0.33 → ×0.0
- B3_FEDPATH: n=23 sign-hit=0.48 → ×0.5
- B4_VIX: n=8 sign-hit=0.50 → ×0.5
- B5_SENTIMENT: n=21 sign-hit=0.33 → ×0.0
- B6_FUTURES: n=15 sign-hit=0.73 → ×1.25
- B7_OIL_DOLLAR: n=21 sign-hit=0.67 → ×1.25
Sectors (pooled S0–S4; per-sector overrides in engine_policy.json):
- S0_SHARED_MACRO: n=120 sign-hit=0.61 → ×1.0
- S1_SECTOR_FACTORS: n=148 sign-hit=0.55 → ×0.5
- S2_BREADTH: n=121 sign-hit=0.59 → ×1.0
- S3_FLOWS_POSITIONING: n=89 sign-hit=0.48 → ×0.5
- S4_ETF_TAPE: n=126 sign-hit=0.63 → ×1.0
Last change: Communication Services.S1_SECTOR_FACTORS: 1.0 -> 0.5 (n=148, hit=0.547); Consumer Defensive.S2_BREADTH: 1.0 -> 1.25 (n=9, hit=0.667); Consumer Defensive.S3_FLOWS_POSITIONING: 0.5 -> 1.0 (n=8, hit=0.625); Financial.S0_SHARED_MACRO: 0.5 -> 1.0 (n=14, hit=0.571); Technology.S4_ETF_TAPE: 1.0 -> 1.25 (n=9, hit=0.667); Utilities.S0_SHARED_MACRO: 1.0 -> 0.0 (n=8, hit=0.375)

## Active adjustments (newest promoted lessons, truncated)

### a-binding-sector-lesson-crowded-long-fuel-unwind-fuel-fires.md
## RULE
When a binding lesson's causal precondition is explicitly identified as absent or inverted, ZERO the lesson's component contribution — do not merely damp it. A crowded-long complex that has just de-risked (prior-day negative rel print) into an easing macro overlay (oil offered, green futures) is multiplicatively BULLISH (reflex bounce), so S3 should be ≈ 0 or positive, not negative. Additionally, on a broad risk-on day (all four index futures green ≥ +0.5%, SPY up), score S2 breadth posi …

### a-deep-multi-horizon-relative-laggard-sector-1m-rel-5-with-a.md
## RULE
No correction required — this is a validated positive pattern. The three stacked corrections (09-09 emit-directional, 09-10 decay-laggard, 09-04 score-once) all fired in the same direction and all helped. The one honest nuance to carry forward: the +1.07% was a gap-and-hold (close 172.37 < open 172.45), so the entire gain was captured overnight and the intraday path was flat-to-down. "Mild" correctly implied no trend day, but a reader interpreting "up/mild" as an intraday grind would hav …

### a-defensive-sector-healthcare-staples-utilities-is-a-multi-d.md
## RULE
When a defensive sector carries a multi-day relative lag (3d/1w rel negative) into a risk-on macro catalyst, score S0 at 0 to −0.3, not positive. Risk-on is a ROTATION signal: capital flows into high-beta/cyclical and OUT of defensives, so a defensive sector is a funding source, not a destination. Additionally: (a) do not score S2/S3/S4 at 0.0 when the rotation-out flow is intact — a lower-high tape (two small positive rel prints then a fade) is distribution, not a base; (b) require a po …

### a-rate-sensitive-bond-proxy-sector-xlre-xlu-xlp-enters-a-ses.md
## RULE
When S0 is genuinely 0 (mixed macro) and the live, knowable-at-open tape is positive (green futures ≥ +0.5%, oil offered, live curve flat-to-easing), a down call requires a LIVE negative input. A stale multi-horizon relative lag (1w/1m) is a structural descriptor, not a same-day tape signal — it must not be scored into S2 or S4. The absence of a positive cushion (09-08 override not firing) is NOT the presence of a negative signal; "no cushion ≠ headwind." If the only negatives are stale …

### a-scheduled-high-impact-macro-binary-cpi-nfp-fomc-is-pending.md
## RULE
When the dominant binary is a scheduled macro release and the sector is a defensive bond-proxy, test the sign of EACH branch against the sector's factor exposure before calling it symmetric. If both branches are negative-to-neutral (in-line CPI → risk-on rotation away from defensives; hot CPI → rates up → bond-proxy down), score the asymmetry: S0 = −1 (not 0), raise S1 rotation-away from PARTIAL to full weight, weight the LIVE 1d/1m relative tape over stale 3d/1w positives in S2, raise S …

### a-scheduled-high-impact-macro-release-cpi-nfp-fomc-is-pendin.md
## RULE
Separate the *binary* (unknowable at the snapshot — do not pre-score) from the *pre-binary tape* (knowable — must be scored). When all four index futures confirm ≥ +0.5% in the same direction AND the sector's own cost driver is easing (oil offered for a chemicals-heavy book), score a modest S0 lean in the futures direction (e.g. +0.5 to +1) rather than 0, while keeping the magnitude band capped and confidence reduced for the pending binary. The 08-21 checklist is a ban on a *stale opposi …

### a-scheduled-high-impact-macro-release-cpi-nfp-fomc-is-the-do.md
## RULE
Add a hard direction gate at emit time: if a scheduled high-impact macro binary is pending AND |B6| ≥ +0.5% (ES or NQ independently confirming), the emitted predicted_direction MUST match the sign of B6, with magnitude capped at mild unless a fresh same-day catalyst or a |B6| ≥ 1.0% move justifies notable. The pipeline may not emit flat against confirming futures. Additionally, re-score B4: VIX term-structure backwardation is a stress *level* signal, not a directional *day* signal — do n …

### a-sector-analysis-names-a-factor-as-a-relative-headwind-drag.md
## RULE
If a factor is named in the prose as a "headwind," "drag," "rotation away from," or "relative negative," it MUST receive a negative score in the corresponding component — or the prose must be deleted. No unscored named factors. Concretely for a defensive sector on a green-futures / oil-offering / in-line-CPI tape: score the risk-on rotation as a NEGATIVE S1 (or S0) contribution of comparable magnitude to the input-cost-relief positive, so the two partially cancel rather than one being co …

### a-sector-etf-s-trailing-1d-relative-print-from-the-prior-clo.md
## RULE
When no fresh same-day constituent/breadth data exists, a single trailing 1d rel print may anchor AT MOST ONE component score. If it is used for S4 (tape), S2 must be scored 0 absent independent breadth evidence (constituent-level moves, flow print, or a multi-day rel pattern that is itself the breadth signal). Do not let one stale rel print justify two positive scores. Additionally, a single day's rel print in a concentrated two-name book is weak evidence of structural leadership — it s …

### a-concentrated-sector-etf-two-mega-cap-names-35-combined-wei.md
## RULE
(1) Never map NQ/ES divergence onto a two-name book — NQ composition (semis/software/AI-infra) is not XLC composition; the morning itself flagged AMAT/LITE/ALAB as premarket leaders *outside* XLC, which is the tell that the NQ weakness was exogenous to the sector. (2) When the only non-zero components are correlated expressions of the same regime (oil shock → risk-off → broad tech lag), collapse them to a single negative rather than stacking S0 and S2 as independent; the effective negati …

### a-fresh-overnight-kinetic-oil-supply-escalation-iran-hormuz.md
## RULE
When B1 is scored at −3 for a fresh kinetic/oil shock but B6 (futures) is flat within ±0.5% and does not confirm ≥0.5% down, FORCE the final magnitude band to MILD regardless of leading_sum magnitude. Operationalize as a hard gate: if |B6| < 0.5% and no same-day hard-data or mega-cap miss confirms, cap predicted_magnitude_band at mild and set multiplier ≤ 0.9. Keep B1=−3 for direction and conviction; the band is set separately by futures confirmation. Reconcile the narrative band with th …

### a-live-macro-risk-off-shock-oil-geopolitical-escalation-bren.md
## RULE
When a live macro risk-off shock coincides with a crowded long-duration complex and a red NQ, premarket strength in that complex must be scored as distribution (negative), not accumulation (positive). S0 and S1 are additive, not offsetting: the premarket rally is the liquidity that lets the crowd exit into the gap-down open. The correct output is down/mild at minimum (arguably down/notable), and the divergence flag must actually propagate to the deterministic output so the conviction cut …

### a-live-macro-shock-oil-100-long-end-yield-stress-is-scored-a.md
## RULE
When the sector's 1d relative tape is flat (|1d rel| < ~0.15%) and credit is tight and futures are mixed, do NOT score S1 as an independent negative on the strength of the macro narrative alone — the sector-specific transmission channel must be confirmed by the sector's own relative tape (a negative 1d rel, or a widening sector-specific spread) before it earns a separate negative score. Cap S1 at 0 in that configuration and let S0 carry the absolute-down call alone. The oil/yields shock …

### a-live-macro-shock-oil-geopolitical-is-present-at-the-open-t.md
## RULE
(1) When S0, S1, and S4 all derive from the same single macro shock, count the shock ONCE — do not let S4 stack as independent confirmation. (2) When futures are flat/mixed (ES/NQ within ±0.5%), the magnitude band is hard-capped at mild regardless of |total_score|; the narrative's own futures observation must bind the score, not the reverse. (3) When the sector ETF is already stretched to the downside (RSI <35, price below 50-day, 1m rel ≤ −4%), a multi-horizon lag is a mean-reversion se …

### a-live-macro-shock-oil-geopolitical-risk-off-is-present-at-t.md
## RULE
When S0 already carries a macro shock, do NOT add an S1 negative for the same fact unless the sector's own live tape independently confirms the transmission channel is firing (e.g. XLF 1d rel ≤ −0.4%, or a fresh sector-specific rate-sensitivity headline). If the sector's 1d and 1m relative are flat (here +0.05% and +0.08%), treat the S1 transmission as UNCONFIRMED and score S1=0, not −0.5. The macro shock is counted ONCE, in S0. Additionally, when 1d/1m rel are flat and only a stale 3d r …

_(+176 older active lessons not excerpted; each predict receives only its own topic's lessons via lesson_select)_

## Per-scope DO-INSTEAD

### scope `general` — wins=6 losses=9
- **win 2026-09-09:** [general] Keep direction; shrink confidence on modest |score| when magnitude historically misses.
- **win 2026-09-10:** [general] Keep direction; shrink confidence on modest |score| when magnitude historically misses.
- **loss 2026-09-11:** [general] When score sign conflicts with sector ETF tape / breadth, cut conviction; prefer flat/mild.

### scope `news` — wins=0 losses=1
- **loss news:** [news] Only emit actions with |net| above a higher floor.

### scope `sector_basic_materials` — wins=7 losses=8
- **win 2026-09-09:** [sector_basic_materials] Keep direction; shrink confidence on modest |score| when magnitude historically misses.
- **win 2026-09-10:** [sector_basic_materials] Keep direction; shrink confidence on modest |score| when magnitude historically misses.
- **loss 2026-09-11:** [sector_basic_materials] When score sign conflicts with sector ETF tape / breadth, cut conviction; prefer flat/mild.

### scope `sector_communication_services` — wins=5 losses=10
- **loss 2026-09-09:** [sector_communication_services] When score sign conflicts with sector ETF tape / breadth, cut conviction; prefer flat/mild.
- **loss 2026-09-10:** [sector_communication_services] When score sign conflicts with sector ETF tape / breadth, cut conviction; prefer flat/mild.
- **win 2026-09-11:** [sector_communication_services] Keep direction; shrink confidence on modest |score| when magnitude historically misses.

### scope `sector_consumer_cyclical` — wins=10 losses=5
- **win 2026-09-09:** [sector_consumer_cyclical] Keep direction; shrink confidence on modest |score| when magnitude historically misses.
- **win 2026-09-10:** [sector_consumer_cyclical] Keep direction; shrink confidence on modest |score| when magnitude historically misses.
- **loss 2026-09-11:** [sector_consumer_cyclical] When score sign conflicts with sector ETF tape / breadth, cut conviction; prefer flat/mild.

### scope `sector_consumer_defensive` — wins=8 losses=7
- **loss 2026-09-10:** [sector_consumer_defensive] When score sign conflicts with sector ETF tape / breadth, cut conviction; prefer flat/mild.
- **win 2026-09-11:** [sector_consumer_defensive] Keep direction; shrink confidence on modest |score| when magnitude historically misses.
- **win 2026-09-14:** [sector_consumer_defensive] Keep direction; shrink confidence on modest |score| when magnitude historically misses.

### scope `sector_energy` — wins=6 losses=9
- **loss 2026-09-10:** [sector_energy] When score sign conflicts with sector ETF tape / breadth, cut conviction; prefer flat/mild.
- **loss 2026-09-11:** [sector_energy] When score sign conflicts with sector ETF tape / breadth, cut conviction; prefer flat/mild.
- **loss 2026-09-14:** [sector_energy] When score sign conflicts with sector ETF tape / breadth, cut conviction; prefer flat/mild.

### scope `sector_financial` — wins=6 losses=9
- **win 2026-09-10:** [sector_financial] Keep direction; shrink confidence on modest |score| when magnitude historically misses.
- **loss 2026-09-11:** [sector_financial] When score sign conflicts with sector ETF tape / breadth, cut conviction; prefer flat/mild.
- **win 2026-09-14:** [sector_financial] Keep direction; shrink confidence on modest |score| when magnitude historically misses.

### scope `sector_healthcare` — wins=8 losses=7
- **win 2026-09-10:** [sector_healthcare] Keep direction; shrink confidence on modest |score| when magnitude historically misses.
- **loss 2026-09-11:** [sector_healthcare] When score sign conflicts with sector ETF tape / breadth, cut conviction; prefer flat/mild.
- **loss 2026-09-14:** [sector_healthcare] When score sign conflicts with sector ETF tape / breadth, cut conviction; prefer flat/mild.

### scope `sector_industrials` — wins=5 losses=10
- **win 2026-09-10:** [sector_industrials] Keep direction; shrink confidence on modest |score| when magnitude historically misses.
- **win 2026-09-11:** [sector_industrials] Keep direction; shrink confidence on modest |score| when magnitude historically misses.
- **win 2026-09-14:** [sector_industrials] Keep direction; shrink confidence on modest |score| when magnitude historically misses.

### scope `sector_real_estate` — wins=7 losses=8
- **win 2026-09-10:** [sector_real_estate] Keep direction; shrink confidence on modest |score| when magnitude historically misses.
- **loss 2026-09-11:** [sector_real_estate] When score sign conflicts with sector ETF tape / breadth, cut conviction; prefer flat/mild.
- **win 2026-09-14:** [sector_real_estate] Keep direction; shrink confidence on modest |score| when magnitude historically misses.

### scope `sector_technology` — wins=8 losses=7
- **loss 2026-09-10:** [sector_technology] When score sign conflicts with sector ETF tape / breadth, cut conviction; prefer flat/mild.
- **win 2026-09-11:** [sector_technology] Keep direction; shrink confidence on modest |score| when magnitude historically misses.
- **win 2026-09-14:** [sector_technology] Keep direction; shrink confidence on modest |score| when magnitude historically misses.

### scope `sector_utilities` — wins=7 losses=8
- **win 2026-09-10:** [sector_utilities] Keep direction; shrink confidence on modest |score| when magnitude historically misses.
- **win 2026-09-11:** [sector_utilities] Keep direction; shrink confidence on modest |score| when magnitude historically misses.
- **win 2026-09-14:** [sector_utilities] Keep direction; shrink confidence on modest |score| when magnitude historically misses.

## Open experiments

- **sector_utilities/loss 2026-08-18:** [sector_utilities] Require one extra confirming source in the dominant bucket before full weight when score sign matches this fail pattern.
- **sector_utilities/loss 2026-08-21:** [sector_utilities] Require one extra confirming source in the dominant bucket before full weight when score sign matches this fail pattern.
- **sector_utilities/loss 2026-08-26:** [sector_utilities] Require one extra confirming source in the dominant bucket before full weight when score sign matches this fail pattern.
- **sector_utilities/loss 2026-08-27:** [sector_utilities] Require one extra confirming source in the dominant bucket before full weight when score sign matches this fail pattern.
- **sector_utilities/win 2026-08-28:** [sector_utilities] On similar setups, test milder bands when |score|<4; log whether lagging tape factors overrode leading ones.
- **sector_utilities/loss 2026-09-03:** [sector_utilities] Require one extra confirming source in the dominant bucket before full weight when score sign matches this fail pattern.
- **sector_utilities/loss 2026-09-04:** [sector_utilities] Require one extra confirming source in the dominant bucket before full weight when score sign matches this fail pattern.
- **sector_utilities/loss 2026-09-09:** [sector_utilities] Require one extra confirming source in the dominant bucket before full weight when score sign matches this fail pattern.
- **sector_utilities/win 2026-09-10:** [sector_utilities] On similar setups, test milder bands when |score|<4; log whether lagging tape factors overrode leading ones.
- **sector_utilities/win 2026-09-11:** [sector_utilities] On similar setups, test milder bands when |score|<4; log whether lagging tape factors overrode leading ones.
- **sector_utilities/win 2026-09-14:** [sector_utilities] On similar setups, test milder bands when |score|<4; log whether lagging tape factors overrode leading ones.
- **news/loss news:** [news] Raise min net weight to map a ticker; drop weak edges.

## Methodology checklist (MEMORY_CONFIRM)

1. Did any open experiment for THIS scope apply today?
2. Missing factor that would have flipped a recent loss?
3. Overweighting one bucket / double-counting one headline?
4. Sectors: S0 macro vs S1 sector factors — which failed?
5. News: event family still earning weight on 1d close?

## Retired / falsified (efficacy-gated, automatic)

- 2026-09-14: `a-utilities-xlu-call-is-built-after-a-stretch-of-risk-on-gro.md` (sector:Utilities) — topic hit 80% → 14% after activation; retired.
- 2026-09-14: `a-sector-call-has-a-scheduled-8-30-et-macro-release-pending.md` (sector:Financial) — topic hit 75% → 14% after activation; retired.
- 2026-09-14: `in-a-utilities-xlu-call-a-second-soft-inflation-print-has-al.md` (sector:Utilities) — topic hit 75% → 14% after activation; retired.
- 2026-09-14: `a-consumer-cyclical-down-call-is-driven-by-a-genuinely-negat.md` (sector:Consumer Cyclical) — topic hit 86% → 29% after activation; retired.
- 2026-09-14: `when-the-pre-fetched-commodity-tape-conflicts-with-live-sour.md` (sector:Energy) — topic hit 71% → 14% after activation; retired.
- 2026-09-14: `a-sector-call-has-a-decisively-negative-fundamental-spine-fr.md` (sector:Consumer Cyclical) — topic hit 83% → 29% after activation; retired.
- 2026-09-14: `a-utility-defensive-sector-call-is-built-on-a-carried-defens.md` (sector:Utilities) — topic hit 67% → 14% after activation; retired.
- 2026-09-14: `sector-prediction-made-when-the-sector-s-dominant-commodity.md` (sector:Energy) — topic hit 67% → 14% after activation; retired.
- 2026-09-14: `a-sector-call-has-a-scheduled-8-30-et-high-impact-macro-rele.md` (sector:Financial) — topic hit 60% → 14% after activation; retired.
- 2026-09-14: `a-fresh-top-holding-legal-regulatory-catalyst-e-g-a-trial-op.md` (sector:Communication Services) — topic hit 43% → 0% after activation; retired.
