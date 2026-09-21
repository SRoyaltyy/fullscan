---
status: living_policy
updated: 2026-09-21
source: src/learn_cycle.py
covers: general, sectors, news
note: Injected into general + sector PREDICT. Core output formats unchanged.
see_also: 03_scoreboard/LEARNINGS.md
---

# Mutable policy (all workflows)

Last learn_cycle: **2026-09-21**. Promoted: 0. Retired: 10. Active lessons: 202. Human digest: `03_scoreboard/LEARNINGS.md`.

## Accuracy by topic (graded window)

- **general**: 60% (9/15)
- **sector:Basic Materials**: 33% (5/15)
- **sector:Communication Services**: 20% (3/15)
- **sector:Consumer Cyclical**: 40% (6/15)
- **sector:Consumer Defensive**: 40% (6/15)
- **sector:Energy**: 53% (8/15)
- **sector:Financial**: 53% (8/15)
- **sector:Healthcare**: 47% (7/15)
- **sector:Industrials**: 40% (6/15)
- **sector:Real Estate**: 33% (5/15)
- **sector:Technology**: 33% (5/15)
- **sector:Utilities**: 33% (5/15)

## Numeric factor weights in force (engine_policy.json — applied by code, not by you)

General (B0–B7 LLM components; multiplier applied by compute_scores):
- B0_ASIA: n=18 sign-hit=0.50 → ×0.5
- B0_EUROPE: n=11 sign-hit=0.91 → ×1.25
- B1_CATALYSTS: n=24 sign-hit=0.79 → ×1.25
- B2_BONDS: n=29 sign-hit=0.38 → ×0.0
- B3_FEDPATH: n=28 sign-hit=0.46 → ×0.5
- B4_VIX: n=11 sign-hit=0.64 → ×1.0
- B5_SENTIMENT: n=21 sign-hit=0.33 → ×0.0
- B6_FUTURES: n=21 sign-hit=0.76 → ×1.25
- B7_OIL_DOLLAR: n=27 sign-hit=0.70 → ×1.25
Sectors (pooled S0–S4; per-sector overrides in engine_policy.json):
- S0_SHARED_MACRO: n=149 sign-hit=0.60 → ×1.0
- S1_SECTOR_FACTORS: n=175 sign-hit=0.57 → ×1.0
- S2_BREADTH: n=134 sign-hit=0.62 → ×1.0
- S3_FLOWS_POSITIONING: n=95 sign-hit=0.49 → ×0.5
- S4_ETF_TAPE: n=141 sign-hit=0.62 → ×1.0
Last change: hold

## Active adjustments (newest promoted lessons, truncated)

### a-low-beta-defensive-sector-etf-xlp-xlu-xlv-like-posts-a-net.md
## RULE
When the LLM overlay sign and the live PM sign AGREE and both are negative for a defensive sector, and the leading S0–S4 sum is net-negative, the `index_carry` leg must NOT be allowed to pull the official direction to flat/up. Promote the morning's own "reject the engine, trust factors" instruction from prose to a scored override: if `sign(overlay) == sign(PM) == sign(leading_sum)` and `sign(index_carry) != sign(leading_sum)`, cap the index_carry contribution so the official direction pr …

### a-sector-card-whose-leading-s0-s4-components-are-unanimously.md
## RULE
(1) When the injected Channel 1 relative tape and the pipeline's `sector_rs_tape` disagree in sign, the injected tape wins and the RS veto must be suppressed — a veto built on numbers that contradict the trusted feed is not a veto. (2) Scope the 09-14 "PM gap is direction, not a notable extrapolant" rule to gap days only; on a trend day (PM green, NQ ≥ +0.5%, 4-horizon rel uniformly green, live same-session sector catalyst), the band must be permitted to reach notable. (3) When the LLM o …

### a-low-beta-bond-proxy-defensive-xlp-like-posts-a-net-negativ.md
## RULE
When leading S0–S4 is net-negative and sector PM is non-haven and already in the flat band, do not accept v2 up/mild from ES tape_anchor + index_carry. Official call is up/flat (PM beta) or flat/flat — never widen magnitude to mild off index beta. Size_gate + non-haven PM caps the band at flat. Trust factors/overlay over leftover RS and over index_carry.

## WHEN IT FIRES
A low-beta bond-proxy defensive (XLP-like) posts a net-negative leading S0–S4 card and a non-haven premarket print al …

### a-low-beta-defensive-healthcare-etf-xlv-like-posts-a-net-non.md
## RULE
No direction/band correction. Keep 09-11 S0 as the relative funding-source read; keep 08-13 as a ban on leftover-RS up/notable. Do not extend 09-16’s force-flat past an unprinted path-binary. When FOMC is paid and PM:XLV is already in the mild band (~+0.4%) on a confirmed NQ-led risk-on tape, official absolute may follow tape_anchor up/mild; relative is the S0 object. Do not rewrite S0 as a duration bid from same-session yield relief, and do not promote XBI/high-beta sleeve or single-nam …

### a-cyclical-industrials-etf-xli-like-posts-an-all-zero-s0-s4.md
## RULE
No signed-call change. Keep flat/flat on an unsigned post-event industrials card. Do not promote the overnight ES gap into up, and do not promote the post-close relative lag into down — both were path, not pre-open factors. A ~0.2% faded-gap print that misses the flat/up direction threshold is banding noise, not a mandate to lift direction.

## WHEN IT FIRES
A cyclical industrials ETF (XLI-like) posts an all-zero S0–S4 card on the session AFTER a paid FOMC/SEP, with 1w/1m relative lag fo …

### a-sector-etf-posts-an-all-zero-or-net-zero-s0-s4-leading-car.md
## RULE
When S0–S3 net to zero BUT (a) the index backdrop is strongly positive (ES/NQ ≥ +0.5% and ideally ≥ +1%), (b) yields are falling / duration-relief is live, and (c) the sector's own PM is green, the residual should be MILD UP with relative lag — not flat. Condition the 8/28 "residual-is-flat" rule on a NEUTRAL index tape; it must not bind when the broad tape is up >1%. Treat the 8/27 S4-cap as a cap on CONVICTION (cannot emit confirmed-up), not on LEVEL (a capped-up call is still an up ca …

### session-after-an-already-printed-fomc-sep-presser-with-a-lar.md
## RULE
When the sector ETF's own live premarket print is flat (|PM| ≤ ~0.1%) and is the worst/tied-worst on the sector board while the index sleeve is strongly green, the index legs of tape_anchor must be **capped or zeroed for this sector** — an index rebound is not a participation certificate (08-27 / 09-10 / 09-16). With S0–S4 all explicitly 0 and PM:XLC = 0.00%, the correct official call is **flat/flat**, not up/mild. If the engine cannot suppress the index sleeve, the LLM overlay must emit …

### a-bond-proxy-defensive-xlp-xlu-xlre-like-has-a-net-negative.md
## RULE
Do not let leftover Finviz 1d/1w or 3d/1w RS veto official direction when (1) live PM is not a haven (mid/red) and/or live Channel 1 1d rel is already ≤0, and (2) leading S0–S3 is net negative. Prefer Channel 1 / PM over stale Finviz RS for the veto input. Keep calendar_size_gate for unprinted FOMC so magnitude stays ≤ mild (not notable); do not also flatten direction. Do not restack a prior-session 10Y break that already printed in the ETF.

## WHEN IT FIRES
A bond-proxy defensive (XLP/ …

### a-mega-cap-duration-heavy-consumer-cyclical-etf-xly-like-amz.md
## RULE
When live futures are inside ±0.5% and the factor card + 1d rel already agree down, keep overlay direction = down. calendar_size_gate / size_gate may cap magnitude to mild/flat only. Do not treat engine divergence against a discarded prior-close ES/NQ anchor as a real leading-vs-tape fight. Do not import XLK/NQ beta (08-27 still binds).

## WHEN IT FIRES
A mega-cap duration-heavy consumer cyclical ETF (XLY-like: AMZN+TSLA+HD dominate, no semis) prints a net-negative S0–S4 card with a con …

### a-two-name-duration-growth-communications-etf-xlc-like-meta.md
## RULE
If XLC S4=0 — not on the PM sector board, live print ~flat, META/GOOGL not participating together — official direction must follow the factor card (flat), not tape_anchor/index_carry. Green NQ/ES/XLK is not an XLC participation certificate. An unprinted FOMC+SEP+presser independently forbids up. Do not convert a correct all-zero card into an up call via futures overlay.

## WHEN IT FIRES
A two-name duration/growth communications ETF (XLC-like: META+GOOGL dominate) posts an all-zero / S4= …

### a-two-name-duration-growth-sector-etf-xlc-like-has-a-live-sa.md
## RULE
Enforce the leftover-ban inside sector_rs_veto: do not flatten a directional call on prior-close 1d/1w RS when live PM:ETF or same-session 1d rel confirms the call. Live tape outranks leftover RS. If narrative and pipeline disagree under a live risk-off overlay, do not let the veto silently win; grade the reconciled live-tape call. Do not score a stale prior predict (up/mild) when the contemporaneous block is down/mild or flat/flat.

## WHEN IT FIRES
A two-name duration/growth sector ETF …

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

_(+187 older active lessons not excerpted; each predict receives only its own topic's lessons via lesson_select)_

## Per-scope DO-INSTEAD

### scope `general` — wins=9 losses=6
- **win 2026-09-17:** [general] Keep direction; shrink confidence on modest |score| when magnitude historically misses.
- **win 2026-09-18:** [general] Keep direction; shrink confidence on modest |score| when magnitude historically misses.
- **win 2026-09-21:** [general] Keep direction; shrink confidence on modest |score| when magnitude historically misses.

### scope `news` — wins=0 losses=1
- **loss news:** [news] Only emit actions with |net| above a higher floor.

### scope `sector_basic_materials` — wins=5 losses=10
- **loss 2026-09-17:** [sector_basic_materials] When score sign conflicts with sector ETF tape / breadth, cut conviction; prefer flat/mild.
- **loss 2026-09-18:** [sector_basic_materials] When score sign conflicts with sector ETF tape / breadth, cut conviction; prefer flat/mild.
- **loss 2026-09-21:** [sector_basic_materials] When score sign conflicts with sector ETF tape / breadth, cut conviction; prefer flat/mild.

### scope `sector_communication_services` — wins=3 losses=12
- **loss 2026-09-17:** [sector_communication_services] When score sign conflicts with sector ETF tape / breadth, cut conviction; prefer flat/mild.
- **loss 2026-09-18:** [sector_communication_services] When score sign conflicts with sector ETF tape / breadth, cut conviction; prefer flat/mild.
- **win 2026-09-21:** [sector_communication_services] Keep direction; shrink confidence on modest |score| when magnitude historically misses.

### scope `sector_consumer_cyclical` — wins=6 losses=9
- **loss 2026-09-17:** [sector_consumer_cyclical] When score sign conflicts with sector ETF tape / breadth, cut conviction; prefer flat/mild.
- **loss 2026-09-18:** [sector_consumer_cyclical] When score sign conflicts with sector ETF tape / breadth, cut conviction; prefer flat/mild.
- **loss 2026-09-21:** [sector_consumer_cyclical] When score sign conflicts with sector ETF tape / breadth, cut conviction; prefer flat/mild.

### scope `sector_consumer_defensive` — wins=6 losses=9
- **win 2026-09-17:** [sector_consumer_defensive] Keep direction; shrink confidence on modest |score| when magnitude historically misses.
- **loss 2026-09-18:** [sector_consumer_defensive] When score sign conflicts with sector ETF tape / breadth, cut conviction; prefer flat/mild.
- **loss 2026-09-21:** [sector_consumer_defensive] When score sign conflicts with sector ETF tape / breadth, cut conviction; prefer flat/mild.

### scope `sector_energy` — wins=8 losses=7
- **win 2026-09-17:** [sector_energy] Keep direction; shrink confidence on modest |score| when magnitude historically misses.
- **win 2026-09-18:** [sector_energy] Keep direction; shrink confidence on modest |score| when magnitude historically misses.
- **win 2026-09-21:** [sector_energy] Keep direction; shrink confidence on modest |score| when magnitude historically misses.

### scope `sector_financial` — wins=8 losses=7
- **win 2026-09-17:** [sector_financial] Keep direction; shrink confidence on modest |score| when magnitude historically misses.
- **win 2026-09-18:** [sector_financial] Keep direction; shrink confidence on modest |score| when magnitude historically misses.
- **win 2026-09-21:** [sector_financial] Keep direction; shrink confidence on modest |score| when magnitude historically misses.

### scope `sector_healthcare` — wins=7 losses=8
- **win 2026-09-17:** [sector_healthcare] Keep direction; shrink confidence on modest |score| when magnitude historically misses.
- **loss 2026-09-18:** [sector_healthcare] When score sign conflicts with sector ETF tape / breadth, cut conviction; prefer flat/mild.
- **win 2026-09-21:** [sector_healthcare] Keep direction; shrink confidence on modest |score| when magnitude historically misses.

### scope `sector_industrials` — wins=6 losses=9
- **loss 2026-09-17:** [sector_industrials] When score sign conflicts with sector ETF tape / breadth, cut conviction; prefer flat/mild.
- **loss 2026-09-18:** [sector_industrials] When score sign conflicts with sector ETF tape / breadth, cut conviction; prefer flat/mild.
- **loss 2026-09-21:** [sector_industrials] When score sign conflicts with sector ETF tape / breadth, cut conviction; prefer flat/mild.

### scope `sector_real_estate` — wins=5 losses=10
- **loss 2026-09-17:** [sector_real_estate] When score sign conflicts with sector ETF tape / breadth, cut conviction; prefer flat/mild.
- **loss 2026-09-18:** [sector_real_estate] When score sign conflicts with sector ETF tape / breadth, cut conviction; prefer flat/mild.
- **loss 2026-09-21:** [sector_real_estate] When score sign conflicts with sector ETF tape / breadth, cut conviction; prefer flat/mild.

### scope `sector_technology` — wins=5 losses=10
- **loss 2026-09-17:** [sector_technology] When score sign conflicts with sector ETF tape / breadth, cut conviction; prefer flat/mild.
- **loss 2026-09-18:** [sector_technology] When score sign conflicts with sector ETF tape / breadth, cut conviction; prefer flat/mild.
- **loss 2026-09-21:** [sector_technology] When score sign conflicts with sector ETF tape / breadth, cut conviction; prefer flat/mild.

### scope `sector_utilities` — wins=5 losses=10
- **loss 2026-09-17:** [sector_utilities] When score sign conflicts with sector ETF tape / breadth, cut conviction; prefer flat/mild.
- **loss 2026-09-18:** [sector_utilities] When score sign conflicts with sector ETF tape / breadth, cut conviction; prefer flat/mild.
- **win 2026-09-21:** [sector_utilities] Keep direction; shrink confidence on modest |score| when magnitude historically misses.

## Open experiments

- **sector_utilities/win 2026-08-28:** [sector_utilities] On similar setups, test milder bands when |score|<4; log whether lagging tape factors overrode leading ones.
- **sector_utilities/loss 2026-09-03:** [sector_utilities] Require one extra confirming source in the dominant bucket before full weight when score sign matches this fail pattern.
- **sector_utilities/loss 2026-09-04:** [sector_utilities] Require one extra confirming source in the dominant bucket before full weight when score sign matches this fail pattern.
- **sector_utilities/loss 2026-09-09:** [sector_utilities] Require one extra confirming source in the dominant bucket before full weight when score sign matches this fail pattern.
- **sector_utilities/win 2026-09-10:** [sector_utilities] On similar setups, test milder bands when |score|<4; log whether lagging tape factors overrode leading ones.
- **sector_utilities/win 2026-09-11:** [sector_utilities] On similar setups, test milder bands when |score|<4; log whether lagging tape factors overrode leading ones.
- **sector_utilities/win 2026-09-14:** [sector_utilities] On similar setups, test milder bands when |score|<4; log whether lagging tape factors overrode leading ones.
- **sector_utilities/loss 2026-09-16:** [sector_utilities] Require one extra confirming source in the dominant bucket before full weight when score sign matches this fail pattern.
- **sector_utilities/loss 2026-09-17:** [sector_utilities] Require one extra confirming source in the dominant bucket before full weight when score sign matches this fail pattern.
- **sector_utilities/loss 2026-09-18:** [sector_utilities] Require one extra confirming source in the dominant bucket before full weight when score sign matches this fail pattern.
- **sector_utilities/win 2026-09-21:** [sector_utilities] On similar setups, test milder bands when |score|<4; log whether lagging tape factors overrode leading ones.
- **news/loss news:** [news] Raise min net weight to map a ticker; drop weak edges.

## Methodology checklist (MEMORY_CONFIRM)

1. Did any open experiment for THIS scope apply today?
2. Missing factor that would have flipped a recent loss?
3. Overweighting one bucket / double-counting one headline?
4. Sectors: S0 macro vs S1 sector factors — which failed?
5. News: event family still earning weight on 1d close?

## Retired / falsified (efficacy-gated, automatic)

- 2026-09-21: `a-utilities-xlu-call-is-built-after-a-stretch-of-risk-on-gro.md` (sector:Utilities) — topic hit 80% → 14% after activation; retired.
- 2026-09-21: `a-sector-call-has-a-scheduled-8-30-et-macro-release-pending.md` (sector:Financial) — topic hit 75% → 14% after activation; retired.
- 2026-09-21: `in-a-utilities-xlu-call-a-second-soft-inflation-print-has-al.md` (sector:Utilities) — topic hit 75% → 14% after activation; retired.
- 2026-09-21: `a-consumer-cyclical-down-call-is-driven-by-a-genuinely-negat.md` (sector:Consumer Cyclical) — topic hit 86% → 29% after activation; retired.
- 2026-09-21: `when-the-pre-fetched-commodity-tape-conflicts-with-live-sour.md` (sector:Energy) — topic hit 71% → 14% after activation; retired.
- 2026-09-21: `a-sector-call-has-a-decisively-negative-fundamental-spine-fr.md` (sector:Consumer Cyclical) — topic hit 83% → 29% after activation; retired.
- 2026-09-21: `a-utility-defensive-sector-call-is-built-on-a-carried-defens.md` (sector:Utilities) — topic hit 67% → 14% after activation; retired.
- 2026-09-21: `sector-prediction-made-when-the-sector-s-dominant-commodity.md` (sector:Energy) — topic hit 67% → 14% after activation; retired.
- 2026-09-21: `a-sector-call-has-a-scheduled-8-30-et-high-impact-macro-rele.md` (sector:Financial) — topic hit 60% → 14% after activation; retired.
- 2026-09-21: `a-fresh-top-holding-legal-regulatory-catalyst-e-g-a-trial-op.md` (sector:Communication Services) — topic hit 43% → 0% after activation; retired.
