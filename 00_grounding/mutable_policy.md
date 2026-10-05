---
status: living_policy
updated: 2026-10-05
source: src/learn_cycle.py
covers: general, sectors, news
note: Injected into general + sector PREDICT. Core output formats unchanged.
see_also: 03_scoreboard/LEARNINGS.md
---

# Mutable policy (all workflows)

Last learn_cycle: **2026-10-05**. Promoted: 1. Retired: 10. Active lessons: 206. Human digest: `03_scoreboard/LEARNINGS.md`.

## Accuracy by topic (graded window)

- **general**: 53% (8/15)
- **sector:Basic Materials**: 33% (5/15)
- **sector:Communication Services**: 20% (3/15)
- **sector:Consumer Cyclical**: 27% (4/15)
- **sector:Consumer Defensive**: 60% (9/15)
- **sector:Energy**: 53% (8/15)
- **sector:Financial**: 60% (9/15)
- **sector:Healthcare**: 40% (6/15)
- **sector:Industrials**: 47% (7/15)
- **sector:Real Estate**: 60% (9/15)
- **sector:Technology**: 40% (6/15)
- **sector:Utilities**: 40% (6/15)

## Numeric factor weights in force (engine_policy.json — applied by code, not by you)

General (B0–B7 LLM components; multiplier applied by compute_scores):
- B0_ASIA: n=21 sign-hit=0.57 → ×1.0
- B0_EUROPE: n=13 sign-hit=0.85 → ×1.25
- B1_CATALYSTS: n=26 sign-hit=0.77 → ×1.25
- B2_BONDS: n=36 sign-hit=0.36 → ×0.0
- B3_FEDPATH: n=31 sign-hit=0.45 → ×0.5
- B4_VIX: n=13 sign-hit=0.54 → ×0.5
- B5_SENTIMENT: n=22 sign-hit=0.32 → ×0.0
- B6_FUTURES: n=26 sign-hit=0.77 → ×1.25
- B7_OIL_DOLLAR: n=35 sign-hit=0.63 → ×1.0
Sectors (pooled S0–S4; per-sector overrides in engine_policy.json):
- S0_SHARED_MACRO: n=188 sign-hit=0.60 → ×1.0
- S1_SECTOR_FACTORS: n=212 sign-hit=0.57 → ×1.0
- S2_BREADTH: n=161 sign-hit=0.60 → ×1.0
- S3_FLOWS_POSITIONING: n=117 sign-hit=0.51 → ×0.5
- S4_ETF_TAPE: n=173 sign-hit=0.61 → ×1.0
Last change: hold

## Active adjustments (newest promoted lessons, truncated)

### before-the-open-channel-1-es-nq-is-locked-from-an-early-snap.md
## RULE
Ops step — re-fetch Channel 1 ES/NQ in the final pre-open window (~09:15–09:28 ET) and overwrite B6 if the later snapshot independently exceeds ±0.5%; after that refresh, if |B6| ≥ 0.5% and no unsigned CPI/NFP/FOMC binary is pending, do not emit predicted_direction=flat against that confirming B6. Do not restack paid NFP/hike-odds into B1; do not refresh-and-follow a later red B0 Asia as the US direction when B6 is the confirming tape.

## WHEN IT FIRES
Before the open, Channel 1 ES/NQ i …

### mega-cap-concentrated-cyclical-etf-xly-like-amzn-tsla-40-ai.md
## RULE
When official OFFICIAL_DIRECTION/BAND and engine predicted_direction/band diverge, grade the official block. If 09-25 conditions hold (net-negative factor sum + green ES/NQ + VIX contango + AI/index-beta top-2), the engine emit must also be flat/flat with relative lean down. Do not let skill_multipliers or index_carry promote a modest negative leading sum into an absolute down/mild the official block already rejected. Do not write a new S0/S1 weighting lesson from this scoreboard line. …

### a-scheduled-us-cash-session-where-a-valid-premarket-predicti.md
## RULE
Before finalizing any ops_fail=True grade, the grader MUST scan the run packet for a valid SCORES_BEGIN block. If one exists, do NOT mark ops_fail — instead flag a timing/path mismatch to the ops log, attempt to pair the artifact to the session, and grade against it. Only mark ops_fail=True when no valid SCORES_BEGIN block exists anywhere in the packet. This is a grader-boundary check, not a generation check.

## WHEN IT FIRES
A scheduled US cash session where a valid premarket predictio …

### all-zero-leading-s0-s4-on-a-t-1-digestion-session-where-es-a.md
## RULE
When leading_sum=0 and both ES and NQ are inside ±0.5% vs prior close with no 09-21-style cross-asset confirmation, suppress tape_anchor and index_carry from flipping the official call off the unsigned factor-card band (flat). Apply symmetrically to leftover-green and leftover-red anchors. Do not write a new XLY prompt rule — enforce 09-16 in the engine.

## WHEN IT FIRES
All-zero leading S0–S4 on a T+1 digestion session where ES and NQ are both inside ±0.5% vs prior close (no cross-asse …

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

_(+191 older active lessons not excerpted; each predict receives only its own topic's lessons via lesson_select)_

## Per-scope DO-INSTEAD

### scope `general` — wins=8 losses=7
- **loss 2026-10-01:** [general] When score sign conflicts with sector ETF tape / breadth, cut conviction; prefer flat/mild.
- **win 2026-10-02:** [general] Keep direction; shrink confidence on modest |score| when magnitude historically misses.
- **loss 2026-10-05:** [general] When score sign conflicts with sector ETF tape / breadth, cut conviction; prefer flat/mild.

### scope `news` — wins=0 losses=1
- **loss news:** [news] Only emit actions with |net| above a higher floor.

### scope `sector_basic_materials` — wins=5 losses=10
- **win 2026-09-28:** [sector_basic_materials] Keep direction; shrink confidence on modest |score| when magnitude historically misses.
- **win 2026-10-01:** [sector_basic_materials] Keep direction; shrink confidence on modest |score| when magnitude historically misses.
- **loss 2026-10-02:** [sector_basic_materials] When score sign conflicts with sector ETF tape / breadth, cut conviction; prefer flat/mild.

### scope `sector_communication_services` — wins=3 losses=12
- **loss 2026-09-28:** [sector_communication_services] When score sign conflicts with sector ETF tape / breadth, cut conviction; prefer flat/mild.
- **loss 2026-10-01:** [sector_communication_services] When score sign conflicts with sector ETF tape / breadth, cut conviction; prefer flat/mild.
- **win 2026-10-02:** [sector_communication_services] Keep direction; shrink confidence on modest |score| when magnitude historically misses.

### scope `sector_consumer_cyclical` — wins=4 losses=11
- **win 2026-09-28:** [sector_consumer_cyclical] Keep direction; shrink confidence on modest |score| when magnitude historically misses.
- **loss 2026-10-01:** [sector_consumer_cyclical] When score sign conflicts with sector ETF tape / breadth, cut conviction; prefer flat/mild.
- **loss 2026-10-02:** [sector_consumer_cyclical] When score sign conflicts with sector ETF tape / breadth, cut conviction; prefer flat/mild.

### scope `sector_consumer_defensive` — wins=9 losses=6
- **loss 2026-09-28:** [sector_consumer_defensive] When score sign conflicts with sector ETF tape / breadth, cut conviction; prefer flat/mild.
- **win 2026-10-01:** [sector_consumer_defensive] Keep direction; shrink confidence on modest |score| when magnitude historically misses.
- **win 2026-10-02:** [sector_consumer_defensive] Keep direction; shrink confidence on modest |score| when magnitude historically misses.

### scope `sector_energy` — wins=8 losses=7
- **loss 2026-09-28:** [sector_energy] When score sign conflicts with sector ETF tape / breadth, cut conviction; prefer flat/mild.
- **loss 2026-10-01:** [sector_energy] When score sign conflicts with sector ETF tape / breadth, cut conviction; prefer flat/mild.
- **loss 2026-10-02:** [sector_energy] When score sign conflicts with sector ETF tape / breadth, cut conviction; prefer flat/mild.

### scope `sector_financial` — wins=9 losses=6
- **loss 2026-09-25:** [sector_financial] When score sign conflicts with sector ETF tape / breadth, cut conviction; prefer flat/mild.
- **loss 2026-10-01:** [sector_financial] When score sign conflicts with sector ETF tape / breadth, cut conviction; prefer flat/mild.
- **win 2026-10-02:** [sector_financial] Keep direction; shrink confidence on modest |score| when magnitude historically misses.

### scope `sector_healthcare` — wins=6 losses=9
- **win 2026-09-25:** [sector_healthcare] Keep direction; shrink confidence on modest |score| when magnitude historically misses.
- **win 2026-10-01:** [sector_healthcare] Keep direction; shrink confidence on modest |score| when magnitude historically misses.
- **loss 2026-10-02:** [sector_healthcare] When score sign conflicts with sector ETF tape / breadth, cut conviction; prefer flat/mild.

### scope `sector_industrials` — wins=7 losses=8
- **loss 2026-09-25:** [sector_industrials] When score sign conflicts with sector ETF tape / breadth, cut conviction; prefer flat/mild.
- **loss 2026-10-01:** [sector_industrials] When score sign conflicts with sector ETF tape / breadth, cut conviction; prefer flat/mild.
- **loss 2026-10-02:** [sector_industrials] When score sign conflicts with sector ETF tape / breadth, cut conviction; prefer flat/mild.

### scope `sector_real_estate` — wins=9 losses=6
- **win 2026-09-25:** [sector_real_estate] Keep direction; shrink confidence on modest |score| when magnitude historically misses.
- **win 2026-10-01:** [sector_real_estate] Keep direction; shrink confidence on modest |score| when magnitude historically misses.
- **loss 2026-10-02:** [sector_real_estate] When score sign conflicts with sector ETF tape / breadth, cut conviction; prefer flat/mild.

### scope `sector_technology` — wins=6 losses=9
- **win 2026-09-28:** [sector_technology] Keep direction; shrink confidence on modest |score| when magnitude historically misses.
- **win 2026-10-01:** [sector_technology] Keep direction; shrink confidence on modest |score| when magnitude historically misses.
- **loss 2026-10-02:** [sector_technology] When score sign conflicts with sector ETF tape / breadth, cut conviction; prefer flat/mild.

### scope `sector_utilities` — wins=6 losses=9
- **loss 2026-09-25:** [sector_utilities] When score sign conflicts with sector ETF tape / breadth, cut conviction; prefer flat/mild.
- **loss 2026-10-01:** [sector_utilities] When score sign conflicts with sector ETF tape / breadth, cut conviction; prefer flat/mild.
- **loss 2026-10-02:** [sector_utilities] When score sign conflicts with sector ETF tape / breadth, cut conviction; prefer flat/mild.

## Open experiments

- **sector_utilities/win 2026-09-14:** [sector_utilities] On similar setups, test milder bands when |score|<4; log whether lagging tape factors overrode leading ones.
- **sector_utilities/loss 2026-09-16:** [sector_utilities] Require one extra confirming source in the dominant bucket before full weight when score sign matches this fail pattern.
- **sector_utilities/loss 2026-09-17:** [sector_utilities] Require one extra confirming source in the dominant bucket before full weight when score sign matches this fail pattern.
- **sector_utilities/loss 2026-09-18:** [sector_utilities] Require one extra confirming source in the dominant bucket before full weight when score sign matches this fail pattern.
- **sector_utilities/win 2026-09-21:** [sector_utilities] On similar setups, test milder bands when |score|<4; log whether lagging tape factors overrode leading ones.
- **sector_utilities/win 2026-09-22:** [sector_utilities] On similar setups, test milder bands when |score|<4; log whether lagging tape factors overrode leading ones.
- **sector_utilities/loss 2026-09-23:** [sector_utilities] Require one extra confirming source in the dominant bucket before full weight when score sign matches this fail pattern.
- **sector_utilities/win 2026-09-24:** [sector_utilities] On similar setups, test milder bands when |score|<4; log whether lagging tape factors overrode leading ones.
- **sector_utilities/loss 2026-09-25:** [sector_utilities] Require one extra confirming source in the dominant bucket before full weight when score sign matches this fail pattern.
- **sector_utilities/loss 2026-10-01:** [sector_utilities] Require one extra confirming source in the dominant bucket before full weight when score sign matches this fail pattern.
- **sector_utilities/loss 2026-10-02:** [sector_utilities] Require one extra confirming source in the dominant bucket before full weight when score sign matches this fail pattern.
- **news/loss news:** [news] Raise min net weight to map a ticker; drop weak edges.

## Methodology checklist (MEMORY_CONFIRM)

1. Did any open experiment for THIS scope apply today?
2. Missing factor that would have flipped a recent loss?
3. Overweighting one bucket / double-counting one headline?
4. Sectors: S0 macro vs S1 sector factors — which failed?
5. News: event family still earning weight on 1d close?

## Retired / falsified (efficacy-gated, automatic)

- 2026-10-05: `a-utilities-xlu-call-is-built-after-a-stretch-of-risk-on-gro.md` (sector:Utilities) — topic hit 80% → 14% after activation; retired.
- 2026-10-05: `a-sector-call-has-a-scheduled-8-30-et-macro-release-pending.md` (sector:Financial) — topic hit 75% → 14% after activation; retired.
- 2026-10-05: `in-a-utilities-xlu-call-a-second-soft-inflation-print-has-al.md` (sector:Utilities) — topic hit 75% → 14% after activation; retired.
- 2026-10-05: `a-consumer-cyclical-down-call-is-driven-by-a-genuinely-negat.md` (sector:Consumer Cyclical) — topic hit 86% → 29% after activation; retired.
- 2026-10-05: `a-mega-cap-duration-heavy-consumer-cyclical-etf-xly-like-amz.md` (sector:Consumer Cyclical) — topic hit 71% → 14% after activation; retired.
- 2026-10-05: `when-the-pre-fetched-commodity-tape-conflicts-with-live-sour.md` (sector:Energy) — topic hit 71% → 14% after activation; retired.
- 2026-10-05: `a-sector-call-has-a-decisively-negative-fundamental-spine-fr.md` (sector:Consumer Cyclical) — topic hit 83% → 29% after activation; retired.
- 2026-10-05: `a-utility-defensive-sector-call-is-built-on-a-carried-defens.md` (sector:Utilities) — topic hit 67% → 14% after activation; retired.
- 2026-10-05: `sector-prediction-made-when-the-sector-s-dominant-commodity.md` (sector:Energy) — topic hit 67% → 14% after activation; retired.
- 2026-10-05: `a-sector-call-has-a-scheduled-8-30-et-high-impact-macro-rele.md` (sector:Financial) — topic hit 60% → 14% after activation; retired.
