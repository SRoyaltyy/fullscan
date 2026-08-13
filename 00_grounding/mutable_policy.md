---
status: living_policy
updated: 2026-08-13
source: src/learn_cycle.py
covers: general, sectors, news
note: Injected into general + sector PREDICT. Core output formats unchanged.
---

# Mutable policy (all workflows)

Last learn_cycle: **2026-08-13**. Promoted: 36 lesson file(s).

## Accuracy by topic (graded window)

- **general**: 78% (7/9)
- **sector:Basic Materials**: 67% (2/3)
- **sector:Communication Services**: 33% (1/3)
- **sector:Consumer Cyclical**: 67% (2/3)
- **sector:Consumer Defensive**: 33% (1/3)
- **sector:Energy**: 67% (2/3)
- **sector:Financial**: 67% (2/3)
- **sector:Healthcare**: 67% (2/3)
- **sector:Industrials**: 33% (1/3)
- **sector:Real Estate**: 67% (2/3)
- **sector:Technology**: 33% (1/3)
- **sector:Utilities**: 67% (2/3)

## Active adjustments (promoted lessons)

### a-basic-materials-xlb-call-builds-a-severe-up-score-from-str.md
---
trigger_pattern: "A Basic Materials/XLB call builds a severe-up score from strong structural supply/monetary-metal factors (record copper, DRC/Congo export ban, gold/silver surge, ultra-low inventories), but the current 1d XLB relative return is modest (<0.5%), a China-demand/PMI contraction is explicitly present as an offset, an active geopolitics/oil headline can flip the broad equity tape risk-off, and the analysis text says “temper.” The deterministic output still emits severe because component scores and multiplier are not adjusted to match the tempered conclusion."
corrected_behavior: "When the text concludes “temper” and the 1d XLB relative return is <0.5% with an active geopolitics/oil risk-off headline and an explicit China/PMI offset, score S0 as 0 or negative for the equity tape, cap S1 at 2 to reflect the offset, set S4 to 0 if the 1d tape is only modestly positive, and reduce the multiplier/band so the output is up/notable or up/mild, not up/severe. Direction may remai

### a-bond-proxy-rate-sensitive-defensive-sector-staples-has-bee.md
---
trigger_pattern: "A bond-proxy/rate-sensitive defensive sector (staples) has been underperforming on 1w/1m because of a real-yield/duration headwind, and a scheduled high-impact CPI print is the dominant catalyst. Premarket equity futures are risk-on and the sector has positive leading flow/rotation signals (e.g., first net inflows in months, contrarian defensive-rotation calls). The model treats the CPI print itself as a risk-off trigger for defensives, scores S0_SHARED_MACRO negative, and emits down/mild — ignoring that an in-line/cool CPI would relieve the duration headwind and can make the defensive ETF outperform."
corrected_behavior: "Before scoring S0 negative for a rate-sensitive/bond-proxy sector on a CPI day, identify the sector’s dominant driver. If the dominant driver is real-yield/duration pressure and the CPI resolution is genuinely two-sided, do not force a negative S0 merely because a CPI print exists. When premarket futures are risk-on and leading flow/rotation sig

### a-bond-proxy-rate-sensitive-sector-has-been-sold-off-for-mul.md
---
trigger_pattern: "A bond-proxy/rate-sensitive sector has been sold off for multiple sessions by a specific macro driver (rising yields). On the next call, pre-fetched data shows that driver is now easing (1d/1w yields tick down) and the sector ETF already shows 1d relative outperformance vs SPY. The analysis text identifies this as the key new input and may even call it a “leading positive” divergence, but component scores remain at or below neutral and the deterministic output continues to emit down/flat. The miss is amplified when a geopolitical/oil supply-shock risk-off headline is knowable at open, because it adds a defensive bid into the same sector."
corrected_behavior: "When the named prior driver is easing and the 1d tape is already showing relative outperformance, do not continue the prior down call mechanically. Re-score the sector components to reflect the inflection: S0 should not remain negative if the macro driver is easing or if geopolitical risk-off is active; S1 an

### a-communication-services-xlc-call-faces-a-live-geopolitical.md
---
trigger_pattern: "A Communication Services/XLC call faces a live geopolitical/oil supply-shock risk (e.g., Hormuz/oil near $90), rising real yields, unresolved Alphabet/Meta AI-capex/FCF negatives, and XLC persistently lagging SPY on 1d/1w/1m, while premarket futures are positive and a scheduled CPI print is moderate. The correct output is down/mild; positive futures and a benign CPI are offsets, not reasons to flip the sector call to up."
corrected_behavior: "No correction required. Continue to cap at flat/down caution when the 2026-08-11 geopolitical/oil risk-off lesson is active, even with positive futures. Optional refinement: when both Alphabet and Meta are under fresh negative AI-capex/FCF catalysts at the open, S1 could be scored negative rather than neutral, since XLC’s two-stock concentration can turn a mild absolute decline into notable relative underperformance."
falsifier: "The pattern would be falsified if XLC rose or matched SPY in a future setup where Alphabet and Me

### a-consumer-cyclical-xly-call-is-built-from-prior-period-cons.md
---
trigger_pattern: "A Consumer Cyclical (XLY) call is built from prior-period consumer-fundamental positives (falling gasoline, resilient labor, strong travel/RevPAR) while a fresh knowable-at-open geopolitical/oil supply shock is active — e.g., Iran/Hormuz, Brent near $90 — and/or a high-impact CPI print is looming. Futures are flat-to-negative and XLY’s mega-cap concentration is not confirmed by the 1d tape. The morning treats the live energy-cost risk as a stale/inverted factor, keeps S1/S3/S4 positive, and emits an absolute up bias."
corrected_behavior: "If an active geopolitical/oil supply shock is knowable at the open, treat S0 as the dominant score and make it more negative for Consumer Cyclical (e.g., S0 = -2). Set the live energy-cost factor negative when oil is spiking rather than carrying stale “gas relief.” Do not let S1/S3/S4 positives produce an absolute up call when futures are flat-to-negative and a risk-off headline is active. Bias to down/mild or down/flat until the

### a-defensive-staples-sector-prediction-is-set-to-flat-solely.md
---
trigger_pattern: "A defensive/staples sector prediction is set to flat solely because premarket US futures are flat and global sessions are mixed, while an active geopolitical/oil supply-shock headline (e.g., Strait-of-Hormuz style impasse) and/or a scheduled high-impact CPI print is present in the news cycle. The model treats the day as neutral/risk-on, scores S0_SHARED_MACRO at 0 or positive, and emits flat/flat — but a defensive ETF can still fall modestly in absolute terms on broad risk-off while outperforming SPY relatively, so flat/flat is a graded miss."
corrected_behavior: "When an active geopolitical/oil/CPI risk-off catalyst is present, score S0_SHARED_MACRO negative/risk-off rather than neutral, keep magnitude capped at mild (not notable), and emit down/mild when the defensive ETF is already underperforming on the multi-day tape. Do not let a flat premarket futures print alone justify an absolute flat call. A defensive sector can fall about -0.3% absolutely while matchin

### a-financials-sector-call-has-strongly-positive-structural-fa.md
---
trigger_pattern: "A Financials sector call has strongly positive structural factor scores (curve steepening, credit tightening, capital-markets/trading strength) and a positive shared-macro score taken from pre-market risk-on indicators, but the sector ETF tape at open is flat/neutral (S4_ETF_TAPE ≈ 0.0) and divergence_flagged is True. A live geopolitical/oil supply-shock headline (e.g., Iran/Hormuz) and/or a high-impact CPI print is knowable at open. The analysis text explicitly says “damp/mild,” but the deterministic score still emits up/severe because the multiplier and component scores are left unchanged. Financials may still outperform SPY relatively, but that does not make the absolute move up or severe."
corrected_behavior: "When S4 is flat/neutral and divergence_flagged=True, cap the emitted magnitude band at mild/flat and use multiplier ≤1.0 unless the absolute tape confirms a same-day move. If a geopolitical/oil supply-shock or high-impact CPI risk is active at open, clas

### a-healthcare-xlv-call-after-a-strong-multi-week-relative-run.md
---
trigger_pattern: "A Healthcare/XLV call after a strong multi-week relative run with many carried positive factors, but the pre-fetched 1d tape is negative absolute and a fresh, knowable-at-open negative catalyst hits a high-weight XLV sub-industry — e.g., a Medicare Advantage rate proposal shock to managed care (UNH, HUM, CVS). The analysis may cite the negative catalyst but treat it as a “caution” or “partial offset” while keeping S1/S4 positive and emitting up/notable."
corrected_behavior: "When a fresh negative policy/regulatory catalyst directly hits a high-weight sub-industry and the 1d XLV tape is already negative, reweight S1 to neutral/negative, set S4 to ≤0 for absolute direction, and do not use 3d/1w/1m relative strength to justify an absolute up call. The sector may still outperform SPY relatively, so the call should be flat/down unless a larger same-day positive catalyst is explicitly present."
falsifier: "Fails if a future XLV setup has a negative 1d tape and negative 

### a-high-conviction-technology-xlk-call-with-a-benign-schedule.md
---
trigger_pattern: "A high-conviction Technology/XLK call with a benign scheduled macro print, fresh mega-cap/AI-infrastructure earnings beats, strongly positive Nasdaq futures, and no leading-vs-tape divergence. The narrative and deterministic pipeline agree, and the outcome fully confirms the call."
corrected_behavior: "No change required. Keep the freshness gate: fresh index-relevant AI/mega-cap catalysts plus non-negative/risk-on futures allow the model to override crowding and real-yield dampeners. Do not generalize this to stale catalysts or negative futures. Also avoid over-weighting S3 crowding as a magnitude cap when fresh catalysts have already drawn flows."
falsifier: "This no-error pattern is falsified if XLK declines despite an in-line CPI, fresh index-relevant AI-infra earnings beats, and strongly positive NQ futures at the open. It is also falsified if XLK rallies on stale catalysts or weak futures. Future calls must require all three conditions — fresh catalysts, beni

### a-long-duration-rate-sensitive-sector-reits-has-been-lagging.md
---
trigger_pattern: "A long-duration/rate-sensitive sector (REITs) has been lagging, real/nominal yields tick up into a scheduled CPI print, and premarket equity futures are positive. The model applies an active “CPI imminent => S0 negative for REITs” lesson without treating the CPI surprise as binary. It also sees a lower 2Y pre-CPI but treats it as a minor offset rather than an easing-expectation tell that a cool print could flip the rate spine and rally rate-sensitive assets."
corrected_behavior: "When CPI is imminent for a long-duration/rate-sensitive sector, treat the catalyst as two-sided. Before scoring S0, check the pre-CPI yield-curve positioning: if the 2Y is drifting lower / easing expectations are visible, do not default S0 to -1; score at least 0 and consider positive S0, because a cool/in-line CPI would relieve the duration headwind and can make the sector rally. Reserve negative S0 for pre-CPI curves pricing hot/higher-for-longer outcomes (2Y rising, no easing tell) or 

### a-rate-sensitive-defensive-sector-utilities-has-a-confirmed.md
---
trigger_pattern: "A rate-sensitive/defensive sector (utilities) has a confirmed positive catalyst (inline CPI → second session of yield relief) and a strong structural narrative (AI data-center load growth), but the broad tape is risk-on with growth/tech leading and a same-day sector-narrative headwind appears (e.g., a credible research report questioning AI power-demand realization). The model correctly flips direction to up but over-corrects magnitude by letting structural/flow component scores dominate, producing “notable” when the defensive bid is structurally capped."
corrected_behavior: "When the broad tape is risk-on with tech leading and a same-day or fresh sector-narrative headwind challenges the structural thesis, cap the magnitude to “mild” unless there is durable sector leadership (sustained 1d/3d relative outperformance, breadth expansion, or confirmed inflows). Reconcile the narrative magnitude with component scores: do not let S1 +2 and S3 +1 drive a notable call whe

### a-scheduled-high-impact-macro-data-release-nfp-cpi-fomc-with.md
---
trigger_pattern: "A scheduled high-impact macro data release (NFP/CPI/FOMC) with a soft/expected-easing narrative is the flagged dominant event-risk of the day, while a separate geopolitical de-escalation story is generating positive overnight momentum; the market's actual driver becomes the macro print's repricing of the Fed path, and the geopolitical/oil catalyst fades or flips as attention shifts."
corrected_behavior: "When a scheduled high-impact macro release is flagged as the day's dominant event risk, set the macro-linked components (Fed path B3, bonds B2) from the expected-print conditional under the regime lens — a soft-print-expected day under bad-news-good cannot carry a negative B3. Independently cap/discount geopolitical-oil components (B1/B7) to at most ±0.5 unless US futures independently confirm them. Add a final narrative-vs-scores consistency check: any narrative sentence claiming a macro print is bullish requires the Fed-path/bond component to carry non-negative 

### active-geopolitical-oil-supply-risk-off-e-g-iran-hormuz-and.md
---
trigger_pattern: "Active geopolitical oil-supply risk-off (e.g., Iran/Hormuz) and/or an imminent high-impact CPI print is knowable at open, while the target sector is long-duration/rate-sensitive (REITs). Premarket equity futures are flat and global equity indices are mildly positive, so the model scores S0_SHARED_MACRO as 0 and treats 1d/1w real-yield easing as a sufficient offset, even though the 1m real-yield trend is still elevated and the sector ETF has been chronically lagging."
corrected_behavior: "When an active geopolitical/oil risk-off story and/or imminent CPI is present for long-duration REITs, score S0_SHARED_MACRO negative rather than neutral, even if premarket equity futures are flat and global equity indices are mildly positive. Treat the 1m real-yield trend as the operative duration horizon for a daily REIT call; 1d/1w easing is not a sufficient positive offset when 1m real yields remain elevated. The appropriate call on 2026-08-11 was down/mild, not down/flat."
fa

### an-effectively-two-stock-sector-etf-meta-alphabet-heavy-has.md
---
trigger_pattern: "An effectively two-stock sector ETF (Meta + Alphabet heavy) has a bullish case built from structural positives that are really one underlying thesis, prior-week flows are treated as same-day support, the largest holdings have unresolved capex/FCF vulnerability, and a fresh geopolitical/oil supply-shock risk is active at the open while futures are flat. The model emits up/notable from structural positives instead of flat/down caution."
corrected_behavior: "Deduplicate sector positives: ad-spend recovery + AI monetization = one ad/AI thesis; rotation + sector inflows = one flow observation. Before emitting an up call on XLC, check for knowable-at-open geopolitical/oil/high-impact-print suppressors. If a live geopolitical risk-off signal is present, score S0 negatively and cap the call at flat/down caution. Flat futures should be treated as non-confirmation, not as bullish confirmation. Do not extend “mega-cap-earnings-over-macro-drag” to a live geopolitical supply s

### an-energy-xle-call-is-scored-up-severe-from-a-geopolitical-s.md
---
trigger_pattern: "An Energy/XLE call is scored up/severe from a geopolitical supply-shock catalyst whose oil-price sign is correct, but the catalyst has already driven a large relative run in XLE (3d/1w rel > +4%) and is therefore largely priced in; a same-day official report (IEA/OPEC/EIA) contains demand-destruction or two-sided supply/demand signals, and the ETF's current-day relative tape is not confirming fresh leadership at the open."
corrected_behavior: "Before assigning severe to Energy, decompose S1 into (a) catalyst sign/freshness and (b) expected transmission to XLE. If XLE has already rallied 1w rel > +4% on the same geopolitical narrative, treat the headline as a continuation, not a new shock. If a same-day official report contains demand destruction or a two-sided signal, apply it as a direct S1 negative offset. Cap S1 at +1.0 and multiplier at 1.0 unless the current-day 1d tape shows fresh XLE relative leadership (not just prior-day momentum). With a continuing-but-s

### consumer-cyclical-xly-is-called-down-mild-from-macro-caution.md
---
trigger_pattern: "Consumer Cyclical (XLY) is called down/mild from macro caution, but actual outcome is down/notable because a same-day company-specific shock hits one of XLY's top 2-3 mega-cap holdings (e.g., CEO leadership news, single-name earnings/valuation shock) while the broad tape (SPY) is flat/up and the scheduled macro catalyst is benign. The specific shock is absent from all pre-open channels and is therefore not knowable at the open."
corrected_behavior: "Do not retrofit a magnitude correction. Keep the pre-open output when it is supported by the available data; do not systematically change down/mild to down/notable merely because XLY has high single-name concentration. In concentrated sectors, note explicitly that magnitude bands are less reliable and set confidence lower, but do not manufacture a catalyst that is not in the evidence."
falsifier: "A future Consumer Cyclical call with the same concentration and no pre-open idiosyncratic catalyst that closes flat/mild wo

### correct-severe-up-energy-call-driven-by-a-knowable-at-open-g.md
---
trigger_pattern: "Correct severe-up Energy call driven by a knowable-at-open geopolitical crude supply shock; no corrective trigger pattern established."
corrected_behavior: "No score change needed. For future Energy sector shocks, if the catalyst is sector-specific geopolitical supply and the broad tape is flat/red, classify the regime as sector_shock rather than broad risk_on; keep S0 muted and let S1 plus confirming tape carry the prediction."
falsifier: "Same setup producing XLE not severe or negative would falsify the crude-surge ⇒ XLE severe-up pattern."
current_behavior: "Predicted XLE up/severe from the oil surge (S1=2.0, score 13.0), but labeled the shared-macro regime as broad risk_on even though SPY was flat/red and the move was sector-specific."
evidence_cited: "XLE +4.66%, SPY -0.03%, rel +4.69%; Brent +3.3%, WTI +3.1%; Hormuz closure/tanker strikes; morning predicted up/severe and both direction and magnitude hit."
error_category: "NONE"
scope: "general"
date: "2026-0

### dominant-positive-catalyst-geopolitical-de-escalation-strong.md
---
trigger_pattern: "Dominant positive catalyst (geopolitical de-escalation / strong earnings) is confirmed by Europe and US futures, but a single idiosyncratic Asia market crash (e.g., Kospi chip unwind) drags the Asia composite negative; the market follows the confirmed positive catalyst and ignores the outlier."
corrected_behavior: "When Asia composite negativity is driven by a single >4% idiosyncratic market move while other Asia markets are mixed and Europe + US futures are clearly positive, set B0 to 0/-0.5 rather than -1; do not let that outlier temper a dominant, independently confirmed catalyst; if the leading sum and futures are both positive and no divergence is flagged, do not cap the prediction in the MILD band solely on secondary macro noise."
falsifier: "If this exact setup recurs (dominant positive catalyst + Europe/futures positive + single-market Asia outlier) and SPX still closes <1.0% or negative, this lesson must be revised to treat the Asia outlier as a real syst

### long-duration-technology-semis-prediction-turns-up-on-a-stal.md
---
trigger_pattern: "Long-duration technology/semis prediction turns up on a stale positive mega-cap catalyst carried from prior context, while a fresh knowable-at-open inflation/geopolitical shock is present, real yields are rising, and the yield-equity correlation is strongly negative — with crowded long positioning making the sector asymmetrically vulnerable to risk-off."
corrected_behavior: "Before using any catalyst from prior context, verify it is fresh for the current session; archived catalysts cannot override a live macro shock. When real yields are elevated/rising and the 5-day 10Y-SPX correlation is strongly negative, treat the macro read as net negative for long-duration tech even if VIX is calm and credit spreads are tightening. When the most-crowded trade faces a fresh inflation/geopolitical shock, forbid/avoid an up call or sharply reduce magnitude and confidence; if direction is uncertain, prefer flat/down rather than up/notable."
falsifier: "The rule would be falsifie

### mega-cap-earnings-over-macro-drag.md
---
trigger_pattern: "Strong positive mega-cap earnings/AI momentum coincides with negative macro/geopolitical headlines (oil spike, China PMI miss, hawkish Fed) — market follows earnings unless futures independently confirm weakness."
corrected_behavior: "When Channel 2 has an index-relevant positive mega-cap earnings catalyst and B6 futures are not negative: set B1 at least 0; cap B2/B3/B7 combined drag; FORBID predicted_direction=down unless futures or leading internals independently confirm weakness."
falsifier: "Wrong if mega-cap earnings green but SPX still falls that day while futures were non-negative at the open."
current_behavior: "Over-weight oil/China/Fed negatives vs Amazon/MSFT-type prints; call down."
evidence_cited: "2026-07-31 predicted down/mild; actual SPX +0.70%; Amazon +10% / Mag7 led."
error_category: "B"
scope: "general"
date: "2026-07-31"
status: "active"
schema_ok: "true"
---

## RULE
When Channel 2 has an index-relevant positive mega-cap earnings catalyst and 

### mixed-catalyst-session-positive-geopolitical-de-escalation-h.md
---
trigger_pattern: "Mixed-catalyst session — positive geopolitical de-escalation headline versus a single-market Asia chip crash, with US futures flat and giving no confirmation in either direction; low-conviction flat-band call is the correct calibration."
corrected_behavior: "No change. When leading indicators are offsetting and futures give no confirmation, hold the flat band at low confidence; weight a positive headline only when Europe/US futures confirm it (08-03 rule), and weight an Asia crash only when futures independently turn negative (07-31 rule)."
falsifier: "If this exact pattern recurs (positive de-escalation headline + Asia chip crash + flat futures) and SPX moves >=1.0% in either direction, the flat-band default is wrong and must be recalibrated. Also, if a futures-confirmed positive catalyst ever yields a down day, candidate 08-03 must be revised."
current_behavior: "Predicted down/flat (total -2.0, multiplier 1.0, confidence 0.55) on offsetting signals — Kospi -4.5

### no-corrective-trigger-full-hit-on-both-axes-under-an-activel.md
---
trigger_pattern: "No corrective trigger — full hit on both axes under an actively-fired mega-cap-earnings-over-macro-drag pattern (index-relevant AI earnings catalyst + non-negative futures + negative macro/geopolitical headlines)."
corrected_behavior: "No change. Continue enforcing the mega-cap-earnings-over-macro-drag forced-checklist at predict time; maintain magnitude humility on borderline flat calls (0.26% vs 0.30% band edge)."
falsifier: "The mega-cap rule is wrong if an index-relevant positive mega-cap earnings catalyst with non-negative futures is followed by a down SPX day — not observed today, so the lesson stands."
current_behavior: "Pipeline predicted up/flat; applied the standing mega-cap-earnings-over-macro-drag rule (B1=+1, capped B2/B3/B7 at −0.5 each), total score 2.25, multiplier 1.0. Actual SPX +0.26% — direction and magnitude both hit."
evidence_cited: "2026-08-12 predicted up/flat, actual SPX +0.26% (up/flat); direction_hit True, magnitude_hit True."
error_cat

### no-error-full-direction-magnitude-hit-no-corrective-trigger.md
---
trigger_pattern: "No error — full direction/magnitude hit; no corrective trigger pattern established"
corrected_behavior: "No correction required; continue treating a verified negative catalyst cluster as sufficient to override positive global-session/futures momentum at low confidence"
falsifier: "N/A for this hit; for candidate 07-31, a repeat positive-futures/positive-earnings setup that fades would falsify it"
current_behavior: "Predicted down/flat on negative AI-capex and geopolitical catalysts despite strong Asia and positive futures; output was directionally/magnitude accurate"
evidence_cited: "2026-08-05 — predicted DOWN/FLAT at -1.575; actual SPX -0.17%, direction HIT and magnitude HIT; no factor mismatch"
error_category: "NONE"
scope: "general"
date: "2026-08-05"
status: "active"
occurrences: "1"
promoted_on: "2026-08-13"
sources: "['2026-08-05_lesson.md']"
schema_ok: "true"
---

## RULE
No correction required; continue treating a verified negative catalyst cluster as suf

### none-no-corrective-trigger-established-correct-down-notable.md
---
trigger_pattern: "None — no corrective trigger established. Correct down/notable Utilities call was driven by knowable rising real/nominal yields plus sustained sector-level relative underperformance."
corrected_behavior: "No correction required. Preserve the same process: treat rates and real yields as one macro channel, confirm with sector ETF tape, avoid double-counting, and keep structural positives from overweighting a rate-driven defensive selloff."
falsifier: "A future Utilities call with the same high-yield, high-real-rate conditions and same pre-existing XLU relative weakness that fails to produce down/notable relative performance would weaken the rate-driven bond-proxy thesis. No lesson is being added, so no direct falsifier is required."
current_behavior: "Utilities predicted down/notable (-8.8, multiplier 0.8) using high 10Y/30Y/real yields, risk-on rotation pressure, and XLU's clear 1w/1m relative underperformance; structural load-growth/nuclear positives were used onl

### ops-missing-predict-file.md
---
trigger_pattern: "Scheduled trading day where premarket YYYY-MM-DD_predict.md is absent or empty at open/grading time."
corrected_behavior: "OPS: verify predict.md exists with SCORES_BEGIN before 09:30 ET; retry if missing. At grading mark ops_fail=true and leave direction_hit/magnitude_hit null — never score as a market miss."
falsifier: "Wrong if a present predict.md is marked ops_fail, or a missing file is still counted direction_hit=false."
current_behavior: "Outcome grades null prediction as miss."
evidence_cited: "2026-08-02 and 2026-08-08 missing predict files."
error_category: "D"
scope: "ops"
date: "2026-08-08"
status: "active"
schema_ok: "true"
---

## RULE
OPS: verify predict.md exists with SCORES_BEGIN before 09:30 ET; retry if missing. At grading mark ops_fail=true and leave direction_hit/magnitude_hit null — never score as a market miss.

## WHEN IT FIRES
Scheduled trading day where premarket predict.md is absent or empty.

## WRONG IF
Wrong if a present predict.md is

### premarket-tape-shows-no-directional-confirmation-global-sess.md
---
trigger_pattern: "Premarket tape shows no directional confirmation: global sessions flat (±0.5%), US index futures flat (±0.5%), and overnight catalysts are moderate (|B1| around 1) with headline risk (stalled geopolitical deal, oil spike, looming CPI) but no panic selloff — S&P pausing near records after a prior rally; no index-relevant earnings catalyst active."
corrected_behavior: "When B6=0 (±0.5%), B0=0 (±0.5%), and no index-relevant earnings catalyst is active: cap |B1| at 0.5 and |B7| at 0.5 (a headline that does not move futures is not worth full weight, especially in a bad-news-good regime), and force the predicted magnitude band to FLAT — do not let a moderate-catalyst raw sum of ~-4.0 produce a mild call absent futures confirmation. Only allow a mild/severe band if a non-flat futures move or a dominant |B1|≥2 event independently confirms a ≥0.5% move. If an earnings catalyst is present, apply mega-cap-earnings-over-macro-drag instead."
falsifier: "If this exact trigger r

### rule.md
---
trigger_pattern: ""
corrected_behavior: ""
falsifier: ""
current_behavior: ""
evidence_cited: ""
error_category: "NONE"
scope: "general"
date: "2026-08-12"
status: "active"
occurrences: "1"
promoted_on: "2026-08-13"
sources: "['2026-08-12_sector_healthcare_lesson.md']"
schema_ok: "true"
---

## RULE


## WHEN IT FIRES


## WRONG IF


## EVIDENCE


(learn_cycle promote)

### scheduled-trading-day-opens-with-the-premarket-prediction-fi.md
---
trigger_pattern: "Scheduled trading day opens with the premarket prediction file (YYYY-MM-DD_predict.md) missing or empty at open/grading time — a pipeline failure upstream of reasoning, not a market condition."
corrected_behavior: "Deploy a pre-open watchdog: verify YYYY-MM-DD_predict.md exists and is non-empty before market open; if missing, retry generation, alert loudly, and mark the run 'no baseline — ungraded' instead of grading a default miss. At grading time, a missing baseline is always D-category pipeline error, never a reasoning miss. Consolidate with candidate 2026-08-08_lesson.md and promote; do not create a duplicate lesson."
falsifier: "If the watchdog is live and a scheduled trading day still reaches grading with no predict file and no loud alert, the deployment is broken — fix the tooling, not the lesson. If the watchdog blocks valid but late-correct baselines, refine to allow flagged late baselines."
current_behavior: "2026-08-09: no prediction file existed at gra

### scheduled-trading-day-where-the-premarket-prediction-file-yy.md
---
trigger_pattern: "Scheduled trading day where the premarket prediction file (YYYY-MM-DD_predict.md) is absent or empty at grading time — or, better, at market open — so no scored baseline exists and the run is graded as a miss by default."
corrected_behavior: "Add a premarket pipeline guard before 09:30 ET: verify YYYY-MM-DD_predict.md exists and is non-empty with complete B0-B7/scores. If missing, fail loudly, retry generation, and alert. If still missing at grading time, mark the run as 'unscorable' rather than a direction/magnitude miss."
falsifier: "If the file-existence guard is implemented and a scheduled trading day still opens with no prediction file — or scoring still records false/false against None — this lesson must be revised to diagnose the scheduler/fetch root cause rather than only enforcing file presence."
current_behavior: "Pipeline executed scoring against a missing prediction file; no prediction was generated for 2026-08-08, direction/magnitude were recorded as 

### when-a-defensive-sector-prediction-is-directionally-negative.md
---
trigger_pattern: "When a defensive-sector prediction is directionally negative but the premarket/global tape shows no directional confirmation (flat US futures, flat global sessions) and the analysis text itself flags an offsetting/dampening signal (e.g., negative 10Y-SPX correlation, cooling risk appetite, nascent defensive bid), the deterministic score still emits a high-magnitude band solely from structural factor scores."
corrected_behavior: "When the analysis text identifies a dampening factor that reduces conviction, the final magnitude band should be capped at mild/flat unless the tape independently confirms notable movement. Structural negatives can still justify a negative direction, but a high negative total like -9.6 requires confirmation from futures/tape, not just S0–S4 factor scores."
falsifier: "If a future Consumer Defensive prediction with flat futures/no tape confirmation nonetheless produces a notable move (e.g., ≥0.75% in the predicted direction), the “cap magni

### when-a-geopolitical-supply-shock-headline-is-active-but-inte.md
---
trigger_pattern: "When a geopolitical supply-shock headline is active but internally conflicting — one source says a deal is agreed / strikes called off, another says it is stalled / demands unresolved — and the pre-fetched Channel 1 oil-futures tape shows a move consistent with the premature/deal-resolved headline, do not treat that pre-fetched tape as authoritative for an Energy call. The oil-price sign is the load-bearing factor for S1; if it is stale or wrong, it cascades into S1, the divergence check, the multiplier, and the final direction call."
corrected_behavior: "For Energy, before scoring S1 and final direction, verify the current oil-price sign against at least one independent live source. If the pre-fetched Channel 1 oil tape conflicts with active headlines or with independent quotes, do not treat it as authoritative; re-score using the live-verified sign, or downgrade confidence/neutral if unresolved. If live oil is up and the geopolitical premium is re-expanding, set

### when-a-technology-xlk-narrative-explicitly-applies-an-active.md
---
trigger_pattern: "When a Technology/XLK narrative explicitly applies an active risk-off reflect lesson and says “flat” or “flat/down,” but the deterministic pipeline still emits “up” because the signed component effects were lost in aggregation (e.g., leading_sum is computed from absolute magnitudes rather than the signed S0+S1+S2+S3 sum), the final graded prediction must be reconciled. Relative tape strength vs SPY must not be converted into absolute up direction; XLK can fall while outperforming SPY."
corrected_behavior: "When the narrative override and the deterministic output disagree, resolve the conflict before finalizing. Use signed component scores rather than absolute magnitudes when computing the leading sum and direction. If a fresh macro shock + crowded tech + stale catalysts is knowable at open, emit flat/down — not up. Also, treat relative outperformance vs SPY as a relative note, not as evidence for an absolute up move. If the fresh catalyst is after-hours, do not co

### when-an-active-sector-lesson-says-a-live-two-sided-geopoliti.md
---
trigger_pattern: "When an active sector lesson says a live two-sided geopolitical/oil supply-shock headline caps magnitude, and the next prediction narrative concludes “oil flat / no overhang” based only on low CL/BZ percentage prints, verify the oil claim against Brent's absolute level and shipping-attack headlines before emitting up/severe. On scheduled CPI days, in-line CPI relief tends to rotate into growth/tech, not into oil-sensitive cyclicals; if the geopolitical oil shock is still active, Industrials can close flat/lag SPY even when premarket futures are risk-on."
corrected_behavior: "Before finalizing an Industrials severe call, check (1) Brent absolute level and overnight move, (2) shipping-attack/Hormuz headline status, and (3) whether the active 08-11 lesson trigger is still firing. If oil is rising or attacks are active, do not describe the tape as “flat”; cap S0 at 0 or negative, set the multiplier ≤ 1.0, and reduce the magnitude band to at most up/notable. On macro-e

### when-an-industrials-xli-severe-up-call-is-built-on-strong-st.md
---
trigger_pattern: "When an Industrials/XLI severe-up call is built on strong structural sector factors (ISM expansion, AI-power/grid backlog, defense budgets) plus positive tape/flow confirmations, while a live two-sided geopolitical/oil supply-shock headline (e.g., US-Iran/Hormuz) is active, do not treat the pre-fetched oil direction as authoritative and do not score S0_SHARED_MACRO +1 merely by taking the constructive side (“peace-deal hopes, oil down”). A stale/misread oil print can flip the regime read from risk-on to risk-off at the open. On such days SPY may fall while XLI still rises modestly through defense/AI-power composition: direction can be right, but severe is not justified."
corrected_behavior: "When an Iran/Hormuz-style two-sided geopolitical headline is active, verify the oil sign from current overnight/news evidence before scoring S0; cap S0_SHARED_MACRO at 0, or negative if oil is confirmed up/risk-off. Keep the sector’s relative-strength signal (tape, flows, defe

## Per-scope DO-INSTEAD (from hypotheses)

### scope `general` — wins=7 losses=2
- **loss 2026-08-10:** [general] When score sign conflicts with sector ETF tape / breadth, cut conviction; prefer flat/mild.
- **win 2026-08-11:** [general] Keep direction; shrink confidence on modest |score| when magnitude historically misses.
- **win 2026-08-12:** [general] Keep direction; shrink confidence on modest |score| when magnitude historically misses.

### scope `news` — wins=1 losses=0
- **win news:** [news] Rank event families by 1d close, not ever-touch MFE.

### scope `sector_basic_materials` — wins=2 losses=1
- **win 2026-08-10:** [sector_basic_materials] Keep direction; shrink confidence on modest |score| when magnitude historically misses.
- **win 2026-08-11:** [sector_basic_materials] Keep direction; shrink confidence on modest |score| when magnitude historically misses.
- **loss 2026-08-12:** [sector_basic_materials] When score sign conflicts with sector ETF tape / breadth, cut conviction; prefer flat/mild.

### scope `sector_communication_services` — wins=1 losses=2
- **loss 2026-08-10:** [sector_communication_services] When score sign conflicts with sector ETF tape / breadth, cut conviction; prefer flat/mild.
- **loss 2026-08-11:** [sector_communication_services] When score sign conflicts with sector ETF tape / breadth, cut conviction; prefer flat/mild.
- **win 2026-08-12:** [sector_communication_services] Keep direction; shrink confidence on modest |score| when magnitude historically misses.

### scope `sector_consumer_cyclical` — wins=2 losses=1
- **win 2026-08-10:** [sector_consumer_cyclical] Keep direction; shrink confidence on modest |score| when magnitude historically misses.
- **loss 2026-08-11:** [sector_consumer_cyclical] When score sign conflicts with sector ETF tape / breadth, cut conviction; prefer flat/mild.
- **win 2026-08-12:** [sector_consumer_cyclical] Keep direction; shrink confidence on modest |score| when magnitude historically misses.

### scope `sector_consumer_defensive` — wins=1 losses=2
- **win 2026-08-10:** [sector_consumer_defensive] Keep direction; shrink confidence on modest |score| when magnitude historically misses.
- **loss 2026-08-11:** [sector_consumer_defensive] When score sign conflicts with sector ETF tape / breadth, cut conviction; prefer flat/mild.
- **loss 2026-08-12:** [sector_consumer_defensive] When score sign conflicts with sector ETF tape / breadth, cut conviction; prefer flat/mild.

### scope `sector_energy` — wins=2 losses=1
- **win 2026-08-10:** [sector_energy] Keep direction; shrink confidence on modest |score| when magnitude historically misses.
- **loss 2026-08-11:** [sector_energy] When score sign conflicts with sector ETF tape / breadth, cut conviction; prefer flat/mild.
- **win 2026-08-12:** [sector_energy] Keep direction; shrink confidence on modest |score| when magnitude historically misses.

### scope `sector_financial` — wins=2 losses=1
- **win 2026-08-10:** [sector_financial] Keep direction; shrink confidence on modest |score| when magnitude historically misses.
- **loss 2026-08-11:** [sector_financial] When score sign conflicts with sector ETF tape / breadth, cut conviction; prefer flat/mild.
- **win 2026-08-12:** [sector_financial] Keep direction; shrink confidence on modest |score| when magnitude historically misses.

### scope `sector_healthcare` — wins=2 losses=1
- **win 2026-08-10:** [sector_healthcare] Keep direction; shrink confidence on modest |score| when magnitude historically misses.
- **loss 2026-08-11:** [sector_healthcare] When score sign conflicts with sector ETF tape / breadth, cut conviction; prefer flat/mild.
- **win 2026-08-12:** [sector_healthcare] Keep direction; shrink confidence on modest |score| when magnitude historically misses.

### scope `sector_industrials` — wins=1 losses=2
- **loss 2026-08-10:** [sector_industrials] When score sign conflicts with sector ETF tape / breadth, cut conviction; prefer flat/mild.
- **win 2026-08-11:** [sector_industrials] Keep direction; shrink confidence on modest |score| when magnitude historically misses.
- **loss 2026-08-12:** [sector_industrials] When score sign conflicts with sector ETF tape / breadth, cut conviction; prefer flat/mild.

### scope `sector_real_estate` — wins=2 losses=1
- **win 2026-08-10:** [sector_real_estate] Keep direction; shrink confidence on modest |score| when magnitude historically misses.
- **win 2026-08-11:** [sector_real_estate] Keep direction; shrink confidence on modest |score| when magnitude historically misses.
- **loss 2026-08-12:** [sector_real_estate] When score sign conflicts with sector ETF tape / breadth, cut conviction; prefer flat/mild.

### scope `sector_technology` — wins=1 losses=2
- **loss 2026-08-10:** [sector_technology] When score sign conflicts with sector ETF tape / breadth, cut conviction; prefer flat/mild.
- **loss 2026-08-11:** [sector_technology] When score sign conflicts with sector ETF tape / breadth, cut conviction; prefer flat/mild.
- **win 2026-08-12:** [sector_technology] Keep direction; shrink confidence on modest |score| when magnitude historically misses.

### scope `sector_utilities` — wins=2 losses=1
- **win 2026-08-10:** [sector_utilities] Keep direction; shrink confidence on modest |score| when magnitude historically misses.
- **loss 2026-08-11:** [sector_utilities] When score sign conflicts with sector ETF tape / breadth, cut conviction; prefer flat/mild.
- **win 2026-08-12:** [sector_utilities] Keep direction; shrink confidence on modest |score| when magnitude historically misses.

## Open experiments

- **sector_industrials/win 2026-08-11:** [sector_industrials] On similar setups, test milder bands when |score|<4; log whether lagging tape factors overrode leading ones.
- **sector_industrials/loss 2026-08-12:** [sector_industrials] Require one extra confirming source in the dominant bucket before full weight when score sign matches this fail pattern.
- **sector_real_estate/win 2026-08-10:** [sector_real_estate] On similar setups, test milder bands when |score|<4; log whether lagging tape factors overrode leading ones.
- **sector_real_estate/win 2026-08-11:** [sector_real_estate] On similar setups, test milder bands when |score|<4; log whether lagging tape factors overrode leading ones.
- **sector_real_estate/loss 2026-08-12:** [sector_real_estate] Require one extra confirming source in the dominant bucket before full weight when score sign matches this fail pattern.
- **sector_technology/loss 2026-08-10:** [sector_technology] Require one extra confirming source in the dominant bucket before full weight when score sign matches this fail pattern.
- **sector_technology/loss 2026-08-11:** [sector_technology] Require one extra confirming source in the dominant bucket before full weight when score sign matches this fail pattern.
- **sector_technology/win 2026-08-12:** [sector_technology] On similar setups, test milder bands when |score|<4; log whether lagging tape factors overrode leading ones.
- **sector_utilities/win 2026-08-10:** [sector_utilities] On similar setups, test milder bands when |score|<4; log whether lagging tape factors overrode leading ones.
- **sector_utilities/loss 2026-08-11:** [sector_utilities] Require one extra confirming source in the dominant bucket before full weight when score sign matches this fail pattern.
- **sector_utilities/win 2026-08-12:** [sector_utilities] On similar setups, test milder bands when |score|<4; log whether lagging tape factors overrode leading ones.
- **news/win news:** [news] Track event-level 1d close win rate daily in learn_cycle.

## Methodology checklist (MEMORY_CONFIRM)

1. Did any open experiment for THIS scope (general/sector/news) apply today?
2. Missing factor that would have flipped a recent loss in this scope?
3. Overweighting one bucket / double-counting one headline?
4. For sectors: was S0 shared macro right but S1 sector factors wrong (or vice versa)?
5. For news: is event family still earning its weight on 1d close, not only MFE?

## Retired / falsified

_(append when a falsifier triggers)_
